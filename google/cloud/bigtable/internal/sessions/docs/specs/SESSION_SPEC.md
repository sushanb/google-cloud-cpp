# Session — C++ Behavioral Specification

**Scope.** This document governs the **runtime behavior** of a single `Session`
stream in the Cloud Bigtable C++ SDK
(`google/cloud/bigtable/internal/sessions/`):

-   **Layer 1 (Transport)**: `transport/session_stream.h`,
    `transport/session_stream.cc`, `transport/peer_info.h`,
    `transport/current_cq.h`, `transport/peak_ewma_tracker.h`.
-   **Layer 2 (Session)**: `session/session_impl.h`, `session/session_impl.cc`,
    `session/write_status.h`.
-   **Layer 4 (Middleware Retry & Cancellation)**: `middleware/retry_oracle.h`,
    `middleware/retrying_vrpc.h`, `middleware/retrying_vrpc.cc`,
    `middleware/cancellable_vrpc.h`, `middleware/cancellable_vrpc.cc`.

It specifies the session state machine, single-in-flight-vRPC slot lifecycle,
`PeerInfo` header resolution, callback hook ordering, graceful `Drain()` vs.
immediate `Close()` semantics, server `GOAWAY` handling, in-flight heartbeat
watchdog, 4-state retry & idempotency oracle, and C++ `grpc::ClientBidiReactor`
concurrency discipline. Any change to these files **MUST** comply with the 10
invariants below.

**Sibling behavioral specs.**

-   [`SESSION_CLIENT_SPEC.md`](SESSION_CLIENT_SPEC.md) —
    `SessionsDataConnection` & `Client` topology, `ClientDirectory`,
    `ClientConfigurationManager`, and `OpenSessionRequest` handshake envelope (4
    invariants).
-   [`SESSION_POOL_SPEC.md`](SESSION_POOL_SPEC.md) — Sharded `SessionPoolImpl`
    topology, two-tier AFE `DynamicPicker`, `Diverter` routing, non-blocking
    observability, `PoolSizer` scaling, and `SessionList` per-handle state
    machine (6 invariants).
-   [`CLIENT_SIDE_METRICS_SPEC.md`](CLIENT_SIDE_METRICS_SPEC.md) — Per-attempt
    `OperationContext` attribute provenance and `SessionTracer` OpenTelemetry
    histograms (3 invariants).

**Component/boundary spec.**
[`SESSION_COMPONENT_SPEC.md`](SESSION_COMPONENT_SPEC.md) — 7-layer DAG,
sole-owner matrix, lock hierarchy, and specification compliance guidelines
(`ALL MUSTs, and SHOULDs if practical`).

--------------------------------------------------------------------------------

### 1. State machine is strictly forward (`kConnecting -> kReady -> kDraining -> kClosing -> kClosed`)

-   **Five-State Reactor Lifecycle (`SessionImpl::State`,
    `session/session_impl.h`)**:

    ```
    kConnecting -> kReady -> kDraining -> kClosing -> kClosed
    ```

    -   `SessionPoolImpl` constructs and starts each `SessionImpl` immediately
        (`kConnecting`). Once `SessionParametersResponse` is processed, the
        session enters `kReady`. On `GoAwayResponse` or `Drain()` while an RPC
        is active, it enters `kDraining`. Stream close handshake
        (`CloseSessionRequest`, `StartWritesDone`, and waiting for server
        `OnDone`) executes inside `SessionImpl::InitiateStreamClose()` and
        `SessionStream::CloseSerialized()` while `state_` is `kClosing`,
        terminating in `kClosed`.
-   **Monotonic Forward Transitions**:

    -   State transitions **MUST** be monotonic under `state_mutex_`. No code
        path may transition backward (e.g., a late `GoAwayResponse` arriving in
        `kClosing` or `kClosed` **MUST** be ignored).
    -   `SendRequest` **MUST** reject any session whose state is not `kReady`
        with an uncommitted error (`StatusCode::kUnavailable` tagged with
        `kWriteStatusPayloadUrl = "0"`, mapping to
        `AttemptOutcomeState::kUncommitted`).
    -   `session/session_state.h` **MUST NOT** diverge from or shadow
        `SessionImpl::State`; `sessions_spec_test.cc` **MUST** exercise
        `SessionImpl` directly.

--------------------------------------------------------------------------------

### 2. One in-flight vRPC per session — enforced, not advisory

-   **Strict `multiplex = 1` Slot**:
    -   Each `SessionImpl` has at most **1** active wire slot (`active_rpc_`).
        Claiming the slot uses an atomic compare-and-swap on
        `active_rpc_.rpc_id` (from `0` to the positive `rpc_id` allocated by
        `pool_->AllocateRpcId()`).
    -   If the slot is already occupied (`active_rpc_.rpc_id != 0`) or
        `state_ != State::kReady`, `SendRequest` **MUST** reject the call
        immediately with `kUnavailable` tagged `kUncommitted` (`"0"`) so
        `RetryingVRpc` routes the attempt to another session without penalizing
        non-idempotent mutations.
-   **Write Failure Cleanup**:
    -   If `SessionStream` reports a write failure
        (`OnWriteDoneSerialized(false)`), `SessionImpl` **MUST** atomically
        clear `active_rpc_.rpc_id` back to `0`, reset `active_rpc_.context` and
        `write_completed`, decrement `active_rpc_count_`, transition to
        `kClosing`, and either fire `listener->OnSlotDrained` (if the caller
        already cancelled post-write) or invoke the vRPC callback with
        `UnavailableError("Write failed")` tagged `kUncommitted` (`"0"`). A
        failed write **MUST NEVER** leave the slot permanently wedged.
-   **Mutually-Exclusive Dual Slot-Release Paths**:
    -   Slot release is partitioned into two mutually-exclusive paths to
        preserve lock-free RCU fast-path reuse for synchronous callers:
    *   **Normal (Non-Cancelled) Completion Path**: When a vRPC completes
        normally (`VirtualRpcResponse` or `ErrorResponse`) without prior
        post-write caller cancellation, `SessionImpl` clears
        `active_rpc_.rpc_id` to `0` *before* invoking `wrapped_cb`. Inside
        `wrapped_cb`, `SessionPoolImpl::FinalizeSessionCall` synchronously calls
        `ReleaseToPoolFast(h)` (or `ReleaseToPool(h)`) *before* invoking the
        user callback. Thus, when a synchronous `SyncWaiter` wakes up on the
        user callback, the warm session is already back in the idle queue for
        immediate re-acquisition.
    *   **Post-Write Caller Cancellation Drain Path
        (`cancelled_awaiting_drain == true`)**: If the caller cancels a vRPC
        (`CancelVirtualRpcSerialized`) *after* the request frame was written to
        `SessionStream`, the caller's callback is invoked immediately with
        `CancelledError`, **but the wire slot remains occupied until the server
        sends the response or the stream closes**. Before invoking the cancelled
        callback, `SessionImpl` stamps `post_write_cancelling_rpc_id_` and sets
        `active_rpc_.cancelled_awaiting_drain = true`. When
        `FinalizeSessionCall` runs inside `wrapped_cb`, its first check (`if
        (session != nullptr && session->IsCancelledAwaitingDrain(rpc_id))
        return;`) returns early **without** releasing the handle to the pool or
        decrementing `in_flight_count`. Later, when the server's response
        arrives (or the stream closes), `SessionImpl` clears `active_rpc_` and
        invokes `listener->OnSlotDrained(shared_from_this(), drained_rpc_id)`,
        which executes the deferred `ReleaseToPool` and wakes a queued pool
        waiter (`SESSION_POOL_SPEC.md #6`).
-   **Mismatched `rpc_id` Protection**:
    -   Any `VirtualRpcResponse` or `ErrorResponse` whose `rpc_id` does not
        match `active_rpc_.rpc_id` **MUST** be logged and dropped without
        corrupting the active slot.

--------------------------------------------------------------------------------

### 3. `PeerInfo` (AFE ID & Transport Metadata) MUST be resolved synchronously before `OnSessionReady`

-   **Initial Metadata Parsing**:
    -   In `SessionStream::OnReadInitialMetadataDone(bool ok)`
        (`transport/session_stream.cc`),
        `transport::PeerInfo::FromHeaders(initial_metadata_)`
        (`transport/peer_info.h`) **MUST** parse the `"bigtable-peer-info"`
        header (URL-safe base64-encoded `google::bigtable::v2::PeerInfo`
        protobuf via `absl::WebSafeBase64Unescape` / `absl::Base64Unescape`) and
        populate:
    -   `application_frontend_id` (`int64_t`)
    -   `application_frontend_region` (`std::string`)
    -   `application_frontend_zone` (`std::string` — mapped to client-side
        metric label `transport_zone`)
    -   `application_frontend_subzone` (`std::string` — mapped to
        `transport_subzone` / `afe_location`)
    -   `transport_type` (`google::bigtable::v2::PeerInfo::TransportType`)
    -   For hermetic unit tests that inject plaintext metadata headers
        (`"bigtable-peer-info-afe-id"`, `"bigtable-peer-info-afe-subzone"`,
        `"bigtable-peer-info-afe-region"`,
        `"bigtable-peer-info-transport-type"`), `PeerInfo::FromHeaders` **MAY**
        fall back to those synthetic keys when `"bigtable-peer-info"` is absent.
-   **Synchronous Ordering Before `OnSessionReady`**:
    -   `SessionImpl::SetPeerInfo` **MUST** run during initial metadata
        processing before the first `SessionParametersResponse` transitions the
        session to `kReady` and fires
        `listener->OnSessionReady(shared_from_this())`.
-   **Zero AFE ID Normalization**:
    -   `application_frontend_id == 0` represents the legal `"unknown"` /
        `"default_peer"` AFE bucket. `SessionImpl::GetPeerId()` and
        `SessionPoolImpl` (`NormalizePeerId`) **MUST** normalize
        `application_frontend_id == 0` consistently to the same bucket key
        (`"default_peer"` / `0`) regardless of whether `backend_identifier_` is
        empty or populated, so `shard.connection_groups` and `SessionList` never
        split unknown-AFE sessions across two keys.

--------------------------------------------------------------------------------

### 4. Lifecycle hooks fire in a fixed order, at most once each

-   **`SessionListener` Ordering**:

    -   For any session that reaches `kReady`, listener notifications fire in
        strict order:

    ```
    OnSessionReady -> OnGoAway (optional, on graceful drain / GOAWAY) -> OnSessionClosed
    ```

    -   For a session that fails during the `kConnecting` handshake before
        receiving `SessionParametersResponse`, only `OnSessionClosed` fires
        (`OnSessionReady` and `OnGoAway` never fire).
-   **Exactly-Once Guarantee**:

    -   `OnGoAway` **MUST** fire at most once per session (on the first
        transition out of `kReady` into `kDraining`). Subsequent
        `GoAwayResponse` frames or `Drain()` calls are no-ops.
    -   `OnSessionClosed` **MUST** be guarded by
        `closed_notified_.exchange(true)` so it fires at most once across all
        teardown paths (`OnCloseSerialized`, `Cancel`, `Close`, destructor).

--------------------------------------------------------------------------------

### 5. `Drain()` (graceful) vs. `Close()` (immediate); `CloseSessionRequest` wire contract; write-once `disconnect_status_`

-   **`Drain()` vs. `Close()`**:
    -   **`SessionImpl::Drain()`** (`session/session_impl.cc`) performs
        **graceful close**: it transitions `kReady -> kDraining`, removes the
        session from pool routing via `OnGoAway`, allows any in-flight vRPC to
        complete within the drain grace period, and once
        `active_rpc_count_ == 0` calls `InitiateStreamClose(reason)` to send
        `CloseSessionRequest` on the wire before `StartWritesDone`.
    -   **`SessionImpl::Close()`** (`session/session_impl.cc`) performs
        **immediate close**: it transitions directly to `kClosing`, aborts any
        in-flight vRPC with `kUnavailable` tagged `kTransportFailure`, and
        cancels the underlying stream.
    -   Both `Drain()` and `Close()` **MUST** be idempotent and thread-safe from
        any caller or `CompletionQueue` thread.
-   **`CloseSessionRequest` Wire Frame & Reason**:
    -   Graceful stream close (`InitiateStreamClose` ->
        `SessionStream::CloseSerialized`) **MUST** construct a `SessionRequest`
        containing `CloseSessionRequest` and write it on the wire via
        `StartWriteLast(&current_write_request_, grpc::WriteOptions())` (or
        queue `pending_close_reason_` if a vRPC write is currently in progress
        on the reactor, flushing `StartWriteLast` inside `OnWriteDone`).
    -   `SessionStream::CloseSerialized` **MUST** keep `StartRead` active until
        the server finishes the stream (`OnDone`) or the fallback close timer
        (`200ms` in `InitiateStreamClose` / `5s` in
        `SessionStream::CloseSerialized`) cancels the stream.
    -   **Close Reason Enum**:
    -   When `InitiateStreamClose` is triggered by a server `GoAwayResponse`
        (`kGoAway`), it **MUST** stamp
        `CloseSessionRequest::CLOSE_SESSION_REASON_GOAWAY` (with description
        `"Server sent GO_AWAY"`).
    -   When triggered by client-initiated `Drain()`, it stamps
        `CloseSessionRequest::CLOSE_SESSION_REASON_USER`.
-   **Write-Once `disconnect_status_` Under `state_mutex_`**:
    -   The first terminal cause (`GOAWAY`, `MissedHeartbeat` in
        `CheckKeepAliveSerialized`, or stream error) **MUST** lock
        `state_mutex_` and set `disconnect_status_` only if
        `disconnect_status_.ok()` (write-once), so a subsequent stream EOF
        (`Cancelled` / `OK` on `OnDone`) never overwrites the root disconnect
        status.

--------------------------------------------------------------------------------

### 6. `GOAWAY` (`GoAwayResponse`) does NOT cancel the in-flight vRPC (`30s` drain grace)

When `SessionImpl::OnMessageSerialized` receives a `GoAwayResponse`:

1.  **State Guard**: Under `state_mutex_`, check
    `cur == State::kReady || cur == State::kConnecting`. If the session is
    already `kDraining`, `kClosing`, or `kClosed`, ignore the duplicate
    `GoAwayResponse` and return immediately.
2.  **Transition & Routing Eviction**: Transition `state_` to `State::kDraining`
    (or `State::kClosing` if `active_rpc_count_ == 0`) and record
    `last_admitted_rpc_id_ = message.go_away().last_rpc_id_admitted()`. Fire
    `listener->OnGoAway(shared_from_this(), message.go_away())` outside
    `state_mutex_` so `SessionPoolImpl` immediately pulls the session from
    `SessionList` routing structures and evaluates replacement via `PoolSizer`.
3.  **Diagnostic Logging**: Log `message.go_away().reason()` and
    `message.go_away().description()` via `BT_SESSIONS_LOG_DEBUG`.
4.  **In-Flight vRPC Continues Under a `30s` Drain Grace Timer**:
    -   The in-flight vRPC is **NOT** cancelled upon receiving `GoAwayResponse`.
    -   If `active_rpc_count_ > 0`, `SessionImpl` arms a bounded drain timer of
        **`30s`** by default
        (`kDefaultGoAwayDrainTimeout = std::chrono::seconds(30)`, overridable in
        unit tests). This ensures admitted non-idempotent `Apply` mutations
        taking >500ms on the backend complete normally rather than being
        prematurely aborted as non-retryable `TransportFailure`.
    -   As soon as the in-flight vRPC receives its `VirtualRpcResponse` /
        `ErrorResponse` (dropping `active_rpc_count_` to `0`), `SessionImpl`
        immediately calls `InitiateStreamClose(CLOSE_SESSION_REASON_GOAWAY)`. If
        the `30s` grace timer expires first, `SessionImpl` aborts the remaining
        call and force-closes the stream.
5.  **`last_rpc_id_admitted` Uncommitted Classification**:
    -   When `last_admitted_rpc_id_ > 0` and an in-flight vRPC has
        `rpc_id > last_admitted_rpc_id_`, the server explicitly certified that
        it never admitted `rpc_id`, so `SessionImpl` tags the error with
        `kWriteStatusPayloadUrl = "0"` (`kUncommitted`), allowing safe retry
        even for non-idempotent writes. When `last_admitted_rpc_id_ == 0`
        (proto3 unset) or `rpc_id <= last_admitted_rpc_id_`, stream termination
        without a response is tagged `kTransportFailure` (`"1"`).

--------------------------------------------------------------------------------

### 7. Heartbeat watchdog is armed ONLY while `state == kReady` and a vRPC is in flight

-   **In-Flight Gate**:
    -   `SessionImpl::CheckKeepAliveSerialized` (`session/session_impl.cc`)
        **MUST** only enforce the keepalive deadline when
        `state_ == State::kReady` and an active vRPC is in flight
        (`active_rpc_count_ > 0` / `rpc_start != 0`). Idle sessions legitimately
        receive no server heartbeats and **MUST NOT** be torn down by the
        heartbeat watchdog.
-   **Known-Frame-Only Deadline Reset**:
    -   Inbound frames reset `last_heartbeat_time_us_` **ONLY** when
        `SessionImpl::OnMessageSerialized` matches a recognized
        `SessionResponse::payload_case()` (`kOpenSession`, `kVirtualRpc`,
        `kError`, `kSessionParameters`, `kGoAway`, `kHeartbeat`,
        `kSessionRefreshConfig`, or wire field `8` heartbeat).
    -   `SessionStream::OnReadDone` **MUST NOT** unconditionally reset
        `last_heartbeat_time_us_` before payload inspection, and
        unknown/unrecognized `SessionResponse` frame types (`default:` in
        `OnMessageSerialized`) **MUST NOT** reset `last_heartbeat_time_us_`.
-   **Timeout Threshold**:
    -   The heartbeat timeout is `keepalive_interval_ns_ * keepalive_multiplier`
        (configured via `SessionsKeepaliveMultiplierOption`, default `3.0`).

--------------------------------------------------------------------------------

### 8. Missed-heartbeat sequence is deterministic and aborts with `kUnavailable` (`TransportFailure`)

When `CheckKeepAliveSerialized` detects `elapsed_ns > timeout_ns` while a vRPC
is in flight:

1.  Under `state_mutex_`, transition `state_` to `State::kClosing` and set
    `disconnect_status_ = internal::UnavailableError("Missed server keepalive
    heartbeat boundaries")` (using write-once semantics:
    `if (disconnect_status_.ok())`).
2.  Log the missed heartbeat via `BT_SESSIONS_LOG_WARN`.
3.  Abort the active vRPC with `StatusCode::kUnavailable` tagged with
    `kWriteStatusPayloadUrl = "1"` (`AttemptOutcomeState::kTransportFailure` —
    the request already left the client, so retry is safe only if
    `is_idempotent` is true).
4.  Cancel the underlying gRPC stream (`stream_->Cancel()`) without sending
    `CloseSessionRequest`.

--------------------------------------------------------------------------------

### 9. 4-State Retry & Idempotency Oracle (`kRejected`, `kUncommitted`, `kTransportFailure`, `kServerResult`)

Retry decisions in `RetryingVRpc` (`middleware/retrying_vrpc.cc`) and
`EvaluateRetryOracle` (`middleware/retry_oracle.h`) are driven by a **4-state
classification** (`AttemptOutcomeState`) carried via `google::cloud::Status`
payload `kWriteStatusPayloadUrl` (`"internal.bigtable.write_status"`), **never**
by guessing from raw gRPC status codes on untagged server responses:

| `AttemptOutcomeState`   | Status Payload Tag                   | Meaning & Tag Sites                  | Retry Rule                 |
:                         :                                      :                                      : (`EvaluateRetryOracle`)    :
| :---------------------- | :----------------------------------- | :----------------------------------- | :------------------------- |
| **`kRejected`**         | `"rejected"` (or pool-rejected       | Pool-level rejection before session  | **Never retry** in         |
:                         : status)                              : checkout\: waiter queue full         : `RetryingVRpc` (terminal;  :
:                         :                                      : (`RESOURCE_EXHAUSTED`), pool closed, : surfaced directly to       :
:                         :                                      : `ConsecutiveSessionFailureThreshold` : caller).                   :
:                         :                                      : circuit breaker tripped, no ready    :                            :
:                         :                                      : AFEs, or terminal handshake error.   :                            :
| **`kUncommitted`**      | `"0"` (`WriteStatus::kUncommitted`)  | Request never reached the wire or    | **Retry unconditionally**  |
:                         :                                      : server certified non-admission\:     : (for both idempotent and   :
:                         :                                      : session not `kReady`, slot busy,     : non-idempotent operations) :
:                         :                                      : pre-write caller cancel,             : within `max_attempts` and  :
:                         :                                      : `OnWriteDoneSerialized(false)` write : caller `deadline`.         :
:                         :                                      : failure, or `last_rpc_id_admitted >  :                            :
:                         :                                      : 0 && rpc_id >                        :                            :
:                         :                                      : go_away.last_rpc_id_admitted()`.     :                            :
| **`kTransportFailure`** | `"1"`                                | Request was written to the stream    | **Retry ONLY if            |
:                         : (`WriteStatus\:\:kTransportFailure`) : (`OnWriteDoneSerialized(true)` or    : `is_idempotent == true`**  :
:                         :                                      : in-flight write), and the stream     : and                        :
:                         :                                      : closed or missed heartbeats before a : `policy.OnFailure(status)` :
:                         :                                      : server response arrived.             : permits, within            :
:                         :                                      :                                      : `max_attempts` and         :
:                         :                                      :                                      : `deadline`.                :
| **`kServerResult`**     | `"2"` (`WriteStatus::kServerResult`, | Server delivered a definitive        | **Retry ONLY if            |
:                         : default when untagged)               : `ErrorResponse` frame on the session : `has_server_retry_info ==  :
:                         :                                      : stream (or response payload failed   : true` AND                  :
:                         :                                      : to decode).                          : `is_idempotent == true`**. :
:                         :                                      :                                      : If `ErrorResponse` has no  :
:                         :                                      :                                      : `RetryInfo`, **never       :
:                         :                                      :                                      : retry** regardless of gRPC :
:                         :                                      :                                      : status code.               :

Additional retry invariants:

-   **`RetryInfo` Delay & Policy Normalization**:
    -   When a `kServerResult` carries `ErrorResponse.retry_info`,
        `RetryingVRpc` uses `retry_info.retry_delay` directly (for any
        `delay >= 0s`) as the sleep duration instead of client exponential
        backoff, provided `now + delay < deadline` (if
        `now + delay >= deadline`, fail immediately without sleeping).
    -   `SessionsDataConnection`'s retry policy adapter normalizes
        `RetryInfo`-bearing errors so `policy.OnFailure` never blocks a
        server-authorized `RetryInfo` retry.
-   **`SafeIdempotentMutationPolicy`**:
    -   `SafeIdempotentMutationPolicy::is_idempotent`
        (`google/cloud/bigtable/idempotent_mutation_policy.cc`) **MUST** switch
        on `m.mutation_case()` and return `true` **ONLY** for `kSetCell` with an
        explicit client timestamp (`timestamp_micros != -1`),
        `kDeleteFromColumn`, `kDeleteFromFamily`, and `kDeleteFromRow`. It
        **MUST** return `false` for `kAddToCell` and `kMergeToCell`.
-   **Hard 3-Attempt Cap**:
    -   Total attempts per operation are capped at `kMaxAttempts = 3`
        (`middleware/retry_oracle.h`).
-   **Transport Turnover Backoff**:
    -   On the first `kUncommitted` / `kTransportFailure` session turnover
        (`attempt == 1`), `RetryingVRpc` applies a `0-2ms` uniform jitter to
        prevent synchronized thundering-herd retries across 16 shards; on
        subsequent transport turnovers (`attempt >= 2`), it uses
        `BackoffPolicy::OnCompletion()` exponential backoff so a total backend
        outage cannot spin CPU in a zero-delay loop.

--------------------------------------------------------------------------------

### 10. C++ Reactor & `CompletionQueue` Concurrency Discipline

-   **Single-Reactor Serialization in `SessionStream`**:
    -   `SessionStream` (`transport/session_stream.cc`) implements
        `grpc::ClientBidiReactor<SessionRequest, SessionResponse>`.
    -   Every stream mutation (`StartOpenSerialized`, `WriteSerialized`,
        `CloseSerialized`, `Cancel`) is serialized onto the stream's bound
        `CompletionQueue` via `CurrentCq::IsCurrent(cq_)` (inline when already
        on `cq_`) or `cq_->RunAsync(...)`.
    -   gRPC forbids concurrent `StartWrite` / `StartWriteLast` calls on a
        single bidi stream. `SessionStream` enforces a strict
        `write_in_progress_` state machine: if `CloseSerialized(reason)` arrives
        while a vRPC write is in flight, it records
        `pending_close_reason_ = reason` and defers `StartWriteLast` until
        `OnWriteDone(true)` fires.
    -   `StartCall()` is invoked only after `StartRead(&current_read_response_)`
        and the initial `StartWrite(&current_write_request_)` are queued,
        guarded by `started_`.
    -   Self-retaining `std::shared_ptr<SessionStream> self_ref_` is released
        strictly inside `OnDone(const grpc::Status&)`.
-   **Lock Ordering & Callback Discipline in `SessionImpl`**:
    -   `SessionImpl::state_mutex_` is an innermost leaf mutex guarding
        `state_`, `disconnect_status_`, and `active_rpc_` metadata.
    -   `SessionImpl` **MUST NEVER** invoke `SessionListener` hooks
        (`OnSessionReady`, `OnGoAway`, `OnSessionClosed`, `OnSlotDrained`,
        `OnConfigUpdated`) or vRPC completion callbacks while holding
        `state_mutex_`.

--------------------------------------------------------------------------------

**See [`SESSION_COMPONENT_SPEC.md`](SESSION_COMPONENT_SPEC.md)** for the 7-layer
dependency DAG, sole-owner matrix, and lock hierarchy rules.
