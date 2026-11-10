# SessionPool, AFE Picking & Shim Routing — C++ Behavioral Specification

**Scope.** This document governs the **runtime behavior** of the session pool,
AFE picker, traffic diverter, and data-connection routing in the Cloud Bigtable
C++ SDK (`google/cloud/bigtable/internal/sessions/`):

-   **Layer 3 (Session Pool & Primitives)**: `pool/session_pool_impl.h`,
    `pool/session_pool_impl.cc`, `pool/session_pool_lifecycle.cc`,
    `pool/session_list.h`, `pool/dynamic_picker.h`, `pool/dynamic_picker.cc`,
    `pool/pool_sizer.h`, `pool/pool_sizer.cc`, `pool/session_creation_budget.h`,
    `pool/metrics_collector.h`, `pool/metrics_collector.cc`.
-   **Layer 6 (Diverter & Shim Routing)**: `shim/diverter.h`,
    `sessions_data_connection.h`, `sessions_data_connection.cc`.
-   **Layer 7 (Debug Snapshot)**: `telemetry/debug_snapshot.h`.

It specifies separate read/write pool isolation, the two-tier AFE
`DynamicPicker` (`K=2` P2C + `status.ok()`-gated `PeakEwma`), `Diverter` +
`SessionsDataConnection` method routing, non-blocking observability,
server-driven `PoolSizer` scaling, and the `SessionList` per-handle state
machine (`I1`–`I6`). Any change to these files **MUST** comply with the 6
invariants below.

**Sibling behavioral specs.**

-   [`SESSION_SPEC.md`](SESSION_SPEC.md) — Per-Session lifecycle, state machine,
    slot drain, and retry oracle (10 invariants).
-   [`SESSION_CLIENT_SPEC.md`](SESSION_CLIENT_SPEC.md) —
    `SessionsDataConnection` & `Client` topology, `ClientDirectory`,
    `ClientConfigurationManager`, and `OpenSessionRequest` handshake envelope (4
    invariants).
-   [`CLIENT_SIDE_METRICS_SPEC.md`](CLIENT_SIDE_METRICS_SPEC.md) — Per-attempt
    `OperationContext` attribute provenance and `SessionTracer` OpenTelemetry
    histograms (3 invariants).

**Component/boundary spec.**
[`SESSION_COMPONENT_SPEC.md`](SESSION_COMPONENT_SPEC.md) — 7-layer DAG,
sole-owner matrix, lock hierarchy, and specification compliance guidelines.

--------------------------------------------------------------------------------

### 1. Every table resource has separate Read and Write session pools, with strict cross-pool isolation

-   **Separate Read and Write Pools (`SESSION_POOL_SPEC.md #1`)**:
    -   Read operations (`ReadRow`, `AsyncReadRow`) acquire sessions from the
        table's `PERMISSION_READ` `SessionPoolImpl`; write operations (`Apply`,
        `AsyncApply`) acquire sessions from the table's `PERMISSION_WRITE`
        `SessionPoolImpl`.
    -   Read and write pools **never** share `SessionImpl` instances or waiter
        queues, preventing write spikes from starving point reads under the
        `multiplex = 1` rule (`SESSION_SPEC.md #2`).
-   **Strict Cross-Pool `thread_local` Drain Isolation**:
    -   `SessionPoolImpl::DrainQueue` (`pool/session_pool_impl.cc`) uses a
        thread-local trampoline (`in_drain` / `pending_drains`) to flatten
        recursive waiter callbacks without stack overflow.
    -   Because a user callback completing on Pool A (e.g. `ReadRow`) can
        synchronously issue an RPC on Pool B (e.g. `Apply`) on the same thread,
        every entry in `thread_local pending_drains` **MUST** store its owning
        `std::weak_ptr<SessionPoolImpl>` alongside
        `{Shard*, std::shared_ptr<ConnectionGroup>}` and dispatch each deferred
        drain to its own pool instance. Indexing `self->shards_[shard_idx]` on a
        foreign `SessionPoolImpl*` is a cross-pool memory corruption bug and is
        strictly forbidden.

--------------------------------------------------------------------------------

### 2. Two-Tier AFE Picker (`K=2` Power-of-Two-Choices, `LEAST_IN_FLIGHT` default, `status.ok()`-gated `PeakEwma`) & FIFO Idle Queue

-   **Sharded RCU Fast Path + Two-Tier AFE Dispatch**:
    -   `SessionPoolImpl` partitions sessions across `num_shards_` (`>= 16`
        shards, `SessionsNumShardsOption`) and publishes an immutable RCU
        snapshot
        (`std::shared_ptr<const std::vector<std::shared_ptr<ConnectionGroup>>>`)
        per shard so hot-path session checkouts run lock-free (`atomic_load`).
    -   Within each shard, sessions are grouped by AFE (`ConnectionGroup` /
        `AfeBucket` in `SessionList`, keyed by
        `PeerInfo.application_frontend_id`). Checkout selects:
    *   **Tier 1 (AFE Selection)**: Filter to AFEs with `>= 1` idle session
        (`ready_afes`) and pick an AFE using the configured load-balancing
        strategy.
    *   **Tier 2 (Session Dequeue)**: Pop the longest-waiting idle session from
        that AFE's `IdleSessionQueue` in **FIFO** order
        (`std::deque::pop_front()`) so traffic rotates evenly across all open
        streams to that AFE rather than repeatedly reusing only the most
        recently returned stream (LIFO).
-   **Load-Balancing Strategies & Defaults (`SESSION_CLIENT_SPEC.md #3`)**:
    -   Configured dynamically via
        `SessionPoolConfiguration.load_balancing_options`: | Strategy | Behavior
        | Default Subset Width (`K`) | | :--- | :--- | :--- | |
        **`kLeastInFlight` (Default)** | Power-of-`K` choices: sample `K` ready
        AFEs uniformly at random, pick minimum `in_flight_count`. | `K = 2`
        (`default_random_subset_size = 2`) | | **`kLeastLatency` (`PEAK_EWMA`)**
        | Power-of-`K` choices: sample `K` ready AFEs uniformly at random, pick
        minimum `e2e_ewma` cost (never divided by `peer_weight`). | `K = 2` | |
        **`kRandom` (`RANDOM`)** | Uniform random over all ready AFEs. | N/A | |
        **`kWeightedRoundRobin`** | Smooth Weighted Round Robin proportional to
        `SessionParametersResponse` peer weight. | N/A |
    -   Unrecognized strategy enums **MUST** fall back to `kLeastInFlight` with
        `K = 2`.
-   **`PeakEwma` Seeds & `status.ok()` Gating**:
    -   Per-AFE `OkGatedPeakEwma` (`pool/session_list.h`) is seeded at
        **`e2e_ewma = 1ms` (`1000us`)** and **`transport_ewma = 500us`** so
        newly discovered AFEs do not appear zero-cost and attract a thundering
        herd.
    -   **`status.ok()` Gate**: Latency trackers (`OkGatedPeakEwma` and
        `PeakEwmaTracker`) **MUST ONLY** be updated when `status.ok()` is
        `true`. Failed or cancelled vRPCs (especially `0ns` write/stream errors)
        **MUST NEVER** update EWMA trackers, as feeding `0ns` on error drives a
        broken AFE's EWMA toward zero and causes `kLeastLatency` to route all
        traffic into the failing AFE.
    -   `kLeastLatency` routes on `e2e_ewma`; `transport_ewma` is recorded for
        telemetry (`PoolSnapshot`) only.
-   **Pool-Level Rejections & Circuit Breaker**:
    -   When no session is ready and a call cannot be queued (waiter queue full
        at `max_queued_calls_`, pool closing/closed, terminal handshake error in
        `{kInvalidArgument, kNotFound, kPermissionDenied, kFailedPrecondition,
        kUnimplemented}`, or `ConsecutiveSessionFailureThreshold = 15`
        consecutive session-open failures reached), `SessionPoolImpl` **MUST**
        fail the call (and drain queued waiters) with
        `AttemptOutcomeState::kRejected` (never `kUncommitted` `"0"`), so
        `RetryingVRpc` returns the error immediately to the caller without
        retrying (`SESSION_SPEC.md #9`).

--------------------------------------------------------------------------------

### 3. Traffic Split (`Diverter` + `SessionsDataConnection`), Sticky `UNIMPLEMENTED` Breaker, and `DataConnection` Method Routability

-   **`Diverter` Policy Layer (`shim/diverter.h`)**:
    -   `Diverter` holds `session_load_` (`std::atomic<double>`, `0.0`–`1.0`),
        updated in production **exclusively** by `ClientConfigurationManager`
        (`SESSION_COMPONENT_SPEC.md §B6`), ignoring `NaN` inputs.
    -   `Diverter::TripUnimplementedBreaker()` atomically latches
        `unimplemented_tripped_ = true` with permanent sticky precedence over
        any subsequent `SetSessionLoad` poll from `ClientConfigurationManager`.
    -   C++ trips `TripUnimplementedBreaker()` on the first
        `StatusCode::kUnimplemented` response from `OpenTable` / vRPC (threshold
        `1`), providing instant zero-overhead fallback when running against the
        Cloud Bigtable Emulator or environments without session support.
-   **`SessionsDataConnection` Mechanism Layer & Fallback Gate**:
    -   On `ReadRow` and `AsyncReadRow`: if
        `status.code() == StatusCode::kUnimplemented`, `SessionsDataConnection`
        trips `diverter_.TripUnimplementedBreaker()` and falls back to
        `classic_connection_->ReadRow` / `AsyncReadRow`.
    -   On `Apply` and `AsyncApply`: if
        `status.code() == StatusCode::kUnimplemented`, `SessionsDataConnection`
        always trips `diverter_.TripUnimplementedBreaker()`, and falls back to
        `classic_connection_->Apply` / `AsyncApply` **ONLY IF**
        `is_idempotent || NeverLeftClient(status)`. A non-idempotent mutation
        (`kAddToCell`, `kMergeToCell`, or server-timestamp `kSetCell`) that
        already left the client is **never** re-executed on
        `classic_connection_`.
-   **Operation Routability Matrix (`DataConnection` Virtual Methods)**:
    -   `shim::IsRoutableToSessions` (`shim/diverter.h`) and
        `SessionsDataConnection` route `google::cloud::bigtable::DataConnection`
        virtual methods by operation shape: | `bigtable::DataConnection` Method
        | Routing Target | Architectural Rationale | | :--- | :--- | :--- | |
        **`ReadRow`** | **Sessions** (via `Diverter`) | Distinct point-read
        virtual method on `bigtable::DataConnection`; maps 1:1 to
        `SessionReadRowRequest` vRPC. | | **`AsyncReadRow`** | **Sessions** (via
        `Diverter`) | Distinct async point-read virtual method on
        `bigtable::DataConnection`; maps 1:1 to `SessionReadRowRequest` vRPC. |
        | **`Apply`** (unconditional single-row) | **Sessions** (via `Diverter`)
        | Distinct point-write virtual method on `bigtable::DataConnection`;
        maps 1:1 to `SessionMutateRowRequest` vRPC. | | **`AsyncApply`**
        (unconditional single-row) | **Sessions** (via `Diverter`) | Distinct
        async point-write virtual method on `bigtable::DataConnection`; maps 1:1
        to `SessionMutateRowRequest` vRPC. | | **`ReadRows` / `AsyncReadRows`**
        | **Always `classic_connection_`** | In `google-cloud-cpp`,
        `Table::ReadRow` and `Table::AsyncReadRow` are already separate
        first-class virtual methods on `bigtable::DataConnection` rather than
        wrappers over `ReadRows`. Keeping `ReadRows` / `AsyncReadRows` on
        `classic_connection_` avoids synthesizing a single-row `RowReader`
        stream adapter while preserving full `ReadRows` streaming semantics. | |
        **`BulkApply` / `AsyncBulkApply`** | **Always `classic_connection_`** |
        Multi-row `MutateRows` streaming RPC has no vRPC equivalent. | |
        **`CheckAndMutateRow` / `AsyncCheckAndMutateRow`** | **Always
        `classic_connection_`** | Conditional mutation has no vRPC equivalent. |
        | **`ReadModifyWriteRow` / `AsyncReadModifyWriteRow`** | **Always
        `classic_connection_`** | Atomic read-modify-write has no vRPC
        equivalent. | | **`SampleRows` / `AsyncSampleRows`** | **Always
        `classic_connection_`** | Table-wide key sampling stays on classic
        streaming RPC. |

--------------------------------------------------------------------------------

### 4. Observability (`PoolSnapshot` & `MetricsCollector`) MUST NOT block hot-path vRPC latencies

-   **Lock-Free Hot-Path `MetricsCollector`**:
    -   `MetricsCollector::OnRequestComplete` (`pool/metrics_collector.cc`) runs
        on every vRPC completion and **MUST NOT** acquire contended mutexes
        (`ReservoirSampler::mu_` or `error_mutex_`) on the hot path. Hot-path
        metric recording **MUST** use atomic counters and OpenTelemetry
        histogram instruments (`SessionTracer`).
-   **Non-Blocking `GetPoolSnapshot()`**:
    -   `SessionPoolImpl::GetPoolSnapshot()` (`telemetry/debug_snapshot.h`)
        **MUST** read atomic counters and RCU `ConnectionGroup` snapshots
        without taking write locks across all 16 shards simultaneously.
-   **OpenTelemetry & In-Memory `PoolSnapshot` Observability**:
    -   Client-side and session lifecycle telemetry are exported exclusively via
        OpenTelemetry (`CLIENT_SIDE_METRICS_SPEC.md`) and bounded in-memory
        `PoolSnapshot` value structs, without starting embedded HTTP debug
        servers or hot-path ring buffers.

--------------------------------------------------------------------------------

### 5. Session pool scaling (`PoolSizer`) is server-driven and rate-limited by `SessionCreationBudget`

-   **Authoritative `PoolSizer` Formula (`SESSION_COMPONENT_SPEC.md` Part A)**:

    -   `PoolSizer::GetScaleDelta` (`pool/pool_sizer.cc`) **MUST** delegate
        directly to `pool::ComputeDesiredPoolCapacity` (`pool/session_list.h`)
        using separate `in_use` and `pending` counts:

    ```
    EffectivePending = ceil(PendingCount / NewSessionQueueLength)   // default NewSessionQueueLength = 10
    SessionsInUse    = InUseCount + EffectivePending
    IdleHeadroom     = max(MinIdleSessions, ceil(SessionsInUse * HeadroomPct)) // default MinIdleSessions = 1, HeadroomPct = 0.10
    DesiredRaw       = SessionsInUse + IdleHeadroom
    DesiredCapacity  = clamp(DesiredRaw, MinSessions, MaxSessions)
    ScaleDelta       = DesiredCapacity - (ReadyCount + ConnectingCount)
    ```

    -   Dividing `PendingCount` by `NewSessionQueueLength` (`10`) prevents a
        cold-start burst of 50 queued calls from spawning 60 sessions when 6
        sessions can drain the queue.
    -   `MaybeReplenishPool` **MUST NOT** perform O(N) scans over all sessions
        for `avg_write_queue_size` or bypass `ScaleDelta <= 0`.
-   **`OnGoAway` & `OnSessionClosed` Replacement Gate**:

    -   When a session transitions to `kDraining` (`OnGoAway`) or closes
        (`OnSessionClosed`), `SessionPoolImpl` **MUST** consult `PoolSizer`
        (`DesiredCapacity > ReadyCount + ConnectingCount`) before spawning a
        replacement session, rather than unconditionally replacing up to
        `max_sessions`.
    -   This provides **passive scale-down** as server `GOAWAY`s rotate sessions
        after a load spike subsides. In addition, `SessionPoolImpl` runs a
        background `ReapIdleSessions()` pass that drains sessions idle for
        `> 5 minutes` down to `MinSessions`.
-   **Rate-Limiting & Timeouts**:

    -   Concurrent `OpenSession` handshakes are gated by `SessionCreationBudget`
        (default `NewSessionCreationBudget = 50`,
        `NewSessionCreationPenalty = 2s` backoff on failure). Initial session
        creation in `SessionPoolImpl::Create` uses `PoolSizer` / `TryReserve()`
        rather than bypassing the budget.
    -   Every `OpenSession` handshake carries a uniform **`30s` deadline**, and
        a 5-minute pool watchdog sweeps any session stuck in `draining_sessions`
        (`kClosing`) past its close timer.

--------------------------------------------------------------------------------

### 6. `SessionList` tracks a strict per-handle state machine (`I1`–`I6`) with lock-free RCU fast-path checkout/release

-   **Per-Handle State Machine (`pool/session_list.h`)**:

    ```
    kNotRegistered -> kIdle <-> kInFlight -> kClosing -> kClosed
    ```
-   **Six Named Invariants (`I1`–`I6`)**:

    -   **I1**: `handle->in_expected_count == true` $\implies$
        `handle_to_group_[session] != nullptr`.
    -   **I2**: `ready_count_` == number of handles in `handle_to_group_` with
        `in_expected_count == true`.
    -   **I3**: `afes_with_ready` == set of `AfeBucket`s with `idle_count > 0`.
    -   **I4**: `bucket.ref_count` == count of handles in `handle_to_group_`
        pointing to `bucket` (`kIdle` + `kInFlight` + `kClosing`).
    -   **I5**: Every session in `idle_queue` has `handle->state == kIdle`,
        `handle->in_expected_count == true`, and `active_rpc_.rpc_id == 0`.
        `ReleaseToPool` / `ReleaseToPoolFast` **MUST** early-return if
        `!handle->in_expected_count` (session received `GOAWAY` /
        `OnSessionClosing` while in flight) AND `FinalizeSessionCall` **MUST**
        early-return if `session->IsCancelledAwaitingDrain(rpc_id)` is true
        (deferring release to `OnSlotDrained`).
    -   **I6**: `bucket.ref_count` is decremented **ONLY** on `OnSessionClosed`
        (never on `OnSessionClosing`), keeping AFE capacity accounting stable
        during the `GOAWAY` drain window. When `bucket.ref_count` reaches `0`,
        empty `ConnectionGroup` / `AfeBucket` entries are pruned.
-   **Lock-Free RCU Fast Path vs. `SessionList::mu_`**:

    -   Lifecycle transitions (`OnSessionStarted`, `OnSessionClosing`,
        `OnSessionClosed`) acquire `SessionList::mu_` (after `Shard::mutex`) and
        publish a new RCU `ConnectionGroup` vector snapshot. Hot-path vRPC
        checkout and normal release use atomic CAS on `SessionHandle::state`
        (`MarkHandleInFlightFast`: `kIdle -> kInFlight`; `ReleaseToPoolFast`:
        `kInFlight -> kIdle`, gated on `in_expected_count`), with
        `GetConnectionGroupsSnapshot()` reconciling group counts when snapshots
        are rebuilt.

--------------------------------------------------------------------------------

**See [`SESSION_COMPONENT_SPEC.md`](SESSION_COMPONENT_SPEC.md)** for the 7-layer
dependency DAG, sole-owner matrix, and lock hierarchy rules.
