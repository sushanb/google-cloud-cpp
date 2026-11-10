# Session Subsystem — C++ Component Topology, Boundary & Strictness Specification

**Scope.** This document governs the **structural layout**, **layer
boundaries**, **sole-owner matrix**, **lock hierarchy**, and **specification
compliance guidelines** (`ALL MUSTs, and SHOULDs if practical`) for the Cloud
Bigtable C++ SDK Sessions subsystem
(`google/cloud/bigtable/internal/sessions/`).

**Companion behavioral specs.**

-   [`SESSION_SPEC.md`](SESSION_SPEC.md) — Per-Session lifecycle, state machine,
    slot drain, and 4-state retry oracle (10 invariants).
-   [`SESSION_CLIENT_SPEC.md`](SESSION_CLIENT_SPEC.md) —
    `SessionsDataConnection` & `Client` topology, `ClientDirectory`,
    `ClientConfigurationManager`, and `OpenSessionRequest` handshake envelope (4
    invariants).
-   [`SESSION_POOL_SPEC.md`](SESSION_POOL_SPEC.md) — Sharded `SessionPoolImpl`
    topology, two-tier AFE `DynamicPicker`, `Diverter` & operation routability
    matrix, non-blocking observability, `PoolSizer` scaling, and `SessionList`
    per-handle state machine (6 invariants).
-   [`CLIENT_SIDE_METRICS_SPEC.md`](CLIENT_SIDE_METRICS_SPEC.md) — Per-attempt
    `OperationContext` attribute provenance and `SessionTracer` OpenTelemetry
    histograms (3 invariants).

**File layout.**

-   **Part A — 7-Layer Architecture & Capacity Sizing**: Directory structure and
    `PoolSizer` decision anatomy.
-   **Part B — Prescriptive Boundary Invariants (`§B1`–`§B10`, `MUST` rules)**:
    Compile-time `BUILD.bazel` targets, exhaustive `sessions_layer_lint_test`
    enforcement, single-writer config authority, and lock hierarchy DAG.
-   **Part C — Sole-Owner Matrix**: Authoritative owner for every runtime state
    field and metric.
-   **Part D — Spec Strictness Guidelines
    (`ALL MUSTs, and SHOULDs if practical`)**: Engineering rubric distinguishing
    non-negotiable wire/safety/telemetry `MUST`s from practical `SHOULD`s and
    C++-idiomatic concurrency choices.

--------------------------------------------------------------------------------

## Part A — 7-Layer Architecture & Capacity Sizing

### 1. Seven-Layer Bottom-Up Architecture (`google/cloud/bigtable/internal/sessions/`)

| Layer        | Directory / Target            | Core Responsibilities & Key    | Allowed Downward Dependencies |
:              : (`BUILD.bazel`)               : Files                          :                               :
| :----------- | :---------------------------- | :----------------------------- | :---------------------------- |
| **L1:        | `transport/`                  | Single bidi gRPC reactor       | Protobuf                      |
: Transport**  : (`sessions_l1_transport`)     : (`SessionStream`), `PeerInfo`  : (`google\:\:bigtable\:\:v2`), :
:              :                               : header decoding, `CurrentCq`   : gRPC, `google\:\:cloud` core. :
:              :                               : thread-local guard,            :                               :
:              :                               : `PeakEwmaTracker`.             :                               :
| **L2:        | `session/`                    | Single-stream state machine    | **L1** (`transport/`), **L7** |
: Session**    : (`sessions_l2_session`)       : (`SessionImpl`), `multiplex=1` : (`telemetry/`).               :
:              :                               : slot, `GOAWAY` 30s drain,      :                               :
:              :                               : heartbeat watchdog,            :                               :
:              :                               : `WriteStatus` tags.            :                               :
| **L3: Pool** | `pool/` (`sessions_l3_pool`)  | Sharded `SessionPoolImpl`      | **L1** (`transport/`), **L2** |
:              :                               : (`session_pool_impl.cc`,       : (`session/`), **L7**          :
:              :                               : `session_pool_lifecycle.cc`),  : (`telemetry/`).               :
:              :                               : `SessionList` (`I1`–`I6`),     :                               :
:              :                               : `DynamicPicker`, `PoolSizer`,  :                               :
:              :                               : `SessionCreationBudget`,       :                               :
:              :                               : `PoolExecutors`,               :                               :
:              :                               : `LightweightThreadPool`,       :                               :
:              :                               : `MetricsCollector`.            :                               :
| **L4:        | `middleware/`                 | 4-state `RetryOracle`,         | **L1** (`transport/`), **L2** |
: Middleware** : (`sessions_l4_middleware`)    : `RetryingVRpc` retry loop,     : (`session/`), **L3**          :
:              :                               : `CancellableVRpc` caller       : (`pool/`), **L7**             :
:              :                               : cancellation bridge.           : (`telemetry/`).               :
| **L5:        | `client.*`, `client/`         | Proto-native `Client`          | **L1**–**L4**, **L7**         |
: Client**     : (`sessions_l5_client`)        : (`SessionClientInterface`),    : (`telemetry/`).               :
:              :                               : `ClientConfigurationManager`,  :                               :
:              :                               : `SyncWaiter`, `MakePoolKey`.   :                               :
| **L6: Shim** | `sessions_data_connection.*`, | `SessionsDataConnection`       | **L1**–**L5**, **L7**, and    |
:              : `shim/` (`sessions_l6_shim`)  : (implementing                  : public                        :
:              :                               : `bigtable\:\:DataConnection`), : `google\:\:cloud\:\:bigtable` :
:              :                               : `ClientDirectory`, `Diverter`, : model headers.                :
:              :                               : `IsRoutableToSessions`. Sole   :                               :
:              :                               : Proto $\leftrightarrow$ Public :                               :
:              :                               : SDK (`Row`,                    :                               :
:              :                               : `SingleRowMutation`) boundary. :                               :
| **L7:        | `telemetry/`                  | Header-only OpenTelemetry      | OpenTelemetry API, Protobuf,  |
: Telemetry**  : (`sessions_l7_telemetry`)     : `SessionTracer` and            : `google\:\:cloud` core (leaf  :
:              :                               : non-blocking `PoolSnapshot` /  : observability layer).         :
:              :                               : `SessionSnapshot` DTOs         :                               :
:              :                               : (`debug_snapshot.h`).          :                               :

### 2. `PoolSizer` Decision Anatomy

`pool::ComputeDesiredPoolCapacity` (`pool/session_list.h`, called by
`PoolSizer::GetScaleDelta`) is the single source of truth for desired session
capacity:

```
EffectivePending = ceil(PendingCount / NewSessionQueueLength)   // default NewSessionQueueLength = 10
SessionsInUse    = InUseCount + EffectivePending
IdleHeadroom     = max(MinIdleSessions, ceil(SessionsInUse * HeadroomPct)) // default MinIdleSessions = 1, HeadroomPct = 0.10
DesiredRaw       = SessionsInUse + IdleHeadroom
DesiredCapacity  = clamp(DesiredRaw, MinSessions, MaxSessions)
ScaleDelta       = DesiredCapacity - (ReadyCount + ConnectingCount)
```

--------------------------------------------------------------------------------

## Part B — Prescriptive Boundary Invariants (`MUST` Rules)

### B1. Layers L1–L5 and L7 MUST stay proto-native

-   Files in `transport/`, `session/`, `pool/`, `middleware/`, `client/`, and
    `telemetry/` **MUST NOT** `#include` public `google::cloud::bigtable` SDK
    model headers (`google/cloud/bigtable/row.h`, `mutations.h`, `filters.h`,
    `table.h`, `row_reader.h`, `read_modify_write_rule.h`).
-   `SessionsDataConnection` (L6) is the **sole translation boundary** between
    `google::bigtable::v2` protobuf messages and public C++ SDK types
    (`bigtable::Row`, `bigtable::SingleRowMutation`, `bigtable::Filter`).

### B2. Downward 7-Layer DAG MUST be enforced at compile time and by exhaustive include lint

-   `google/cloud/bigtable/BUILD.bazel` defines per-layer targets
    (`sessions_l1_transport` through `sessions_l7_telemetry`) whose `deps`
    strictly follow the Part A table.
-   `sessions_layer_lint_test.cc` (`B2_ExhaustiveIncludeScanOverAllSources`)
    dynamically walks every `.h` and `.cc` file under
    `google/cloud/bigtable/internal/sessions/` and verifies both:
    1.  No file in layer $L_i$ includes a header from a higher layer $L_j$ ($j >
        i$, with L7 `telemetry/` treated as a leaf utility layer).
    2.  No file in L1–L5 or L7 includes public `google::cloud::bigtable` model
        headers (§B1).

### B3. Observability consumers MUST access pool/session state ONLY via value snapshots (`PoolSnapshot`)

-   Diagnostic callers and tests inspecting pool health **MUST** consume
    `PoolSnapshot` / `SessionSnapshot` value structs
    (`telemetry/debug_snapshot.h`) returned by `GetPoolSnapshot()`, never raw
    pointers into `SessionPoolImpl::Shard` or `SessionList` internals.

### B4. `Diverter` MUST remain operation-shape-agnostic

-   `shim::Diverter` (`shim/diverter.h`) takes `session_load` (`SetSessionLoad`)
    and `TripUnimplementedBreaker()`, exposes `UseSession()`, and tracks atomic
    pick counters (`session_picks()`, `classic_picks()`).
-   Operation-shape routability (`ReadRow`/`Apply` vs. `ReadRows`/`BulkApply`)
    lives in `shim::IsRoutableToSessions` and `SessionsDataConnection`, never
    inside `Diverter`.

### B5. `SessionsDataConnection` MUST interact with L3 `SessionPoolImpl` through L5 `Client`

-   `SessionsDataConnection` owns 16 `Client` shards per
    `{table, app_profile, permission}` entry (`Client::Create` for shard 0,
    `Client::CreateWithPool` for shards `1..15`) and forwards configuration
    updates via `Client::UpdatePoolConfig`.
-   `SessionsDataConnection` **MUST NOT** retry a session-path RPC failure on
    `classic_connection_` unless the failure is `StatusCode::kUnimplemented` AND
    (`is_idempotent || NeverLeftClient(status)`).

### B6. `ClientConfigurationManager` is the SOLE production writer of `Diverter` load and pool configuration

-   In production code, `Diverter::SetSessionLoad`,
    `SessionPoolImpl::UpdateConfig`, and `SessionCreationBudget::UpdateConfig`
    are driven **exclusively** by `ClientConfigurationManager` (while RPC
    handlers may only call `Diverter::TripUnimplementedBreaker()` on
    `kUnimplemented`).
-   `SessionPoolImpl::UpdateConfig` and `SessionCreationBudget::UpdateConfig`
    **MUST** apply server-provided `max_session_count` and
    `new_session_creation_budget` authoritatively without local
    `>= initial_pool_size_` or `<= 10` clamps.
-   Test-only overrides on `SessionsDataConnection` **SHOULD** be named
    `SetSessionLoadForTesting`.

### B7. Per-Session vs. Per-AFE vs. Per-Pool state ownership & O(1) lookup discipline

-   `SessionImpl` (L2) owns single-stream state (`state_`, `active_rpc_`,
    `peer_info_`, `refresh_config_`, `last_heartbeat_time_us_`). It holds an
    opaque `void* pool_handle_` back-pointer solely so `SessionPoolImpl` can
    execute O(1) lock-free RCU fast-path handle transitions without hash-map
    lookups.
-   Per-AFE latency EWMAs (`OkGatedPeakEwma`), `ref_count`, and
    `IdleSessionQueue` belong on the AFE `ConnectionGroup` / `AfeBucket` in L3,
    not scattered across O(N) session scans.
-   `SessionHandle` stores its owning `shard_index` so `GetShardForSession` is
    O(1).

### B8. Strict Lock Acquisition Hierarchy

To prevent ABBA deadlocks across multi-threaded callers and `CompletionQueue`
reactor callbacks, lock acquisitions **MUST** obey the following strict partial
order (outermost to innermost):

1.  `SessionsDataConnection::mu_` (never held across
    `ClientConfigurationManager::Start()` or `Client::Create()`)
2.  `Client::mutex_` (never held across `RetryingVRpc` destruction or
    `StartOperation`)
3.  `SessionPoolImpl::Shard::mutex` (never hold two `Shard::mutex` locks
    simultaneously; release `other_shard->mutex` before locking `shard.mutex`)
4.  `SessionList::mu_` (inner to `Shard::mutex`)
5.  `IdleSessionQueue::mu_` / `PoolSizer::mutex_` /
    `SessionCreationBudget::mutex_` / `SessionImpl::state_mutex_` (leaf mutexes
    — **never** acquire any other mutex or invoke user/listener callbacks while
    holding a leaf mutex).

### B9. Retry classification lives at the L2/L3 tag site and is consumed only by L4 `RetryingVRpc`

-   L2 `SessionImpl` and L3 `SessionPoolImpl` stamp `kWriteStatusPayloadUrl`
    (`kRejected`, `kUncommitted`, `kTransportFailure`, `kServerResult`).
-   L4 `RetryingVRpc` (`EvaluateRetryOracle`) is the sole component that decides
    retryability and backoff. Neither L2 nor L3 may run its own retry loop.

### B10. Spec compliance unit tests (`sessions_spec_test.cc`) MUST exercise live production classes

-   Every test in `sessions_spec_test.cc` **MUST** invoke the live production
    classes and methods (`SessionImpl`, `SessionPoolImpl`, `PoolSizer`,
    `OkGatedPeakEwma`, `Client`, `SessionsDataConnection`, `Diverter`) rather
    than disconnected test-only facade functions.

--------------------------------------------------------------------------------

## Part C — Sole-Owner Matrix

| Concern                    | Sole Owner in C++                          | Notes                              |
| :------------------------- | :----------------------------------------- | :--------------------------------- |
| Session lifecycle state    | `session::SessionImpl` (`state_` under     | Monotonic forward; server-close    |
: (`kConnecting`..`kClosed`) : `state_mutex_`)                            : wait encapsulated inside           :
:                            :                                            : `InitiateStreamClose`.             :
| First terminal disconnect  | `session::SessionImpl::disconnect_status_` | Write-once under `state_mutex_`.   |
: cause                      :                                            :                                    :
| Parsed `PeerInfo` (AFE ID, | `session::SessionImpl::peer_info_`         | Parsed once in                     |
: zone, subzone, transport)  :                                            : `OnReadInitialMetadataDone`.       :
| Single in-flight vRPC slot | `session::SessionImpl::active_rpc_`        | `IsCancelledAwaitingDrain(rpc_id)` |
: & post-write drain flag    :                                            : gates `FinalizeSessionCall`.       :
| Pool-wide monotonic        | `pool::SessionPoolImpl::AllocateRpcId()`   | Shared across all 16 `Client`      |
: `rpc_id`                   :                                            : shards.                            :
| Per-AFE `ref_count`,       | `pool::SessionList` / `ConnectionGroup`    | Guarded by `I1`–`I6`; EWMA updated |
: `IdleSessionQueue`,        : (`AfeBucket`)                              : only on `status.ok()`.             :
: `OkGatedPeakEwma`          :                                            :                                    :
| Desired pool capacity      | `pool::PoolSizer`                          | `effectivePending = ceil(pending / |
: formula                    : (`ComputeDesiredPoolCapacity`)             : newSessionQueueLength)`.           :
| Handshake concurrency gate | `pool::SessionCreationBudget` &            | Driven by                          |
: & 15-failure breaker       : `SessionPoolImpl`                          : `ClientConfigurationManager`.      :
| Stochastic session vs.     | `shim::Diverter`                           | `SetSessionLoad` by config         |
: classic split &            :                                            : manager;                           :
: `UNIMPLEMENTED` latch      :                                            : `TripUnimplementedBreaker` on      :
:                            :                                            : `kUnimplemented`.                  :
| Server-driven              | `client::ClientConfigurationManager`       | One per `instance_name` on         |
: `ClientConfiguration`      :                                            : `SessionsDataConnection`.          :
: polling                    :                                            :                                    :
| 4-state retry decision     | `middleware::RetryingVRpc`                 | Hard-capped at `kMaxAttempts = 3`. |
: (`EvaluateRetryOracle`)    : (`retry_oracle.h`)                         :                                    :
| Proto $\leftrightarrow$    | `SessionsDataConnection`                   | Sole boundary for `Row`,           |
: Public C++ SDK type        :                                            : `SingleRowMutation`, `Filter`.     :
: translation                :                                            :                                    :
| Per-attempt client-side    | `OperationContext` / `TableSchemaMetric`   | Populated on both classic and      |
: metrics                    : (`MetricsOperationContextFactory`)         : session paths.                     :
: (`attempt_latencies`,      :                                            :                                    :
: etc.)                      :                                            :                                    :
| Per-session OTel           | `telemetry::SessionTracer`                 | Bound to                           |
: histograms (`session/*`,   :                                            : `MetricsOperationContextFactory`'s :
: `transport_latencies`)     :                                            : `MeterProvider`.                   :

--------------------------------------------------------------------------------

## Part D — Spec Strictness Guidelines (`ALL MUSTs, and SHOULDs if practical`)

When evolving or reviewing the C++ Sessions implementation, apply this 3-tier
rubric:

1.  **Strict 100% Compliance Required (`ALL MUSTs`)**:
    -   **External Wire Protocol & Handshake**: Every gRPC header, protobuf
        field, enum value, and timeout visible to the AFE/server
        (`protocol_version = 1`, `FeatureFlags`, `SessionRefreshConfig` header
        override precedence, `CloseSessionRequest` with
        `CLOSE_SESSION_REASON_GOAWAY`, 1-indexed
        `VirtualRpcRequest.attempt_number`, URL-safe base64
        `"bigtable-peer-info"` decoding, and `30s` GOAWAY drain grace) **MUST**
        hold without exception.
    -   **4-State Retry & Idempotency Oracle**: `kRejected` (never retry),
        `kUncommitted` (retry within deadline), `kTransportFailure` (retry iff
        `is_idempotent`), `kServerResult` (retry iff
        `is_idempotent && has_server_retry_info`),
        `SafeIdempotentMutationPolicy` (`kAddToCell`/`kMergeToCell`
        non-idempotent), `is_idempotent || NeverLeftClient(status)` fallback
        gate, and `ConsecutiveSessionFailureThreshold = 15` circuit breaker
        **MUST** hold without exception.
    -   **Slot & Pool Memory/Concurrency Safety**: `IsCancelledAwaitingDrain`
        slot-drain guard, per-pool `thread_local pending_drains` isolation, and
        `PoolSizer` math
        (`effectivePending = ceil(pending / newSessionQueueLength)` + `OnGoAway`
        gate) **MUST** hold without exception.
    -   **GCM Client-Side Metrics Schema**: Metric names, `"ms"` units,
        `ViewRegistry` bucket boundaries, monitored resource labels, and
        `status.ok()` EWMA gating **MUST** hold so Google Cloud Monitoring (GCM)
        client-side metrics schemas remain consistent.
2.  **Adopt Practical `SHOULDs` Where They Improve Stability or Latency at
    Modest Cost**:
    -   Lock-free hot-path `MetricsCollector`, uniform `30s` `OpenSession`
        deadline & 5m stuck-closing watchdog, 2-tier AFE-first selection & O(1)
        shard lookup, `ABSL_GUARDED_BY` lock-hierarchy annotations, and clean
        non-test factory naming.
3.  **Preserve C++-Idiomatic Concurrency & API Choices (Documented in
    `docs/specs/*.md`)**:
    -   Preserve native C++ reactor and lock-free design choices: the 5-state
        `grpc::ClientBidiReactor` state machine, `Drain()` vs. `Close()`,
        synchronous `FinalizeSessionCall` before user callback, 16-shard
        lock-free RCU pool & 16 `Client` shards, `0-2ms` first-turnover jitter +
        exponential backoff on repeated transport failures, direct
        `DataConnection::ReadRow`/`AsyncReadRow` routing on sessions with
        streaming `ReadRows` on `classic_connection_`, wait-free
        `ClientDirectory`, and OpenTelemetry + `PoolSnapshot` observability
        without embedded HTTP endpoints.
