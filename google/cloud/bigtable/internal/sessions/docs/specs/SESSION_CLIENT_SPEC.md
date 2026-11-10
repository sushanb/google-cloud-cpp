# SessionClient & SessionsDataConnection — C++ Behavioral Specification

**Scope.** This document governs the **runtime behavior** of the client and
data-connection layers in the Cloud Bigtable C++ SDK
(`google/cloud/bigtable/internal/sessions/`):

-   **Layer 5 (Session Client & Config Manager)**: `client.h`, `client.cc`,
    `client/session_client.h`, `client/client_configuration_manager.h`,
    `client/client_configuration_manager.cc`, `client/sync_waiter.h`,
    `client/sync_waiter.cc`.
-   **Layer 6 (DataConnection Shim & Directory)**: `sessions_data_connection.h`,
    `sessions_data_connection.cc`, `shim/client_directory.h`, `shim/diverter.h`,
    and factory wiring in `google/cloud/bigtable/data_connection.cc`.

It specifies the `SessionsDataConnection` ↔ `ClientDirectory` ↔ `Client` ↔
`SessionPoolImpl` topology, wait-free `ClientDirectory` lookup and intra-pool
idle session reaping, shared channel/executor pools, `GetClientConfiguration`
polling and propagation, and the `OpenSessionRequest` handshake envelope. Any
change to these files **MUST** comply with the 4 invariants below.

**Sibling behavioral specs.**

-   [`SESSION_SPEC.md`](SESSION_SPEC.md) — Per-Session lifecycle, state machine,
    slot drain, and retry oracle (10 invariants).
-   [`SESSION_POOL_SPEC.md`](SESSION_POOL_SPEC.md) — Sharded `SessionPoolImpl`
    topology, two-tier AFE `DynamicPicker`, `Diverter` & operation routability
    matrix, non-blocking observability, `PoolSizer` scaling, and `SessionList`
    per-handle state machine (6 invariants).
-   [`CLIENT_SIDE_METRICS_SPEC.md`](CLIENT_SIDE_METRICS_SPEC.md) — Per-attempt
    `OperationContext` attribute provenance and `SessionTracer` OpenTelemetry
    histograms (3 invariants).

**Component/boundary spec.**
[`SESSION_COMPONENT_SPEC.md`](SESSION_COMPONENT_SPEC.md) — 7-layer DAG,
sole-owner matrix, lock hierarchy, and specification compliance guidelines.

--------------------------------------------------------------------------------

### 1. One `SessionsDataConnection` owns one `ClientDirectory` and per-instance `ClientConfigurationManager`; session pools are lazily created per `{resource, app_profile, permission}`

-   **`DataConnection` Integration in `google-cloud-cpp`**:
    -   In `google-cloud-cpp`, `google::cloud::bigtable::Table` is a lightweight
        value-type handle wrapping `std::shared_ptr<bigtable::DataConnection>`
        (`google/cloud/bigtable/table.h`).
    -   When `EnableSessionsOption(true)` is set (or
        `GOOGLE_CLOUD_CPP_BIGTABLE_ENABLE_SESSIONS` is enabled),
        `MakeDataConnection` constructs a single `SessionsDataConnection`
        instance that serves as the unified mixed-mode connection across all
        `Table` handles sharing that connection.
    -   *Supported Resource Types*: `google::cloud::bigtable::DataConnection`
        currently exposes `Table` resources (`TableResource`), which open
        `OpenTable` (`SessionResourceType::kTable`) sessions.
        `OpenAuthorizedView` (`kAuthorizedView`) and `OpenMaterializedView`
        (`kMaterializedView`) pool-key encodings are defined in
        `client::MakePoolKey` (`client/session_client.h`) and reserved until
        `AuthorizedView` and `MaterializedView` data-plane types are added to
        `google-cloud-cpp`.
-   **Canonical Pool Deduplication Key**:
    -   `SessionsDataConnection` deduplicates session pools by
        `{resource_type, resource_name, app_profile_id, permission}` via
        `client::MakePoolKey`: | Resource Type | Read Pool Key | Write Pool Key
        | | :--- | :--- | :--- | | **Table** |
        `"table:<table_name>:<app_profile>:READ"` |
        `"table:<table_name>:<app_profile>:WRITE"` | | **AuthorizedView**
        *(reserved)* | `"av:<view_name>:<app_profile>:READ"` |
        `"av:<view_name>:<app_profile>:WRITE"` | | **MaterializedView**
        *(reserved, read-only)* | `"mv:<view_name>:<app_profile>:READ"` | *(none
        — writes rejected client-side)* |
-   **16 Lock-Sharded `Client` Handles Sharing 1 `SessionPoolImpl`**:
    -   For each opened `{table, app_profile, permission}` entry in
        `shim::ClientDirectory`, `SessionsDataConnection::GetClient` creates
        **one** `SessionPoolImpl` (`first_client->pool()`) and wraps it in
        `kNumClientShards = 16` `Client` handles (`Client::CreateWithPool`) so
        concurrent caller threads tracking `active_retry_loops_` for
        cancellation never contend on a single `Client::mutex_`.
    -   **Pool-Wide Monotonic `rpc_id`**: `Client` shards **MUST NOT** maintain
        per-shard `next_rpc_id_` counters. Every `Client::StartOperation`
        allocates its `rpc_id` from the shared pool via
        `pool_->AllocateRpcId()`, guaranteeing unique `rpc_id`s across all 16
        shards.
    -   **1-Indexed Wire `attempt_number` & Default Deadline**:
        `Client::StartOperation` **MUST** initialize `attempt_counter` to `1` so
        `VirtualRpcRequest.attempt_number` is 1-indexed (`1, 2, 3`) on the wire.
        When the caller supplies an unconstrained `grpc::ClientContext` deadline
        (`InfiniteFuture()`), `CalculateVrpcDeadline` applies a default `60s`
        per-operation deadline.
    -   **All-Shard Teardown**: `~SessionsDataConnection()` and `Close()`
        **MUST** iterate and close all 16 `Client` shards in every
        `ClientDirectory` entry, never just shard `0`.
-   **Two-Layer Lazy Pool Creation**:
    -   **Lazy per-direction pool**: C++ creates the read pool only on the first
        `ReadRow`/`AsyncReadRow` (or `PreWarm`) and creates the write pool only
        on the first `Apply`/`AsyncApply` (or `PreWarm`). Read-only workloads
        never allocate a write pool.
    -   **Failed opens are NOT cached**: If `Client::Create` or pool
        initialization returns `nullptr`, `SessionsDataConnection::GetClient`
        **MUST NOT** insert the failed entry into `ClientDirectory`, allowing
        subsequent calls to retry cleanly.
-   **Wait-Free `shim::ClientDirectory` & Intra-Pool Idle Session Reaping**:
    -   Because `google::cloud::bigtable::Table` is a copyable value handle
        without an explicit `Close()` method, `shim::ClientDirectory`
        (`shim/client_directory.h`) uses an insert-only, fixed-capacity
        (`kCapacity = 256`) wait-free open-addressing hash table
        (`std::atomic<Entry const*>` slots holding
        `std::array<std::shared_ptr<Client>, 16>`):
    -   **Wait-Free Hot-Path Lookup (`FindRef`)**: Per-RPC lookup executes `0`
        mutex locks, `0` `shared_ptr` atomic reference-count RMWs, and `0`
        cache-line invalidations.
    -   **Lifetime & Idle Stream Reaping**: Directory entries remain valid for
        the lifetime of `SessionsDataConnection` (eliminating stale-handle
        races), while idle streams *inside* each `SessionPoolImpl` scale down to
        `MinSessions` via `PoolSizer` and `ReapIdleSessions()`.
    -   **Bounded Capacity Fallback**: Applications exceeding `256` distinct
        `{table, app_profile, permission}` tuples on a single `DataConnection`
        receive `nullptr` from `ClientDirectory` and fall back transparently to
        `classic_connection_`.

--------------------------------------------------------------------------------

### 2. All Session Pools on a `SessionsDataConnection` share one gRPC Channel Pool and `PoolExecutors`

-   **Shared Session Channel Pool & Executor Loops
    (`SESSION_CLIENT_SPEC.md #2`)**:
    -   `SessionsDataConnection` initializes a shared pool of gRPC channels
        (sized by `GrpcNumChannelsOption`, default `4`) and a shared
        `PoolExecutors` instance (`SessionsNumEventLoopsOption` dedicated
        `CompletionQueue` threads + `LightweightThreadPool` offload workers).
    -   Every `SessionPoolImpl` and `ClientConfigurationManager` owned by that
        `SessionsDataConnection` dials through the shared session channel pool
        and schedules timers/callbacks on the shared `PoolExecutors`.
    -   The session channel pool is **separate from `classic_connection_`'s
        channel pool**, ensuring that `SessionLoad = 0.0` or classic streaming
        `ReadRows` traffic never starves session bidi streams or
        `GetClientConfiguration` polls.

--------------------------------------------------------------------------------

### 3. `GetClientConfiguration` (`ClientConfigurationManager`) is the authoritative source for `Diverter` load and `SessionPoolConfiguration`

-   **Per-Instance Manager & Eager Construction-Time Startup**:
    -   `SessionsDataConnection` keys `ClientConfigurationManager` instances by
        `instance_name` (`projects/<p>/instances/<i>`).
    -   When `MakeDataConnection(instances, options)` is called with known
        `instances`, it **MUST** forward `instances` into
        `MakeSessionsDataConnection` so `EnsureConfigManager(instance_name)`
        starts polling at connection construction time rather than waiting for
        the first data-plane RPC. If `instances` is empty at construction,
        `EnsureConfigManager` starts on the first `ReadRow`/`Apply`/`PreWarm`
        call (releasing `SessionsDataConnection::mu_` before calling
        `manager_to_start->Start()` to prevent lock-order inversion).
-   **Authoritative Server Knobs**:
    -   `ClientConfigurationManager` (`client/client_configuration_manager.cc`)
        is the **sole production writer** (`SESSION_COMPONENT_SPEC.md §B6`) of:
        | Proto Field (`ClientConfiguration`) | Target Component & Effect |
        | :--- | :--- | | `session_configuration.session_load` (`0.0`–`1.0`) |
        `Diverter::SetSessionLoad(load)` (skipped if
        `TripUnimplementedBreaker()` is latched or value is `NaN`). | |
        `session_configuration.session_pool_configuration.headroom` |
        `PoolSizer` target headroom fraction (default `0.10`). | |
        `session_configuration.session_pool_configuration.min_session_count` /
        `max_session_count` | `SessionPoolImpl::UpdateConfig` & `PoolSizer`
        bounds. **MUST NOT** be locally clamped above `initial_pool_size_`. | |
        `session_configuration.session_pool_configuration.new_session_creation_budget`
        / `new_session_creation_penalty` | `SessionCreationBudget::UpdateConfig`
        (default budget `50`, penalty `2s`). **MUST NOT** be locally clamped to
        `<= 10`. | |
        `session_configuration.session_pool_configuration.consecutive_session_failure_threshold`
        | `SessionPoolImpl` consecutive open-failure circuit breaker (default
        `15`). | |
        `session_configuration.session_pool_configuration.new_session_queue_length`
        | `PoolSizer` `NewSessionQueueLength` divisor
        (`effectivePending = ceil(pending / newSessionQueueLength)`, default
        `10`). | |
        `session_configuration.session_pool_configuration.load_balancing_options`
        | `DynamicPicker` strategy (`LEAST_IN_FLIGHT`, `RANDOM`, `PEAK_EWMA`,
        `WEIGHTED_ROUND_ROBIN`) and `random_subset_size` (default `K=2`). | |
        `polling_configuration.polling_interval` / `validity_duration` /
        `max_rpc_retry_count` / `stop_polling` | Poller cadence (**clamped to
        `>= 1 minute`**), config lease TTL, per-poll retry cap, and graceful
        polling stop. |
-   **Monotonic `fetch_seq`, Seeding New Pools, & Lease Expiry**:
    -   Every `FetchConfigAsync` increments a monotonic `fetch_seq` and ignores
        out-of-order completions where `fetch_seq < latest_applied_seq_`.
    -   `SessionsDataConnection` caches the latest `SessionPoolConfiguration`
        per instance in `instance_pool_configs_` under `mu_` and seeds any newly
        opened `Client` pool via
        `first_client->UpdatePoolConfig(*it_cfg->second)` before inserting
        shards into `ClientDirectory`.
    -   `ClientConfigurationManager::Stop()` sets `stopped_ = true` before
        cancelling timers, ensuring no listener callbacks fire after `Stop()`
        returns.
    -   If all polls fail past `validity_duration`, `ClientConfigurationManager`
        expires the cached config (`config_expired_ = true`) and reverts
        `Diverter` to its bootstrap default (`session_load = 1.0`).

--------------------------------------------------------------------------------

### 4. `OpenSessionRequest` is a resource-agnostic transport envelope; `Payload` is pre-serialized once per pool; `SessionRefreshConfig` overrides handshake and metadata

-   **Wire Handshake Envelope**:
    -   Every `OpenTable` (and reserved `OpenAuthorizedView` /
        `OpenMaterializedView`) bidi stream sends
        `google::bigtable::v2::OpenSessionRequest`:
    -   `protocol_version`: **MUST** be set to `1`
        (`open_request.set_protocol_version(1)`).
    -   `flags` (`FeatureFlags` header `bigtable-features`):
        -   `sessions_compatible = true`.
        -   `sessions_required`: `false` when mixed-mode `classic_connection_`
            fallback is active; `true` only in session-only standalone/test
            mode.
        -   `reverse_scans = true`, `last_scanned_row_responses = true`,
            `client_side_metrics_enabled = <EnableMetricsOption>`,
            `traffic_director_enabled`, `direct_access_requested`.
    -   `payload`: Serialized inner request proto (`OpenTableRequest` with
        `table_name`, `app_profile_id`, and `permission` in
        `{PERMISSION_READ, PERMISSION_WRITE}`).
-   **Pool-Level Payload Pre-Serialization**:
    -   `OpenTableRequest.SerializeToString(&payload_bytes)` **MUST** be
        executed once at pool construction time (checking its `bool` return
        value and failing the pool open if serialization fails) and reused
        across all sessions spawned by that pool unless overridden by
        `SessionRefreshConfig`.
-   **Routing Metadata & `SessionRefreshConfig` Precedence**:
    -   Default routing metadata attaches `x-goog-request-params:
        open_session.payload.table_name=<url_encoded_table_name>&app_profile_id=<url_encoded_profile>`.
    -   When the server sends a `SessionRefreshConfig` frame, `SessionPoolImpl`
        caches it for subsequent sessions. In
        `SessionStream::StartOpenSerialized`:
    *   If `refresh_config_->has_optimized_open_request()`, its
        `optimized_open_request()` replaces the default `OpenSessionRequest`.
    *   All `refresh_config_->metadata()` key-value pairs are added to
        `context_.AddMetadata(md.key(), md.value())`.
    *   **No Duplicate `x-goog-request-params`**: If
        `refresh_config_->metadata()` already contains
        `"x-goog-request-params"`, `StartOpenSerialized` **MUST NOT** append a
        second default `"x-goog-request-params"` header.

--------------------------------------------------------------------------------

**See [`SESSION_COMPONENT_SPEC.md`](SESSION_COMPONENT_SPEC.md)** for the 7-layer
dependency DAG, sole-owner matrix, and lock hierarchy rules.
