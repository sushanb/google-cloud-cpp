# Client-Side Metrics — C++ Behavioral Specification

**Scope.** This document governs the **runtime behavior** of client-side
OpenTelemetry metrics emission across both classic (unary/streaming) and session
(vRPC) data paths in the Cloud Bigtable C++ SDK:

-   **Per-Operation & Per-Attempt Metrics Pipeline**:
    `google/cloud/bigtable/internal/operation_context_factory.cc`
    (`MetricsOperationContextFactory`),
    `google/cloud/bigtable/internal/table_schema_metrics.h`,
    `google/cloud/bigtable/internal/table_schema_metrics.cc`
    (`TableSchemaMetric`), `google/cloud/bigtable/data_connection.cc`, and
    `google/cloud/bigtable/internal/sessions/sessions_data_connection.cc`.
-   **Session-Tracer OpenTelemetry Histograms**:
    `google/cloud/bigtable/internal/sessions/telemetry/session_tracer.h`,
    `google/cloud/bigtable/internal/sessions/pool/session_pool_impl.cc`, and
    `google/cloud/bigtable/internal/sessions/pool/session_pool_lifecycle.cc`.

It specifies how `SessionsDataConnection` shares
`MetricsOperationContextFactory` and its Google Cloud Monitoring (GCM)
`MeterProvider`, how per-attempt routing/identity attributes and latencies are
stamped on session-routed RPCs, and how `SessionTracer` records session
lifecycle and transport overhead histograms. Any change to these files **MUST**
comply with the 3 invariants below.

**Sibling behavioral specs.**

-   [`SESSION_SPEC.md`](SESSION_SPEC.md) — Per-Session lifecycle, state machine,
    `PeerInfo` decoding (`#3`), and retry oracle (10 invariants).
-   [`SESSION_CLIENT_SPEC.md`](SESSION_CLIENT_SPEC.md) —
    `SessionsDataConnection` & `Client` topology, `ClientDirectory`,
    `ClientConfigurationManager`, and `OpenSessionRequest` handshake envelope (4
    invariants).
-   [`SESSION_POOL_SPEC.md`](SESSION_POOL_SPEC.md) — Sharded `SessionPoolImpl`
    topology, two-tier AFE `DynamicPicker`, `status.ok()`-gated `PeakEwma`
    (`#2`), and `PoolSizer` scaling (6 invariants).

**Component/boundary spec.**
[`SESSION_COMPONENT_SPEC.md`](SESSION_COMPONENT_SPEC.md) — 7-layer DAG,
sole-owner matrix (Part C), and specification compliance guidelines.

--------------------------------------------------------------------------------

### 1. Per-attempt routing & identity attributes (`cluster`, `zone`, `uuid`, and transport peer labels) MUST be populated on BOTH classic and session paths via shared `MetricsOperationContextFactory`

-   **Shared `MetricsOperationContextFactory` & GCM Exporter Wiring**:
    -   In `google-cloud-cpp`,
        `MetricsOperationContextFactory::InitializeProvider`
        (`google/cloud/bigtable/internal/operation_context_factory.cc`)
        constructs a private
        `opentelemetry::nostd::shared_ptr<opentelemetry::metrics::MeterProvider>
        provider_` configured with `otel_internal::MakeMonitoringExporter`
        (`monitoring.googleapis.com`), `PeriodicExportingMetricReader`,
        `ServiceTimeSeriesOption(true)`, and `MetricNameFormatterOption`
        prepending `"bigtable.googleapis.com/internal/client/"`.
    -   When `EnableMetricsOption` is enabled, `MakeDataConnection`
        (`google/cloud/bigtable/data_connection.cc`) **MUST** construct
        `MetricsOperationContextFactory` (including the 16-character
        alphanumeric `client_uid` generated via
        `google::cloud::internal::Sample(gen, 16,
        "abcdefghijklmnopqrstuvwxyz0123456789")`) and pass both the
        `OperationContextFactory` and its `MeterProvider` into
        `MakeSessionsDataConnection` and `SessionTracer`.
    -   `SessionTracer` **MUST NOT** call the unconfigured global
        `opentelemetry::metrics::Provider::GetMeterProvider()` (which defaults
        to `NoopMeterProvider` and silently drops all session metrics in GCM).
-   **Per-Operation & Per-Attempt `OperationContext` Callbacks on the Session
    Path**:
    -   Every session-routed operation (`ReadRow`, `AsyncReadRow`, `Apply`,
        `AsyncApply` in `SessionsDataConnection`) **MUST** create an
        `OperationContext` via
        `operation_context_factory_->ReadRow(table_name, app_profile_id)` or
        `MutateRow(table_name, app_profile_id)`
        (`MetricsOperationContextFactory::ReadRow` **MUST** set
        `streaming = "false"`, not `"true"`).
    -   Each vRPC attempt inside `RetryingVRpc` / `Client` **MUST** invoke
        `operation_context->PreCall(...)` before attempt start,
        `operation_context->PostCall(...)` on attempt completion, and
        `operation_context->OnDone(status)` when the operation finishes, so all
        standard `bigtable.googleapis.com/internal/client/*` metrics
        (`operation_latencies`, `attempt_latencies`, `attempt_latencies2`,
        `retry_count`, `application_blocking_latencies`, `throttling_latencies`,
        `connectivity_error_count`, `first_response_latencies`) are emitted on
        session-routed RPCs.
-   **Path-Specific Attribute Provenance**: | Metric Attribute / Resource Label
    | Classic (Unary / Streaming) Source | Session (vRPC) Source | | :--- | :---
    | :--- | | **`cluster`** (`cluster_id`) & **`zone`** (`zone_id`) |
    `google::bigtable::v2::ResponseParams` extracted from gRPC trailing/initial
    metadata (`x-goog-cbt-cookie-*`). | Typed
    `VirtualRpcResponse.cluster_info()` (`cluster_id()`, `zone()`) returned on
    the vRPC response frame. | | **`uuid`** (`client_uid`) | 16-char
    alphanumeric `client_uid` from `MetricsOperationContextFactory`. | Same
    16-char alphanumeric `client_uid` from the shared
    `MetricsOperationContextFactory`. | | **`transport_type`**,
    **`transport_region`**, **`transport_zone`**, **`transport_subzone`** |
    Per-attempt `bigtable-peer-info` header on unary/streaming
    `grpc::ClientContext`. | `SessionImpl::peer_info()` (`transport::PeerInfo`),
    parsed once from URL-safe base64 `"bigtable-peer-info"` at session open
    (`SESSION_SPEC.md #3`), including `application_frontend_zone` $\to$
    `transport_zone`. Fixed for all attempts on that session. |
-   **3-Prong `TableSchemaMetric::IsConnectivityError` Interlock**:
    -   Because `server_latencies` (`server-timing` header) is not present on
        session-routed vRPCs, `TableSchemaMetric::IsConnectivityError`
        (`internal/table_schema_metrics.cc`) **MUST** implement the 3-prong
        check: an attempt is a connectivity error **only if** it lacks
        `PeerInfo` (`!has_peer_info` / `transport_type` empty) **AND** lacks
        `server-timing` **AND** lacks cluster/zone location
        (`cluster.empty() || zone.empty()`). Presence of
        `SessionImpl::peer_info()` proves the session stream reached an AFE.

--------------------------------------------------------------------------------

### 2. Per-attempt latency metrics on the session path (`throttling_latencies`, `server_latencies` suppression, and `transport_latencies` positive-delta gate)

-   **`throttling_latencies` (`client_blocking_latencies`)**:
    -   **Definition**: `ConvertToMs(sent_at - attempt_start)` in milliseconds
        (`"ms"`), capturing client-side pool checkout, waiter queue delay, slot
        CAS, and protobuf serialization before the frame is handed to
        `SessionStream::Write`.
    -   **Session Source**: `SessionImpl::SendRequest` records
        `sent_at = std::chrono::steady_clock::now()` immediately before calling
        `stream_->Write(...)`. On attempt completion, if `sent_at` was stamped
        (`sent_at > attempt_start`), `throttling_latencies` records
        `sent_at - attempt_start` via `OperationContext` (`TableSchemaMetric`).
        If the attempt failed before `stream_->Write` (e.g., pool rejection or
        pre-write cancel), `sent_at` is unset and `throttling_latencies` is
        skipped (never hardcoded to `0.0`).
-   **`server_latencies` Suppression on Session Path**:
    -   `VirtualRpcResponse.stats().backend_latency()` is **not**
        wire-comparable to classic's `server-timing` header (session AFE + wire
        overhead is tracked separately via `transport_latencies`). Therefore,
        `server_latencies` **MUST NOT** be recorded on session-path attempts.
-   **`transport_latencies` Positive-Delta Gate Across All Completion Paths**:

    -   On vRPC completion across **all** pool completion paths (lock-free fast
        path, locked fallback path, and `DrainQueue` path in
        `pool/session_pool_impl.cc`), `transport_latencies`
        (`e2e_latency - backend_latency`) **MUST ONLY** be recorded when:

    ```cpp
    status.ok() && response.has_stats() && response.stats().has_backend_latency() &&
    backend_latency_ms > 0.0 && (e2e_latency_ms - backend_latency_ms) > 0.0
    ```

    -   If `has_backend_latency()` is `false`, `backend_latency_ms <= 0.0`, or
        clock/rounding skew yields `e2e_latency_ms <= backend_latency_ms`,
        `RecordTransportLatencyMs` **MUST** be skipped (never recording full
        `e2e_latency_ms` as transport overhead or clamping negative deltas to
        `0.0`).
-   **OK-Gated EWMA Updates**:

    -   Per `SESSION_POOL_SPEC.md #2`, neither `PeakEwmaTracker` nor
        `OkGatedPeakEwma::RecordOutcome` may be updated when `!status.ok()`.

--------------------------------------------------------------------------------

### 3. `SessionTracer` OpenTelemetry histograms (`session/open_latencies`, `session/durations`, `session/uptime`, `transport_latencies`)

-   **Instrument Names, `"ms"` Units, & `ViewRegistry` Bucket Boundaries**:

    -   `SessionTracer` (`telemetry/session_tracer.h`) registers 4 OpenTelemetry
        `DoubleHistogram` instruments on the `MeterProvider` shared from
        `MetricsOperationContextFactory`. Because `MetricNameFormatterOption`
        prepends `"bigtable.googleapis.com/internal/client/"`, instrument names
        passed to `CreateDoubleHistogram` **MUST** use the short suffix names
        (or match the formatter contract so exported GCM metric types are
        `bigtable.googleapis.com/internal/client/<short_name>` without
        double-prefixing):

    | Short Instrument Name        | Exported GCM Metric Type            | Unit       | Bucket Boundaries        | When Recorded                    |
    :                              :                                     :            : (`ViewRegistry`)         :                                  :
    | :--------------------------- | :---------------------------------- | :--------- | :----------------------- | :------------------------------- |
    | **`session/open_latencies`** | `.../client/session/open_latencies` | **`"ms"`** | `sessionLifetimeBounds`  | In `OnSessionReady` (handshake   |
    :                              :                                     :            : / OTel default           : success) **AND** in              :
    :                              :                                     :            :                          : `OnSessionClosed` when           :
    :                              :                                     :            :                          : `was_connecting == true`         :
    :                              :                                     :            :                          : (handshake failure).             :
    | **`session/durations`**      | `.../client/session/durations`      | **`"ms"`** | `sessionLifetimeBounds`  | Once at terminal                 |
    :                              :                                     :            : (`s` $\to$ `hrs` in ms)  : `OnSessionClosed`                :
    :                              :                                     :            :                          : (`msSince(session_start_time)`). :
    :                              :                                     :            :                          : **Never `"s"`**.                 :
    | **`session/uptime`**         | `.../client/session/uptime`         | **`"ms"`** | `sessionLifetimeBounds`  | Periodically sampled every       |
    :                              :                                     :            : (`s` $\to$ `hrs` in ms)  : **60s** across all active        :
    :                              :                                     :            :                          : (`kConnecting` / `kReady` /      :
    :                              :                                     :            :                          : `kDraining`) sessions in the     :
    :                              :                                     :            :                          : pool                             :
    :                              :                                     :            :                          : (`msSince(session_start_time)`). :
    :                              :                                     :            :                          : **Never `"s"`**.                 :
    | **`transport_latencies`**    | `.../client/transport_latencies`    | **`"ms"`** | `FineGrainLatencyBounds` | Per `status.ok()` vRPC meeting   |
    :                              :                                     :            : (`0.0` to `3.0ms` in     : the positive-delta gate (`#2`).  :
    :                              :                                     :            : `0.1ms` steps + coarse   :                                  :
    :                              :                                     :            : tail to `5000s`)         :                                  :

-   **Required Attribute Label Schema**:

    -   Every `SessionTracer` histogram `Record(value, attributes, context)`
        call **MUST** populate its required attributes (never empty `{}`):
    -   **`session/open_latencies`**: `transport_type`, `status` (`"OK"` or gRPC
        status code string), `session_type` (`"READ"` / `"WRITE"`),
        `afe_location` (`PeerInfo.application_frontend_subzone`, or `""` on
        pre-metadata failure), `session_name`.
    -   **`session/durations`**: `transport_type`, `status`, `session_type`,
        `afe_location`, `session_name`, `closing_reason` (`"GOAWAY"`,
        `"MISSED_HEARTBEAT"`, `"USER"`, `"ERROR"`), `vrpcs` (`"none"`,
        `"all_ok"`, `"all_error"`, `"some_ok"`), `ready` (`"true"` / `"false"`).
    -   **`session/uptime`**: `transport_type`, `session_type`, `ready`
        (`"true"` / `"false"`), `afe_location`, `session_name`.
    -   **`transport_latencies`**: `transport_type`, `session_type`,
        `afe_location`, `session_name`, `method` (`"Bigtable.ReadRow"` /
        `"Bigtable.MutateRow"`).
    -   **Bounded `session_name` Cardinality**: `session_name` **MUST** be the
        **pool-scoped** identifier (one per `{table, app_profile, permission}`
        pool), **NEVER** the per-stream `session_id` (which is unbounded and
        would cause a GCM metric cardinality explosion).

--------------------------------------------------------------------------------

**See [`SESSION_COMPONENT_SPEC.md`](SESSION_COMPONENT_SPEC.md)** Part C for the
complete metrics ownership matrix.
