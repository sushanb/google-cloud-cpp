// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SESSIONS_OPTIONS_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SESSIONS_OPTIONS_H

#include "google/cloud/options.h"
#include "google/cloud/version.h"
#include "google/bigtable/v2/feature_flags.pb.h"
#include "google/bigtable/v2/session.pb.h"
#include <grpcpp/client_context.h>
#include <chrono>
#include <functional>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN

/**
 * Option to configure the initial session pool size for Sessions.
 */
struct SessionsInitialPoolSizeOption {
  using Type = int;
};

/**
 * Option to configure the maximum queued calls in a Sessions pool.
 */
struct SessionsMaxQueuedCallsOption {
  using Type = int;
};

/**
 * Option to configure the keepalive timeout multiplier for Sessions.
 */
struct SessionsKeepaliveMultiplierOption {
  using Type = double;
};

/**
 * Option to inject a callback for customizing gRPC `ClientContext` metadata
 * on `OpenSession` streams.
 */
struct SessionsMetadataHookOption {
  using Type = std::function<void(::grpc::ClientContext&)>;
};

/**
 * Option to configure the number of sharded `CompletionQueue` event loops.
 */
struct SessionsNumEventLoopsOption {
  using Type = int;
};

/**
 * Option to configure the number of sharded session pool partitions.
 */
struct SessionsNumShardsOption {
  using Type = int;
};

/**
 * Option to configure the maximum number of sessions per pool. When unset, the
 * process-wide default (400, or `GOOGLE_CLOUD_BIGTABLE_SESSIONS_MAX_SESSIONS`)
 * applies.
 */
struct SessionsMaxSessionsOption {
  using Type = int;
};

/**
 * Option to let synchronous `ReadRow` / `Apply` callers poll briefly for the
 * response instead of sleeping for the whole call.
 *
 * Values above 10ms are clamped. Zero (the default) disables polling.
 */
struct SessionsSyncSpinBudgetOption {
  using Type = std::chrono::nanoseconds;
};

/**
 * Option to cap how many synchronous callers may poll concurrently when
 * `SessionsSyncSpinBudgetOption` is non-zero. Zero or negative uses
 * `max(1, std::thread::hardware_concurrency() / 4)`.
 */
struct SessionsSyncMaxSpinnersOption {
  using Type = int;
};

/**
 * Internal option carrying the resolved `EnableMetricsOption` boolean from
 * `SessionsDataConnection` (L6) into `SessionPoolImpl` (L3) without introducing
 * an upward header dependency on `google/cloud/bigtable/options.h`.
 */
struct SessionsClientSideMetricsEnabledOption {
  using Type = bool;
};

/**
 * Option to specify the permission when opening an AuthorizedView session
 * stream (`OpenAuthorizedViewRequest::Permission`).
 */
struct SessionsOpenAuthorizedViewPermissionOption {
  using Type = google::bigtable::v2::OpenAuthorizedViewRequest::Permission;
};

/**
 * Option to specify the permission when opening a MaterializedView session
 * stream (`OpenMaterializedViewRequest::Permission`).
 */
struct SessionsOpenMaterializedViewPermissionOption {
  using Type = google::bigtable::v2::OpenMaterializedViewRequest::Permission;
};

/**
 * Option to override the `FeatureFlags` advertised in the `bigtable-features`
 * metadata header on `OpenSession` streams.
 */
struct SessionsFeatureFlagsOption {
  using Type = google::bigtable::v2::FeatureFlags;
};

// Note: the public `experimental::EnableSessionsOption` lives in
// `google/cloud/bigtable/options.h`. This header must not include public
// Bigtable headers so that the L3 pool target stays free of upward
// dependencies.
using SessionsOptionList = OptionList<
    SessionsInitialPoolSizeOption, SessionsMaxQueuedCallsOption,
    SessionsKeepaliveMultiplierOption, SessionsMetadataHookOption,
    SessionsNumEventLoopsOption, SessionsNumShardsOption,
    SessionsMaxSessionsOption, SessionsSyncSpinBudgetOption,
    SessionsSyncMaxSpinnersOption, SessionsClientSideMetricsEnabledOption,
    SessionsOpenAuthorizedViewPermissionOption,
    SessionsOpenMaterializedViewPermissionOption, SessionsFeatureFlagsOption>;

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SESSIONS_OPTIONS_H
