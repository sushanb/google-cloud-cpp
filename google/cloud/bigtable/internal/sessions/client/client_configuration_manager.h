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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_CLIENT_CLIENT_CONFIGURATION_MANAGER_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_CLIENT_CLIENT_CONFIGURATION_MANAGER_H

#include "google/cloud/version.h"
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/bigtable/v2/session.pb.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace client {

/// Per-method traffic diversion ratios resolved from
/// `SessionClientConfiguration` (`session_diversion_configuration` or legacy
/// `session_load`).
struct DiversionLoads {
  double read_row = 1.0;
  double mutate_row = 1.0;
};

/// Resolves per-method session diversion loads from a server
/// `SessionClientConfiguration`.
DiversionLoads ResolveDiversionLoads(
    google::bigtable::v2::SessionClientConfiguration const& session_cfg);

/// Periodically polls `Bigtable.GetClientConfiguration` for an
/// `(instance_name, app_profile_id)` pair and delivers updated traffic-diverter
/// load fractions and session pool configurations.
class ClientConfigurationManager
    : public std::enable_shared_from_this<ClientConfigurationManager> {
 public:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;
  using ScheduleFn =
      std::function<void(std::chrono::nanoseconds, std::function<void()>)>;
  using SessionLoadCallback = std::function<void(double)>;
  using DiversionLoadsCallback = std::function<void(DiversionLoads const&)>;
  using PoolConfigCallback =
      std::function<void(google::bigtable::v2::SessionClientConfiguration::
                             SessionPoolConfiguration const&)>;
  using ContextSetupFn = std::function<void(grpc::ClientContext&)>;
  using FetchFn = std::function<grpc::Status(
      grpc::ClientContext&,
      google::bigtable::v2::GetClientConfigurationRequest const&,
      google::bigtable::v2::ClientConfiguration*)>;

  static std::shared_ptr<ClientConfigurationManager> Create(
      std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
      std::string instance_name, std::string app_profile_id,
      ScheduleFn scheduler, SessionLoadCallback on_session_load,
      PoolConfigCallback on_pool_config,
      DiversionLoadsCallback on_diversion_loads = nullptr,
      ContextSetupFn context_setup = nullptr);

  static std::shared_ptr<ClientConfigurationManager> CreateForTesting(
      FetchFn fetch_fn, std::string instance_name, std::string app_profile_id,
      ScheduleFn scheduler, SessionLoadCallback on_session_load,
      PoolConfigCallback on_pool_config,
      DiversionLoadsCallback on_diversion_loads = nullptr);

  void Start();

  /// Stops polling and blocks until any in-flight configuration callback has
  /// returned, so owners that captured raw pointers in the callbacks may be
  /// safely destroyed after `Stop()` returns. Must not be called from inside a
  /// callback.
  void Stop();

  /// Synchronously executes a single `GetClientConfiguration` RPC and applies
  /// any returned configuration updates. Exposed for deterministic unit
  /// testing.
  bool FetchAndApplyOnce();

  std::uint64_t fetch_success_count() const {
    return fetch_success_count_.load(std::memory_order_relaxed);
  }

  std::chrono::nanoseconds next_poll_interval() const {
    std::lock_guard<std::mutex> l(callback_mu_);
    return next_poll_interval_;
  }

 private:
  ClientConfigurationManager(
      std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
      FetchFn fetch_fn, std::string instance_name, std::string app_profile_id,
      ScheduleFn scheduler, SessionLoadCallback on_session_load,
      PoolConfigCallback on_pool_config,
      DiversionLoadsCallback on_diversion_loads, ContextSetupFn context_setup);

  void ScheduleNextPoll(std::chrono::nanoseconds delay);

  std::vector<google::bigtable::v2::Bigtable::Stub*> stubs_;
  FetchFn fetch_fn_;
  std::string instance_name_;
  std::string app_profile_id_;
  ScheduleFn scheduler_;
  SessionLoadCallback on_session_load_;
  PoolConfigCallback on_pool_config_;
  DiversionLoadsCallback on_diversion_loads_;
  ContextSetupFn context_setup_;
  std::atomic<bool> stopped_{false};
  std::atomic<bool> started_{false};
  std::mutex fetch_mu_;
  std::condition_variable fetch_cv_;
  int in_flight_fetches_ = 0;
  std::unordered_set<grpc::ClientContext*> active_contexts_;
  // Serializes callback delivery and configuration state against Stop().
  mutable std::mutex callback_mu_;
  std::uint64_t latest_applied_seq_ = 0;
  std::atomic<std::uint64_t> next_fetch_seq_{1};
  std::chrono::nanoseconds next_poll_interval_ = std::chrono::minutes(5);
  TimePoint config_valid_until_ = (TimePoint::max)();
  int max_rpc_retry_count_ = 3;
  std::atomic<std::size_t> next_stub_idx_{0};
  std::atomic<std::uint64_t> fetch_success_count_{0};
};

}  // namespace client
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_CLIENT_CLIENT_CONFIGURATION_MANAGER_H
