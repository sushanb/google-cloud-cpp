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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_MIDDLEWARE_RETRYING_VRPC_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_MIDDLEWARE_RETRYING_VRPC_H

#include "google/cloud/bigtable/internal/sessions/session/write_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/version.h"
#include <google/bigtable/v2/session.pb.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace middleware {

inline bool NeverLeftClient(google::cloud::Status const& status) {
  if (status.ok()) return false;
  auto payload =
      google::cloud::internal::GetPayload(status, kWriteStatusPayloadUrl);
  if (payload.has_value()) {
    return payload.value() == "0";
  }
  return false;
}

/// Executes outbound virtual RPCs with three-state retry evaluation, attempt
/// caps, deadline-fit checks, and jittered backoff.
class RetryingVRpc : public std::enable_shared_from_this<RetryingVRpc> {
 public:
  using GenericCallback = std::function<void(
      google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>)>;

  /// Factory function invoking a single virtual RPC attempt.
  using OperationFactory = std::function<void(GenericCallback)>;

  /// Schedules a callback to run after the specified delay.
  using ScheduleFn =
      std::function<void(std::chrono::nanoseconds, std::function<void()>)>;

  /// Per-attempt telemetry reported to `CallPolicy::on_attempt_complete`.
  struct AttemptTelemetry {
    google::cloud::Status status;
    std::string cluster_id;
    std::string zone_id;
    std::string transport_type;
    std::string transport_region;
    std::string transport_zone;
    std::string transport_subzone;
    bool has_peer_info = false;
    std::optional<std::chrono::nanoseconds> throttling_latency = std::nullopt;
    bool delivered_response = false;
  };

  /// Per-call retry policy supplied by callers. All fields are optional;
  /// defaults apply 3 attempts, jittered exponential backoff, and no overall
  /// deadline.
  struct CallPolicy {
    /// Absolute deadline for the whole operation (all attempts + backoff).
    std::chrono::system_clock::time_point deadline =
        (std::chrono::system_clock::time_point::max)();
    /// User retry policy hook. Called on each retryable failure; returning
    /// false stops retrying (e.g. a cloned `DataRetryPolicy::OnFailure`).
    std::function<bool(google::cloud::Status const&)> on_failure;
    /// User backoff hook returning the next (pre-jitter) delay (e.g. a cloned
    /// `BackoffPolicy::OnCompletion`). When unset, the built-in truncated
    /// exponential backoff is used.
    std::function<std::chrono::nanoseconds()> next_backoff;
    /// Optional hook invoked immediately before each vRPC attempt starts.
    std::function<void()> on_attempt_start;
    /// Optional hook invoked when each vRPC attempt completes (success or
    /// error), before retry evaluation.
    std::function<void(AttemptTelemetry const&)> on_attempt_complete;
  };

  struct Options {
    int max_attempts = 3;
    bool is_idempotent = true;
    CallPolicy policy = {};
  };

  /// Built-in backoff parameters (used when `CallPolicy::next_backoff` unset).
  static constexpr std::chrono::nanoseconds kInitialBackoff =
      std::chrono::milliseconds(10);
  static constexpr std::chrono::nanoseconds kMaxBackoff =
      std::chrono::seconds(1);
  /// Upper bound on the jittered fast re-route delay after a session turnover.
  static constexpr std::chrono::nanoseconds kMaxTurnoverDelay =
      std::chrono::milliseconds(2);

  /// Computes the delay before the next attempt. Exposed for testing.
  /// `attempt` is the 1-based number of the attempt that just failed.
  /// `unit_random` must be in [0, 1) and supplies the jitter.
  static std::chrono::nanoseconds ComputeBackoff(
      int attempt, bool session_turnover, std::chrono::nanoseconds policy_delay,
      double unit_random);

  /// Number of attempts started so far. Exposed for testing.
  int attempts_started() const;

  /// Creates a retrying interceptor pipeline without starting it.
  static std::shared_ptr<RetryingVRpc> Create(OperationFactory factory,
                                              GenericCallback user_callback,
                                              Options options,
                                              ScheduleFn scheduler);

  /// Creates and immediately starts a retrying interceptor pipeline.
  static std::shared_ptr<RetryingVRpc> Execute(OperationFactory factory,
                                               GenericCallback user_callback,
                                               Options options,
                                               ScheduleFn scheduler);

  ~RetryingVRpc();

  void StartAttempt();
  void Cancel();

 private:
  RetryingVRpc(OperationFactory factory, GenericCallback user_callback,
               Options options, ScheduleFn scheduler);

  void HandleAttemptResult(
      google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse> result);
  bool IsRetryable(google::cloud::Status const& status) const;
  void Fulfill(
      google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse> result);

  OperationFactory factory_;
  GenericCallback user_callback_;
  Options options_;
  ScheduleFn scheduler_;

  std::atomic<int> current_attempt_{0};
  std::atomic<bool> cancelled_{false};
  mutable std::mutex mu_;
  bool fulfilled_ = false;
};

}  // namespace middleware
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_MIDDLEWARE_RETRYING_VRPC_H
