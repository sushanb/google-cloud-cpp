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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_SESSION_CREATION_BUDGET_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_SESSION_CREATION_BUDGET_H

#include "google/cloud/version.h"
#include <google/bigtable/v2/session.pb.h>
#include <google/protobuf/duration.pb.h>
#include <algorithm>
#include <chrono>
#include <deque>
#include <mutex>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

/// Rate-limits concurrent session handshakes using server-tuned capacity
/// budgets and cooldown penalties on failure.
class SessionCreationBudget {
 public:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;

  /// Initializes the controller with configured budget limits and failure
  /// penalties.
  SessionCreationBudget(int max_concurrent_requests,
                        std::chrono::nanoseconds penalty)
      : max_concurrent_requests_(max_concurrent_requests),
        penalty_duration_(penalty),
        active_creations_(0) {}

  /// Updates budget settings from server configuration payloads.
  void UpdateConfig(google::bigtable::v2::SessionClientConfiguration::
                        SessionPoolConfiguration const& config) {
    std::lock_guard<std::mutex> l(mutex_);
    if (config.new_session_creation_budget() > 0) {
      max_concurrent_requests_ =
          static_cast<int>(config.new_session_creation_budget());
    }
    if (config.has_new_session_creation_penalty()) {
      auto const penalty =
          std::chrono::seconds(
              config.new_session_creation_penalty().seconds()) +
          std::chrono::nanoseconds(
              config.new_session_creation_penalty().nanos());
      if (penalty.count() > 0) {
        penalty_duration_ = penalty;
      }
    }
  }

  /// Attempts to acquire a single session creation slot.
  ///
  /// Returns true if budget is available and no failure penalties block the
  /// slot.
  bool TryReserve() {
    std::lock_guard<std::mutex> l(mutex_);
    if (active_creations_ >= max_concurrent_requests_) {
      DrainCreationFailuresLocked();
    }
    if (active_creations_ >= max_concurrent_requests_) {
      return false;
    }
    active_creations_++;
    return true;
  }

  /// Reserves a slot unconditionally (used during startup pre-warming).
  void ForceReserve() {
    std::lock_guard<std::mutex> l(mutex_);
    active_creations_++;
  }

  /// Releases an active reservation slot upon successful stream opening.
  void OnSuccess() {
    std::lock_guard<std::mutex> l(mutex_);
    active_creations_ = (std::max)(0, active_creations_ - 1);
  }

  /// Records a handshake failure penalty token before releasing the slot.
  void OnFailure() {
    std::lock_guard<std::mutex> l(mutex_);
    DrainCreationFailuresLocked();
    TimePoint const exp = Clock::now() + penalty_duration_;
    if (static_cast<int>(delayed_creation_tokens_.size()) >=
        (std::max)(1, max_concurrent_requests_)) {
      delayed_creation_tokens_.back() = exp;
      active_creations_ =
          (std::min)(active_creations_, max_concurrent_requests_);
    } else {
      delayed_creation_tokens_.push_back(exp);
    }
  }

  /// Returns the earliest timestamp when a creation budget token will become
  /// available.
  TimePoint NextAvailableTime() const {
    std::lock_guard<std::mutex> l(mutex_);
    if (active_creations_ < max_concurrent_requests_ ||
        delayed_creation_tokens_.empty()) {
      return Clock::now();
    }
    return delayed_creation_tokens_.front();
  }

  int max_concurrent_requests() const {
    std::lock_guard<std::mutex> l(mutex_);
    return max_concurrent_requests_;
  }

 private:
  void DrainCreationFailuresLocked() {
    TimePoint const now = Clock::now();
    while (!delayed_creation_tokens_.empty()) {
      if (delayed_creation_tokens_.front() < now) {
        active_creations_ = (std::max)(0, active_creations_ - 1);
        delayed_creation_tokens_.pop_front();
      } else {
        break;
      }
    }
  }

  mutable std::mutex mutex_;
  int max_concurrent_requests_;
  std::chrono::nanoseconds penalty_duration_;
  int active_creations_;
  std::deque<TimePoint> delayed_creation_tokens_;
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_SESSION_CREATION_BUDGET_H
