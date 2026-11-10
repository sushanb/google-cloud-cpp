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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_POOL_SIZER_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_POOL_SIZER_H

#include "google/cloud/version.h"
#include <google/bigtable/v2/session.pb.h>
#include <algorithm>
#include <mutex>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

/// Computes target session scaling deltas from active load and configured
/// headroom.
class PoolSizer {
 public:
  /// Initializes the sizer with minimum/maximum session bounds and headroom.
  PoolSizer(int min_sessions, int max_sessions, float headroom = 0.10F,
            int new_session_queue_length = 10)
      : min_sessions_(min_sessions),
        max_sessions_(max_sessions),
        headroom_(headroom),
        new_session_queue_length_((std::max)(1, new_session_queue_length)) {}

  /// Updates sizing bounds and headroom from server configuration payloads.
  void UpdateConfig(google::bigtable::v2::SessionClientConfiguration::
                        SessionPoolConfiguration const& config);

  /// Computes the number of additional sessions needed to satisfy active
  /// in-use and queued demand using `pool::ComputeDesiredPoolCapacity`.
  int GetScaleDelta(int ready_count, int connecting_count, int in_use_count,
                    int pending_count) const;

  /// Legacy 4-argument overload (with `avg_write_queue_size` ignored).
  int GetScaleDelta(int ready_count, int connecting_count, int active_calls,
                    float avg_write_queue_size) const;

  /// Returns the configured minimum session count.
  int min_sessions() const;

  /// Returns the configured maximum session count.
  int max_sessions() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return max_sessions_;
  }

  /// Returns the configured headroom ratio.
  float headroom() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return headroom_;
  }

  /// Returns the configured new session queue length divisor.
  int new_session_queue_length() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return new_session_queue_length_;
  }

 private:
  mutable std::mutex mutex_;
  int min_sessions_;
  int max_sessions_;
  float headroom_;
  int new_session_queue_length_;
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_POOL_SIZER_H
