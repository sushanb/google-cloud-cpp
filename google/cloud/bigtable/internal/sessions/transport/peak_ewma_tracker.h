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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TRANSPORT_PEAK_EWMA_TRACKER_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TRANSPORT_PEAK_EWMA_TRACKER_H

#include "google/cloud/version.h"
#include <atomic>
#include <chrono>
#include <cstdint>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

/// Lock-free tracker for exponentially weighted moving average (Peak-EWMA)
/// latency and in-flight request concurrency for load-aware session and peer
/// selection.
class PeakEwmaTracker {
 public:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;

  PeakEwmaTracker();
  explicit PeakEwmaTracker(std::chrono::nanoseconds decay_time);

  void SetDecayTime(std::chrono::nanoseconds decay_time);
  void Reset();

  /// Records that a new request has started.
  void OnRequestStart();

  /// Records that an in-flight request failed without updating the latency
  /// EWMA (so fast-failing peers do not skew latency downward).
  void OnRequestFailed();

  /// Records request completion and updates the latency EWMA.
  void OnRequestComplete(std::chrono::nanoseconds latency);
  void OnRequestComplete(std::chrono::nanoseconds latency, TimePoint now);

  /// Computes the current load score (`decayed_ewma * (active_requests + 1)`).
  double GetScore() const;
  double GetScore(TimePoint now) const;

  /// Returns the current time-decayed latency EWMA.
  std::chrono::nanoseconds GetCurrentEwma() const;
  std::chrono::nanoseconds GetCurrentEwma(TimePoint now) const;

  int active_requests() const;

 private:
  double GetDecayedEwmaNs(TimePoint now) const;
  static std::int64_t ToNanos(TimePoint tp);

  std::atomic<std::int64_t> decay_time_ns_;
  std::atomic<double> ewma_ns_{0.0};
  std::atomic<std::int64_t> last_update_time_ns_{0};
  std::atomic<int> active_requests_{0};
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TRANSPORT_PEAK_EWMA_TRACKER_H
