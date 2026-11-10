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

#include "google/cloud/bigtable/internal/sessions/transport/peak_ewma_tracker.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

std::int64_t PeakEwmaTracker::ToNanos(TimePoint tp) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             tp.time_since_epoch())
      .count();
}

PeakEwmaTracker::PeakEwmaTracker() : PeakEwmaTracker(std::chrono::seconds(1)) {}

PeakEwmaTracker::PeakEwmaTracker(std::chrono::nanoseconds decay_time)
    : decay_time_ns_(decay_time.count()),
      last_update_time_ns_(ToNanos(Clock::now())) {}

void PeakEwmaTracker::SetDecayTime(std::chrono::nanoseconds decay_time) {
  decay_time_ns_.store(decay_time.count(), std::memory_order_relaxed);
}

void PeakEwmaTracker::OnRequestStart() {
  active_requests_.fetch_add(1, std::memory_order_relaxed);
}

void PeakEwmaTracker::OnRequestFailed() {
  int cur = active_requests_.load(std::memory_order_relaxed);
  while (cur > 0 && !active_requests_.compare_exchange_weak(
                        cur, cur - 1, std::memory_order_relaxed)) {
  }
}

void PeakEwmaTracker::OnRequestComplete(std::chrono::nanoseconds latency) {
  OnRequestComplete(latency, Clock::now());
}

void PeakEwmaTracker::OnRequestComplete(std::chrono::nanoseconds latency,
                                        TimePoint now) {
  OnRequestFailed();

  double const lat_ns = (std::max)(0.0, static_cast<double>(latency.count()));
  std::int64_t const now_ns = ToNanos(now);
  std::int64_t const decay_ns = decay_time_ns_.load(std::memory_order_relaxed);
  double cur_ewma_ns = ewma_ns_.load(std::memory_order_acquire);
  for (;;) {
    std::int64_t const last_ns =
        last_update_time_ns_.load(std::memory_order_acquire);
    double decayed_ns = cur_ewma_ns;
    if (cur_ewma_ns > 0.0 && now_ns > last_ns && decay_ns > 0) {
      double const exponent = -static_cast<double>(now_ns - last_ns) /
                              static_cast<double>(decay_ns);
      decayed_ns = cur_ewma_ns * std::exp(exponent);
    }
    double const next_ewma_ns = (std::max)(decayed_ns, lat_ns);
    if (ewma_ns_.compare_exchange_weak(cur_ewma_ns, next_ewma_ns,
                                       std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
      last_update_time_ns_.store(now_ns, std::memory_order_release);
      break;
    }
  }
}

double PeakEwmaTracker::GetDecayedEwmaNs(TimePoint now) const {
  double const cur_ewma_ns = ewma_ns_.load(std::memory_order_acquire);
  if (cur_ewma_ns <= 0.0) {
    return 0.0;
  }
  std::int64_t const last_ns =
      last_update_time_ns_.load(std::memory_order_acquire);
  std::int64_t const now_ns = ToNanos(now);
  std::int64_t const dt_ns = now_ns - last_ns;
  if (dt_ns <= 0) {
    return cur_ewma_ns;
  }
  std::int64_t const decay_ns = decay_time_ns_.load(std::memory_order_relaxed);
  if (decay_ns <= 0) {
    return cur_ewma_ns;
  }
  double const exponent =
      -static_cast<double>(dt_ns) / static_cast<double>(decay_ns);
  return cur_ewma_ns * std::exp(exponent);
}

std::chrono::nanoseconds PeakEwmaTracker::GetCurrentEwma() const {
  return GetCurrentEwma(Clock::now());
}

std::chrono::nanoseconds PeakEwmaTracker::GetCurrentEwma(TimePoint now) const {
  return std::chrono::nanoseconds(
      static_cast<std::int64_t>(std::llround(GetDecayedEwmaNs(now))));
}

double PeakEwmaTracker::GetScore() const { return GetScore(Clock::now()); }

double PeakEwmaTracker::GetScore(TimePoint now) const {
  double ewma_ns = GetDecayedEwmaNs(now);
  if (ewma_ns == 0.0) {
    ewma_ns = static_cast<double>(
        std::chrono::nanoseconds(std::chrono::milliseconds(1)).count());
  }
  int const active =
      (std::max)(0, active_requests_.load(std::memory_order_relaxed));
  return ewma_ns * (active + 1);
}

int PeakEwmaTracker::active_requests() const {
  return (std::max)(0, active_requests_.load(std::memory_order_relaxed));
}

void PeakEwmaTracker::Reset() {
  ewma_ns_.store(0.0, std::memory_order_release);
  last_update_time_ns_.store(ToNanos(Clock::now()), std::memory_order_release);
  active_requests_.store(0, std::memory_order_release);
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
