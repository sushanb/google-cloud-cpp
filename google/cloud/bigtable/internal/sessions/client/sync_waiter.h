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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_CLIENT_SYNC_WAITER_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_CLIENT_SYNC_WAITER_H

#include "google/cloud/bigtable/internal/sessions/latch.h"
#include "google/cloud/options.h"
#include "google/cloud/version.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

/// Layer 5 (Client): caps how many synchronous callers poll at the same time.
///
/// Every `Client` shares `Global()`, so the CPU spent polling is bounded per
/// process rather than per table.
class SyncSpinGate {
 public:
  SyncSpinGate() = default;
  SyncSpinGate(SyncSpinGate const&) = delete;
  SyncSpinGate& operator=(SyncSpinGate const&) = delete;

  /// Takes a slot if fewer than `limit` slots are in use.
  bool TryAcquire(int limit);

  /// Returns a slot taken by a successful `TryAcquire()`.
  void Release();

  /// Number of slots in use.
  int active() const { return active_.load(std::memory_order_acquire); }

  /// The gate shared by every `Client` in the process.
  static SyncSpinGate& Global();

 private:
  std::atomic<int> active_{0};
};

/// Layer 5 (Client): how a synchronous `ReadRow` / `MutateRow` caller waits for
/// the terminal callback of its operation.
///
/// Disabled (the default): park on the latch until the deadline.
///
/// Enabled (`spin_budget > 0`):
///   1. Park until `predicted - spin_budget / 2`, where `predicted` is a low
///      percentile of recent successful latencies (see `RecordLatency()`).
///      Skipped until the first sample arrives.
///   2. Poll the latch for up to `spin_budget`, if a `SyncSpinGate` slot is
///      free.
///   3. Park until the deadline.
class SyncWaiter {
 public:
  using Clock = std::chrono::system_clock;
  using TimePoint = Clock::time_point;

  struct Config {
    /// Maximum time to poll per call. Zero or negative disables polling.
    std::chrono::nanoseconds spin_budget = std::chrono::nanoseconds::zero();
    /// Maximum concurrent pollers, process-wide. Zero or negative means
    /// `max(1, std::thread::hardware_concurrency() / 4)`.
    int max_spinners = 0;
  };

  /// Upper bound applied to `Config::spin_budget`.
  static constexpr std::chrono::nanoseconds kMaxSpinBudget =
      std::chrono::microseconds(10000);

  /// Reads `SessionsSyncSpinBudgetOption` and `SessionsSyncMaxSpinnersOption`.
  static Config ConfigFromOptions(Options const& options);

  /// Disabled: `Wait()` only parks on the latch.
  SyncWaiter();
  /// A null `gate` means `SyncSpinGate::Global()`.
  explicit SyncWaiter(Config config, SyncSpinGate* gate = nullptr);

  SyncWaiter(SyncWaiter const&) = delete;
  SyncWaiter& operator=(SyncWaiter const&) = delete;

  bool enabled() const {
    return spin_budget_ > std::chrono::nanoseconds::zero();
  }
  std::chrono::nanoseconds spin_budget() const { return spin_budget_; }
  int max_spinners() const { return max_spinners_; }

  /// Waits until `done` is notified or `deadline` passes, and returns true if
  /// `done` was notified. `start` is when the operation was issued.
  bool Wait(Latch& done, TimePoint start, TimePoint deadline);

  /// Feeds the latency of one successful operation to the predictor. No-op
  /// when disabled.
  void RecordLatency(std::chrono::nanoseconds latency);

  /// Predicted completion latency. Zero until the first sample.
  std::chrono::nanoseconds predicted_latency() const {
    return std::chrono::nanoseconds(
        predicted_ns_.load(std::memory_order_relaxed));
  }

  struct Stats {
    std::uint64_t early = 0;        ///< Completed during the step 1 park.
    std::uint64_t spin_hits = 0;    ///< Completed while polling.
    std::uint64_t spin_misses = 0;  ///< The polling window elapsed first.
    std::uint64_t gate_full = 0;    ///< No polling slot was free.
  };
  Stats stats() const;

  /// Invoked after a polling slot is taken and before the first poll. Set it
  /// before any call to `Wait()`.
  void SetSpinStartHookForTesting(std::function<void()> hook) {
    spin_start_hook_ = std::move(hook);
  }

 private:
  std::chrono::nanoseconds spin_budget_;
  int max_spinners_;
  SyncSpinGate* gate_;
  std::atomic<std::int64_t> predicted_ns_{0};
  std::atomic<std::uint64_t> early_{0};
  std::atomic<std::uint64_t> spin_hits_{0};
  std::atomic<std::uint64_t> spin_misses_{0};
  std::atomic<std::uint64_t> gate_full_{0};
  std::function<void()> spin_start_hook_;
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_CLIENT_SYNC_WAITER_H
