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

#include "google/cloud/bigtable/internal/sessions/client/sync_waiter.h"
#include "google/cloud/bigtable/internal/sessions/sessions_options.h"
#include <algorithm>
#include <chrono>
#include <thread>
#if defined(_MSC_VER)
#include <intrin.h>
#endif

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

constexpr std::int64_t kDownDivisor = 4;
constexpr std::int64_t kUpDivisor = 64;

void CpuRelax() {
#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
  _mm_pause();
#elif defined(_MSC_VER) && defined(_M_ARM64)
  __yield();
#elif (defined(__GNUC__) || defined(__clang__)) && \
    (defined(__x86_64__) || defined(__i386__))
  __builtin_ia32_pause();
#elif (defined(__GNUC__) || defined(__clang__)) && defined(__aarch64__)
  __asm__ __volatile__("yield" ::: "memory");
#else
  std::this_thread::yield();
#endif
}

int DefaultMaxSpinners() {
  auto const hw = static_cast<int>(std::thread::hardware_concurrency());
  return std::max(1, hw / 4);
}

bool WaitUntilOrForever(Latch& done, SyncWaiter::TimePoint deadline) {
  if (deadline == (SyncWaiter::TimePoint::max)()) {
    done.Wait();
    return true;
  }
  return done.WaitUntil(deadline);
}

}  // namespace

bool SyncSpinGate::TryAcquire(int limit) {
  int current = active_.load(std::memory_order_relaxed);
  while (current < limit) {
    if (active_.compare_exchange_weak(current, current + 1,
                                      std::memory_order_acq_rel,
                                      std::memory_order_relaxed)) {
      return true;
    }
  }
  return false;
}

void SyncSpinGate::Release() {
  active_.fetch_sub(1, std::memory_order_acq_rel);
}

SyncSpinGate& SyncSpinGate::Global() {
  // Never destroyed, so late callers during static destruction stay safe.
  static auto* const kGate = new SyncSpinGate;
  return *kGate;
}

SyncWaiter::Config SyncWaiter::ConfigFromOptions(Options const& options) {
  Config config;
  if (options.has<SessionsSyncSpinBudgetOption>()) {
    config.spin_budget = options.get<SessionsSyncSpinBudgetOption>();
  }
  if (options.has<SessionsSyncMaxSpinnersOption>()) {
    config.max_spinners = options.get<SessionsSyncMaxSpinnersOption>();
  }
  return config;
}

SyncWaiter::SyncWaiter() : SyncWaiter(Config{}) {}

SyncWaiter::SyncWaiter(Config config, SyncSpinGate* gate)
    : spin_budget_(std::clamp(config.spin_budget,
                              std::chrono::nanoseconds::zero(),
                              kMaxSpinBudget)),
      max_spinners_(config.max_spinners > 0 ? config.max_spinners
                                            : DefaultMaxSpinners()),
      gate_(gate != nullptr ? gate : &SyncSpinGate::Global()) {}

bool SyncWaiter::Wait(Latch& done, TimePoint start, TimePoint deadline) {
  if (!enabled() || done.HasBeenNotified()) {
    return WaitUntilOrForever(done, deadline);
  }

  // Step 1: park until shortly before the predicted completion time.
  auto const predicted = predicted_latency();
  if (predicted > std::chrono::nanoseconds::zero()) {
    auto const wake_at = start + std::chrono::duration_cast<Clock::duration>(
                                     predicted - spin_budget_ / 2);
    auto const now = Clock::now();
    if (wake_at > now) {
      auto park =
          std::chrono::duration_cast<std::chrono::nanoseconds>(wake_at - now);
      if (deadline != (TimePoint::max)()) {
        auto const rem = std::chrono::duration_cast<std::chrono::nanoseconds>(
            deadline - now);
        park = std::min(park, rem);
      }
      if (park > std::chrono::nanoseconds::zero() && done.WaitFor(park)) {
        early_.fetch_add(1, std::memory_order_relaxed);
        return true;
      }
    }
  }

  // Step 2: poll for up to `spin_budget_`, never past the deadline.
  auto const now = Clock::now();
  auto const remaining =
      (deadline == (TimePoint::max)())
          ? spin_budget_
          : std::chrono::duration_cast<std::chrono::nanoseconds>(deadline -
                                                                 now);
  if (remaining > std::chrono::nanoseconds::zero()) {
    if (gate_->TryAcquire(max_spinners_)) {
      if (spin_start_hook_) spin_start_hook_();
      auto const window = std::min(spin_budget_, remaining);
      auto const until = std::chrono::steady_clock::now() + window;
      bool hit = false;
      do {
        if (done.HasBeenNotified()) {
          hit = true;
          break;
        }
        CpuRelax();
      } while (std::chrono::steady_clock::now() < until);
      gate_->Release();
      if (hit) {
        spin_hits_.fetch_add(1, std::memory_order_relaxed);
        return true;
      }
      spin_misses_.fetch_add(1, std::memory_order_relaxed);
    } else {
      gate_full_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  // Step 3: park until the deadline.
  return WaitUntilOrForever(done, deadline);
}

void SyncWaiter::RecordLatency(std::chrono::nanoseconds latency) {
  if (!enabled()) return;
  auto sample = latency.count();
  if (sample <= 0) return;
  auto const current = predicted_ns_.load(std::memory_order_relaxed);
  auto next = sample;
  if (current > 0) {
    if (sample < current) {
      next = current - (current - sample) / kDownDivisor;
    } else {
      auto const budget_ns = spin_budget_.count();
      sample = std::min(sample, 2 * current + budget_ns);
      next = current + (sample - current) / kUpDivisor;
    }
  }
  predicted_ns_.store(next, std::memory_order_relaxed);
}

SyncWaiter::Stats SyncWaiter::stats() const {
  Stats s;
  s.early = early_.load(std::memory_order_relaxed);
  s.spin_hits = spin_hits_.load(std::memory_order_relaxed);
  s.spin_misses = spin_misses_.load(std::memory_order_relaxed);
  s.gate_full = gate_full_.load(std::memory_order_relaxed);
  return s;
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
