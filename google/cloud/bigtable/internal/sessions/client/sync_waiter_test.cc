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
#include "google/cloud/options.h"
#include <gmock/gmock.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

SyncWaiter::Config MakeConfig(std::chrono::nanoseconds budget,
                              int max_spinners = 4) {
  SyncWaiter::Config config;
  config.spin_budget = budget;
  config.max_spinners = max_spinners;
  return config;
}

int ExpectedDefaultMaxSpinners() {
  return std::max(1, static_cast<int>(std::thread::hardware_concurrency()) / 4);
}

TEST(SyncSpinGateTest, EnforcesLimit) {
  SyncSpinGate gate;
  EXPECT_FALSE(gate.TryAcquire(0));
  EXPECT_TRUE(gate.TryAcquire(2));
  EXPECT_TRUE(gate.TryAcquire(2));
  EXPECT_FALSE(gate.TryAcquire(2));
  EXPECT_EQ(gate.active(), 2);
  gate.Release();
  EXPECT_EQ(gate.active(), 1);
  EXPECT_TRUE(gate.TryAcquire(2));
  gate.Release();
  gate.Release();
  EXPECT_EQ(gate.active(), 0);
}

TEST(SyncSpinGateTest, GlobalIsASingleton) {
  EXPECT_EQ(&SyncSpinGate::Global(), &SyncSpinGate::Global());
}

TEST(SyncWaiterTest, DisabledByDefault) {
  SyncWaiter waiter;
  EXPECT_FALSE(waiter.enabled());
  EXPECT_EQ(waiter.spin_budget(), std::chrono::microseconds(0));

  Latch notified;
  notified.Notify();
  EXPECT_TRUE(waiter.Wait(notified, SyncWaiter::Clock::now(),
                          SyncWaiter::Clock::now() + std::chrono::seconds(5)));

  Latch never;
  EXPECT_FALSE(
      waiter.Wait(never, SyncWaiter::Clock::now(),
                  SyncWaiter::Clock::now() + std::chrono::milliseconds(10)));

  waiter.RecordLatency(std::chrono::milliseconds(1));
  EXPECT_EQ(waiter.predicted_latency(), std::chrono::nanoseconds(0));

  auto const stats = waiter.stats();
  EXPECT_EQ(stats.early, 0u);
  EXPECT_EQ(stats.spin_hits, 0u);
  EXPECT_EQ(stats.spin_misses, 0u);
  EXPECT_EQ(stats.gate_full, 0u);
}

TEST(SyncWaiterTest, ConfigIsClampedAndDefaulted) {
  SyncWaiter too_long(MakeConfig(std::chrono::microseconds(50000), 3));
  EXPECT_TRUE(too_long.enabled());
  EXPECT_EQ(too_long.spin_budget(), SyncWaiter::kMaxSpinBudget);
  EXPECT_EQ(too_long.max_spinners(), 3);

  SyncWaiter negative(MakeConfig(std::chrono::microseconds(-5), 0));
  EXPECT_FALSE(negative.enabled());
  EXPECT_EQ(negative.spin_budget(), std::chrono::microseconds(0));
  EXPECT_EQ(negative.max_spinners(), ExpectedDefaultMaxSpinners());

  SyncWaiter negative_spinners(MakeConfig(std::chrono::microseconds(100), -7));
  EXPECT_EQ(negative_spinners.max_spinners(), ExpectedDefaultMaxSpinners());
}

TEST(SyncWaiterTest, ConfigFromOptions) {
  auto const defaults = SyncWaiter::ConfigFromOptions(Options{});
  EXPECT_EQ(defaults.spin_budget, std::chrono::microseconds(0));
  EXPECT_EQ(defaults.max_spinners, 0);

  auto const config = SyncWaiter::ConfigFromOptions(
      Options{}
          .set<SessionsSyncSpinBudgetOption>(std::chrono::microseconds(250))
          .set<SessionsSyncMaxSpinnersOption>(3));
  EXPECT_EQ(config.spin_budget, std::chrono::microseconds(250));
  EXPECT_EQ(config.max_spinners, 3);
}

TEST(SyncWaiterTest, AlreadyNotifiedReturnsWithoutPolling) {
  SyncSpinGate gate;
  SyncWaiter waiter(MakeConfig(std::chrono::microseconds(1000)), &gate);
  Latch done;
  done.Notify();
  EXPECT_TRUE(waiter.Wait(done, SyncWaiter::Clock::now(),
                          SyncWaiter::Clock::now() + std::chrono::seconds(5)));
  auto const stats = waiter.stats();
  EXPECT_EQ(stats.early + stats.spin_hits + stats.spin_misses + stats.gate_full,
            0u);
}

TEST(SyncWaiterTest, CompletionWhilePollingIsAHit) {
  SyncSpinGate gate;
  SyncWaiter waiter(MakeConfig(SyncWaiter::kMaxSpinBudget), &gate);
  Latch done;
  int active_during_poll = -1;
  waiter.SetSpinStartHookForTesting([&] {
    active_during_poll = gate.active();
    done.Notify();
  });
  EXPECT_TRUE(waiter.Wait(done, SyncWaiter::Clock::now(),
                          SyncWaiter::Clock::now() + std::chrono::seconds(5)));
  EXPECT_EQ(active_during_poll, 1);
  EXPECT_EQ(gate.active(), 0);
  auto const stats = waiter.stats();
  EXPECT_EQ(stats.spin_hits, 1u);
  EXPECT_EQ(stats.spin_misses, 0u);
  EXPECT_EQ(stats.early, 0u);
}

TEST(SyncWaiterTest, CompletionAfterPollingIsAMiss) {
  SyncSpinGate gate;
  SyncWaiter waiter(MakeConfig(std::chrono::microseconds(1000)), &gate);
  Latch done;
  std::atomic<bool> polling{false};
  waiter.SetSpinStartHookForTesting([&] { polling.store(true); });

  std::thread notifier([&] {
    while (!polling.load()) std::this_thread::yield();
    while (gate.active() != 0) std::this_thread::yield();
    done.Notify();
  });
  EXPECT_TRUE(waiter.Wait(done, SyncWaiter::Clock::now(),
                          SyncWaiter::Clock::now() + std::chrono::seconds(30)));
  notifier.join();
  auto const stats = waiter.stats();
  EXPECT_EQ(stats.spin_hits, 0u);
  EXPECT_EQ(stats.spin_misses, 1u);
  EXPECT_EQ(gate.active(), 0);
}

TEST(SyncWaiterTest, ReturnsFalseAtDeadline) {
  SyncSpinGate gate;
  SyncWaiter waiter(MakeConfig(std::chrono::microseconds(1000)), &gate);
  Latch never;
  auto const start = SyncWaiter::Clock::now();
  EXPECT_FALSE(waiter.Wait(never, SyncWaiter::Clock::now(),
                           start + std::chrono::milliseconds(20)));
  EXPECT_GE(SyncWaiter::Clock::now() - start, std::chrono::milliseconds(19));
  EXPECT_EQ(waiter.stats().spin_misses, 1u);
  EXPECT_EQ(gate.active(), 0);
}

TEST(SyncWaiterTest, PastDeadlineDoesNotPoll) {
  SyncSpinGate gate;
  SyncWaiter waiter(MakeConfig(std::chrono::microseconds(1000)), &gate);
  bool polled = false;
  waiter.SetSpinStartHookForTesting([&] { polled = true; });
  Latch never;
  EXPECT_FALSE(
      waiter.Wait(never, SyncWaiter::Clock::now(),
                  SyncWaiter::Clock::now() - std::chrono::milliseconds(1)));
  EXPECT_FALSE(polled);
  auto const stats = waiter.stats();
  EXPECT_EQ(stats.early + stats.spin_hits + stats.spin_misses + stats.gate_full,
            0u);
}

TEST(SyncWaiterTest, FullGateSkipsPolling) {
  SyncSpinGate gate;
  ASSERT_TRUE(gate.TryAcquire(1));
  SyncWaiter waiter(MakeConfig(std::chrono::microseconds(1000), 1), &gate);
  bool polled = false;
  waiter.SetSpinStartHookForTesting([&] { polled = true; });
  Latch never;
  EXPECT_FALSE(
      waiter.Wait(never, SyncWaiter::Clock::now(),
                  SyncWaiter::Clock::now() + std::chrono::milliseconds(10)));
  EXPECT_FALSE(polled);
  EXPECT_EQ(waiter.stats().gate_full, 1u);
  EXPECT_EQ(waiter.stats().spin_misses, 0u);
  EXPECT_EQ(gate.active(), 1);
  gate.Release();
}

TEST(SyncWaiterTest, InfiniteDeadlineIsSupported) {
  SyncSpinGate gate;
  SyncWaiter waiter(MakeConfig(std::chrono::microseconds(1000)), &gate);
  Latch done;
  waiter.SetSpinStartHookForTesting([&] { done.Notify(); });
  EXPECT_TRUE(waiter.Wait(done, SyncWaiter::Clock::now(),
                          (SyncWaiter::TimePoint::max)()));
  EXPECT_EQ(waiter.stats().spin_hits, 1u);
}

TEST(SyncWaiterTest, PredictorTracksALowPercentile) {
  SyncWaiter waiter(MakeConfig(std::chrono::microseconds(100)));
  EXPECT_EQ(waiter.predicted_latency(), std::chrono::nanoseconds(0));

  waiter.RecordLatency(std::chrono::microseconds(1000));
  EXPECT_EQ(waiter.predicted_latency(), std::chrono::nanoseconds(1000000));

  waiter.RecordLatency(std::chrono::microseconds(600));
  EXPECT_EQ(waiter.predicted_latency(), std::chrono::nanoseconds(900000));

  waiter.RecordLatency(std::chrono::microseconds(1000));
  EXPECT_EQ(waiter.predicted_latency(), std::chrono::nanoseconds(901562));

  waiter.RecordLatency(std::chrono::seconds(1));
  EXPECT_EQ(waiter.predicted_latency(), std::chrono::nanoseconds(917211));

  waiter.RecordLatency(std::chrono::nanoseconds(0));
  waiter.RecordLatency(std::chrono::nanoseconds(-5));
  EXPECT_EQ(waiter.predicted_latency(), std::chrono::nanoseconds(917211));
}

TEST(SyncWaiterTest, EarlyCompletionWakesTheParkedCaller) {
  SyncSpinGate gate;
  SyncWaiter waiter(MakeConfig(std::chrono::microseconds(1000)), &gate);
  waiter.RecordLatency(std::chrono::milliseconds(500));
  bool polled = false;
  waiter.SetSpinStartHookForTesting([&] { polled = true; });

  Latch done;
  auto const start = SyncWaiter::Clock::now();
  std::thread notifier([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    done.Notify();
  });
  EXPECT_TRUE(waiter.Wait(done, SyncWaiter::Clock::now(),
                          SyncWaiter::Clock::now() + std::chrono::seconds(30)));
  auto const elapsed = SyncWaiter::Clock::now() - start;
  notifier.join();
  EXPECT_LT(elapsed, std::chrono::milliseconds(400));
  EXPECT_FALSE(polled);
  EXPECT_EQ(waiter.stats().early, 1u);
}

TEST(SyncWaiterTest, ParkNeverOutlivesTheDeadline) {
  SyncSpinGate gate;
  SyncWaiter waiter(MakeConfig(std::chrono::microseconds(1000)), &gate);
  waiter.RecordLatency(std::chrono::seconds(5));
  Latch never;
  auto const start = SyncWaiter::Clock::now();
  EXPECT_FALSE(waiter.Wait(never, SyncWaiter::Clock::now(),
                           start + std::chrono::milliseconds(50)));
  auto const elapsed = SyncWaiter::Clock::now() - start;
  EXPECT_GE(elapsed, std::chrono::milliseconds(49));
  EXPECT_LT(elapsed, std::chrono::seconds(2));
  EXPECT_EQ(waiter.stats().early, 0u);
}

TEST(SyncWaiterTest, ConcurrentCallersRespectTheSpinnerCap) {
  constexpr int kThreads = 8;
  constexpr int kIterations = 200;
  constexpr int kCap = 2;
  SyncSpinGate gate;
  SyncWaiter waiter(MakeConfig(std::chrono::microseconds(200), kCap), &gate);

  thread_local Latch* notify_in_hook = nullptr;
  std::atomic<int> max_active{0};
  waiter.SetSpinStartHookForTesting([&] {
    int const active = gate.active();
    int seen = max_active.load();
    while (active > seen && !max_active.compare_exchange_weak(seen, active)) {
    }
    if (notify_in_hook != nullptr) notify_in_hook->Notify();
  });

  std::atomic<int> notified_results{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t != kThreads; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i != kIterations; ++i) {
        Latch done;
        notify_in_hook = ((i + t) % 2 == 0) ? &done : nullptr;
        if (waiter.Wait(
                done, SyncWaiter::Clock::now(),
                SyncWaiter::Clock::now() + std::chrono::microseconds(300))) {
          notified_results.fetch_add(1);
        }
        notify_in_hook = nullptr;
      }
    });
  }
  for (auto& th : threads) th.join();

  auto const stats = waiter.stats();
  EXPECT_LE(max_active.load(), kCap);
  EXPECT_GE(max_active.load(), 1);
  EXPECT_EQ(gate.active(), 0);
  EXPECT_GT(stats.spin_hits, 0u);
  EXPECT_EQ(static_cast<int>(stats.spin_hits), notified_results.load());
  EXPECT_LE(stats.spin_hits + stats.spin_misses + stats.gate_full,
            static_cast<std::uint64_t>(kThreads * kIterations));
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
