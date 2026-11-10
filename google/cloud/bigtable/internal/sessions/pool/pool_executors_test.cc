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

#include "google/cloud/bigtable/internal/sessions/pool/pool_executors.h"
#include "google/cloud/bigtable/internal/sessions/latch.h"
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

TEST(PoolExecutorsTest, ExplicitSizes) {
  auto ex = PoolExecutors::Create(2, 3);
  ASSERT_NE(ex, nullptr);
  EXPECT_EQ(ex->num_event_loops(), 2u);
  EXPECT_EQ(ex->num_callback_threads(), 3u);
  EXPECT_EQ(ex->thread_count(), 5u);
  EXPECT_NE(ex->callback_pool(), nullptr);
}

TEST(PoolExecutorsTest, ZeroMeansHardwareConcurrency) {
  auto ex = PoolExecutors::Create(0, 0);
  auto const hw = std::thread::hardware_concurrency();
  std::size_t const expected = hw == 0 ? 4 : hw;
  EXPECT_EQ(ex->num_event_loops(), expected);
  EXPECT_EQ(ex->num_callback_threads(), expected);
}

TEST(PoolExecutorsTest, CqIndexWrapsAndRunsWork) {
  auto ex = PoolExecutors::Create(2, 1);
  for (std::size_t i = 0; i < 5; ++i) {
    Latch done;
    ex->cq(i).RunAsync([&done] { done.Notify(); });
    EXPECT_TRUE(done.WaitFor(std::chrono::seconds(5))) << "cq index " << i;
  }
}

TEST(PoolExecutorsTest, CallbackPoolRunsTasks) {
  auto ex = PoolExecutors::Create(1, 2);
  constexpr int kTasks = 1000;
  std::atomic<int> ran{0};
  Latch done;
  for (int i = 0; i < kTasks; ++i) {
    ex->callback_pool()->Submit(
        [&ran, &done] {
          if (ran.fetch_add(1) + 1 == kTasks) done.Notify();
        },
        static_cast<std::size_t>(i));
  }
  EXPECT_TRUE(done.WaitFor(std::chrono::seconds(10)));
  EXPECT_EQ(ran.load(), kTasks);
}

TEST(PoolExecutorsTest, CancelAllIsIdempotentAndDestructorJoins) {
  auto ex = PoolExecutors::Create(2, 2);
  std::atomic<int> ran{0};
  for (int i = 0; i < 100; ++i) {
    ex->callback_pool()->Submit([&ran] { ran.fetch_add(1); },
                                static_cast<std::size_t>(i));
  }
  ex->CancelAll();
  ex->CancelAll();
  ex.reset();
  EXPECT_LE(ran.load(), 100);
}

TEST(PoolExecutorsTest, SharedOwnershipKeepsExecutorsAlive) {
  auto ex = PoolExecutors::Create(1, 1);
  std::weak_ptr<PoolExecutors> weak = ex;
  auto second_owner = ex;
  ex.reset();
  ASSERT_FALSE(weak.expired());
  Latch done;
  second_owner->callback_pool()->Submit([&done] { done.Notify(); }, 0);
  EXPECT_TRUE(done.WaitFor(std::chrono::seconds(5)));
  second_owner.reset();
  EXPECT_TRUE(weak.expired());
}

TEST(PoolExecutorsTest, DestructorOnEventLoopThreadDoesNotSelfJoin) {
  auto ex = PoolExecutors::Create(2, 1);
  auto cq = ex->cq(0);
  Latch done;
  cq.RunAsync([last_owner = std::move(ex), &done]() mutable {
    last_owner.reset();
    done.Notify();
  });
  ex.reset();
  EXPECT_TRUE(done.WaitFor(std::chrono::seconds(5)));
}

TEST(PoolExecutorsTest, DestructorOnCallbackWorkerThreadDoesNotSelfJoin) {
  auto ex = PoolExecutors::Create(1, 2);
  auto* pool = ex->callback_pool();
  Latch done;
  pool->Submit(
      [last_owner = std::move(ex), &done]() mutable {
        last_owner.reset();
        done.Notify();
      },
      0);
  ex.reset();
  EXPECT_TRUE(done.WaitFor(std::chrono::seconds(5)));
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
