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

#include "google/cloud/bigtable/internal/sessions/pool/lightweight_thread_pool.h"
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

TEST(LightweightThreadPoolTest, ExecutesTasks) {
  LightweightThreadPool pool(2);
  std::atomic<int> counter{0};

  for (int i = 0; i < 10; ++i) {
    pool.Submit(LightweightThreadPool::Task([&counter] { counter++; }),
                static_cast<std::size_t>(i));
  }

  pool.Shutdown();
  EXPECT_EQ(counter.load(), 10);
}

TEST(LightweightThreadPoolTest, ShutdownBlocksUntilDrained) {
  LightweightThreadPool pool(2);
  std::atomic<int> counter{0};

  pool.Submit(LightweightThreadPool::Task([&counter] {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                counter++;
              }),
              0);

  pool.Shutdown();
  EXPECT_EQ(counter.load(), 1);
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
