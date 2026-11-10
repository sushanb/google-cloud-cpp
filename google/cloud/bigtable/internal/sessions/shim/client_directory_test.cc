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

#include "google/cloud/bigtable/internal/sessions/shim/client_directory.h"
#include <gmock/gmock.h>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace shim {
namespace {

TEST(ClientDirectoryTest, InsertFindAndDefaultAppProfileNormalization) {
  ClientDirectory<int> dir;
  EXPECT_EQ(dir.Find("t1", 1), nullptr);

  auto const* e1 = dir.Insert("t1", 1, 42, "");
  ASSERT_NE(e1, nullptr);
  EXPECT_EQ(e1->app_profile_id, "default");

  auto const* found = dir.Find("t1", 1, "default");
  ASSERT_EQ(found, e1);
  EXPECT_EQ(found->value, 42);
  EXPECT_EQ(dir.Find("t1", 2, "default"), nullptr);
}

TEST(ClientDirectoryTest, EnforcesBoundedMaxEntriesCap) {
  ClientDirectory<int> dir(/*max_entries=*/4);
  for (int i = 0; i < 4; ++i) {
    ASSERT_NE(dir.Insert("table-" + std::to_string(i), 1, i), nullptr);
  }
  EXPECT_EQ(dir.size(), 4u);
  EXPECT_EQ(dir.Insert("table-overflow", 1, 99), nullptr);
  EXPECT_EQ(dir.size(), 4u);
}

TEST(ClientDirectoryTest, ConcurrentReadersWhileGrowingNeverCrashOrCorrupt) {
  ClientDirectory<int> dir;
  std::atomic<bool> stop{false};

  std::vector<std::thread> readers;
  readers.reserve(4);
  for (int r = 0; r < 4; ++r) {
    readers.emplace_back([&] {
      while (!stop.load(std::memory_order_acquire)) {
        for (int i = 0; i < 128; ++i) {
          auto const* e = dir.Find("t-" + std::to_string(i), 1, "prof");
          if (e != nullptr) {
            EXPECT_EQ(e->value, i * 10);
          }
        }
      }
    });
  }

  for (int i = 0; i < 128; ++i) {
    ASSERT_NE(dir.Insert("t-" + std::to_string(i), 1, i * 10, "prof"), nullptr);
  }
  stop.store(true, std::memory_order_release);
  for (auto& t : readers) t.join();

  for (int i = 0; i < 128; ++i) {
    auto const* e = dir.Find("t-" + std::to_string(i), 1, "prof");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->value, i * 10);
  }
}

}  // namespace
}  // namespace shim
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
