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

#include "google/cloud/bigtable/internal/sessions/pool/pool_sizer.h"
#include <google/bigtable/v2/session.pb.h>
#include <gtest/gtest.h>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

TEST(PoolSizerTest, EvaluatesScalingDeltaCorrectly) {
  int const ready_count = 2;
  int const active_calls = 4;

  PoolSizer sizer(1, 10, 0.2F);

  EXPECT_EQ(
      sizer.GetScaleDelta(ready_count, /*connecting_count=*/0, active_calls,
                          /*avg_write_queue_size=*/0.0F),
      3);
}

TEST(PoolSizerTest, AccountsForConnectingSessions) {
  PoolSizer sizer(1, 10, 0.2F);
  EXPECT_EQ(sizer.GetScaleDelta(/*ready_count=*/2, /*connecting_count=*/2,
                                /*active_calls=*/4,
                                /*avg_write_queue_size=*/0.0F),
            1);
}

TEST(PoolSizerTest, ClampsToMinimumAndMaximumBounds) {
  PoolSizer sizer_min(2, 10, 0.2F);
  EXPECT_EQ(sizer_min.GetScaleDelta(/*ready_count=*/1, /*connecting_count=*/0,
                                    /*active_calls=*/0,
                                    /*avg_write_queue_size=*/0.0F),
            1);

  PoolSizer sizer_max(1, 5, 0.2F);
  EXPECT_EQ(sizer_max.GetScaleDelta(/*ready_count=*/2, /*connecting_count=*/0,
                                    /*active_calls=*/100,
                                    /*avg_write_queue_size=*/0.0F),
            3);
}

TEST(PoolSizerTest, UpdatesConfigurationDynamically) {
  PoolSizer sizer(1, 5, 0.2F);

  google::bigtable::v2::SessionClientConfiguration::SessionPoolConfiguration
      config;
  config.set_min_session_count(3);
  config.set_max_session_count(8);
  config.set_headroom(0.5F);
  config.set_new_session_queue_length(5);

  sizer.UpdateConfig(config);

  EXPECT_EQ(sizer.GetScaleDelta(/*ready_count=*/1, /*connecting_count=*/0,
                                /*in_use_count=*/2, /*pending_count=*/5),
            4);
}

TEST(PoolSizerTest, DefaultHeadroomIsTenPercentAndIncorporatesPendingQueue) {
  PoolSizer sizer(1, 20);
  EXPECT_EQ(sizer.GetScaleDelta(/*ready_count=*/5, /*connecting_count=*/1,
                                /*in_use_count=*/10, /*pending_count=*/20),
            8);
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
