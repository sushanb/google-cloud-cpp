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

#include "google/cloud/bigtable/internal/sessions/sessions_options.h"
#include "google/cloud/internal/populate_common_options.h"
#include "google/cloud/testing_util/scoped_log.h"
#include <gmock/gmock.h>
#include <chrono>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace {

using ::testing::Eq;
using ::testing::IsEmpty;

TEST(SessionsOptionsTest, PopulateAndCheckExpectedOptions) {
  testing_util::ScopedLog log;
  Options opts;
  opts.set<SessionsInitialPoolSizeOption>(8)
      .set<SessionsMaxQueuedCallsOption>(256)
      .set<SessionsKeepaliveMultiplierOption>(3.5)
      .set<SessionsNumEventLoopsOption>(4)
      .set<SessionsNumShardsOption>(16)
      .set<SessionsMaxSessionsOption>(128)
      .set<SessionsSyncSpinBudgetOption>(std::chrono::microseconds(200))
      .set<SessionsSyncMaxSpinnersOption>(2)
      .set<SessionsClientSideMetricsEnabledOption>(true);

  google::cloud::internal::CheckExpectedOptions<SessionsOptionList>(opts,
                                                                    "test");
  EXPECT_THAT(log.ExtractLines(), IsEmpty());
  EXPECT_THAT(opts.get<SessionsInitialPoolSizeOption>(), Eq(8));
  EXPECT_THAT(opts.get<SessionsMaxQueuedCallsOption>(), Eq(256));
  EXPECT_THAT(opts.get<SessionsKeepaliveMultiplierOption>(), Eq(3.5));
  EXPECT_THAT(opts.get<SessionsNumEventLoopsOption>(), Eq(4));
  EXPECT_THAT(opts.get<SessionsNumShardsOption>(), Eq(16));
  EXPECT_THAT(opts.get<SessionsMaxSessionsOption>(), Eq(128));
  EXPECT_THAT(opts.get<SessionsSyncSpinBudgetOption>(),
              Eq(std::chrono::microseconds(200)));
  EXPECT_THAT(opts.get<SessionsSyncMaxSpinnersOption>(), Eq(2));
  EXPECT_TRUE(opts.get<SessionsClientSideMetricsEnabledOption>());
}

}  // namespace
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
