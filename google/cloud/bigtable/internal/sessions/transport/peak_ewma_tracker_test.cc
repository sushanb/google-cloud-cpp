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
#include <gtest/gtest.h>
#include <chrono>
#include <cmath>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

double ToDoubleMs(std::chrono::nanoseconds d) {
  return std::chrono::duration<double, std::milli>(d).count();
}

double MsToDoubleNs(double ms) { return ms * 1e6; }

TEST(PeakEwmaTrackerTest, InitialState) {
  PeakEwmaTracker tracker(std::chrono::seconds(1));
  EXPECT_EQ(tracker.active_requests(), 0);
  EXPECT_NEAR(tracker.GetScore(), MsToDoubleNs(1.0), 1.0);
}

TEST(PeakEwmaTrackerTest, TracksActiveRequests) {
  PeakEwmaTracker tracker(std::chrono::seconds(1));
  tracker.OnRequestStart();
  EXPECT_EQ(tracker.active_requests(), 1);
  tracker.OnRequestStart();
  EXPECT_EQ(tracker.active_requests(), 2);
  tracker.OnRequestComplete(std::chrono::milliseconds(10));
  EXPECT_EQ(tracker.active_requests(), 1);
  tracker.OnRequestComplete(std::chrono::milliseconds(10));
  EXPECT_EQ(tracker.active_requests(), 0);
  tracker.OnRequestComplete(std::chrono::milliseconds(10));
  EXPECT_EQ(tracker.active_requests(), 0);
}

TEST(PeakEwmaTrackerTest, UpdatesEwmaAndScore) {
  auto const t0 = PeakEwmaTracker::TimePoint(std::chrono::seconds(100));
  PeakEwmaTracker tracker(std::chrono::seconds(1));

  tracker.OnRequestComplete(std::chrono::milliseconds(100), t0);
  EXPECT_EQ(tracker.GetCurrentEwma(t0), std::chrono::milliseconds(100));
  EXPECT_NEAR(tracker.GetScore(t0), MsToDoubleNs(100.0), 1.0);

  auto const t1 = t0 + std::chrono::seconds(1);
  double const expected_decayed_ms = 100.0 * std::exp(-1.0);
  EXPECT_NEAR(ToDoubleMs(tracker.GetCurrentEwma(t1)), expected_decayed_ms,
              0.01);
  EXPECT_NEAR(tracker.GetScore(t1), MsToDoubleNs(expected_decayed_ms), 1000.0);
}

TEST(PeakEwmaTrackerTest, PeakBehavior) {
  auto const t0 = PeakEwmaTracker::TimePoint(std::chrono::seconds(100));
  PeakEwmaTracker tracker(std::chrono::seconds(1));

  tracker.OnRequestComplete(std::chrono::milliseconds(100), t0);

  auto const t1 = t0 + std::chrono::seconds(1);
  tracker.OnRequestComplete(std::chrono::milliseconds(10), t1);
  double const expected_decayed_ms = 100.0 * std::exp(-1.0);
  EXPECT_NEAR(ToDoubleMs(tracker.GetCurrentEwma(t1)), expected_decayed_ms,
              0.01);

  auto const t2 = t1 + std::chrono::seconds(1);
  tracker.OnRequestComplete(std::chrono::milliseconds(200), t2);
  EXPECT_EQ(tracker.GetCurrentEwma(t2), std::chrono::milliseconds(200));
}

TEST(PeakEwmaTrackerTest, ScoreAccountsForActiveRequests) {
  auto const t0 = PeakEwmaTracker::TimePoint(std::chrono::seconds(100));
  PeakEwmaTracker tracker(std::chrono::seconds(1));

  tracker.OnRequestComplete(std::chrono::milliseconds(100), t0);

  tracker.OnRequestStart();
  tracker.OnRequestStart();

  EXPECT_NEAR(tracker.GetScore(t0), MsToDoubleNs(300.0), 1.0);
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
