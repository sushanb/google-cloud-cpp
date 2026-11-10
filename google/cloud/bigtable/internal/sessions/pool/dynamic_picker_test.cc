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

#include "google/cloud/bigtable/internal/sessions/pool/dynamic_picker.h"
#include "google/cloud/bigtable/internal/sessions/session/session_impl.h"
#include <google/bigtable/v2/session.pb.h>
#include <gtest/gtest.h>
#include <chrono>
#include <memory>
#include <unordered_set>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

class StubSessionForPicker : public SessionImpl {
 public:
  StubSessionForPicker(State state, bool is_write_congested,
                       int active_rpc_count = 0)
      : state_(state),
        is_write_congested_(is_write_congested),
        active_rpc_count_(active_rpc_count) {}

  State state() const override { return state_; }
  bool IsWriteCongested() const override { return is_write_congested_; }
  int active_rpc_count() const override { return active_rpc_count_; }

  State state_;
  bool is_write_congested_;
  int active_rpc_count_;
};

TEST(DynamicPickerTest, SelectsReadyNonBusySession) {
  auto busy_sess = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 1);
  auto connecting_sess = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kConnecting, false, 0);
  auto ready_sess = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);

  std::vector<SessionEntry> sessions = {
      {busy_sess}, {connecting_sess}, {ready_sess}};

  DynamicPicker picker;
  auto picked = picker.PickSession(sessions);

  ASSERT_TRUE(picked.has_value());
  EXPECT_EQ(picked.value(), ready_sess);
}

TEST(DynamicPickerTest, ReturnsNulloptWhenAllStreamsBusy) {
  auto busy1 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 1);
  auto busy2 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 1);

  std::vector<SessionEntry> sessions = {{busy1}, {busy2}};

  DynamicPicker picker;
  EXPECT_FALSE(picker.PickSession(sessions).has_value());
}

TEST(DynamicPickerTest, UpdatesConfigToRandomSelection) {
  DynamicPicker picker;

  google::bigtable::v2::SessionClientConfiguration::SessionPoolConfiguration
      config;
  config.mutable_load_balancing_options()->mutable_random();
  picker.UpdateConfig(config);

  auto ready1 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);
  auto ready2 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);

  std::vector<SessionEntry> sessions = {{ready1}, {ready2}};

  EXPECT_TRUE(picker.PickSession(sessions).has_value());
}

TEST(DynamicPickerTest, SelectsLeastLatencySession) {
  auto sess1 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);
  auto sess2 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);
  auto sess3 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);

  sess1->GetLatencyTracker()->OnRequestStart();
  sess1->GetLatencyTracker()->OnRequestComplete(std::chrono::milliseconds(100));

  sess2->GetLatencyTracker()->OnRequestStart();
  sess2->GetLatencyTracker()->OnRequestComplete(std::chrono::milliseconds(10));

  sess3->GetLatencyTracker()->OnRequestStart();
  sess3->GetLatencyTracker()->OnRequestComplete(std::chrono::milliseconds(50));

  std::vector<SessionEntry> sessions = {{sess1}, {sess2}, {sess3}};

  DynamicPicker picker;
  google::bigtable::v2::SessionClientConfiguration::SessionPoolConfiguration
      config;
  config.mutable_load_balancing_options()->mutable_peak_ewma();
  picker.UpdateConfig(config);

  auto picked = picker.PickSession(sessions);
  ASSERT_TRUE(picked.has_value());
  EXPECT_EQ(picked.value(), sess2);
}

TEST(DynamicPickerTest, LeastLatencyAccountsForInFlight) {
  auto sess1 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);
  auto sess2 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);

  // sess1: EWMA = 10ms, active = 2. Score = 10 * 3 = 30ms
  sess1->GetLatencyTracker()->OnRequestStart();
  sess1->GetLatencyTracker()->OnRequestComplete(std::chrono::milliseconds(10));
  sess1->GetLatencyTracker()->OnRequestStart();
  sess1->GetLatencyTracker()->OnRequestStart();

  // sess2: EWMA = 40ms, active = 0. Score = 40 * 1 = 40ms
  sess2->GetLatencyTracker()->OnRequestStart();
  sess2->GetLatencyTracker()->OnRequestComplete(std::chrono::milliseconds(40));

  std::vector<SessionEntry> sessions = {{sess1}, {sess2}};

  DynamicPicker picker;
  google::bigtable::v2::SessionClientConfiguration::SessionPoolConfiguration
      config;
  config.mutable_load_balancing_options()->mutable_peak_ewma();
  picker.UpdateConfig(config);

  auto picked = picker.PickSession(sessions);
  ASSERT_TRUE(picked.has_value());
  EXPECT_EQ(picked.value(), sess1);
}

TEST(DynamicPickerTest, LeastInFlightStrategyPicksLeastInFlight) {
  auto sess1 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 1);
  auto sess2 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);

  std::vector<SessionEntry> sessions = {{sess1}, {sess2}};

  DynamicPicker picker;

  auto picked = picker.PickSession(sessions);
  ASSERT_TRUE(picked.has_value());
  EXPECT_EQ(picked.value(), sess2);
}

TEST(DynamicPickerTest, RandomStrategyEdgeCases) {
  DynamicPicker picker;
  google::bigtable::v2::SessionClientConfiguration::SessionPoolConfiguration
      config;
  config.mutable_load_balancing_options()->mutable_random();
  picker.UpdateConfig(config);

  EXPECT_FALSE(picker.PickSession({}).has_value());

  auto sess1 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);
  std::unordered_set<SessionImpl*> ignored = {sess1.get()};
  EXPECT_FALSE(picker.PickSession({{sess1}}, ignored).has_value());

  for (int i = 0; i < 100; ++i) {
    auto picked = picker.PickSession({{sess1}});
    ASSERT_TRUE(picked.has_value());
    EXPECT_EQ(picked.value(), sess1);
  }

  std::vector<SessionEntry> large_sessions;
  large_sessions.reserve(10000);
  for (int i = 0; i < 10000; ++i) {
    large_sessions.push_back({std::make_shared<StubSessionForPicker>(
        SessionImpl::State::kReady, false, 0)});
  }
  auto picked_large = picker.PickSession(large_sessions);
  EXPECT_TRUE(picked_large.has_value());
}

TEST(DynamicPickerTest, RandomStrategyReservoirSamplingUnbiased) {
  DynamicPicker picker;
  google::bigtable::v2::SessionClientConfiguration::SessionPoolConfiguration
      config;
  config.mutable_load_balancing_options()->mutable_random();
  picker.UpdateConfig(config);

  auto sess0 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);
  auto sess1 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);
  auto sess2 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);
  std::vector<SessionEntry> sessions = {{sess0}, {sess1}, {sess2}};

  constexpr int kIterations = 60000;
  int count0 = 0;
  int count1 = 0;
  int count2 = 0;

  for (int i = 0; i < kIterations; ++i) {
    auto picked = picker.PickSession(sessions);
    ASSERT_TRUE(picked.has_value());
    if (picked.value() == sess0) {
      count0++;
    } else if (picked.value() == sess1) {
      count1++;
    } else if (picked.value() == sess2) {
      count2++;
    }
  }

  EXPECT_NEAR(count0, 20000, 1200);
  EXPECT_NEAR(count1, 20000, 1200);
  EXPECT_NEAR(count2, 20000, 1200);
}

TEST(DynamicPickerTest, WeightedRoundRobinBalancesProportionalToPeerWeight) {
  auto sess0 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);
  auto sess1 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);
  auto sess2 = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);

  sess0->SetPeerWeight(3.0);
  sess1->SetPeerWeight(2.0);
  sess2->SetPeerWeight(1.0);

  std::vector<SessionEntry> sessions = {{sess0}, {sess1}, {sess2}};

  DynamicPicker picker;
  picker.SetStrategy(DynamicPicker::Strategy::kWeightedRoundRobin);
  EXPECT_EQ(picker.strategy(), DynamicPicker::Strategy::kWeightedRoundRobin);

  int count0 = 0;
  int count1 = 0;
  int count2 = 0;
  for (int i = 0; i < 600; ++i) {
    auto picked = picker.PickSession(sessions);
    ASSERT_TRUE(picked.has_value());
    if (picked.value() == sess0) {
      count0++;
    } else if (picked.value() == sess1) {
      count1++;
    } else if (picked.value() == sess2) {
      count2++;
    }
  }

  EXPECT_EQ(count0, 300);
  EXPECT_EQ(count1, 200);
  EXPECT_EQ(count2, 100);
}

TEST(DynamicPickerTest,
     WeightedRoundRobinReducesTrafficOnPeerLoadInfoDegradation) {
  auto healthy_peer_sess = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);
  auto degraded_peer_sess = std::make_shared<StubSessionForPicker>(
      SessionImpl::State::kReady, false, 0);

  healthy_peer_sess->SetPeerWeight(1.0);
  degraded_peer_sess->SetPeerWeight(0.25);

  std::vector<SessionEntry> sessions = {{healthy_peer_sess},
                                        {degraded_peer_sess}};

  DynamicPicker picker;
  picker.SetStrategy(DynamicPicker::Strategy::kWeightedRoundRobin);

  int healthy_count = 0;
  int degraded_count = 0;
  for (int i = 0; i < 100; ++i) {
    auto picked = picker.PickSession(sessions);
    ASSERT_TRUE(picked.has_value());
    if (picked.value() == healthy_peer_sess) {
      healthy_count++;
    } else if (picked.value() == degraded_peer_sess) {
      degraded_count++;
    }
  }

  EXPECT_EQ(healthy_count, 80);
  EXPECT_EQ(degraded_count, 20);
}

TEST(DynamicPickerTest, LeastInFlightAndPeakEwmaRespectRandomSubsetSize) {
  DynamicPicker picker;
  google::bigtable::v2::SessionClientConfiguration::SessionPoolConfiguration
      config;
  config.mutable_load_balancing_options()
      ->mutable_least_in_flight()
      ->set_random_subset_size(2);
  picker.UpdateConfig(config);
  EXPECT_EQ(picker.strategy(), DynamicPicker::Strategy::kLeastInFlight);
  EXPECT_EQ(picker.random_subset_size(), 2);

  std::vector<SessionEntry> sessions;
  sessions.reserve(8);
  for (int i = 0; i < 8; ++i) {
    sessions.push_back({std::make_shared<StubSessionForPicker>(
        SessionImpl::State::kReady, false, 0)});
  }

  for (int i = 0; i < 50; ++i) {
    auto picked = picker.PickSession(sessions);
    EXPECT_TRUE(picked.has_value());
  }

  config.mutable_load_balancing_options()
      ->mutable_peak_ewma()
      ->set_random_subset_size(2);
  picker.UpdateConfig(config);
  EXPECT_EQ(picker.strategy(), DynamicPicker::Strategy::kLeastLatency);
  EXPECT_EQ(picker.random_subset_size(), 2);

  for (int i = 0; i < 50; ++i) {
    auto picked = picker.PickSession(sessions);
    EXPECT_TRUE(picked.has_value());
  }
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
