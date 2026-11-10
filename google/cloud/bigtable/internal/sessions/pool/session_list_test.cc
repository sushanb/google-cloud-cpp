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

#include "google/cloud/bigtable/internal/sessions/pool/session_list.h"
#include "google/cloud/internal/make_status.h"
#include <gtest/gtest.h>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace pool {
namespace {

TEST(SessionListTest, OkGatedPeakEwmaIgnoresErrorsAndSubtractsPoolWait) {
  OkGatedPeakEwma ewma;
  EXPECT_DOUBLE_EQ(ewma.e2e_ewma_us(), 1000.0);
  EXPECT_DOUBLE_EQ(ewma.transport_ewma_us(), 500.0);

  // Non-OK status must NOT poison EWMA toward 0.
  ewma.RecordOutcome(google::cloud::internal::UnavailableError("fail"), 10.0,
                     0.0, 0.0);
  EXPECT_DOUBLE_EQ(ewma.e2e_ewma_us(), 1000.0);
  EXPECT_DOUBLE_EQ(ewma.transport_ewma_us(), 500.0);

  // OK status with 5000us total, 2000us pool wait, 1200us backend -> 3000us
  // wire e2e and 1800us transport (both > seed, so Peak-EWMA jumps to peak).
  ewma.RecordOutcome(Status{}, 5000.0, 2000.0, 1200.0);
  EXPECT_DOUBLE_EQ(ewma.e2e_ewma_us(), 3000.0);
  EXPECT_DOUBLE_EQ(ewma.transport_ewma_us(), 1800.0);
}

TEST(SessionListTest, EnforcesBudgetRefCountAndClosingReleaseInvariants) {
  SessionList list;
  auto h1 = list.NewStartingHandle(1, 0);
  EXPECT_EQ(list.sessions_holding_budget_count(), 1u);
  EXPECT_EQ(list.expected_capacity(), 1u);

  // Handshake completes -> budget slot released, peer ref_count = 1.
  list.OnSessionReady(h1, 101);
  EXPECT_EQ(list.sessions_holding_budget_count(), 0u);
  EXPECT_EQ(list.peer_ref_count(101), 1u);
  EXPECT_EQ(list.ready_count(), 1u);

  // Checkout -> kInFlight; GOAWAY while in-flight -> OnSessionClosing drops
  // expected_capacity without dropping peer_ref_count, and blocks
  // ReleaseToPool.
  auto checked_out = list.CheckoutFromPeer(101);
  ASSERT_EQ(checked_out, h1);
  EXPECT_EQ(list.ready_count(), 0u);

  list.OnSessionClosing(h1);
  EXPECT_EQ(list.expected_capacity(), 0u);
  EXPECT_EQ(list.peer_ref_count(101), 1u);
  EXPECT_FALSE(list.ReleaseToPool(h1));
  EXPECT_EQ(list.ready_count(), 0u);

  // Final close decrements peer_ref_count to 0 and cleans up group.
  list.OnSessionClosed(h1);
  EXPECT_EQ(list.peer_ref_count(101), 0u);
}

}  // namespace
}  // namespace pool
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
