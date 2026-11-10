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

#include "google/cloud/bigtable/internal/sessions/latch.h"
#include "google/cloud/bigtable/internal/sessions/pool/metrics_collector.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_pool_impl.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_pool_impl_test_helpers.h"
#include "google/cloud/bigtable/internal/sessions/session/session_impl.h"
#include "google/cloud/bigtable/internal/sessions/session/write_status.h"
#include "google/cloud/bigtable/internal/sessions/sessions_options.h"
#include "google/cloud/internal/completion_queue_impl.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/options.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/str_cat.h"
#include <google/bigtable/v2/request_stats.pb.h>
#include <google/bigtable/v2/session.pb.h>
#include <google/protobuf/duration.pb.h>
#include <google/rpc/status.pb.h>
#include <gmock/gmock.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <sys/socket.h>
#include <unistd.h>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

namespace {

using ::google::bigtable::v2::VirtualRpcRequest;
using ::google::bigtable::v2::VirtualRpcResponse;

class ScopedPausedTcpListener {
 public:
  ScopedPausedTcpListener() {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ >= 0) {
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      addr.sin_port = 0;
      if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0 &&
          ::listen(fd_, 64) == 0) {
        socklen_t len = sizeof(addr);
        if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len) == 0) {
          port_ = ntohs(addr.sin_port);
        }
      }
    }
  }
  ~ScopedPausedTcpListener() {
    if (fd_ >= 0) ::close(fd_);
  }
  std::string endpoint() const { return absl::StrCat("127.0.0.1:", port_); }

 private:
  int fd_ = -1;
  uint16_t port_ = 0;
};

TEST_F(SessionPoolImplTest, AbortsPendingQueueOnPoolClose) {
  sess1_->is_busy_ = true;
  sess2_->is_busy_ = true;

  VirtualRpcRequest q;
  q.set_rpc_id(55);
  bool aborted = false;
  EXPECT_TRUE(pool_
                  ->StartVirtualRpc(
                      nullptr, q,
                      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
                        aborted = true;
                        EXPECT_EQ(resp.status().code(),
                                  google::cloud::StatusCode::kCancelled);
                      })
                  .ok());

  pool_->Close();
  EXPECT_TRUE(aborted);
  EXPECT_TRUE(sess1_->is_closed_);
  EXPECT_TRUE(sess2_->is_closed_);
}

TEST_F(SessionPoolImplTest, CloseGracefullyDrainsActiveSessions) {
  VirtualRpcRequest req1;
  req1.set_rpc_id(10);
  bool cb1_called = false;
  EXPECT_TRUE(pool_
                  ->StartVirtualRpc(
                      nullptr, req1,
                      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
                        cb1_called = true;
                        EXPECT_TRUE(resp.ok());
                      })
                  .ok());

  // Make sess2_ busy by starting a dummy call
  VirtualRpcRequest dummy;
  dummy.set_rpc_id(99);
  EXPECT_TRUE(pool_->StartVirtualRpc(nullptr, dummy, [](auto) {}).ok());

  VirtualRpcRequest req2;
  req2.set_rpc_id(20);
  bool cb2_called = false;
  EXPECT_TRUE(pool_
                  ->StartVirtualRpc(
                      nullptr, req2,
                      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
                        cb2_called = true;
                        EXPECT_EQ(resp.status().code(),
                                  google::cloud::StatusCode::kCancelled);
                      })
                  .ok());

  Latch close_done;
  google::cloud::Status close_status;
  std::thread close_thread([&]() {
    close_status = pool_->CloseGracefully(std::chrono::seconds(5));
    close_done.Notify();
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(close_done.HasBeenNotified());
  EXPECT_TRUE(sess1_->is_draining_);
  EXPECT_FALSE(sess1_->is_closed_);
  EXPECT_TRUE(cb2_called);

  VirtualRpcResponse v_resp;
  sess1_->CompleteRpc(10, std::chrono::nanoseconds(0), v_resp);
  sess2_->CompleteRpc(99, std::chrono::nanoseconds(0), v_resp);

  close_done.Wait();
  close_thread.join();

  EXPECT_TRUE(close_status.ok());
  EXPECT_TRUE(sess1_->is_closed_);
  EXPECT_TRUE(cb1_called);
}

TEST_F(SessionPoolImplTest, CloseGracefullyTimesOutAndForceCloses) {
  VirtualRpcRequest req1;
  req1.set_rpc_id(10);
  bool cb1_called = false;
  EXPECT_TRUE(pool_
                  ->StartVirtualRpc(
                      nullptr, req1,
                      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
                        cb1_called = true;
                        EXPECT_EQ(resp.status().code(),
                                  google::cloud::StatusCode::kCancelled);
                      })
                  .ok());

  SessionImpl::TimePoint start = SessionImpl::Clock::now();
  google::cloud::Status close_status =
      pool_->CloseGracefully(std::chrono::milliseconds(50));
  std::chrono::nanoseconds elapsed = SessionImpl::Clock::now() - start;

  EXPECT_GE(elapsed, std::chrono::milliseconds(50));
  EXPECT_EQ(close_status.code(), google::cloud::StatusCode::kDeadlineExceeded);

  EXPECT_TRUE(sess1_->is_closed_);
  EXPECT_TRUE(cb1_called);
}

TEST_F(SessionPoolImplTest, ExposesCountersAndHandlesGoAway) {
  EXPECT_EQ(pool_->active_sessions_count(), 2);
  pool_->OnGoAway(sess1_);
}

TEST_F(SessionPoolImplTest, PropagatesSessionStartFailureAndCleansUpBuffer) {
  sess1_->next_start_status_ =
      google::cloud::internal::InternalError("Socket write failure");

  VirtualRpcRequest req;
  req.set_rpc_id(99);
  bool cb_invoked = false;
  google::cloud::Status status = pool_->StartVirtualRpc(
      nullptr, req, [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_invoked = true;
        EXPECT_EQ(resp.status().code(), google::cloud::StatusCode::kInternal);
      });

  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(cb_invoked);
}

TEST_F(SessionPoolImplTest, PreWarmingComplesesSuccessfully) {
  auto sess3 = std::make_shared<MockSessionImplForPool>();
  sess3->state_ = SessionImpl::State::kConnecting;
  auto sess4 = std::make_shared<MockSessionImplForPool>();
  sess4->state_ = SessionImpl::State::kConnecting;

  auto local_pool = SessionPoolImpl::CreateForTesting({sess3, sess4}, 10);
  sess3->SetListener(local_pool);
  sess4->SetListener(local_pool);

  std::thread background_thread([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    google::bigtable::v2::OpenSessionResponse resp;
    local_pool->OnSessionReady(sess3, resp);
    local_pool->OnSessionReady(sess4, resp);
  });

  google::cloud::Status status =
      local_pool->WaitForPreWarming(std::chrono::seconds(2));
  EXPECT_TRUE(status.ok());
  background_thread.join();
}

TEST_F(SessionPoolImplTest, PreWarmingTimeout) {
  auto sess3 = std::make_shared<MockSessionImplForPool>();
  sess3->state_ = SessionImpl::State::kConnecting;

  auto local_pool = SessionPoolImpl::CreateForTesting({sess3}, 10);
  sess3->SetListener(local_pool);

  google::cloud::Status status =
      local_pool->WaitForPreWarming(std::chrono::milliseconds(100));
  EXPECT_EQ(status.code(), google::cloud::StatusCode::kDeadlineExceeded);
}

TEST_F(SessionPoolImplTest, PreWarmingPartialFailure) {
  auto sess3 = std::make_shared<MockSessionImplForPool>();
  sess3->state_ = SessionImpl::State::kConnecting;
  auto sess4 = std::make_shared<MockSessionImplForPool>();
  sess4->state_ = SessionImpl::State::kConnecting;

  auto local_pool = SessionPoolImpl::CreateForTesting({sess3, sess4}, 10);
  sess3->SetListener(local_pool);
  sess4->SetListener(local_pool);

  std::thread background_thread([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    google::bigtable::v2::OpenSessionResponse resp;
    local_pool->OnSessionReady(sess3, resp);
    local_pool->OnSessionClosed(
        sess4, google::cloud::internal::InternalError("Failed connection"));
  });

  google::cloud::Status status =
      local_pool->WaitForPreWarming(std::chrono::seconds(2));
  EXPECT_EQ(status.code(), google::cloud::StatusCode::kInternal);
  EXPECT_THAT(status.message(),
              ::testing::HasSubstr("Started: 2, Succeeded: 1"));
  background_thread.join();
}

TEST_F(SessionPoolImplTest, IdleReapingReapsOldestRespectingMinimums) {
  auto sess3 = std::make_shared<MockSessionImplForPool>();
  sess3->afe_id_ = "afe1";
  sess3->last_active_time_ =
      SessionImpl::Clock::now() - std::chrono::minutes(10);
  sess3->is_idle_ = true;

  auto sess4 = std::make_shared<MockSessionImplForPool>();
  sess4->afe_id_ = "afe1";
  sess4->last_active_time_ =
      SessionImpl::Clock::now() - std::chrono::minutes(8);
  sess4->is_idle_ = true;

  auto sess5 = std::make_shared<MockSessionImplForPool>();
  sess5->afe_id_ = "afe1";
  sess5->is_idle_ = false;

  auto local_pool =
      SessionPoolImpl::CreateForTesting({sess3, sess4, sess5}, 10, std::nullopt,
                                        nullptr, std::chrono::minutes(5), 1);
  sess3->SetListener(local_pool);
  sess4->SetListener(local_pool);
  sess5->SetListener(local_pool);

  EXPECT_EQ(local_pool->active_sessions_count(), 3);

  local_pool->ReapIdleSessions();

  EXPECT_EQ(local_pool->active_sessions_count(), 1);
  EXPECT_TRUE(sess3->is_closed_);
  EXPECT_TRUE(sess4->is_closed_);
  EXPECT_FALSE(sess5->is_closed_);
}

TEST_F(SessionPoolImplTest, IdleReapingRespectsGroupMinSize) {
  auto sess3 = std::make_shared<MockSessionImplForPool>();
  sess3->afe_id_ = "afe1";
  sess3->last_active_time_ =
      SessionImpl::Clock::now() - std::chrono::minutes(10);
  sess3->is_idle_ = true;

  auto sess4 = std::make_shared<MockSessionImplForPool>();
  sess4->afe_id_ = "afe1";
  sess4->last_active_time_ =
      SessionImpl::Clock::now() - std::chrono::minutes(5);
  sess4->is_idle_ = true;

  auto sess5 = std::make_shared<MockSessionImplForPool>();
  sess5->afe_id_ = "afe1";
  sess5->is_idle_ = false;

  // Set min_pool_size to 2
  auto local_pool =
      SessionPoolImpl::CreateForTesting({sess3, sess4, sess5}, 10, std::nullopt,
                                        nullptr, std::chrono::minutes(5), 2);
  sess3->SetListener(local_pool);
  sess4->SetListener(local_pool);
  sess5->SetListener(local_pool);

  EXPECT_EQ(local_pool->active_sessions_count(), 3);

  local_pool->ReapIdleSessions();

  EXPECT_EQ(local_pool->active_sessions_count(), 2);
  EXPECT_TRUE(sess3->is_closed_);
  EXPECT_FALSE(sess4->is_closed_);
  EXPECT_FALSE(sess5->is_closed_);
}

TEST_F(SessionPoolImplTest, IdleReapingRespectsGlobalMinSizer) {
  auto sess3 = std::make_shared<MockSessionImplForPool>();
  sess3->afe_id_ = "afe1";
  sess3->last_active_time_ =
      SessionImpl::Clock::now() - std::chrono::minutes(10);
  sess3->is_idle_ = true;

  auto sess4 = std::make_shared<MockSessionImplForPool>();
  sess4->afe_id_ = "afe1";
  sess4->last_active_time_ =
      SessionImpl::Clock::now() - std::chrono::minutes(5);
  sess4->is_idle_ = true;

  auto sess5 = std::make_shared<MockSessionImplForPool>();
  sess5->afe_id_ = "afe2";
  sess5->last_active_time_ =
      SessionImpl::Clock::now() - std::chrono::minutes(12);
  sess5->is_idle_ = true;

  // Set min_pool_size to 0 (per_group_min = 0)
  auto local_pool =
      SessionPoolImpl::CreateForTesting({sess3, sess4, sess5}, 10, std::nullopt,
                                        nullptr, std::chrono::minutes(5), 0);

  // Set global_min to 1 via config
  google::bigtable::v2::SessionClientConfiguration::SessionPoolConfiguration
      config;
  config.set_min_session_count(1);
  config.set_max_session_count(10);
  local_pool->UpdateConfigForTesting(config);

  sess3->SetListener(local_pool);
  sess4->SetListener(local_pool);
  sess5->SetListener(local_pool);

  EXPECT_EQ(local_pool->active_sessions_count(), 3);

  local_pool->ReapIdleSessions();

  // global_min is 1.
  // sess4 is kept because it's the newest idle session.
  // sess3, sess5 are reaped.
  EXPECT_EQ(local_pool->active_sessions_count(), 1);
  EXPECT_TRUE(sess3->is_closed_);
  EXPECT_FALSE(sess4->is_closed_);
  EXPECT_TRUE(sess5->is_closed_);
}

TEST_F(SessionPoolImplTest, BudgetSuccessTracking) {
  SessionPoolImplTestPeer::SetBudgetLimit(*pool_, 1);

  // Budget is initially empty.
  EXPECT_TRUE(SessionPoolImplTestPeer::TryReserveBudget(*pool_));
  // Budget is now full (1/1).
  EXPECT_FALSE(SessionPoolImplTestPeer::TryReserveBudget(*pool_));

  // Simulate session ready. It should call budget_.OnSuccess().
  google::bigtable::v2::OpenSessionResponse resp;
  pool_->OnSessionReady(sess1_, resp);

  // Budget should have 1 slot available now.
  EXPECT_TRUE(SessionPoolImplTestPeer::TryReserveBudget(*pool_));
}

TEST_F(SessionPoolImplTest, BudgetFailureTracking) {
  auto sess3 = std::make_shared<MockSessionImplForPool>();
  sess3->state_ = SessionImpl::State::kConnecting;

  auto local_pool = SessionPoolImpl::CreateForTesting({sess3}, 10);
  sess3->SetListener(local_pool);

  // Set budget to 1, penalty to 100ms
  SessionPoolImplTestPeer::SetBudgetConfig(*local_pool, 1,
                                           std::chrono::milliseconds(100));

  // Reserve the slot.
  EXPECT_TRUE(SessionPoolImplTestPeer::TryReserveBudget(*local_pool));
  EXPECT_FALSE(SessionPoolImplTestPeer::TryReserveBudget(*local_pool));

  // Close the connecting session (failure).
  local_pool->OnSessionClosed(
      sess3, google::cloud::internal::InternalError("connection failed"));

  // Budget should NOT be immediately available because of penalty.
  EXPECT_FALSE(SessionPoolImplTestPeer::TryReserveBudget(*local_pool));

  // Wait for penalty to expire.
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  // Budget should be available now.
  EXPECT_TRUE(SessionPoolImplTestPeer::TryReserveBudget(*local_pool));
}

TEST_F(SessionPoolImplTest,
       ShardSessionListSynchronizationAndPoolSnapshotPeers) {
  auto peer_sess = std::make_shared<MockSessionImplForPool>();
  peer_sess->peer_id_ = "42";
  auto local_pool =
      SessionPoolImpl::CreateForTesting({peer_sess}, 10, std::nullopt, nullptr);
  peer_sess->SetListener(local_pool);

  // 1. Verify initial ready state in SessionList via PoolSnapshot (`B3`/`B7`):
  auto snap1 = local_pool->GetPoolSnapshot();
  ASSERT_EQ(snap1.peers.size(), 1u);
  EXPECT_EQ(snap1.peers[0].peer_id, 42);
  EXPECT_EQ(snap1.peers[0].ref_count, 1u);
  EXPECT_EQ(snap1.peers[0].idle_count, 1u);
  EXPECT_EQ(snap1.peers[0].in_flight_count, 0u);

  // 2. Dispatch an in-flight vRPC and verify SessionList transitions handle to
  // `kInFlight`:
  VirtualRpcRequest req;
  req.set_rpc_id(201);
  EXPECT_TRUE(local_pool->StartVirtualRpc(nullptr, req, [](auto) {}).ok());
  auto snap2 = local_pool->GetPoolSnapshot();
  ASSERT_EQ(snap2.peers.size(), 1u);
  EXPECT_EQ(snap2.peers[0].idle_count, 0u);
  EXPECT_EQ(snap2.peers[0].in_flight_count, 1u);

  // 3. Complete vRPC and verify `ReleaseToPool` (`I5`) restores `idle_count`:
  VirtualRpcResponse resp;
  resp.set_rpc_id(201);
  peer_sess->CompleteRpc(201, std::chrono::nanoseconds(0), resp);
  auto snap3 = local_pool->GetPoolSnapshot();
  ASSERT_EQ(snap3.peers.size(), 1u);
  EXPECT_EQ(snap3.peers[0].idle_count, 1u);
  EXPECT_EQ(snap3.peers[0].in_flight_count, 0u);

  // 4. Close session and verify `OnSessionClosed` (`I6`) decrements
  // `ref_count` to 0 and prunes the empty ConnectionGroup:
  local_pool->OnSessionClosed(peer_sess, google::cloud::Status());
  auto snap4 = local_pool->GetPoolSnapshot();
  EXPECT_TRUE(snap4.peers.empty());
}

TEST_F(SessionPoolImplTest,
       ConcurrentStartVirtualRpcAndCloseInvokeEachCallbackOnce) {
  // Force the locked fallback/queue path so Close() races FailPendingCalls
  // against concurrent enqueues.
  sess1_->is_busy_ = true;
  sess2_->is_busy_ = true;
  constexpr int kThreads = 8;
  constexpr int kPerThread = 200;
  std::atomic<int> callbacks{0};
  std::atomic<int> started{0};
  std::atomic<bool> go{false};
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      while (!go.load()) std::this_thread::yield();
      for (int i = 0; i < kPerThread; ++i) {
        VirtualRpcRequest req;
        req.set_rpc_id(t * kPerThread + i + 1);
        started.fetch_add(1);
        (void)pool_->StartVirtualRpc(
            nullptr, std::move(req),
            [&](google::cloud::StatusOr<VirtualRpcResponse>) {
              callbacks.fetch_add(1);
            });
      }
    });
  }
  go = true;
  // Let some calls land, then close while the rest are still arriving.
  while (started.load() < kThreads * kPerThread / 4) std::this_thread::yield();
  pool_->Close();
  for (auto& th : threads) th.join();

  auto const deadline = SessionImpl::Clock::now() + std::chrono::seconds(10);
  while (callbacks.load() < kThreads * kPerThread &&
         SessionImpl::Clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_EQ(callbacks.load(), kThreads * kPerThread);
  EXPECT_EQ(pool_->pending_queue_size(), 0u);
}

TEST(SessionCreationBudgetTest,
     BoundsDelayedTokensAndRecoversAfterSinglePenaltyWindow) {
  SessionCreationBudget budget(3, std::chrono::milliseconds(25));
  for (int i = 0; i < 16; ++i) {
    budget.ForceReserve();
  }
  for (int i = 0; i < 16; ++i) {
    budget.OnFailure();
  }
  EXPECT_FALSE(budget.TryReserve());
  std::this_thread::sleep_for(std::chrono::milliseconds(40));
  EXPECT_TRUE(budget.TryReserve());
}

TEST_F(SessionPoolImplTest,
       TracksSessionsHoldingBudgetAcrossCreateReadyAndClose) {
  ScopedPausedTcpListener listener;
  auto channel = grpc::CreateChannel(
      listener.endpoint(), grpc::experimental::LocalCredentials(LOCAL_TCP));
  auto stub = google::bigtable::v2::Bigtable::NewStub(channel);
  std::vector<google::bigtable::v2::Bigtable::Stub*> stubs = {stub.get()};
  google::cloud::Options options;
  options.set<::google::cloud::bigtable_internal::SessionsNumEventLoopsOption>(
      1);
  options
      .set<::google::cloud::bigtable_internal::SessionsInitialPoolSizeOption>(
          2);

  auto pool = SessionPoolImpl::Create(
      stubs, "table", "profile",
      google::bigtable::v2::OpenTableRequest::PERMISSION_UNSET, options,
      nullptr);

  auto connecting = SessionPoolImplTestPeer::GetConnectingSessions(*pool);
  ASSERT_EQ(connecting.size(), 2u);
  EXPECT_EQ(SessionPoolImplTestPeer::GetSessionsHoldingBudgetCount(*pool), 2u);

  google::bigtable::v2::OpenSessionResponse open_resp;
  pool->OnSessionReady(connecting[0], open_resp);
  EXPECT_EQ(SessionPoolImplTestPeer::GetSessionsHoldingBudgetCount(*pool), 1u);

  // Set budget limit to 1 so that connecting[1]'s OnFailure() penalty occupies
  // the 1 budget slot and prevents MaybeReplenishPool from spawning a 3rd
  // session while we verify that connecting[1] was erased from
  // sessions_holding_budget_.
  SessionPoolImplTestPeer::SetBudgetLimit(*pool, 1);

  pool->OnSessionClosed(
      connecting[1],
      google::cloud::internal::UnavailableError("handshake failed"));
  EXPECT_EQ(SessionPoolImplTestPeer::GetSessionsHoldingBudgetCount(*pool), 0u);
  pool->Close();
}

TEST_F(SessionPoolImplTest,
       ClosedAndDrainingSessionsRemovedFromIdleQueueAndReadyAborted) {
  // Part 1: OnGoAway and OnSessionClosed remove sessions from group->idle_queue
  // so TryDrainGroupPendingQueueLocked never pops a draining/closed session.
  {
    auto s1 =
        std::shared_ptr<MockSessionImplForPool>(new MockSessionImplForPool());
    auto s2 =
        std::shared_ptr<MockSessionImplForPool>(new MockSessionImplForPool());
    auto local_pool =
        SessionPoolImpl::CreateForTesting({s1, s2}, 10, std::nullopt, nullptr);
    s1->SetListener(local_pool);
    s2->SetListener(local_pool);

    // Push both s1 and s2 into group->idle_queue via OnWriteDone.
    local_pool->OnWriteDone(s1);
    local_pool->OnWriteDone(s2);
    EXPECT_TRUE(s1->in_idle_queue());
    EXPECT_TRUE(s2->in_idle_queue());

    // Transition s2 to draining via OnGoAway -> must be removed from
    // idle_queue.
    s2->state_ = SessionImpl::State::kDraining;
    local_pool->OnGoAway(s2);
    EXPECT_FALSE(s2->in_idle_queue());

    // Close s1 via OnSessionClosed -> must be removed from idle_queue.
    s1->state_ = SessionImpl::State::kClosed;
    local_pool->OnSessionClosed(
        s1, google::cloud::internal::UnavailableError("closed"));
    EXPECT_FALSE(s1->in_idle_queue());
    local_pool->Close();
  }

  // Part 2: If a session transitions to kClosing/kClosed concurrently with
  // OnSessionReady, OnSessionReady must not register it as a ready session.
  {
    ScopedPausedTcpListener listener;
    auto channel = grpc::CreateChannel(
        listener.endpoint(), grpc::experimental::LocalCredentials(LOCAL_TCP));
    auto stub = google::bigtable::v2::Bigtable::NewStub(channel);
    std::vector<google::bigtable::v2::Bigtable::Stub*> stubs = {stub.get()};
    google::cloud::Options options;
    options
        .set<::google::cloud::bigtable_internal::SessionsNumEventLoopsOption>(
            1);
    options
        .set<::google::cloud::bigtable_internal::SessionsInitialPoolSizeOption>(
            1);

    auto pool = SessionPoolImpl::Create(
        stubs, "table", "profile",
        google::bigtable::v2::OpenTableRequest::PERMISSION_UNSET, options,
        nullptr);

    auto connecting = SessionPoolImplTestPeer::GetConnectingSessions(*pool);
    ASSERT_EQ(connecting.size(), 1u);
    SessionPoolImplTestPeer::SetBudgetLimit(*pool, 1);
    pool->OnSessionClosed(
        connecting[0], google::cloud::internal::UnavailableError("clear real"));

    auto mock_sess =
        std::shared_ptr<MockSessionImplForPool>(new MockSessionImplForPool());
    mock_sess->peer_id_ = "88";
    mock_sess->state_ = SessionImpl::State::kClosed;
    mock_sess->SetListener(pool);

    google::bigtable::v2::OpenSessionResponse open_resp;
    pool->OnSessionReady(mock_sess, open_resp);

    EXPECT_EQ(pool->active_sessions_count(), 0u);
    EXPECT_FALSE(mock_sess->in_idle_queue());
    pool->Close();
  }
}

TEST_F(SessionPoolImplTest,
       ReapIdleSessionsSkipsAcquiredSessionsAndRemovesEmptyGroup) {
  auto s1 =
      std::shared_ptr<MockSessionImplForPool>(new MockSessionImplForPool());
  auto s2 =
      std::shared_ptr<MockSessionImplForPool>(new MockSessionImplForPool());
  s1->is_idle_.store(true);
  s2->is_idle_.store(true);
  s1->last_active_time_ = SessionImpl::TimePoint{};
  s2->last_active_time_ = SessionImpl::TimePoint{};

  auto local_pool = SessionPoolImpl::CreateForTesting(
      {s1, s2}, 10, std::nullopt, nullptr, std::chrono::minutes(5),
      /*min_pool_size=*/0);
  s1->SetListener(local_pool);
  s2->SetListener(local_pool);

  // Simulate s2 being acquired by a caller right before ReapIdleSessions runs
  // Pass 2: s2 has active_calls_ == 1 even if is_idle_ was true during Pass 1.
  ASSERT_TRUE(s2->Acquire());
  local_pool->ReapIdleSessions();

  // s1 is reaped, and s2 must NOT be closed because its single-flight slot is
  // occupied!
  EXPECT_TRUE(s1->is_closed_.load());
  EXPECT_FALSE(s2->is_closed_.load());
  s2->FinalizeActiveRpc(0);

  // Now add a session in a second group "peer_to_reap", keep s2 active, and
  // verify ReapIdleSessions closes g2_sess and removes the empty group.
  auto g2_sess =
      std::shared_ptr<MockSessionImplForPool>(new MockSessionImplForPool());
  g2_sess->peer_id_ = "peer_to_reap";
  g2_sess->is_idle_.store(true);
  g2_sess->last_active_time_ = SessionImpl::TimePoint{};
  g2_sess->SetListener(local_pool);
  SessionPoolImplTestPeer::AddSessionToShard(*local_pool, 0, g2_sess,
                                             "peer_to_reap");

  s2->is_idle_.store(false);
  local_pool->ReapIdleSessions();

  EXPECT_TRUE(g2_sess->is_closed_.load());
  // The emptied "peer_to_reap" ConnectionGroup must be marked closed and
  // removed from shard.connection_groups.
  EXPECT_EQ(SessionPoolImplTestPeer::GetGroupPendingQueueSize(*local_pool,
                                                              "peer_to_reap"),
            0u);
  local_pool->Close();
}

TEST_F(SessionPoolImplTest,
       DoesNotAbortQueuedWaitersDuringBudgetCooldownWhenTimerArmed) {
  ScopedPausedTcpListener listener;
  auto channel = grpc::CreateChannel(
      listener.endpoint(), grpc::experimental::LocalCredentials(LOCAL_TCP));
  auto stub = google::bigtable::v2::Bigtable::NewStub(channel);
  std::vector<google::bigtable::v2::Bigtable::Stub*> stubs = {stub.get()};
  auto noop_scheduler = [](std::chrono::nanoseconds, std::function<void()>) {};

  // Sub-test 1: OnSessionClosed with transient error while budget is in
  // cooldown must arm replenish_timer_ and preserve shard.pending_queue.
  {
    google::cloud::Options options;
    options
        .set<::google::cloud::bigtable_internal::SessionsNumEventLoopsOption>(
            1);
    options
        .set<::google::cloud::bigtable_internal::SessionsInitialPoolSizeOption>(
            1);

    auto pool = SessionPoolImpl::Create(
        stubs, "table", "profile",
        google::bigtable::v2::OpenTableRequest::PERMISSION_UNSET, options,
        noop_scheduler);

    SessionPoolImplTestPeer::SetBudgetConfig(*pool, 1,
                                             std::chrono::seconds(10));
    auto connecting = SessionPoolImplTestPeer::GetConnectingSessions(*pool);
    ASSERT_EQ(connecting.size(), 1u);

    VirtualRpcRequest req;
    req.set_rpc_id(601);
    auto callback_fired = std::make_shared<std::atomic<bool>>(false);
    ASSERT_TRUE(
        pool->StartVirtualRpc(
                nullptr, req,
                [callback_fired](google::cloud::StatusOr<VirtualRpcResponse>) {
                  callback_fired->store(true);
                })
            .ok());
    ASSERT_EQ(pool->pending_queue_size(), 1u);

    pool->OnSessionClosed(
        connecting[0],
        google::cloud::internal::UnavailableError("transient backend restart"));

    EXPECT_FALSE(callback_fired->load());
    EXPECT_EQ(pool->pending_queue_size(), 1u);
    pool->Close();
    pool.reset();
    EXPECT_TRUE(callback_fired->load());
  }

  // Sub-test 2: StartVirtualRpc arriving during budget cooldown after
  // replenish_timer_ is armed must stay queued rather than self-aborting.
  {
    google::cloud::Options options;
    options
        .set<::google::cloud::bigtable_internal::SessionsNumEventLoopsOption>(
            1);
    options
        .set<::google::cloud::bigtable_internal::SessionsInitialPoolSizeOption>(
            1);

    auto pool = SessionPoolImpl::Create(
        stubs, "table", "profile",
        google::bigtable::v2::OpenTableRequest::PERMISSION_UNSET, options,
        noop_scheduler);

    SessionPoolImplTestPeer::SetBudgetConfig(*pool, 1,
                                             std::chrono::seconds(10));
    auto connecting = SessionPoolImplTestPeer::GetConnectingSessions(*pool);
    ASSERT_EQ(connecting.size(), 1u);

    pool->OnSessionClosed(
        connecting[0],
        google::cloud::internal::UnavailableError("transient backend restart"));
    ASSERT_EQ(SessionPoolImplTestPeer::GetConnectingSessions(*pool).size(), 0u);

    VirtualRpcRequest req;
    req.set_rpc_id(602);
    auto callback_fired = std::make_shared<std::atomic<bool>>(false);
    ASSERT_TRUE(
        pool->StartVirtualRpc(
                nullptr, req,
                [callback_fired](google::cloud::StatusOr<VirtualRpcResponse>) {
                  callback_fired->store(true);
                })
            .ok());

    EXPECT_FALSE(callback_fired->load());
    EXPECT_EQ(pool->pending_queue_size(), 1u);
    pool->Close();
    pool.reset();
    EXPECT_TRUE(callback_fired->load());
  }
}

TEST_F(SessionPoolImplTest, GetShardForSessionFindsNonZeroShardAfterPoolClose) {
  ScopedPausedTcpListener listener;
  auto channel = grpc::CreateChannel(
      listener.endpoint(), grpc::experimental::LocalCredentials(LOCAL_TCP));
  auto stub = google::bigtable::v2::Bigtable::NewStub(channel);
  std::vector<google::bigtable::v2::Bigtable::Stub*> stubs = {stub.get()};
  google::cloud::Options options;
  options.set<::google::cloud::bigtable_internal::SessionsNumEventLoopsOption>(
      2);
  options
      .set<::google::cloud::bigtable_internal::SessionsInitialPoolSizeOption>(
          1);

  auto pool = SessionPoolImpl::Create(
      stubs, "table", "profile",
      google::bigtable::v2::OpenTableRequest::PERMISSION_UNSET, options,
      nullptr);

  SessionPoolImplTestPeer::SetBudgetLimit(*pool, 1);
  auto connecting = SessionPoolImplTestPeer::GetConnectingSessions(*pool);
  for (auto const& c : connecting) {
    pool->OnSessionClosed(
        c, google::cloud::internal::UnavailableError("clear real"));
  }

  // Register a mock session on shard 1 (`shard_idx = 1`), transition it to
  // ready, then call pool->Close() (which clears shard->connecting_sessions,
  // draining_sessions, and group->sessions before OnSessionClosed runs on a
  // non-CQ thread).
  auto shard1_sess =
      std::shared_ptr<MockSessionImplForPool>(new MockSessionImplForPool());
  shard1_sess->peer_id_ = "99";
  SessionPoolImplTestPeer::RegisterConnectingSessionOnShard(*pool, 1,
                                                            shard1_sess);
  google::bigtable::v2::OpenSessionResponse open_resp;
  pool->OnSessionReady(shard1_sess, open_resp);

  auto snap_before = pool->GetPoolSnapshot();
  ASSERT_EQ(snap_before.expected_capacity, 1u);

  // Close pool without shard1_sess firing OnSessionClosed inside Close(), then
  // invoke OnSessionClosed(shard1_sess) from the test thread.
  pool->Close();
  pool->OnSessionClosed(shard1_sess,
                        google::cloud::internal::CancelledError("closed"));

  auto snap_after = pool->GetPoolSnapshot();
  EXPECT_EQ(snap_after.expected_capacity, 0u);
  EXPECT_EQ(snap_after.sessions_holding_budget, 0u);
  for (auto const& cg : snap_after.peers) {
    EXPECT_EQ(cg.ref_count, 0u);
  }
}

TEST_F(SessionPoolImplTest,
       ConsecutiveHandshakeFailureCircuitBreakerTripsAndResets) {
  auto conn_sess1 = std::make_shared<MockSessionImplForPool>();
  conn_sess1->state_ = SessionImpl::State::kConnecting;
  auto conn_sess2 = std::make_shared<MockSessionImplForPool>();
  conn_sess2->state_ = SessionImpl::State::kConnecting;
  auto pool = SessionPoolImpl::CreateForTesting({conn_sess1, conn_sess2}, 10,
                                                std::nullopt, nullptr);
  conn_sess1->SetListener(pool);
  conn_sess2->SetListener(pool);
  pool->SetConsecutiveSessionFailureThresholdForTesting(2);

  pool->OnSessionClosed(
      conn_sess1, google::cloud::internal::UnavailableError("transient 1"));
  EXPECT_EQ(pool->consecutive_session_failures(), 1);
  pool->OnSessionClosed(
      conn_sess2, google::cloud::internal::UnavailableError("transient 2"));
  EXPECT_EQ(pool->consecutive_session_failures(), 2);

  VirtualRpcRequest vreq;
  vreq.set_rpc_id(501);
  google::cloud::Status cb_status;
  bool cb_fired = false;
  (void)pool->StartVirtualRpc(
      nullptr, vreq, [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
        cb_fired = true;
        cb_status = r.status();
      });
  EXPECT_TRUE(cb_fired);
  EXPECT_FALSE(cb_status.ok());
  EXPECT_TRUE(IsPoolRejected(cb_status));

  auto ready_sess = std::make_shared<MockSessionImplForPool>();
  ready_sess->state_ = SessionImpl::State::kReady;
  ready_sess->SetListener(pool);
  google::bigtable::v2::OpenSessionResponse open_resp;
  pool->OnSessionReady(ready_sess, open_resp);
  EXPECT_EQ(pool->consecutive_session_failures(), 0);
}

TEST_F(SessionPoolImplTest,
       UpdateConfigUpdatesConsecutiveSessionFailureThresholdAndViewPayloads) {
  auto pool = SessionPoolImpl::CreateForTesting({}, 10, std::nullopt, nullptr);
  EXPECT_EQ(pool->consecutive_session_failure_threshold(), 15);

  google::bigtable::v2::SessionClientConfiguration::SessionPoolConfiguration
      cfg;
  cfg.set_consecutive_session_failure_threshold(7);
  pool->UpdateConfigForTesting(cfg);
  EXPECT_EQ(pool->consecutive_session_failure_threshold(), 7);

  auto av_pool = SessionPoolImpl::Create(
      {}, "projects/p/instances/i/tables/t/authorizedViews/v", "app",
      google::bigtable::v2::OpenAuthorizedViewRequest::PERMISSION_READ_WRITE,
      google::cloud::Options{}, nullptr);
  google::bigtable::v2::OpenAuthorizedViewRequest av_req;
  ASSERT_TRUE(av_req.ParseFromString(av_pool->serialized_open_payload()));
  EXPECT_EQ(av_req.authorized_view_name(),
            "projects/p/instances/i/tables/t/authorizedViews/v");
  EXPECT_EQ(av_req.app_profile_id(), "app");
  EXPECT_EQ(
      av_req.permission(),
      google::bigtable::v2::OpenAuthorizedViewRequest::PERMISSION_READ_WRITE);
  EXPECT_EQ(av_pool->session_tracer().config().session_type,
            "SESSION_TYPE_AUTHORIZED_VIEW");
  EXPECT_EQ(av_pool->session_tracer().config().session_name,
            "projects/p/instances/i/tables/t/authorizedViews/v/app/WRITE");
  av_pool->Close();

  auto mv_pool = SessionPoolImpl::Create(
      {}, "projects/p/instances/i/materializedViews/mv", "app",
      google::bigtable::v2::OpenMaterializedViewRequest::PERMISSION_READ,
      google::cloud::Options{}, nullptr);
  google::bigtable::v2::OpenMaterializedViewRequest mv_req;
  ASSERT_TRUE(mv_req.ParseFromString(mv_pool->serialized_open_payload()));
  EXPECT_EQ(mv_req.materialized_view_name(),
            "projects/p/instances/i/materializedViews/mv");
  EXPECT_EQ(mv_req.app_profile_id(), "app");
  EXPECT_EQ(mv_req.permission(),
            google::bigtable::v2::OpenMaterializedViewRequest::PERMISSION_READ);
  EXPECT_EQ(mv_pool->session_tracer().config().session_type,
            "SESSION_TYPE_MATERIALIZED_VIEW");
  EXPECT_EQ(mv_pool->session_tracer().config().session_name,
            "projects/p/instances/i/materializedViews/mv/app/READ");
  mv_pool->Close();
  pool->Close();
}

TEST(PoolExecutorsTest,
     DestructorOnCallbackWorkerJoinsSiblingWorkersBeforeTeardown) {
  auto executors = PoolExecutors::Create(1, 2);
  auto* raw_pool = executors->callback_pool();
  ASSERT_NE(raw_pool, nullptr);

  Latch sibling_started;
  Latch allow_sibling_finish;
  std::atomic<bool> sibling_finished{false};
  Latch self_destruct_done;

  // Worker 0 runs a sibling callback that accesses executors' state before
  // finishing.
  raw_pool->Submit(
      [&]() {
        sibling_started.Notify();
        allow_sibling_finish.Wait();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        sibling_finished.store(true, std::memory_order_release);
      },
      0);

  sibling_started.Wait();

  // Worker 1 holds the final shared_ptr<PoolExecutors> and resets it inside
  // its own callback worker thread while Worker 0 is still running.
  raw_pool->Submit(
      [exec = std::move(executors), &allow_sibling_finish, &sibling_finished,
       &self_destruct_done]() mutable {
        allow_sibling_finish.Notify();
        exec.reset();
        // ~PoolExecutors() must have waited for sibling Worker 0 to finish
        // before returning and destroying event_loops_.
        EXPECT_TRUE(sibling_finished.load(std::memory_order_acquire));
        self_destruct_done.Notify();
      },
      1);

  self_destruct_done.Wait();
  // Give the detached self worker thread a brief moment to exit WorkerLoop.
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
