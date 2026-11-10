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

#include "google/cloud/bigtable/internal/sessions/pool/session_pool_impl.h"
#include "google/cloud/bigtable/internal/sessions/latch.h"
#include "google/cloud/bigtable/internal/sessions/pool/metrics_collector.h"
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

TEST_F(SessionPoolImplTest, RoutesDirectlyWhenSessionReady) {
  VirtualRpcRequest req;
  req.set_rpc_id(10);

  bool cb_invoked = false;
  EXPECT_TRUE(pool_
                  ->StartVirtualRpc(
                      nullptr, req,
                      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
                        cb_invoked = true;
                        EXPECT_TRUE(resp.ok());
                      })
                  .ok());

  EXPECT_EQ(sess1_->start_count_, 1);
  EXPECT_EQ(sess1_->last_request_.rpc_id(), 10);
  EXPECT_TRUE(sess1_->is_busy_);
  EXPECT_EQ(pool_->pending_queue_size(), 0);

  VirtualRpcResponse v_resp;
  v_resp.set_rpc_id(10);
  sess1_->CompleteRpc(10, std::chrono::nanoseconds(0), v_resp);
  EXPECT_TRUE(cb_invoked);
}

TEST_F(SessionPoolImplTest, BuffersRequestsWhenSessionsSaturated) {
  VirtualRpcRequest req1;
  req1.set_rpc_id(10);
  EXPECT_TRUE(pool_->StartVirtualRpc(nullptr, req1, [](auto resp) {}).ok());
  EXPECT_EQ(sess1_->start_count_, 1);

  VirtualRpcRequest req2;
  req2.set_rpc_id(20);
  EXPECT_TRUE(pool_->StartVirtualRpc(nullptr, req2, [](auto resp) {}).ok());
  EXPECT_EQ(sess2_->start_count_, 1);

  VirtualRpcRequest req3;
  req3.set_rpc_id(30);
  bool queued_cb_invoked = false;
  EXPECT_TRUE(pool_
                  ->StartVirtualRpc(
                      nullptr, req3,
                      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
                        queued_cb_invoked = true;
                        EXPECT_TRUE(resp.ok());
                      })
                  .ok());

  EXPECT_EQ(pool_->pending_queue_size(), 1);

  VirtualRpcResponse v_resp;
  v_resp.set_rpc_id(10);
  sess1_->CompleteRpc(10, std::chrono::nanoseconds(0), v_resp);

  EXPECT_EQ(pool_->pending_queue_size(), 0);
  EXPECT_EQ(sess1_->start_count_, 2);
  EXPECT_EQ(sess1_->last_request_.rpc_id(), 30);
}

TEST_F(SessionPoolImplTest, RejectsRequestsWhenQueueFull) {
  VirtualRpcRequest dummy1, dummy2;
  dummy1.set_rpc_id(1);
  dummy2.set_rpc_id(2);
  EXPECT_TRUE(pool_->StartVirtualRpc(nullptr, dummy1, [](auto resp) {}).ok());
  EXPECT_TRUE(pool_->StartVirtualRpc(nullptr, dummy2, [](auto resp) {}).ok());

  VirtualRpcRequest q1;
  q1.set_rpc_id(101);
  EXPECT_TRUE(pool_->StartVirtualRpc(nullptr, q1, [](auto resp) {}).ok());

  VirtualRpcRequest q2;
  q2.set_rpc_id(102);
  EXPECT_TRUE(pool_->StartVirtualRpc(nullptr, q2, [](auto resp) {}).ok());

  EXPECT_EQ(pool_->pending_queue_size(), 2);

  VirtualRpcRequest overflow;
  overflow.set_rpc_id(103);
  bool cb_called = false;
  google::cloud::Status status = pool_->StartVirtualRpc(
      nullptr, overflow, [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_called = true;
        EXPECT_EQ(resp.status().code(),
                  google::cloud::StatusCode::kResourceExhausted);
      });
  EXPECT_EQ(status.code(), google::cloud::StatusCode::kResourceExhausted);
  EXPECT_TRUE(cb_called);
}

TEST_F(SessionPoolImplTest, RejectsRequestsWhenClosed) {
  pool_->Close();

  VirtualRpcRequest req;
  req.set_rpc_id(200);
  bool cb_called = false;
  google::cloud::Status status = pool_->StartVirtualRpc(
      nullptr, req, [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_called = true;
        EXPECT_EQ(resp.status().code(),
                  google::cloud::StatusCode::kUnavailable);
      });
  EXPECT_EQ(status.code(), google::cloud::StatusCode::kUnavailable);
  EXPECT_TRUE(cb_called);
}

TEST_F(SessionPoolImplTest, PreventsPickerRaceCondition) {
  VirtualRpcRequest req1;
  req1.set_rpc_id(1);
  VirtualRpcRequest req2;
  req2.set_rpc_id(2);

  EXPECT_TRUE(pool_->StartVirtualRpc(nullptr, req1, [](auto resp) {}).ok());
  EXPECT_TRUE(pool_->StartVirtualRpc(nullptr, req2, [](auto resp) {}).ok());

  EXPECT_EQ(sess1_->start_count_, 1);
  EXPECT_EQ(sess2_->start_count_, 1);
}

TEST_F(SessionPoolImplTest, CancelsCallOnContextCancellation) {
  google::cloud::CompletionQueue cq;
  std::thread t([cq]() mutable { cq.Run(); });
  pool_ = SessionPoolImpl::CreateForTesting({sess1_}, 10, cq, nullptr);
  sess1_->SetListener(pool_);

  grpc::ClientContext context;
  VirtualRpcRequest req;
  req.set_rpc_id(42);

  Latch done;
  bool cb_called = false;
  google::cloud::Status status = pool_->StartVirtualRpc(
      &context, req, [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_called = true;
        EXPECT_EQ(resp.status().code(), google::cloud::StatusCode::kCancelled);
        done.Notify();
      });
  EXPECT_TRUE(status.ok());

  context.TryCancel();
  pool_->CancelByContext(&context);

  done.Wait();

  EXPECT_TRUE(cb_called);
  EXPECT_TRUE(sess1_->is_cancelled_);

  cq.Shutdown();
  t.join();
}

TEST_F(SessionPoolImplTest, LockShardingAllowsConcurrentAccess) {
  auto sess1 = std::make_shared<MockSessionImplForPool>();
  sess1->afe_id_ = "afe1";
  auto sess2 = std::make_shared<MockSessionImplForPool>();
  sess2->afe_id_ = "afe2";

  auto pool = SessionPoolImpl::CreateForTesting({sess1, sess2}, 10,
                                                std::nullopt, nullptr);
  sess1->SetListener(pool);
  sess2->SetListener(pool);

  Latch sess1_start_blocked;
  sess1->block_start_ = &sess1_start_blocked;

  // Thread 1: Start RPC on sess1 (afe1). It will block inside StartVirtualRpc.
  Latch thread1_started;
  Latch thread1_done;
  std::thread t1([&]() {
    VirtualRpcRequest req1;
    req1.set_rpc_id(1);
    thread1_started.Notify();
    EXPECT_TRUE(pool->StartVirtualRpc(nullptr, req1, [](auto) {}).ok());
    thread1_done.Notify();
  });

  thread1_started.Wait();
  // Wait a bit to ensure Thread 1 has called StartVirtualRpc and is blocked.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Thread 2: Start RPC on sess2 (afe2). It should NOT block because it locks a
  // different group.
  VirtualRpcRequest req2;
  req2.set_rpc_id(2);

  // Start RPC on sess2. If sharding is broken (global lock), this will block.
  // We use a timeout to verify it doesn't block.
  Latch thread2_done;
  std::thread t2([&]() {
    google::cloud::Status status =
        pool->StartVirtualRpc(nullptr, req2, [](auto) {});
    EXPECT_TRUE(status.ok());
    thread2_done.Notify();
  });

  // Verify Thread 2 completes within a short time (e.g. 500ms), even though
  // Thread 1 is still blocked.
  EXPECT_TRUE(thread2_done.WaitFor(std::chrono::milliseconds(500)));

  // Clean up: unblock Thread 1
  sess1_start_blocked.Notify();
  t1.join();
  t2.join();
}

class MockMetricsExporter : public MetricsExporter {
 public:
  MOCK_METHOD(void, Export, (MetricsSnapshot const& snapshot), (override));
};

TEST_F(SessionPoolImplTest, BackpressureRoutingOnWriteCongestion) {
  pool_->Close();

  auto sess3 = std::make_shared<MockSessionImplForPool>();
  sess3->afe_id_ = "afe1";
  sess3->state_ = SessionImpl::State::kReady;

  auto local_pool = SessionPoolImpl::CreateForTesting({sess3}, 10);
  sess3->SetListener(local_pool);

  sess3->is_write_congested_ = true;

  VirtualRpcRequest req1;
  req1.set_rpc_id(1);
  bool cb1_called = false;
  EXPECT_TRUE(local_pool
                  ->StartVirtualRpc(
                      nullptr, req1,
                      [&](auto resp) {
                        cb1_called = true;
                        EXPECT_TRUE(resp.ok());
                      },
                      /*is_write_request=*/true)
                  .ok());

  EXPECT_EQ(local_pool->pending_queue_size(), 1);
  EXPECT_EQ(sess3->start_count_, 0);

  sess3->is_write_congested_ = false;
  local_pool->OnWriteDone(sess3);

  EXPECT_EQ(local_pool->pending_queue_size(), 0);
  EXPECT_EQ(sess3->start_count_, 1);

  VirtualRpcResponse v_resp;
  v_resp.set_rpc_id(1);
  sess3->CompleteRpc(1, std::chrono::nanoseconds(0), v_resp);
  EXPECT_TRUE(cb1_called);
}

TEST_F(SessionPoolImplTest, ReadBypassesWriteCongestion) {
  pool_->Close();

  auto sess = std::make_shared<MockSessionImplForPool>();
  sess->afe_id_ = "afe1";
  sess->state_ = SessionImpl::State::kReady;

  auto local_pool = SessionPoolImpl::CreateForTesting({sess}, 10);
  sess->SetListener(local_pool);

  sess->is_write_congested_ = true;

  VirtualRpcRequest req1;
  req1.set_rpc_id(1);
  bool cb1_called = false;
  EXPECT_TRUE(local_pool
                  ->StartVirtualRpc(
                      nullptr, req1,
                      [&](auto resp) {
                        cb1_called = true;
                        EXPECT_TRUE(resp.ok());
                      },
                      /*is_write_request=*/false)
                  .ok());

  // Read request ignores write congestion and starts immediately
  EXPECT_EQ(local_pool->pending_queue_size(), 0);
  EXPECT_EQ(sess->start_count_, 1);

  VirtualRpcResponse v_resp;
  v_resp.set_rpc_id(1);
  sess->CompleteRpc(1, std::chrono::nanoseconds(0), v_resp);
  EXPECT_TRUE(cb1_called);
}

TEST_F(SessionPoolImplTest, RoundRobinDistribution) {
  ScopedPausedTcpListener listener;
  auto channel = grpc::CreateChannel(
      listener.endpoint(), grpc::experimental::LocalCredentials(LOCAL_TCP));
  auto stub = google::bigtable::v2::Bigtable::NewStub(channel);
  std::vector<google::bigtable::v2::Bigtable::Stub*> stubs = {stub.get()};
  google::cloud::Options options;
  options.set<::google::cloud::bigtable_internal::SessionsNumEventLoopsOption>(
      4);
  options
      .set<::google::cloud::bigtable_internal::SessionsInitialPoolSizeOption>(
          8);

  auto pool = SessionPoolImpl::Create(
      stubs, "table", "profile",
      google::bigtable::v2::OpenTableRequest::PERMISSION_UNSET, options,
      nullptr);

  auto connecting = SessionPoolImplTestPeer::GetConnectingSessions(*pool);
  EXPECT_EQ(connecting.size(), 8);

  // Verify round-robin distribution of connecting sessions across 4 shards
  std::vector<int> shard_counts(4, 0);
  for (auto const& sess : connecting) {
    auto cq = SessionImplTestPeer::GetEventCq(*sess);
    ASSERT_TRUE(cq.has_value());
    auto cqs = SessionPoolImplTestPeer::GetEventLoopCqs(*pool);
    int found_idx = -1;
    for (size_t i = 0; i < cqs.size(); ++i) {
      if (google::cloud::internal::GetCompletionQueueImpl(*cq) ==
          google::cloud::internal::GetCompletionQueueImpl(cqs[i])) {
        found_idx = i;
        break;
      }
    }
    EXPECT_NE(found_idx, -1);
    shard_counts[found_idx]++;
  }

  for (int count : shard_counts) {
    EXPECT_EQ(count, 2);
  }
  pool->Close();
}

TEST_F(SessionPoolImplTest, ShardRoutingPrefersActiveShards) {
  std::vector<google::bigtable::v2::Bigtable::Stub*> stubs;
  google::cloud::Options options;
  options
      .set<::google::cloud::bigtable_internal::SessionsInitialPoolSizeOption>(
          0);
  options.set<::google::cloud::bigtable_internal::SessionsNumEventLoopsOption>(
      2);
  options.set<::google::cloud::bigtable_internal::SessionsNumShardsOption>(2);

  auto local_pool = SessionPoolImpl::Create(
      stubs, "table", "profile",
      google::bigtable::v2::OpenTableRequest::PERMISSION_UNSET, options,
      nullptr);

  // 1. Same thread should always get the same shard (affinity)
  void* shard_a =
      SessionPoolImplTestPeer::GetShardAddressForCurrentThread(*local_pool);
  void* shard_b =
      SessionPoolImplTestPeer::GetShardAddressForCurrentThread(*local_pool);
  EXPECT_EQ(shard_a, shard_b);

  // 2. Different threads should distribute (we check that we can get both
  // shards)
  std::set<void*> observed_shards;
  std::vector<std::thread> threads;
  std::mutex mu;
  for (int i = 0; i < 10; ++i) {
    threads.push_back(std::thread([&]() {
      void* s =
          SessionPoolImplTestPeer::GetShardAddressForCurrentThread(*local_pool);
      std::lock_guard<std::mutex> l(mu);
      observed_shards.insert(s);
    }));
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(observed_shards.size(), 2);

  // 3. Make Shard 0 active.
  auto mock_sess = std::make_shared<MockSessionImplForPool>();
  SessionPoolImplTestPeer::AddSessionToShard(*local_pool, 0, mock_sess, "afe1");

  // Now ALL threads should get Shard 0.
  observed_shards.clear();
  threads.clear();
  for (int i = 0; i < 10; ++i) {
    threads.push_back(std::thread([&]() {
      void* s =
          SessionPoolImplTestPeer::GetShardAddressForCurrentThread(*local_pool);
      std::lock_guard<std::mutex> l(mu);
      observed_shards.insert(s);
    }));
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(observed_shards.size(), 1);

  // 4. Make Shard 1 active too.
  auto mock_sess2 = std::make_shared<MockSessionImplForPool>();
  SessionPoolImplTestPeer::AddSessionToShard(*local_pool, 1, mock_sess2,
                                             "afe1");

  // Now they should distribute again.
  observed_shards.clear();
  threads.clear();
  for (int i = 0; i < 10; ++i) {
    threads.push_back(std::thread([&]() {
      void* s =
          SessionPoolImplTestPeer::GetShardAddressForCurrentThread(*local_pool);
      std::lock_guard<std::mutex> l(mu);
      observed_shards.insert(s);
    }));
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(observed_shards.size(), 2);
}

TEST(IdleSessionQueueTest, ConcurrentPushPop) {
  IdleSessionQueue queue;
  std::vector<std::thread> threads;
  std::atomic<int> success_pops{0};
  int num_threads = 16;
  int ops_per_thread = 1000;

  std::vector<std::shared_ptr<SessionImpl>> sessions;
  for (int i = 0; i < num_threads * ops_per_thread; ++i) {
    sessions.push_back(std::make_shared<MockSessionImplForPool>());
  }

  // Push concurrently
  for (int i = 0; i < num_threads; ++i) {
    threads.push_back(std::thread([&queue, &sessions, i, ops_per_thread]() {
      for (int j = 0; j < ops_per_thread; ++j) {
        queue.Push(sessions[i * ops_per_thread + j]);
      }
    }));
  }

  for (auto& t : threads) {
    t.join();
  }
  threads.clear();

  // Pop concurrently
  for (int i = 0; i < num_threads; ++i) {
    threads.push_back(std::thread([&queue, &success_pops, ops_per_thread]() {
      for (int j = 0; j < ops_per_thread; ++j) {
        auto sess = queue.Pop();
        if (sess != nullptr) {
          success_pops++;
        }
      }
    }));
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(success_pops.load(), num_threads * ops_per_thread);
  EXPECT_EQ(queue.Pop(), nullptr);
}

TEST_F(SessionPoolImplTest,
       SessionIsFinalizedBeforeCallbackCompletesForImmediateSyncReuse) {
  VirtualRpcRequest req1;
  req1.set_rpc_id(10);

  Latch callback_started;
  Latch callback_blocked;
  Latch callback_finished;

  bool pool_callback_invoked = false;
  EXPECT_TRUE(pool_
                  ->StartVirtualRpc(
                      nullptr, req1,
                      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
                        pool_callback_invoked = true;
                        callback_started.Notify();
                        callback_blocked.Wait();
                        EXPECT_TRUE(resp.ok());
                        callback_finished.Notify();
                      })
                  .ok());

  EXPECT_EQ(sess1_->start_count_ + sess2_->start_count_, 1);
  auto active_sess = (sess1_->start_count_ == 1) ? sess1_ : sess2_;
  EXPECT_EQ(active_sess->finalize_count_, 0);

  std::thread completion_thread([&]() {
    VirtualRpcResponse v_resp;
    v_resp.set_rpc_id(10);
    active_sess->CompleteRpc(10, std::chrono::nanoseconds(0), v_resp);
  });

  callback_started.Wait();
  // The response payload has already been moved out of SessionImpl before
  // wrapped_cb runs, so FinalizeSessionCall releases the slot before invoking
  // the user callback (matching DrainQueue). That allows a synchronous caller
  // waking from Latch::Notify() to immediately re-acquire the
  // session on the lock-free fast path.
  EXPECT_EQ(active_sess->finalize_count_, 1);

  callback_blocked.Notify();
  callback_finished.Wait();

  completion_thread.join();

  active_sess->finalize_done_.Wait();
  EXPECT_EQ(active_sess->finalize_count_, 1);
  EXPECT_TRUE(pool_callback_invoked);
}

TEST(IdleSessionQueueTest, DestructorStackOverflow) {
  // Push a large number of sessions to build a long linked list.
  // 50,000 nodes should be enough to trigger stack overflow with recursive
  // destructor.
  {
    IdleSessionQueue queue;
    for (int i = 0; i < 50000; ++i) {
      queue.Push(std::make_shared<MockSessionImplForPool>());
    }
  }
}

TEST_F(SessionPoolImplTest, PreventsDuplicateSessionsInIdleQueue) {
  auto group =
      SessionPoolImplTestPeer::GetOrCreateGroup(*pool_, "default_peer");

  // Initially, both sess1_ and sess2_ are in the idle queue.
  // Pop them both to empty the queue.
  auto p1 = group->idle_queue.Pop();
  auto p2 = group->idle_queue.Pop();
  EXPECT_NE(p1, nullptr);
  EXPECT_NE(p2, nullptr);
  EXPECT_EQ(group->idle_queue.Pop(), nullptr);

  // Mark sess1_ as ready, not write congested, and no active calls.
  sess1_->state_ = SessionImpl::State::kReady;
  sess1_->is_write_congested_ = false;
  sess1_->active_calls_ = 0;
  // Make sure in_idle_queue flag is false (since we popped it).
  EXPECT_FALSE(sess1_->in_idle_queue());

  // Call OnWriteDone. It should push sess1_ to idle_queue.
  pool_->OnWriteDone(sess1_);
  EXPECT_TRUE(sess1_->in_idle_queue());

  // Call OnWriteDone AGAIN. It should NOT push it again.
  pool_->OnWriteDone(sess1_);
  EXPECT_TRUE(sess1_->in_idle_queue());

  // Verify the queue has exactly ONE instance of sess1_.
  auto popped = group->idle_queue.Pop();
  EXPECT_EQ(popped, sess1_);
  EXPECT_FALSE(sess1_->in_idle_queue());
  EXPECT_EQ(group->idle_queue.Pop(), nullptr);
}

TEST_F(SessionPoolImplTest, ChannelCongestionLoadBalancing) {
  // Configure 2 mock stubs (idx 0 at 0x1000, idx 1 at 0x1010)
  SessionPoolImplTestPeer::ConfigureMockStubsForLoadBalancing(*pool_, 2);

  // Create 2 mock sessions and assign their stub pointers
  auto stub0 = SessionPoolImplTestPeer::GetStub(*pool_, 0);
  auto stub1 = SessionPoolImplTestPeer::GetStub(*pool_, 1);

  sess1_->mock_stub_ = stub0;
  sess2_->mock_stub_ = stub1;

  // Set up 5 active calls on stub 0, and 1 active call on stub 1
  SessionPoolImplTestPeer::SetActiveCallsForStubIndex(*pool_, 0, 5);
  SessionPoolImplTestPeer::SetActiveCallsForStubIndex(*pool_, 1, 1);

  // Verify GetActiveCallsForStub returns correct counts
  EXPECT_EQ(pool_->GetActiveCallsForStub(stub0), 5);
  EXPECT_EQ(pool_->GetActiveCallsForStub(stub1), 1);

  // Verify that GetStubActiveCalls returns correct counts
  EXPECT_EQ(sess1_->GetStubActiveCalls(), 5);
  EXPECT_EQ(sess2_->GetStubActiveCalls(), 1);

  // Both sessions are ready and have 0 active stream calls.
  sess1_->state_ = SessionImpl::State::kReady;
  sess2_->state_ = SessionImpl::State::kReady;
  sess1_->active_calls_.store(0);
  sess2_->active_calls_.store(0);

  // Request a session through the picker using LeastInFlight strategy.
  // The picker should pick sess2_ because its stub load is lower (joint score:
  // 1 vs 5)
  std::vector<SessionEntry> sessions = {SessionEntry{sess1_},
                                        SessionEntry{sess2_}};
  auto picked = SessionPoolImplTestPeer::GetPicker(*pool_).PickSession(
      sessions, {}, /*is_write_request=*/false);

  ASSERT_TRUE(picked.has_value());
  EXPECT_EQ(picked.value(), sess2_);
}

TEST_F(SessionPoolImplTest,
       ConnectionGroupSelectionPrefersLeastLoadedAndSkipsBlacklisted) {
  auto peer1_sess = std::make_shared<MockSessionImplForPool>();
  peer1_sess->peer_id_ = "peer-1";
  auto peer2_sess = std::make_shared<MockSessionImplForPool>();
  peer2_sess->peer_id_ = "peer-2";

  auto local_pool = SessionPoolImpl::CreateForTesting(
      {peer1_sess, peer2_sess}, 10, std::nullopt, nullptr);
  peer1_sess->SetListener(local_pool);
  peer2_sess->SetListener(local_pool);

  // Degrade peer-1's routing weight (0.2) while peer-2 has full weight (1.0):
  // GetLeastLoadedGroup should prefer peer-2 even when both have 0 active calls
  peer1_sess->SetPeerWeight(0.2);
  peer2_sess->SetPeerWeight(1.0);

  VirtualRpcRequest req1;
  req1.set_rpc_id(101);
  EXPECT_TRUE(local_pool->StartVirtualRpc(nullptr, req1, [](auto) {}).ok());
  EXPECT_EQ(peer2_sess->start_count_.load(), 1);
  EXPECT_EQ(peer1_sess->start_count_.load(), 0);

  VirtualRpcResponse resp1;
  resp1.set_rpc_id(101);
  peer2_sess->CompleteRpc(101, std::chrono::nanoseconds(0), resp1);

  // Now blacklist peer-2's ConnectionGroup temporarily; routing must shift to
  // peer-1
  auto group2 =
      SessionPoolImplTestPeer::GetOrCreateGroup(*local_pool, "peer-2");
  group2->is_blacklisted.store(true);
  group2->blacklist_expire_time_us.store(
      (SessionImpl::ToNanos(SessionImpl::Clock::now() +
                            std::chrono::seconds(60)) /
       1000));

  VirtualRpcRequest req2;
  req2.set_rpc_id(102);
  EXPECT_TRUE(local_pool->StartVirtualRpc(nullptr, req2, [](auto) {}).ok());
  EXPECT_EQ(peer1_sess->start_count_.load(), 1);
}

TEST_F(SessionPoolImplTest, PostEnqueueDrainWakesQueuedCallsWithoutLostWakeup) {
  class AsyncCompletingMockSession : public MockSessionImplForPool {
   public:
    void StartVirtualRpc(grpc::ClientContext* /*context*/,
                         VirtualRpcRequest request, GenericCallback callback,
                         bool /*is_write_request*/) override {
      std::lock_guard<std::mutex> lk(mu_);
      in_flight_.push_back({request.rpc_id(), std::move(callback)});
    }

    bool PopAndCompleteOne() {
      std::pair<int64_t, GenericCallback> item;
      {
        std::lock_guard<std::mutex> lk(mu_);
        if (in_flight_.empty()) return false;
        item = std::move(in_flight_.front());
        in_flight_.erase(in_flight_.begin());
      }
      active_calls_.store(0, std::memory_order_release);
      VirtualRpcResponse resp;
      resp.set_rpc_id(item.first);
      item.second(std::move(resp));
      return true;
    }

   private:
    std::mutex mu_;
    std::vector<std::pair<int64_t, GenericCallback>> in_flight_;
  };

  auto single_sess = std::make_shared<AsyncCompletingMockSession>();
  auto local_pool = SessionPoolImpl::CreateForTesting({single_sess}, 500,
                                                      std::nullopt, nullptr);
  single_sess->SetListener(local_pool);

  constexpr int kProducers = 4;
  constexpr int kPerProducer = 50;
  std::atomic<int> completed{0};
  std::atomic<int> next_id{1};

  std::vector<std::thread> producers;
  for (int t = 0; t < kProducers; ++t) {
    producers.emplace_back([&] {
      for (int i = 0; i < kPerProducer; ++i) {
        int id = next_id.fetch_add(1, std::memory_order_relaxed);
        VirtualRpcRequest req;
        req.set_rpc_id(id);
        EXPECT_TRUE(local_pool
                        ->StartVirtualRpc(
                            nullptr, std::move(req),
                            [&](google::cloud::StatusOr<VirtualRpcResponse>) {
                              completed.fetch_add(1, std::memory_order_relaxed);
                            })
                        .ok());
      }
    });
  }

  auto const deadline = SessionImpl::Clock::now() + std::chrono::seconds(10);
  while (completed.load(std::memory_order_relaxed) <
             kProducers * kPerProducer &&
         SessionImpl::Clock::now() < deadline) {
    if (!single_sess->PopAndCompleteOne()) {
      std::this_thread::yield();
    }
  }

  for (auto& th : producers) th.join();
  EXPECT_EQ(completed.load(), kProducers * kPerProducer);
  EXPECT_EQ(local_pool->pending_queue_size(), 0u);
  EXPECT_EQ(local_pool->active_calls_count(), 0u);
}

TEST_F(SessionPoolImplTest, FastPathCancelsContextDeadlineTimerOnCompletion) {
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));

  VirtualRpcRequest req;
  req.set_rpc_id(901);
  bool cb_called = false;
  EXPECT_TRUE(
      pool_
          ->StartVirtualRpc(&ctx, req,
                            [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
                              cb_called = true;
                              EXPECT_TRUE(r.ok());
                            })
          .ok());

  VirtualRpcResponse v_resp;
  v_resp.set_rpc_id(901);
  v_resp.mutable_stats()->mutable_backend_latency()->set_nanos(500000);
  if (sess1_->start_count_ > 0) {
    sess1_->CompleteRpc(901, std::chrono::nanoseconds(0), v_resp);
  } else {
    sess2_->CompleteRpc(901, std::chrono::nanoseconds(0), v_resp);
  }
  EXPECT_TRUE(cb_called);
}

TEST_F(SessionPoolImplTest,
       PostWriteCancellationKeepsActiveCallsCountUntilOnSlotDrained) {
  class DrainHoldingMockSession : public MockSessionImplForPool {
   public:
    bool IsCancelledAwaitingDrain(int64_t rpc_id) const override {
      return awaiting_drain_rpc_id_.load() == rpc_id;
    }

    void CancelPostWrite(int64_t rpc_id) {
      awaiting_drain_rpc_id_.store(rpc_id);
      GenericCallback cb = std::move(callback_);
      if (cb) {
        cb(google::cloud::internal::CancelledError(
            "vRPC cancelled after write"));
      }
    }

    void DrainSlotFromServerResponse(int64_t rpc_id) {
      awaiting_drain_rpc_id_.store(0);
      is_busy_.store(false);
      active_calls_.store(0);
      if (auto l = listener().lock()) {
        l->OnSlotDrained(shared_from_this(), rpc_id);
      }
    }

   private:
    std::atomic<int64_t> awaiting_drain_rpc_id_{0};
  };

  auto sess = std::make_shared<DrainHoldingMockSession>();
  auto local_pool =
      SessionPoolImpl::CreateForTesting({sess}, 10, std::nullopt, nullptr);
  sess->SetListener(local_pool);

  VirtualRpcRequest req;
  req.set_rpc_id(777);
  bool cb_called = false;
  EXPECT_TRUE(
      local_pool
          ->StartVirtualRpc(nullptr, req,
                            [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
                              cb_called = true;
                              EXPECT_EQ(r.status().code(),
                                        google::cloud::StatusCode::kCancelled);
                            })
          .ok());
  EXPECT_EQ(local_pool->active_calls_count(), 1u);

  // Cancel the vRPC after its write completed on the wire: the caller callback
  // fires immediately, but the session slot remains occupied awaiting the
  // server response frame, so active_calls_count() must stay at 1.
  sess->CancelPostWrite(777);
  EXPECT_TRUE(cb_called);
  EXPECT_EQ(local_pool->active_calls_count(), 1u);

  // When the server response frame arrives and drains the slot, OnSlotDrained
  // decrements active_calls_count() back to 0.
  sess->DrainSlotFromServerResponse(777);
  EXPECT_EQ(local_pool->active_calls_count(), 0u);
}

TEST_F(SessionPoolImplTest,
       SessionListTrackingPreservesInvariantsAcrossDrainAndFastPath) {
  ScopedPausedTcpListener listener;
  auto channel = grpc::CreateChannel(
      listener.endpoint(), grpc::experimental::LocalCredentials(LOCAL_TCP));
  auto stub = google::bigtable::v2::Bigtable::NewStub(channel);
  std::vector<google::bigtable::v2::Bigtable::Stub*> stubs = {stub.get()};

  // Part 1: Drain path -> FinalizeSessionCall -> OnSessionClosed.
  {
    google::cloud::Options options;
    options
        .set<::google::cloud::bigtable_internal::SessionsNumEventLoopsOption>(
            1);
    options
        .set<::google::cloud::bigtable_internal::SessionsInitialPoolSizeOption>(
            1);
    options.set<::google::cloud::bigtable_internal::SessionsMaxSessionsOption>(
        1);

    auto pool = SessionPoolImpl::Create(
        stubs, "table", "profile",
        google::bigtable::v2::OpenTableRequest::PERMISSION_UNSET, options,
        nullptr);

    auto connecting = SessionPoolImplTestPeer::GetConnectingSessions(*pool);
    ASSERT_EQ(connecting.size(), 1u);
    pool->OnSessionClosed(
        connecting[0], google::cloud::internal::UnavailableError("clear real"));

    auto peer_sess =
        std::shared_ptr<MockSessionImplForPool>(new MockSessionImplForPool());
    peer_sess->peer_id_ = "42";
    peer_sess->SetListener(pool);

    google::bigtable::v2::OpenSessionResponse open_resp;
    pool->OnSessionReady(peer_sess, open_resp);

    // 1. Occupy peer_sess via StartVirtualRpc (fast path).
    VirtualRpcRequest req1;
    req1.set_rpc_id(201);
    Latch cb1_done;
    ASSERT_TRUE(pool->StartVirtualRpc(
                        nullptr, req1,
                        [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
                          EXPECT_TRUE(r.ok());
                          cb1_done.Notify();
                        })
                    .ok());

    // 2. Enqueue 2 waiting RPCs into group->pending_queue.
    VirtualRpcRequest req2;
    req2.set_rpc_id(202);
    Latch cb2_done;
    ASSERT_TRUE(pool->StartVirtualRpc(
                        nullptr, req2,
                        [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
                          EXPECT_TRUE(r.ok());
                          cb2_done.Notify();
                        })
                    .ok());

    VirtualRpcRequest req3;
    req3.set_rpc_id(203);
    Latch cb3_done;
    ASSERT_TRUE(pool->StartVirtualRpc(
                        nullptr, req3,
                        [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
                          EXPECT_TRUE(r.ok());
                          cb3_done.Notify();
                        })
                    .ok());
    ASSERT_EQ(pool->pending_queue_size(), 2u);

    // 3. Complete RPC 1, draining RPC 2, then complete RPC 2 (draining RPC 3)
    // and RPC 3.
    VirtualRpcResponse resp1;
    resp1.set_rpc_id(201);
    peer_sess->CompleteRpc(201, std::chrono::milliseconds(1), resp1);
    ASSERT_TRUE(cb1_done.WaitFor(std::chrono::seconds(5)));

    VirtualRpcResponse resp2;
    resp2.set_rpc_id(202);
    peer_sess->CompleteRpc(202, std::chrono::milliseconds(1), resp2);
    ASSERT_TRUE(cb2_done.WaitFor(std::chrono::seconds(5)));

    VirtualRpcResponse resp3;
    resp3.set_rpc_id(203);
    peer_sess->CompleteRpc(203, std::chrono::milliseconds(1), resp3);
    ASSERT_TRUE(cb3_done.WaitFor(std::chrono::seconds(5)));

    ASSERT_EQ(pool->active_calls_count(), 0u);
    ASSERT_EQ(pool->pending_queue_size(), 0u);

    auto snap = pool->GetPoolSnapshot();
    ASSERT_EQ(snap.peers.size(), 1u);
    auto const& cg = snap.peers[0];
    EXPECT_EQ(cg.ref_count, 1u);
    EXPECT_EQ(cg.idle_count, 1u);
    EXPECT_EQ(cg.in_flight_count, 0u);

    pool->OnSessionClosed(peer_sess,
                          google::cloud::internal::CancelledError("closed"));
    auto snap_after_close = pool->GetPoolSnapshot();
    for (auto const& g : snap_after_close.peers) {
      EXPECT_EQ(g.ref_count, 0u);
      EXPECT_EQ(g.idle_count, 0u);
      EXPECT_EQ(g.in_flight_count, 0u);
    }
    pool->Close();
  }

  // Part 2: Fast path + OnSlotDrained must not underflow a sibling session's
  // in_flight_count.
  {
    class DrainSlotMockSession : public MockSessionImplForPool {
     public:
      bool IsCancelledAwaitingDrain(int64_t rpc_id) const override {
        return awaiting_drain_.load() == rpc_id;
      }
      void CancelPostWrite(int64_t rpc_id) {
        awaiting_drain_.store(rpc_id);
        GenericCallback cb = std::move(callback_);
        if (cb) {
          cb(google::cloud::internal::CancelledError("cancelled post-write"));
        }
      }
      void DrainSlot(int64_t rpc_id) {
        awaiting_drain_.store(0);
        is_busy_.store(false);
        active_calls_.store(0);
        if (auto l = listener().lock()) {
          l->OnSlotDrained(shared_from_this(), rpc_id);
        }
      }

     private:
      std::atomic<int64_t> awaiting_drain_{0};
    };

    google::cloud::Options options;
    options
        .set<::google::cloud::bigtable_internal::SessionsNumEventLoopsOption>(
            1);
    options
        .set<::google::cloud::bigtable_internal::SessionsInitialPoolSizeOption>(
            1);
    options.set<::google::cloud::bigtable_internal::SessionsMaxSessionsOption>(
        2);

    auto pool = SessionPoolImpl::Create(
        stubs, "table", "profile",
        google::bigtable::v2::OpenTableRequest::PERMISSION_UNSET, options,
        nullptr);

    auto connecting = SessionPoolImplTestPeer::GetConnectingSessions(*pool);
    ASSERT_EQ(connecting.size(), 1u);
    pool->OnSessionClosed(
        connecting[0], google::cloud::internal::UnavailableError("clear real"));

    auto s1 = std::make_shared<DrainSlotMockSession>();
    s1->peer_id_ = "77";
    s1->SetListener(pool);
    auto s2 = std::make_shared<DrainSlotMockSession>();
    s2->peer_id_ = "77";
    s2->SetListener(pool);

    google::bigtable::v2::OpenSessionResponse open_resp;
    pool->OnSessionReady(s1, open_resp);

    // 1. Occupy s1 via fast path so RPC 302 enqueues on group->pending_queue.
    VirtualRpcRequest req_a;
    req_a.set_rpc_id(301);
    ASSERT_TRUE(pool->StartVirtualRpc(
                        nullptr, req_a,
                        [](google::cloud::StatusOr<VirtualRpcResponse>) {})
                    .ok());

    VirtualRpcRequest req_b;
    req_b.set_rpc_id(302);
    ASSERT_TRUE(pool->StartVirtualRpc(
                        nullptr, req_b,
                        [](google::cloud::StatusOr<VirtualRpcResponse>) {})
                    .ok());
    ASSERT_EQ(pool->pending_queue_size(), 1u);

    // 2. Complete RPC 301 on s1 -> drains RPC 302 onto s1.
    VirtualRpcResponse resp_a;
    resp_a.set_rpc_id(301);
    s1->CompleteRpc(301, std::chrono::milliseconds(1), resp_a);
    ASSERT_EQ(pool->pending_queue_size(), 0u);

    // 3. Register s2 as ready and start RPC 303 on s2 via fast path.
    pool->OnSessionReady(s2, open_resp);
    VirtualRpcRequest req_c;
    req_c.set_rpc_id(303);
    ASSERT_TRUE(pool->StartVirtualRpc(
                        nullptr, req_c,
                        [](google::cloud::StatusOr<VirtualRpcResponse>) {})
                    .ok());

    // 4. Cancel RPC 303 post-write on s2 and drain its slot via OnSlotDrained
    // while s1 is still executing RPC 302.
    s2->CancelPostWrite(303);
    s2->DrainSlot(303);

    auto snap = pool->GetPoolSnapshot();
    ASSERT_EQ(snap.peers.size(), 1u);
    auto const& cg = snap.peers[0];
    EXPECT_EQ(cg.ref_count, 2u);
    EXPECT_EQ(cg.idle_count, 1u);
    EXPECT_EQ(cg.in_flight_count, 1u);

    VirtualRpcResponse resp_b;
    resp_b.set_rpc_id(302);
    s1->CompleteRpc(302, std::chrono::milliseconds(1), resp_b);
    pool->Close();
  }
}

TEST_F(SessionPoolImplTest,
       ConcurrentDrainAndFinalizeDoesNotPushDuplicateToIdleQueue) {
  class HookedAcquireMockSession : public MockSessionImplForPool {
   public:
    bool Acquire() override {
      int expected = 0;
      if (active_calls_.compare_exchange_strong(expected, 1)) {
        return true;
      }
      if (on_acquire_failed_) {
        auto fn = std::move(on_acquire_failed_);
        on_acquire_failed_ = nullptr;
        fn();
      }
      return false;
    }

    std::function<void()> on_acquire_failed_;
  };

  auto s1 = std::make_shared<HookedAcquireMockSession>();
  auto s2 = std::make_shared<HookedAcquireMockSession>();
  auto local_pool =
      SessionPoolImpl::CreateForTesting({s1, s2}, 10, std::nullopt, nullptr);
  s1->SetListener(local_pool);
  s2->SetListener(local_pool);

  // 1. Occupy both s1 and s2 via fast-path StartVirtualRpc. Note that fast-path
  // acquisition leaves s1 and s2 in group->idle_queue while active_calls_ == 1.
  VirtualRpcRequest req1;
  req1.set_rpc_id(101);
  ASSERT_TRUE(
      local_pool
          ->StartVirtualRpc(nullptr, req1,
                            [](google::cloud::StatusOr<VirtualRpcResponse>) {})
          .ok());

  VirtualRpcRequest req2;
  req2.set_rpc_id(102);
  ASSERT_TRUE(
      local_pool
          ->StartVirtualRpc(nullptr, req2,
                            [](google::cloud::StatusOr<VirtualRpcResponse>) {})
          .ok());

  // 2. Enqueue a waiting call into group->pending_queue.
  VirtualRpcRequest req_wait;
  req_wait.set_rpc_id(103);
  bool wait_done = false;
  ASSERT_TRUE(
      local_pool
          ->StartVirtualRpc(nullptr, req_wait,
                            [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
                              EXPECT_TRUE(r.ok());
                              wait_done = true;
                            })
          .ok());
  ASSERT_EQ(local_pool->pending_queue_size(), 1u);

  int s2_active_id = (s2->last_request_.rpc_id() == 102) ? 102 : 101;
  int s1_active_id = (s2_active_id == 102) ? 101 : 102;

  // 3. When TryDrainGroupPendingQueueLocked pops s2 from idle_queue and
  // s2->Acquire() fails, simulate s2's active RPC finishing concurrently.
  std::thread t_s2_finish;
  s2->on_acquire_failed_ = [&]() {
    Latch s2_finalized;
    t_s2_finish = std::thread([&]() {
      VirtualRpcResponse resp;
      resp.set_rpc_id(s2_active_id);
      s2->CompleteRpc(s2_active_id, std::chrono::milliseconds(1), resp);
      s2_finalized.Notify();
    });
    s2_finalized.Wait();
  };

  VirtualRpcResponse resp1;
  resp1.set_rpc_id(s1_active_id);
  s1->CompleteRpc(s1_active_id, std::chrono::milliseconds(1), resp1);
  if (t_s2_finish.joinable()) {
    t_s2_finish.join();
  }

  // 4. Complete req_wait on s1 and occupy s1 again so s2 is the only idle
  // session in group->idle_queue.
  VirtualRpcResponse resp_wait;
  resp_wait.set_rpc_id(103);
  s1->CompleteRpc(103, std::chrono::milliseconds(1), resp_wait);
  ASSERT_TRUE(wait_done);

  // 5. Enqueue two new pending calls while both s1 and s2 are marked busy, then
  // unblock only s2 and trigger DrainQueue via OnWriteDone(s1).
  s1->active_calls_.store(1);
  s2->active_calls_.store(1);

  VirtualRpcRequest req_a;
  req_a.set_rpc_id(201);
  ASSERT_TRUE(
      local_pool
          ->StartVirtualRpc(nullptr, req_a,
                            [](google::cloud::StatusOr<VirtualRpcResponse>) {})
          .ok());

  VirtualRpcRequest req_b;
  req_b.set_rpc_id(202);
  ASSERT_TRUE(
      local_pool
          ->StartVirtualRpc(nullptr, req_b,
                            [](google::cloud::StatusOr<VirtualRpcResponse>) {})
          .ok());
  ASSERT_EQ(local_pool->pending_queue_size(), 2u);

  s2->active_calls_.store(0);
  int const s2_starts_before = s2->start_count_.load();
  local_pool->OnWriteDone(s1);

  // s2 must be popped from idle_queue exactly once and remain in_idle_queue()
  // == false while executing req_a.
  EXPECT_EQ(s2->start_count_.load() - s2_starts_before, 1);
  EXPECT_EQ(s2->active_rpc_count(), 1);
  EXPECT_FALSE(s2->in_idle_queue());

  s1->active_calls_.store(0);
  VirtualRpcResponse resp_a;
  resp_a.set_rpc_id(201);
  s2->CompleteRpc(201, std::chrono::milliseconds(1), resp_a);
  VirtualRpcResponse resp_b;
  resp_b.set_rpc_id(202);
  s2->CompleteRpc(202, std::chrono::milliseconds(1), resp_b);
  local_pool->Close();
}

TEST_F(SessionPoolImplTest,
       ColdStartWaiterNotLostBetweenSnapshotCheckAndQueuePush) {
  auto sess =
      std::shared_ptr<MockSessionImplForPool>(new MockSessionImplForPool());
  auto local_pool =
      SessionPoolImpl::CreateForTesting({sess}, 10, std::nullopt, nullptr);
  sess->SetListener(local_pool);

  // Simulate the cold-start race window: the lock-free groups_snapshot was
  // observed empty right as OnSessionReady populated shard.connection_groups
  // and drained shard.pending_queue.
  SessionPoolImplTestPeer::ClearGroupsSnapshotOnShard0(*local_pool);

  VirtualRpcRequest req;
  req.set_rpc_id(1401);
  bool cb_done = false;
  ASSERT_TRUE(
      local_pool
          ->StartVirtualRpc(nullptr, req,
                            [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
                              EXPECT_TRUE(r.ok());
                              cb_done = true;
                            })
          .ok());

  // The call must not be stranded in shard.pending_queue when a ready session
  // already exists in shard.connection_groups.
  EXPECT_EQ(local_pool->pending_queue_size(), 0u);
  EXPECT_EQ(sess->start_count_.load(), 1);

  VirtualRpcResponse resp;
  resp.set_rpc_id(1401);
  sess->CompleteRpc(1401, std::chrono::milliseconds(1), resp);
  EXPECT_TRUE(cb_done);
  local_pool->Close();
}

TEST_F(SessionPoolImplTest,
       CancelByContextAttachesWriteStatusPayloadAndCancelsDraining) {
  auto s1 =
      std::shared_ptr<MockSessionImplForPool>(new MockSessionImplForPool());
  auto local_pool =
      SessionPoolImpl::CreateForTesting({s1}, 10, std::nullopt, nullptr);
  s1->SetListener(local_pool);

  // 1. Occupy s1 with an active RPC on ctx_active.
  grpc::ClientContext ctx_active;
  VirtualRpcRequest req_active;
  req_active.set_rpc_id(501);
  bool active_cancelled = false;
  ASSERT_TRUE(
      local_pool
          ->StartVirtualRpc(&ctx_active, req_active,
                            [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
                              EXPECT_EQ(r.status().code(),
                                        google::cloud::StatusCode::kCancelled);
                              active_cancelled = true;
                            })
          .ok());

  // 2. Enqueue a waiting non-idempotent write on ctx_queued while s1 is busy.
  grpc::ClientContext ctx_queued;
  VirtualRpcRequest req_queued;
  req_queued.set_rpc_id(502);
  google::cloud::Status queued_result;
  bool queued_done = false;
  ASSERT_TRUE(local_pool
                  ->StartVirtualRpc(
                      &ctx_queued, req_queued,
                      [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
                        queued_result = r.status();
                        queued_done = true;
                      },
                      /*is_write_request=*/true)
                  .ok());
  ASSERT_EQ(local_pool->pending_queue_size(), 1u);

  // 3. Cancel the queued write via CancelByContext before it ever leaves the
  // client queue -> must carry kWriteStatusPayloadUrl == "0".
  local_pool->CancelByContext(
      &ctx_queued,
      google::cloud::internal::DeadlineExceededError("queue deadline"));
  ASSERT_TRUE(queued_done);
  EXPECT_EQ(queued_result.code(), google::cloud::StatusCode::kDeadlineExceeded);
  EXPECT_EQ(google::cloud::internal::GetPayload(queued_result,
                                                kWriteStatusPayloadUrl),
            std::optional<std::string>("0"));

  // 4. Transition s1 to shard->draining_sessions via OnGoAway while req_active
  // is still in flight, then cancel ctx_active via CancelByContext.
  local_pool->OnGoAway(s1);
  EXPECT_FALSE(active_cancelled);

  local_pool->CancelByContext(
      &ctx_active,
      google::cloud::internal::CancelledError("caller cancelled draining rpc"));
  EXPECT_TRUE(active_cancelled);
  EXPECT_TRUE(s1->is_cancelled_.load());
  local_pool->Close();
}

TEST_F(SessionPoolImplTest, P2cSaturationFallsBackToFullGroupScan) {
  auto s1 = std::make_shared<MockSessionImplForPool>();
  auto s2 = std::make_shared<MockSessionImplForPool>();
  auto s3 = std::make_shared<MockSessionImplForPool>();
  auto s4 = std::make_shared<MockSessionImplForPool>();
  s1->peer_id_ = "1";
  s2->peer_id_ = "2";
  s3->peer_id_ = "3";
  s4->peer_id_ = "4";

  auto local_pool = SessionPoolImpl::CreateForTesting({s1, s2, s3, s4}, 10,
                                                      std::nullopt, nullptr);
  s1->SetListener(local_pool);
  s2->SetListener(local_pool);
  s3->SetListener(local_pool);
  s4->SetListener(local_pool);

  for (int wave = 0; wave < 10; ++wave) {
    for (int i = 0; i < 4; ++i) {
      VirtualRpcRequest req;
      req.set_rpc_id(wave * 10 + i + 1);
      ASSERT_TRUE(local_pool
                      ->StartVirtualRpc(
                          nullptr, req,
                          [](google::cloud::StatusOr<VirtualRpcResponse>) {})
                      .ok());
    }
    // Even when 3 of the 4 groups are saturated, the 4th RPC must fall back
    // from a saturated 2-sample P2C choice to the full group scan and dispatch
    // immediately on the remaining idle group without queueing.
    EXPECT_EQ(local_pool->pending_queue_size(), 0u);
    EXPECT_TRUE(s1->is_busy_.load());
    EXPECT_TRUE(s2->is_busy_.load());
    EXPECT_TRUE(s3->is_busy_.load());
    EXPECT_TRUE(s4->is_busy_.load());

    VirtualRpcResponse resp;
    s1->CompleteRpc(s1->last_request_.rpc_id(), std::chrono::milliseconds(1),
                    resp);
    s2->CompleteRpc(s2->last_request_.rpc_id(), std::chrono::milliseconds(1),
                    resp);
    s3->CompleteRpc(s3->last_request_.rpc_id(), std::chrono::milliseconds(1),
                    resp);
    s4->CompleteRpc(s4->last_request_.rpc_id(), std::chrono::milliseconds(1),
                    resp);
  }

  local_pool->Close();
}

TEST_F(SessionPoolImplTest,
       DrainQueueRecordsPositiveTransportLatencyAfterQueueWait) {
  auto s1 = std::make_shared<MockSessionImplForPool>();
  s1->peer_id_ = "1";
  auto local_pool =
      SessionPoolImpl::CreateForTesting({s1}, 10, std::nullopt, nullptr);
  s1->SetListener(local_pool);

  // 1. Occupy s1 with RPC 601.
  VirtualRpcRequest req1;
  req1.set_rpc_id(601);
  ASSERT_TRUE(
      local_pool
          ->StartVirtualRpc(nullptr, req1,
                            [](google::cloud::StatusOr<VirtualRpcResponse>) {})
          .ok());

  // 2. Enqueue RPC 602 while s1 is busy and wait 20ms in the pending queue so
  // queue_latency (>20ms) exceeds the subsequent wire execution time (~5ms).
  VirtualRpcRequest req2;
  req2.set_rpc_id(602);
  bool rpc2_done = false;
  ASSERT_TRUE(
      local_pool
          ->StartVirtualRpc(nullptr, req2,
                            [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
                              EXPECT_TRUE(r.ok());
                              rpc2_done = true;
                            })
          .ok());
  ASSERT_EQ(local_pool->pending_queue_size(), 1u);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  // 3. Complete RPC 601 (without backend_latency stats) to trigger DrainQueue
  // for RPC 602.
  VirtualRpcResponse resp1;
  s1->CompleteRpc(601, std::chrono::milliseconds(1), resp1);
  EXPECT_EQ(local_pool->pending_queue_size(), 0u);

  // 4. Simulate 5ms wire execution for RPC 602 with 1ms backend latency.
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  VirtualRpcResponse resp2;
  resp2.mutable_stats()->mutable_backend_latency()->set_nanos(1'000'000);
  s1->CompleteRpc(s1->last_request_.rpc_id(), std::chrono::milliseconds(5),
                  resp2);
  EXPECT_TRUE(rpc2_done);

  EXPECT_EQ(local_pool->session_tracer().vrpc_count(), 1);
  EXPECT_GT(local_pool->session_tracer().last_transport_latency_ms(), 0.0);
  auto const snap = local_pool->GetPoolSnapshot();
  ASSERT_EQ(snap.peers.size(), 1u);
  EXPECT_GT(snap.peers[0].e2e_ewma_us, 0.0);

  local_pool->Close();
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
