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

#include "google/cloud/bigtable/internal/sessions/client.h"
#include "google/cloud/bigtable/internal/sessions/latch.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_pool_impl.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_pool_impl_test_helpers.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/data.pb.h>
#include <google/bigtable/v2/request_stats.pb.h>
#include <google/bigtable/v2/session.pb.h>
#include <google/protobuf/duration.pb.h>
#include <google/rpc/status.pb.h>
#include <gmock/gmock.h>
#include <grpcpp/client_context.h>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

using ::google::bigtable::v2::SessionMutateRowRequest;
using ::google::bigtable::v2::SessionReadRowRequest;
using ::google::bigtable::v2::TableResponse;
using ::google::bigtable::v2::VirtualRpcRequest;
using ::google::bigtable::v2::VirtualRpcResponse;

class MockSessionPoolImpl : public SessionPoolImpl {
 public:
  MockSessionPoolImpl() = default;

  google::cloud::Status StartVirtualRpc(grpc::ClientContext* /*context*/,
                                        VirtualRpcRequest request,
                                        GenericCallback callback,
                                        bool /*is_write_request*/) override {
    last_request_ = std::move(request);
    start_count_++;
    if (!next_start_status_.ok()) {
      if (callback != nullptr) {
        std::move(callback)(next_start_status_);
      }
      return next_start_status_;
    }
    last_callback_ = std::move(callback);
    return google::cloud::Status();
  }

  void Close() override { is_closed_ = true; }

  std::size_t active_sessions_count() const override {
    return active_sessions_;
  }
  std::size_t pending_queue_size() const override { return pending_queue_; }

  google::cloud::Status next_start_status_ = google::cloud::Status();
  VirtualRpcRequest last_request_;
  GenericCallback last_callback_;
  int start_count_ = 0;
  std::size_t active_sessions_ = 2;
  std::size_t pending_queue_ = 0;
  bool is_closed_ = false;
};

class ClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    pool_ = std::shared_ptr<MockSessionPoolImpl>(new MockSessionPoolImpl());
    client_ = Client::CreateForTesting(pool_);
  }

  std::shared_ptr<MockSessionPoolImpl> pool_;
  std::shared_ptr<Client> client_;
};

TEST_F(ClientTest, ReadRowSuccess) {
  SessionReadRowRequest req;
  req.set_key("test_row_key");

  bool callback_invoked = false;
  client_->ReadRowAsync(
      req,
      [&](google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>
              resp) {
        callback_invoked = true;
        EXPECT_TRUE(resp.ok());
        EXPECT_EQ(resp->row().key(), "test_row_key");
      });

  EXPECT_EQ(pool_->start_count_, 1);
  EXPECT_EQ(pool_->last_request_.rpc_id(), 1);
  EXPECT_EQ(pool_->last_request_.metadata().attempt_number(), 1);
  EXPECT_TRUE(pool_->last_callback_ != nullptr);

  TableResponse table_resp;
  table_resp.mutable_read_row()->mutable_row()->set_key("test_row_key");

  VirtualRpcResponse v_resp;
  v_resp.set_rpc_id(1);
  EXPECT_TRUE(table_resp.SerializeToString(v_resp.mutable_payload()));

  auto cb = std::move(pool_->last_callback_);
  pool_->last_callback_ = nullptr;
  std::move(cb)(std::move(v_resp));
  EXPECT_TRUE(callback_invoked);
}

TEST_F(ClientTest, MutateRowSuccess) {
  SessionMutateRowRequest req;
  req.set_key("write_key");

  bool callback_invoked = false;
  client_->MutateRowAsync(req,
                          [&](google::cloud::StatusOr<
                              google::bigtable::v2::SessionMutateRowResponse>
                                  resp) {
                            callback_invoked = true;
                            EXPECT_TRUE(resp.ok());
                          });

  EXPECT_EQ(pool_->start_count_, 1);
  EXPECT_EQ(pool_->last_request_.rpc_id(), 1);

  TableResponse table_resp;
  table_resp.mutable_mutate_row();

  VirtualRpcResponse v_resp;
  v_resp.set_rpc_id(1);
  EXPECT_TRUE(table_resp.SerializeToString(v_resp.mutable_payload()));

  auto cb = std::move(pool_->last_callback_);
  pool_->last_callback_ = nullptr;
  std::move(cb)(std::move(v_resp));
  EXPECT_TRUE(callback_invoked);
}

TEST_F(ClientTest, HandlesInnerPayloadDecodingError) {
  SessionReadRowRequest req;
  bool cb_invoked = false;
  client_->ReadRowAsync(
      req,
      [&](google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>
              resp) {
        cb_invoked = true;
        EXPECT_EQ(resp.status().code(), google::cloud::StatusCode::kInternal);
      });

  VirtualRpcResponse v_resp;
  v_resp.set_payload("corrupted_protobuf_bytes");

  auto cb = std::move(pool_->last_callback_);
  pool_->last_callback_ = nullptr;
  std::move(cb)(std::move(v_resp));
  EXPECT_TRUE(cb_invoked);
}

TEST_F(ClientTest, PropagatesPoolStartFailureDirectly) {
  pool_->next_start_status_ =
      google::cloud::internal::ResourceExhaustedError("Queue full");

  SessionReadRowRequest req;
  bool cb_invoked = false;
  client_->ReadRowAsync(
      req,
      [&](google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>
              resp) {
        cb_invoked = true;
        EXPECT_EQ(resp.status().code(),
                  google::cloud::StatusCode::kResourceExhausted);
      });
  EXPECT_TRUE(cb_invoked);
  EXPECT_EQ(pool_->start_count_, 3);
}

TEST_F(ClientTest, ExposesPoolCounters) {
  EXPECT_EQ(client_->active_sessions_count(), 2u);
  EXPECT_EQ(client_->pending_queue_size(), 0u);
}

TEST_F(ClientTest, HandlesMismatchedReadRowResponseType) {
  SessionReadRowRequest req;
  bool cb_invoked = false;
  client_->ReadRowAsync(
      req,
      [&](google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>
              resp) {
        cb_invoked = true;
        EXPECT_EQ(resp.status().code(), google::cloud::StatusCode::kInternal);
      });

  google::bigtable::v2::TableResponse table_resp;
  table_resp.mutable_mutate_row();

  VirtualRpcResponse v_resp;
  EXPECT_TRUE(table_resp.SerializeToString(v_resp.mutable_payload()));

  auto cb = std::move(pool_->last_callback_);
  pool_->last_callback_ = nullptr;
  std::move(cb)(std::move(v_resp));
  EXPECT_TRUE(cb_invoked);
}

TEST_F(ClientTest, HandlesMismatchedMutateRowResponseType) {
  google::bigtable::v2::SessionMutateRowRequest req;
  bool cb_invoked = false;
  client_->MutateRowAsync(
      req, [&](google::cloud::StatusOr<
               google::bigtable::v2::SessionMutateRowResponse>
                   resp) {
        cb_invoked = true;
        EXPECT_EQ(resp.status().code(), google::cloud::StatusCode::kInternal);
      });

  google::bigtable::v2::TableResponse table_resp;
  table_resp.mutable_read_row();

  VirtualRpcResponse v_resp;
  EXPECT_TRUE(table_resp.SerializeToString(v_resp.mutable_payload()));

  auto cb = std::move(pool_->last_callback_);
  pool_->last_callback_ = nullptr;
  std::move(cb)(std::move(v_resp));
  EXPECT_TRUE(cb_invoked);
}

TEST_F(ClientTest, HandlesCloseGracefully) {
  client_->Close();
  EXPECT_TRUE(pool_->is_closed_);
}

// --- Retry / deadline hardening -------------------------------------------

TEST_F(ClientTest, SyncIdempotentCallRunsExactlyOneBoundedRetryLoop) {
  pool_->next_start_status_ =
      google::cloud::internal::UnavailableError("backend down");
  SessionMutateRowRequest req;
  auto start = std::chrono::steady_clock::now();
  auto resp = client_->MutateRow(nullptr, req, /*is_idempotent=*/true);
  EXPECT_EQ(resp.status().code(), google::cloud::StatusCode::kUnavailable);
  EXPECT_EQ(pool_->start_count_, Client::kMaxAttempts);
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(1));
  EXPECT_EQ(pool_->last_request_.metadata().attempt_number(),
            Client::kMaxAttempts);
  EXPECT_TRUE(pool_->last_request_.metadata().has_attempt_start());
  EXPECT_GT(pool_->last_request_.metadata().attempt_start().seconds(), 0);
}

TEST_F(ClientTest, SyncNonIdempotentWriteIsNotRetriedAfterLeavingClient) {
  pool_->next_start_status_ =
      google::cloud::internal::UnavailableError("stream reset");
  SessionMutateRowRequest req;
  auto resp = client_->MutateRow(nullptr, req, /*is_idempotent=*/false);
  EXPECT_EQ(resp.status().code(), google::cloud::StatusCode::kUnavailable);
  EXPECT_EQ(pool_->start_count_, 1);
}

TEST_F(ClientTest, SyncCallWithLostCallbackTimesOutAtDeadlinePlusGrace) {
  grpc::ClientContext ctx;
  auto const budget = std::chrono::milliseconds(200);
  ctx.set_deadline(std::chrono::system_clock::now() + budget);
  SessionReadRowRequest req;
  auto start = std::chrono::steady_clock::now();
  auto resp = client_->ReadRow(&ctx, req);
  auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_EQ(resp.status().code(), google::cloud::StatusCode::kDeadlineExceeded);
  EXPECT_EQ(pool_->start_count_, 1);
  EXPECT_GE(elapsed, budget - std::chrono::milliseconds(20));
  EXPECT_LT(elapsed, budget + std::chrono::seconds(4));
  if (pool_->last_callback_) {
    auto cb = std::move(pool_->last_callback_);
    std::move(cb)(google::cloud::internal::UnavailableError("late"));
  }
}

TEST_F(ClientTest, PolicyDeadlineBoundsTheVirtualRpcDeadline) {
  middleware::RetryingVRpc::CallPolicy policy;
  policy.deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
  client_->ReadRowAsync(
      nullptr, SessionReadRowRequest{},
      [](google::cloud::StatusOr<
          google::bigtable::v2::SessionReadRowResponse>) {},
      policy);
  ASSERT_EQ(pool_->start_count_, 1);
  auto const& d = pool_->last_request_.deadline();
  EXPECT_LE(d.seconds(), 5);
  EXPECT_GT(d.seconds() * 1000 + d.nanos() / 1000000, 4000);
}

TEST_F(ClientTest, NoDeadlineUsesDefaultOperationTimeout) {
  client_->ReadRowAsync(SessionReadRowRequest{},
                        [](google::cloud::StatusOr<
                            google::bigtable::v2::SessionReadRowResponse>) {});
  ASSERT_EQ(pool_->start_count_, 1);
  EXPECT_EQ(pool_->last_request_.deadline().seconds(), 60);
}

TEST_F(ClientTest, ContextDeadlineWinsWhenEarlierThanPolicy) {
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
  middleware::RetryingVRpc::CallPolicy policy;
  policy.deadline = std::chrono::system_clock::now() + std::chrono::seconds(30);
  client_->ReadRowAsync(
      &ctx, SessionReadRowRequest{},
      [](google::cloud::StatusOr<
          google::bigtable::v2::SessionReadRowResponse>) {},
      policy);
  ASSERT_EQ(pool_->start_count_, 1);
  EXPECT_LE(pool_->last_request_.deadline().seconds(), 2);
}

TEST_F(ClientTest, UserRetryPolicyIsHonored) {
  pool_->next_start_status_ =
      google::cloud::internal::UnavailableError("backend down");
  middleware::RetryingVRpc::CallPolicy policy;
  policy.on_failure = [](google::cloud::Status const&) { return false; };
  auto resp = client_->ReadRow(nullptr, SessionReadRowRequest{}, policy);
  EXPECT_EQ(resp.status().code(), google::cloud::StatusCode::kUnavailable);
  EXPECT_EQ(pool_->start_count_, 1);
}

class CountingClient : public Client {
 public:
  using Client::ReadRowAsync;

  static std::shared_ptr<CountingClient> Make() {
    return std::shared_ptr<CountingClient>(new CountingClient());
  }
  void ReadRowAsync(
      grpc::ClientContext*, SessionReadRowRequest,
      std::function<void(google::cloud::StatusOr<
                         google::bigtable::v2::SessionReadRowResponse>)>
          callback,
      middleware::RetryingVRpc::CallPolicy policy) override {
    ++read_calls;
    last_deadline = policy.deadline;
    callback(google::bigtable::v2::SessionReadRowResponse{});
  }
  int read_calls = 0;
  std::chrono::system_clock::time_point last_deadline;
};

TEST(ClientSeamTest, SyncCallsDispatchThroughVirtualAsyncExactlyOnce) {
  auto c = CountingClient::Make();
  auto resp = c->ReadRow(nullptr, SessionReadRowRequest{});
  EXPECT_TRUE(resp.ok());
  EXPECT_EQ(c->read_calls, 1);
  EXPECT_NE(c->last_deadline, (std::chrono::system_clock::time_point::max)());
  EXPECT_LE(c->last_deadline,
            std::chrono::system_clock::now() + std::chrono::seconds(61));
}

TEST(ClientSeamTest, MissingPoolFailsCleanly) {
  auto c = Client::CreateForTesting(nullptr);
  auto resp = c->ReadRow(nullptr, SessionReadRowRequest{});
  EXPECT_EQ(resp.status().code(),
            google::cloud::StatusCode::kFailedPrecondition);
}

TEST_F(ClientTest, ConcurrentAsyncCallsAllocateUniqueRpcIds) {
  constexpr int kThreads = 8;
  constexpr int kPerThread = 500;
  std::vector<std::thread> threads;
  std::mutex mu;
  std::set<std::int64_t> ids;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < kPerThread; ++i) {
        auto id = client_->AllocateRpcId();
        std::lock_guard<std::mutex> l(mu);
        ids.insert(id);
      }
    });
  }
  for (auto& th : threads) th.join();
  EXPECT_EQ(ids.size(), static_cast<std::size_t>(kThreads * kPerThread));
}

// --- Synchronous wait strategy (SyncWaiter) --------------------------------

class DelayedResponsePool : public SessionPoolImpl {
 public:
  explicit DelayedResponsePool(std::chrono::nanoseconds delay)
      : delay_(delay) {}
  ~DelayedResponsePool() override { JoinResponders(); }

  google::cloud::Status StartVirtualRpc(grpc::ClientContext* /*context*/,
                                        VirtualRpcRequest request,
                                        GenericCallback callback,
                                        bool is_write_request) override {
    std::lock_guard<std::mutex> lk(mu_);
    ++start_count_;
    google::cloud::StatusOr<VirtualRpcResponse> answer;
    if (error_.ok()) {
      TableResponse table_resp;
      if (is_write_request) {
        table_resp.mutable_mutate_row();
      } else {
        table_resp.mutable_read_row()->mutable_row()->set_key("row");
      }
      VirtualRpcResponse v_resp;
      v_resp.set_rpc_id(request.rpc_id());
      EXPECT_TRUE(table_resp.SerializeToString(v_resp.mutable_payload()));
      answer = std::move(v_resp);
    } else {
      answer = error_;
    }
    responders_.emplace_back([delay = delay_, cb = std::move(callback),
                              answer = std::move(answer)]() mutable {
      std::this_thread::sleep_for(delay);
      cb(std::move(answer));
    });
    return google::cloud::Status();
  }

  void set_error(google::cloud::Status error) {
    std::lock_guard<std::mutex> lk(mu_);
    error_ = std::move(error);
  }

  int start_count() {
    std::lock_guard<std::mutex> lk(mu_);
    return start_count_;
  }

  void JoinResponders() {
    std::vector<std::thread> responders;
    {
      std::lock_guard<std::mutex> lk(mu_);
      responders.swap(responders_);
    }
    for (auto& t : responders) t.join();
  }

 private:
  std::chrono::nanoseconds const delay_;
  std::mutex mu_;
  google::cloud::Status error_;
  int start_count_ = 0;
  std::vector<std::thread> responders_;
};

class ClientSyncWaitTest : public ::testing::Test {
 protected:
  static SyncWaiter::Config PollingConfig() {
    SyncWaiter::Config config;
    config.spin_budget = std::chrono::microseconds(500);
    config.max_spinners = 2;
    return config;
  }

  std::shared_ptr<Client> MakeClient(std::chrono::nanoseconds delay) {
    pool_ = std::make_shared<DelayedResponsePool>(delay);
    return Client::CreateForTesting(pool_, nullptr, PollingConfig(), &gate_);
  }

  void TearDown() override {
    if (pool_) pool_->JoinResponders();
  }

  SyncSpinGate gate_;
  std::shared_ptr<DelayedResponsePool> pool_;
};

TEST_F(ClientSyncWaitTest, PollingIsDisabledByDefault) {
  auto client = Client::CreateForTesting(
      std::make_shared<DelayedResponsePool>(std::chrono::microseconds(0)));
  EXPECT_FALSE(client->read_sync_waiter().enabled());
  EXPECT_FALSE(client->write_sync_waiter().enabled());
}

TEST_F(ClientSyncWaitTest, SyncReadsReturnResultsAndTrainTheReadPredictor) {
  auto client = MakeClient(std::chrono::milliseconds(2));
  ASSERT_TRUE(client->read_sync_waiter().enabled());
  constexpr int kCalls = 5;
  for (int i = 0; i != kCalls; ++i) {
    auto resp = client->ReadRow(nullptr, SessionReadRowRequest{});
    ASSERT_TRUE(resp.ok()) << resp.status();
    EXPECT_EQ(resp->row().key(), "row");
  }
  EXPECT_EQ(pool_->start_count(), kCalls);
  EXPECT_GE(client->read_sync_waiter().predicted_latency(),
            std::chrono::milliseconds(1));
  EXPECT_EQ(client->write_sync_waiter().predicted_latency(),
            std::chrono::nanoseconds(0));
  EXPECT_EQ(gate_.active(), 0);
  auto const stats = client->read_sync_waiter().stats();
  EXPECT_GE(stats.early + stats.spin_hits + stats.spin_misses + stats.gate_full,
            1U);
}

TEST_F(ClientSyncWaitTest, SyncWritesTrainOnlyTheWritePredictor) {
  auto client = MakeClient(std::chrono::milliseconds(1));
  for (int i = 0; i != 3; ++i) {
    auto resp = client->MutateRow(nullptr, SessionMutateRowRequest{},
                                  /*is_idempotent=*/true);
    ASSERT_TRUE(resp.ok()) << resp.status();
  }
  EXPECT_GT(client->write_sync_waiter().predicted_latency(),
            std::chrono::nanoseconds(0));
  EXPECT_EQ(client->read_sync_waiter().predicted_latency(),
            std::chrono::nanoseconds(0));
  EXPECT_EQ(gate_.active(), 0);
}

TEST_F(ClientSyncWaitTest, FailedCallsDoNotTrainThePredictor) {
  auto client = MakeClient(std::chrono::milliseconds(1));
  pool_->set_error(
      google::cloud::internal::InvalidArgumentError("malformed row key"));
  auto resp = client->ReadRow(nullptr, SessionReadRowRequest{});
  EXPECT_EQ(resp.status().code(), google::cloud::StatusCode::kInvalidArgument);
  EXPECT_EQ(pool_->start_count(), 1);
  EXPECT_EQ(client->read_sync_waiter().predicted_latency(),
            std::chrono::nanoseconds(0));
  EXPECT_EQ(gate_.active(), 0);
}

TEST_F(ClientSyncWaitTest, LostCallbackStillTimesOutWithPollingEnabled) {
  auto pool = std::shared_ptr<MockSessionPoolImpl>(new MockSessionPoolImpl());
  auto client =
      Client::CreateForTesting(pool, nullptr, PollingConfig(), &gate_);
  grpc::ClientContext ctx;
  auto const budget = std::chrono::milliseconds(200);
  ctx.set_deadline(std::chrono::system_clock::now() + budget);
  auto const start = std::chrono::steady_clock::now();
  auto resp = client->ReadRow(&ctx, SessionReadRowRequest{});
  auto const elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_EQ(resp.status().code(), google::cloud::StatusCode::kDeadlineExceeded);
  EXPECT_GE(elapsed, budget - std::chrono::milliseconds(20));
  EXPECT_LT(elapsed, budget + std::chrono::seconds(4));
  EXPECT_EQ(client->read_sync_waiter().stats().spin_misses, 1U);
  EXPECT_EQ(gate_.active(), 0);
  EXPECT_EQ(client->read_sync_waiter().predicted_latency(),
            std::chrono::nanoseconds(0));
  if (pool->last_callback_) {
    auto cb = std::move(pool->last_callback_);
    std::move(cb)(google::cloud::internal::UnavailableError("late"));
  }
}

TEST(ClientCrossShardRpcIdTest, SiblingClientsSharingPoolNeverCollideOnRpcId) {
  auto sess1 = std::make_shared<MockSessionImplForPool>();
  sess1->peer_id_ = "peer_a";
  sess1->state_ = SessionImpl::State::kReady;

  auto sess2 = std::make_shared<MockSessionImplForPool>();
  sess2->peer_id_ = "peer_b";
  sess2->state_ = SessionImpl::State::kReady;

  auto pool = SessionPoolImpl::CreateForTesting({sess1, sess2});
  sess1->SetListener(pool);
  sess2->SetListener(pool);
  auto shard0 = Client::CreateForTesting(pool);
  auto shard1 = Client::CreateForTesting(pool);

  std::optional<
      google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>>
      result_a;
  std::optional<
      google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>>
      result_b;

  SessionReadRowRequest req_a;
  req_a.set_key("row-from-shard-0");
  shard0->ReadRowAsync(req_a, [&](auto resp) { result_a = std::move(resp); });

  SessionReadRowRequest req_b;
  req_b.set_key("row-from-shard-1");
  shard1->ReadRowAsync(req_b, [&](auto resp) { result_b = std::move(resp); });

  EXPECT_EQ(sess1->start_count_.load() + sess2->start_count_.load(), 2);
  EXPECT_NE(sess1->last_request_.rpc_id(), sess2->last_request_.rpc_id());

  for (auto const& sess : {sess1, sess2}) {
    TableResponse table_resp;
    table_resp.mutable_read_row()->mutable_row()->set_key(
        sess == sess1 ? "row-from-shard-0" : "row-from-shard-1");
    VirtualRpcResponse v_resp;
    v_resp.set_rpc_id(sess->last_request_.rpc_id());
    ASSERT_TRUE(table_resp.SerializeToString(v_resp.mutable_payload()));
    sess->CompleteRpc(sess->last_request_.rpc_id(),
                      std::chrono::nanoseconds::zero(), std::move(v_resp));
  }

  ASSERT_TRUE(result_a.has_value());
  ASSERT_TRUE(result_b.has_value());
  ASSERT_TRUE(result_a->ok()) << result_a->status();
  ASSERT_TRUE(result_b->ok()) << result_b->status();
}

class TimingMockSession : public MockSessionImplForPool {
 public:
  explicit TimingMockSession(std::string peer_id = "peer1") {
    peer_id_ = std::move(peer_id);
  }

  std::function<void()> on_acquire_;
  std::function<void()> on_start_rpc_;
  std::function<void(grpc::ClientContext*)> on_has_active_context_;

  bool Acquire() override {
    if (!MockSessionImplForPool::Acquire()) return false;
    if (on_acquire_) on_acquire_();
    return true;
  }

  void StartVirtualRpc(grpc::ClientContext* context, VirtualRpcRequest request,
                       GenericCallback callback,
                       bool is_write_request) override {
    MockSessionImplForPool::StartVirtualRpc(
        context, std::move(request), std::move(callback), is_write_request);
    if (on_start_rpc_) on_start_rpc_();
  }

  bool HasActiveContext(grpc::ClientContext* ctx) const override {
    if (on_has_active_context_) on_has_active_context_(ctx);
    return MockSessionImplForPool::HasActiveContext(ctx);
  }
};

TEST(ClientCloseGracefullyTest,
     CloseGracefullyWaitsForInFlightRetryLoopToDrainBeforeReturning) {
  auto sess = std::make_shared<TimingMockSession>("peer1");
  sess->state_ = SessionImpl::State::kReady;

  auto pool = SessionPoolImpl::CreateForTesting({sess});
  sess->SetListener(pool);
  auto client = Client::CreateForTesting(pool);

  Latch rpc_started_in_session;
  sess->on_start_rpc_ = [&]() { rpc_started_in_session.Notify(); };

  Latch callback_done;
  google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>
      async_result;

  grpc::ClientContext ctx;
  SessionReadRowRequest req;
  req.set_key("row-1");
  client->ReadRowAsync(&ctx, req, [&](auto resp) {
    async_result = std::move(resp);
    callback_done.Notify();
  });

  rpc_started_in_session.Wait();
  ASSERT_EQ(pool->active_calls_count(), 1U);

  std::thread backend_completion([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    ASSERT_NE(sess->callback_, nullptr);
    TableResponse table_resp;
    table_resp.mutable_read_row()->mutable_row()->set_key("row-1");
    VirtualRpcResponse v_resp;
    v_resp.set_rpc_id(sess->last_request_.rpc_id());
    ASSERT_TRUE(table_resp.SerializeToString(v_resp.mutable_payload()));
    sess->CompleteRpc(sess->last_request_.rpc_id(),
                      std::chrono::nanoseconds::zero(), std::move(v_resp));
  });

  auto close_status = client->CloseGracefully(std::chrono::seconds(2));
  backend_completion.join();
  callback_done.Wait();

  EXPECT_TRUE(close_status.ok()) << close_status;
  EXPECT_TRUE(async_result.ok()) << async_result.status();
}

#if defined(__GNUC__) || defined(__clang__)
#define SESSIONS_TEST_NOINLINE __attribute__((noinline))
#else
#define SESSIONS_TEST_NOINLINE
#endif

SESSIONS_TEST_NOINLINE
google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>
CallSyncReadRowWithStackContext(
    Client& client, std::atomic<grpc::ClientContext*>& published_ctx,
    std::atomic<bool>& ctx_alive, Latch* ctx_published,
    std::string const& key) {
  struct ScopedAlive {
    std::atomic<bool>& flag;
    explicit ScopedAlive(std::atomic<bool>& f) : flag(f) {
      flag.store(true, std::memory_order_release);
    }
    ~ScopedAlive() { flag.store(false, std::memory_order_release); }
  };
  grpc::ClientContext stack_ctx;
  ScopedAlive alive(ctx_alive);
  published_ctx.store(&stack_ctx, std::memory_order_release);
  if (ctx_published != nullptr && !ctx_published->HasBeenNotified()) {
    ctx_published->Notify();
  }
  SessionReadRowRequest req;
  req.set_key(key);
  return client.ReadRow(&stack_ctx, std::move(req));
}

TEST(ClientSyncCancelContextLifetimeTest,
     CancelContextCompletesPoolScanBeforeWakeAndNoCrossCancel) {
  auto sess1 = std::make_shared<TimingMockSession>("peer1");
  sess1->state_ = SessionImpl::State::kReady;

  auto sess2 = std::make_shared<TimingMockSession>("peer2");
  sess2->state_ = SessionImpl::State::kReady;

  auto pool = SessionPoolImpl::CreateForTesting({sess1, sess2});
  sess1->SetListener(pool);
  sess2->SetListener(pool);
  auto client = Client::CreateForTesting(pool);

  std::atomic<grpc::ClientContext*> call1_ctx{nullptr};
  std::atomic<grpc::ClientContext*> call2_ctx{nullptr};
  std::atomic<bool> call1_ctx_alive{false};
  std::atomic<bool> call2_ctx_alive{false};
  Latch call1_started_in_session;
  Latch call2_started_in_session;
  std::atomic<bool> ctx_was_alive_during_pool_scan{false};

  std::atomic<int> start_count{0};
  auto on_start = [&]() {
    int n = start_count.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (n == 1 && !call1_started_in_session.HasBeenNotified()) {
      call1_started_in_session.Notify();
    } else if (n == 2 && !call2_started_in_session.HasBeenNotified()) {
      call2_started_in_session.Notify();
    }
  };
  sess1->on_start_rpc_ = on_start;
  sess2->on_start_rpc_ = on_start;
  auto on_has_ctx = [&](grpc::ClientContext* queried_ctx) {
    if (queried_ctx == call1_ctx.load(std::memory_order_acquire)) {
      ctx_was_alive_during_pool_scan.store(
          call1_ctx_alive.load(std::memory_order_acquire),
          std::memory_order_release);
    }
  };
  sess1->on_has_active_context_ = on_has_ctx;
  sess2->on_has_active_context_ = on_has_ctx;

  google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse> resp1;
  google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse> resp2;

  std::thread caller_thread([&]() {
    resp1 = CallSyncReadRowWithStackContext(*client, call1_ctx, call1_ctx_alive,
                                            nullptr, "row-1");
    resp2 = CallSyncReadRowWithStackContext(*client, call2_ctx, call2_ctx_alive,
                                            nullptr, "row-2");
  });

  call1_started_in_session.Wait();
  ASSERT_NE(call1_ctx.load(std::memory_order_acquire), nullptr);

  client->CancelContext(call1_ctx.load(std::memory_order_acquire));

  call2_started_in_session.Wait();
  for (auto const& s : {sess1, sess2}) {
    if (s->callback_) {
      TableResponse table_resp;
      table_resp.mutable_read_row()->mutable_row()->set_key("row-2");
      VirtualRpcResponse v_resp;
      v_resp.set_rpc_id(s->last_request_.rpc_id());
      ASSERT_TRUE(table_resp.SerializeToString(v_resp.mutable_payload()));
      s->CompleteRpc(s->last_request_.rpc_id(),
                     std::chrono::nanoseconds::zero(), std::move(v_resp));
    }
  }

  caller_thread.join();

  EXPECT_EQ(resp1.status().code(), google::cloud::StatusCode::kCancelled);
  EXPECT_TRUE(ctx_was_alive_during_pool_scan.load(std::memory_order_acquire));
  EXPECT_TRUE(resp2.ok()) << resp2.status();
}

TEST(ClientSyncCancelContextLifetimeTest,
     CancelContextDuringStartAttemptCancelsSessionAndReleasesPoolSlot) {
  auto sess_leak = std::make_shared<TimingMockSession>("peer-leak");
  sess_leak->state_ = SessionImpl::State::kReady;

  std::atomic<int> acquire_count{0};
  Latch attempt2_acquired_session;
  Latch cancel_and_caller_finished;

  sess_leak->on_acquire_ = [&]() {
    int n = acquire_count.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (n == 2) {
      attempt2_acquired_session.Notify();
      cancel_and_caller_finished.Wait();
    }
  };

  std::mutex retry_mu;
  std::vector<std::thread> retry_threads;
  auto scheduler_leak = [&](std::chrono::nanoseconds,
                            std::function<void()> task) {
    std::lock_guard<std::mutex> l(retry_mu);
    retry_threads.emplace_back(std::move(task));
  };

  auto pool_leak = SessionPoolImpl::CreateForTesting({sess_leak});
  sess_leak->SetListener(pool_leak);
  auto client_leak = Client::CreateForTesting(pool_leak, scheduler_leak);

  std::atomic<int> start_rpc_count{0};
  sess_leak->on_start_rpc_ = [&]() {
    int n = start_rpc_count.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (n == 1) {
      sess_leak->CompleteRpc(
          sess_leak->last_request_.rpc_id(), std::chrono::nanoseconds::zero(),
          google::cloud::internal::UnavailableError("transient attempt 1"));
    }
  };

  std::atomic<grpc::ClientContext*> leak_ctx{nullptr};
  std::atomic<bool> leak_ctx_alive{false};
  google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>
      leak_resp;

  std::thread caller_thread([&]() {
    leak_resp = CallSyncReadRowWithStackContext(
        *client_leak, leak_ctx, leak_ctx_alive, nullptr, "row-leak");
  });

  attempt2_acquired_session.Wait();
  client_leak->CancelContext(leak_ctx.load(std::memory_order_acquire));
  caller_thread.join();

  EXPECT_FALSE(leak_ctx_alive.load(std::memory_order_acquire));
  cancel_and_caller_finished.Notify();
  std::vector<std::thread> threads_to_join;
  {
    std::lock_guard<std::mutex> l(retry_mu);
    threads_to_join.swap(retry_threads);
  }
  for (auto& t : threads_to_join) {
    if (t.joinable()) t.join();
  }

  EXPECT_EQ(leak_resp.status().code(), google::cloud::StatusCode::kCancelled);
  EXPECT_EQ(pool_leak->active_calls_count(), 0U);
  EXPECT_EQ(sess_leak->active_rpc_count(), 0);
  EXPECT_TRUE(sess_leak->is_cancelled_.load());
  EXPECT_EQ(sess_leak->context_, nullptr);
}

TEST(ClientNullContextTimeoutTest,
     NullContextSyncReadRowTimeoutCancelsInFlightSessionRpc) {
  auto sess = std::make_shared<TimingMockSession>("peer1");
  sess->state_ = SessionImpl::State::kReady;

  auto pool = SessionPoolImpl::CreateForTesting({sess});
  sess->SetListener(pool);
  auto client = Client::CreateForTesting(pool);

  SessionReadRowRequest req;
  req.set_key("row-timeout-active");

  middleware::RetryingVRpc::CallPolicy policy;
  policy.deadline =
      std::chrono::system_clock::now() + std::chrono::milliseconds(50);

  auto resp = client->ReadRow(nullptr, req, std::move(policy));
  EXPECT_EQ(resp.status().code(), google::cloud::StatusCode::kDeadlineExceeded);
  EXPECT_EQ(pool->active_calls_count(), 0U);
  EXPECT_EQ(sess->active_rpc_count(), 0);
  EXPECT_TRUE(sess->is_cancelled_.load());
}

TEST(ClientNullContextTimeoutTest,
     ContextFreeSyncReadRowTimeoutEvictsPendingCallFromPoolQueue) {
  auto busy_sess = std::make_shared<TimingMockSession>("peer1");
  busy_sess->state_ = SessionImpl::State::kReady;
  busy_sess->active_calls_.store(1);
  busy_sess->is_busy_.store(true);

  auto pool = SessionPoolImpl::CreateForTesting({busy_sess});
  busy_sess->SetListener(pool);
  auto client = Client::CreateForTesting(pool);

  SessionReadRowRequest req;
  req.set_key("row-timeout-queued");

  middleware::RetryingVRpc::CallPolicy policy;
  policy.deadline =
      std::chrono::system_clock::now() + std::chrono::milliseconds(50);

  auto resp = client->ReadRow(nullptr, req, std::move(policy));
  EXPECT_EQ(resp.status().code(), google::cloud::StatusCode::kDeadlineExceeded);
  EXPECT_EQ(pool->pending_queue_size(), 0U);
  EXPECT_EQ(pool->active_calls_count(), 0U);
}

TEST(ClientCancelAndContextReuseTest,
     PreRegistrationCancelContextCancelsOperationImmediately) {
  auto pool = std::shared_ptr<MockSessionPoolImpl>(new MockSessionPoolImpl());
  auto client = Client::CreateForTesting(pool);

  grpc::ClientContext ctx;
  ctx.TryCancel();
  client->CancelContext(&ctx);

  Latch done;
  google::cloud::StatusOr<google::bigtable::v2::SessionMutateRowResponse>
      mutate_result;

  SessionMutateRowRequest req;
  req.set_key("row-pre-cancelled");
  client->MutateRowAsync(
      &ctx, req,
      [&](google::cloud::StatusOr<
          google::bigtable::v2::SessionMutateRowResponse>
              resp) {
        mutate_result = std::move(resp);
        done.Notify();
      },
      /*is_idempotent=*/false);

  EXPECT_TRUE(done.WaitFor(std::chrono::milliseconds(250)));
  EXPECT_EQ(pool->start_count_, 0);
  EXPECT_EQ(mutate_result.status().code(),
            google::cloud::StatusCode::kCancelled);
}

class MultiCallRecordingPool : public SessionPoolImpl {
 public:
  google::cloud::Status StartVirtualRpc(grpc::ClientContext* context,
                                        VirtualRpcRequest request,
                                        GenericCallback callback,
                                        bool /*is_write_request*/) override {
    int idx = start_count_.fetch_add(1, std::memory_order_acq_rel);
    {
      std::lock_guard<std::mutex> l(mu_);
      requests_.push_back(request);
      contexts_.push_back(context);
      callbacks_.push_back(std::move(callback));
    }
    if (idx == 0) {
      call1_started_.Notify();
    } else if (idx == 1) {
      call2_started_.Notify();
    }
    return google::cloud::Status();
  }

  void CompleteCall(std::size_t idx,
                    google::cloud::StatusOr<VirtualRpcResponse> resp) {
    GenericCallback cb;
    {
      std::lock_guard<std::mutex> l(mu_);
      if (idx < callbacks_.size()) {
        cb = std::move(callbacks_[idx]);
      }
    }
    if (cb) std::move(cb)(std::move(resp));
  }

  std::mutex mu_;
  std::atomic<int> start_count_{0};
  std::vector<VirtualRpcRequest> requests_;
  std::vector<grpc::ClientContext*> contexts_;
  std::vector<GenericCallback> callbacks_;
  Latch call1_started_;
  Latch call2_started_;
};

struct ReentrantCancelGuard {
  std::shared_ptr<Client> client;
  grpc::ClientContext* ctx = nullptr;
  std::shared_ptr<std::atomic<bool>> completed;

  ~ReentrantCancelGuard() {
    if (client && ctx && completed) {
      client->CancelContext(ctx);
      completed->store(true, std::memory_order_release);
    }
  }
};

TEST(ClientCancelAndContextReuseTest,
     SharedClientContextPreservesBothRetryLoopsAndDoesNotSelfDeadlock) {
  auto pool =
      std::shared_ptr<MultiCallRecordingPool>(new MultiCallRecordingPool());
  auto client = Client::CreateForTesting(pool);

  grpc::ClientContext shared_ctx;

  Latch call1_done;
  google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>
      call1_resp;
  SessionReadRowRequest req1;
  req1.set_key("row-call-1");
  client->ReadRowAsync(&shared_ctx, req1, [&](auto resp) {
    call1_resp = std::move(resp);
    call1_done.Notify();
  });
  pool->call1_started_.Wait();

  auto destructor_completed = std::make_shared<std::atomic<bool>>(false);
  auto guard = std::make_shared<ReentrantCancelGuard>(
      ReentrantCancelGuard{client, &shared_ctx, destructor_completed});
  middleware::RetryingVRpc::CallPolicy policy2;
  policy2.on_failure = [guard](google::cloud::Status const&) { return false; };
  guard.reset();

  Latch call2_done;
  google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>
      call2_resp;
  SessionReadRowRequest req2;
  req2.set_key("row-call-2");
  client->ReadRowAsync(
      &shared_ctx, req2,
      [&](auto resp) {
        call2_resp = std::move(resp);
        call2_done.Notify();
      },
      std::move(policy2));
  pool->call2_started_.Wait();

  TableResponse t_resp2;
  t_resp2.mutable_read_row()->mutable_row()->set_key("row-call-2");
  VirtualRpcResponse v_resp2;
  v_resp2.set_rpc_id(2);
  ASSERT_TRUE(t_resp2.SerializeToString(v_resp2.mutable_payload()));
  pool->CompleteCall(1, std::move(v_resp2));

  ASSERT_TRUE(call2_done.WaitFor(std::chrono::seconds(1)));
  ASSERT_TRUE(call2_resp.ok()) << call2_resp.status();
  EXPECT_TRUE(destructor_completed->load(std::memory_order_acquire));

  EXPECT_TRUE(call1_done.WaitFor(std::chrono::seconds(1)));
  EXPECT_EQ(call1_resp.status().code(), google::cloud::StatusCode::kCancelled);
}

TEST_F(ClientTest,
       PreservesClusterInfoOnVirtualRpcResponseAndAttemptTelemetry) {
  SessionReadRowRequest req;
  req.set_key("row-cluster-info");

  middleware::RetryingVRpc::AttemptTelemetry captured_telemetry;
  bool telemetry_called = false;
  middleware::RetryingVRpc::CallPolicy policy;
  policy.on_attempt_complete =
      [&](middleware::RetryingVRpc::AttemptTelemetry const& t) {
        telemetry_called = true;
        captured_telemetry = t;
      };

  Latch done;
  google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>
      read_resp;
  client_->ReadRowAsync(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::SessionReadRowResponse>
              resp) {
        read_resp = std::move(resp);
        done.Notify();
      },
      std::move(policy));

  ASSERT_NE(pool_->last_callback_, nullptr);
  TableResponse table_resp;
  table_resp.mutable_read_row()->mutable_row()->set_key("row-cluster-info");
  VirtualRpcResponse v_resp;
  v_resp.set_rpc_id(pool_->last_request_.rpc_id());
  v_resp.mutable_cluster_info()->set_cluster_id("cluster-a");
  v_resp.mutable_cluster_info()->set_zone_id("us-central1-a");
  ASSERT_TRUE(table_resp.SerializeToString(v_resp.mutable_payload()));
  std::move(pool_->last_callback_)(std::move(v_resp));

  ASSERT_TRUE(done.WaitFor(std::chrono::seconds(1)));
  ASSERT_TRUE(read_resp.ok()) << read_resp.status();
  EXPECT_TRUE(telemetry_called);
  EXPECT_EQ(captured_telemetry.cluster_id, "cluster-a");
  EXPECT_EQ(captured_telemetry.zone_id, "us-central1-a");
  ASSERT_TRUE(client_->cluster_info().has_value());
  EXPECT_EQ(client_->cluster_info()->cluster_id(), "cluster-a");
  EXPECT_EQ(client_->cluster_info()->zone_id(), "us-central1-a");
}

TEST(ClientViewCreateTest,
     CreateOverloadsSupportAuthorizedViewAndMaterializedViewPermissions) {
  auto av_client = Client::Create(
      {}, "projects/p/instances/i/tables/t/authorizedViews/av1", "default",
      google::bigtable::v2::OpenAuthorizedViewRequest::PERMISSION_READ_WRITE,
      google::cloud::Options{});
  ASSERT_NE(av_client, nullptr);
  ASSERT_NE(av_client->pool(), nullptr);
  EXPECT_EQ(av_client->pool()->session_tracer().config().session_type,
            "SESSION_TYPE_AUTHORIZED_VIEW");
  av_client->Close();

  auto mv_client = Client::Create(
      {}, "projects/p/instances/i/materializedViews/mv1", "default",
      google::bigtable::v2::OpenMaterializedViewRequest::PERMISSION_READ,
      google::cloud::Options{});
  ASSERT_NE(mv_client, nullptr);
  ASSERT_NE(mv_client->pool(), nullptr);
  EXPECT_EQ(mv_client->pool()->session_tracer().config().session_type,
            "SESSION_TYPE_MATERIALIZED_VIEW");
  mv_client->Close();
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
