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

#include "google/cloud/bigtable/internal/sessions/session/session_impl.h"
#include "google/cloud/bigtable/internal/sessions/latch.h"
#include "google/cloud/bigtable/internal/sessions/session/write_status.h"
#include "google/cloud/bigtable/internal/sessions/transport/current_cq.h"
#include "google/cloud/bigtable/internal/sessions/transport/session_stream.h"
#include "google/cloud/completion_queue.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/escaping.h"
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/bigtable/v2/peer_info.pb.h>
#include <google/bigtable/v2/session.pb.h>
#include <google/protobuf/duration.pb.h>
#include <google/rpc/status.pb.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

class SessionImplTestPeer {
 public:
  static void RunAsync(SessionImpl& session, std::function<void()> task) {
    session.RunAsync(std::move(task));
  }
  static void SetState(SessionImpl& session, SessionImpl::State state) {
    std::lock_guard<std::mutex> l(session.state_mutex_);
    session.state_.store(state, std::memory_order_relaxed);
  }
  static std::optional<google::cloud::CompletionQueue> GetEventCq(
      SessionImpl& session) {
    return session.event_cq_;
  }
  static int64_t GetLastHeartbeatTimeUs(SessionImpl const& session) {
    return session.last_heartbeat_time_ns_.load(std::memory_order_relaxed) /
           1000;
  }
  static void SetLastHeartbeatTimeUs(SessionImpl& session, int64_t us) {
    session.last_heartbeat_time_ns_.store(us * 1000, std::memory_order_relaxed);
  }
  static void SetDisconnectStatus(SessionImpl& session,
                                  google::cloud::Status status) {
    std::lock_guard<std::mutex> l(session.state_mutex_);
    session.disconnect_status_ = std::move(status);
  }
  static google::cloud::Status GetDisconnectStatus(SessionImpl const& session) {
    std::lock_guard<std::mutex> l(session.state_mutex_);
    return session.disconnect_status_;
  }
};

namespace {

using ::google::bigtable::v2::SessionRequest;
using ::google::bigtable::v2::SessionResponse;
using ::google::bigtable::v2::VirtualRpcRequest;
using ::google::bigtable::v2::VirtualRpcResponse;

class MockSessionStream : public SessionStream {
 public:
  void OnInitialMetadata(
      std::multimap<grpc::string_ref, grpc::string_ref> const& metadata) {}

  google::cloud::Status Write(
      google::bigtable::v2::SessionRequest request) override {
    std::lock_guard<std::mutex> l(mu_);
    write_count_++;
    last_written_request_ = std::move(request);
    cv_.notify_all();

    if (!defer_write_done_) {
      if (auto listener = listener_.lock()) {
        listener->OnWriteDone(next_write_status_.ok());
      }
    }
    return next_write_status_;
  }

  void Close(google::bigtable::v2::CloseSessionRequest::CloseSessionReason
                 reason) override {
    std::lock_guard<std::mutex> l(mu_);
    is_closed_ = true;
    close_reason_ = reason;
    cv_.notify_all();
  }

  void Cancel() override {
    std::lock_guard<std::mutex> l(mu_);
    is_cancelled_ = true;
    cv_.notify_all();
  }

  void WaitForWrite(int expected) {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this, expected] { return write_count_ >= expected; });
  }

  void WaitForClose() {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this] { return is_closed_; });
  }

  void WaitForCancel() {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this] { return is_cancelled_; });
  }

  void set_write_in_flight(bool v) {
    write_in_flight_.store(v, std::memory_order_release);
  }

  std::mutex mu_;
  std::condition_variable cv_;
  int write_count_ = 0;
  google::bigtable::v2::SessionRequest last_written_request_;
  google::cloud::Status next_write_status_ = google::cloud::Status();
  bool defer_write_done_ = false;
  bool is_closed_ = false;
  bool is_cancelled_ = false;
  google::bigtable::v2::CloseSessionRequest::CloseSessionReason close_reason_;
};

class MockSessionListener : public SessionListener {
 public:
  void OnSessionReady(
      std::shared_ptr<SessionImpl> session,
      google::bigtable::v2::OpenSessionResponse const& response) override {
    std::lock_guard<std::mutex> l(mu_);
    is_ready_ = true;
    last_session_ = session;
    cv_.notify_all();
  }

  void OnGoAway(std::shared_ptr<SessionImpl> session) override {
    std::lock_guard<std::mutex> l(mu_);
    last_session_ = session;
    go_away_count_++;
    cv_.notify_all();
  }

  void OnSessionClosed(std::shared_ptr<SessionImpl> session,
                       google::cloud::Status status) override {
    std::lock_guard<std::mutex> l(mu_);
    last_session_ = session;
    is_closed_ = true;
    close_status_ = status;
    cv_.notify_all();
  }

  int GetActiveCallsForStub(
      google::bigtable::v2::Bigtable::Stub const* stub) const override {
    return 0;
  }

  void OnSlotDrained(std::shared_ptr<SessionImpl> session,
                     int64_t rpc_id) override {
    std::lock_guard<std::mutex> l(mu_);
    last_session_ = std::move(session);
    last_drained_rpc_id_ = rpc_id;
    slot_drained_count_++;
    cv_.notify_all();
  }

  void OnWriteDone(std::shared_ptr<SessionImpl> session) override {
    std::lock_guard<std::mutex> l(mu_);
    last_session_ = std::move(session);
    write_done_count_++;
    cv_.notify_all();
  }

  void WaitForSlotDrained(int expected) {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this, expected] { return slot_drained_count_ >= expected; });
  }

  void WaitForReady() {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this] { return is_ready_; });
  }

  void WaitForGoAway(int expected) {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this, expected] { return go_away_count_ >= expected; });
  }

  void WaitForClosed() {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this] { return is_closed_; });
  }

  std::mutex mu_;
  std::condition_variable cv_;
  std::shared_ptr<SessionImpl> last_session_;
  bool is_ready_ = false;
  bool is_closed_ = false;
  int go_away_count_ = 0;
  int slot_drained_count_ = 0;
  int write_done_count_ = 0;
  int64_t last_drained_rpc_id_ = 0;
  google::cloud::Status close_status_;
};

class SessionImplTest : public ::testing::Test {
 protected:
  void SetUp() override {
    stream_ = std::shared_ptr<MockSessionStream>(new MockSessionStream());
    listener_ = std::make_shared<MockSessionListener>();
    cq_thread_ = std::thread([this]() { cq_.Run(); });
    session_ = SessionImpl::CreateForTesting(
        stream_, listener_, cq_,
        [](std::chrono::nanoseconds delay, std::function<void()> task) {});
  }

  void TearDown() override {
    SessionImpl::SetKeepaliveTimeoutMultiplier(3.0);
    SessionImpl::SetGoAwayDrainTimeout(std::chrono::seconds(30));
    cq_.Shutdown();
    if (cq_thread_.joinable()) {
      cq_thread_.join();
    }
  }

  google::cloud::CompletionQueue cq_;
  std::thread cq_thread_;
  std::shared_ptr<MockSessionStream> stream_;
  std::shared_ptr<MockSessionListener> listener_;
  std::shared_ptr<SessionImpl> session_;
};

TEST_F(SessionImplTest, StartVirtualRpcSuccess) {
  VirtualRpcRequest req;
  req.set_rpc_id(42);
  req.set_payload("test_payload");

  bool called = false;
  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) { called = true; });
  stream_->WaitForWrite(1);
  EXPECT_TRUE(stream_->last_written_request_.has_virtual_rpc());
  EXPECT_EQ(stream_->last_written_request_.virtual_rpc().rpc_id(), 42);
  EXPECT_FALSE(called);
}

TEST_F(SessionImplTest, EnforcesMaxOpLimit) {
  for (int i = 0; i < SessionImpl::kMaxConcurrentRpcsPerSession; ++i) {
    EXPECT_TRUE(session_->Acquire());
  }
  EXPECT_FALSE(session_->Acquire());
}

TEST_F(SessionImplTest, RoutesVirtualRpcResponseCorrectly) {
  VirtualRpcRequest req;
  req.set_rpc_id(100);

  google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
      received_response;
  bool called = false;

  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        called = true;
        received_response = std::move(resp);
      });
  stream_->WaitForWrite(1);

  SessionResponse resp;
  auto* vresp = resp.mutable_virtual_rpc();
  vresp->set_rpc_id(100);
  vresp->set_payload("success_bytes");

  session_->OnMessage(resp);

  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  EXPECT_TRUE(called);
  EXPECT_TRUE(received_response.ok());
  EXPECT_EQ(received_response->payload(), "success_bytes");
}

TEST_F(SessionImplTest, IgnoresMismatchedRpcResponse) {
  VirtualRpcRequest req;
  req.set_rpc_id(50);

  bool called = false;
  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) { called = true; });
  stream_->WaitForWrite(1);

  SessionResponse resp;
  resp.mutable_virtual_rpc()->set_rpc_id(999);
  session_->OnMessage(resp);

  // Flush executor (though OnMessage runs synchronously, we want to be sure)
  Latch flushed;
  SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
  flushed.Wait();

  EXPECT_FALSE(called);
}

TEST_F(SessionImplTest, RoutesErrorResponseCorrectly) {
  VirtualRpcRequest req;
  req.set_rpc_id(77);

  google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
      received_response;
  bool called = false;

  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        called = true;
        received_response = std::move(resp);
      });
  stream_->WaitForWrite(1);

  SessionResponse resp;
  auto* err = resp.mutable_error();
  err->set_rpc_id(77);
  err->mutable_status()->set_code(
      static_cast<int>(google::cloud::StatusCode::kNotFound));
  err->mutable_status()->set_message("Row not found");

  session_->OnMessage(resp);

  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  EXPECT_TRUE(called);
  EXPECT_FALSE(received_response.ok());
  EXPECT_EQ(received_response.status().code(),
            google::cloud::StatusCode::kNotFound);
  EXPECT_EQ(received_response.status().message(), "Row not found");
}

TEST_F(SessionImplTest, HandlesGoAway) {
  SessionResponse resp;
  resp.mutable_go_away();
  session_->OnMessage(resp);
  listener_->WaitForGoAway(1);

  EXPECT_EQ(listener_->go_away_count_, 1);
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);
}

TEST_F(SessionImplTest, CachesParametersAndRefreshConfig) {
  SessionResponse resp1;
  resp1.mutable_session_parameters()->mutable_keep_alive()->set_seconds(30);
  session_->OnMessage(resp1);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_TRUE(session_->session_parameters().has_value());
  EXPECT_EQ(session_->session_parameters()->keep_alive().seconds(), 30);

  SessionResponse resp2;
  resp2.mutable_session_refresh_config()->add_metadata()->set_key("cookie");
  session_->OnMessage(resp2);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_TRUE(session_->refresh_config().has_value());
  EXPECT_EQ(session_->refresh_config()->metadata_size(), 1);
}

TEST_F(SessionImplTest, AbortsActiveCallOnSessionClose) {
  VirtualRpcRequest req;
  req.set_rpc_id(12);

  google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
      received_response;
  bool called = false;

  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        called = true;
        received_response = std::move(resp);
      });
  stream_->WaitForWrite(1);

  session_->OnClosed(
      google::cloud::internal::UnavailableError("Socket dropped"));
  listener_->WaitForClosed();

  EXPECT_TRUE(called);
  EXPECT_FALSE(received_response.ok());
  EXPECT_EQ(received_response.status().code(),
            google::cloud::StatusCode::kUnavailable);
  EXPECT_TRUE(listener_->is_closed_);
}

TEST_F(SessionImplTest, NormalizesPostHandshakeTransportResetToUnavailable) {
  VirtualRpcRequest req;
  req.set_rpc_id(13);

  google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
      received_response;
  bool called = false;

  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        called = true;
        received_response = std::move(resp);
      });
  stream_->WaitForWrite(1);

  session_->OnClosed(google::cloud::internal::InternalError(
      "Stream removed (RST_STREAM (Received RST_STREAM with error code 2))"));
  listener_->WaitForClosed();

  EXPECT_TRUE(called);
  EXPECT_FALSE(received_response.ok());
  EXPECT_EQ(received_response.status().code(),
            google::cloud::StatusCode::kUnavailable);
  EXPECT_NE(received_response.status().message().find("RST_STREAM"),
            std::string::npos);
  EXPECT_TRUE(listener_->is_closed_);
  EXPECT_EQ(listener_->close_status_.code(),
            google::cloud::StatusCode::kUnavailable);
}

TEST_F(SessionImplTest, HandlesCloseGracefully) {
  session_->Close();
  stream_->WaitForClose();
  EXPECT_TRUE(stream_->is_closed_);
}

TEST_F(SessionImplTest, HandlesOnSessionReady) {
  google::bigtable::v2::OpenSessionResponse resp;
  session_->OnSessionReady(resp);
  listener_->WaitForReady();
  EXPECT_TRUE(listener_->is_ready_);
  EXPECT_EQ(session_->state(), SessionImpl::State::kReady);
}

TEST_F(SessionImplTest, KeepAliveCheckMissedHeartbeats) {
  SessionImpl::SetKeepaliveTimeoutMultiplier(2.0);
  std::function<void()> keepalive_task;
  std::chrono::nanoseconds scheduled_delay = std::chrono::nanoseconds(0);
  auto mock_scheduler = [&](std::chrono::nanoseconds delay,
                            std::function<void()> task) {
    scheduled_delay = delay;
    keepalive_task = std::move(task);
  };

  session_ =
      SessionImpl::CreateForTesting(stream_, listener_, cq_, mock_scheduler);
  EXPECT_TRUE(session_->Acquire());

  SessionResponse message;
  message.mutable_session_parameters()->mutable_keep_alive()->set_nanos(
      10 * 1000 * 1000);  // 10ms
  session_->OnMessage(message);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  EXPECT_EQ(scheduled_delay, std::chrono::milliseconds(10));
  EXPECT_TRUE(keepalive_task != nullptr);

  std::this_thread::sleep_for(std::chrono::milliseconds(22));

  keepalive_task();
  stream_->WaitForCancel();

  EXPECT_TRUE(stream_->is_cancelled_);

  session_->OnClosed(google::cloud::internal::CancelledError("cancelled"));
  listener_->WaitForClosed();

  EXPECT_TRUE(listener_->is_closed_);
  EXPECT_EQ(listener_->close_status_.code(),
            google::cloud::StatusCode::kUnavailable);
}

TEST_F(SessionImplTest, KeepAliveDoesNotTimeoutWhenIdlePerGoSpec) {
  SessionImpl::SetKeepaliveTimeoutMultiplier(2.0);
  std::function<void()> keepalive_task;
  int schedule_count = 0;
  auto mock_scheduler = [&](std::chrono::nanoseconds delay,
                            std::function<void()> task) {
    (void)delay;
    keepalive_task = std::move(task);
    schedule_count++;
  };

  session_ =
      SessionImpl::CreateForTesting(stream_, listener_, cq_, mock_scheduler);
  // Session is idle (active_rpc_count() == 0).
  EXPECT_EQ(session_->active_rpc_count(), 0);

  SessionResponse message;
  message.mutable_session_parameters()->mutable_keep_alive()->set_nanos(
      10 * 1000 * 1000);  // 10ms
  session_->OnMessage(message);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  ASSERT_TRUE(keepalive_task != nullptr);
  std::this_thread::sleep_for(std::chrono::milliseconds(25));

  keepalive_task();
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  // Idle session must NOT be cancelled when no vRPC is in flight.
  EXPECT_FALSE(stream_->is_cancelled_);
  EXPECT_EQ(schedule_count, 2);
}

TEST_F(SessionImplTest, KeepAliveCheckHappyPath) {
  std::function<void()> keepalive_task;
  std::chrono::nanoseconds scheduled_delay = std::chrono::nanoseconds(0);
  int schedule_count = 0;
  auto mock_scheduler = [&](std::chrono::nanoseconds delay,
                            std::function<void()> task) {
    scheduled_delay = delay;
    keepalive_task = std::move(task);
    schedule_count++;
  };

  session_ =
      SessionImpl::CreateForTesting(stream_, listener_, cq_, mock_scheduler);

  SessionResponse message;
  message.mutable_session_parameters()->mutable_keep_alive()->set_nanos(
      10 * 1000 * 1000);  // 10ms
  session_->OnMessage(message);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  EXPECT_EQ(schedule_count, 1);
  EXPECT_EQ(scheduled_delay, std::chrono::milliseconds(10));
  EXPECT_TRUE(keepalive_task != nullptr);

  SessionResponse heartbeat;
  heartbeat.mutable_heartbeat();
  session_->OnMessage(heartbeat);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  keepalive_task();
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  EXPECT_FALSE(stream_->is_cancelled_);
  EXPECT_EQ(schedule_count, 2);
}

TEST_F(SessionImplTest, KeepAliveCheckMultiplierRespected) {
  std::function<void()> keepalive_task;
  std::chrono::nanoseconds scheduled_delay = std::chrono::nanoseconds(0);
  int schedule_count = 0;
  auto mock_scheduler = [&](std::chrono::nanoseconds delay,
                            std::function<void()> task) {
    scheduled_delay = delay;
    keepalive_task = std::move(task);
    schedule_count++;
  };

  session_ =
      SessionImpl::CreateForTesting(stream_, listener_, cq_, mock_scheduler);
  EXPECT_TRUE(session_->Acquire());

  // Set multiplier to 3.0.
  SessionImpl::SetKeepaliveTimeoutMultiplier(3.0);

  // A 100ms keep-alive leaves enough slack for scheduling delays on loaded
  // test machines (a 10ms keep-alive left only 8ms and was flaky).
  SessionResponse message;
  message.mutable_session_parameters()->mutable_keep_alive()->set_nanos(
      100 * 1000 * 1000);  // 100ms
  session_->OnMessage(message);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  EXPECT_EQ(schedule_count, 1);
  EXPECT_EQ(scheduled_delay, std::chrono::milliseconds(100));
  EXPECT_TRUE(keepalive_task != nullptr);

  // Send a heartbeat to reset last_heartbeat_time_ to now to avoid setup
  // overhead.
  SessionResponse heartbeat;
  heartbeat.mutable_heartbeat();
  session_->OnMessage(heartbeat);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  // Sleep 150ms. With multiplier 3.0, timeout is 300ms, so 150ms should not
  // trigger timeout.
  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  keepalive_task();
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  EXPECT_FALSE(stream_->is_cancelled_);
  EXPECT_EQ(schedule_count, 2);
  EXPECT_TRUE(keepalive_task != nullptr);

  // Sleep another 200ms (at least 350ms since last heartbeat). Timeout is
  // 300ms, so it should trigger timeout.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  keepalive_task();
  stream_->WaitForCancel();

  EXPECT_TRUE(stream_->is_cancelled_);

  session_->OnClosed(google::cloud::internal::CancelledError("cancelled"));
  listener_->WaitForClosed();

  EXPECT_TRUE(listener_->is_closed_);
  EXPECT_EQ(listener_->close_status_.code(),
            google::cloud::StatusCode::kUnavailable);
}

TEST_F(SessionImplTest, RejectsVirtualRpcWhenNotReady) {
  SessionImplTestPeer::SetState(*session_, SessionImpl::State::kConnecting);

  VirtualRpcRequest req;
  req.set_rpc_id(1);

  google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
      received_response;
  bool called = false;

  // StartVirtualRpc should fail immediately because state is not ready.
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        called = true;
        received_response = std::move(resp);
      });

  EXPECT_TRUE(called);
  EXPECT_FALSE(received_response.ok());
  EXPECT_EQ(received_response.status().code(),
            google::cloud::StatusCode::kUnavailable);

  SessionImplTestPeer::SetState(*session_, SessionImpl::State::kClosing);
  called = false;
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        called = true;
        received_response = std::move(resp);
      });
  EXPECT_TRUE(called);
  EXPECT_EQ(received_response.status().code(),
            google::cloud::StatusCode::kUnavailable);

  SessionImplTestPeer::SetState(*session_, SessionImpl::State::kClosed);
  called = false;
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        called = true;
        received_response = std::move(resp);
      });
  EXPECT_TRUE(called);
  EXPECT_EQ(received_response.status().code(),
            google::cloud::StatusCode::kUnavailable);
}

TEST_F(SessionImplTest, HandlesSocketWriteFailureInsideStartVirtualRpc) {
  stream_->next_write_status_ =
      google::cloud::internal::UnavailableError("Socket broken");

  VirtualRpcRequest req;
  req.set_rpc_id(1);

  google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
      received_response;
  bool called = false;

  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        called = true;
        received_response = std::move(resp);
      });

  EXPECT_TRUE(called);
  EXPECT_FALSE(received_response.ok());
  EXPECT_EQ(received_response.status().code(),
            google::cloud::StatusCode::kUnavailable);
  EXPECT_NE(received_response.status().message().find("Socket broken"),
            std::string::npos);
}

TEST_F(SessionImplTest, OnHeartbeatUpdatesActiveTime) {
  SessionImpl::TimePoint t1 = session_->GetLastActiveTime();
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  session_->OnHeartbeat();
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  SessionImpl::TimePoint t2 = session_->GetLastActiveTime();
  EXPECT_GT(t2, t1);
}

TEST_F(SessionImplTest, OnInitialMetadataIsNoOp) {
  std::multimap<grpc::string_ref, grpc::string_ref> metadata;
  session_->OnInitialMetadata(metadata);
  EXPECT_EQ(session_->state(), SessionImpl::State::kReady);
}

TEST_F(SessionImplTest, OnInitialMetadataParsesBigtablePeerInfoHeader) {
  std::multimap<grpc::string_ref, grpc::string_ref> metadata;
  metadata.emplace("bigtable-peer-info-afe-id", "987654321");
  metadata.emplace("bigtable-peer-info-gfe-id", "123456789");
  metadata.emplace("bigtable-peer-info-transport", "direct_access");
  session_->OnInitialMetadata(metadata);

  ASSERT_TRUE(session_->peer_info().has_value());
  EXPECT_EQ(session_->peer_info()->application_frontend_id, 987654321);
  EXPECT_EQ(session_->peer_info()->google_frontend_id, 123456789);
  EXPECT_TRUE(session_->peer_info()->IsDirectAccess());
  ASSERT_TRUE(session_->GetAfeId().has_value());
  EXPECT_EQ(*session_->GetAfeId(), "987654321");
}

TEST_F(SessionImplTest, CancelVirtualRpcTearsDownStream) {
  stream_->defer_write_done_ = true;
  VirtualRpcRequest req1;
  req1.set_rpc_id(1);

  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req1,
      [](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
             resp) {});
  stream_->WaitForWrite(1);

  session_->CancelVirtualRpc(1);
  stream_->WaitForCancel();

  EXPECT_TRUE(stream_->is_cancelled_);
}

TEST_F(SessionImplTest,
       CancelAfterWriteCompletedKeepsStreamOpenUntilSlotDrainedPerGoSpec) {
  VirtualRpcRequest req1;
  req1.set_rpc_id(55);

  google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
      caller_result;
  Latch caller_notified;

  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req1,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        caller_result = std::move(resp);
        caller_notified.Notify();
      });
  stream_->WaitForWrite(1);

  // Simulate socket write completion so the frame is on the wire.
  session_->OnWriteDone(true);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  // Cancel the in-flight vRPC: caller gets kCancelled immediately, stream stays
  // open, and the multiplex=1 slot remains occupied until the server responds.
  session_->CancelVirtualRpc(55);
  caller_notified.Wait();

  EXPECT_FALSE(caller_result.ok());
  EXPECT_EQ(caller_result.status().code(),
            google::cloud::StatusCode::kCancelled);
  EXPECT_FALSE(stream_->is_cancelled_);
  EXPECT_EQ(session_->active_rpc_count(), 1);
  EXPECT_FALSE(session_->Acquire());

  // Now the server's response for rpc_id 55 arrives on the wire and drains the
  // slot.
  SessionResponse late_resp;
  late_resp.mutable_virtual_rpc()->set_rpc_id(55);
  late_resp.mutable_virtual_rpc()->set_payload("late_server_bytes");
  session_->OnMessage(late_resp);

  listener_->WaitForSlotDrained(1);
  EXPECT_EQ(listener_->last_drained_rpc_id_, 55);
  EXPECT_EQ(session_->active_rpc_count(), 0);
  EXPECT_EQ(session_->latency_tracker().active_requests(), 0);
  EXPECT_EQ(session_->GetLatencyTracker()->active_requests(), 0);
  EXPECT_TRUE(session_->Acquire());
}

TEST_F(SessionImplTest, CloseWhileConnectingCancelsStream) {
  SessionImplTestPeer::SetState(*session_, SessionImpl::State::kConnecting);
  session_->Close();
  stream_->WaitForCancel();
  EXPECT_TRUE(stream_->is_cancelled_);
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);
}

TEST_F(SessionImplTest, DrainWhileConnectingCancelsStream) {
  SessionImplTestPeer::SetState(*session_, SessionImpl::State::kConnecting);
  session_->Drain();
  stream_->WaitForCancel();
  EXPECT_TRUE(stream_->is_cancelled_);
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);
}

TEST_F(SessionImplTest, DrainWithActiveRpcsSetsDraining) {
  VirtualRpcRequest req1;
  req1.set_rpc_id(1);

  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req1,
      [](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
             resp) {});
  stream_->WaitForWrite(1);

  session_->Drain();

  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  EXPECT_EQ(session_->state(), SessionImpl::State::kDraining);
  EXPECT_EQ(listener_->go_away_count_, 1);
}

TEST_F(SessionImplTest, DrainWithNoActiveRpcsClosesStream) {
  session_->Drain();
  stream_->WaitForClose();
  EXPECT_TRUE(stream_->is_closed_);
  EXPECT_EQ(
      stream_->close_reason_,
      google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_USER);
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);
  EXPECT_EQ(listener_->go_away_count_, 1);
}

TEST_F(SessionImplTest, DrainActiveRpcsCloseOnLastResponse) {
  VirtualRpcRequest req1;
  req1.set_rpc_id(1);

  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req1,
      [](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
             resp) {});
  stream_->WaitForWrite(1);

  session_->Drain();

  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_EQ(session_->state(), SessionImpl::State::kDraining);

  SessionResponse resp;
  auto* vrpc_resp = resp.mutable_virtual_rpc();
  vrpc_resp->set_rpc_id(1);

  session_->OnMessage(resp);

  stream_->WaitForClose();
  EXPECT_TRUE(stream_->is_closed_);
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);
}

TEST_F(SessionImplTest, IsIdleTests) {
  SessionImpl::TimePoint now = SessionImpl::Clock::now();
  std::chrono::nanoseconds timeout = std::chrono::seconds(5);

  EXPECT_FALSE(session_->IsIdle(now, timeout));
  EXPECT_TRUE(session_->IsIdle(now + std::chrono::seconds(6), timeout));

  VirtualRpcRequest req1;
  req1.set_rpc_id(1);

  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req1,
      [](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
             resp) {});
  stream_->WaitForWrite(1);
  EXPECT_FALSE(session_->IsIdle(now + std::chrono::seconds(6), timeout));

  SessionResponse resp;
  auto* vrpc_resp = resp.mutable_virtual_rpc();
  vrpc_resp->set_rpc_id(1);
  session_->OnMessage(resp);

  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  EXPECT_TRUE(session_->IsIdle(
      SessionImpl::Clock::now() + std::chrono::seconds(6), timeout));
}

TEST_F(SessionImplTest, OnSessionReadyWithBackendSetsAfeId) {
  EXPECT_EQ(session_->GetAfeId(), std::nullopt);

  google::bigtable::v2::OpenSessionResponse resp;
  resp.mutable_backend()->set_application_frontend_id(123);

  session_->OnSessionReady(resp);

  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  EXPECT_EQ(session_->GetAfeId(), "123");
}

TEST_F(SessionImplTest, StartVirtualRpcSynchronousWhenOnPinnedThread) {
  VirtualRpcRequest req;
  req.set_rpc_id(43);
  req.set_payload("test_payload_sync");

  Latch done;
  SessionImplTestPeer::RunAsync(*session_, [&]() {
    auto cq = SessionImplTestPeer::GetEventCq(*session_);
    ASSERT_TRUE(cq.has_value());
    CurrentCqGuard guard(*cq);
    EXPECT_EQ(current_cq_impl,
              google::cloud::internal::GetCompletionQueueImpl(*cq).get());

    EXPECT_TRUE(session_->Acquire());
    session_->StartVirtualRpc(
        nullptr, req,
        [](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
               resp) {});

    {
      std::lock_guard<std::mutex> l(stream_->mu_);
      EXPECT_EQ(stream_->write_count_, 1);
      EXPECT_EQ(stream_->last_written_request_.virtual_rpc().rpc_id(), 43);
    }
    done.Notify();
  });
  done.Wait();
}

TEST_F(SessionImplTest, AcquireRefusesWhenStreamWriteInFlight) {
  stream_->set_write_in_flight(true);
  EXPECT_TRUE(session_->IsWriteCongested());
  EXPECT_FALSE(session_->Acquire());
  EXPECT_EQ(session_->active_rpc_count(), 0);

  stream_->set_write_in_flight(false);
  EXPECT_FALSE(session_->IsWriteCongested());
  EXPECT_TRUE(session_->Acquire());
  EXPECT_EQ(session_->active_rpc_count(), 1);
}

TEST_F(SessionImplTest, RoutesErrorResponseWithRetryInfoAndMarksServerResult) {
  VirtualRpcRequest req;
  req.set_rpc_id(88);

  google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
      received_response;
  bool called = false;

  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        called = true;
        received_response = std::move(resp);
      });
  stream_->WaitForWrite(1);

  SessionResponse resp;
  auto* err = resp.mutable_error();
  err->set_rpc_id(88);
  err->mutable_status()->set_code(
      static_cast<int>(google::cloud::StatusCode::kUnavailable));
  err->mutable_status()->set_message("Tablet temporarily overloaded");
  err->mutable_retry_info()->mutable_retry_delay()->set_seconds(0);
  err->mutable_retry_info()->mutable_retry_delay()->set_nanos(25 * 1000 * 1000);

  session_->OnMessage(resp);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  EXPECT_TRUE(called);
  ASSERT_FALSE(received_response.ok());
  EXPECT_EQ(received_response.status().code(),
            google::cloud::StatusCode::kUnavailable);
  EXPECT_TRUE(IsServerResult(received_response.status()));
  auto retry_delay = GetServerRetryDelay(received_response.status());
  ASSERT_TRUE(retry_delay.has_value());
  EXPECT_EQ(*retry_delay, std::chrono::milliseconds(25));
}

TEST_F(SessionImplTest, RecordsCreatedAtAndReadyAtTimestamps) {
  EXPECT_GT(session_->created_at(), SessionImpl::TimePoint{});
  EXPECT_FALSE(session_->ready_at().has_value());

  google::bigtable::v2::OpenSessionResponse resp;
  session_->OnSessionReady(resp);
  listener_->WaitForReady();

  ASSERT_TRUE(session_->ready_at().has_value());
  EXPECT_GE(*session_->ready_at(), session_->created_at());
}

TEST_F(SessionImplTest,
       KeepAliveReschedulesWhenSessionParametersArrivesWhileConnecting) {
  std::function<void()> keepalive_task;
  int schedule_count = 0;
  auto mock_scheduler = [&](std::chrono::nanoseconds,
                            std::function<void()> task) {
    keepalive_task = std::move(task);
    schedule_count++;
  };

  session_ =
      SessionImpl::CreateForTesting(stream_, listener_, cq_, mock_scheduler);
  SessionImplTestPeer::SetState(*session_, SessionImpl::State::kConnecting);

  SessionResponse message;
  message.mutable_session_parameters()->mutable_keep_alive()->set_nanos(
      10 * 1000 * 1000);
  session_->OnMessage(message);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_EQ(schedule_count, 1);
  ASSERT_NE(keepalive_task, nullptr);

  // Firing while still kConnecting must reschedule rather than drop the timer.
  keepalive_task();
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_EQ(schedule_count, 2);
  EXPECT_FALSE(stream_->is_cancelled_);
}

TEST_F(SessionImplTest, MidStreamUnimplementedPreservesStatusCodeOnClose) {
  VirtualRpcRequest req;
  req.set_rpc_id(99);
  google::cloud::Status rpc_status;
  bool called = false;

  EXPECT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        called = true;
        rpc_status = resp.status();
      });
  stream_->WaitForWrite(1);

  session_->OnClosed(
      google::cloud::internal::UnimplementedError("Sessions disabled"));
  listener_->WaitForClosed();

  EXPECT_TRUE(called);
  EXPECT_EQ(rpc_status.code(), google::cloud::StatusCode::kUnimplemented);
  EXPECT_EQ(listener_->close_status_.code(),
            google::cloud::StatusCode::kUnimplemented);
}

TEST_F(SessionImplTest,
       PreWriteCancelTransitionsSessionToClosingAndTearsDownStream) {
  stream_->defer_write_done_ = true;
  session_->SetAutoFinalize(false);

  VirtualRpcRequest req;
  req.set_rpc_id(101);

  Latch cb_done;
  SessionImpl::State state_in_cb = SessionImpl::State::kReady;
  int active_count_in_cb = -1;
  google::cloud::Status cb_status;

  ASSERT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        state_in_cb = session_->state();
        active_count_in_cb = session_->active_rpc_count();
        cb_status = resp.status();
        cb_done.Notify();
      });
  stream_->WaitForWrite(1);

  session_->CancelVirtualRpc(101);
  cb_done.Wait();

  EXPECT_EQ(cb_status.code(), google::cloud::StatusCode::kCancelled);
  EXPECT_EQ(
      google::cloud::internal::GetPayload(cb_status, kWriteStatusPayloadUrl),
      "0");
  EXPECT_EQ(state_in_cb, SessionImpl::State::kClosing);
  EXPECT_EQ(active_count_in_cb, 0);
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);
  EXPECT_EQ(session_->active_rpc_count(), 0);
  EXPECT_TRUE(stream_->is_cancelled_);
  EXPECT_FALSE(session_->Acquire());
}

TEST_F(SessionImplTest,
       OnWriteDoneFailureTransitionsToClosingAndSuppressesListenerOnWriteDone) {
  stream_->defer_write_done_ = true;
  session_->SetAutoFinalize(false);

  VirtualRpcRequest req;
  req.set_rpc_id(102);

  Latch cb_done;
  SessionImpl::State state_in_cb = SessionImpl::State::kReady;
  int active_count_in_cb = -1;
  google::cloud::Status cb_status;

  ASSERT_TRUE(session_->Acquire());
  session_->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        state_in_cb = session_->state();
        active_count_in_cb = session_->active_rpc_count();
        cb_status = resp.status();
        cb_done.Notify();
      });
  stream_->WaitForWrite(1);

  session_->OnWriteDone(false);
  cb_done.Wait();
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  EXPECT_EQ(cb_status.code(), google::cloud::StatusCode::kUnavailable);
  EXPECT_EQ(state_in_cb, SessionImpl::State::kClosing);
  EXPECT_EQ(active_count_in_cb, 0);
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);
  EXPECT_EQ(session_->active_rpc_count(), 0);
  EXPECT_TRUE(stream_->is_cancelled_);
  {
    std::lock_guard<std::mutex> l(listener_->mu_);
    EXPECT_EQ(listener_->write_done_count_, 0);
  }
  EXPECT_FALSE(session_->Acquire());
}

TEST_F(SessionImplTest,
       ConcurrentAcquireDuringOnWriteDoneWindowCannotOverwriteWritingRpcId) {
  stream_->defer_write_done_ = true;

  VirtualRpcRequest req1;
  req1.set_rpc_id(1);
  Latch r1_done;
  ASSERT_TRUE(session_->Acquire());
  stream_->set_write_in_flight(true);
  session_->StartVirtualRpc(
      nullptr, req1,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>) {
        r1_done.Notify();
      });
  stream_->WaitForWrite(1);

  // Server responds to r1 before r1's OnWriteDone runs on SessionImpl.
  SessionResponse resp1;
  resp1.mutable_virtual_rpc()->set_rpc_id(1);
  session_->OnMessage(resp1);
  r1_done.Wait();
  EXPECT_EQ(session_->active_rpc_count(), 0);

  // Simulate SessionStream::OnWriteDoneSerialized clearing write_in_flight_
  // before SessionImpl::OnWriteDoneSerialized acquires state_mutex_.
  stream_->set_write_in_flight(false);

  // A second caller r2 must NOT be able to Acquire() while r1's
  // writing_rpc_id_ / write_queue_size_ is still pending in SessionImpl.
  EXPECT_FALSE(session_->Acquire());

  // Now complete r1's OnWriteDone(true) on SessionImpl.
  session_->OnWriteDone(true);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }

  // Now r2 can Acquire() and start with defer_write_done_ = true; cancelling r2
  // before its write completes must report uncommitted ("0") and tear down.
  ASSERT_TRUE(session_->Acquire());
  VirtualRpcRequest req2;
  req2.set_rpc_id(2);
  Latch r2_done;
  google::cloud::Status r2_status;
  session_->StartVirtualRpc(
      nullptr, req2,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        r2_status = resp.status();
        r2_done.Notify();
      });
  stream_->WaitForWrite(2);

  session_->CancelVirtualRpc(2);
  r2_done.Wait();
  EXPECT_EQ(r2_status.code(), google::cloud::StatusCode::kCancelled);
  EXPECT_EQ(
      google::cloud::internal::GetPayload(r2_status, kWriteStatusPayloadUrl),
      "0");
  EXPECT_FALSE(session_->IsCancelledAwaitingDrain(2));
}

TEST_F(SessionImplTest,
       ConcurrentOnMessageAndCancelNeverWedgesSlotInCancelledAwaitingDrain) {
  session_->SetAutoFinalize(false);

  for (int iter = 1; iter <= 150; ++iter) {
    int64_t const rpc_id = 1000 + iter;
    ASSERT_TRUE(session_->Acquire()) << "Slot wedged at iteration " << iter;

    VirtualRpcRequest req;
    req.set_rpc_id(rpc_id);
    Latch cb_done;
    session_->StartVirtualRpc(
        nullptr, req,
        [&, rpc_id](
            google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>) {
          if (!session_->IsCancelledAwaitingDrain(rpc_id)) {
            session_->FinalizeActiveRpc(rpc_id);
          }
          cb_done.Notify();
        });
    stream_->WaitForWrite(iter);

    SessionResponse resp;
    resp.mutable_virtual_rpc()->set_rpc_id(rpc_id);

    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::thread t_msg([&]() {
      ready.fetch_add(1, std::memory_order_acq_rel);
      while (!go.load(std::memory_order_acquire)) {
      }
      session_->OnMessage(resp);
    });
    std::thread t_cancel([&]() {
      ready.fetch_add(1, std::memory_order_acq_rel);
      while (!go.load(std::memory_order_acquire)) {
      }
      session_->CancelVirtualRpc(rpc_id);
    });
    while (ready.load(std::memory_order_acquire) < 2) {
    }
    go.store(true, std::memory_order_release);
    t_msg.join();
    t_cancel.join();

    cb_done.Wait();
    {
      Latch flushed;
      SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
      flushed.Wait();
    }

    EXPECT_FALSE(session_->IsCancelledAwaitingDrain(rpc_id))
        << "Wedged at iter " << iter;
    EXPECT_EQ(session_->active_rpc_count(), 0) << "Leaked at iter " << iter;
  }
}

TEST_F(SessionImplTest,
       ConcurrentPostWriteCancelAndOnMessageAccountsSlotReleaseExactlyOnce) {
  session_->SetAutoFinalize(false);
  std::atomic<int> active_calls{0};

  for (int iter = 1; iter <= 150; ++iter) {
    int64_t const rpc_id = 2000 + iter;
    ASSERT_TRUE(session_->Acquire()) << "Acquire failed at iter " << iter;
    active_calls.fetch_add(1, std::memory_order_acq_rel);

    int prev_drained = 0;
    {
      std::lock_guard<std::mutex> l(listener_->mu_);
      prev_drained = listener_->slot_drained_count_;
    }

    VirtualRpcRequest req;
    req.set_rpc_id(rpc_id);
    Latch cb_done;
    session_->StartVirtualRpc(
        nullptr, req,
        [&, rpc_id](
            google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>) {
          if (!session_->IsCancelledAwaitingDrain(rpc_id)) {
            session_->FinalizeActiveRpc(rpc_id);
            active_calls.fetch_sub(1, std::memory_order_acq_rel);
          }
          cb_done.Notify();
        });
    stream_->WaitForWrite(iter);

    SessionResponse resp;
    resp.mutable_virtual_rpc()->set_rpc_id(rpc_id);

    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::thread t_cancel([&]() {
      ready.fetch_add(1, std::memory_order_acq_rel);
      while (!go.load(std::memory_order_acquire)) {
      }
      session_->CancelVirtualRpc(rpc_id);
    });
    std::thread t_msg([&]() {
      ready.fetch_add(1, std::memory_order_acq_rel);
      while (!go.load(std::memory_order_acquire)) {
      }
      session_->OnMessage(resp);
    });
    while (ready.load(std::memory_order_acquire) < 2) {
    }
    go.store(true, std::memory_order_release);
    t_cancel.join();
    t_msg.join();

    cb_done.Wait();
    {
      Latch flushed;
      SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
      flushed.Wait();
    }

    int new_drained = 0;
    {
      std::lock_guard<std::mutex> l(listener_->mu_);
      new_drained = listener_->slot_drained_count_ - prev_drained;
    }
    if (new_drained > 0) {
      active_calls.fetch_sub(new_drained, std::memory_order_acq_rel);
    }

    EXPECT_EQ(active_calls.load(std::memory_order_acquire), 0)
        << "Double decrement or leak at iter " << iter;
    EXPECT_EQ(session_->active_rpc_count(), 0);
  }
}

TEST_F(SessionImplTest, CloseOrDrainWhileConnectingSuppressesOnSessionReady) {
  // Sub-case 1: Close() while kConnecting followed by OnSessionReady.
  SessionImplTestPeer::SetState(*session_, SessionImpl::State::kConnecting);
  session_->Close();
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);
  EXPECT_TRUE(stream_->is_cancelled_);

  google::bigtable::v2::OpenSessionResponse open_resp;
  session_->OnSessionReady(open_resp);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);
  EXPECT_FALSE(listener_->is_ready_);

  // Sub-case 2: Drain() while kConnecting followed by OnSessionReady.
  stream_->is_cancelled_ = false;
  SessionImplTestPeer::SetState(*session_, SessionImpl::State::kConnecting);
  session_->Drain();
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);
  EXPECT_TRUE(stream_->is_cancelled_);

  session_->OnSessionReady(open_resp);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);
  EXPECT_FALSE(listener_->is_ready_);
}

TEST_F(SessionImplTest,
       SplitPhaseAcquireRaceWithCloseDrainAndGoAwayPreservesInvariants) {
  // Sub-case A: Acquire() -> Close() -> StartVirtualRpc() must not underflow
  // active_rpc_count_ to -1.
  ASSERT_TRUE(session_->Acquire());
  EXPECT_EQ(session_->active_rpc_count(), 1);
  session_->Close();
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);

  VirtualRpcRequest req1;
  req1.set_rpc_id(701);
  Latch cb1_done;
  session_->StartVirtualRpc(
      nullptr, req1,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        EXPECT_EQ(resp.status().code(),
                  google::cloud::StatusCode::kUnavailable);
        cb1_done.Notify();
      });
  cb1_done.Wait();
  EXPECT_EQ(session_->active_rpc_count(), 0);

  // Sub-case B: Acquire() -> Drain() -> StartVirtualRpc() must transition
  // kDraining to kClosing and close the stream instead of stranding in
  // kDraining with 0 active RPCs.
  auto stream_b = std::shared_ptr<MockSessionStream>(new MockSessionStream());
  auto session_b = SessionImpl::CreateForTesting(
      stream_b, listener_, cq_,
      [](std::chrono::nanoseconds, std::function<void()>) {});
  ASSERT_TRUE(session_b->Acquire());
  session_b->Drain();
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_b, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_EQ(session_b->state(), SessionImpl::State::kDraining);

  VirtualRpcRequest req2;
  req2.set_rpc_id(702);
  Latch cb2_done;
  session_b->StartVirtualRpc(
      nullptr, req2,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        EXPECT_EQ(resp.status().code(),
                  google::cloud::StatusCode::kUnavailable);
        cb2_done.Notify();
      });
  cb2_done.Wait();
  EXPECT_EQ(session_b->active_rpc_count(), 0);
  EXPECT_EQ(session_b->state(), SessionImpl::State::kClosing);
  EXPECT_TRUE(stream_b->is_closed_);

  // Sub-case C: Acquire() -> OnMessage(kGoAway) -> StartVirtualRpc() must
  // transition kDraining to kClosing and close the stream.
  auto stream_c = std::shared_ptr<MockSessionStream>(new MockSessionStream());
  auto session_c = SessionImpl::CreateForTesting(
      stream_c, listener_, cq_,
      [](std::chrono::nanoseconds, std::function<void()>) {});
  ASSERT_TRUE(session_c->Acquire());
  SessionResponse goaway_msg;
  goaway_msg.mutable_go_away();
  session_c->OnMessage(goaway_msg);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_c, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_EQ(session_c->state(), SessionImpl::State::kDraining);

  VirtualRpcRequest req3;
  req3.set_rpc_id(703);
  Latch cb3_done;
  session_c->StartVirtualRpc(
      nullptr, req3,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        EXPECT_EQ(resp.status().code(),
                  google::cloud::StatusCode::kUnavailable);
        cb3_done.Notify();
      });
  cb3_done.Wait();
  EXPECT_EQ(session_c->active_rpc_count(), 0);
  EXPECT_EQ(session_c->state(), SessionImpl::State::kClosing);
  EXPECT_TRUE(stream_c->is_closed_);
}

class PrematureReadService final
    : public google::bigtable::v2::Bigtable::Service {
 public:
  explicit PrematureReadService(bool send_goaway_instead_of_params)
      : send_goaway_instead_of_params_(send_goaway_instead_of_params) {}

  grpc::Status OpenTable(
      grpc::ServerContext* /*context*/,
      grpc::ServerReaderWriter<google::bigtable::v2::SessionResponse,
                               google::bigtable::v2::SessionRequest>* stream)
      override {
    google::bigtable::v2::SessionResponse open_resp;
    open_resp.mutable_open_session()->mutable_backend()->set_google_frontend_id(
        101);
    stream->Write(open_resp);

    if (send_goaway_instead_of_params_) {
      google::bigtable::v2::SessionResponse goaway_resp;
      goaway_resp.mutable_go_away()->set_reason("goaway_while_open_write");
      stream->Write(goaway_resp);
    } else {
      google::bigtable::v2::SessionResponse params_resp;
      params_resp.mutable_session_parameters()
          ->mutable_keep_alive()
          ->set_seconds(10);
      stream->Write(params_resp);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    google::bigtable::v2::SessionRequest req;
    while (stream->Read(&req)) {
      if (req.has_close_session()) break;
    }
    return grpc::Status::OK;
  }

 private:
  bool send_goaway_instead_of_params_;
};

TEST(SessionImplGrpcStreamTest,
     DeferredOpenWriteDeliversOnSessionReadyBeforeSubsequentMessages) {
  for (bool send_goaway : {false, true}) {
    PrematureReadService service(send_goaway);
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddChannelArgument(GRPC_ARG_HTTP2_STREAM_LOOKAHEAD_BYTES, 1024);
    builder.AddListeningPort(
        "localhost:0", grpc::experimental::LocalServerCredentials(LOCAL_TCP),
        &port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_HTTP2_STREAM_LOOKAHEAD_BYTES, 1024);
    args.SetInt(GRPC_ARG_HTTP2_WRITE_BUFFER_SIZE, 1024);
    auto channel = grpc::CreateCustomChannel(
        "localhost:" + std::to_string(port),
        grpc::experimental::LocalCredentials(LOCAL_TCP), args);
    auto stub = google::bigtable::v2::Bigtable::NewStub(channel);

    google::cloud::CompletionQueue cq;
    std::thread cq_thread([&cq]() { cq.Run(); });

    google::bigtable::v2::SessionRefreshConfig refresh_cfg;
    refresh_cfg.mutable_optimized_open_request()->set_payload(
        std::string(256 * 1024, 'R'));

    auto listener = std::make_shared<MockSessionListener>();
    auto session = SessionImpl::Create(
        stub.get(), listener, "projects/p/instances/i/tables/t", "default",
        google::bigtable::v2::OpenTableRequest::PERMISSION_READ, cq,
        [](std::chrono::nanoseconds, std::function<void()> task) { task(); },
        refresh_cfg);

    listener->WaitForReady();
    EXPECT_TRUE(listener->is_ready_);
    if (send_goaway) {
      listener->WaitForGoAway(1);
      EXPECT_EQ(listener->go_away_count_, 1);
    }

    session->Close();
    server->Shutdown();
    cq.Shutdown();
    cq_thread.join();
  }
}

class HangingGracefulCloseService final
    : public google::bigtable::v2::Bigtable::Service {
 public:
  grpc::Status OpenTable(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<google::bigtable::v2::SessionResponse,
                               google::bigtable::v2::SessionRequest>* stream)
      override {
    google::bigtable::v2::SessionRequest open_req;
    if (!stream->Read(&open_req)) return grpc::Status::OK;

    google::bigtable::v2::SessionResponse open_resp;
    open_resp.mutable_open_session();
    stream->Write(open_resp);

    google::bigtable::v2::SessionRequest next_req;
    while (stream->Read(&next_req)) {
      if (next_req.has_close_session()) {
        close_session_received_.Notify();
        break;
      }
    }

    auto const deadline =
        SessionImpl::Clock::now() + std::chrono::milliseconds(1500);
    while (!context->IsCancelled() && SessionImpl::Clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    observed_client_cancel_.store(context->IsCancelled(),
                                  std::memory_order_release);
    server_done_.Notify();
    return grpc::Status::OK;
  }

  Latch close_session_received_;
  Latch server_done_;
  std::atomic<bool> observed_client_cancel_{false};
};

TEST(SessionImplGrpcStreamTest,
     GracefulCloseAndDrainTimeoutForceCancelHangingPeer) {
  for (bool test_drain_timeout : {false, true}) {
    HangingGracefulCloseService service;
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort(
        "localhost:0", grpc::experimental::LocalServerCredentials(LOCAL_TCP),
        &port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    auto channel =
        grpc::CreateChannel("localhost:" + std::to_string(port),
                            grpc::experimental::LocalCredentials(LOCAL_TCP));
    auto stub = google::bigtable::v2::Bigtable::NewStub(channel);

    google::cloud::CompletionQueue cq;
    std::thread cq_thread([&cq]() { cq.Run(); });

    auto scheduler = [cq](std::chrono::nanoseconds d,
                          std::function<void()> task) mutable {
      cq.MakeRelativeTimer(d).then([t = std::move(task)](auto f) mutable {
        if (f.get().ok()) t();
      });
    };

    auto listener = std::make_shared<MockSessionListener>();
    auto session = SessionImpl::Create(
        stub.get(), listener, "projects/p/instances/i/tables/t", "default",
        google::bigtable::v2::OpenTableRequest::PERMISSION_READ, cq, scheduler);
    session->set_goaway_drain_timeout(std::chrono::milliseconds(200));
    listener->WaitForReady();

    Latch vrpc_aborted;
    if (test_drain_timeout) {
      ASSERT_TRUE(session->Acquire());
      VirtualRpcRequest req;
      req.set_rpc_id(901);
      session->StartVirtualRpc(
          nullptr, req,
          [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
                  resp) {
            EXPECT_FALSE(resp.ok());
            vrpc_aborted.Notify();
          });
      session->Drain();
      ASSERT_TRUE(vrpc_aborted.WaitFor(std::chrono::milliseconds(1500)));
    } else {
      session->Close();
    }

    ASSERT_TRUE(service.close_session_received_.WaitFor(
        std::chrono::milliseconds(1500)));
    listener->WaitForClosed();
    ASSERT_TRUE(service.server_done_.WaitFor(std::chrono::milliseconds(1500)));
    EXPECT_TRUE(listener->is_closed_);
    EXPECT_TRUE(service.observed_client_cancel_.load());
    EXPECT_EQ(session->state(), SessionImpl::State::kClosed);

    server->Shutdown();
    cq.Shutdown();
    cq_thread.join();
  }
}

TEST_F(SessionImplTest,
       GoAwaySendsCloseSessionReasonGoAwayAndDefault30sDrainTimer) {
  // 1. Default keepalive multiplier is 3.0 and GoAway drain timeout is 30s.
  EXPECT_DOUBLE_EQ(SessionImpl::GetKeepaliveTimeoutMultiplier(), 3.0);
  EXPECT_EQ(SessionImpl::GetGoAwayDrainTimeout(), std::chrono::seconds(30));

  // 2. Idle GoAway sends CLOSE_SESSION_REASON_GOAWAY immediately.
  SessionResponse idle_goaway;
  idle_goaway.mutable_go_away()->set_reason("server_rebalance");
  idle_goaway.mutable_go_away()->set_description("Planned AFE drain");
  session_->OnMessage(idle_goaway);
  stream_->WaitForClose();
  EXPECT_TRUE(stream_->is_closed_);
  EXPECT_EQ(
      stream_->close_reason_,
      google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_GOAWAY);

  // 3. GoAway with admitted in-flight vRPC arms a 30s drain timer, waits for
  // the admitted vRPC to complete, and then sends CLOSE_SESSION_REASON_GOAWAY.
  auto stream_admitted =
      std::shared_ptr<MockSessionStream>(new MockSessionStream());
  std::chrono::nanoseconds scheduled_drain_delay = std::chrono::nanoseconds(0);
  auto session_admitted = SessionImpl::CreateForTesting(
      stream_admitted, listener_, cq_,
      [&](std::chrono::nanoseconds d, std::function<void()>) {
        if (d >= std::chrono::seconds(1)) {
          scheduled_drain_delay = d;
        }
      });

  ASSERT_TRUE(session_admitted->Acquire());
  VirtualRpcRequest req;
  req.set_rpc_id(42);
  Latch rpc_done;
  bool rpc_completed = false;
  session_admitted->StartVirtualRpc(
      nullptr, req, [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        EXPECT_TRUE(resp.ok());
        rpc_completed = true;
        rpc_done.Notify();
      });
  stream_admitted->WaitForWrite(1);

  SessionResponse goaway_admitted;
  goaway_admitted.mutable_go_away()->set_reason("load_shed");
  goaway_admitted.mutable_go_away()->set_description("Draining admitted RPC");
  goaway_admitted.mutable_go_away()->set_last_rpc_id_admitted(42);
  session_admitted->OnMessage(goaway_admitted);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_admitted,
                                  [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_EQ(session_admitted->state(), SessionImpl::State::kDraining);
  EXPECT_EQ(scheduled_drain_delay, std::chrono::seconds(30));
  EXPECT_FALSE(stream_admitted->is_closed_);

  SessionResponse vrpc_resp;
  vrpc_resp.mutable_virtual_rpc()->set_rpc_id(42);
  session_admitted->OnMessage(vrpc_resp);
  stream_admitted->WaitForClose();
  ASSERT_TRUE(rpc_done.WaitFor(std::chrono::seconds(1)));
  EXPECT_TRUE(rpc_completed);
  EXPECT_TRUE(stream_admitted->is_closed_);
  EXPECT_EQ(
      stream_admitted->close_reason_,
      google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_GOAWAY);
}

TEST_F(SessionImplTest,
       GoAwayImmediatelyFailsUnadmittedInFlightRpcAndClosesWithGoAway) {
  ASSERT_TRUE(session_->Acquire());
  VirtualRpcRequest req;
  req.set_rpc_id(50);
  Latch cb_done;
  google::cloud::Status failed_status;
  session_->StartVirtualRpc(
      nullptr, req, [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        ASSERT_FALSE(resp.ok());
        failed_status = resp.status();
        cb_done.Notify();
      });
  stream_->WaitForWrite(1);

  // Server sends GoAway with last_rpc_id_admitted = 49 (< 50).
  SessionResponse goaway_unadmitted;
  goaway_unadmitted.mutable_go_away()->set_reason("overload");
  goaway_unadmitted.mutable_go_away()->set_last_rpc_id_admitted(49);
  session_->OnMessage(goaway_unadmitted);

  ASSERT_TRUE(cb_done.WaitFor(std::chrono::seconds(1)));
  EXPECT_EQ(failed_status.code(), google::cloud::StatusCode::kUnavailable);
  EXPECT_TRUE(IsSessionTurnover(failed_status));
  EXPECT_EQ(google::cloud::internal::GetPayload(failed_status,
                                                kWriteStatusPayloadUrl),
            "0");

  stream_->WaitForClose();
  EXPECT_TRUE(stream_->is_closed_);
  EXPECT_EQ(
      stream_->close_reason_,
      google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_GOAWAY);
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);
}

TEST_F(SessionImplTest,
       UnknownFrameDoesNotResetHeartbeatAndWriteOnceDisconnectStatus) {
  int64_t const sentinel_us = 123456789LL;
  SessionImplTestPeer::SetLastHeartbeatTimeUs(*session_, sentinel_us);

  // Unknown empty SessionResponse frame must not reset last_heartbeat_time_us_.
  SessionResponse unknown_frame;
  session_->OnMessage(unknown_frame);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_EQ(SessionImplTestPeer::GetLastHeartbeatTimeUs(*session_),
            sentinel_us);

  // Pre-existing disconnect_status_ has write-once semantics on session-level
  // ErrorResponse (rpc_id == 0).
  auto const first_err =
      google::cloud::internal::PermissionDeniedError("First fatal error");
  SessionImplTestPeer::SetDisconnectStatus(*session_, first_err);

  SessionResponse second_err_frame;
  second_err_frame.mutable_error()->set_rpc_id(0);
  second_err_frame.mutable_error()->mutable_status()->set_code(
      grpc::StatusCode::INTERNAL);
  second_err_frame.mutable_error()->mutable_status()->set_message(
      "Second error must not clobber first");
  session_->OnMessage(second_err_frame);
  stream_->WaitForCancel();
  EXPECT_EQ(SessionImplTestPeer::GetDisconnectStatus(*session_), first_err);
}

TEST_F(SessionImplTest,
       Base64PeerInfoHeaderAndZeroAfeIdNormalizationInGetPeerId) {
  google::bigtable::v2::PeerInfo proto_peer;
  proto_peer.set_google_frontend_id(777);
  proto_peer.set_application_frontend_id(0);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
  proto_peer.set_application_frontend_zone("us-central1-a");
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
  proto_peer.set_application_frontend_subzone("us-central1-a:sz2");
  proto_peer.set_transport_type(
      google::bigtable::v2::PeerInfo::TRANSPORT_TYPE_SESSION_CLOUD_PATH);
  std::string const b64 =
      absl::WebSafeBase64Escape(proto_peer.SerializeAsString());

  std::multimap<grpc::string_ref, grpc::string_ref> md = {
      {"bigtable-peer-info", b64}};
  session_->OnInitialMetadata(md);

  ASSERT_TRUE(session_->peer_info().has_value());
  EXPECT_EQ(session_->peer_info()->google_frontend_id, 777);
  EXPECT_EQ(session_->peer_info()->application_frontend_id, 0);
  EXPECT_EQ(session_->peer_info()->application_frontend_zone, "us-central1-a");
  EXPECT_EQ(session_->peer_info()->application_frontend_subzone,
            "us-central1-a:sz2");
  // AFE ID 0 normalizes to std::nullopt so pool maps all unknown AFE IDs to
  // "default_peer" rather than splitting across "0" and "default_peer".
  EXPECT_EQ(session_->GetPeerId(), std::nullopt);

  // Even if OpenSessionResponse sets backend.application_frontend_id = 0,
  // GetPeerId() still returns std::nullopt.
  google::bigtable::v2::OpenSessionResponse open_resp;
  open_resp.mutable_backend()->set_application_frontend_id(0);
  session_->OnSessionReady(open_resp);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_EQ(session_->GetPeerId(), std::nullopt);
}

TEST_F(SessionImplTest,
       GoAwayWithUnsetLastRpcIdAdmittedDrainsAndPreservesServerErrorResult) {
  ASSERT_TRUE(session_->Acquire());
  VirtualRpcRequest req;
  req.set_rpc_id(1);
  Latch cb_done;
  google::cloud::Status failed_status;
  session_->StartVirtualRpc(
      nullptr, req, [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        ASSERT_FALSE(resp.ok());
        failed_status = resp.status();
        cb_done.Notify();
      });
  stream_->WaitForWrite(1);

  // Real Anviltop servers emit GoAwayResponse without setting
  // last_rpc_id_admitted (proto3 default 0) followed by the ErrorResponse for
  // the in-flight vRPC.
  SessionResponse goaway_unset;
  goaway_unset.mutable_go_away()->set_reason("session_refresh");
  goaway_unset.mutable_go_away()->set_description(
      "Session requires refresh, please open a new session");
  session_->OnMessage(goaway_unset);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_EQ(session_->state(), SessionImpl::State::kDraining);
  EXPECT_FALSE(cb_done.HasBeenNotified());

  SessionResponse err_frame;
  err_frame.mutable_error()->set_rpc_id(1);
  err_frame.mutable_error()->mutable_status()->set_code(
      grpc::StatusCode::UNAVAILABLE);
  err_frame.mutable_error()->mutable_status()->set_message(
      "Session requires refresh");
  err_frame.mutable_error()
      ->mutable_retry_info()
      ->mutable_retry_delay()
      ->set_seconds(0);
  session_->OnMessage(err_frame);

  ASSERT_TRUE(cb_done.WaitFor(std::chrono::seconds(1)));
  EXPECT_EQ(failed_status.code(), google::cloud::StatusCode::kUnavailable);
  EXPECT_EQ(failed_status.message(), "Session requires refresh");
  EXPECT_TRUE(IsServerResult(failed_status));
  EXPECT_TRUE(IsSessionTurnover(failed_status));

  stream_->WaitForClose();
  EXPECT_TRUE(stream_->is_closed_);
  EXPECT_EQ(
      stream_->close_reason_,
      google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_GOAWAY);
  EXPECT_EQ(session_->state(), SessionImpl::State::kClosing);
}

TEST_F(SessionImplTest, TimersFiringAfterCqShutdownDoNotLeakSessionCycle) {
  std::weak_ptr<SessionImpl> weak_session;
  {
    google::cloud::CompletionQueue local_cq;
    std::thread local_cq_thread([&local_cq]() { local_cq.Run(); });
    auto stream = std::shared_ptr<MockSessionStream>(new MockSessionStream());
    auto listener = std::make_shared<MockSessionListener>();
    std::vector<std::function<void()>> scheduled_timers;
    auto session = SessionImpl::CreateForTesting(
        stream, listener, local_cq,
        [&scheduled_timers](std::chrono::nanoseconds,
                            std::function<void()> task) {
          scheduled_timers.push_back(std::move(task));
        });
    weak_session = session;

    SessionResponse params_resp;
    params_resp.mutable_session_parameters()->mutable_keep_alive()->set_seconds(
        1);
    session->OnMessage(params_resp);
    {
      Latch flushed;
      SessionImplTestPeer::RunAsync(*session, [&]() { flushed.Notify(); });
      flushed.Wait();
    }
    ASSERT_EQ(scheduled_timers.size(), 1u);

    ASSERT_TRUE(session->Acquire());
    VirtualRpcRequest req;
    req.set_rpc_id(1);
    session->StartVirtualRpc(
        nullptr, req,
        [](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>) {
        });
    stream->WaitForWrite(1);
    session->Drain();
    ASSERT_EQ(scheduled_timers.size(), 2u);

    local_cq.Shutdown();
    local_cq_thread.join();

    auto timers = std::move(scheduled_timers);
    scheduled_timers.clear();
    for (auto& timer : timers) {
      timer();
    }
    scheduled_timers.clear();
    listener->last_session_.reset();
    session.reset();
  }
  EXPECT_TRUE(weak_session.expired());
}

TEST_F(SessionImplTest,
       SynchronousSchedulerCallbackDoesNotDeadlockSessionMutex) {
  google::cloud::CompletionQueue local_cq;
  std::thread local_cq_thread([&local_cq]() { local_cq.Run(); });
  auto stream = std::shared_ptr<MockSessionStream>(new MockSessionStream());
  auto listener = std::make_shared<MockSessionListener>();
  std::weak_ptr<SessionImpl> weak_session;
  std::atomic<int> schedule_calls{0};
  auto session = SessionImpl::CreateForTesting(
      stream, listener, local_cq,
      [&](std::chrono::nanoseconds, std::function<void()> task) {
        int const n = schedule_calls.fetch_add(1, std::memory_order_relaxed);
        if (auto s = weak_session.lock()) {
          (void)s->session_parameters();
        }
        if (n < 3) {
          task();
        }
      });
  weak_session = session;

  google::bigtable::v2::OpenSessionResponse open_resp;
  session->OnSessionReady(open_resp);
  listener->WaitForReady();

  SessionResponse params_resp;
  params_resp.mutable_session_parameters()->mutable_keep_alive()->set_seconds(
      1);
  session->OnMessage(params_resp);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_GE(schedule_calls.load(), 1);

  ASSERT_TRUE(session->Acquire());
  Latch cb_done;
  VirtualRpcRequest req;
  req.set_rpc_id(1);
  session->StartVirtualRpc(
      nullptr, req,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) {
        EXPECT_FALSE(resp.ok());
        cb_done.Notify();
      });
  stream->WaitForWrite(1);

  schedule_calls.store(0, std::memory_order_relaxed);
  session->Drain();
  EXPECT_EQ(session->state(), SessionImpl::State::kClosing);

  session->OnClosed(google::cloud::Status());
  ASSERT_TRUE(cb_done.WaitFor(std::chrono::seconds(1)));

  local_cq.Shutdown();
  local_cq_thread.join();
}

TEST_F(SessionImplTest,
       PostWriteCancelDrainOnErrorGoAwayAndCloseDecrementsActiveRequests) {
  // 1. Drain via kError response frame.
  ASSERT_TRUE(session_->Acquire());
  VirtualRpcRequest req1;
  req1.set_rpc_id(101);
  Latch cb1_done;
  session_->StartVirtualRpc(
      nullptr, req1,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>) {
        cb1_done.Notify();
      });
  stream_->WaitForWrite(1);
  session_->OnWriteDone(true);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  EXPECT_EQ(session_->latency_tracker().active_requests(), 1);
  session_->CancelVirtualRpc(101);
  cb1_done.Wait();
  EXPECT_EQ(session_->latency_tracker().active_requests(), 1);

  SessionResponse err_resp;
  err_resp.mutable_error()->set_rpc_id(101);
  err_resp.mutable_error()->mutable_status()->set_code(
      grpc::StatusCode::UNAVAILABLE);
  session_->OnMessage(err_resp);
  listener_->WaitForSlotDrained(1);
  EXPECT_EQ(session_->latency_tracker().active_requests(), 0);

  // 2. Drain via kGoAway unadmitted frame.
  ASSERT_TRUE(session_->Acquire());
  VirtualRpcRequest req2;
  req2.set_rpc_id(105);
  Latch cb2_done;
  session_->StartVirtualRpc(
      nullptr, req2,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>) {
        cb2_done.Notify();
      });
  stream_->WaitForWrite(2);
  session_->OnWriteDone(true);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  session_->CancelVirtualRpc(105);
  cb2_done.Wait();
  EXPECT_EQ(session_->latency_tracker().active_requests(), 1);

  SessionResponse goaway_resp;
  goaway_resp.mutable_go_away()->set_last_rpc_id_admitted(101);
  session_->OnMessage(goaway_resp);
  listener_->WaitForSlotDrained(2);
  EXPECT_EQ(session_->latency_tracker().active_requests(), 0);

  // 3. Drain via Close() on a fresh session.
  auto stream_b = std::shared_ptr<MockSessionStream>(new MockSessionStream());
  auto listener_b = std::make_shared<MockSessionListener>();
  auto session_b = SessionImpl::CreateForTesting(stream_b, listener_b, cq_);
  ASSERT_TRUE(session_b->Acquire());
  VirtualRpcRequest req3;
  req3.set_rpc_id(201);
  Latch cb3_done;
  session_b->StartVirtualRpc(
      nullptr, req3,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>) {
        cb3_done.Notify();
      });
  stream_b->WaitForWrite(1);
  session_b->OnWriteDone(true);
  {
    Latch flushed;
    SessionImplTestPeer::RunAsync(*session_b, [&]() { flushed.Notify(); });
    flushed.Wait();
  }
  session_b->CancelVirtualRpc(201);
  cb3_done.Wait();
  EXPECT_EQ(session_b->latency_tracker().active_requests(), 1);
  session_b->Close();
  listener_b->WaitForSlotDrained(1);
  EXPECT_EQ(session_b->latency_tracker().active_requests(), 0);
}

TEST_F(SessionImplTest,
       LastClusterInfoCapturedFromVirtualRpcAndErrorResponses) {
  EXPECT_FALSE(session_->last_cluster_info().has_value());

  ASSERT_TRUE(session_->Acquire());
  VirtualRpcRequest req1;
  req1.set_rpc_id(1);
  Latch cb1_done;
  session_->StartVirtualRpc(
      nullptr, req1,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>) {
        cb1_done.Notify();
      });
  stream_->WaitForWrite(1);

  SessionResponse vrpc_resp;
  vrpc_resp.mutable_virtual_rpc()->set_rpc_id(1);
  vrpc_resp.mutable_virtual_rpc()->mutable_cluster_info()->set_cluster_id(
      "cluster-a");
  vrpc_resp.mutable_virtual_rpc()->mutable_cluster_info()->set_zone_id(
      "us-east1-b");
  session_->OnMessage(vrpc_resp);
  cb1_done.Wait();

  ASSERT_TRUE(session_->last_cluster_info().has_value());
  EXPECT_EQ(session_->last_cluster_info()->cluster_id(), "cluster-a");
  EXPECT_EQ(session_->last_cluster_info()->zone_id(), "us-east1-b");

  ASSERT_TRUE(session_->Acquire());
  VirtualRpcRequest req2;
  req2.set_rpc_id(2);
  Latch cb2_done;
  session_->StartVirtualRpc(
      nullptr, req2,
      [&](google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>) {
        cb2_done.Notify();
      });
  stream_->WaitForWrite(2);

  SessionResponse err_resp;
  err_resp.mutable_error()->set_rpc_id(2);
  err_resp.mutable_error()->mutable_status()->set_code(
      grpc::StatusCode::UNAVAILABLE);
  err_resp.mutable_error()->mutable_cluster_info()->set_cluster_id("cluster-b");
  err_resp.mutable_error()->mutable_cluster_info()->set_zone_id("us-west1-a");
  session_->OnMessage(err_resp);
  cb2_done.Wait();

  ASSERT_TRUE(session_->last_cluster_info().has_value());
  EXPECT_EQ(session_->last_cluster_info()->cluster_id(), "cluster-b");
  EXPECT_EQ(session_->last_cluster_info()->zone_id(), "us-west1-a");
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
