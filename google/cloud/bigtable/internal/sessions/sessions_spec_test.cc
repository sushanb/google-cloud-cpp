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
#include "google/cloud/bigtable/internal/sessions/client/session_client.h"
#include "google/cloud/bigtable/internal/sessions/latch.h"
#include "google/cloud/bigtable/internal/sessions/middleware/retry_oracle.h"
#include "google/cloud/bigtable/internal/sessions/middleware/retrying_vrpc.h"
#include "google/cloud/bigtable/internal/sessions/pool/pool_sizer.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_list.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_pool_impl.h"
#include "google/cloud/bigtable/internal/sessions/session/session_impl.h"
#include "google/cloud/bigtable/internal/sessions/session/session_state.h"
#include "google/cloud/bigtable/internal/sessions/session/write_status.h"
#include "google/cloud/bigtable/internal/sessions/sessions_data_connection.h"
#include "google/cloud/bigtable/internal/sessions/shim/client_directory.h"
#include "google/cloud/bigtable/internal/sessions/shim/diverter.h"
#include "google/cloud/bigtable/internal/sessions/telemetry/debug_snapshot.h"
#include "google/cloud/bigtable/internal/sessions/telemetry/session_tracer.h"
#include "google/cloud/bigtable/internal/sessions/transport/peer_info.h"
#include "google/cloud/bigtable/internal/sessions/transport/session_stream.h"
#include "google/cloud/bigtable/mocks/mock_data_connection.h"
#include "google/cloud/completion_queue.h"
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
#include "google/cloud/testing_util/mock_opentelemetry_metrics.h"
#include "google/cloud/testing_util/opentelemetry_attributes.h"
#endif
#include <google/bigtable/v2/data.pb.h>
#include <google/bigtable/v2/request_stats.pb.h>
#include <gmock/gmock.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

using ::testing::ElementsAre;

class SpecTestStream : public SessionStream {
 public:
  google::cloud::Status Write(
      google::bigtable::v2::SessionRequest request) override {
    {
      std::lock_guard<std::mutex> l(mu_);
      write_count_++;
      last_request_ = std::move(request);
      cv_.notify_all();
    }
    if (auto listener = listener_.lock()) {
      listener->OnWriteDone(true);
    }
    return {};
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

  std::mutex mu_;
  std::condition_variable cv_;
  int write_count_ = 0;
  google::bigtable::v2::SessionRequest last_request_;
  bool is_closed_ = false;
  bool is_cancelled_ = false;
  google::bigtable::v2::CloseSessionRequest::CloseSessionReason close_reason_{};
};

class SpecTestListener : public SessionListener {
 public:
  void OnSessionReady(
      std::shared_ptr<SessionImpl> session,
      google::bigtable::v2::OpenSessionResponse const&) override {
    std::lock_guard<std::mutex> l(mu_);
    std::int64_t afe_id = 0;
    if (auto p = session->peer_info(); p.has_value()) {
      afe_id = p->application_frontend_id;
    }
    events_.push_back("OnSessionReady:" + std::to_string(afe_id));
    is_ready_ = true;
    cv_.notify_all();
  }

  void OnGoAway(std::shared_ptr<SessionImpl>) override {
    std::lock_guard<std::mutex> l(mu_);
    events_.push_back("OnGoAway");
    go_away_count_++;
    cv_.notify_all();
  }

  void OnSessionClosed(std::shared_ptr<SessionImpl>,
                       google::cloud::Status) override {
    std::lock_guard<std::mutex> l(mu_);
    events_.push_back("OnSessionClosed");
    is_closed_ = true;
    cv_.notify_all();
  }

  void OnSlotDrained(std::shared_ptr<SessionImpl>,
                     std::int64_t rpc_id) override {
    std::lock_guard<std::mutex> l(mu_);
    events_.push_back("OnSlotDrained:" + std::to_string(rpc_id));
    slot_drained_count_++;
    cv_.notify_all();
  }

  int GetActiveCallsForStub(
      google::bigtable::v2::Bigtable::Stub const*) const override {
    return 0;
  }

  void WaitForReady() {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this] { return is_ready_; });
  }

  void WaitForGoAway(int expected = 1) {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this, expected] { return go_away_count_ >= expected; });
  }

  void WaitForSlotDrained(int expected = 1) {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this, expected] { return slot_drained_count_ >= expected; });
  }

  void WaitForClosed() {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this] { return is_closed_; });
  }

  std::mutex mu_;
  std::condition_variable cv_;
  std::vector<std::string> events_;
  bool is_ready_ = false;
  bool is_closed_ = false;
  int go_away_count_ = 0;
  int slot_drained_count_ = 0;
};

// ============================================================================
// Tier 2: SESSION_SPEC.md & SESSION_POOL_SPEC.md Behavioral Invariant Tests
// ============================================================================

TEST(SessionSpecTest, Invariant1_And_4_MonotonicFiveStateMachineAndHookOrder) {
  google::cloud::CompletionQueue cq;
  std::thread cq_thread([&cq]() { cq.Run(); });
  auto stream = std::make_shared<SpecTestStream>();
  auto listener = std::make_shared<SpecTestListener>();
  auto session = SessionImpl::CreateForTesting(stream, listener, cq);

  std::multimap<grpc::string_ref, grpc::string_ref> headers = {
      {"bigtable-peer-info-afe-id", "42"},
      {"bigtable-peer-info-subzone", "us-east1-b"},
      {"bigtable-peer-info-transport", "direct_access"},
  };
  session->OnInitialMetadata(headers);

  google::bigtable::v2::OpenSessionResponse open_resp;
  session->OnSessionReady(open_resp);
  listener->WaitForReady();
  EXPECT_EQ(session->state(), SessionImpl::State::kReady);
  ASSERT_TRUE(session->peer_info().has_value());
  EXPECT_EQ(session->peer_info()->application_frontend_id, 42);
  EXPECT_EQ(session->peer_info()->transport_type,
            transport::PeerInfo::TransportType::kSessionDirectAccess);

  google::bigtable::v2::SessionResponse goaway_resp;
  goaway_resp.mutable_go_away()->set_last_rpc_id_admitted(0);
  session->OnMessage(goaway_resp);
  listener->WaitForGoAway(1);
  stream->WaitForClose();
  EXPECT_EQ(
      stream->close_reason_,
      google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_GOAWAY);

  session->OnClosed(Status{});
  listener->WaitForClosed();
  EXPECT_EQ(session->state(), SessionImpl::State::kClosed);
  EXPECT_THAT(listener->events_,
              ElementsAre("OnSessionReady:42", "OnGoAway", "OnSessionClosed"));

  cq.Shutdown();
  cq_thread.join();
}

TEST(SessionSpecTest,
     Invariant2_MultiplexOneAndOnSlotDrainedAfterEarlyCallerCancel) {
  google::cloud::CompletionQueue cq;
  std::thread cq_thread([&cq]() { cq.Run(); });
  auto stream = std::make_shared<SpecTestStream>();
  auto listener = std::make_shared<SpecTestListener>();
  auto session = SessionImpl::CreateForTesting(stream, listener, cq);

  EXPECT_TRUE(session->Acquire());
  EXPECT_FALSE(session->Acquire());

  Latch cancel_done;
  Status rpc_status;
  google::bigtable::v2::VirtualRpcRequest req;
  req.set_rpc_id(101);
  session->StartVirtualRpc(
      nullptr, req,
      [&](StatusOr<google::bigtable::v2::VirtualRpcResponse> resp) {
        rpc_status = resp.status();
        cancel_done.Notify();
      });
  stream->WaitForWrite(1);

  session->CancelVirtualRpc(101);
  cancel_done.Wait();
  EXPECT_EQ(rpc_status.code(), StatusCode::kCancelled);
  EXPECT_TRUE(session->IsCancelledAwaitingDrain(101));
  EXPECT_FALSE(session->Acquire());

  google::bigtable::v2::SessionResponse vresp;
  vresp.mutable_virtual_rpc()->set_rpc_id(101);
  session->OnMessage(vresp);
  listener->WaitForSlotDrained(1);
  EXPECT_FALSE(session->IsCancelledAwaitingDrain(101));
  EXPECT_EQ(session->active_rpc_count(), 0);
  EXPECT_TRUE(session->Acquire());

  session->Close();
  cq.Shutdown();
  cq_thread.join();
}

TEST(SessionSpecTest,
     Invariant6_GoAwayDoesNotCancelActiveInFlightRpcAndAdvancesAfterDrain) {
  google::cloud::CompletionQueue cq;
  std::thread cq_thread([&cq]() { cq.Run(); });
  auto stream = std::make_shared<SpecTestStream>();
  auto listener = std::make_shared<SpecTestListener>();
  auto session = SessionImpl::CreateForTesting(stream, listener, cq);

  EXPECT_TRUE(session->Acquire());
  Latch rpc_done;
  StatusOr<google::bigtable::v2::VirtualRpcResponse> rpc_result;
  google::bigtable::v2::VirtualRpcRequest req;
  req.set_rpc_id(77);
  session->StartVirtualRpc(
      nullptr, req,
      [&](StatusOr<google::bigtable::v2::VirtualRpcResponse> resp) {
        rpc_result = std::move(resp);
        rpc_done.Notify();
      });
  stream->WaitForWrite(1);

  google::bigtable::v2::SessionResponse goaway_resp;
  goaway_resp.mutable_go_away()->set_last_rpc_id_admitted(77);
  session->OnMessage(goaway_resp);
  listener->WaitForGoAway(1);
  EXPECT_EQ(session->state(), SessionImpl::State::kDraining);
  EXPECT_FALSE(rpc_done.HasBeenNotified());

  google::bigtable::v2::SessionResponse vresp;
  vresp.mutable_virtual_rpc()->set_rpc_id(77);
  vresp.mutable_virtual_rpc()->set_payload("ok");
  session->OnMessage(vresp);
  rpc_done.Wait();
  EXPECT_TRUE(rpc_result.ok());
  stream->WaitForClose();
  EXPECT_EQ(
      stream->close_reason_,
      google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_GOAWAY);
  EXPECT_EQ(session->state(), SessionImpl::State::kClosing);

  session->OnClosed(Status{});
  listener->WaitForClosed();
  cq.Shutdown();
  cq_thread.join();
}

TEST(SessionSpecTest, Invariant9_ThreeStateRetryOracleOutcomes) {
  using middleware::EvaluateRetryOracle;
  using session::AttemptOutcomeState;
  auto const ms = [](std::int64_t v) { return std::chrono::milliseconds(v); };

  auto r1 = EvaluateRetryOracle(AttemptOutcomeState::kUncommitted,
                                /*is_idempotent=*/false,
                                /*has_server_retry_info=*/false, ms(0),
                                /*attempt_number=*/1, ms(500));
  EXPECT_TRUE(r1.should_retry);

  auto r2_non_idem = EvaluateRetryOracle(AttemptOutcomeState::kTransportFailure,
                                         /*is_idempotent=*/false,
                                         /*has_server_retry_info=*/false, ms(0),
                                         /*attempt_number=*/1, ms(500));
  EXPECT_FALSE(r2_non_idem.should_retry);

  auto r2_idem = EvaluateRetryOracle(AttemptOutcomeState::kTransportFailure,
                                     /*is_idempotent=*/true,
                                     /*has_server_retry_info=*/false, ms(0),
                                     /*attempt_number=*/1, ms(500));
  EXPECT_TRUE(r2_idem.should_retry);

  auto r3_no_info = EvaluateRetryOracle(AttemptOutcomeState::kServerResult,
                                        /*is_idempotent=*/true,
                                        /*has_server_retry_info=*/false, ms(10),
                                        /*attempt_number=*/1, ms(500));
  EXPECT_FALSE(r3_no_info.should_retry);

  auto r3_non_idem_with_info = EvaluateRetryOracle(
      AttemptOutcomeState::kServerResult, /*is_idempotent=*/false,
      /*has_server_retry_info=*/true, ms(25), /*attempt_number=*/2, ms(500));
  EXPECT_FALSE(r3_non_idem_with_info.should_retry);

  auto r3_with_info = EvaluateRetryOracle(
      AttemptOutcomeState::kServerResult, /*is_idempotent=*/true,
      /*has_server_retry_info=*/true, ms(25), /*attempt_number=*/2, ms(500));
  EXPECT_TRUE(r3_with_info.should_retry);
  EXPECT_EQ(r3_with_info.backoff_delay, ms(25));

  auto r3_capped = EvaluateRetryOracle(
      AttemptOutcomeState::kServerResult, /*is_idempotent=*/true,
      /*has_server_retry_info=*/true, ms(10), /*attempt_number=*/3, ms(500));
  EXPECT_FALSE(r3_capped.should_retry);
}

TEST(SessionPoolSpecTest,
     InvariantI5_And_I6_And_BudgetTrackingUnderGoAwayDuringStarting) {
  pool::SessionList list;

  auto h_starting = list.NewStartingHandle(1);
  EXPECT_EQ(list.sessions_holding_budget_count(), 1u);
  EXPECT_EQ(list.expected_capacity(), 1u);
  list.OnSessionClosed(h_starting);
  EXPECT_EQ(list.sessions_holding_budget_count(), 0u);
  EXPECT_EQ(list.expected_capacity(), 0u);

  auto h_active = list.NewStartingHandle(2);
  list.OnSessionReady(h_active, /*peer_id=*/99);
  EXPECT_EQ(list.sessions_holding_budget_count(), 0u);
  EXPECT_EQ(list.ready_count(), 1u);
  EXPECT_EQ(list.peer_ref_count(99), 1u);

  auto checked_out = list.CheckoutFromPeer(99);
  ASSERT_EQ(checked_out, h_active);
  EXPECT_EQ(list.ready_count(), 0u);

  list.OnSessionClosing(checked_out);
  EXPECT_EQ(list.peer_ref_count(99), 1u);
  EXPECT_FALSE(list.ReleaseToPool(checked_out));
  EXPECT_EQ(list.ready_count(), 0u);

  list.OnSessionClosed(checked_out);
  EXPECT_EQ(list.peer_ref_count(99), 0u);
}

TEST(SessionPoolSpecTest, Invariant5_ServerDrivenPoolSizerFormula) {
  EXPECT_EQ(pool::ComputeDesiredPoolCapacity(
                /*in_use_count=*/10, /*pending_count=*/25,
                /*new_session_queue_length=*/10, /*headroom_ratio=*/0.2,
                /*min_sessions=*/4, /*max_sessions=*/64),
            16u);

  PoolSizer sizer(/*min_sessions=*/4, /*max_sessions=*/64, /*headroom=*/0.20f,
                  /*new_session_queue_length=*/10);
  EXPECT_EQ(sizer.GetScaleDelta(/*ready_count=*/10, /*connecting_count=*/2,
                                /*in_use_count=*/10, /*pending_count=*/25),
            4);

  EXPECT_EQ(pool::ComputeDesiredPoolCapacity(0, 0, 10, 0.2, 4, 64), 4u);
  EXPECT_EQ(sizer.GetScaleDelta(/*ready_count=*/1, /*connecting_count=*/0,
                                /*in_use_count=*/0, /*pending_count=*/0),
            3);

  EXPECT_EQ(pool::ComputeDesiredPoolCapacity(100, 50, 10, 0.2, 4, 64), 64u);
  EXPECT_EQ(sizer.GetScaleDelta(/*ready_count=*/20, /*connecting_count=*/4,
                                /*in_use_count=*/100, /*pending_count=*/50),
            40);
}

TEST(SessionPoolSpecTest, PeakEwma_OkGatedAndSubtractsPoolWait) {
  pool::OkGatedPeakEwma tracker(/*alpha=*/0.5);
  EXPECT_DOUBLE_EQ(tracker.e2e_ewma_us(), 1000.0);
  EXPECT_DOUBLE_EQ(tracker.transport_ewma_us(), 500.0);

  tracker.RecordOutcome(Status(StatusCode::kUnavailable, "fast fail"),
                        /*total_elapsed_us=*/10.0, /*pool_wait_us=*/0.0,
                        /*backend_latency_us=*/5.0);
  EXPECT_DOUBLE_EQ(tracker.e2e_ewma_us(), 1000.0);

  tracker.RecordOutcome(Status{}, /*total_elapsed_us=*/2000.0,
                        /*pool_wait_us=*/1400.0, /*backend_latency_us=*/400.0);
  EXPECT_DOUBLE_EQ(tracker.e2e_ewma_us(), 800.0);
}

// ============================================================================
// Tier 3: Multi-Threaded Concurrency & Race Verification Tests (TSAN / ASAN)
// ============================================================================

TEST(SessionConcurrencyStressTest,
     ConcurrentCheckoutReleaseGoAwayAndBudgetInvariants) {
  pool::SessionList list;
  constexpr int kNumThreads = 8;
  constexpr int kIterationsPerThread = 250;

  std::vector<std::thread> workers;
  workers.reserve(kNumThreads);
  for (int t = 0; t < kNumThreads; ++t) {
    workers.emplace_back([&, t] {
      std::int64_t const peer_id = (t % 4) + 1;
      std::vector<std::shared_ptr<pool::SessionList::SessionHandle>>
          owned_handles;
      owned_handles.reserve(kIterationsPerThread);

      for (int i = 0; i < kIterationsPerThread; ++i) {
        std::int64_t const sid = t * 100000 + i;
        auto h = list.NewStartingHandle(sid);
        if (i % 7 == 0) {
          list.OnSessionClosed(h);
          continue;
        }
        list.OnSessionReady(h, peer_id);
        owned_handles.push_back(h);

        auto out = list.CheckoutFromPeer(peer_id);
        if (out != nullptr) {
          if (i % 5 == 0) {
            list.OnSessionClosing(out);
            EXPECT_FALSE(list.ReleaseToPool(out));
          } else {
            (void)list.ReleaseToPool(out);
          }
        }
      }

      for (auto const& h : owned_handles) {
        list.OnSessionClosing(h);
        list.OnSessionClosed(h);
      }
    });
  }
  for (auto& w : workers) w.join();

  EXPECT_EQ(list.sessions_holding_budget_count(), 0u);
  EXPECT_EQ(list.ready_count(), 0u);
  EXPECT_EQ(list.expected_capacity(), 0u);
  for (std::int64_t peer_id = 1; peer_id <= 4; ++peer_id) {
    EXPECT_EQ(list.peer_ref_count(peer_id), 0u);
  }
}

TEST(SessionsOptionAndShimTest,
     EnableSessionsOptionAndDiverterAndRoutabilityInvariants) {
  shim::Diverter diverter(/*initial_session_load=*/0.5);
  EXPECT_TRUE(diverter.UseSession(0.25));
  EXPECT_FALSE(diverter.UseSession(0.75));
  EXPECT_EQ(diverter.session_picks(), 1u);
  EXPECT_EQ(diverter.classic_picks(), 1u);

  diverter.TripUnimplementedBreaker();
  EXPECT_TRUE(diverter.unimplemented_tripped());
  EXPECT_DOUBLE_EQ(diverter.session_load(), 0.0);
  EXPECT_FALSE(diverter.UseSession(0.1));
  EXPECT_FALSE(diverter.UseSession());
  diverter.SetSessionLoad(0.8);
  EXPECT_DOUBLE_EQ(diverter.session_load(), 0.0);
  diverter.ResetUnimplementedBreaker();
  EXPECT_FALSE(diverter.unimplemented_tripped());
  EXPECT_DOUBLE_EQ(diverter.session_load(), 0.8);
  EXPECT_TRUE(diverter.UseSession(0.5));

  EXPECT_TRUE(shim::IsRoutableToSessions(shim::TableOperationKind::kReadRow));
  EXPECT_TRUE(shim::IsRoutableToSessions(
      shim::TableOperationKind::kApplyNonConditional));
  EXPECT_FALSE(
      shim::IsRoutableToSessions(shim::TableOperationKind::kReadRowsScan));
  EXPECT_FALSE(
      shim::IsRoutableToSessions(shim::TableOperationKind::kBulkApply));

  auto const table_read_key =
      client::MakePoolKey(client::SessionResourceType::kTable, "t1", "prof-a",
                          client::PoolPermission::kRead);
  ASSERT_TRUE(table_read_key.ok());
  EXPECT_EQ(*table_read_key, "table:t1:prof-a:READ");

  auto const av_write_key =
      client::MakePoolKey(client::SessionResourceType::kAuthorizedView, "av1",
                          "", client::PoolPermission::kWrite);
  ASSERT_TRUE(av_write_key.ok());
  EXPECT_EQ(*av_write_key, "av:av1:default:WRITE");

  auto const mv_read_key =
      client::MakePoolKey(client::SessionResourceType::kMaterializedView, "mv1",
                          client::PoolPermission::kRead);
  ASSERT_TRUE(mv_read_key.ok());
  EXPECT_EQ(*mv_read_key, "mv:mv1:default:READ");

  EXPECT_FALSE(
      client::MakePoolKey(client::SessionResourceType::kMaterializedView, "mv1",
                          client::PoolPermission::kWrite)
          .ok());
}

TEST(SessionSpecTest,
     DuplicateGoAwayInDrainingOrClosingDoesNotRegressStateOrDuplicateCallback) {
  google::cloud::CompletionQueue cq;
  std::thread cq_thread([&cq]() { cq.Run(); });
  auto stream = std::make_shared<SpecTestStream>();
  auto listener = std::make_shared<SpecTestListener>();
  auto session = SessionImpl::CreateForTesting(stream, listener, cq);

  EXPECT_TRUE(session->Acquire());
  Latch rpc_done;
  google::bigtable::v2::VirtualRpcRequest req;
  req.set_rpc_id(50);
  session->StartVirtualRpc(
      nullptr, req, [&](StatusOr<google::bigtable::v2::VirtualRpcResponse>) {
        rpc_done.Notify();
      });
  stream->WaitForWrite(1);

  google::bigtable::v2::SessionResponse goaway1;
  goaway1.mutable_go_away()->set_last_rpc_id_admitted(50);
  session->OnMessage(goaway1);
  listener->WaitForGoAway(1);
  EXPECT_EQ(session->state(), SessionImpl::State::kDraining);

  session->OnMessage(goaway1);
  EXPECT_EQ(session->state(), SessionImpl::State::kDraining);
  {
    std::lock_guard<std::mutex> l(listener->mu_);
    EXPECT_EQ(listener->go_away_count_, 1);
  }

  google::bigtable::v2::SessionResponse vresp;
  vresp.mutable_virtual_rpc()->set_rpc_id(50);
  session->OnMessage(vresp);
  rpc_done.Wait();
  stream->WaitForClose();
  EXPECT_EQ(session->state(), SessionImpl::State::kClosing);

  session->OnMessage(goaway1);
  session->OnClosed(Status{});
  listener->WaitForClosed();
  EXPECT_EQ(session->state(), SessionImpl::State::kClosed);
  {
    std::lock_guard<std::mutex> l(listener->mu_);
    EXPECT_EQ(listener->go_away_count_, 1);
  }

  cq.Shutdown();
  cq_thread.join();
}

TEST(SessionsOptionAndShimTest,
     ClientDirectoryAppProfilesAndAggregateIdempotency) {
  shim::ClientDirectory<int> dir;
  dir.Insert("projects/p/instances/i/tables/t1", 1, 100, "profile-a");
  dir.Insert("projects/p/instances/i/tables/t1", 1, 200, "profile-b");

  auto const* ea = dir.Find("projects/p/instances/i/tables/t1", 1, "profile-a");
  auto const* eb = dir.Find("projects/p/instances/i/tables/t1", 1, "profile-b");
  auto const* ed = dir.Find("projects/p/instances/i/tables/t1", 1, "default");
  ASSERT_NE(ea, nullptr);
  ASSERT_NE(eb, nullptr);
  EXPECT_EQ(ed, nullptr);
  EXPECT_EQ(ea->value, 100);
  EXPECT_EQ(eb->value, 200);

  google::bigtable::v2::Mutation add_mut;
  add_mut.mutable_add_to_cell()->set_family_name("cf");
  EXPECT_FALSE(client::IsIdempotentMutation(add_mut));

  google::bigtable::v2::Mutation merge_mut;
  merge_mut.mutable_merge_to_cell()->set_family_name("cf");
  EXPECT_FALSE(client::IsIdempotentMutation(merge_mut));
}

TEST(SessionsOptionAndShimTest,
     PoolRejectionAndMaterializedViewWriteRejection) {
  Status rejected = google::cloud::internal::ResourceExhaustedError(
      "Session pool queuing capacity fully exhausted");
  google::cloud::internal::SetPayload(rejected, kWriteStatusPayloadUrl, "0");
  MarkPoolRejected(rejected);
  EXPECT_TRUE(IsPoolRejected(rejected));
  auto const decision = middleware::EvaluateRetryOracle(
      session::AttemptOutcomeState::kRejected, /*is_idempotent=*/true,
      /*has_server_retry_info=*/true,
      /*server_retry_delay=*/std::chrono::milliseconds(0),
      /*attempt_number=*/1,
      /*remaining_deadline=*/std::chrono::milliseconds(10000));
  EXPECT_FALSE(decision.should_retry);

  auto const mv_resource = "projects/p/instances/i/materializedViews/mv1";
  auto const mv_write_key =
      client::MakePoolKey(client::SessionResourceType::kMaterializedView,
                          mv_resource, client::PoolPermission::kWrite);
  EXPECT_EQ(mv_write_key.status().code(), StatusCode::kInvalidArgument);
}

TEST(SessionsOptionAndShimTest,
     ErrorResponseClusterInfoAndSessionTracerTableTypePreservation) {
  google::cloud::CompletionQueue cq;
  std::thread cq_thread([&cq]() { cq.Run(); });
  auto stream = std::make_shared<SpecTestStream>();
  auto listener = std::make_shared<SpecTestListener>();
  auto session = SessionImpl::CreateForTesting(stream, listener, cq);

  auto pool = SessionPoolImpl::CreateForTesting({session});
  auto client = Client::CreateForTesting(pool);

  middleware::RetryingVRpc::AttemptTelemetry captured_telemetry;
  middleware::RetryingVRpc::CallPolicy policy;
  policy.on_attempt_complete =
      [&](middleware::RetryingVRpc::AttemptTelemetry const& t) {
        captured_telemetry = t;
      };

  Latch rpc_done;
  StatusOr<google::bigtable::v2::SessionReadRowResponse> read_resp;
  google::bigtable::v2::SessionReadRowRequest req;
  req.set_key("row-err-cluster");
  client->ReadRowAsync(
      nullptr, req,
      [&](StatusOr<google::bigtable::v2::SessionReadRowResponse> resp) {
        read_resp = std::move(resp);
        rpc_done.Notify();
      },
      std::move(policy));
  stream->WaitForWrite(1);

  google::bigtable::v2::SessionResponse err_resp;
  auto* err = err_resp.mutable_error();
  err->set_rpc_id(stream->last_request_.virtual_rpc().rpc_id());
  err->mutable_status()->set_code(grpc::StatusCode::PERMISSION_DENIED);
  err->mutable_status()->set_message("denied on cluster");
  err->mutable_cluster_info()->set_cluster_id("cluster-err-1");
  err->mutable_cluster_info()->set_zone_id("us-east1-b");
  session->OnMessage(err_resp);

  ASSERT_TRUE(rpc_done.WaitFor(std::chrono::seconds(1)));
  EXPECT_EQ(read_resp.status().code(), StatusCode::kPermissionDenied);
  EXPECT_EQ(captured_telemetry.cluster_id, "cluster-err-1");
  EXPECT_EQ(captured_telemetry.zone_id, "us-east1-b");
  ASSERT_TRUE(client->cluster_info().has_value());
  EXPECT_EQ(client->cluster_info()->cluster_id(), "cluster-err-1");
  EXPECT_EQ(client->cluster_info()->zone_id(), "us-east1-b");

  auto write_table_pool = SessionPoolImpl::Create(
      {}, "projects/p/instances/i/tables/t1", "prof1",
      google::bigtable::v2::OpenTableRequest::PERMISSION_WRITE, Options{},
      nullptr);
  ASSERT_NE(write_table_pool, nullptr);
  EXPECT_EQ(write_table_pool->session_tracer().config().session_type,
            "SESSION_TYPE_TABLE");
  EXPECT_EQ(write_table_pool->session_tracer().config().session_name,
            "projects/p/instances/i/tables/t1/prof1/WRITE");
  write_table_pool->Close();

  session->Close();
  pool->Close();
  cq.Shutdown();
  cq_thread.join();
}

#if !defined(_WIN32) && !defined(__MACH__)
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
TEST(SessionsOptionAndShimTest, SessionTracerClientSideMetricsAttributes) {
  using ::google::cloud::testing_util::MakeAttributesMap;
  using ::google::cloud::testing_util::MockHistogram;
  using ::google::cloud::testing_util::MockMeter;
  using ::google::cloud::testing_util::MockMeterProvider;
  using ::testing::A;
  using ::testing::Eq;

  auto open_hist = std::make_unique<MockHistogram<double>>();
  auto dur_hist = std::make_unique<MockHistogram<double>>();
  auto uptime_hist = std::make_unique<MockHistogram<double>>();
  auto transport_hist = std::make_unique<MockHistogram<double>>();

  EXPECT_CALL(
      *open_hist,
      Record(A<double>(), A<opentelemetry::common::KeyValueIterable const&>(),
             A<opentelemetry::context::Context const&>()))
      .WillOnce([](double value,
                   opentelemetry::common::KeyValueIterable const& attrs,
                   opentelemetry::context::Context const&) {
        EXPECT_THAT(value, Eq(12.5));
        auto map = MakeAttributesMap(attrs);
        EXPECT_THAT(map["status"], Eq("OK"));
        EXPECT_THAT(map["session_type"], Eq("SESSION_TYPE_TABLE"));
        EXPECT_THAT(map["session_name"], Eq("t1/default/READ"));
        EXPECT_THAT(map["transport_type"],
                    Eq("transport_type_session_direct_access"));
        EXPECT_THAT(map["afe_location"], Eq("us-central1-a"));
      });

  EXPECT_CALL(
      *dur_hist,
      Record(A<double>(), A<opentelemetry::common::KeyValueIterable const&>(),
             A<opentelemetry::context::Context const&>()))
      .WillOnce([](double value,
                   opentelemetry::common::KeyValueIterable const& attrs,
                   opentelemetry::context::Context const&) {
        EXPECT_THAT(value, Eq(60000.0));
        auto map = MakeAttributesMap(attrs);
        EXPECT_THAT(map["closing_reason"], Eq("GO_AWAY"));
        EXPECT_THAT(map["vrpcs"], Eq("<10,1>"));
        EXPECT_THAT(map["ready"], Eq("true"));
        EXPECT_THAT(map["session_type"], Eq("SESSION_TYPE_TABLE"));
        EXPECT_THAT(map["session_name"], Eq("t1/default/READ"));
      });

  EXPECT_CALL(
      *uptime_hist,
      Record(A<double>(), A<opentelemetry::common::KeyValueIterable const&>(),
             A<opentelemetry::context::Context const&>()))
      .WillOnce([](double value,
                   opentelemetry::common::KeyValueIterable const& attrs,
                   opentelemetry::context::Context const&) {
        EXPECT_THAT(value, Eq(30000.0));
        auto map = MakeAttributesMap(attrs);
        EXPECT_THAT(map["ready"], Eq("true"));
        EXPECT_THAT(map["session_type"], Eq("SESSION_TYPE_TABLE"));
      });

  EXPECT_CALL(
      *transport_hist,
      Record(A<double>(), A<opentelemetry::common::KeyValueIterable const&>(),
             A<opentelemetry::context::Context const&>()))
      .WillOnce([](double value,
                   opentelemetry::common::KeyValueIterable const& attrs,
                   opentelemetry::context::Context const&) {
        EXPECT_THAT(value, Eq(1.75));
        auto map = MakeAttributesMap(attrs);
        EXPECT_THAT(map["method"], Eq("Bigtable.ReadRow"));
        EXPECT_THAT(map["transport_type"],
                    Eq("transport_type_session_direct_access"));
      });

  opentelemetry::nostd::shared_ptr<MockMeter> mock_meter =
      std::make_shared<MockMeter>();
  EXPECT_CALL(*mock_meter, CreateDoubleHistogram)
      .WillOnce([h = std::move(open_hist)](
                    opentelemetry::nostd::string_view name,
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view unit) mutable {
        EXPECT_THAT(name, Eq("session/open_latencies"));
        EXPECT_THAT(unit, Eq("ms"));
        return std::move(h);
      })
      .WillOnce([h = std::move(dur_hist)](
                    opentelemetry::nostd::string_view name,
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view unit) mutable {
        EXPECT_THAT(name, Eq("session/durations"));
        EXPECT_THAT(unit, Eq("ms"));
        return std::move(h);
      })
      .WillOnce([h = std::move(uptime_hist)](
                    opentelemetry::nostd::string_view name,
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view unit) mutable {
        EXPECT_THAT(name, Eq("session/uptime"));
        EXPECT_THAT(unit, Eq("ms"));
        return std::move(h);
      })
      .WillOnce([h = std::move(transport_hist)](
                    opentelemetry::nostd::string_view name,
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view unit) mutable {
        EXPECT_THAT(name, Eq("transport_latencies"));
        EXPECT_THAT(unit, Eq("ms"));
        return std::move(h);
      });

  opentelemetry::nostd::shared_ptr<MockMeterProvider> mock_provider =
      std::make_shared<MockMeterProvider>();
  EXPECT_CALL(*mock_provider, GetMeter).WillOnce(::testing::Return(mock_meter));

  telemetry::SessionTracerConfig cfg;
  cfg.meter_provider = mock_provider;
  cfg.project_id = "p1";
  cfg.instance_id = "i1";
  cfg.app_profile_id = "default";
  cfg.client_name = "cpp.Bigtable/test";
  cfg.client_uid = "uid-1";
  cfg.session_type = "SESSION_TYPE_TABLE";
  cfg.session_name = "t1/default/READ";

  telemetry::SessionTracer tracer;
  tracer.Configure(std::move(cfg));

  telemetry::OpenLatencyAttributes open_attrs;
  open_attrs.status = "OK";
  open_attrs.transport_type = "transport_type_session_direct_access";
  open_attrs.afe_location = "us-central1-a";
  tracer.RecordSessionOpenLatencyMs(12.5, open_attrs);

  telemetry::DurationAttributes dur_attrs;
  dur_attrs.closing_reason = "GO_AWAY";
  dur_attrs.vrpcs = "<10,1>";
  dur_attrs.ready = "true";
  dur_attrs.transport_type = "transport_type_session_direct_access";
  dur_attrs.afe_location = "us-central1-a";
  tracer.RecordSessionDurationMs(60000.0, dur_attrs);

  telemetry::UptimeAttributes up_attrs;
  up_attrs.ready = "true";
  up_attrs.transport_type = "transport_type_session_direct_access";
  up_attrs.afe_location = "us-central1-a";
  tracer.RecordSessionUptimeMs(30000.0, up_attrs);

  telemetry::TransportLatencyAttributes tr_attrs;
  tr_attrs.method = "Bigtable.ReadRow";
  tr_attrs.transport_type = "transport_type_session_direct_access";
  tr_attrs.afe_location = "us-central1-a";
  tracer.RecordTransportLatencyMs(1.75, tr_attrs);
}
#endif  // GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
#endif  // !defined(_WIN32) && !defined(__MACH__)

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
