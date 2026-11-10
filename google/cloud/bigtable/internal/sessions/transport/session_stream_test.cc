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

#include "google/cloud/bigtable/internal/sessions/transport/session_stream.h"
#include "google/cloud/bigtable/internal/sessions/transport/peer_info.h"
#include "google/cloud/completion_queue.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "absl/strings/escaping.h"
#include <google/bigtable/v2/peer_info.pb.h>
#include <google/bigtable/v2/session.pb.h>
#include <google/protobuf/unknown_field_set.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/string_ref.h>
#include <gtest/gtest.h>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

using ::google::bigtable::v2::CloseSessionRequest;
using ::google::bigtable::v2::OpenSessionResponse;
using ::google::bigtable::v2::SessionResponse;

class MockStreamListenerForTest : public SessionStreamListener {
 public:
  void OnInitialMetadata(
      std::multimap<grpc::string_ref, grpc::string_ref> const&) override {
    std::lock_guard<std::mutex> l(mu_);
    initial_metadata_calls_++;
    cv_.notify_all();
  }

  void OnSessionReady(OpenSessionResponse const&) override {
    std::lock_guard<std::mutex> l(mu_);
    session_ready_calls_++;
    cv_.notify_all();
  }

  void OnMessage(SessionResponse) override {
    std::lock_guard<std::mutex> l(mu_);
    message_calls_++;
    cv_.notify_all();
  }

  void OnHeartbeat() override {
    std::lock_guard<std::mutex> l(mu_);
    heartbeat_calls_++;
    cv_.notify_all();
  }

  void OnWriteDone(bool) override {
    std::lock_guard<std::mutex> l(mu_);
    write_done_calls_++;
    cv_.notify_all();
  }

  void OnClosed(google::cloud::Status status) override {
    std::lock_guard<std::mutex> l(mu_);
    closed_calls_++;
    last_status_ = std::move(status);
    cv_.notify_all();
  }

  void WaitForInitialMetadata(int expected) {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l,
             [this, expected] { return initial_metadata_calls_ >= expected; });
  }

  void WaitForSessionReady(int expected) {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this, expected] { return session_ready_calls_ >= expected; });
  }

  void WaitForMessage(int expected) {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this, expected] { return message_calls_ >= expected; });
  }

  void WaitForWriteDone(int expected) {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this, expected] { return write_done_calls_ >= expected; });
  }

  void WaitForClosed(int expected) {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this, expected] { return closed_calls_ >= expected; });
  }

  std::mutex mu_;
  std::condition_variable cv_;
  int initial_metadata_calls_ = 0;
  int session_ready_calls_ = 0;
  int message_calls_ = 0;
  int heartbeat_calls_ = 0;
  int write_done_calls_ = 0;
  int closed_calls_ = 0;
  google::cloud::Status last_status_;
};

// Subclass exposing protected reactor handling interfaces to verify line
// coverage.
class TestableSessionStream : public SessionStream {
 public:
  explicit TestableSessionStream(std::weak_ptr<SessionStreamListener> listener)
      : SessionStream(nullptr, std::move(listener),
                      google::cloud::CompletionQueue{}) {}

  void ExecuteOnReadInitialMetadataDone(bool ok) {
    OnReadInitialMetadataDone(ok);
  }

  void OnReadInitialMetadataDone(bool ok) override {
    if (ok) {
      std::multimap<grpc::string_ref, grpc::string_ref> dummy;
      if (auto l = listener_.lock()) {
        l->OnInitialMetadata(dummy);
      }
    } else {
      SessionStream::OnReadInitialMetadataDone(false);
    }
  }

  bool IsTerminated() const { return terminated_; }

  void ExecuteOnReadDone(bool ok, SessionResponse resp) {
    bool const old_closed = is_closed_.load();
    current_response_ = std::move(resp);
    is_closed_.store(true);
    OnReadDone(ok);
    is_closed_.store(old_closed);
  }

  void ExecuteOnWriteDone(bool ok) { OnWriteDone(ok); }

  void ExecuteOnDone(grpc::Status const& status) { OnDone(status); }

  void SetClosed(bool closed) { is_closed_.store(closed); }
  bool IsClosed() const { return is_closed_.load(); }
  void SetWriteInFlight(bool in_flight) { write_in_flight_.store(in_flight); }
  void SetOpenWriteInFlight(bool in_flight) {
    open_write_in_flight_ = in_flight;
  }
  bool HasPendingCloseReason() const {
    return pending_close_reason_.has_value();
  }
  bool IsClosingWriteSent() const { return closing_write_sent_; }
  google::bigtable::v2::SessionRequest const& current_write_request() const {
    return current_write_request_;
  }
  void SetRefreshConfig(google::bigtable::v2::SessionRefreshConfig cfg) {
    refresh_config_ = std::move(cfg);
  }
  void SetMetadataHook(MetadataHook hook) { metadata_hook_ = std::move(hook); }
  void ExecuteStartOpen() {
    StartOpen("projects/p/instances/i/tables/t", "default",
              google::bigtable::v2::OpenTableRequest::PERMISSION_READ);
  }
  void ExecuteStartOpenWithName(std::string resource_name,
                                std::string app_profile_id) {
    StartOpen(std::move(resource_name), std::move(app_profile_id),
              google::bigtable::v2::OpenTableRequest::PERMISSION_READ);
  }
};

class SessionStreamTest : public ::testing::Test {
 protected:
  void SetUp() override {
    listener_ = std::make_shared<MockStreamListenerForTest>();
    stream_ = std::make_shared<TestableSessionStream>(listener_);
  }

  std::shared_ptr<MockStreamListenerForTest> listener_;
  std::shared_ptr<TestableSessionStream> stream_;
};

TEST_F(SessionStreamTest, WriteRejectsWhenStreamClosedOrBusy) {
  stream_->SetClosed(true);
  EXPECT_EQ(stream_->Write(google::bigtable::v2::SessionRequest()).code(),
            google::cloud::StatusCode::kUnavailable);

  stream_->SetClosed(false);
  stream_->SetWriteInFlight(true);
  EXPECT_EQ(stream_->Write(google::bigtable::v2::SessionRequest()).code(),
            google::cloud::StatusCode::kUnavailable);
}

TEST_F(SessionStreamTest, CloseGracefullySkipsWhenAlreadyClosed) {
  stream_->SetClosed(true);
  stream_->Close(CloseSessionRequest::CLOSE_SESSION_REASON_UNSET);
  EXPECT_EQ(listener_->closed_calls_, 0);
}

TEST_F(SessionStreamTest, CloseAttemptsCancellationWhenWritesSaturated) {
  stream_->SetClosed(false);
  stream_->SetWriteInFlight(true);
  stream_->Close(CloseSessionRequest::CLOSE_SESSION_REASON_UNSET);
}

TEST_F(SessionStreamTest, CancelAssertsClosureStateDirectly) {
  stream_->Cancel();
}

TEST_F(SessionStreamTest, RoutesInitialMetadataDoneEventsSuccess) {
  stream_->ExecuteOnReadInitialMetadataDone(true);
  listener_->WaitForInitialMetadata(1);
  EXPECT_EQ(listener_->initial_metadata_calls_, 1);
  EXPECT_FALSE(stream_->IsTerminated());
}

TEST_F(SessionStreamTest, RoutesInitialMetadataDoneEventsFailure) {
  EXPECT_FALSE(stream_->IsTerminated());
  stream_->ExecuteOnReadInitialMetadataDone(false);
  EXPECT_EQ(listener_->initial_metadata_calls_, 0);
  EXPECT_TRUE(stream_->IsTerminated());
}

TEST_F(SessionStreamTest, RoutesReadDoneSessionReadyEnvelope) {
  SessionResponse resp;
  resp.mutable_open_session();

  stream_->ExecuteOnReadDone(false, resp);
  EXPECT_EQ(listener_->session_ready_calls_, 0);
  EXPECT_EQ(listener_->heartbeat_calls_, 0);

  stream_->ExecuteOnReadDone(true, resp);
  listener_->WaitForSessionReady(1);
  EXPECT_EQ(listener_->session_ready_calls_, 1);
  EXPECT_EQ(listener_->message_calls_, 0);
  EXPECT_EQ(listener_->heartbeat_calls_, 1);
}

TEST_F(SessionStreamTest, RoutesReadDoneStandardMessageEnvelope) {
  SessionResponse resp;
  resp.mutable_heartbeat();

  stream_->ExecuteOnReadDone(true, resp);
  listener_->WaitForMessage(1);
  EXPECT_EQ(listener_->session_ready_calls_, 0);
  EXPECT_EQ(listener_->message_calls_, 1);
  EXPECT_EQ(listener_->heartbeat_calls_, 1);
}

TEST_F(SessionStreamTest, RoutesWriteDoneStateReclamation) {
  stream_->SetWriteInFlight(true);
  stream_->ExecuteOnWriteDone(true);
  listener_->WaitForWriteDone(1);
  EXPECT_EQ(listener_->write_done_calls_, 1);
}

TEST_F(SessionStreamTest, RoutesOnDoneTerminalStatusMappings) {
  stream_->ExecuteOnDone(grpc::Status::OK);
  listener_->WaitForClosed(1);
  EXPECT_EQ(listener_->closed_calls_, 1);
  EXPECT_TRUE(listener_->last_status_.ok());

  stream_->ExecuteOnDone(
      grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "Access rejected"));
  listener_->WaitForClosed(2);
  EXPECT_EQ(listener_->closed_calls_, 2);
  EXPECT_EQ(listener_->last_status_.code(),
            google::cloud::StatusCode::kPermissionDenied);
  EXPECT_EQ(listener_->last_status_.message(), "Access rejected");
}

TEST_F(SessionStreamTest, CancelBeforeStartOpenNotifiesOnClosedWithoutLeak) {
  stream_->Cancel();
  stream_->ExecuteStartOpen();
  listener_->WaitForClosed(1);
  EXPECT_EQ(listener_->closed_calls_, 1);
  EXPECT_EQ(listener_->last_status_.code(),
            google::cloud::StatusCode::kCancelled);
}

TEST_F(SessionStreamTest, DefersOpenSessionReadyUntilOpenWriteCompletes) {
  stream_->SetOpenWriteInFlight(true);
  stream_->SetWriteInFlight(true);

  SessionResponse resp;
  resp.mutable_open_session();
  stream_->ExecuteOnReadDone(true, resp);
  // OpenSessionResponse arrived before OpenSessionRequest OnWriteDone:
  // OnSessionReady must be deferred until OnWriteDone(true).
  EXPECT_EQ(listener_->session_ready_calls_, 0);
  EXPECT_EQ(listener_->heartbeat_calls_, 1);

  stream_->ExecuteOnWriteDone(true);
  listener_->WaitForSessionReady(1);
  EXPECT_EQ(listener_->session_ready_calls_, 1);
}

TEST_F(SessionStreamTest,
       CloseQueuesPendingCloseReasonAtomicallyWithWriteDone) {
  stream_->SetClosed(false);
  stream_->SetWriteInFlight(true);

  stream_->Close(CloseSessionRequest::CLOSE_SESSION_REASON_USER);
  EXPECT_TRUE(stream_->IsClosed());
  stream_->Close(CloseSessionRequest::CLOSE_SESSION_REASON_USER);
  EXPECT_TRUE(stream_->IsClosed());
}

TEST_F(SessionStreamTest,
       StartOpenSetsProtocolVersionOneAndFeatureFlagsAndDedupesRequestParams) {
  google::bigtable::v2::SessionRefreshConfig cfg;
  auto* md = cfg.add_metadata();
  md->set_key("x-goog-request-params");
  md->set_value("custom_refresh_params=1");
  stream_->SetRefreshConfig(cfg);
  stream_->ExecuteStartOpen();

  auto const& req = stream_->current_write_request();
  ASSERT_TRUE(req.has_open_session());
  EXPECT_EQ(req.open_session().protocol_version(), 1);
  EXPECT_TRUE(req.open_session().flags().sessions_compatible());
  EXPECT_TRUE(req.open_session().flags().sessions_required());
  EXPECT_TRUE(req.open_session().flags().client_side_metrics_enabled());
  EXPECT_TRUE(req.open_session().flags().reverse_scans());
  EXPECT_TRUE(req.open_session().flags().last_scanned_row_responses());
}

TEST_F(SessionStreamTest,
       UnknownFrameDoesNotTriggerOnHeartbeatWhileField8PeerLoadInfoDoes) {
  SessionResponse unknown_resp;
  stream_->ExecuteOnReadDone(true, unknown_resp);
  listener_->WaitForMessage(1);
  EXPECT_EQ(listener_->message_calls_, 1);
  EXPECT_EQ(listener_->heartbeat_calls_, 0);

  SessionResponse peer_load_resp;
  auto* mutable_unknown =
      peer_load_resp.GetReflection()->MutableUnknownFields(&peer_load_resp);
  mutable_unknown->AddLengthDelimited(8, "");
  stream_->ExecuteOnReadDone(true, peer_load_resp);
  listener_->WaitForMessage(2);
  EXPECT_EQ(listener_->message_calls_, 2);
  EXPECT_EQ(listener_->heartbeat_calls_, 1);
}

TEST_F(SessionStreamTest, PeerInfoDecodesBase64ProtobufHeaderAndZone) {
  google::bigtable::v2::PeerInfo proto_peer;
  proto_peer.set_google_frontend_id(10101);
  proto_peer.set_application_frontend_id(20202);
  proto_peer.set_application_frontend_region("us-east1");
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
  proto_peer.set_application_frontend_zone("us-east1-b");
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
  proto_peer.set_application_frontend_subzone("us-east1-b:sub1");
  proto_peer.set_transport_type(
      google::bigtable::v2::PeerInfo::TRANSPORT_TYPE_SESSION_DIRECT_ACCESS);

  std::string const encoded =
      absl::WebSafeBase64Escape(proto_peer.SerializeAsString());
  std::multimap<std::string, std::string> const headers = {
      {"bigtable-peer-info", encoded}};
  auto const info = transport::PeerInfo::FromHeaders(headers);
  EXPECT_TRUE(info.present_in_headers);
  EXPECT_EQ(info.google_frontend_id, 10101);
  EXPECT_EQ(info.application_frontend_id, 20202);
  EXPECT_EQ(info.application_frontend_region, "us-east1");
  EXPECT_EQ(info.application_frontend_zone, "us-east1-b");
  EXPECT_EQ(info.application_frontend_subzone, "us-east1-b:sub1");
  EXPECT_EQ(info.transport_type,
            transport::PeerInfo::TransportType::kSessionDirectAccess);
  EXPECT_TRUE(info.IsDirectAccess());
}

TEST_F(SessionStreamTest, StartOpenEncodesAuthorizedViewOpenPayload) {
  SessionStream::OpenConfig open_cfg;
  open_cfg.authorized_view_permission =
      google::bigtable::v2::OpenAuthorizedViewRequest::PERMISSION_READ_WRITE;
  stream_->set_open_config(open_cfg);
  stream_->ExecuteStartOpenWithName(
      "projects/p/instances/i/tables/t/authorizedViews/av1", "app1");

  auto const& req = stream_->current_write_request();
  ASSERT_TRUE(req.has_open_session());
  EXPECT_EQ(req.open_session().protocol_version(), 1);
  google::bigtable::v2::OpenAuthorizedViewRequest payload;
  ASSERT_TRUE(payload.ParseFromString(req.open_session().payload()));
  EXPECT_EQ(payload.authorized_view_name(),
            "projects/p/instances/i/tables/t/authorizedViews/av1");
  EXPECT_EQ(payload.app_profile_id(), "app1");
  EXPECT_EQ(
      payload.permission(),
      google::bigtable::v2::OpenAuthorizedViewRequest::PERMISSION_READ_WRITE);
}

TEST_F(SessionStreamTest, StartOpenEncodesMaterializedViewOpenPayload) {
  SessionStream::OpenConfig open_cfg;
  open_cfg.materialized_view_permission =
      google::bigtable::v2::OpenMaterializedViewRequest::PERMISSION_READ;
  stream_->set_open_config(open_cfg);
  stream_->ExecuteStartOpenWithName(
      "projects/p/instances/i/materializedViews/mv1", "app2");

  auto const& req = stream_->current_write_request();
  ASSERT_TRUE(req.has_open_session());
  EXPECT_EQ(req.open_session().protocol_version(), 1);
  google::bigtable::v2::OpenMaterializedViewRequest payload;
  ASSERT_TRUE(payload.ParseFromString(req.open_session().payload()));
  EXPECT_EQ(payload.materialized_view_name(),
            "projects/p/instances/i/materializedViews/mv1");
  EXPECT_EQ(payload.app_profile_id(), "app2");
  EXPECT_EQ(payload.permission(),
            google::bigtable::v2::OpenMaterializedViewRequest::PERMISSION_READ);
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
