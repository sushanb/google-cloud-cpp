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
#include "google/cloud/bigtable/internal/sessions/logging_helper.h"
#include "google/cloud/bigtable/internal/sessions/transport/current_cq.h"
#include "google/cloud/completion_queue.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/internal/url_encode.h"
#include "google/cloud/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/protobuf/unknown_field_set.h>
#include <grpcpp/support/status.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

std::shared_ptr<SessionStream> SessionStream::Create(
    google::bigtable::v2::Bigtable::Stub* stub,
    std::weak_ptr<SessionStreamListener> listener,
    google::cloud::CompletionQueue event_cq,
    std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config,
    MetadataHook metadata_hook, OpenConfig open_config) {
  auto stream = std::shared_ptr<SessionStream>(
      new SessionStream(stub, std::move(listener), std::move(event_cq)));
  stream->refresh_config_ = std::move(refresh_config);
  stream->metadata_hook_ = std::move(metadata_hook);
  stream->open_config_ = std::move(open_config);
  return stream;
}

SessionStream::SessionStream(google::bigtable::v2::Bigtable::Stub* stub,
                             std::weak_ptr<SessionStreamListener> listener,
                             google::cloud::CompletionQueue event_cq)
    : stub_(stub),
      listener_(std::move(listener)),
      event_cq_(std::move(event_cq)) {}

void SessionStream::StartOpen(
    std::string table_name, std::string app_profile_id,
    google::bigtable::v2::OpenTableRequest::Permission permission) {
  StartOpenSerialized(std::move(table_name), std::move(app_profile_id),
                      permission);
}

void SessionStream::StartOpenSerialized(
    std::string table_name, std::string app_profile_id,
    google::bigtable::v2::OpenTableRequest::Permission permission) {
  std::unique_lock<std::mutex> l(grpc_mutex_);
  if (is_closed_.load(std::memory_order_acquire) || terminated_ || started_) {
    bool const notify_closed = !started_;
    if (notify_closed) {
      terminated_ = true;
      event_cq_.reset();
    }
    l.unlock();
    if (notify_closed) {
      if (auto listener = listener_.lock()) {
        listener->OnClosed(google::cloud::internal::CancelledError(
            "SessionStream closed before open"));
      }
    }
    return;
  }
  started_ = true;
  if (stub_ != nullptr) {
    self_ = shared_from_this();
  }
  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      5, "SessionStream",
      absl::StrCat(
          "StartOpenSerialized - Initiating bidi stream connection on socket, "
          "table_name: ",
          table_name));
  google::bigtable::v2::FeatureFlags feature_flags;
  if (open_config_.feature_flags.has_value()) {
    feature_flags = *open_config_.feature_flags;
  } else if (feature_flags_.has_value()) {
    feature_flags = *feature_flags_;
  } else {
    feature_flags.set_sessions_compatible(true);
    feature_flags.set_sessions_required(true);
    feature_flags.set_routing_cookie(true);
    feature_flags.set_retry_info(true);
    feature_flags.set_peer_info(true);
    feature_flags.set_client_side_metrics_enabled(true);
    feature_flags.set_reverse_scans(true);
    feature_flags.set_last_scanned_row_responses(true);
    feature_flags.set_traffic_director_enabled(false);
    feature_flags.set_direct_access_requested(false);
  }

  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      5, "SessionStream",
      absl::StrCat("Connection 0x",
                   absl::Hex(reinterpret_cast<std::uintptr_t>(this)),
                   " - Client Metadata Sent:"));
  auto add_and_print = [this](std::string_view key, std::string_view value) {
    context_.AddMetadata(std::string(key), std::string(value));
    ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
        5, "SessionStream", absl::StrCat("  ", key, ": ", value));
  };

  add_and_print("bigtable-features",
                absl::WebSafeBase64Escape(feature_flags.SerializeAsString()));

  bool has_request_params_override = false;
  if (refresh_config_.has_value()) {
    for (auto const& meta : refresh_config_->metadata()) {
      if (absl::EqualsIgnoreCase(meta.key(), "x-goog-request-params")) {
        has_request_params_override = true;
      }
      add_and_print(meta.key(), meta.value());
    }
  }

  if (!has_request_params_override) {
    if (open_config_.authorized_view_permission.has_value()) {
      add_and_print(
          "x-goog-request-params",
          absl::StrCat("open_session.payload.authorized_view_name=",
                       google::cloud::internal::UrlEncode(table_name),
                       "&open_session.payload.app_profile_id=",
                       google::cloud::internal::UrlEncode(app_profile_id),
                       "&open_session.payload.permission=",
                       google::cloud::internal::UrlEncode(
                           google::bigtable::v2::OpenAuthorizedViewRequest::
                               Permission_Name(
                                   *open_config_.authorized_view_permission))));
    } else if (open_config_.materialized_view_permission.has_value()) {
      add_and_print(
          "x-goog-request-params",
          absl::StrCat(
              "open_session.payload.materialized_view_name=",
              google::cloud::internal::UrlEncode(table_name),
              "&open_session.payload.app_profile_id=",
              google::cloud::internal::UrlEncode(app_profile_id),
              "&open_session.payload.permission=",
              google::cloud::internal::UrlEncode(
                  google::bigtable::v2::OpenMaterializedViewRequest::
                      Permission_Name(
                          *open_config_.materialized_view_permission))));
    } else {
      add_and_print(
          "x-goog-request-params",
          absl::StrCat(
              "open_session.payload.table_name=",
              google::cloud::internal::UrlEncode(table_name),
              "&open_session.payload.app_profile_id=",
              google::cloud::internal::UrlEncode(app_profile_id),
              "&open_session.payload.permission=",
              google::cloud::internal::UrlEncode(
                  google::bigtable::v2::OpenTableRequest::Permission_Name(
                      permission))));
    }
  }

  if (metadata_hook_ != nullptr) {
    metadata_hook_(context_);
  }

  google::bigtable::v2::OpenSessionRequest open_request;
  if (refresh_config_.has_value() &&
      refresh_config_->has_optimized_open_request()) {
    open_request = refresh_config_->optimized_open_request();
    if (open_request.protocol_version() == 0) {
      open_request.set_protocol_version(1);
    }
    *open_request.mutable_flags() = feature_flags;
  } else if (open_config_.authorized_view_permission.has_value()) {
    google::bigtable::v2::OpenAuthorizedViewRequest open_payload;
    open_payload.set_authorized_view_name(table_name);
    open_payload.set_app_profile_id(app_profile_id);
    open_payload.set_permission(*open_config_.authorized_view_permission);

    open_request.set_protocol_version(1);
    *open_request.mutable_flags() = feature_flags;
    (void)open_payload.SerializeToString(open_request.mutable_payload());
  } else if (open_config_.materialized_view_permission.has_value()) {
    google::bigtable::v2::OpenMaterializedViewRequest open_payload;
    open_payload.set_materialized_view_name(table_name);
    open_payload.set_app_profile_id(app_profile_id);
    open_payload.set_permission(*open_config_.materialized_view_permission);

    open_request.set_protocol_version(1);
    *open_request.mutable_flags() = feature_flags;
    (void)open_payload.SerializeToString(open_request.mutable_payload());
  } else {
    open_request.set_protocol_version(1);
    *open_request.mutable_flags() = feature_flags;
    if (!pre_serialized_open_payload_.empty()) {
      *open_request.mutable_payload() = pre_serialized_open_payload_;
    } else {
      google::bigtable::v2::OpenTableRequest open_payload;
      open_payload.set_table_name(table_name);
      open_payload.set_app_profile_id(app_profile_id);
      open_payload.set_permission(permission);
      if (!open_payload.SerializeToString(open_request.mutable_payload())) {
        ::google::cloud::bigtable_internal::sessions::internal::
            LogVerboseWarning(1, "SessionStream",
                              "Failed to serialize OpenTableRequest payload");
      }
    }
  }

  current_write_request_.Clear();
  *current_write_request_.mutable_open_session() = std::move(open_request);

  open_write_in_flight_ = true;
  write_in_flight_.store(true, std::memory_order_release);

  if (stub_ == nullptr) {
    call_started_ = true;
    return;
  }
  call_started_ = true;
  l.unlock();
  if (open_config_.authorized_view_permission.has_value()) {
    stub_->async()->OpenAuthorizedView(&context_, this);
  } else if (open_config_.materialized_view_permission.has_value()) {
    stub_->async()->OpenMaterializedView(&context_, this);
  } else {
    stub_->async()->OpenTable(&context_, this);
  }
  StartWrite(&current_write_request_);
  StartRead(&current_response_);
  StartCall();

  bool should_try_cancel = false;
  {
    std::lock_guard<std::mutex> l2(grpc_mutex_);
    should_try_cancel = cancel_requested_ && !terminated_;
  }
  if (should_try_cancel) {
    context_.TryCancel();
  }
}

google::cloud::Status SessionStream::Write(
    google::bigtable::v2::SessionRequest request) {
  std::unique_lock<std::mutex> l(grpc_mutex_);
  if (is_closed_.load(std::memory_order_acquire) || terminated_) {
    return google::cloud::internal::UnavailableError("SessionStream is closed");
  }
  if (write_in_flight_.exchange(true, std::memory_order_acq_rel)) {
    return google::cloud::internal::UnavailableError(
        "A write is already in flight on this stream");
  }
  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      5, "SessionStream",
      absl::StrCat("Submitting Request Serialized, payload_case: ",
                   request.payload_case(), ", is_closed: ", is_closed_.load(),
                   ", write_in_flight: ", write_in_flight_.load()));
  current_write_request_ = std::move(request);
  l.unlock();
  StartWrite(&current_write_request_);
  return google::cloud::Status();
}

void SessionStream::WriteSerialized(
    google::bigtable::v2::SessionRequest request) {
  (void)Write(std::move(request));
}

namespace {

void PopulateCloseSessionRequest(
    google::bigtable::v2::CloseSessionRequest* close_session,
    google::bigtable::v2::CloseSessionRequest::CloseSessionReason reason) {
  close_session->set_reason(reason);
  switch (reason) {
    case google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_GOAWAY:
      close_session->set_description("Server sent GO_AWAY");
      break;
    case google::bigtable::v2::CloseSessionRequest::
        CLOSE_SESSION_REASON_DOWNSIZE:
      close_session->set_description("Session pool downsize");
      break;
    case google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_ERROR:
      close_session->set_description("Client session error");
      break;
    case google::bigtable::v2::CloseSessionRequest::
        CLOSE_SESSION_REASON_MISSED_HEARTBEAT:
      close_session->set_description("Missed server keepalive heartbeat");
      break;
    case google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_USER:
      close_session->set_description("Client closed session");
      break;
    case google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_UNSET:
    default:
      break;
  }
}

bool IsKnownSessionResponseFrame(
    google::bigtable::v2::SessionResponse const& response) {
  if (response.payload_case() !=
      google::bigtable::v2::SessionResponse::PAYLOAD_NOT_SET) {
    return true;
  }
  auto const* reflection = response.GetReflection();
  if (reflection != nullptr) {
    auto const& unknown_fields = reflection->GetUnknownFields(response);
    for (int i = 0; i < unknown_fields.field_count(); ++i) {
      if (unknown_fields.field(i).number() == 8) {
        return true;
      }
    }
  }
  return false;
}

}  // namespace

void SessionStream::Close(
    google::bigtable::v2::CloseSessionRequest::CloseSessionReason reason) {
  CloseSerialized(reason);
}

void SessionStream::CloseSerialized(
    google::bigtable::v2::CloseSessionRequest::CloseSessionReason reason) {
  std::unique_lock<std::mutex> l(grpc_mutex_);
  if (is_closed_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  if (terminated_) return;
  if (stub_ == nullptr) {
    l.unlock();
    context_.TryCancel();
    return;
  }
  if (!write_in_flight_.exchange(true, std::memory_order_acq_rel)) {
    closing_write_sent_ = true;
    current_write_request_.Clear();
    PopulateCloseSessionRequest(current_write_request_.mutable_close_session(),
                                reason);
    l.unlock();
    StartWriteLast(&current_write_request_, grpc::WriteOptions());
    return;
  }
  pending_close_reason_ = reason;
}

void SessionStream::Cancel() {
  bool should_try_cancel = false;
  {
    std::lock_guard<std::mutex> l(grpc_mutex_);
    is_closed_.store(true, std::memory_order_release);
    cancel_requested_ = true;
    pending_close_reason_.reset();
    should_try_cancel = (call_started_ || stub_ == nullptr) && !terminated_;
  }
  if (should_try_cancel) {
    context_.TryCancel();
  }
}

void SessionStream::OnReadInitialMetadataDone(bool ok) {
  SessionCallbackGuard guard(event_cq_);
  if (!ok) {
    std::lock_guard<std::mutex> l(grpc_mutex_);
    terminated_ = true;
  }
  OnReadInitialMetadataDoneSerialized(ok);
}

void SessionStream::OnReadInitialMetadataDoneSerialized(bool ok) {
  if (!ok) return;
  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      5, "SessionStream",
      absl::StrCat("Connection 0x",
                   absl::Hex(reinterpret_cast<std::uintptr_t>(this)),
                   " - Server Initial Metadata Received:"));
  for (auto const& [key, value] : context_.GetServerInitialMetadata()) {
    ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
        5, "SessionStream",
        absl::StrCat("  ", std::string_view(key.data(), key.size()), ": ",
                     std::string_view(value.data(), value.size())));
  }
  if (auto listener = listener_.lock()) {
    listener->OnInitialMetadata(context_.GetServerInitialMetadata());
  }

  std::optional<google::bigtable::v2::OpenSessionResponse> ready_open_response;
  {
    std::lock_guard<std::mutex> l(grpc_mutex_);
    initial_metadata_delivered_.store(true, std::memory_order_release);
    if (!open_write_in_flight_ && !terminated_ &&
        !is_closed_.load(std::memory_order_acquire) &&
        pending_open_response_.has_value()) {
      ready_open_response = std::move(*pending_open_response_);
      pending_open_response_.reset();
    }
  }
  if (ready_open_response.has_value()) {
    if (auto listener = listener_.lock()) {
      if (!is_closed_.load(std::memory_order_acquire)) {
        listener->OnSessionReady(*ready_open_response);
      }
    }
    std::unique_lock<std::mutex> l(grpc_mutex_);
    if (call_started_ && !terminated_ && !cancel_requested_) {
      l.unlock();
      StartRead(&current_response_);
    }
  }
}

void SessionStream::OnReadDone(bool ok) {
  SessionCallbackGuard guard(event_cq_);
  if (!ok) {
    std::lock_guard<std::mutex> l(grpc_mutex_);
    terminated_ = true;
    pending_open_response_.reset();
    return;
  }

  google::bigtable::v2::SessionResponse response;
  bool defer_open_session = false;
  {
    std::lock_guard<std::mutex> l(grpc_mutex_);
    response.Swap(&current_response_);
    current_response_.Clear();
    if (response.has_open_session() &&
        (open_write_in_flight_ ||
         (stub_ != nullptr &&
          !initial_metadata_delivered_.load(std::memory_order_acquire)))) {
      pending_open_response_ = response.open_session();
      defer_open_session = true;
    }
  }

  if (auto listener = listener_.lock()) {
    if (IsKnownSessionResponseFrame(response)) {
      listener->OnHeartbeat();
    }
    if (!defer_open_session) {
      if (response.has_open_session()) {
        listener->OnSessionReady(response.open_session());
      } else {
        listener->OnMessage(std::move(response));
      }
    }
  }

  if (!defer_open_session) {
    std::unique_lock<std::mutex> l(grpc_mutex_);
    if (call_started_ && !terminated_ && !cancel_requested_) {
      l.unlock();
      StartRead(&current_response_);
    }
  }
}

void SessionStream::OnWriteDone(bool ok) {
  SessionCallbackGuard guard(event_cq_);
  if (!ok) {
    std::lock_guard<std::mutex> l(grpc_mutex_);
    terminated_ = true;
    pending_open_response_.reset();
    pending_close_reason_.reset();
  }
  OnWriteDoneSerialized(ok);
}

void SessionStream::OnWriteDoneSerialized(bool ok) {
  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      5, "SessionStream",
      absl::StrCat("Write completed on socket serialized, ok: ", ok));
  std::optional<google::bigtable::v2::OpenSessionResponse> ready_open_response;
  bool send_close_write = false;
  bool should_try_cancel = false;
  {
    std::lock_guard<std::mutex> l(grpc_mutex_);
    if (ok && pending_close_reason_.has_value() && !terminated_ &&
        stub_ != nullptr) {
      auto const reason = *pending_close_reason_;
      pending_close_reason_.reset();
      closing_write_sent_ = true;
      current_write_request_.Clear();
      PopulateCloseSessionRequest(
          current_write_request_.mutable_close_session(), reason);
      send_close_write = true;
    } else {
      current_write_request_.Clear();
      write_in_flight_.store(false, std::memory_order_release);
    }
    if (open_write_in_flight_) {
      open_write_in_flight_ = false;
      if (!ok || terminated_ || is_closed_.load(std::memory_order_acquire)) {
        pending_open_response_.reset();
      } else if ((stub_ == nullptr || initial_metadata_delivered_.load(
                                          std::memory_order_acquire)) &&
                 pending_open_response_.has_value()) {
        ready_open_response = std::move(*pending_open_response_);
        pending_open_response_.reset();
      }
    }
    should_try_cancel =
        is_closed_.load(std::memory_order_acquire) && !closing_write_sent_;
  }
  if (send_close_write) {
    StartWriteLast(&current_write_request_, grpc::WriteOptions());
  }
  if (should_try_cancel) {
    context_.TryCancel();
  }
  if (auto listener = listener_.lock()) {
    listener->OnWriteDone(ok);
    if (ready_open_response.has_value() &&
        !is_closed_.load(std::memory_order_acquire)) {
      listener->OnSessionReady(*ready_open_response);
    }
  }
  if (ready_open_response.has_value()) {
    std::unique_lock<std::mutex> l(grpc_mutex_);
    if (call_started_ && !terminated_ && !cancel_requested_) {
      l.unlock();
      StartRead(&current_response_);
    }
  }
}

void SessionStream::OnDone(grpc::Status const& status) {
  SessionCallbackGuard guard(event_cq_);
  {
    std::lock_guard<std::mutex> l(grpc_mutex_);
    terminated_ = true;
  }
  OnDoneSerialized(status);
}

void SessionStream::OnDoneSerialized(grpc::Status const& status) {
  {
    std::lock_guard<std::mutex> l(grpc_mutex_);
    is_closed_.store(true, std::memory_order_release);
    pending_open_response_.reset();
    pending_close_reason_.reset();
    event_cq_.reset();
  }
  google::cloud::Status cloud_status =
      status.ok()
          ? google::cloud::Status()
          : google::cloud::Status(
                static_cast<google::cloud::StatusCode>(status.error_code()),
                status.error_message());

  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      5, "SessionStream",
      absl::StrCat("Connection 0x",
                   absl::Hex(reinterpret_cast<std::uintptr_t>(this)),
                   " - Server Trailing Metadata Received:"));
  for (auto const& [key, value] : context_.GetServerTrailingMetadata()) {
    ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
        5, "SessionStream",
        absl::StrCat("  ", std::string_view(key.data(), key.size()), ": ",
                     std::string_view(value.data(), value.size())));
  }
  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      5, "SessionStream",
      absl::StrCat("Reactor terminated with status: ", cloud_status.message()));

  auto self_keep_alive = std::move(self_);
  self_.reset();
  if (auto listener = listener_.lock()) {
    listener->OnClosed(cloud_status);
  }
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
