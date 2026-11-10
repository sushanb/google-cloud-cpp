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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TRANSPORT_SESSION_STREAM_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TRANSPORT_SESSION_STREAM_H

#include "google/cloud/bigtable/internal/sessions/transport/current_cq.h"
#include "google/cloud/completion_queue.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/version.h"
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/feature_flags.pb.h>
#include <google/bigtable/v2/session.pb.h>
#include <grpcpp/grpcpp.h>
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

/// Consumes lifecycle and message events from the `SessionStream` gRPC reactor.
class SessionStreamListener {
 public:
  virtual ~SessionStreamListener() = default;

  /// Called when the server delivers initial metadata headers.
  virtual void OnInitialMetadata(
      std::multimap<grpc::string_ref, grpc::string_ref> const& metadata) = 0;

  /// Called when the session finishes handshaking and transitions to READY.
  virtual void OnSessionReady(
      google::bigtable::v2::OpenSessionResponse const& response) = 0;

  /// Called when an interleaved envelope message arrives from the server.
  virtual void OnMessage(google::bigtable::v2::SessionResponse message) = 0;

  /// Called when any message is received from the server, indicating activity.
  virtual void OnHeartbeat() = 0;

  /// Called when the in-flight write operation completes.
  virtual void OnWriteDone(bool ok) = 0;

  /// Called when the underlying transport stream terminates.
  virtual void OnClosed(google::cloud::Status status) = 0;
};

/// Manages a single full-duplex gRPC streaming connection (`OpenTable`) for
/// Sessions operations, enforcing single-write-in-flight serialization.
class SessionStream
    : public grpc::ClientBidiReactor<google::bigtable::v2::SessionRequest,
                                     google::bigtable::v2::SessionResponse>,
      public std::enable_shared_from_this<SessionStream> {
  friend class SessionImpl;

 public:
  using MetadataHook = std::function<void(::grpc::ClientContext&)>;

  struct OpenConfig {
    std::optional<google::bigtable::v2::FeatureFlags> feature_flags;
    std::optional<google::bigtable::v2::OpenAuthorizedViewRequest::Permission>
        authorized_view_permission;
    std::optional<google::bigtable::v2::OpenMaterializedViewRequest::Permission>
        materialized_view_permission;
  };

  /// Creates a session stream reactor targeting a specific table or view.
  static std::shared_ptr<SessionStream> Create(
      google::bigtable::v2::Bigtable::Stub* stub,
      std::weak_ptr<SessionStreamListener> listener,
      google::cloud::CompletionQueue event_cq) {
    return Create(stub, std::move(listener), std::move(event_cq), std::nullopt,
                  nullptr, OpenConfig{});
  }

  static std::shared_ptr<SessionStream> Create(
      google::bigtable::v2::Bigtable::Stub* stub,
      std::weak_ptr<SessionStreamListener> listener,
      google::cloud::CompletionQueue event_cq,
      std::optional<google::bigtable::v2::SessionRefreshConfig>
          refresh_config) {
    return Create(stub, std::move(listener), std::move(event_cq),
                  std::move(refresh_config), nullptr, OpenConfig{});
  }

  static std::shared_ptr<SessionStream> Create(
      google::bigtable::v2::Bigtable::Stub* stub,
      std::weak_ptr<SessionStreamListener> listener,
      google::cloud::CompletionQueue event_cq,
      std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config,
      MetadataHook metadata_hook) {
    return Create(stub, std::move(listener), std::move(event_cq),
                  std::move(refresh_config), std::move(metadata_hook),
                  OpenConfig{});
  }

  static std::shared_ptr<SessionStream> Create(
      google::bigtable::v2::Bigtable::Stub* stub,
      std::weak_ptr<SessionStreamListener> listener,
      google::cloud::CompletionQueue event_cq,
      std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config,
      MetadataHook metadata_hook, OpenConfig open_config);

  static std::shared_ptr<SessionStream> Create(
      google::bigtable::v2::Bigtable::Stub* stub,
      std::weak_ptr<SessionStreamListener> listener,
      google::cloud::CompletionQueue event_cq,
      std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config,
      MetadataHook metadata_hook,
      std::optional<google::bigtable::v2::FeatureFlags> feature_flags) {
    OpenConfig cfg;
    cfg.feature_flags = std::move(feature_flags);
    return Create(stub, std::move(listener), std::move(event_cq),
                  std::move(refresh_config), std::move(metadata_hook),
                  std::move(cfg));
  }

  ~SessionStream() override = default;

  /// Emits a raw envelope request onto the stream.
  ///
  /// Returns `UNAVAILABLE` if the stream is closed or already has a write in
  /// flight.
  virtual google::cloud::Status Write(
      google::bigtable::v2::SessionRequest request);

  /// Initiates graceful stream shutdown.
  virtual void Close(
      google::bigtable::v2::CloseSessionRequest::CloseSessionReason reason);

  /// Cancels the underlying gRPC context immediately.
  virtual void Cancel();

  /// Returns the stub pointer used by this stream.
  google::bigtable::v2::Bigtable::Stub* stub() const { return stub_; }

  /// Returns true if a gRPC `StartWrite` is currently in flight on this stream.
  bool write_in_flight() const {
    return write_in_flight_.load(std::memory_order_acquire);
  }

  /// Sets the pool-level pre-serialized `OpenTableRequest` payload bytes
  /// (`SESSION_CLIENT_SPEC.md #4`).
  void set_pre_serialized_open_payload(std::string payload) {
    pre_serialized_open_payload_ = std::move(payload);
  }

  /// Overrides the default `FeatureFlags` sent in `bigtable-features` and
  /// `OpenSessionRequest.flags` (`SESSION_CLIENT_SPEC.md #4`).
  void set_feature_flags(google::bigtable::v2::FeatureFlags flags) {
    feature_flags_ = std::move(flags);
  }

  /// Sets or updates the `OpenConfig` for this stream before `StartOpen()`.
  void set_open_config(OpenConfig open_config) {
    open_config_ = std::move(open_config);
  }
  OpenConfig const& open_config() const { return open_config_; }

 protected:
  SessionStream() = default;  // For test mocks
  SessionStream(google::bigtable::v2::Bigtable::Stub* stub,
                std::weak_ptr<SessionStreamListener> listener,
                google::cloud::CompletionQueue event_cq);

  void StartOpen(std::string table_name, std::string app_profile_id,
                 google::bigtable::v2::OpenTableRequest::Permission permission);

  // grpc::ClientBidiReactor interface implementations:
  void OnReadInitialMetadataDone(bool ok) override;
  void OnReadDone(bool ok) override;
  void OnWriteDone(bool ok) override;
  void OnDone(grpc::Status const& status) override;

 private:
  void StartOpenSerialized(
      std::string table_name, std::string app_profile_id,
      google::bigtable::v2::OpenTableRequest::Permission permission);
  void WriteSerialized(google::bigtable::v2::SessionRequest request);
  void CloseSerialized(
      google::bigtable::v2::CloseSessionRequest::CloseSessionReason reason);
  void OnReadInitialMetadataDoneSerialized(bool ok);
  void OnWriteDoneSerialized(bool ok);
  void OnDoneSerialized(grpc::Status const& status);

  std::mutex grpc_mutex_;

 protected:
  grpc::ClientContext context_;
  google::bigtable::v2::Bigtable::Stub* stub_ = nullptr;
  std::weak_ptr<SessionStreamListener> listener_;
  std::shared_ptr<SessionStream> self_;

  std::atomic<bool> is_closed_{false};
  std::atomic<bool> write_in_flight_{false};
  std::atomic<bool> initial_metadata_delivered_{false};
  bool started_ = false;
  bool call_started_ = false;
  bool cancel_requested_ = false;
  bool terminated_ = false;
  bool open_write_in_flight_ = false;
  bool closing_write_sent_ = false;
  std::optional<google::bigtable::v2::CloseSessionRequest::CloseSessionReason>
      pending_close_reason_;
  std::optional<google::bigtable::v2::OpenSessionResponse>
      pending_open_response_;

  google::bigtable::v2::SessionRequest current_write_request_;
  google::bigtable::v2::SessionResponse current_response_;
  std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config_;
  std::string pre_serialized_open_payload_;
  std::optional<google::bigtable::v2::FeatureFlags> feature_flags_;
  MetadataHook metadata_hook_;
  OpenConfig open_config_;
  std::optional<google::cloud::CompletionQueue> event_cq_;
};

namespace transport {
using SessionStream =
    ::google::cloud::bigtable_internal::sessions::SessionStream;
}  // namespace transport

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TRANSPORT_SESSION_STREAM_H
