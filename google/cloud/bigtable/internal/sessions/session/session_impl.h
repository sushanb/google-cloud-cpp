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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SESSION_SESSION_IMPL_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SESSION_SESSION_IMPL_H

#include "google/cloud/bigtable/internal/sessions/session/session_state.h"
#include "google/cloud/bigtable/internal/sessions/transport/current_cq.h"
#include "google/cloud/bigtable/internal/sessions/transport/peak_ewma_tracker.h"
#include "google/cloud/bigtable/internal/sessions/transport/peer_info.h"
#include "google/cloud/bigtable/internal/sessions/transport/session_stream.h"
#include "google/cloud/completion_queue.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/version.h"
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/bigtable/v2/bigtable.pb.h>
#include <google/bigtable/v2/session.pb.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <map>
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

class SessionImpl;

using ScheduleFn =
    std::function<void(std::chrono::nanoseconds, std::function<void()>)>;

using GenericCallback = std::function<void(
    google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>)>;

/// Consumes decoded virtual RPC events and session lifecycle state changes.
class SessionListener {
 public:
  virtual ~SessionListener() = default;

  /// Invoked when the server delivers initial metadata headers.
  virtual void OnInitialMetadata(
      std::shared_ptr<SessionImpl>,
      std::multimap<grpc::string_ref, grpc::string_ref> const&) {}

  /// Invoked when the underlying stream completes the `OpenSession` handshake.
  virtual void OnSessionReady(
      std::shared_ptr<SessionImpl> session,
      google::bigtable::v2::OpenSessionResponse const& response) = 0;

  /// Invoked when the server requests graceful session rotation (`GoAway`).
  virtual void OnGoAway(std::shared_ptr<SessionImpl> session) = 0;

  /// Invoked when the session closes or experiences a transport drop.
  virtual void OnSessionClosed(std::shared_ptr<SessionImpl> session,
                               google::cloud::Status status) = 0;

  /// Invoked when a write operation completes on the session.
  virtual void OnWriteDone(std::shared_ptr<SessionImpl>) {}

  /// Invoked when an in-flight slot whose caller cancelled early is drained by
  /// the arrival of the server response frame on the wire.
  virtual void OnSlotDrained(std::shared_ptr<SessionImpl>, std::int64_t) {}

  /// Returns the number of active calls on the channel (`Stub`) sharing this
  /// session.
  virtual int GetActiveCallsForStub(
      google::bigtable::v2::Bigtable::Stub const* stub) const = 0;
};

/// Manages the per-session state machine, control frames (`OpenSession`,
/// `GoAway`, `SessionParameters`, `Heartbeat`, `ClusterInformation`), keepalive
/// watchdog, and single-flight virtual RPC execution over a `SessionStream`.
class SessionImpl : public SessionStreamListener,
                    public std::enable_shared_from_this<SessionImpl> {
  friend class SessionImplTestPeer;

 public:
  static constexpr int kMaxConcurrentRpcsPerSession = 1;
  static constexpr std::int64_t kUnsetTimeNs =
      (std::numeric_limits<std::int64_t>::min)();

  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;
  using State = session::SessionLifecycleState;

  static std::int64_t ToNanos(TimePoint tp) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               tp.time_since_epoch())
        .count();
  }

  static TimePoint FromNanos(std::int64_t ns) {
    return TimePoint(std::chrono::duration_cast<Clock::duration>(
        std::chrono::nanoseconds(ns)));
  }

  /// Process-wide default keepalive multiplier (env override:
  /// `GOOGLE_CLOUD_BIGTABLE_SESSIONS_KEEPALIVE_MULTIPLIER`). Prefer the
  /// per-session override below, which the pool sets from
  /// `SessionsKeepaliveMultiplierOption` so separate connections do not affect
  /// each other.
  static double GetKeepaliveTimeoutMultiplier();
  static void SetKeepaliveTimeoutMultiplier(double multiplier);

  /// Per-session keepalive multiplier override (`<= 0` uses the default).
  void set_keepalive_timeout_multiplier(double multiplier) {
    keepalive_multiplier_override_.store(multiplier, std::memory_order_relaxed);
  }
  double effective_keepalive_timeout_multiplier() const {
    double const v =
        keepalive_multiplier_override_.load(std::memory_order_relaxed);
    return v > 0.0 ? v : GetKeepaliveTimeoutMultiplier();
  }
  static int GetMaxWriteQueueSize();
  static void SetMaxWriteQueueSize(int max_size);

  /// Process-wide default GoAway / Drain grace timeout (default 30s; env
  /// override: `GOOGLE_CLOUD_BIGTABLE_SESSIONS_GOAWAY_DRAIN_TIMEOUT_MS`).
  static std::chrono::nanoseconds GetGoAwayDrainTimeout();
  static void SetGoAwayDrainTimeout(std::chrono::nanoseconds timeout);

  /// Per-session GoAway / Drain grace timeout override (`<= 0` uses
  /// `GetGoAwayDrainTimeout()`).
  void set_goaway_drain_timeout(std::chrono::nanoseconds timeout) {
    goaway_drain_timeout_override_ms_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count(),
        std::memory_order_relaxed);
  }
  std::chrono::nanoseconds effective_goaway_drain_timeout() const {
    std::int64_t const ms =
        goaway_drain_timeout_override_ms_.load(std::memory_order_relaxed);
    return ms > 0 ? std::chrono::milliseconds(ms) : GetGoAwayDrainTimeout();
  }

  /// Creates a session controller and initiates the underlying transport
  /// stream (unless `start_open` is false, in which case the caller must invoke
  /// `StartOpen()` after registering the session).
  static std::shared_ptr<SessionImpl> Create(
      google::bigtable::v2::Bigtable::Stub* stub,
      std::weak_ptr<SessionListener> listener, std::string_view table_name,
      std::string_view app_profile_id,
      google::bigtable::v2::OpenTableRequest::Permission permission,
      google::cloud::CompletionQueue event_cq, ScheduleFn scheduler = nullptr,
      std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config =
          std::nullopt,
      SessionStream::MetadataHook metadata_hook = nullptr,
      SessionStream::OpenConfig open_config = {}, bool start_open = true);

  static std::shared_ptr<SessionImpl> Create(
      google::bigtable::v2::Bigtable::Stub* stub,
      std::weak_ptr<SessionListener> listener, std::string_view table_name,
      std::string_view app_profile_id,
      google::bigtable::v2::OpenTableRequest::Permission permission,
      google::cloud::CompletionQueue event_cq, ScheduleFn scheduler,
      std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config,
      SessionStream::MetadataHook metadata_hook, bool start_open) {
    return Create(stub, std::move(listener), table_name, app_profile_id,
                  permission, std::move(event_cq), std::move(scheduler),
                  std::move(refresh_config), std::move(metadata_hook),
                  SessionStream::OpenConfig{}, start_open);
  }

  static std::shared_ptr<SessionImpl> Create(
      google::bigtable::v2::Bigtable::Stub* stub,
      std::weak_ptr<SessionListener> listener, std::string_view table_name,
      std::string_view app_profile_id,
      google::bigtable::v2::OpenTableRequest::Permission permission,
      google::cloud::CompletionQueue event_cq, ScheduleFn scheduler,
      std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config,
      SessionStream::MetadataHook metadata_hook,
      std::optional<google::bigtable::v2::FeatureFlags> feature_flags,
      bool start_open = true) {
    SessionStream::OpenConfig cfg;
    cfg.feature_flags = std::move(feature_flags);
    return Create(stub, std::move(listener), table_name, app_profile_id,
                  permission, std::move(event_cq), std::move(scheduler),
                  std::move(refresh_config), std::move(metadata_hook),
                  std::move(cfg), start_open);
  }

  /// Starts the underlying `OpenTable` stream if `Create` was called with
  /// `start_open = false`.
  void StartOpen();
  void Cancel();

  /// Supplies a pool-wide pre-serialized `OpenTableRequest` payload to reuse
  /// across session handshakes instead of re-serializing per session.
  void set_pre_serialized_open_payload(std::string payload) {
    if (stream_ != nullptr) {
      stream_->set_pre_serialized_open_payload(std::move(payload));
    }
  }

  /// Overrides the `FeatureFlags` advertised in the `bigtable-features`
  /// header on the underlying `OpenSession` stream.
  void set_feature_flags(google::bigtable::v2::FeatureFlags flags) {
    if (stream_ != nullptr) {
      stream_->set_feature_flags(std::move(flags));
    }
  }

  /// Sets the `OpenConfig` on the underlying `SessionStream` before
  /// `StartOpen()`.
  void set_open_config(SessionStream::OpenConfig open_config) {
    if (stream_ != nullptr) {
      stream_->set_open_config(std::move(open_config));
    }
  }

  /// Injects a custom `SessionStream` for unit testing.
  static std::shared_ptr<SessionImpl> CreateForTesting(
      std::shared_ptr<SessionStream> stream,
      std::weak_ptr<SessionListener> listener,
      google::cloud::CompletionQueue event_cq, ScheduleFn scheduler = nullptr);

  ~SessionImpl() override;

  virtual void StartVirtualRpc(grpc::ClientContext* context,
                               google::bigtable::v2::VirtualRpcRequest request,
                               GenericCallback callback, bool is_write_request);

  void StartVirtualRpc(grpc::ClientContext* context,
                       google::bigtable::v2::VirtualRpcRequest request,
                       GenericCallback callback) {
    StartVirtualRpc(context, std::move(request), std::move(callback), false);
  }

  /// Finalizes the active virtual RPC, clearing its slot on the session.
  virtual void FinalizeActiveRpc(std::int64_t rpc_id);

  /// Cancels an active virtual RPC.
  virtual void CancelVirtualRpc(std::int64_t rpc_id);

  /// Initiates graceful termination of the streaming session.
  virtual void Close();

  /// Transitions the session to `kDraining`, allowing the in-flight RPC to
  /// finish before closing.
  virtual void Drain();

  /// Returns the current operational state of the session.
  virtual State state() const;
  virtual void SetAutoFinalize(bool enable);
  virtual int active_rpc_count() const;
  virtual int write_queue_size() const;
  virtual std::string state_str() const;
  virtual bool HasActiveContext(grpc::ClientContext* context) const;
  virtual void CancelRpcByContext(grpc::ClientContext* context);

  /// Returns true if the session is actively servicing a virtual RPC or is
  /// write-congested.
  virtual bool IsBusy() const;

  /// Returns true if the internal write queue exceeds the backpressure limit.
  virtual bool IsWriteCongested() const;

  /// Atomically acquires the session slot for a virtual RPC.
  ///
  /// Returns true if the session transitioned from idle to busy.
  virtual bool Acquire();

  /// Returns true if the session has no active RPCs and has been idle for
  /// longer than `timeout`.
  virtual bool IsIdle(TimePoint now, std::chrono::nanoseconds timeout) const;

  /// Returns the timestamp of the most recent RPC activity on this session.
  virtual TimePoint GetLastActiveTime() const;

  /// Returns the timestamp when this `SessionImpl` was constructed.
  TimePoint created_at() const { return created_at_; }

  /// Returns the timestamp when `OnSessionReady` transitioned this session to
  /// `kReady` (or `std::nullopt` if the session never became ready).
  std::optional<TimePoint> ready_at() const {
    std::int64_t const ns = ready_at_ns_.load(std::memory_order_acquire);
    if (ns == kUnsetTimeNs) return std::nullopt;
    return FromNanos(ns);
  }

 protected:
  SessionImpl() = default;  // For unit testing mocks
  void SetListener(std::weak_ptr<SessionListener> listener) {
    listener_ = std::move(listener);
  }
  std::weak_ptr<SessionListener> listener() const { return listener_; }

 public:
  /// Returns server-tuned session parameters if received.
  std::optional<google::bigtable::v2::SessionParametersResponse>
  session_parameters() const;

  /// Returns server-sent session refresh configuration if received.
  std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config()
      const;

  std::optional<google::bigtable::v2::BackendIdentifier> backend() const;

  /// Returns the backend peer identifier associated with this session if ready.
  virtual std::optional<std::string> GetPeerId() const;

  /// Backward-compatible alias for `GetPeerId()`.
  virtual std::optional<std::string> GetAfeId() const { return GetPeerId(); }

  struct PeerLoadInfo {
    std::int64_t reason = 0;
    double weight = 1.0;
  };

  /// Returns or sets the peer load-balancing weight (default 1.0) for Weighted
  /// Round Robin (WRR) routing, derived from `PeerLoadInfo` frames.
  virtual double GetPeerWeight() const {
    return peer_weight_.load(std::memory_order_relaxed);
  }
  virtual void SetPeerWeight(double weight) {
    peer_weight_.store(weight > 0.0 ? weight : 1.0, std::memory_order_relaxed);
  }
  void OnPeerLoadInfo(PeerLoadInfo const& info);
  std::optional<PeerLoadInfo> peer_load_info() const;

  /// Returns parsed `bigtable-peer-info` header metadata if received.
  std::optional<transport::PeerInfo> peer_info() const;

  /// Returns the most recent `ClusterInformation` received on a
  /// `VirtualRpcResponse` or `ErrorResponse` frame on this session.
  std::optional<google::bigtable::v2::ClusterInformation> last_cluster_info()
      const;

  /// Returns the timestamp when the active (or most recent) virtual RPC frame
  /// was handed to `SessionStream::Write` (`std::nullopt` if never sent).
  std::optional<TimePoint> active_rpc_sent_at() const {
    std::int64_t ns = active_rpc_.sent_at_ns.load(std::memory_order_acquire);
    if (ns == kUnsetTimeNs) {
      ns = last_sent_at_ns_.load(std::memory_order_acquire);
    }
    if (ns == kUnsetTimeNs) return std::nullopt;
    return FromNanos(ns);
  }

  /// Returns the timestamp when the most recent virtual RPC frame was handed to
  /// `SessionStream::Write` (`std::nullopt` if never sent).
  std::optional<TimePoint> last_sent_at() const {
    std::int64_t const ns = last_sent_at_ns_.load(std::memory_order_acquire);
    if (ns == kUnsetTimeNs) return std::nullopt;
    return FromNanos(ns);
  }

  /// Returns the highest `rpc_id` ever claimed by `StartVirtualRpc` on this
  /// session (`0` if none).
  std::int64_t last_dispatched_rpc_id() const {
    return last_dispatched_rpc_id_.load(std::memory_order_acquire);
  }

  /// Returns the close reason string for `session/durations` telemetry
  /// (`"GOAWAY"`, `"MISSED_HEARTBEAT"`, `"USER"`, `"ERROR"`).
  std::string close_reason_str() const;

  /// Returns the vRPC outcome summary for `session/durations` telemetry
  /// (`"none"`, `"all_ok"`, `"all_error"`, `"some_ok"`).
  std::string vrpcs_summary() const {
    std::int64_t const ok = ok_vrpc_count_.load(std::memory_order_relaxed);
    std::int64_t const err = error_vrpc_count_.load(std::memory_order_relaxed);
    if (ok == 0 && err == 0) return "none";
    if (err == 0) return "all_ok";
    if (ok == 0) return "all_error";
    return "some_ok";
  }

  /// Atomically marks this session's `session/durations` telemetry as recorded
  /// so pool shutdown (`Close()`) and async stream teardown (`OnSessionClosed`)
  /// never double-record the same session.
  bool TryMarkDurationRecorded() {
    return !duration_recorded_.exchange(true, std::memory_order_acq_rel);
  }

  /// Returns true if `rpc_id` was cancelled by the caller after write
  /// completion and the slot is waiting for the server response frame to drain
  /// it.
  virtual bool IsCancelledAwaitingDrain(std::int64_t rpc_id) const;

  /// Returns the latency tracker for this session.
  virtual PeakEwmaTracker* GetLatencyTracker();
  PeakEwmaTracker& latency_tracker() { return latency_tracker_; }
  PeakEwmaTracker const& latency_tracker() const { return latency_tracker_; }

  /// Returns the stub pointer used by this session.
  virtual google::bigtable::v2::Bigtable::Stub* stub() const;

  /// Returns the number of active calls on the channel (`Stub`) sharing this
  /// session.
  virtual int GetStubActiveCalls() const;

  /// Tracks whether the session is currently queued inside the idle pool stack.
  virtual bool in_idle_queue() const;
  virtual void set_in_idle_queue(bool v);

  /// O(1) index into `SessionPoolImpl::stubs_` (`-1` if unset/mock).
  int stub_index() const { return stub_index_; }
  void set_stub_index(int idx) { stub_index_ = idx; }

  /// Type-erased pointer to the owning shard's `SessionList::SessionHandle`,
  /// avoiding shard mutex acquisition on the per-RPC fast path while keeping
  /// the session layer independent of pool headers.
  std::shared_ptr<void> const& pool_handle() const { return pool_handle_; }
  void set_pool_handle(std::shared_ptr<void> handle) {
    pool_handle_ = std::move(handle);
  }

  // SessionStreamListener implementations:
  void OnInitialMetadata(
      std::multimap<grpc::string_ref, grpc::string_ref> const& metadata)
      override;
  void OnSessionReady(
      google::bigtable::v2::OpenSessionResponse const& response) override;
  void OnMessage(google::bigtable::v2::SessionResponse message) override;
  void OnHeartbeat() override;
  void OnWriteDone(bool ok) override;
  void OnClosed(google::cloud::Status status) override;

 private:
  SessionImpl(std::weak_ptr<SessionListener> listener,
              google::cloud::CompletionQueue event_cq, ScheduleFn scheduler);

  void CancelVirtualRpcSerialized(std::int64_t rpc_id);
  void CloseSerialized();
  void DrainSerialized();
  void InitiateStreamClose();
  void OnSessionReadySerialized(
      google::bigtable::v2::OpenSessionResponse const& response);
  void OnMessageSerialized(google::bigtable::v2::SessionResponse message);
  void OnWriteDoneSerialized(bool ok);
  void OnClosedSerialized(google::cloud::Status status);

  void ScheduleKeepAliveCheck();
  void CheckKeepAliveSerialized(TimePoint scheduled_time);
  std::chrono::nanoseconds GetKeepAliveInterval() const;
  bool IsRunningOnEventLoopThread() const;
  void RunAsync(std::function<void()> task);

  std::weak_ptr<SessionListener> listener_;
  std::shared_ptr<SessionStream> stream_;
  std::optional<google::cloud::CompletionQueue> event_cq_;
  ::google::cloud::internal::CompletionQueueImpl* event_cq_impl_ = nullptr;
  std::atomic<double> keepalive_multiplier_override_{0.0};
  std::atomic<std::int64_t> goaway_drain_timeout_override_ms_{0};
  ScheduleFn scheduler_;

  struct ActiveRpcInfo {
    std::atomic<std::int64_t> rpc_id{0};
    TimePoint start_time;
    std::atomic<std::int64_t> sent_at_ns{kUnsetTimeNs};
    GenericCallback callback;
    grpc::ClientContext* context = nullptr;
    std::atomic<bool> write_completed{false};
    std::atomic<bool> cancelled_awaiting_drain{false};
  };

  mutable std::mutex state_mutex_;
  std::atomic<State> state_{State::kConnecting};
  std::atomic<int> active_rpc_count_{0};
  google::cloud::Status disconnect_status_ = google::cloud::Status();
  google::bigtable::v2::CloseSessionRequest::CloseSessionReason close_reason_ =
      google::bigtable::v2::CloseSessionRequest::CLOSE_SESSION_REASON_USER;
  std::int64_t last_admitted_rpc_id_ = -1;
  std::atomic<std::int64_t> last_dispatched_rpc_id_{0};
  std::atomic<std::int64_t> last_sent_at_ns_{kUnsetTimeNs};
  std::atomic<std::int64_t> ok_vrpc_count_{0};
  std::atomic<std::int64_t> error_vrpc_count_{0};

  std::optional<google::bigtable::v2::SessionParametersResponse>
      session_params_;
  std::optional<google::bigtable::v2::BackendIdentifier> backend_identifier_;
  std::optional<google::bigtable::v2::ClusterInformation> last_cluster_info_;
  std::optional<transport::PeerInfo> peer_info_;
  std::optional<PeerLoadInfo> peer_load_info_;
  std::atomic<double> peer_weight_{1.0};
  PeakEwmaTracker latency_tracker_;
  ActiveRpcInfo active_rpc_;
  std::deque<google::bigtable::v2::SessionRequest> write_queue_;
  std::atomic<std::int64_t> writing_rpc_id_{0};
  mutable std::atomic<std::int64_t> post_write_cancelling_rpc_id_{0};
  std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config_;
  std::atomic<std::int64_t> last_heartbeat_time_ns_{0};

  std::atomic<std::int64_t> last_active_ns_{0};
  TimePoint created_at_{Clock::now()};
  std::atomic<std::int64_t> ready_at_ns_{kUnsetTimeNs};
  std::atomic<int> write_queue_size_{0};
  std::atomic<bool> in_idle_queue_{false};
  std::atomic<bool> auto_finalize_{true};
  std::atomic<bool> duration_recorded_{false};
  int stub_index_ = -1;
  std::shared_ptr<void> pool_handle_;
  std::string open_table_name_;
  std::string open_app_profile_id_;
  google::bigtable::v2::OpenTableRequest::Permission open_permission_ =
      google::bigtable::v2::OpenTableRequest::PERMISSION_UNSET;
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SESSION_SESSION_IMPL_H
