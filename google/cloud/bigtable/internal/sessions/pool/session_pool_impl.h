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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_SESSION_POOL_IMPL_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_SESSION_POOL_IMPL_H

#include "google/cloud/bigtable/internal/sessions/pool/dynamic_picker.h"
#include "google/cloud/bigtable/internal/sessions/pool/lightweight_thread_pool.h"
#include "google/cloud/bigtable/internal/sessions/pool/pool_executors.h"
#include "google/cloud/bigtable/internal/sessions/pool/pool_sizer.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_creation_budget.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_list.h"
#include "google/cloud/bigtable/internal/sessions/session/session_impl.h"
#include "google/cloud/bigtable/internal/sessions/telemetry/session_tracer.h"
#include "google/cloud/bigtable/internal/sessions/transport/peak_ewma_tracker.h"
#include "google/cloud/background_threads.h"
#include "google/cloud/completion_queue.h"
#include "google/cloud/future.h"
#include "google/cloud/internal/background_threads_impl.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/options.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/version.h"
#include <google/bigtable/v2/bigtable.grpc.pb.h>
#include <google/bigtable/v2/session.pb.h>
#include <grpcpp/grpcpp.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

class MetricsCollector;

/// Thread-safe FIFO queue of idle `SessionImpl` instances within a connection
/// group.
class IdleSessionQueue {
 public:
  ~IdleSessionQueue() { Clear(); }

  void Push(std::shared_ptr<SessionImpl> session) {
    if (!session) return;
    std::lock_guard<std::mutex> lock(mu_);
    if (session->state() != SessionImpl::State::kReady ||
        session->in_idle_queue()) {
      return;
    }
    session->set_in_idle_queue(true);
    queue_.push_back(std::move(session));
  }

  std::shared_ptr<SessionImpl> Pop() {
    std::lock_guard<std::mutex> lock(mu_);
    while (!queue_.empty()) {
      auto sess = std::move(queue_.front());
      queue_.pop_front();
      if (!sess) continue;
      sess->set_in_idle_queue(false);
      if (sess->state() == SessionImpl::State::kReady) {
        return sess;
      }
    }
    return nullptr;
  }

  void Clear() {
    std::deque<std::shared_ptr<SessionImpl>> old;
    {
      std::lock_guard<std::mutex> lock(mu_);
      for (auto& sess : queue_) {
        if (sess) {
          sess->set_in_idle_queue(false);
        }
      }
      old = std::move(queue_);
      queue_.clear();
    }
  }

  void Remove(std::unordered_set<SessionImpl*> const& to_remove) {
    std::vector<std::shared_ptr<SessionImpl>> removed;
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = std::remove_if(
          queue_.begin(), queue_.end(),
          [&](std::shared_ptr<SessionImpl> const& s) {
            return s != nullptr && to_remove.find(s.get()) != to_remove.end();
          });
      removed.reserve(static_cast<size_t>(std::distance(it, queue_.end())));
      for (auto r = it; r != queue_.end(); ++r) {
        if (*r) {
          (*r)->set_in_idle_queue(false);
        }
        removed.push_back(std::move(*r));
      }
      queue_.erase(it, queue_.end());
    }
  }

  size_t Size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return queue_.size();
  }

 private:
  mutable std::mutex mu_;
  std::deque<std::shared_ptr<SessionImpl>> queue_;
};

/// Maps a gRPC peer ID string to a stable numeric ID. Numeric IDs are parsed
/// directly; other non-empty IDs (except `"default_peer"`) are hashed.
std::int64_t ParseNumericPeerId(std::string_view peer_id);

/// Manages sharded collections of active `SessionImpl` streams grouped by
/// backend peer (`ConnectionGroup`), load-balancing virtual RPCs, queuing
/// requests under contention, and scaling capacity within creation budgets.
class SessionPoolImpl : public SessionListener,
                        public telemetry::SessionDebugProvider,
                        public std::enable_shared_from_this<SessionPoolImpl> {
  friend class SessionPoolImplTestPeer;

 public:
  /// Process-wide default for the maximum sessions per pool, used only when
  /// `SessionsMaxSessionsOption` is not set. Prefer the per-connection option.
  static int GetDefaultMaxSessions();
  static void SetDefaultMaxSessions(int max_sessions);

  /// Process-wide default handshake timeout for opening a session (default
  /// 30s).
  static std::chrono::nanoseconds GetHandshakeTimeout();
  static void SetHandshakeTimeout(std::chrono::nanoseconds timeout);

  void SetConsecutiveSessionFailureThresholdForTesting(int threshold) {
    consecutive_session_failure_threshold_.store(std::max(1, threshold),
                                                 std::memory_order_relaxed);
  }
  int consecutive_session_failures() const {
    return consecutive_session_failures_.load(std::memory_order_relaxed);
  }
  int consecutive_session_failure_threshold() const {
    return consecutive_session_failure_threshold_.load(
        std::memory_order_relaxed);
  }
  std::string const& serialized_open_payload() const {
    return serialized_open_payload_;
  }

  /// Executors used by this pool (shared with sibling pools when supplied via
  /// `SessionsPoolExecutorsOption`). Exposed for testing.
  std::shared_ptr<PoolExecutors> executors() const { return executors_; }
  bool owns_executors() const { return owns_executors_; }

  telemetry::PoolSnapshot GetPoolSnapshot() const override;

  telemetry::SessionTracer const& session_tracer() const {
    return session_tracer_;
  }
  telemetry::SessionTracer& mutable_session_tracer_for_testing() {
    return session_tracer_;
  }
  void ConfigureSessionTracerForTesting(telemetry::SessionTracerConfig config) {
    session_tracer_.Configure(std::move(config));
  }

  /// Session metadata captured at the moment a virtual RPC attempt completes,
  /// exposed on the calling thread while invoking the attempt completion
  /// callback.
  struct CompletedAttemptSessionInfo {
    std::optional<transport::PeerInfo> peer_info;
    std::optional<SessionImpl::TimePoint> sent_at;
    std::optional<google::bigtable::v2::ClusterInformation> cluster_info;
  };
  static CompletedAttemptSessionInfo const*
  CurrentCompletedAttemptSessionInfo();

  using GenericCallback = std::function<void(
      google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>)>;

  struct PendingCall {
    grpc::ClientContext* context;
    google::bigtable::v2::VirtualRpcRequest request;
    GenericCallback callback;
    std::function<void()> cancel_timer;
    SessionImpl::TimePoint enqueue_time = SessionImpl::Clock::now();
    bool is_write_request = false;
  };

  struct ConnectionGroup {
    std::string peer_id;
    std::vector<SessionEntry> sessions;
    std::shared_ptr<std::vector<SessionEntry> const> sessions_ptr;
    std::deque<PendingCall> pending_queue;
    std::atomic<int> active_calls_count{0};
    std::atomic<int> pending_calls_count{0};
    std::atomic<int> active_sessions_count{0};
    std::atomic<int> consecutive_connection_failures{0};
    std::atomic<bool> is_blacklisted{false};
    std::atomic<int64_t> blacklist_expire_time_us{0};
    std::atomic<bool> is_closed{false};
    std::atomic<double> avg_peer_weight{1.0};
    pool::OkGatedPeakEwma ok_gated_ewma;
    IdleSessionQueue idle_queue;
  };
  using AfeGroup = ConnectionGroup;

  /// Creates the session pool and initiates startup session pre-warming.
  static std::shared_ptr<SessionPoolImpl> Create(
      std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
      std::string_view table_name, std::string_view app_profile_id,
      google::bigtable::v2::OpenTableRequest::Permission permission,
      google::cloud::Options options, ScheduleFn scheduler);

  static std::shared_ptr<SessionPoolImpl> Create(
      std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
      std::string_view authorized_view_name, std::string_view app_profile_id,
      google::bigtable::v2::OpenAuthorizedViewRequest::Permission permission,
      google::cloud::Options options, ScheduleFn scheduler);

  static std::shared_ptr<SessionPoolImpl> Create(
      std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
      std::string_view materialized_view_name, std::string_view app_profile_id,
      google::bigtable::v2::OpenMaterializedViewRequest::Permission permission,
      google::cloud::Options options, ScheduleFn scheduler);

  /// Creates a session pool pre-populated with `initial_sessions` for unit
  /// testing.
  static std::shared_ptr<SessionPoolImpl> CreateForTesting(
      std::vector<std::shared_ptr<SessionImpl>> initial_sessions,
      int max_queued_calls = 1000,
      std::optional<google::cloud::CompletionQueue> background_executor =
          std::nullopt,
      ScheduleFn scheduler = nullptr,
      std::chrono::nanoseconds idle_session_timeout = std::chrono::minutes(5),
      size_t min_pool_size = 1);

  ~SessionPoolImpl() override = default;

  /// Dispatches a virtual RPC to an available session or queues it if all
  /// sessions are currently busy.
  virtual google::cloud::Status StartVirtualRpc(
      grpc::ClientContext* context,
      google::bigtable::v2::VirtualRpcRequest request, GenericCallback callback,
      bool is_write_request);
  google::cloud::Status StartVirtualRpc(
      grpc::ClientContext* context,
      google::bigtable::v2::VirtualRpcRequest request,
      GenericCallback callback) {
    return StartVirtualRpc(context, std::move(request), std::move(callback),
                           false);
  }

  /// Immediately closes all sessions and aborts queued requests.
  virtual void Close();

  /// Drains active virtual RPCs up to `timeout` before closing all sessions.
  virtual google::cloud::Status CloseGracefully(
      std::chrono::nanoseconds timeout);

  /// Queries current session and call counts across all shards.
  virtual size_t active_sessions_count() const;
  virtual size_t pending_queue_size() const;
  virtual size_t active_calls_count() const;

  virtual std::chrono::nanoseconds GetReadRttEwma() const {
    return read_latency_tracker_.GetCurrentEwma();
  }
  virtual std::chrono::nanoseconds GetWriteRttEwma() const {
    return write_latency_tracker_.GetCurrentEwma();
  }

  /// Blocks until the startup pre-warming sessions finish handshaking.
  ///
  /// Returns `OK` if at least `initial_pool_size` sessions succeeded.
  virtual google::cloud::Status WaitForPreWarming(
      std::chrono::nanoseconds timeout);

  /// Returns the metrics collector for this pool.
  std::shared_ptr<MetricsCollector> metrics_collector() const {
    return metrics_collector_;
  }

  /// Updates the pool sizer, session creation budget, and picker routing policy
  /// from a server `SessionPoolConfiguration` payload.
  void UpdateConfig(google::bigtable::v2::SessionClientConfiguration::
                        SessionPoolConfiguration const& config) {
    if (pool_sizer_) {
      auto adjusted = config;
      if (user_max_sessions_override_ > 0 && adjusted.max_session_count() > 0 &&
          adjusted.max_session_count() > user_max_sessions_override_) {
        adjusted.set_max_session_count(user_max_sessions_override_);
      }
      pool_sizer_->UpdateConfig(adjusted);
    }
    budget_.UpdateConfig(config);
    picker_.UpdateConfig(config);
    if (config.consecutive_session_failure_threshold() > 0) {
      consecutive_session_failure_threshold_.store(
          config.consecutive_session_failure_threshold(),
          std::memory_order_relaxed);
    }
  }

  /// Updates sizer, budget, and picker configs directly for unit testing.
  void UpdateConfigForTesting(google::bigtable::v2::SessionClientConfiguration::
                                  SessionPoolConfiguration const& config) {
    if (pool_sizer_) {
      pool_sizer_->UpdateConfig(config);
    }
    budget_.UpdateConfig(config);
    picker_.UpdateConfig(config);
    if (config.consecutive_session_failure_threshold() > 0) {
      consecutive_session_failure_threshold_.store(
          config.consecutive_session_failure_threshold(),
          std::memory_order_relaxed);
    }
  }

  /// Scans all connection groups and closes eligible idle sessions.
  void ReapIdleSessions();

  /// Cancels all active and pending calls associated with `context`.
  void CancelByContext(
      grpc::ClientContext* context,
      google::cloud::Status status = google::cloud::internal::CancelledError(
          "Client context cancelled by user"));

  /// Allocates a pool-unique virtual RPC identifier shared across all client
  /// shards bound to this pool.
  int64_t AllocateRpcId() {
    return next_rpc_id_.fetch_add(1, std::memory_order_relaxed);
  }
  void SetFirstRpcId(int64_t first_rpc_id) {
    next_rpc_id_.store(first_rpc_id, std::memory_order_relaxed);
  }

  /// Clears any pending cancellation marker for `context` when a new operation
  /// starts on the same stack address.
  void ClearCancelledContext(grpc::ClientContext const* context);

 protected:
  SessionPoolImpl() : budget_(50, std::chrono::seconds(2)) {}  // For test mocks

 public:
  std::multimap<grpc::string_ref, grpc::string_ref> initial_metadata() const;
  std::optional<google::bigtable::v2::BackendIdentifier> backend() const;
  std::optional<google::bigtable::v2::SessionRefreshConfig>
  session_refresh_config() const;
  google::cloud::Status last_connection_error() const;

  // SessionListener implementations:
  void OnInitialMetadata(
      std::shared_ptr<SessionImpl> session,
      std::multimap<grpc::string_ref, grpc::string_ref> const& metadata)
      override;
  void OnSessionReady(
      std::shared_ptr<SessionImpl> session,
      google::bigtable::v2::OpenSessionResponse const& response) override;
  void OnGoAway(std::shared_ptr<SessionImpl> session) override;
  void OnSessionClosed(std::shared_ptr<SessionImpl> session,
                       google::cloud::Status status) override;
  void OnWriteDone(std::shared_ptr<SessionImpl> session) override;
  void OnSlotDrained(std::shared_ptr<SessionImpl> session,
                     int64_t rpc_id) override;
  int GetActiveCallsForStub(
      google::bigtable::v2::Bigtable::Stub const* stub) const override;

 private:
  SessionPoolImpl(std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
                  std::string_view table_name, std::string_view app_profile_id,
                  google::bigtable::v2::OpenTableRequest::Permission permission,
                  google::cloud::Options options, ScheduleFn scheduler);

  SessionPoolImpl(
      std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
      std::string_view table_name, std::string_view app_profile_id,
      google::bigtable::v2::OpenTableRequest::Permission permission,
      std::optional<google::cloud::CompletionQueue> background_executor,
      ScheduleFn scheduler, int max_queued_calls,
      std::chrono::nanoseconds idle_session_timeout, size_t min_pool_size);

  std::optional<google::cloud::CompletionQueue> GetGeneralCQ();

  struct Shard {
    size_t index = 0;
    mutable std::mutex mutex;
    std::optional<google::cloud::CompletionQueue> cq;
    ::google::cloud::internal::CompletionQueueImpl* cq_impl = nullptr;
    std::vector<std::shared_ptr<SessionImpl>> connecting_sessions;
    std::unordered_set<std::shared_ptr<SessionImpl>> draining_sessions;
    std::unordered_map<SessionImpl*, SessionImpl::TimePoint> draining_since;
    std::map<std::string, std::shared_ptr<ConnectionGroup>> connection_groups;
    std::shared_ptr<std::vector<std::shared_ptr<ConnectionGroup>> const>
        groups_snapshot;
    pool::SessionList session_list;
    std::unordered_map<SessionImpl*,
                       std::shared_ptr<pool::SessionList::SessionHandle>>
        session_handles;
    std::deque<PendingCall> pending_queue;
    std::atomic<int> total_queued_calls{0};
    std::atomic<int> total_active_calls{0};
    std::atomic<int> ready_sessions_count{0};
    bool is_closed = false;
    std::atomic<int64_t> fast_path_count{0};
    std::atomic<int64_t> fallback_path_count{0};
    std::atomic<int64_t> drain_path_count{0};
  };

  static bool IsTerminalHandshakeError(google::cloud::StatusCode code);
  std::shared_ptr<SessionImpl> PickAndAcquire(
      std::vector<SessionEntry> const& sessions, bool is_write_request);
  std::shared_ptr<SessionImpl> SpawnSessionOnShard(
      Shard& shard,
      std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config,
      bool is_pre_warm = false);
  void IncrementStubActiveCalls(SessionImpl const& session, int delta);
  void MaybeReplenishPool(Shard& shard, bool force_half_open_probe = false);
  void UpdateSessionsSnapshotLocked(ConnectionGroup* group);
  // Applies pool-scoped per-session settings (auto-finalize, keepalive).
  void ConfigureNewSession(SessionImpl& session) const;
  void CreateSession(
      Shard& shard,
      std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config);
  void FailPendingCalls(std::deque<PendingCall>& queue,
                        google::cloud::Status status);
  struct CallToStart {
    std::shared_ptr<SessionImpl> session;
    grpc::ClientContext* context;
    google::bigtable::v2::VirtualRpcRequest request;
    GenericCallback callback;
    std::function<void()> cancel_timer;
    SessionImpl::TimePoint enqueue_time;
    bool is_write_request = false;
  };

  void TryDrainGroupPendingQueueLocked(
      Shard& shard, ConnectionGroup* group,
      std::vector<CallToStart>& calls_to_start);

  void DrainQueue(Shard& shard, std::shared_ptr<ConnectionGroup> group);
  void FinalizeSessionCall(std::shared_ptr<SessionImpl> session,
                           std::shared_ptr<ConnectionGroup> group, Shard* shard,
                           int64_t rpc_id);
  std::shared_ptr<ConnectionGroup> GetLeastLoadedGroup(
      std::shared_ptr<
          std::vector<std::shared_ptr<ConnectionGroup>> const> const& snapshot)
      const;
  void UpdateGroupsSnapshotLocked(Shard& shard);
  void ScheduleReaper();
  void RecordSessionCloseTelemetry(std::shared_ptr<SessionImpl> const& session,
                                   google::cloud::Status const& status,
                                   bool was_connecting, bool was_ready,
                                   bool was_draining);
  Shard& GetShardForSession(std::shared_ptr<SessionImpl> const& session);
  Shard& GetShardForCurrentThread();

  std::vector<google::bigtable::v2::Bigtable::Stub*> stubs_;
  std::atomic<size_t> next_stub_index_{0};
  std::string table_name_;
  std::string app_profile_id_;
  google::bigtable::v2::OpenTableRequest::Permission permission_;
  std::string serialized_open_payload_;
  google::bigtable::v2::FeatureFlags feature_flags_;
  google::cloud::Options options_;
  std::optional<google::cloud::CompletionQueue> default_cq_;
  // I/O loops + callback threads. Shared across pools of one connection
  // unless this pool created them itself (`owns_executors_`).
  std::shared_ptr<PoolExecutors> executors_;
  bool owns_executors_ = false;
  // Borrowed from `executors_`; null for CreateForTesting pools.
  LightweightThreadPool* user_callback_pool_ = nullptr;
  std::atomic<size_t> next_event_loop_index_{0};
  ScheduleFn scheduler_;
  int max_queued_calls_ = 0;
  // Per-pool keepalive multiplier (0 = use the SessionImpl process default).
  double keepalive_multiplier_ = 0.0;

  std::atomic<bool> is_closed_{false};
  std::vector<std::unique_ptr<Shard>> shards_;
  std::atomic<size_t> next_shard_index_{0};

  DynamicPicker picker_;
  std::unique_ptr<PoolSizer> pool_sizer_;
  SessionCreationBudget budget_;
  std::atomic<int> total_queued_calls_{0};
  std::atomic<int> total_active_calls_{0};
  std::atomic<int> total_ready_sessions_{0};
  std::atomic<int> total_connecting_sessions_{0};
  std::atomic<int> total_draining_sessions_{0};
  std::atomic<int> active_lifecycle_callbacks_{0};
  std::atomic<int> consecutive_session_failures_{0};
  std::atomic<int> consecutive_session_failure_threshold_{15};

  // Lock acquisition hierarchy (outer -> inner):
  //   Shard::mutex -> SessionList::mu_
  //   budget_holding_mutex_, pre_warm_mutex_, error_mutex_, cancel_ctx_mutex_
  //   are leaf mutexes (never acquire Shard::mutex while holding a leaf mutex).
  mutable std::mutex pre_warm_mutex_;
  std::condition_variable pre_warm_condvar_;
  std::condition_variable active_calls_condvar_;
  std::unordered_set<std::shared_ptr<SessionImpl>> pre_warm_sessions_;
  int pre_warm_success_count_ = 0;
  int pre_warm_started_count_ = 0;
  bool pre_warm_started_ = false;
  int initial_pool_size_ = 0;
  int user_max_sessions_override_ = 0;

  std::chrono::nanoseconds idle_session_timeout_;
  size_t min_pool_size_;
  std::shared_ptr<MetricsCollector> metrics_collector_;

  mutable std::mutex error_mutex_;
  google::cloud::Status last_connection_error_;
  mutable std::mutex metadata_mutex_;
  std::vector<std::pair<std::string, std::string>> initial_metadata_storage_;
  std::optional<google::bigtable::v2::SessionRefreshConfig>
      last_refresh_config_;
  SessionStream::MetadataHook metadata_hook_;

  // Read/write latency EWMAs (exported via GetReadRttEwma/GetWriteRttEwma).
  PeakEwmaTracker read_latency_tracker_;
  PeakEwmaTracker write_latency_tracker_;

  std::vector<std::atomic<int>> active_calls_per_stub_;
  telemetry::SessionTracer session_tracer_;
  mutable std::mutex budget_holding_mutex_;
  std::unordered_set<std::shared_ptr<SessionImpl>> sessions_holding_budget_;
  std::atomic<bool> replenish_timer_scheduled_{false};
  mutable std::mutex cancel_ctx_mutex_;
  std::unordered_set<grpc::ClientContext const*> cancelled_contexts_;
  bool ConsumeCancelledContext(grpc::ClientContext const* context);
  std::atomic<int64_t> next_rpc_id_{1};
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_SESSION_POOL_IMPL_H
