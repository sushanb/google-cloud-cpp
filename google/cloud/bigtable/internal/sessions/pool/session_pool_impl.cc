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
#include "google/cloud/bigtable/internal/sessions/logging_helper.h"
#include "google/cloud/bigtable/internal/sessions/pool/dynamic_picker.h"
#include "google/cloud/bigtable/internal/sessions/pool/lightweight_thread_pool.h"
#include "google/cloud/bigtable/internal/sessions/pool/metrics_collector.h"
#include "google/cloud/bigtable/internal/sessions/pool/pool_executors.h"
#include "google/cloud/bigtable/internal/sessions/pool/pool_sizer.h"
#include "google/cloud/bigtable/internal/sessions/session/session_impl.h"
#include "google/cloud/bigtable/internal/sessions/session/write_status.h"
#include "google/cloud/bigtable/internal/sessions/sessions_options.h"
#include "google/cloud/bigtable/internal/sessions/transport/current_cq.h"
#include "google/cloud/internal/getenv.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/internal/random.h"
#include "google/cloud/log.h"
#include "google/cloud/options.h"
#include "google/cloud/status.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include <google/bigtable/v2/request_stats.pb.h>
#include <google/protobuf/duration.pb.h>
#include <grpcpp/client_context.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

namespace {
std::atomic<int> g_default_max_sessions{400};
std::atomic<int64_t> g_handshake_timeout_ns{
    std::chrono::nanoseconds(std::chrono::seconds(30)).count()};

thread_local SessionPoolImpl::CompletedAttemptSessionInfo const*
    g_current_completed_attempt_session_info = nullptr;

class ScopedCompletedAttemptSessionInfo {
 public:
  explicit ScopedCompletedAttemptSessionInfo(
      SessionPoolImpl::CompletedAttemptSessionInfo const* info)
      : prev_(g_current_completed_attempt_session_info) {
    g_current_completed_attempt_session_info = info;
  }
  ~ScopedCompletedAttemptSessionInfo() {
    g_current_completed_attempt_session_info = prev_;
  }

 private:
  SessionPoolImpl::CompletedAttemptSessionInfo const* prev_;
};

void RecordTransportLatencyIfPositive(
    telemetry::SessionTracer& tracer,
    SessionPoolImpl::ConnectionGroup* group_ptr,
    google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse> const&
        resp,
    std::chrono::nanoseconds e2e_latency, double client_blocking_ms,
    std::optional<transport::PeerInfo> const& peer_info,
    bool is_write_request) {
  double const e2e_ms =
      std::chrono::duration<double, std::milli>(e2e_latency).count();
  double const wire_e2e_ms = std::max(0.0, e2e_ms - client_blocking_ms);
  double backend_ms = 0.0;
  if (resp.ok() && resp->has_stats() && resp->stats().has_backend_latency()) {
    auto const& b = resp->stats().backend_latency();
    std::chrono::nanoseconds const backend_lat =
        std::chrono::seconds(b.seconds()) + std::chrono::nanoseconds(b.nanos());
    backend_ms = std::chrono::duration<double, std::milli>(backend_lat).count();
  }
  if (group_ptr != nullptr) {
    group_ptr->ok_gated_ewma.RecordOutcome(resp.status(), e2e_ms * 1000.0,
                                           client_blocking_ms * 1000.0,
                                           std::max(0.0, backend_ms) * 1000.0);
  }
  if (!resp.ok() || backend_ms <= 0.0 || (wire_e2e_ms - backend_ms) <= 0.0) {
    return;
  }
  telemetry::TransportLatencyAttributes attrs;
  if (peer_info.has_value()) {
    attrs.transport_type =
        transport::PeerInfo::TransportTypeToString(peer_info->transport_type);
    attrs.afe_location = peer_info->application_frontend_subzone;
  }
  attrs.method = is_write_request ? "Bigtable.MutateRow" : "Bigtable.ReadRow";
  tracer.RecordTransportLatencyMs(wire_e2e_ms - backend_ms, attrs);
}
}  // namespace

SessionPoolImpl::CompletedAttemptSessionInfo const*
SessionPoolImpl::CurrentCompletedAttemptSessionInfo() {
  return g_current_completed_attempt_session_info;
}

std::chrono::nanoseconds SessionPoolImpl::GetHandshakeTimeout() {
  return std::chrono::nanoseconds(
      g_handshake_timeout_ns.load(std::memory_order_relaxed));
}

void SessionPoolImpl::SetHandshakeTimeout(std::chrono::nanoseconds timeout) {
  g_handshake_timeout_ns.store(timeout.count(), std::memory_order_relaxed);
}

std::int64_t ParseNumericPeerId(std::string_view peer_id) {
  if (peer_id.empty() || peer_id == "default_peer") return 0;
  std::int64_t val = 0;
  if (absl::SimpleAtoi(peer_id, &val)) {
    return val;
  }
  return static_cast<std::int64_t>(
      std::hash<std::string>{}(std::string(peer_id)));
}

int SessionPoolImpl::GetDefaultMaxSessions() {
  if (auto env = google::cloud::internal::GetEnv(
          "GOOGLE_CLOUD_BIGTABLE_SESSIONS_MAX_SESSIONS")) {
    int v = 0;
    if (absl::SimpleAtoi(*env, &v) && v > 0) return v;
  }
  return g_default_max_sessions.load(std::memory_order_relaxed);
}

void SessionPoolImpl::SetDefaultMaxSessions(int max_sessions) {
  g_default_max_sessions.store(max_sessions, std::memory_order_relaxed);
}

telemetry::PoolSnapshot SessionPoolImpl::GetPoolSnapshot() const {
  telemetry::PoolSnapshot snap;
  snap.pool_key = table_name_;
  snap.ready_sessions = static_cast<std::size_t>(
      std::max<int>(0, total_ready_sessions_.load(std::memory_order_relaxed)));
  std::size_t shard_holding_budget = 0;
  std::size_t shard_expected_capacity = 0;
  std::map<std::int64_t, telemetry::PeerSnapshotRow> merged_peers;
  for (auto const& shard : shards_) {
    std::lock_guard<std::mutex> l(shard->mutex);
    shard_holding_budget += shard->session_list.sessions_holding_budget_count();
    shard_expected_capacity += shard->session_list.expected_capacity();
    for (auto const& [peer_id, cg] :
         shard->session_list.GetConnectionGroupsSnapshot()) {
      auto& row = merged_peers[peer_id];
      row.peer_id = peer_id;
      row.ref_count += cg.ref_count;
      row.idle_count += cg.idle_sessions.size();
      row.in_flight_count += cg.in_flight_count;
      row.e2e_ewma_us = cg.ewma.e2e_ewma_us();
      row.transport_ewma_us = cg.ewma.transport_ewma_us();
    }
    for (auto const& [peer_str, group] : shard->connection_groups) {
      if (!group) continue;
      auto const pid = ParseNumericPeerId(peer_str);
      auto it = merged_peers.find(pid);
      if (it != merged_peers.end()) {
        it->second.e2e_ewma_us = group->ok_gated_ewma.e2e_ewma_us();
        it->second.transport_ewma_us = group->ok_gated_ewma.transport_ewma_us();
      }
    }
  }
  {
    std::lock_guard<std::mutex> lock(budget_holding_mutex_);
    snap.sessions_holding_budget =
        std::max(sessions_holding_budget_.size(), shard_holding_budget);
  }
  snap.expected_capacity =
      std::max(snap.ready_sessions + snap.sessions_holding_budget,
               shard_expected_capacity);
  for (auto const& [peer_id, row] : merged_peers) {
    (void)peer_id;
    snap.peers.push_back(row);
  }
  return snap;
}

std::shared_ptr<SessionPoolImpl> SessionPoolImpl::Create(
    std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
    std::string_view table_name, std::string_view app_profile_id,
    google::bigtable::v2::OpenTableRequest::Permission permission,
    google::cloud::Options options, ScheduleFn scheduler) {
  auto pool = std::shared_ptr<SessionPoolImpl>(new SessionPoolImpl(
      stubs, table_name, app_profile_id, permission, options, scheduler));

  int initial_pool_size = 1;
  if (options.has<::google::cloud::bigtable_internal::
                      SessionsInitialPoolSizeOption>()) {
    initial_pool_size = options.get<
        ::google::cloud::bigtable_internal::SessionsInitialPoolSizeOption>();
    if (initial_pool_size < 0) initial_pool_size = 1;
  }
  pool->initial_pool_size_ = initial_pool_size;

  int max_sessions = GetDefaultMaxSessions();
  if (options.has<
          ::google::cloud::bigtable_internal::SessionsMaxSessionsOption>()) {
    int v = options.get<
        ::google::cloud::bigtable_internal::SessionsMaxSessionsOption>();
    if (v > 0) {
      max_sessions = v;
      pool->user_max_sessions_override_ = v;
    }
  }
  pool->pool_sizer_ = std::make_unique<PoolSizer>(
      initial_pool_size, std::max(initial_pool_size, max_sessions), 0.10f);

  pool->pre_warm_started_ = true;
  int batch_size = 64;
  if (pool->scheduler_ == nullptr) {
    batch_size = initial_pool_size;
  }

  std::weak_ptr<SessionPoolImpl> weak_pool = pool;
  auto spawn_next_batch = std::make_shared<std::function<void()>>();
  std::weak_ptr<std::function<void()>> weak_spawn = spawn_next_batch;
  *spawn_next_batch = [weak_pool, batch_size, initial_pool_size, spawned = 0,
                       weak_spawn, stubs, t_name = std::string(table_name),
                       ap_id = std::string(app_profile_id),
                       permission]() mutable {
    auto pool = weak_pool.lock();
    if (!pool) return;

    if (pool->is_closed_.load(std::memory_order_acquire)) return;
    int to_spawn = std::min(batch_size, initial_pool_size - spawned);
    std::vector<std::shared_ptr<SessionImpl>> spawned_batch;
    spawned_batch.reserve(std::max(0, to_spawn));

    // Spawn sessions outside `pre_warm_mutex_` so `Shard::mutex` is never
    // acquired while holding `pre_warm_mutex_` (preserving the lock hierarchy).
    int actually_spawned = 0;
    for (int i = 0; i < to_spawn; ++i) {
      if (pool->is_closed_.load(std::memory_order_acquire)) break;
      if (pool->scheduler_ != nullptr) {
        if (!pool->budget_.TryReserve()) break;
      } else {
        pool->budget_.ForceReserve();
      }
      actually_spawned++;
      size_t shard_idx =
          pool->next_event_loop_index_.fetch_add(1, std::memory_order_relaxed) %
          pool->shards_.size();
      auto new_sess =
          pool->SpawnSessionOnShard(*pool->shards_[shard_idx], std::nullopt,
                                    /*is_pre_warm=*/true);
      if (!new_sess) continue;
      spawned_batch.push_back(std::move(new_sess));
    }

    std::shared_ptr<std::function<void()>> next_batch_cb;
    std::chrono::nanoseconds next_batch_delay = std::chrono::nanoseconds(0);
    {
      std::lock_guard<std::mutex> l_pw(pool->pre_warm_mutex_);
      if (pool->pre_warm_sessions_.empty() &&
          (spawned + actually_spawned >= initial_pool_size ||
           pool->scheduler_ == nullptr)) {
        pool->pre_warm_condvar_.notify_all();
      }
      spawned += actually_spawned;
      if (!pool->is_closed_.load(std::memory_order_acquire) &&
          spawned < initial_pool_size && pool->scheduler_ != nullptr) {
        if (auto self_spawn = weak_spawn.lock()) {
          std::chrono::nanoseconds delay = std::chrono::milliseconds(100);
          SessionImpl::TimePoint const next_avail =
              pool->budget_.NextAvailableTime();
          SessionImpl::TimePoint const now = SessionImpl::Clock::now();
          if (next_avail > now) {
            delay = std::max(delay,
                             (next_avail - now) + std::chrono::milliseconds(5));
          }
          next_batch_cb = std::move(self_spawn);
          next_batch_delay = delay;
        }
      }
    }
    if (next_batch_cb && pool->scheduler_ != nullptr) {
      pool->scheduler_(
          next_batch_delay,
          [self_spawn = std::move(next_batch_cb)]() { (*self_spawn)(); });
    }

    if (pool->is_closed_.load(std::memory_order_acquire)) {
      for (auto const& sess : spawned_batch) {
        if (sess != nullptr) sess->Close();
      }
    }
  };

  (*spawn_next_batch)();

  if (pool->scheduler_ != nullptr) {
    pool->ScheduleReaper();
  }
  if (pool->metrics_collector_) {
    pool->metrics_collector_->AddExporter(
        std::make_unique<LoggingMetricsExporter>());
    pool->metrics_collector_->Start();
  }

  return pool;
}

std::shared_ptr<SessionPoolImpl> SessionPoolImpl::Create(
    std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
    std::string_view authorized_view_name, std::string_view app_profile_id,
    google::bigtable::v2::OpenAuthorizedViewRequest::Permission permission,
    google::cloud::Options options, ScheduleFn scheduler) {
  options.set<::google::cloud::bigtable_internal::
                  SessionsOpenAuthorizedViewPermissionOption>(permission);
  auto table_perm = google::bigtable::v2::OpenTableRequest::PERMISSION_UNSET;
  if (permission ==
      google::bigtable::v2::OpenAuthorizedViewRequest::PERMISSION_READ) {
    table_perm = google::bigtable::v2::OpenTableRequest::PERMISSION_READ;
  } else if (permission == google::bigtable::v2::OpenAuthorizedViewRequest::
                               PERMISSION_WRITE) {
    table_perm = google::bigtable::v2::OpenTableRequest::PERMISSION_WRITE;
  } else if (permission == google::bigtable::v2::OpenAuthorizedViewRequest::
                               PERMISSION_READ_WRITE) {
    table_perm = google::bigtable::v2::OpenTableRequest::PERMISSION_READ_WRITE;
  }
  return Create(std::move(stubs), authorized_view_name, app_profile_id,
                table_perm, std::move(options), std::move(scheduler));
}

std::shared_ptr<SessionPoolImpl> SessionPoolImpl::Create(
    std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
    std::string_view materialized_view_name, std::string_view app_profile_id,
    google::bigtable::v2::OpenMaterializedViewRequest::Permission permission,
    google::cloud::Options options, ScheduleFn scheduler) {
  options.set<::google::cloud::bigtable_internal::
                  SessionsOpenMaterializedViewPermissionOption>(permission);
  auto table_perm =
      (permission ==
       google::bigtable::v2::OpenMaterializedViewRequest::PERMISSION_READ)
          ? google::bigtable::v2::OpenTableRequest::PERMISSION_READ
          : google::bigtable::v2::OpenTableRequest::PERMISSION_UNSET;
  return Create(std::move(stubs), materialized_view_name, app_profile_id,
                table_perm, std::move(options), std::move(scheduler));
}

std::shared_ptr<SessionPoolImpl> SessionPoolImpl::CreateForTesting(
    std::vector<std::shared_ptr<SessionImpl>> initial_sessions,
    int max_queued_calls,
    std::optional<google::cloud::CompletionQueue> background_executor,
    ScheduleFn scheduler, std::chrono::nanoseconds idle_session_timeout,
    size_t min_pool_size) {
  auto pool = std::shared_ptr<SessionPoolImpl>(new SessionPoolImpl(
      {}, "", "", google::bigtable::v2::OpenTableRequest::PERMISSION_UNSET,
      background_executor, scheduler, max_queued_calls, idle_session_timeout,
      min_pool_size));
  pool->initial_pool_size_ = initial_sessions.size();
  pool->pool_sizer_ = std::make_unique<PoolSizer>(
      static_cast<int>(min_pool_size),
      std::max<int>(pool->initial_pool_size_, 10), 0.10f);

  pool->pre_warm_started_ = true;
  for (auto const& sess : initial_sessions) {
    auto& shard = *pool->shards_[0];  // In testing, we usually have 1 shard
    if (sess->state() == SessionImpl::State::kReady) {
      std::string peer_id = sess->GetPeerId().value_or("default_peer");
      std::shared_ptr<ConnectionGroup> group;
      {
        std::lock_guard<std::mutex> l_shard(shard.mutex);
        auto handle = shard.session_list.NewStartingHandle(
            reinterpret_cast<std::int64_t>(sess.get()), shard.index);
        shard.session_list.OnSessionReady(handle, ParseNumericPeerId(peer_id));
        sess->set_pool_handle(handle);
        shard.session_handles[sess.get()] = std::move(handle);
        auto it = shard.connection_groups.find(peer_id);
        bool new_group = false;
        if (it == shard.connection_groups.end()) {
          group = std::make_shared<ConnectionGroup>();
          group->peer_id = peer_id;
          shard.connection_groups[peer_id] = group;
          new_group = true;
        } else {
          group = it->second;
        }
        group->sessions.push_back(SessionEntry{sess});
        group->idle_queue.Push(sess);
        group->active_sessions_count++;
        pool->UpdateSessionsSnapshotLocked(group.get());
        if (new_group) {
          pool->UpdateGroupsSnapshotLocked(shard);
        }
      }
      shard.ready_sessions_count.fetch_add(1, std::memory_order_relaxed);
      pool->total_ready_sessions_++;
      if (pool->metrics_collector_) {
        pool->metrics_collector_->OnSessionCreated();
      }
    } else {
      {
        std::lock_guard<std::mutex> l_shard(shard.mutex);
        auto handle = shard.session_list.NewStartingHandle(
            reinterpret_cast<std::int64_t>(sess.get()), shard.index);
        sess->set_pool_handle(handle);
        shard.session_handles[sess.get()] = std::move(handle);
        shard.connecting_sessions.push_back(sess);
      }
      pool->total_connecting_sessions_++;
      std::lock_guard<std::mutex> l_pw(pool->pre_warm_mutex_);
      pool->pre_warm_sessions_.insert(sess);
      pool->pre_warm_started_count_++;
    }
  }

  if (pool->scheduler_ != nullptr) {
    pool->ScheduleReaper();
  }
  if (pool->metrics_collector_) {
    pool->metrics_collector_->Start();
  }

  return pool;
}

SessionPoolImpl::SessionPoolImpl(
    std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
    std::string_view table_name, std::string_view app_profile_id,
    google::bigtable::v2::OpenTableRequest::Permission permission,
    google::cloud::Options options, ScheduleFn scheduler)
    : stubs_(stubs),
      table_name_(table_name),
      app_profile_id_(app_profile_id),
      permission_(permission),
      options_(std::move(options)),
      scheduler_(std::move(scheduler)),
      budget_(50, std::chrono::seconds(2)),
      metrics_collector_(std::make_shared<MetricsCollector>(scheduler_)),
      last_connection_error_(google::cloud::Status()),
      active_calls_per_stub_(stubs.size()) {
  max_queued_calls_ = options_.get<
      ::google::cloud::bigtable_internal::SessionsMaxQueuedCallsOption>();
  if (max_queued_calls_ <= 0) max_queued_calls_ = 1000;

  if (options_.has<
          ::google::cloud::bigtable_internal::SessionsMetadataHookOption>()) {
    metadata_hook_ = options_.get<
        ::google::cloud::bigtable_internal::SessionsMetadataHookOption>();
  }
  if (options_.has<::google::cloud::bigtable_internal::
                       SessionsKeepaliveMultiplierOption>()) {
    keepalive_multiplier_ =
        options_.get<::google::cloud::bigtable_internal::
                         SessionsKeepaliveMultiplierOption>();
  }

  if (!options_.has<::google::cloud::bigtable_internal::
                        SessionsOpenAuthorizedViewPermissionOption>() &&
      table_name_.find("/authorizedViews/") != std::string::npos) {
    auto const av_perm =
        (permission_ ==
         google::bigtable::v2::OpenTableRequest::PERMISSION_WRITE)
            ? google::bigtable::v2::OpenAuthorizedViewRequest::PERMISSION_WRITE
        : (permission_ ==
           google::bigtable::v2::OpenTableRequest::PERMISSION_READ_WRITE)
            ? google::bigtable::v2::OpenAuthorizedViewRequest::
                  PERMISSION_READ_WRITE
            : google::bigtable::v2::OpenAuthorizedViewRequest::PERMISSION_READ;
    options_.set<::google::cloud::bigtable_internal::
                     SessionsOpenAuthorizedViewPermissionOption>(av_perm);
  } else if (!options_
                  .has<::google::cloud::bigtable_internal::
                           SessionsOpenMaterializedViewPermissionOption>() &&
             table_name_.find("/materializedViews/") != std::string::npos) {
    options_.set<::google::cloud::bigtable_internal::
                     SessionsOpenMaterializedViewPermissionOption>(
        google::bigtable::v2::OpenMaterializedViewRequest::PERMISSION_READ);
  }

  {
    if (options_.has<::google::cloud::bigtable_internal::
                         SessionsOpenAuthorizedViewPermissionOption>()) {
      google::bigtable::v2::OpenAuthorizedViewRequest av_req;
      av_req.set_authorized_view_name(table_name_);
      av_req.set_app_profile_id(app_profile_id_);
      av_req.set_permission(
          options_.get<::google::cloud::bigtable_internal::
                           SessionsOpenAuthorizedViewPermissionOption>());
      (void)av_req.SerializeToString(&serialized_open_payload_);
    } else if (options_
                   .has<::google::cloud::bigtable_internal::
                            SessionsOpenMaterializedViewPermissionOption>()) {
      google::bigtable::v2::OpenMaterializedViewRequest mv_req;
      mv_req.set_materialized_view_name(table_name_);
      mv_req.set_app_profile_id(app_profile_id_);
      mv_req.set_permission(
          options_.get<::google::cloud::bigtable_internal::
                           SessionsOpenMaterializedViewPermissionOption>());
      (void)mv_req.SerializeToString(&serialized_open_payload_);
    } else {
      google::bigtable::v2::OpenTableRequest table_req;
      table_req.set_table_name(table_name_);
      table_req.set_app_profile_id(app_profile_id_);
      table_req.set_permission(permission_);
      (void)table_req.SerializeToString(&serialized_open_payload_);
    }

    if (options_.has<
            ::google::cloud::bigtable_internal::SessionsFeatureFlagsOption>()) {
      feature_flags_ = options_.get<
          ::google::cloud::bigtable_internal::SessionsFeatureFlagsOption>();
    } else {
      feature_flags_.set_sessions_compatible(true);
      feature_flags_.set_sessions_required(true);
      feature_flags_.set_routing_cookie(true);
      feature_flags_.set_retry_info(true);
      feature_flags_.set_peer_info(true);
      bool const metrics_enabled =
          !options_.has<::google::cloud::bigtable_internal::
                            SessionsClientSideMetricsEnabledOption>() ||
          options_.get<::google::cloud::bigtable_internal::
                           SessionsClientSideMetricsEnabledOption>();
      feature_flags_.set_client_side_metrics_enabled(metrics_enabled);
      feature_flags_.set_reverse_scans(true);
      feature_flags_.set_last_scanned_row_responses(true);
      feature_flags_.set_traffic_director_enabled(false);
      feature_flags_.set_direct_access_requested(false);
    }
  }

  {
    telemetry::SessionTracerConfig tracer_cfg;
    if (options_.has<telemetry::SessionsTracerConfigOption>()) {
      tracer_cfg = options_.get<telemetry::SessionsTracerConfigOption>();
    }
    if (options_.has<::google::cloud::bigtable_internal::
                         SessionsOpenAuthorizedViewPermissionOption>() ||
        table_name_.find("/authorizedViews/") != std::string::npos) {
      if (tracer_cfg.session_type.empty() ||
          tracer_cfg.session_type == "SESSION_TYPE_TABLE" ||
          tracer_cfg.session_type.rfind("SESSION_TYPE_", 0) != 0) {
        tracer_cfg.session_type = "SESSION_TYPE_AUTHORIZED_VIEW";
      }
    } else if (options_
                   .has<::google::cloud::bigtable_internal::
                            SessionsOpenMaterializedViewPermissionOption>() ||
               table_name_.find("/materializedViews/") != std::string::npos) {
      if (tracer_cfg.session_type.empty() ||
          tracer_cfg.session_type == "SESSION_TYPE_TABLE" ||
          tracer_cfg.session_type.rfind("SESSION_TYPE_", 0) != 0) {
        tracer_cfg.session_type = "SESSION_TYPE_MATERIALIZED_VIEW";
      }
    } else if (tracer_cfg.session_type.rfind("SESSION_TYPE_", 0) != 0) {
      tracer_cfg.session_type = "SESSION_TYPE_TABLE";
    }
    char const* perm_str =
        (permission_ ==
             google::bigtable::v2::OpenTableRequest::PERMISSION_WRITE ||
         permission_ ==
             google::bigtable::v2::OpenTableRequest::PERMISSION_READ_WRITE)
            ? "WRITE"
            : "READ";
    if (tracer_cfg.session_name.empty() ||
        tracer_cfg.session_name == "default") {
      tracer_cfg.session_name =
          !table_name_.empty()
              ? absl::StrCat(table_name_, "/", app_profile_id_, "/", perm_str)
              : std::string("default");
    }
    session_tracer_.Configure(std::move(tracer_cfg));
  }

  idle_session_timeout_ = std::chrono::minutes(5);
  min_pool_size_ = 1;

  int num_event_loops = options_.get<
      ::google::cloud::bigtable_internal::SessionsNumEventLoopsOption>();
  if (num_event_loops <= 0) {
    num_event_loops = static_cast<int>(std::thread::hardware_concurrency());
    if (num_event_loops <= 0) num_event_loops = 4;
  }

  int num_shards = 0;
  if (options_
          .has<::google::cloud::bigtable_internal::SessionsNumShardsOption>()) {
    num_shards =
        options_
            .get<::google::cloud::bigtable_internal::SessionsNumShardsOption>();
  }
  if (num_shards <= 0) {
    num_shards = static_cast<int>(std::thread::hardware_concurrency()) * 2;
    if (num_shards < 16) num_shards = 16;
  }

  // 1. Event loops and callback threads. Prefer connection-scoped executors
  // so thread count does not grow with the number of {table, permission}
  // pools; fall back to private executors for standalone pools.
  if (options_.has<SessionsPoolExecutorsOption>() &&
      options_.get<SessionsPoolExecutorsOption>() != nullptr) {
    executors_ = options_.get<SessionsPoolExecutorsOption>();
    owns_executors_ = false;
  } else {
    executors_ =
        PoolExecutors::Create(static_cast<std::size_t>(num_event_loops),
                              static_cast<std::size_t>(num_event_loops));
    owns_executors_ = true;
  }
  user_callback_pool_ = executors_->callback_pool();
  std::size_t const loops = executors_->num_event_loops();

  // 2. Initialize Shards (decoupled from loops)
  shards_.reserve(num_shards);
  for (int i = 0; i < num_shards; ++i) {
    auto shard = std::make_unique<Shard>();
    shard->index = i;
    shard->cq = executors_->cq(static_cast<std::size_t>(i) % loops);
    shard->cq_impl =
        ::google::cloud::internal::GetCompletionQueueImpl(*shard->cq).get();
    shards_.push_back(std::move(shard));
  }
}

SessionPoolImpl::SessionPoolImpl(
    std::vector<google::bigtable::v2::Bigtable::Stub*> stubs,
    std::string_view table_name, std::string_view app_profile_id,
    google::bigtable::v2::OpenTableRequest::Permission permission,
    std::optional<google::cloud::CompletionQueue> background_executor,
    ScheduleFn scheduler, int max_queued_calls,
    std::chrono::nanoseconds idle_session_timeout, size_t min_pool_size)
    : stubs_(stubs),
      table_name_(table_name),
      app_profile_id_(app_profile_id),
      permission_(permission),
      default_cq_(std::move(background_executor)),
      scheduler_(std::move(scheduler)),
      max_queued_calls_(max_queued_calls),
      budget_(50, std::chrono::seconds(2)),
      idle_session_timeout_(idle_session_timeout),
      min_pool_size_(min_pool_size),
      metrics_collector_(std::make_shared<MetricsCollector>(scheduler_)),
      last_connection_error_(google::cloud::Status()),
      active_calls_per_stub_(stubs.size()) {
  shards_.reserve(1);
  auto shard = std::make_unique<Shard>();
  if (default_cq_.has_value()) {
    shard->cq = *default_cq_;
    shard->cq_impl =
        ::google::cloud::internal::GetCompletionQueueImpl(*shard->cq).get();
  }
  shards_.push_back(std::move(shard));

  telemetry::SessionTracerConfig tracer_cfg;
  tracer_cfg.session_type = "SESSION_TYPE_TABLE";
  char const* perm_str =
      (permission_ ==
           google::bigtable::v2::OpenTableRequest::PERMISSION_WRITE ||
       permission_ ==
           google::bigtable::v2::OpenTableRequest::PERMISSION_READ_WRITE)
          ? "WRITE"
          : "READ";
  tracer_cfg.session_name =
      !table_name_.empty()
          ? absl::StrCat(table_name_, "/", app_profile_id_, "/", perm_str)
          : std::string("default");
  session_tracer_.Configure(std::move(tracer_cfg));
}

void SessionPoolImpl::ConfigureNewSession(SessionImpl& session) const {
  session.SetAutoFinalize(false);
  if (keepalive_multiplier_ > 0.0) {
    session.set_keepalive_timeout_multiplier(keepalive_multiplier_);
  }
  if (!serialized_open_payload_.empty()) {
    session.set_pre_serialized_open_payload(serialized_open_payload_);
  }
  session.set_feature_flags(feature_flags_);
}

std::optional<google::cloud::CompletionQueue> SessionPoolImpl::GetGeneralCQ() {
  if (default_cq_.has_value()) return *default_cq_;
  if (executors_ != nullptr) return shards_[0]->cq;
  return std::nullopt;
}

bool SessionPoolImpl::IsTerminalHandshakeError(google::cloud::StatusCode code) {
  return code == google::cloud::StatusCode::kUnimplemented ||
         code == google::cloud::StatusCode::kPermissionDenied ||
         code == google::cloud::StatusCode::kNotFound ||
         code == google::cloud::StatusCode::kInvalidArgument ||
         code == google::cloud::StatusCode::kFailedPrecondition;
}

std::shared_ptr<SessionImpl> SessionPoolImpl::PickAndAcquire(
    std::vector<SessionEntry> const& sessions, bool is_write_request) {
  std::unique_ptr<std::unordered_set<SessionImpl*>> ignored;
  while (true) {
    auto sess = picker_
                    .PickSession(
                        sessions,
                        ignored ? *ignored : std::unordered_set<SessionImpl*>{},
                        is_write_request)
                    .value_or(nullptr);
    if (sess == nullptr) return nullptr;
    if (sess->Acquire()) return sess;
    if (!ignored) {
      ignored = std::make_unique<std::unordered_set<SessionImpl*>>();
    }
    ignored->insert(sess.get());
  }
}

google::cloud::Status SessionPoolImpl::StartVirtualRpc(
    grpc::ClientContext* context,
    google::bigtable::v2::VirtualRpcRequest request, GenericCallback callback,
    bool is_write_request) {
  if (metrics_collector_) {
    metrics_collector_->OnRequestStart();
  }

  auto reject = [&](google::cloud::Status status, GenericCallback cb) {
    MarkPoolRejected(status);
    if (metrics_collector_) {
      metrics_collector_->OnRequestComplete(std::chrono::nanoseconds(0),
                                            status);
      metrics_collector_->SetPendingQueueSize(total_queued_calls_.load());
      metrics_collector_->SetActiveCalls(total_active_calls_.load());
    }
    if (cb) {
      std::move(cb)(status);
    }
    return status;
  };

  if (is_closed_.load()) {
    return reject(google::cloud::internal::UnavailableError(
                      "SessionPoolImpl is fully closed"),
                  std::move(callback));
  }

  if (context != nullptr && ConsumeCancelledContext(context)) {
    return reject(google::cloud::internal::CancelledError(
                      "Client context cancelled by user"),
                  std::move(callback));
  }

  Shard& shard = GetShardForCurrentThread();

  std::function<void()> cancel_timer;
  if (context != nullptr) {
    auto deadline = context->deadline();
    auto now = std::chrono::system_clock::now();
    if (deadline < std::chrono::system_clock::time_point::max()) {
      auto delay =
          std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now);
      if (delay <= std::chrono::nanoseconds(0)) {
        return reject(google::cloud::internal::DeadlineExceededError(
                          "Client context deadline exceeded prior to dispatch"),
                      std::move(callback));
      }
      if (shard.cq.has_value()) {
        std::weak_ptr<SessionPoolImpl> weak_self = shared_from_this();
        google::cloud::future<void> timer_fut =
            shard.cq->MakeRelativeTimer(delay).then(
                [weak_self, context, rpc_id = request.rpc_id()](
                    google::cloud::future<google::cloud::StatusOr<
                        std::chrono::system_clock::time_point>>
                        f) {
                  auto status = f.get().status();

                  if (status.ok()) {
                    if (auto self = weak_self.lock()) {
                      self->CancelByContext(
                          context,
                          google::cloud::internal::DeadlineExceededError(
                              "Deadline exceeded"));
                    }
                  }
                });
        auto timer_fut_ptr =
            std::make_shared<google::cloud::future<void>>(std::move(timer_fut));
        cancel_timer = [timer_fut_ptr]() {
          if (timer_fut_ptr->valid()) timer_fut_ptr->cancel();
        };
      }
    }
  }

  // 1. Lock-free hot path over the shard's immutable group snapshot.
  std::shared_ptr<SessionImpl> selected_session;
  std::shared_ptr<ConnectionGroup> target_group;
  Shard* selected_shard = &shard;

  {
    auto snapshot = std::atomic_load(&shard.groups_snapshot);
    if (snapshot && !snapshot->empty()) {
      target_group = GetLeastLoadedGroup(snapshot);
      if (target_group != nullptr) {
        auto sessions_ptr = std::atomic_load(&target_group->sessions_ptr);
        if (sessions_ptr && !sessions_ptr->empty()) {
          selected_session = PickAndAcquire(*sessions_ptr, is_write_request);
        }
      }
    }

    // Try Lock-Free Stealing from other shards if local group is busy/empty
    if (selected_session == nullptr &&
        (total_active_calls_.load(std::memory_order_relaxed) -
         total_queued_calls_.load(std::memory_order_relaxed)) <
            total_ready_sessions_.load(std::memory_order_relaxed)) {
      std::string target_peer_id =
          target_group != nullptr ? target_group->peer_id : "";
      for (int pass = 0; pass < 2 && selected_session == nullptr; ++pass) {
        if (pass == 1 && target_peer_id.empty()) break;
        for (auto& other_shard : shards_) {
          if (other_shard.get() == &shard) continue;

          auto other_snapshot = std::atomic_load(&other_shard->groups_snapshot);
          if (!other_snapshot) continue;

          for (auto const& other_group : *other_snapshot) {
            if (!other_group || other_group->is_closed) continue;
            if (pass == 0 && !target_peer_id.empty() &&
                other_group->peer_id != target_peer_id) {
              continue;
            }
            if (pass == 1 && other_group->peer_id == target_peer_id) {
              continue;
            }
            auto other_sessions = std::atomic_load(&other_group->sessions_ptr);
            if (!other_sessions || other_sessions->empty()) continue;

            if (auto sess = PickAndAcquire(*other_sessions, is_write_request)) {
              selected_session = std::move(sess);
              target_group = other_group;
              selected_shard = other_shard.get();
              break;
            }
          }
          if (selected_session != nullptr) break;
        }
      }
    }
  }

  if (selected_session != nullptr) {
    Shard& owning_shard = *selected_shard;
    if (auto* h = static_cast<pool::SessionList::SessionHandle*>(
            selected_session->pool_handle().get())) {
      pool::SessionList::MarkHandleInFlightFast(h);
    }
    if (target_group) {
      target_group->active_calls_count.fetch_add(1, std::memory_order_relaxed);
    }
    owning_shard.total_active_calls.fetch_add(1, std::memory_order_relaxed);
    total_active_calls_.fetch_add(1, std::memory_order_relaxed);

    IncrementStubActiveCalls(*selected_session, 1);

    if (metrics_collector_) {
      metrics_collector_->SetPendingQueueSize(total_queued_calls_.load());
      metrics_collector_->SetActiveCalls(total_active_calls_.load());
    }

    if (request.rpc_id() <= selected_session->last_dispatched_rpc_id()) {
      request.set_rpc_id(AllocateRpcId());
    }

    auto wrapped_cb =
        [weak_pool = weak_from_this(), shard_idx = owning_shard.index,
         weak_session = std::weak_ptr<SessionImpl>(selected_session),
         weak_group = std::weak_ptr<ConnectionGroup>(target_group),
         cancel_timer = std::move(cancel_timer), callback = std::move(callback),
         start_time = SessionImpl::Clock::now(), rpc_id = request.rpc_id(),
         is_write_request](
            google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
                resp) mutable {
          if (cancel_timer) {
            cancel_timer();
          }
          auto self = weak_pool.lock();
          if (!self) {
            std::move(callback)(std::move(resp));
            return;
          }
          Shard* shard_ptr = self->shards_[shard_idx].get();
          auto group_ptr = weak_group.lock();
          auto session = weak_session.lock();

          std::chrono::nanoseconds latency =
              SessionImpl::Clock::now() - start_time;
          if (resp.ok()) {
            if (is_write_request) {
              self->write_latency_tracker_.OnRequestComplete(latency);
            } else {
              self->read_latency_tracker_.OnRequestComplete(latency);
            }
          }

          CompletedAttemptSessionInfo session_info;
          if (session != nullptr) {
            session_info.peer_info = session->peer_info();
            session_info.sent_at = session->active_rpc_sent_at();
          }
          if (!session_info.sent_at.has_value()) {
            session_info.sent_at = start_time;
          }
          session_info.cluster_info =
              (resp.ok() && resp->has_cluster_info())
                  ? std::make_optional(resp->cluster_info())
                  : (session != nullptr ? session->last_cluster_info()
                                        : std::nullopt);

          double const client_blocking_ms =
              session_info.sent_at.has_value()
                  ? std::max(0.0, std::chrono::duration<double, std::milli>(
                                      *session_info.sent_at - start_time)
                                      .count())
                  : 0.0;
          RecordTransportLatencyIfPositive(
              self->session_tracer_, group_ptr.get(), resp, latency,
              client_blocking_ms, session_info.peer_info, is_write_request);

          if (self->metrics_collector_) {
            self->metrics_collector_->OnRequestComplete(latency, resp.status());
            self->metrics_collector_->SetPendingQueueSize(
                self->total_queued_calls_.load());
            self->metrics_collector_->SetActiveCalls(
                self->total_active_calls_.load());
          }

          shard_ptr->fast_path_count.fetch_add(1, std::memory_order_relaxed);

          // Fast path: release the session slot before invoking the user
          // callback so a synchronous caller waking up from Notify() can
          // immediately re-acquire this session without falling back to the
          // pending queue.
          self->FinalizeSessionCall(session, group_ptr, shard_ptr, rpc_id);
          ScopedCompletedAttemptSessionInfo scope(&session_info);
          std::move(callback)(std::move(resp));
        };

    selected_session->StartVirtualRpc(context, std::move(request),
                                      std::move(wrapped_cb));
    if (context != nullptr && ConsumeCancelledContext(context)) {
      selected_session->CancelRpcByContext(context);
    }
    return google::cloud::Status();
  }

  // 2. Fallback Path (Locked)
  bool use_shard_queue = false;
  auto snapshot = std::atomic_load(&shard.groups_snapshot);
  if (!snapshot || snapshot->empty()) {
    use_shard_queue = true;
  } else {
    target_group = GetLeastLoadedGroup(snapshot);
    if (target_group == nullptr) {
      use_shard_queue = true;
    }
  }

  if (use_shard_queue) {
    google::cloud::Status terminal_err;
    bool wake_half_open_probe = false;
    {
      std::lock_guard<std::mutex> l_err(error_mutex_);
      bool const is_terminal =
          IsTerminalHandshakeError(last_connection_error_.code());
      bool const circuit_open =
          consecutive_session_failures_.load(std::memory_order_acquire) >=
          consecutive_session_failure_threshold_.load(
              std::memory_order_relaxed);
      if (total_ready_sessions_.load() == 0 && (is_terminal || circuit_open)) {
        terminal_err = last_connection_error_.ok()
                           ? google::cloud::internal::UnavailableError(
                                 "Session pool consecutive handshake failure "
                                 "threshold exceeded")
                           : last_connection_error_;
        wake_half_open_probe =
            !is_terminal &&
            total_connecting_sessions_.load(std::memory_order_relaxed) == 0 &&
            !replenish_timer_scheduled_.load(std::memory_order_acquire);
      }
    }
    if (!terminal_err.ok()) {
      if (wake_half_open_probe) {
        MaybeReplenishPool(shard, /*force_half_open_probe=*/true);
      }
      if (cancel_timer) cancel_timer();
      return reject(terminal_err, std::move(callback));
    }

    bool is_closed_local = false;
    bool queue_full = false;
    {
      std::lock_guard<std::mutex> l_shard(shard.mutex);
      if (shard.is_closed) {
        is_closed_local = true;
      } else {
        for (auto const& [id, group] : shard.connection_groups) {
          if (group && !group->is_closed.load(std::memory_order_acquire)) {
            target_group = group;
            use_shard_queue = false;
            break;
          }
        }
        if (use_shard_queue) {
          if (total_queued_calls_.load() >= max_queued_calls_) {
            queue_full = true;
          } else {
            shard.pending_queue.push_back(
                PendingCall{context, std::move(request), std::move(callback),
                            std::move(cancel_timer), SessionImpl::Clock::now(),
                            is_write_request});
            shard.total_queued_calls++;
            total_queued_calls_++;
            total_active_calls_++;
            if (metrics_collector_) {
              metrics_collector_->SetPendingQueueSize(
                  total_queued_calls_.load());
              metrics_collector_->SetActiveCalls(total_active_calls_.load());
            }
          }
        }
      }
    }
    if (is_closed_local) {
      if (cancel_timer) cancel_timer();
      return reject(google::cloud::internal::UnavailableError(
                        "SessionPoolImpl Shard is closed"),
                    std::move(callback));
    }
    if (queue_full) {
      if (cancel_timer) cancel_timer();
      return reject(google::cloud::internal::ResourceExhaustedError(
                        "Session pool queuing capacity fully exhausted"),
                    std::move(callback));
    }

    if (use_shard_queue) {
      MaybeReplenishPool(shard);

      if (total_ready_sessions_.load(std::memory_order_acquire) > 0) {
        DrainQueue(shard, nullptr);
        if (shards_.size() > 1 &&
            shard.total_queued_calls.load(std::memory_order_relaxed) > 0) {
          for (auto& other_shard : shards_) {
            if (other_shard.get() == &shard) continue;
            std::shared_ptr<ConnectionGroup> ready_group;
            {
              std::lock_guard<std::mutex> l_other(other_shard->mutex);
              if (!other_shard->is_closed) {
                for (auto const& [id, g] : other_shard->connection_groups) {
                  if (g && !g->is_closed.load(std::memory_order_acquire) &&
                      !g->sessions.empty()) {
                    ready_group = g;
                    break;
                  }
                }
              }
            }
            if (ready_group != nullptr) {
              std::deque<PendingCall> stolen_calls;
              {
                std::lock_guard<std::mutex> l_shard(shard.mutex);
                if (shard.connection_groups.empty() &&
                    !shard.pending_queue.empty()) {
                  stolen_calls = std::move(shard.pending_queue);
                  shard.pending_queue.clear();
                  shard.total_queued_calls -=
                      static_cast<int>(stolen_calls.size());
                }
              }
              if (!stolen_calls.empty()) {
                bool moved_to_other = false;
                {
                  std::lock_guard<std::mutex> l_other(other_shard->mutex);
                  if (!other_shard->is_closed &&
                      !ready_group->is_closed.load(std::memory_order_acquire) &&
                      !ready_group->sessions.empty()) {
                    other_shard->total_queued_calls +=
                        static_cast<int>(stolen_calls.size());
                    for (auto& item : stolen_calls) {
                      ready_group->pending_queue.push_back(std::move(item));
                      ready_group->pending_calls_count++;
                    }
                    moved_to_other = true;
                  }
                }
                if (moved_to_other) {
                  DrainQueue(*other_shard, ready_group);
                  break;
                }
                {
                  std::lock_guard<std::mutex> l_shard(shard.mutex);
                  shard.total_queued_calls +=
                      static_cast<int>(stolen_calls.size());
                  for (auto it = stolen_calls.rbegin();
                       it != stolen_calls.rend(); ++it) {
                    shard.pending_queue.push_front(std::move(*it));
                  }
                }
              }
            }
          }
        }
      }

      // Guard against the TOCTOU race where the last connecting session failed
      // right as this call was enqueued and MaybeReplenishPool could not spawn
      // a replacement (e.g., terminal handshake error or no replenish timer).
      if (total_ready_sessions_.load() == 0) {
        google::cloud::Status fail_status;
        bool is_terminal = false;
        bool const circuit_open =
            consecutive_session_failures_.load(std::memory_order_acquire) >=
            consecutive_session_failure_threshold_.load(
                std::memory_order_relaxed);
        {
          std::lock_guard<std::mutex> l_err(error_mutex_);
          fail_status = last_connection_error_;
          is_terminal =
              IsTerminalHandshakeError(fail_status.code()) || circuit_open;
        }
        if (is_terminal ||
            (total_connecting_sessions_.load() == 0 &&
             !replenish_timer_scheduled_.load(std::memory_order_acquire))) {
          if (fail_status.ok()) {
            fail_status = google::cloud::internal::UnavailableError(
                "No active or connecting sessions available in pool");
          }
          std::deque<PendingCall> aborted_shard_queue;
          {
            std::lock_guard<std::mutex> l_shard(shard.mutex);
            if ((is_terminal || (shard.connecting_sessions.empty() &&
                                 !replenish_timer_scheduled_.load(
                                     std::memory_order_acquire))) &&
                shard.connection_groups.empty() &&
                !shard.pending_queue.empty()) {
              aborted_shard_queue = std::move(shard.pending_queue);
              shard.pending_queue.clear();
              int shard_pending_count =
                  static_cast<int>(aborted_shard_queue.size());
              shard.total_queued_calls -= shard_pending_count;
              total_queued_calls_ -= shard_pending_count;
              total_active_calls_ -= shard_pending_count;
            }
          }
          if (!aborted_shard_queue.empty()) {
            FailPendingCalls(aborted_shard_queue, fail_status);
          }
        }
      }
      return google::cloud::Status();
    }
  }

  bool need_replenish = false;
  bool target_group_closed = false;
  bool target_group_queue_full = false;

  {
    std::lock_guard<std::mutex> l_shard(shard.mutex);
    if (shard.is_closed) {
      target_group_closed = true;
    } else {
      if (target_group == nullptr ||
          target_group->is_closed.load(std::memory_order_acquire)) {
        target_group = nullptr;
        for (auto const& [id, group] : shard.connection_groups) {
          if (group && !group->is_closed.load(std::memory_order_acquire)) {
            target_group = group;
            break;
          }
        }
      }
      if (target_group != nullptr) {
        selected_session =
            PickAndAcquire(target_group->sessions, is_write_request);
      }
    }
  }

  if (selected_session == nullptr && !target_group_closed &&
      (total_active_calls_.load(std::memory_order_relaxed) -
       total_queued_calls_.load(std::memory_order_relaxed)) <
          total_ready_sessions_.load(std::memory_order_relaxed)) {
    std::string target_peer_id =
        target_group != nullptr ? target_group->peer_id : "";
    for (int pass = 0; pass < 2 && selected_session == nullptr; ++pass) {
      for (auto& other_shard : shards_) {
        if (other_shard.get() == &shard) continue;

        std::lock_guard<std::mutex> l_other(other_shard->mutex);
        if (other_shard->is_closed) continue;

        for (auto const& [peer_id, other_group] :
             other_shard->connection_groups) {
          if (!other_group || other_group->is_closed) continue;
          if (!target_peer_id.empty()) {
            if (pass == 0 && peer_id != target_peer_id) continue;
            if (pass == 1 && peer_id == target_peer_id) continue;
          }

          if (auto sess =
                  PickAndAcquire(other_group->sessions, is_write_request)) {
            selected_session = std::move(sess);
            target_group = other_group;
            selected_shard = other_shard.get();
            break;
          }
        }
        if (selected_session != nullptr) break;
      }
    }
  }

  if (selected_session == nullptr && !target_group_closed) {
    if (total_queued_calls_.load() >= max_queued_calls_) {
      target_group_queue_full = true;
    } else {
      std::lock_guard<std::mutex> l_shard(shard.mutex);
      if (shard.is_closed) {
        target_group_closed = true;
      } else {
        if (target_group == nullptr ||
            target_group->is_closed.load(std::memory_order_acquire)) {
          target_group = nullptr;
          for (auto const& [id, group] : shard.connection_groups) {
            if (group && !group->is_closed.load(std::memory_order_acquire)) {
              target_group = group;
              break;
            }
          }
        }
        if (target_group != nullptr) {
          target_group->pending_queue.push_back(
              PendingCall{context, std::move(request), std::move(callback),
                          std::move(cancel_timer), SessionImpl::Clock::now(),
                          is_write_request});
          shard.total_queued_calls++;
          total_queued_calls_++;
          target_group->pending_calls_count.fetch_add(
              1, std::memory_order_seq_cst);
          total_active_calls_++;
          need_replenish = true;
        } else {
          shard.pending_queue.push_back(
              PendingCall{context, std::move(request), std::move(callback),
                          std::move(cancel_timer), SessionImpl::Clock::now(),
                          is_write_request});
          shard.total_queued_calls++;
          total_queued_calls_++;
          total_active_calls_++;
          need_replenish = true;
        }
      }
    }
  }

  if (target_group_closed) {
    if (cancel_timer) cancel_timer();
    return reject(
        google::cloud::internal::UnavailableError("Connection group is closed"),
        std::move(callback));
  }
  if (target_group_queue_full) {
    if (cancel_timer) cancel_timer();
    return reject(google::cloud::internal::ResourceExhaustedError(
                      "Session pool queuing capacity fully exhausted"),
                  std::move(callback));
  }

  if (metrics_collector_) {
    metrics_collector_->SetPendingQueueSize(total_queued_calls_.load());
    metrics_collector_->SetActiveCalls(total_active_calls_.load());
  }

  if (need_replenish) {
    // Re-check and drain after enqueueing in case the last active session
    // finished and checked pending_calls_count right before we acquired
    // shard.mutex.
    DrainQueue(shard, target_group);
    MaybeReplenishPool(shard);
    if (target_group == nullptr &&
        total_ready_sessions_.load(std::memory_order_acquire) > 0 &&
        shards_.size() > 1 &&
        shard.total_queued_calls.load(std::memory_order_relaxed) > 0) {
      for (auto& other_shard : shards_) {
        if (other_shard.get() == &shard) continue;
        std::shared_ptr<ConnectionGroup> ready_group;
        {
          std::lock_guard<std::mutex> l_other(other_shard->mutex);
          if (!other_shard->is_closed) {
            for (auto const& [id, g] : other_shard->connection_groups) {
              if (g && !g->is_closed.load(std::memory_order_acquire) &&
                  !g->sessions.empty()) {
                ready_group = g;
                break;
              }
            }
          }
        }
        if (ready_group != nullptr) {
          std::deque<PendingCall> stolen_calls;
          {
            std::lock_guard<std::mutex> l_shard(shard.mutex);
            if (shard.connection_groups.empty() &&
                !shard.pending_queue.empty()) {
              stolen_calls = std::move(shard.pending_queue);
              shard.pending_queue.clear();
              shard.total_queued_calls -= static_cast<int>(stolen_calls.size());
            }
          }
          if (!stolen_calls.empty()) {
            bool moved_to_other = false;
            {
              std::lock_guard<std::mutex> l_other(other_shard->mutex);
              if (!other_shard->is_closed &&
                  !ready_group->is_closed.load(std::memory_order_acquire) &&
                  !ready_group->sessions.empty()) {
                other_shard->total_queued_calls +=
                    static_cast<int>(stolen_calls.size());
                for (auto& item : stolen_calls) {
                  ready_group->pending_queue.push_back(std::move(item));
                  ready_group->pending_calls_count++;
                }
                moved_to_other = true;
              }
            }
            if (moved_to_other) {
              DrainQueue(*other_shard, ready_group);
              break;
            }
            {
              std::lock_guard<std::mutex> l_shard(shard.mutex);
              shard.total_queued_calls += static_cast<int>(stolen_calls.size());
              for (auto it = stolen_calls.rbegin(); it != stolen_calls.rend();
                   ++it) {
                shard.pending_queue.push_front(std::move(*it));
              }
            }
          }
        }
      }
    }
  }

  if (selected_session != nullptr) {
    Shard& owning_shard = *selected_shard;
    if (auto* h = static_cast<pool::SessionList::SessionHandle*>(
            selected_session->pool_handle().get())) {
      pool::SessionList::MarkHandleInFlightFast(h);
    }
    if (target_group) {
      target_group->active_calls_count.fetch_add(1, std::memory_order_relaxed);
    }
    owning_shard.total_active_calls.fetch_add(1, std::memory_order_relaxed);
    total_active_calls_.fetch_add(1, std::memory_order_relaxed);

    IncrementStubActiveCalls(*selected_session, 1);

    if (request.rpc_id() <= selected_session->last_dispatched_rpc_id()) {
      request.set_rpc_id(AllocateRpcId());
    }

    auto wrapped_cb =
        [weak_pool = weak_from_this(), shard_idx = owning_shard.index,
         weak_session = std::weak_ptr<SessionImpl>(selected_session),
         weak_group = std::weak_ptr<ConnectionGroup>(target_group),
         cancel_timer = std::move(cancel_timer), callback = std::move(callback),
         start_time = SessionImpl::Clock::now(), rpc_id = request.rpc_id(),
         is_write_request](
            google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
                resp) mutable {
          if (cancel_timer) {
            cancel_timer();
          }
          auto self = weak_pool.lock();
          if (!self) {
            std::move(callback)(std::move(resp));
            return;
          }
          Shard* shard_ptr = self->shards_[shard_idx].get();
          auto group_ptr = weak_group.lock();
          auto session = weak_session.lock();

          std::chrono::nanoseconds latency =
              SessionImpl::Clock::now() - start_time;
          if (resp.ok()) {
            if (is_write_request) {
              self->write_latency_tracker_.OnRequestComplete(latency);
            } else {
              self->read_latency_tracker_.OnRequestComplete(latency);
            }
          }

          CompletedAttemptSessionInfo session_info;
          if (session != nullptr) {
            session_info.peer_info = session->peer_info();
            session_info.sent_at = session->active_rpc_sent_at();
          }
          if (!session_info.sent_at.has_value()) {
            session_info.sent_at = start_time;
          }
          session_info.cluster_info =
              (resp.ok() && resp->has_cluster_info())
                  ? std::make_optional(resp->cluster_info())
                  : (session != nullptr ? session->last_cluster_info()
                                        : std::nullopt);

          double const client_blocking_ms =
              session_info.sent_at.has_value()
                  ? std::max(0.0, std::chrono::duration<double, std::milli>(
                                      *session_info.sent_at - start_time)
                                      .count())
                  : 0.0;
          RecordTransportLatencyIfPositive(
              self->session_tracer_, group_ptr.get(), resp, latency,
              client_blocking_ms, session_info.peer_info, is_write_request);

          if (self->metrics_collector_) {
            self->metrics_collector_->OnRequestComplete(latency, resp.status());
            self->metrics_collector_->SetPendingQueueSize(
                self->total_queued_calls_.load());
            self->metrics_collector_->SetActiveCalls(
                self->total_active_calls_.load());
          }

          shard_ptr->fallback_path_count.fetch_add(1,
                                                   std::memory_order_relaxed);
          self->FinalizeSessionCall(session, group_ptr, shard_ptr, rpc_id);
          if (self->user_callback_pool_ == nullptr) {
            ScopedCompletedAttemptSessionInfo scope(&session_info);
            std::move(callback)(std::move(resp));
          } else {
            // Executors may be shared across pools (and outlive this one), so
            // never capture a raw `this` into a queued task.
            self->user_callback_pool_->Submit(
                [cb = std::move(callback), resp = std::move(resp),
                 session_info = std::move(session_info)]() mutable {
                  ScopedCompletedAttemptSessionInfo scope(&session_info);
                  std::move(cb)(std::move(resp));
                },
                shard_ptr->index);
          }
        };

    selected_session->StartVirtualRpc(context, std::move(request),
                                      std::move(wrapped_cb));
    if (context != nullptr && ConsumeCancelledContext(context)) {
      selected_session->CancelRpcByContext(context);
    }
  }

  if (selected_session == nullptr && context != nullptr &&
      ConsumeCancelledContext(context)) {
    CancelByContext(context);
  }

  return google::cloud::Status();
}  // NOLINT(readability/fn_size)

size_t SessionPoolImpl::active_sessions_count() const {
  size_t count = 0;
  for (auto const& shard : shards_) {
    std::lock_guard<std::mutex> l(shard->mutex);
    for (auto const& [id, group] : shard->connection_groups) {
      count += group->sessions.size();
    }
  }
  return count;
}

size_t SessionPoolImpl::pending_queue_size() const {
  size_t count = 0;
  for (auto const& shard : shards_) {
    std::lock_guard<std::mutex> l(shard->mutex);
    count += shard->pending_queue.size();
    for (auto const& [id, group] : shard->connection_groups) {
      count += group->pending_queue.size();
    }
  }
  return count;
}

size_t SessionPoolImpl::active_calls_count() const {
  return total_active_calls_.load();
}

void SessionPoolImpl::TryDrainGroupPendingQueueLocked(
    Shard& shard, ConnectionGroup* group,
    std::vector<CallToStart>& calls_to_start) {
  if (group == nullptr || group->is_closed.load(std::memory_order_acquire) ||
      group->sessions.empty()) {
    return;
  }
  while (!shard.pending_queue.empty()) {
    auto& item = shard.pending_queue.front();
    group->pending_queue.push_back(std::move(item));
    group->pending_calls_count++;
    shard.pending_queue.pop_front();
  }
  while (!group->pending_queue.empty()) {
    auto& item = group->pending_queue.front();
    bool is_write_request = item.is_write_request;

    std::shared_ptr<SessionImpl> selected_session;
    std::vector<std::shared_ptr<SessionImpl>> skipped_idle;
    while (true) {
      auto sess = group->idle_queue.Pop();
      if (!sess) break;
      if (sess->state() == SessionImpl::State::kReady &&
          (!is_write_request || !sess->IsWriteCongested())) {
        if (sess->Acquire()) {
          selected_session = std::move(sess);
          break;
        }
      }
      if (sess->state() == SessionImpl::State::kReady &&
          sess->active_rpc_count() == 0) {
        skipped_idle.push_back(std::move(sess));
      }
    }
    for (auto& s : skipped_idle) {
      group->idle_queue.Push(std::move(s));
    }
    skipped_idle.clear();

    if (selected_session == nullptr) {
      selected_session = PickAndAcquire(group->sessions, is_write_request);
    }

    if (selected_session == nullptr) {
      break;
    }

    auto it_h = shard.session_handles.find(selected_session.get());
    if (it_h != shard.session_handles.end()) {
      shard.session_list.MarkHandleInFlight(it_h->second);
    }

    shard.total_queued_calls--;
    total_queued_calls_--;

    calls_to_start.push_back({selected_session, item.context,
                              std::move(item.request), std::move(item.callback),
                              std::move(item.cancel_timer), item.enqueue_time,
                              is_write_request});
    group->pending_queue.pop_front();
    group->pending_calls_count--;
  }
}

void SessionPoolImpl::DrainQueue(Shard& shard,
                                 std::shared_ptr<ConnectionGroup> group) {
  struct PendingDrainEntry {
    std::weak_ptr<SessionPoolImpl> pool;
    Shard* shard;
    std::shared_ptr<ConnectionGroup> group;
  };
  thread_local bool in_drain = false;
  thread_local std::vector<PendingDrainEntry> pending_drains;

  pending_drains.push_back({weak_from_this(), &shard, std::move(group)});
  if (in_drain) {
    return;
  }
  in_drain = true;

  while (!pending_drains.empty()) {
    auto entry = std::move(pending_drains.back());
    pending_drains.pop_back();
    auto owner = entry.pool.lock();
    if (!owner) continue;
    Shard* s_ptr = entry.shard;
    auto g_ptr = std::move(entry.group);

    std::vector<CallToStart> calls_to_start;
    {
      std::lock_guard<std::mutex> l_shard(s_ptr->mutex);
      if (g_ptr && (g_ptr->is_closed.load(std::memory_order_acquire) ||
                    g_ptr->sessions.empty())) {
        g_ptr = nullptr;
      }
      if (!g_ptr) {
        for (auto const& [id, candidate] : s_ptr->connection_groups) {
          if (candidate &&
              !candidate->is_closed.load(std::memory_order_acquire) &&
              !candidate->sessions.empty()) {
            g_ptr = candidate;
            break;
          }
        }
      }
      if (g_ptr) {
        owner->TryDrainGroupPendingQueueLocked(*s_ptr, g_ptr.get(),
                                               calls_to_start);
      }
    }

    for (auto& call : calls_to_start) {
      if (g_ptr) {
        g_ptr->active_calls_count.fetch_add(1, std::memory_order_relaxed);
      }
      s_ptr->total_active_calls.fetch_add(1, std::memory_order_relaxed);
      if (call.session) {
        owner->IncrementStubActiveCalls(*call.session, 1);
        if (call.request.rpc_id() <= call.session->last_dispatched_rpc_id()) {
          call.request.set_rpc_id(owner->AllocateRpcId());
        }
      }

      auto wrapped_cb =
          [weak_pool = entry.pool, shard_idx = s_ptr->index,
           weak_session = std::weak_ptr<SessionImpl>(call.session),
           weak_group = std::weak_ptr<ConnectionGroup>(g_ptr),
           cancel_timer = std::move(call.cancel_timer),
           cb = std::move(call.callback), enqueue_time = call.enqueue_time,
           start_time = SessionImpl::Clock::now(),
           rpc_id = call.request.rpc_id(),
           is_write_request = call.is_write_request](
              google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
                  resp) mutable {
            if (cancel_timer) {
              cancel_timer();
            }
            auto self = weak_pool.lock();
            if (!self) {
              std::move(cb)(std::move(resp));
              return;
            }
            Shard* shard_ptr = self->shards_[shard_idx].get();
            auto group_ptr = weak_group.lock();
            auto session = weak_session.lock();

            std::chrono::nanoseconds latency =
                SessionImpl::Clock::now() - start_time;
            if (resp.ok()) {
              if (is_write_request) {
                self->write_latency_tracker_.OnRequestComplete(latency);
              } else {
                self->read_latency_tracker_.OnRequestComplete(latency);
              }
            }

            CompletedAttemptSessionInfo session_info;
            if (session != nullptr) {
              session_info.peer_info = session->peer_info();
              session_info.sent_at = session->active_rpc_sent_at();
            }
            if (!session_info.sent_at.has_value()) {
              session_info.sent_at = start_time;
            }
            session_info.cluster_info =
                (resp.ok() && resp->has_cluster_info())
                    ? std::make_optional(resp->cluster_info())
                    : (session != nullptr ? session->last_cluster_info()
                                          : std::nullopt);

            std::chrono::nanoseconds const queue_latency =
                start_time - enqueue_time;
            std::chrono::nanoseconds const e2e_latency =
                latency + queue_latency;
            std::chrono::nanoseconds const blocking_duration =
                session_info.sent_at.has_value()
                    ? std::max(queue_latency,
                               *session_info.sent_at - enqueue_time)
                    : queue_latency;
            double const client_blocking_ms = std::max(
                0.0,
                std::chrono::duration<double, std::milli>(blocking_duration)
                    .count());
            RecordTransportLatencyIfPositive(
                self->session_tracer_, group_ptr.get(), resp, e2e_latency,
                client_blocking_ms, session_info.peer_info, is_write_request);

            if (self->metrics_collector_) {
              self->metrics_collector_->OnRequestComplete(
                  queue_latency, latency, resp.status());
              self->metrics_collector_->SetPendingQueueSize(
                  self->total_queued_calls_.load());
              self->metrics_collector_->SetActiveCalls(
                  self->total_active_calls_.load());
            }

            shard_ptr->drain_path_count.fetch_add(1, std::memory_order_relaxed);
            self->FinalizeSessionCall(session, group_ptr, shard_ptr, rpc_id);
            if (self->user_callback_pool_ == nullptr) {
              ScopedCompletedAttemptSessionInfo scope(&session_info);
              std::move(cb)(std::move(resp));
            } else {
              self->user_callback_pool_->Submit(
                  [c = std::move(cb), r = std::move(resp),
                   session_info = std::move(session_info)]() mutable {
                    ScopedCompletedAttemptSessionInfo scope(&session_info);
                    std::move(c)(std::move(r));
                  },
                  shard_ptr->index);
            }
          };

      call.session->StartVirtualRpc(call.context, std::move(call.request),
                                    std::move(wrapped_cb));
    }
    if (owner->metrics_collector_) {
      owner->metrics_collector_->SetPendingQueueSize(
          owner->total_queued_calls_.load());
    }
  }

  in_drain = false;
}

void SessionPoolImpl::IncrementStubActiveCalls(SessionImpl const& session,
                                               int delta) {
  int idx = session.stub_index();
  if (idx >= 0 && static_cast<size_t>(idx) < active_calls_per_stub_.size()) {
    active_calls_per_stub_[idx].fetch_add(delta, std::memory_order_relaxed);
    return;
  }
  auto stub_ptr = session.stub();
  if (stub_ptr == nullptr) return;
  auto it = std::find(stubs_.begin(), stubs_.end(), stub_ptr);
  if (it != stubs_.end()) {
    size_t found_idx = std::distance(stubs_.begin(), it);
    if (found_idx < active_calls_per_stub_.size()) {
      active_calls_per_stub_[found_idx].fetch_add(delta,
                                                  std::memory_order_relaxed);
    }
  }
}

void SessionPoolImpl::FinalizeSessionCall(
    std::shared_ptr<SessionImpl> session,
    std::shared_ptr<ConnectionGroup> group_ptr, Shard* shard_ptr,
    int64_t rpc_id) {
  // If the caller cancelled this vRPC after its write completed on the wire,
  // the session's single-flight slot remains occupied until the server's
  // response frame arrives and invokes OnSlotDrained(). Keep active-call
  // counters elevated until OnSlotDrained() so PoolSizer and
  // GetLeastLoadedGroup accurately reflect the occupied slot.
  if (session != nullptr && session->IsCancelledAwaitingDrain(rpc_id)) {
    return;
  }

  if (session) {
    session->FinalizeActiveRpc(rpc_id);
    IncrementStubActiveCalls(*session, -1);
  }

  if (group_ptr) {
    group_ptr->active_calls_count.fetch_sub(1, std::memory_order_relaxed);
  }
  shard_ptr->total_active_calls.fetch_sub(1, std::memory_order_relaxed);
  if (total_active_calls_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    std::lock_guard<std::mutex> l_pw(pre_warm_mutex_);
    active_calls_condvar_.notify_all();
  }

  if (group_ptr) {
    bool eligible_by_session_list = true;
    if (session) {
      if (auto* h = static_cast<pool::SessionList::SessionHandle*>(
              session->pool_handle().get())) {
        eligible_by_session_list = pool::SessionList::ReleaseToPoolFast(h);
      } else {
        std::lock_guard<std::mutex> l_shard(shard_ptr->mutex);
        auto it_h = shard_ptr->session_handles.find(session.get());
        if (it_h != shard_ptr->session_handles.end()) {
          eligible_by_session_list =
              shard_ptr->session_list.ReleaseToPool(it_h->second);
        }
      }
    }
    if (eligible_by_session_list && session &&
        !group_ptr->is_closed.load(std::memory_order_acquire) &&
        session->state() == SessionImpl::State::kReady &&
        !session->IsWriteCongested() && session->active_rpc_count() == 0 &&
        !session->in_idle_queue()) {
      group_ptr->idle_queue.Push(session);
    }
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (group_ptr->pending_calls_count.load(std::memory_order_seq_cst) > 0 ||
        shard_ptr->total_queued_calls.load(std::memory_order_relaxed) > 0) {
      DrainQueue(*shard_ptr, group_ptr);
    }
    if (total_queued_calls_.load(std::memory_order_relaxed) > 0 &&
        shards_.size() > 1 && session && session->active_rpc_count() == 0 &&
        session->state() == SessionImpl::State::kReady &&
        !group_ptr->is_closed.load(std::memory_order_acquire)) {
      std::optional<PendingCall> stolen_call;
      for (auto& other_shard : shards_) {
        if (other_shard.get() == shard_ptr) continue;
        if (other_shard->total_queued_calls.load(std::memory_order_relaxed) <=
            0) {
          continue;
        }
        std::lock_guard<std::mutex> l_other(other_shard->mutex);
        if (other_shard->is_closed) continue;
        if (!other_shard->pending_queue.empty()) {
          stolen_call = std::move(other_shard->pending_queue.front());
          other_shard->pending_queue.pop_front();
          other_shard->total_queued_calls--;
          break;
        }
        for (auto& [id, other_group] : other_shard->connection_groups) {
          if (other_group && !other_group->pending_queue.empty()) {
            stolen_call = std::move(other_group->pending_queue.front());
            other_group->pending_queue.pop_front();
            other_group->pending_calls_count--;
            other_shard->total_queued_calls--;
            break;
          }
        }
        if (stolen_call.has_value()) break;
      }
      if (stolen_call.has_value()) {
        bool enqueued_locally = false;
        {
          std::lock_guard<std::mutex> l_shard(shard_ptr->mutex);
          if (!shard_ptr->is_closed &&
              !group_ptr->is_closed.load(std::memory_order_acquire) &&
              !group_ptr->sessions.empty()) {
            group_ptr->pending_queue.push_back(std::move(*stolen_call));
            group_ptr->pending_calls_count.fetch_add(1,
                                                     std::memory_order_seq_cst);
            shard_ptr->total_queued_calls++;
            enqueued_locally = true;
          }
        }
        if (enqueued_locally) {
          DrainQueue(*shard_ptr, group_ptr);
        } else {
          total_queued_calls_--;
          total_active_calls_--;
          if (stolen_call->cancel_timer) stolen_call->cancel_timer();
          (void)StartVirtualRpc(
              stolen_call->context, std::move(stolen_call->request),
              std::move(stolen_call->callback), stolen_call->is_write_request);
        }
      }
    }
  }
}

void SessionPoolImpl::UpdateGroupsSnapshotLocked(Shard& shard) {
  auto new_vector =
      std::make_shared<std::vector<std::shared_ptr<ConnectionGroup>>>();
  new_vector->reserve(shard.connection_groups.size());
  for (auto const& [id, group] : shard.connection_groups) {
    new_vector->push_back(group);
  }
  std::atomic_store(
      &shard.groups_snapshot,
      std::shared_ptr<std::vector<std::shared_ptr<ConnectionGroup>> const>(
          std::move(new_vector)));
}

void SessionPoolImpl::UpdateSessionsSnapshotLocked(ConnectionGroup* group) {
  auto new_vector = std::make_shared<std::vector<SessionEntry>>();
  *new_vector = group->sessions;
  double weight_sum = 0.0;
  std::size_t weight_count = 0;
  for (auto const& entry : group->sessions) {
    if (entry.session) {
      weight_sum += std::max(0.01, entry.session->GetPeerWeight());
      weight_count++;
    }
  }
  group->avg_peer_weight.store(
      weight_count > 0 ? (weight_sum / static_cast<double>(weight_count)) : 1.0,
      std::memory_order_relaxed);
  std::atomic_store(
      &group->sessions_ptr,
      std::shared_ptr<std::vector<SessionEntry> const>(std::move(new_vector)));
}

std::shared_ptr<SessionPoolImpl::ConnectionGroup>
SessionPoolImpl::GetLeastLoadedGroup(
    std::shared_ptr<std::vector<std::shared_ptr<ConnectionGroup>> const> const&
        snapshot) const {
  if (!snapshot || snapshot->empty()) return nullptr;
  auto is_group_usable = [](std::shared_ptr<ConnectionGroup> const& group) {
    if (group == nullptr || group->is_closed.load(std::memory_order_acquire)) {
      return false;
    }
    if (group->is_blacklisted.load(std::memory_order_relaxed) &&
        SessionImpl::Clock::now() <
            SessionImpl::FromNanos((group->blacklist_expire_time_us.load(
                                       std::memory_order_relaxed)) *
                                   1000)) {
      return false;
    }
    return true;
  };
  if (snapshot->size() == 1) {
    auto const& group = (*snapshot)[0];
    return is_group_usable(group) ? group : nullptr;
  }

  DynamicPicker::Strategy const strat = picker_.strategy();
  auto evaluate_group_load = [strat](ConnectionGroup const& group) {
    int const active_calls =
        group.active_calls_count.load(std::memory_order_relaxed);
    int const pending_calls =
        group.pending_calls_count.load(std::memory_order_relaxed);
    int const active_sessions =
        group.active_sessions_count.load(std::memory_order_relaxed);
    double peer_weight = group.avg_peer_weight.load(std::memory_order_relaxed);
    auto sessions_ptr = std::atomic_load(&group.sessions_ptr);
    if (sessions_ptr && !sessions_ptr->empty() &&
        (*sessions_ptr)[0].session != nullptr) {
      peer_weight = std::max(0.01, (*sessions_ptr)[0].session->GetPeerWeight());
    }
    if (active_sessions <= 0) {
      return std::numeric_limits<double>::max() - 1.0;
    }
    if (strat == DynamicPicker::Strategy::kLeastLatency) {
      double const ewma_us = group.ok_gated_ewma.e2e_ewma_us();
      if (ewma_us > 0.0) {
        return ewma_us *
               (static_cast<double>(active_calls + pending_calls) + 1.0) /
               (static_cast<double>(active_sessions) * peer_weight);
      }
    }
    return (static_cast<double>(active_calls + pending_calls) + 0.001) /
           (static_cast<double>(active_sessions) * peer_weight);
  };

  std::int64_t const subset_k = (strat == DynamicPicker::Strategy::kRandom)
                                    ? 1
                                    : picker_.random_subset_size();
  std::size_t const n = snapshot->size();
  bool const use_p2c = subset_k > 0 && n > static_cast<std::size_t>(subset_k);

  std::shared_ptr<ConnectionGroup> target_group;
  double min_load = std::numeric_limits<double>::max();
  std::shared_ptr<ConnectionGroup> non_saturated_target;
  double min_non_saturated_load = std::numeric_limits<double>::max();

  auto consider_group = [&](std::shared_ptr<ConnectionGroup> const& group) {
    if (!is_group_usable(group)) return;
    int const active_calls =
        group->active_calls_count.load(std::memory_order_relaxed);
    int const pending_calls =
        group->pending_calls_count.load(std::memory_order_relaxed);
    int const active_sessions =
        group->active_sessions_count.load(std::memory_order_relaxed);
    double const load = evaluate_group_load(*group);

    if (active_sessions > 0 && active_calls < active_sessions &&
        pending_calls == 0) {
      if (load < min_non_saturated_load) {
        min_non_saturated_load = load;
        non_saturated_target = group;
      }
    }
    if (load < min_load) {
      min_load = load;
      target_group = group;
    }
  };

  if (use_p2c) {
    thread_local google::cloud::internal::DefaultPRNG gen =
        google::cloud::internal::MakeDefaultPRNG();
    std::size_t const start =
        std::uniform_int_distribution<std::size_t>(0, n - 1)(gen);
    std::size_t const stride =
        (n > 2)
            ? (1 + std::uniform_int_distribution<std::size_t>(0, n - 2)(gen))
            : 1;
    std::size_t sampled = 0;
    for (std::size_t i = 0;
         i < n && sampled < static_cast<std::size_t>(subset_k); ++i) {
      auto const& group = (*snapshot)[(start + i * stride) % n];
      if (!is_group_usable(group)) continue;
      consider_group(group);
      sampled++;
    }
    if (non_saturated_target != nullptr) return non_saturated_target;
  }

  for (auto const& group : *snapshot) {
    consider_group(group);
  }
  return non_saturated_target != nullptr ? non_saturated_target : target_group;
}

void SessionPoolImpl::OnWriteDone(std::shared_ptr<SessionImpl> session) {
  Shard& shard = GetShardForSession(session);
  std::string peer_id = session->GetPeerId().value_or("default_peer");
  std::shared_ptr<ConnectionGroup> group;
  {
    std::lock_guard<std::mutex> l_shard(shard.mutex);
    auto it = shard.connection_groups.find(peer_id);
    if (it != shard.connection_groups.end()) {
      group = it->second;
    } else {
      for (auto const& [id, candidate] : shard.connection_groups) {
        for (auto const& entry : candidate->sessions) {
          if (entry.session == session) {
            group = candidate;
            break;
          }
        }
        if (group != nullptr) break;
      }
    }
  }

  if (group == nullptr) {
    return;
  }

  {
    std::lock_guard<std::mutex> l_shard(shard.mutex);
    if (!group->is_closed && session->state() == SessionImpl::State::kReady &&
        !session->IsWriteCongested() && session->active_rpc_count() == 0 &&
        !session->in_idle_queue()) {
      group->idle_queue.Push(session);
    }
  }

  DrainQueue(shard, group);
  if (total_queued_calls_.load(std::memory_order_relaxed) > 0 &&
      shards_.size() > 1) {
    if (session && session->active_rpc_count() == 0 &&
        session->state() == SessionImpl::State::kReady &&
        !session->IsWriteCongested()) {
      std::optional<PendingCall> stolen_call;
      for (auto& other_shard : shards_) {
        if (other_shard.get() == &shard) continue;
        if (other_shard->total_queued_calls.load(std::memory_order_relaxed) <=
            0) {
          continue;
        }
        std::lock_guard<std::mutex> l_other(other_shard->mutex);
        if (other_shard->is_closed) continue;
        if (!other_shard->pending_queue.empty()) {
          stolen_call = std::move(other_shard->pending_queue.front());
          other_shard->pending_queue.pop_front();
          other_shard->total_queued_calls--;
          break;
        }
        for (auto& [id, other_group] : other_shard->connection_groups) {
          if (other_group && !other_group->pending_queue.empty()) {
            stolen_call = std::move(other_group->pending_queue.front());
            other_group->pending_queue.pop_front();
            other_group->pending_calls_count--;
            other_shard->total_queued_calls--;
            break;
          }
        }
        if (stolen_call.has_value()) break;
      }
      if (stolen_call.has_value()) {
        bool enqueued_locally = false;
        {
          std::lock_guard<std::mutex> l_shard(shard.mutex);
          if (!shard.is_closed &&
              !group->is_closed.load(std::memory_order_acquire) &&
              !group->sessions.empty()) {
            group->pending_queue.push_back(std::move(*stolen_call));
            group->pending_calls_count.fetch_add(1, std::memory_order_seq_cst);
            shard.total_queued_calls++;
            enqueued_locally = true;
          }
        }
        if (enqueued_locally) {
          DrainQueue(shard, group);
        } else {
          total_queued_calls_--;
          total_active_calls_--;
          if (stolen_call->cancel_timer) stolen_call->cancel_timer();
          (void)StartVirtualRpc(
              stolen_call->context, std::move(stolen_call->request),
              std::move(stolen_call->callback), stolen_call->is_write_request);
        }
      }
    }
    for (auto& other_shard : shards_) {
      if (other_shard.get() != &shard) {
        DrainQueue(*other_shard, nullptr);
      }
    }
  }
}

void SessionPoolImpl::OnSlotDrained(std::shared_ptr<SessionImpl> session,
                                    int64_t rpc_id) {
  (void)rpc_id;
  if (!session) return;
  IncrementStubActiveCalls(*session, -1);
  Shard& shard = GetShardForSession(session);
  std::string peer_id = session->GetPeerId().value_or("default_peer");
  std::shared_ptr<ConnectionGroup> group;
  {
    std::lock_guard<std::mutex> l_shard(shard.mutex);
    auto it = shard.connection_groups.find(peer_id);
    if (it != shard.connection_groups.end()) {
      group = it->second;
    }
    bool eligible_by_session_list = true;
    auto it_h = shard.session_handles.find(session.get());
    if (it_h != shard.session_handles.end()) {
      eligible_by_session_list = shard.session_list.ReleaseToPool(it_h->second);
    }
    // `ReleaseToPool` only re-enqueues the session if `state == kReady` and
    // the session is still counted in `in_expected_count` (i.e. did not
    // receive `GOAWAY` while in flight).
    if (eligible_by_session_list && group != nullptr && !group->is_closed &&
        session->state() == SessionImpl::State::kReady &&
        !session->IsWriteCongested() && session->active_rpc_count() == 0 &&
        !session->in_idle_queue()) {
      group->idle_queue.Push(session);
    }
  }

  if (group != nullptr) {
    group->active_calls_count.fetch_sub(1, std::memory_order_relaxed);
  }
  shard.total_active_calls.fetch_sub(1, std::memory_order_relaxed);
  if (total_active_calls_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    std::lock_guard<std::mutex> l_pw(pre_warm_mutex_);
    active_calls_condvar_.notify_all();
  }
  if (metrics_collector_) {
    metrics_collector_->SetActiveCalls(total_active_calls_.load());
  }

  if (group != nullptr) {
    DrainQueue(shard, group);
    if (total_queued_calls_.load(std::memory_order_relaxed) > 0 &&
        shards_.size() > 1) {
      for (auto& other_shard : shards_) {
        if (other_shard.get() != &shard) {
          DrainQueue(*other_shard, nullptr);
        }
      }
    }
  }
}

void SessionPoolImpl::ClearCancelledContext(
    grpc::ClientContext const* context) {
  if (context == nullptr) return;
  std::lock_guard<std::mutex> l(cancel_ctx_mutex_);
  cancelled_contexts_.erase(context);
}

bool SessionPoolImpl::ConsumeCancelledContext(
    grpc::ClientContext const* context) {
  if (context == nullptr) return false;
  std::lock_guard<std::mutex> l(cancel_ctx_mutex_);
  return cancelled_contexts_.erase(context) > 0;
}

void SessionPoolImpl::CancelByContext(grpc::ClientContext* context,
                                      google::cloud::Status status) {
  if (context == nullptr) return;

  {
    std::lock_guard<std::mutex> l(cancel_ctx_mutex_);
    if (cancelled_contexts_.size() > 4096) {
      cancelled_contexts_.clear();
    }
    cancelled_contexts_.insert(context);
  }

  std::vector<GenericCallback> callbacks_to_fail;
  std::vector<std::shared_ptr<SessionImpl>> sessions_to_cancel;
  int pending_aborted_count = 0;

  for (auto& shard : shards_) {
    std::lock_guard<std::mutex> l_shard(shard->mutex);
    if (shard->is_closed) continue;

    auto it = shard->pending_queue.begin();
    while (it != shard->pending_queue.end()) {
      if (it->context == context) {
        if (it->cancel_timer) {
          it->cancel_timer();
        }
        if (it->callback != nullptr) {
          callbacks_to_fail.push_back(std::move(it->callback));
        }
        it = shard->pending_queue.erase(it);
        shard->total_queued_calls--;
        total_queued_calls_--;
        pending_aborted_count++;
      } else {
        ++it;
      }
    }

    for (auto& [id, group] : shard->connection_groups) {
      auto it_pending = group->pending_queue.begin();
      while (it_pending != group->pending_queue.end()) {
        if (it_pending->context == context) {
          if (it_pending->cancel_timer) {
            it_pending->cancel_timer();
          }
          if (it_pending->callback != nullptr) {
            callbacks_to_fail.push_back(std::move(it_pending->callback));
          }
          it_pending = group->pending_queue.erase(it_pending);
          group->pending_calls_count--;
          shard->total_queued_calls--;
          total_queued_calls_--;
          pending_aborted_count++;
        } else {
          ++it_pending;
        }
      }

      for (auto& entry : group->sessions) {
        if (entry.session->HasActiveContext(context)) {
          sessions_to_cancel.push_back(entry.session);
        }
      }
    }

    for (auto const& draining_sess : shard->draining_sessions) {
      if (draining_sess && draining_sess->HasActiveContext(context)) {
        sessions_to_cancel.push_back(draining_sess);
      }
    }
  }

  if (pending_aborted_count > 0) {
    if (total_active_calls_.fetch_sub(pending_aborted_count) ==
        pending_aborted_count) {
      std::lock_guard<std::mutex> l_pw(pre_warm_mutex_);
      active_calls_condvar_.notify_all();
    }
    if (metrics_collector_) {
      metrics_collector_->SetActiveCalls(total_active_calls_.load());
      metrics_collector_->SetPendingQueueSize(total_queued_calls_.load());
    }
  }

  google::cloud::Status queued_status = status;
  google::cloud::internal::SetPayload(queued_status, kWriteStatusPayloadUrl,
                                      "0");

  for (auto& cb : callbacks_to_fail) {
    if (cb != nullptr) {
      if (user_callback_pool_ != nullptr) {
        static std::atomic<size_t> rr_counter{0};
        user_callback_pool_->Submit(
            [c = std::move(cb), queued_status]() mutable {
              std::move(c)(queued_status);
            },
            rr_counter.fetch_add(1, std::memory_order_relaxed));
      } else {
        auto general_cq = GetGeneralCQ();
        if (general_cq) {
          general_cq->RunAsync([c = std::move(cb), queued_status]() mutable {
            std::move(c)(queued_status);
          });
        } else {
          std::move(cb)(queued_status);
        }
      }
    }
  }

  for (auto const& session : sessions_to_cancel) {
    session->CancelRpcByContext(context);
  }
  if (pending_aborted_count > 0 || !sessions_to_cancel.empty()) {
    ClearCancelledContext(context);
  }
}

SessionPoolImpl::Shard& SessionPoolImpl::GetShardForSession(
    std::shared_ptr<SessionImpl> const& session) {
  if (session != nullptr) {
    if (auto* h = static_cast<pool::SessionList::SessionHandle*>(
            session->pool_handle().get())) {
      if (h->shard_index < shards_.size()) {
        return *shards_[h->shard_index];
      }
    }
  }
  for (auto& shard : shards_) {
    std::lock_guard<std::mutex> l(shard->mutex);
    if (session != nullptr && shard->session_handles.find(session.get()) !=
                                  shard->session_handles.end()) {
      return *shard;
    }
    for (auto const& s : shard->connecting_sessions) {
      if (s == session) return *shard;
    }
    for (auto const& s : shard->draining_sessions) {
      if (s == session) return *shard;
    }
    for (auto const& [id, group] : shard->connection_groups) {
      for (auto const& entry : group->sessions) {
        if (entry.session == session) return *shard;
      }
    }
  }
  auto* current = current_cq_impl;
  if (current != nullptr) {
    for (auto& shard : shards_) {
      if (shard->cq_impl == current) {
        return *shard;
      }
    }
  }
  return *shards_[0];
}

int SessionPoolImpl::GetActiveCallsForStub(
    google::bigtable::v2::Bigtable::Stub const* stub) const {
  auto it = std::find(stubs_.begin(), stubs_.end(), stub);
  if (it != stubs_.end()) {
    size_t idx = std::distance(stubs_.begin(), it);
    if (idx < active_calls_per_stub_.size()) {
      return active_calls_per_stub_[idx].load(std::memory_order_relaxed);
    }
  }
  return 0;
}

SessionPoolImpl::Shard& SessionPoolImpl::GetShardForCurrentThread() {
  auto* current = current_cq_impl;
  if (current != nullptr) {
    for (auto& shard : shards_) {
      if (shard->cq_impl == current) {
        return *shard;
      }
    }
  }
  thread_local size_t const cached_hash = [] {
    // Assign each thread a sequential index on first access so worker thread
    // pools distribute evenly across shards without hash-collision clustering
    // or pthread_t address reuse across short-lived threads.
    static std::atomic<size_t> next_thread_id{0};
    return next_thread_id.fetch_add(1, std::memory_order_relaxed);
  }();

  size_t const num_shards = shards_.size();
  Shard& preferred = *shards_[cached_hash % num_shards];
  if (preferred.ready_sessions_count.load(std::memory_order_relaxed) > 0) {
    return preferred;
  }

  size_t ready_count = 0;
  for (auto const& shard : shards_) {
    if (shard->ready_sessions_count.load(std::memory_order_relaxed) > 0) {
      ++ready_count;
    }
  }
  if (ready_count > 0) {
    size_t target_idx = cached_hash % ready_count;
    for (auto& shard : shards_) {
      if (shard->ready_sessions_count.load(std::memory_order_relaxed) > 0) {
        if (target_idx == 0) return *shard;
        --target_idx;
      }
    }
  }
  return preferred;
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
