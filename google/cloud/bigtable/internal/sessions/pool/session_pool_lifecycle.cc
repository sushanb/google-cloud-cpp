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

// SessionPoolImpl lifecycle: session creation and replenishment, session
// readiness/GOAWAY/close callbacks, pre-warming, idle reaping, and pool
// shutdown. The request fast path lives in session_pool_impl.cc.

#include "google/cloud/bigtable/internal/sessions/logging_helper.h"
#include "google/cloud/bigtable/internal/sessions/pool/dynamic_picker.h"
#include "google/cloud/bigtable/internal/sessions/pool/lightweight_thread_pool.h"
#include "google/cloud/bigtable/internal/sessions/pool/metrics_collector.h"
#include "google/cloud/bigtable/internal/sessions/pool/pool_executors.h"
#include "google/cloud/bigtable/internal/sessions/pool/pool_sizer.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_pool_impl.h"
#include "google/cloud/bigtable/internal/sessions/session/session_impl.h"
#include "google/cloud/bigtable/internal/sessions/session/write_status.h"
#include "google/cloud/bigtable/internal/sessions/sessions_options.h"
#include "google/cloud/bigtable/internal/sessions/transport/current_cq.h"
#include "google/cloud/internal/getenv.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/log.h"
#include "google/cloud/options.h"
#include "google/cloud/status.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include <grpcpp/client_context.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
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
struct ScopedCounter {
  std::atomic<int>& counter;
  std::mutex& mu;
  std::condition_variable& cv;
  ScopedCounter(std::atomic<int>& c, std::mutex& m, std::condition_variable& v)
      : counter(c), mu(m), cv(v) {
    counter.fetch_add(1, std::memory_order_acq_rel);
  }
  ~ScopedCounter() {
    if (counter.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      std::lock_guard<std::mutex> l(mu);
      cv.notify_all();
    }
  }
};
}  // namespace

void SessionPoolImpl::Close() {
  if (is_closed_.exchange(true)) {
    return;
  }

  if (metrics_collector_) {
    metrics_collector_->Stop();
    metrics_collector_->SetPendingQueueSize(0);
    metrics_collector_->SetActiveSessions(0);
    metrics_collector_->SetActiveCalls(0);
  }

  {
    std::lock_guard<std::mutex> l_pw(pre_warm_mutex_);
    pre_warm_condvar_.notify_all();
  }

  SessionImpl::TimePoint const close_now = SessionImpl::Clock::now();
  bool const emit_uptime_on_close = session_tracer_.uptime_sample_count() == 0;
  for (auto& shard : shards_) {
    struct ClosingSessionInfo {
      std::shared_ptr<SessionImpl> session;
      bool was_connecting = false;
      bool was_ready = false;
      bool was_draining = false;
    };
    std::vector<ClosingSessionInfo> sessions_to_close;
    std::deque<PendingCall> aborted_queue;

    {
      std::lock_guard<std::mutex> l_shard(shard->mutex);
      shard->is_closed = true;

      size_t total_to_close =
          shard->connecting_sessions.size() + shard->draining_sessions.size();
      for (auto const& [id, group] : shard->connection_groups) {
        total_to_close += group->sessions.size();
      }
      sessions_to_close.reserve(total_to_close);

      for (auto& s : shard->connecting_sessions) {
        sessions_to_close.push_back({std::move(s), true, false, false});
      }
      shard->connecting_sessions.clear();
      for (auto const& s : shard->draining_sessions) {
        sessions_to_close.push_back({s, false, false, true});
      }
      shard->draining_sessions.clear();

      for (auto& [id, group] : shard->connection_groups) {
        for (auto const& entry : group->sessions) {
          sessions_to_close.push_back({entry.session, false, true, false});
        }
        group->sessions.clear();
        group->idle_queue.Clear();
        std::atomic_store(&group->sessions_ptr,
                          std::shared_ptr<std::vector<SessionEntry> const>());
        group->active_sessions_count = 0;
        group->active_calls_count = 0;

        for (auto& item : group->pending_queue) {
          if (item.cancel_timer) {
            item.cancel_timer();
          }
          aborted_queue.push_back(std::move(item));
        }
        group->pending_queue.clear();
        group->pending_calls_count = 0;
      }

      for (auto& item : shard->pending_queue) {
        if (item.cancel_timer) {
          item.cancel_timer();
        }
        aborted_queue.push_back(std::move(item));
      }
      shard->pending_queue.clear();

      shard->total_queued_calls = 0;
      shard->total_active_calls = 0;
      shard->ready_sessions_count.store(0, std::memory_order_relaxed);
    }

    for (auto const& info : sessions_to_close) {
      if (info.session != nullptr) {
        if (emit_uptime_on_close) {
          double const uptime_ms =
              std::max(0.0, std::chrono::duration<double, std::milli>(
                                close_now - info.session->created_at())
                                .count());
          telemetry::UptimeAttributes up_attrs;
          up_attrs.ready = (info.was_ready || info.was_draining ||
                            info.session->ready_at().has_value())
                               ? "true"
                               : "false";
          if (auto pi = info.session->peer_info(); pi.has_value()) {
            up_attrs.transport_type =
                transport::PeerInfo::TransportTypeToString(pi->transport_type);
            up_attrs.afe_location = pi->application_frontend_subzone;
          }
          session_tracer_.RecordSessionUptimeMs(uptime_ms, up_attrs);
        }
        info.session->Close();
        RecordSessionCloseTelemetry(info.session, google::cloud::Status(),
                                    info.was_connecting, info.was_ready,
                                    info.was_draining);
      }
    }

    google::cloud::Status drop_err_never_left =
        google::cloud::internal::CancelledError(
            "Session pool gracefully closed");
    google::cloud::internal::SetPayload(drop_err_never_left,
                                        kWriteStatusPayloadUrl, "0");

    FailPendingCalls(aborted_queue, drop_err_never_left);
    // Shared (connection-scoped) completion queues also serve other pools;
    // only cancel them when this pool owns its executors.
    if (owns_executors_ && shard->cq.has_value()) {
      shard->cq->CancelAll();
    }
  }

  total_queued_calls_.store(0);
  total_active_calls_.store(0);
  total_ready_sessions_.store(0);
  total_connecting_sessions_.store(0);
  total_draining_sessions_.store(0);
  {
    std::lock_guard<std::mutex> l_cv(pre_warm_mutex_);
    active_calls_condvar_.notify_all();
  }

  bool is_on_shard_cq = false;
  auto* current_cq = current_cq_impl;
  if (current_cq != nullptr) {
    for (auto const& shard : shards_) {
      if (shard->cq_impl == current_cq) {
        is_on_shard_cq = true;
        break;
      }
    }
  }
  if (!is_on_shard_cq) {
    std::unique_lock<std::mutex> l_cv(pre_warm_mutex_);
    while (active_lifecycle_callbacks_.load(std::memory_order_acquire) > 0) {
      active_calls_condvar_.wait_for(l_cv, std::chrono::milliseconds(10));
    }
  }
}

void SessionPoolImpl::FailPendingCalls(std::deque<PendingCall>& queue,
                                       google::cloud::Status status) {
  if (status.ok()) {
    status = google::cloud::internal::UnavailableError(
        "Session closed before request could be dispatched");
  }
  google::cloud::internal::SetPayload(status, kWriteStatusPayloadUrl, "0");
  MarkPoolRejected(status);
  for (auto& item : queue) {
    if (item.callback != nullptr) {
      if (user_callback_pool_ != nullptr) {
        user_callback_pool_->Submit(
            [cb = std::move(item.callback), status]() mutable {
              std::move(cb)(status);
            },
            item.request.rpc_id());
      } else {
        auto general_cq = GetGeneralCQ();
        if (general_cq) {
          general_cq->RunAsync([cb = std::move(item.callback),
                                status]() mutable { std::move(cb)(status); });
        } else {
          std::move(item.callback)(status);
        }
      }
    }
  }
}

void SessionPoolImpl::MaybeReplenishPool(Shard& shard,
                                         bool force_half_open_probe) {
  if (is_closed_.load() || stubs_.empty()) {
    return;
  }

  int ready_count = total_ready_sessions_.load();
  bool circuit_open = false;
  if (ready_count == 0) {
    circuit_open =
        consecutive_session_failures_.load(std::memory_order_acquire) >=
        consecutive_session_failure_threshold_.load(std::memory_order_relaxed);
    std::lock_guard<std::mutex> l_err(error_mutex_);
    if (IsTerminalHandshakeError(last_connection_error_.code())) {
      return;
    }
  }
  int active_calls = total_active_calls_.load();
  int pending_calls = std::max(0, total_queued_calls_.load());
  int in_use_calls = std::max(0, active_calls - pending_calls);
  int connecting_count = total_connecting_sessions_.load();
  int const max_initial_connecting =
      circuit_open ? 1 : std::max(1, initial_pool_size_);
  if (ready_count == 0 && connecting_count >= max_initial_connecting) {
    return;
  }

  int delta = pool_sizer_->GetScaleDelta(ready_count, connecting_count,
                                         in_use_calls, pending_calls);

  bool shard_starved = false;
  {
    std::lock_guard<std::mutex> l(shard.mutex);
    bool has_ready_in_shard = false;
    for (auto const& [id, group] : shard.connection_groups) {
      if (!group->is_closed && !group->sessions.empty()) {
        has_ready_in_shard = true;
        break;
      }
    }
    if (!has_ready_in_shard && shard.connecting_sessions.empty()) {
      shard_starved = true;
    }
  }
  int const total_sessions = ready_count + connecting_count;
  int const min_sessions = pool_sizer_ ? pool_sizer_->min_sessions() : 1;
  int const max_sessions = pool_sizer_ ? pool_sizer_->max_sessions() : 64;
  if (shard_starved && delta <= 0 &&
      (total_sessions < min_sessions ||
       ((pending_calls > 0 || force_half_open_probe) &&
        total_sessions < max_sessions))) {
    delta = 1;
  }
  if (circuit_open && delta > 1) {
    delta = 1;
  }

  for (int i = 0; i < delta; i++) {
    Shard* target_shard = &shard;
    if (i > 0 || !shard_starved) {
      Shard* starved_candidate = nullptr;
      for (auto& s : shards_) {
        std::lock_guard<std::mutex> l(s->mutex);
        if (!s->is_closed &&
            s->ready_sessions_count.load(std::memory_order_relaxed) == 0 &&
            s->connecting_sessions.empty()) {
          starved_candidate = s.get();
          break;
        }
      }
      if (starved_candidate != nullptr) {
        target_shard = starved_candidate;
      } else if (i > 0 && !shards_.empty()) {
        size_t idx = next_shard_index_.fetch_add(1, std::memory_order_relaxed) %
                     shards_.size();
        target_shard = shards_[idx].get();
      }
    }
    CreateSession(*target_shard, std::nullopt);
  }

  if (delta > 0 &&
      total_connecting_sessions_.load(std::memory_order_relaxed) == 0 &&
      scheduler_ != nullptr && !is_closed_.load(std::memory_order_relaxed)) {
    SessionImpl::TimePoint const next_avail = budget_.NextAvailableTime();
    SessionImpl::TimePoint const now = SessionImpl::Clock::now();
    if (next_avail > now &&
        !replenish_timer_scheduled_.exchange(true, std::memory_order_acq_rel)) {
      std::weak_ptr<SessionPoolImpl> weak_self = shared_from_this();
      scheduler_((next_avail - now) + std::chrono::milliseconds(10),
                 [weak_self]() {
                   if (auto self = weak_self.lock()) {
                     self->replenish_timer_scheduled_.store(
                         false, std::memory_order_release);
                     if (!self->is_closed_.load(std::memory_order_relaxed) &&
                         !self->shards_.empty()) {
                       self->MaybeReplenishPool(*self->shards_[0]);
                     }
                   }
                 });
    }
  }
}

std::shared_ptr<SessionImpl> SessionPoolImpl::SpawnSessionOnShard(
    Shard& shard,
    std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config,
    bool is_pre_warm) {
  google::bigtable::v2::Bigtable::Stub* stub = nullptr;
  int stub_idx = -1;
  if (!stubs_.empty()) {
    size_t idx = next_stub_index_.fetch_add(1, std::memory_order_relaxed) %
                 stubs_.size();
    stub = stubs_[idx];
    stub_idx = static_cast<int>(idx);
  }

  if (!refresh_config.has_value()) {
    std::lock_guard<std::mutex> l_meta(metadata_mutex_);
    refresh_config = last_refresh_config_;
  }

  SessionStream::OpenConfig open_config;
  open_config.feature_flags = feature_flags_;
  if (options_.has<
          ::google::cloud::bigtable_internal::SessionsFeatureFlagsOption>()) {
    open_config.feature_flags = options_.get<
        ::google::cloud::bigtable_internal::SessionsFeatureFlagsOption>();
  }
  if (options_.has<::google::cloud::bigtable_internal::
                       SessionsOpenAuthorizedViewPermissionOption>()) {
    open_config.authorized_view_permission =
        options_.get<::google::cloud::bigtable_internal::
                         SessionsOpenAuthorizedViewPermissionOption>();
  }
  if (options_.has<::google::cloud::bigtable_internal::
                       SessionsOpenMaterializedViewPermissionOption>()) {
    open_config.materialized_view_permission =
        options_.get<::google::cloud::bigtable_internal::
                         SessionsOpenMaterializedViewPermissionOption>();
  }

  auto new_sess = SessionImpl::Create(
      stub, shared_from_this(), table_name_, app_profile_id_, permission_,
      *shard.cq, scheduler_, std::move(refresh_config), metadata_hook_,
      std::move(open_config), /*start_open=*/false);
  new_sess->set_stub_index(stub_idx);
  ConfigureNewSession(*new_sess);

  {
    std::lock_guard<std::mutex> l_b(budget_holding_mutex_);
    sessions_holding_budget_.insert(new_sess);
  }
  {
    std::unique_lock<std::mutex> l(shard.mutex);
    if (shard.is_closed || is_closed_.load(std::memory_order_acquire)) {
      l.unlock();
      std::lock_guard<std::mutex> l_b(budget_holding_mutex_);
      sessions_holding_budget_.erase(new_sess);
      return nullptr;
    }
    auto handle = shard.session_list.NewStartingHandle(
        reinterpret_cast<std::int64_t>(new_sess.get()), shard.index);
    new_sess->set_pool_handle(handle);
    shard.session_handles[new_sess.get()] = std::move(handle);
    shard.connecting_sessions.push_back(new_sess);
    total_connecting_sessions_++;
  }
  if (is_pre_warm) {
    std::lock_guard<std::mutex> l_pw(pre_warm_mutex_);
    pre_warm_started_count_++;
    pre_warm_sessions_.insert(new_sess);
  }
  new_sess->StartOpen();
  std::chrono::nanoseconds const handshake_timeout = GetHandshakeTimeout();
  if (scheduler_ != nullptr &&
      handshake_timeout > std::chrono::nanoseconds(0)) {
    std::weak_ptr<SessionPoolImpl> weak_self = shared_from_this();
    std::weak_ptr<SessionImpl> weak_sess = new_sess;
    scheduler_(handshake_timeout, [weak_self, weak_sess]() {
      if (auto self = weak_self.lock()) {
        if (auto sess = weak_sess.lock()) {
          if (sess->state() == SessionImpl::State::kConnecting) {
            GCP_LOG(WARNING)
                << "[SESSIONS] Connection handshake timed out for session: "
                << sess.get();
            sess->Close();
          }
        }
      }
    });
  }
  if (is_closed_.load(std::memory_order_acquire)) {
    bool was_connecting = false;
    {
      std::lock_guard<std::mutex> l(shard.mutex);
      auto it = std::find(shard.connecting_sessions.begin(),
                          shard.connecting_sessions.end(), new_sess);
      if (it != shard.connecting_sessions.end()) {
        shard.connecting_sessions.erase(it);
        total_connecting_sessions_--;
        was_connecting = true;
      }
    }
    if (was_connecting) {
      std::lock_guard<std::mutex> l_cv(pre_warm_mutex_);
      active_calls_condvar_.notify_all();
    }
    new_sess->Close();
  }
  return new_sess;
}

void SessionPoolImpl::CreateSession(
    Shard& shard,
    std::optional<google::bigtable::v2::SessionRefreshConfig> refresh_config) {
  if (is_closed_.load() || stubs_.empty() || !shard.cq.has_value()) {
    return;
  }
  if (budget_.TryReserve()) {
    (void)SpawnSessionOnShard(shard, std::move(refresh_config));
  }
}

void SessionPoolImpl::OnInitialMetadata(
    std::shared_ptr<SessionImpl>,
    std::multimap<grpc::string_ref, grpc::string_ref> const& metadata) {
  std::lock_guard<std::mutex> l(metadata_mutex_);
  initial_metadata_storage_.clear();
  for (auto const& [k, v] : metadata) {
    initial_metadata_storage_.emplace_back(std::string(k.data(), k.size()),
                                           std::string(v.data(), v.size()));
  }
}

std::multimap<grpc::string_ref, grpc::string_ref>
SessionPoolImpl::initial_metadata() const {
  std::lock_guard<std::mutex> l(metadata_mutex_);
  std::multimap<grpc::string_ref, grpc::string_ref> result;
  for (auto const& [k, v] : initial_metadata_storage_) {
    result.emplace(grpc::string_ref(k.data(), k.size()),
                   grpc::string_ref(v.data(), v.size()));
  }
  return result;
}

std::optional<google::bigtable::v2::BackendIdentifier>
SessionPoolImpl::backend() const {
  for (auto const& shard : shards_) {
    std::lock_guard<std::mutex> l(shard->mutex);
    for (auto const& [id, group] : shard->connection_groups) {
      for (auto const& entry : group->sessions) {
        if (entry.session != nullptr && entry.session->backend().has_value()) {
          return entry.session->backend();
        }
      }
    }
  }
  return std::nullopt;
}

std::optional<google::bigtable::v2::SessionRefreshConfig>
SessionPoolImpl::session_refresh_config() const {
  for (auto const& shard : shards_) {
    std::lock_guard<std::mutex> l(shard->mutex);
    for (auto const& [id, group] : shard->connection_groups) {
      for (auto const& entry : group->sessions) {
        if (entry.session != nullptr &&
            entry.session->refresh_config().has_value()) {
          return entry.session->refresh_config();
        }
      }
    }
    for (auto const& s : shard->draining_sessions) {
      if (s != nullptr && s->refresh_config().has_value()) {
        return s->refresh_config();
      }
    }
  }
  std::lock_guard<std::mutex> l(metadata_mutex_);
  return last_refresh_config_;
}

google::cloud::Status SessionPoolImpl::last_connection_error() const {
  std::lock_guard<std::mutex> l(error_mutex_);
  return last_connection_error_;
}

void SessionPoolImpl::OnSessionReady(
    std::shared_ptr<SessionImpl> session,
    google::bigtable::v2::OpenSessionResponse const& response) {
  ScopedCounter lifecycle_guard(active_lifecycle_callbacks_, pre_warm_mutex_,
                                active_calls_condvar_);
  (void)response;
  budget_.OnSuccess();
  {
    std::lock_guard<std::mutex> l_b(budget_holding_mutex_);
    sessions_holding_budget_.erase(session);
  }
  consecutive_session_failures_.store(0, std::memory_order_release);
  Shard& shard = GetShardForSession(session);

  bool pool_closed = false;
  {
    std::lock_guard<std::mutex> l_shard(shard.mutex);
    pool_closed = shard.is_closed;
  }
  if (pool_closed) {
    session->Close();
    return;
  }

  std::string peer_id = session->GetPeerId().value_or("default_peer");

  std::shared_ptr<ConnectionGroup> group;
  bool notify_pre_warm = false;
  {
    std::lock_guard<std::mutex> l_pw(pre_warm_mutex_);
    auto it = pre_warm_sessions_.find(session);
    if (it != pre_warm_sessions_.end()) {
      pre_warm_sessions_.erase(it);
      pre_warm_success_count_++;
      if (pre_warm_sessions_.empty()) {
        notify_pre_warm = true;
      }
    }
  }

  bool abort_ready = false;
  bool close_session_now = false;
  {
    std::lock_guard<std::mutex> l_shard(shard.mutex);

    // Remove from connecting sessions
    auto it_conn = std::find(shard.connecting_sessions.begin(),
                             shard.connecting_sessions.end(), session);
    if (it_conn != shard.connecting_sessions.end()) {
      shard.connecting_sessions.erase(it_conn);
      total_connecting_sessions_--;
    }

    if (shard.is_closed || is_closed_.load(std::memory_order_acquire) ||
        session->state() != SessionImpl::State::kReady) {
      abort_ready = true;
      close_session_now =
          shard.is_closed || is_closed_.load(std::memory_order_acquire);
      auto it_h = shard.session_handles.find(session.get());
      if (it_h != shard.session_handles.end()) {
        if (close_session_now ||
            session->state() == SessionImpl::State::kClosed) {
          shard.session_list.OnSessionClosed(it_h->second);
          shard.session_handles.erase(it_h);
        } else {
          shard.session_list.OnSessionClosing(it_h->second);
        }
      }
    } else {
      auto it_h = shard.session_handles.find(session.get());
      if (it_h == shard.session_handles.end()) {
        auto handle = shard.session_list.NewStartingHandle(
            reinterpret_cast<std::int64_t>(session.get()), shard.index);
        it_h = shard.session_handles.emplace(session.get(), std::move(handle))
                   .first;
      }
      session->set_pool_handle(it_h->second);
      shard.session_list.OnSessionReady(it_h->second,
                                        ParseNumericPeerId(peer_id));

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

      group->sessions.push_back(SessionEntry{session});
      group->idle_queue.Push(session);
      group->active_sessions_count++;
      shard.ready_sessions_count.fetch_add(1, std::memory_order_relaxed);
      total_ready_sessions_++;
      UpdateSessionsSnapshotLocked(group.get());

      if (new_group) {
        UpdateGroupsSnapshotLocked(shard);
      }

      // Try to drain pending calls from shard queue and group queue
      // 1. Drain shard queue first to the group that just became ready
      while (!shard.pending_queue.empty()) {
        auto& item = shard.pending_queue.front();
        group->pending_queue.push_back(std::move(item));
        group->pending_calls_count++;
        shard.pending_queue.pop_front();
      }
    }
  }

  if (abort_ready) {
    {
      std::lock_guard<std::mutex> l_b(budget_holding_mutex_);
      sessions_holding_budget_.erase(session);
    }
    if (notify_pre_warm) {
      std::lock_guard<std::mutex> l_pw(pre_warm_mutex_);
      pre_warm_condvar_.notify_all();
    }
    if (close_session_now) {
      session->Close();
    }
    return;
  }

  {
    std::lock_guard<std::mutex> l_err(error_mutex_);
    last_connection_error_ = google::cloud::Status();
  }

  // Also drain any cold-start calls queued on other shards that have no ready
  // connection groups yet.
  for (auto& other_shard : shards_) {
    if (other_shard.get() == &shard) continue;
    std::deque<PendingCall> stolen_calls;
    {
      std::lock_guard<std::mutex> l_other(other_shard->mutex);
      if (other_shard->connection_groups.empty() &&
          !other_shard->pending_queue.empty()) {
        stolen_calls = std::move(other_shard->pending_queue);
        other_shard->pending_queue.clear();
        other_shard->total_queued_calls -=
            static_cast<int>(stolen_calls.size());
      }
    }
    if (!stolen_calls.empty()) {
      std::lock_guard<std::mutex> l_shard(shard.mutex);
      shard.total_queued_calls += static_cast<int>(stolen_calls.size());
      for (auto& item : stolen_calls) {
        group->pending_queue.push_back(std::move(item));
        group->pending_calls_count++;
      }
    }
  }

  // 2. Drain group queue (outside lock)
  DrainQueue(shard, group);

  if (notify_pre_warm) {
    std::lock_guard<std::mutex> l_pw(pre_warm_mutex_);
    pre_warm_condvar_.notify_all();
  }

  {
    std::lock_guard<std::mutex> l_b(budget_holding_mutex_);
    sessions_holding_budget_.erase(session);
  }
  double const open_latency_ms =
      session != nullptr
          ? std::max(0.0, std::chrono::duration<double, std::milli>(
                              SessionImpl::Clock::now() - session->created_at())
                              .count())
          : 0.0;
  telemetry::OpenLatencyAttributes open_attrs;
  open_attrs.status = "OK";
  if (session != nullptr) {
    if (auto pi = session->peer_info(); pi.has_value()) {
      open_attrs.transport_type =
          transport::PeerInfo::TransportTypeToString(pi->transport_type);
      open_attrs.afe_location = pi->application_frontend_subzone;
    }
  }
  session_tracer_.RecordSessionOpenLatencyMs(open_latency_ms, open_attrs);
  if (metrics_collector_) {
    metrics_collector_->OnSessionCreated();
    metrics_collector_->SetActiveSessions(total_ready_sessions_.load());
    metrics_collector_->SetPendingQueueSize(total_queued_calls_.load());
  }
}

void SessionPoolImpl::OnGoAway(std::shared_ptr<SessionImpl> session) {
  ScopedCounter lifecycle_guard(active_lifecycle_callbacks_, pre_warm_mutex_,
                                active_calls_condvar_);
  Shard& shard = GetShardForSession(session);
  std::string peer_id = session->GetPeerId().value_or("default_peer");
  std::shared_ptr<ConnectionGroup> group;
  {
    std::lock_guard<std::mutex> l_shard(shard.mutex);
    auto it_h = shard.session_handles.find(session.get());
    if (it_h != shard.session_handles.end()) {
      shard.session_list.OnSessionClosing(it_h->second);
    }
    auto it = shard.connection_groups.find(peer_id);
    if (it != shard.connection_groups.end()) {
      group = it->second;
    }
  }

  std::deque<PendingCall> aborted_group_queue;
  bool need_replacement_on_shard = false;

  if (group != nullptr) {
    std::lock_guard<std::mutex> l_shard(shard.mutex);
    group->idle_queue.Remove({session.get()});
    auto it = std::find_if(group->sessions.begin(), group->sessions.end(),
                           [&session](SessionEntry const& entry) {
                             return entry.session == session;
                           });
    if (it != group->sessions.end()) {
      group->sessions.erase(it);
      group->active_sessions_count--;
      shard.ready_sessions_count.fetch_sub(1, std::memory_order_relaxed);
      total_ready_sessions_--;
      shard.draining_sessions.insert(session);
      shard.draining_since[session.get()] = SessionImpl::Clock::now();
      total_draining_sessions_++;
      UpdateSessionsSnapshotLocked(group.get());
    }
    if (group->sessions.empty()) {
      group->is_closed.store(true, std::memory_order_release);
      shard.connection_groups.erase(peer_id);
      UpdateGroupsSnapshotLocked(shard);

      aborted_group_queue = std::move(group->pending_queue);
      group->pending_queue.clear();
      int group_pending_count = group->pending_calls_count.exchange(0);
      shard.total_queued_calls -= group_pending_count;
      total_queued_calls_ -= group_pending_count;
      total_active_calls_ -= group_pending_count;
    }

    int const ready_now = total_ready_sessions_.load(std::memory_order_relaxed);
    int const conn_now =
        total_connecting_sessions_.load(std::memory_order_relaxed);
    int const pending_now =
        std::max(0, total_queued_calls_.load(std::memory_order_relaxed));
    int const in_use_now = std::max(
        0, total_active_calls_.load(std::memory_order_relaxed) - pending_now);
    int const desired_delta =
        pool_sizer_ ? pool_sizer_->GetScaleDelta(ready_now, conn_now,
                                                 in_use_now, pending_now)
                    : ((ready_now + conn_now) < 64 ? 1 : 0);
    if (!is_closed_.load(std::memory_order_acquire) && desired_delta > 0) {
      need_replacement_on_shard = true;
    }
  }

  if (metrics_collector_) {
    metrics_collector_->SetActiveSessions(total_ready_sessions_.load());
  }

  if (session != nullptr && session->refresh_config().has_value()) {
    std::lock_guard<std::mutex> l_meta(metadata_mutex_);
    last_refresh_config_ = session->refresh_config();
  }

  if (need_replacement_on_shard) {
    CreateSession(shard, session->refresh_config());
  }
  session->Drain();
  MaybeReplenishPool(shard);

  if (!aborted_group_queue.empty()) {
    for (auto& call : aborted_group_queue) {
      if (call.cancel_timer) {
        call.cancel_timer();
      }
      auto start_status =
          StartVirtualRpc(call.context, std::move(call.request),
                          std::move(call.callback), call.is_write_request);
      if (!start_status.ok()) {
        ::google::cloud::bigtable_internal::sessions::internal::
            LogVerboseWarning(1, "SessionPoolImpl",
                              absl::StrCat("Failed to reschedule GoAway call: ",
                                           start_status.message()));
      }
    }
  }
}

void SessionPoolImpl::OnSessionClosed(std::shared_ptr<SessionImpl> session,
                                      google::cloud::Status status) {
  ScopedCounter lifecycle_guard(active_lifecycle_callbacks_, pre_warm_mutex_,
                                active_calls_condvar_);
  bool held_budget = false;
  {
    std::lock_guard<std::mutex> l_b(budget_holding_mutex_);
    held_budget = (sessions_holding_budget_.erase(session) > 0);
  }
  Shard& shard = GetShardForSession(session);
  std::string peer_id = session->GetPeerId().value_or("default_peer");

  if (session != nullptr && session->refresh_config().has_value()) {
    std::lock_guard<std::mutex> l_meta(metadata_mutex_);
    last_refresh_config_ = session->refresh_config();
  }
  if (!status.ok()) {
    std::lock_guard<std::mutex> l_err(error_mutex_);
    last_connection_error_ = status;
  }

  bool was_ready = false;
  bool was_connecting = false;
  bool was_draining = false;
  std::shared_ptr<ConnectionGroup> group;

  {
    std::lock_guard<std::mutex> l_shard(shard.mutex);
    auto it_h = shard.session_handles.find(session.get());
    if (it_h != shard.session_handles.end()) {
      shard.session_list.OnSessionClosed(it_h->second);
      shard.session_handles.erase(it_h);
    }
    auto it_conn = std::find(shard.connecting_sessions.begin(),
                             shard.connecting_sessions.end(), session);
    if (it_conn != shard.connecting_sessions.end()) {
      shard.connecting_sessions.erase(it_conn);
      total_connecting_sessions_--;
      was_connecting = true;
    }
    if (shard.draining_sessions.erase(session) > 0) {
      shard.draining_since.erase(session.get());
      total_draining_sessions_--;
      was_draining = true;
    }

    auto it_group = shard.connection_groups.find(peer_id);
    if (it_group != shard.connection_groups.end()) {
      group = it_group->second;
      group->idle_queue.Remove({session.get()});
      auto it_sess =
          std::find_if(group->sessions.begin(), group->sessions.end(),
                       [&session](SessionEntry const& entry) {
                         return entry.session == session;
                       });
      if (it_sess != group->sessions.end()) {
        group->sessions.erase(it_sess);
        group->active_sessions_count--;
        shard.ready_sessions_count.fetch_sub(1, std::memory_order_relaxed);
        total_ready_sessions_--;
        was_ready = true;
        UpdateSessionsSnapshotLocked(group.get());
      }
    } else {
      for (auto& [id, g] : shard.connection_groups) {
        if (g) g->idle_queue.Remove({session.get()});
      }
    }
  }

  RecordSessionCloseTelemetry(session, status, was_connecting, was_ready,
                              was_draining);

  if (was_ready || was_connecting || was_draining) {
    std::lock_guard<std::mutex> l_cv(pre_warm_mutex_);
    active_calls_condvar_.notify_all();
  }

  if (was_connecting || held_budget) {
    if (was_connecting || !status.ok()) {
      budget_.OnFailure();
    }
    std::lock_guard<std::mutex> l_pw(pre_warm_mutex_);
    auto it = pre_warm_sessions_.find(session);
    if (it != pre_warm_sessions_.end()) {
      pre_warm_sessions_.erase(it);
      if (pre_warm_sessions_.empty()) {
        pre_warm_condvar_.notify_all();
      }
    }
  }

  if (metrics_collector_) {
    metrics_collector_->SetActiveSessions(total_ready_sessions_.load());
  }

  std::deque<PendingCall> aborted_group_queue;

  if (group != nullptr) {
    std::lock_guard<std::mutex> l_shard(shard.mutex);
    if (group->sessions.empty()) {
      group->is_closed.store(true, std::memory_order_release);
      shard.connection_groups.erase(peer_id);
      UpdateGroupsSnapshotLocked(shard);

      aborted_group_queue = std::move(group->pending_queue);
      group->pending_queue.clear();
      int group_pending_count = group->pending_calls_count.exchange(0);
      shard.total_queued_calls -= group_pending_count;
      total_queued_calls_ -= group_pending_count;
      total_active_calls_ -= group_pending_count;
    }
  }

  // Fail or reschedule pending calls
  if (!aborted_group_queue.empty()) {
    bool can_reschedule =
        !is_closed_.load() && (total_ready_sessions_.load() > 0 ||
                               total_connecting_sessions_.load() > 0);
    if (!can_reschedule) {
      std::lock_guard<std::mutex> l_shard(shard.mutex);
      can_reschedule = !shard.connection_groups.empty();
    }
    if (can_reschedule) {
      for (auto& call : aborted_group_queue) {
        if (call.cancel_timer) {
          call.cancel_timer();
        }
        auto start_status =
            StartVirtualRpc(call.context, std::move(call.request),
                            std::move(call.callback), call.is_write_request);
        if (!start_status.ok()) {
          ::google::cloud::bigtable_internal::sessions::internal::
              LogVerboseWarning(
                  1, "SessionPoolImpl",
                  absl::StrCat("Failed to reschedule aborted call: ",
                               start_status.message()));
        }
      }
    } else {
      FailPendingCalls(
          aborted_group_queue,
          status.code() == google::cloud::StatusCode::kUnimplemented
              ? status
              : google::cloud::internal::UnavailableError(
                    absl::StrCat("Connection group ", peer_id, " is dead")));
    }
  }

  bool const is_terminal_handshake_error =
      was_connecting && IsTerminalHandshakeError(status.code());
  int consec_failures = 0;
  if (was_connecting) {
    consec_failures =
        consecutive_session_failures_.fetch_add(1, std::memory_order_acq_rel) +
        1;
  }
  bool const circuit_open =
      was_connecting &&
      consec_failures >= consecutive_session_failure_threshold_.load(
                             std::memory_order_relaxed);

  if (!is_terminal_handshake_error) {
    MaybeReplenishPool(shard);
  }

  if (was_connecting &&
      (is_terminal_handshake_error || circuit_open ||
       (total_connecting_sessions_.load() == 0 &&
        !replenish_timer_scheduled_.load(std::memory_order_acquire))) &&
      total_ready_sessions_.load() == 0) {
    std::deque<PendingCall> aborted_shard_queue;
    for (auto& s : shards_) {
      std::lock_guard<std::mutex> l_shard(s->mutex);
      if ((is_terminal_handshake_error || circuit_open ||
           (s->connecting_sessions.empty() &&
            !replenish_timer_scheduled_.load(std::memory_order_acquire))) &&
          s->connection_groups.empty() && !s->pending_queue.empty()) {
        for (auto& call : s->pending_queue) {
          aborted_shard_queue.push_back(std::move(call));
        }
        int shard_pending_count = static_cast<int>(s->pending_queue.size());
        s->pending_queue.clear();
        s->total_queued_calls -= shard_pending_count;
        total_queued_calls_ -= shard_pending_count;
        total_active_calls_ -= shard_pending_count;
      }
    }
    if (!aborted_shard_queue.empty()) {
      FailPendingCalls(aborted_shard_queue, status);
    }
  }
}

google::cloud::Status SessionPoolImpl::WaitForPreWarming(
    std::chrono::nanoseconds timeout) {
  std::unique_lock<std::mutex> l(pre_warm_mutex_);
  if (!pre_warm_started_) {
    return google::cloud::internal::FailedPreconditionError(
        "Pre-warming was not started");
  }

  auto const deadline = SessionImpl::Clock::now() + timeout;
  pre_warm_condvar_.wait_until(l, deadline,
                               [this] { return pre_warm_sessions_.empty(); });

  if (!pre_warm_sessions_.empty()) {
    return google::cloud::internal::DeadlineExceededError(absl::StrCat(
        "Pre-warming timed out. ", pre_warm_sessions_.size(),
        " sessions failed to handshake within ",
        std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count(),
        "ms"));
  }

  if (pre_warm_success_count_ < pre_warm_started_count_) {
    if (pre_warm_success_count_ == 0) {
      std::lock_guard<std::mutex> l_err(error_mutex_);
      if (!last_connection_error_.ok()) {
        return last_connection_error_;
      }
    }
    return google::cloud::internal::InternalError(absl::StrCat(
        "Pre-warming completed with failures. Started: ",
        pre_warm_started_count_, ", Succeeded: ", pre_warm_success_count_));
  }

  return google::cloud::Status();
}

google::cloud::Status SessionPoolImpl::CloseGracefully(
    std::chrono::nanoseconds timeout) {
  if (metrics_collector_) {
    metrics_collector_->Stop();
    metrics_collector_->SetPendingQueueSize(0);
    metrics_collector_->SetActiveSessions(0);
    metrics_collector_->SetActiveCalls(0);
  }

  is_closed_.store(true);
  {
    std::lock_guard<std::mutex> l_pw(pre_warm_mutex_);
    pre_warm_condvar_.notify_all();
  }

  std::vector<std::shared_ptr<SessionImpl>> sessions_to_drain;
  std::vector<std::shared_ptr<SessionImpl>> connecting_to_close;
  std::deque<PendingCall> aborted_queue;

  for (auto& shard : shards_) {
    std::lock_guard<std::mutex> l_shard(shard->mutex);
    shard->is_closed = true;

    for (auto& s : shard->connecting_sessions) {
      connecting_to_close.push_back(s);
    }

    for (auto& [id, group] : shard->connection_groups) {
      std::atomic_store(&group->sessions_ptr,
                        std::shared_ptr<std::vector<SessionEntry> const>());
      for (auto const& entry : group->sessions) {
        sessions_to_drain.push_back(entry.session);
        shard->draining_sessions.insert(entry.session);
        total_draining_sessions_++;
      }
      group->sessions.clear();
      group->idle_queue.Clear();
      group->active_sessions_count = 0;

      int group_aborted = group->pending_queue.size();
      for (auto& item : group->pending_queue) {
        if (item.cancel_timer) {
          item.cancel_timer();
        }
        aborted_queue.push_back(std::move(item));
      }
      group->pending_queue.clear();
      group->pending_calls_count = 0;

      shard->total_queued_calls -= group_aborted;
      total_queued_calls_ -= group_aborted;
      if (group_aborted > 0) {
        if (total_active_calls_.fetch_sub(group_aborted) == group_aborted) {
          std::lock_guard<std::mutex> l_cv(pre_warm_mutex_);
          active_calls_condvar_.notify_all();
        }
      }
    }
    shard->ready_sessions_count.store(0, std::memory_order_relaxed);

    int shard_aborted = shard->pending_queue.size();
    for (auto& item : shard->pending_queue) {
      if (item.cancel_timer) {
        item.cancel_timer();
      }
      aborted_queue.push_back(std::move(item));
    }
    shard->pending_queue.clear();
    shard->total_queued_calls -= shard_aborted;
    total_queued_calls_ -= shard_aborted;
    if (shard_aborted > 0) {
      if (total_active_calls_.fetch_sub(shard_aborted) == shard_aborted) {
        std::lock_guard<std::mutex> l_cv(pre_warm_mutex_);
        active_calls_condvar_.notify_all();
      }
    }
  }
  total_ready_sessions_.store(0);

  for (auto const& session : connecting_to_close) {
    if (session != nullptr) {
      session->Close();
    }
  }

  google::cloud::Status drop_err =
      google::cloud::internal::CancelledError("Session pool gracefully closed");
  google::cloud::internal::SetPayload(drop_err, kWriteStatusPayloadUrl, "0");
  FailPendingCalls(aborted_queue, drop_err);

  for (auto const& session : sessions_to_drain) {
    if (session != nullptr) {
      session->Drain();
    }
  }

  auto const deadline = SessionImpl::Clock::now() + timeout;

  bool timed_out = false;
  {
    std::unique_lock<std::mutex> l(pre_warm_mutex_);
    active_calls_condvar_.wait_until(l, deadline, [this] {
      return total_active_calls_.load() == 0 &&
             total_ready_sessions_.load() == 0 &&
             total_connecting_sessions_.load() == 0 &&
             total_draining_sessions_.load() == 0;
    });
    timed_out = total_active_calls_.load() > 0 ||
                total_ready_sessions_.load() > 0 ||
                total_connecting_sessions_.load() > 0 ||
                total_draining_sessions_.load() > 0;
  }

  if (timed_out) {
    std::vector<std::shared_ptr<SessionImpl>> sessions_to_force_close;

    for (auto& shard : shards_) {
      std::lock_guard<std::mutex> l_shard(shard->mutex);
      for (auto& [id, group] : shard->connection_groups) {
        for (auto& entry : group->sessions) {
          sessions_to_force_close.push_back(entry.session);
        }
        group->sessions.clear();
        group->active_sessions_count = 0;
        group->active_calls_count = 0;
      }
      for (auto& s : shard->connecting_sessions) {
        sessions_to_force_close.push_back(s);
      }
      shard->connecting_sessions.clear();
      for (auto const& s : shard->draining_sessions) {
        sessions_to_force_close.push_back(s);
      }
      shard->draining_sessions.clear();
      shard->draining_since.clear();
      shard->total_active_calls = 0;
      shard->ready_sessions_count.store(0, std::memory_order_relaxed);
    }
    total_draining_sessions_.store(0);

    for (auto const& session : sessions_to_force_close) {
      if (session != nullptr) {
        session->Close();
        RecordSessionCloseTelemetry(
            session, google::cloud::Status(),
            /*was_connecting=*/!session->ready_at().has_value(),
            /*was_ready=*/session->ready_at().has_value(),
            /*was_draining=*/false);
      }
    }
  }

  if (owns_executors_) {
    for (auto& shard : shards_) {
      if (shard->cq.has_value()) {
        shard->cq->CancelAll();
      }
    }
  }

  bool is_on_shard_cq = false;
  auto* current_cq = current_cq_impl;
  if (current_cq != nullptr) {
    for (auto const& shard : shards_) {
      if (shard->cq_impl == current_cq) {
        is_on_shard_cq = true;
        break;
      }
    }
  }
  if (!is_on_shard_cq) {
    std::unique_lock<std::mutex> l_cv(pre_warm_mutex_);
    while (active_lifecycle_callbacks_.load(std::memory_order_acquire) > 0) {
      active_calls_condvar_.wait_for(l_cv, std::chrono::milliseconds(10));
    }
  }

  if (timed_out) {
    return google::cloud::internal::DeadlineExceededError(
        "Graceful close timed out");
  }

  return google::cloud::Status();
}

void SessionPoolImpl::ReapIdleSessions() {
  SessionImpl::TimePoint now = SessionImpl::Clock::now();
  std::chrono::nanoseconds idle_timeout = idle_session_timeout_;
  size_t per_group_min = min_pool_size_;
  size_t global_min = pool_sizer_ != nullptr
                          ? static_cast<size_t>(pool_sizer_->min_sessions())
                          : 1;

  std::vector<std::shared_ptr<SessionImpl>> sessions_to_close;

  struct GroupInfo {
    std::shared_ptr<ConnectionGroup> group;
    std::vector<std::shared_ptr<SessionImpl>> active_ready;
    std::vector<std::shared_ptr<SessionImpl>> other;
    std::vector<std::shared_ptr<SessionImpl>> idle;
    Shard* shard;
  };

  std::vector<GroupInfo> groups;
  size_t total_sessions = 0;
  std::vector<std::shared_ptr<SessionImpl>> expired_draining_to_close;
  std::vector<std::pair<std::shared_ptr<SessionImpl>, bool>> uptime_samples;

  for (auto& shard : shards_) {
    std::lock_guard<std::mutex> l_shard(shard->mutex);
    if (shard->is_closed) continue;

    for (auto const& conn_sess : shard->connecting_sessions) {
      if (conn_sess) {
        uptime_samples.emplace_back(conn_sess, false);
      }
    }

    for (auto const& draining_sess : shard->draining_sessions) {
      if (!draining_sess) continue;
      uptime_samples.emplace_back(draining_sess, true);
      auto it_since = shard->draining_since.find(draining_sess.get());
      SessionImpl::TimePoint const drain_start =
          it_since != shard->draining_since.end() ? it_since->second : now;
      if ((now - drain_start) >= std::chrono::minutes(5)) {
        expired_draining_to_close.push_back(draining_sess);
      }
    }

    for (auto& [peer_id, group] : shard->connection_groups) {
      (void)peer_id;
      GroupInfo info{group, {}, {}, {}, shard.get()};
      for (auto& entry : group->sessions) {
        auto sess = entry.session;
        if (sess) {
          uptime_samples.emplace_back(sess, true);
        }
        total_sessions++;
        if (sess->IsIdle(now, idle_timeout)) {
          info.idle.push_back(sess);
        } else if (sess->state() == SessionImpl::State::kReady) {
          info.active_ready.push_back(sess);
        } else {
          info.other.push_back(sess);
        }
      }
      groups.push_back(std::move(info));
    }
  }

  for (auto const& [sess, is_ready] : uptime_samples) {
    if (!sess) continue;
    double const uptime_ms = std::max(
        0.0, std::chrono::duration<double, std::milli>(now - sess->created_at())
                 .count());
    telemetry::UptimeAttributes up_attrs;
    up_attrs.ready =
        (is_ready || sess->ready_at().has_value()) ? "true" : "false";
    if (auto pi = sess->peer_info(); pi.has_value()) {
      up_attrs.transport_type =
          transport::PeerInfo::TransportTypeToString(pi->transport_type);
      up_attrs.afe_location = pi->application_frontend_subzone;
    }
    session_tracer_.RecordSessionUptimeMs(uptime_ms, up_attrs);
  }

  std::vector<std::shared_ptr<SessionImpl>> candidates_to_reap;
  std::vector<std::shared_ptr<SessionImpl>> kept_idle_sessions;

  for (auto& info : groups) {
    size_t must_keep_in_group = info.active_ready.size() + info.other.size();
    if (must_keep_in_group < per_group_min) {
      size_t need_from_idle = per_group_min - must_keep_in_group;
      struct IdleCandidate {
        std::shared_ptr<SessionImpl> session;
        SessionImpl::TimePoint last_active;
      };
      std::vector<IdleCandidate> idle_candidates;
      idle_candidates.reserve(info.idle.size());
      for (auto const& sess : info.idle) {
        idle_candidates.push_back({sess, sess->GetLastActiveTime()});
      }
      std::sort(idle_candidates.begin(), idle_candidates.end(),
                [](IdleCandidate const& a, IdleCandidate const& b) {
                  return a.last_active > b.last_active;
                });

      if (need_from_idle < idle_candidates.size()) {
        for (size_t i = 0; i < need_from_idle; ++i) {
          kept_idle_sessions.push_back(idle_candidates[i].session);
        }
        for (size_t i = need_from_idle; i < idle_candidates.size(); ++i) {
          candidates_to_reap.push_back(idle_candidates[i].session);
        }
      } else {
        for (auto const& candidate : idle_candidates) {
          kept_idle_sessions.push_back(candidate.session);
        }
      }
    } else {
      candidates_to_reap.insert(candidates_to_reap.end(), info.idle.begin(),
                                info.idle.end());
    }
  }

  size_t current_projected_size =
      total_sessions > candidates_to_reap.size()
          ? total_sessions - candidates_to_reap.size()
          : 0;
  if (current_projected_size < global_min) {
    size_t need_to_keep_globally = global_min - current_projected_size;
    if (need_to_keep_globally < candidates_to_reap.size()) {
      struct ReapCandidate {
        std::shared_ptr<SessionImpl> session;
        SessionImpl::TimePoint last_active;
      };
      std::vector<ReapCandidate> reap_candidates;
      reap_candidates.reserve(candidates_to_reap.size());
      for (auto const& sess : candidates_to_reap) {
        reap_candidates.push_back({sess, sess->GetLastActiveTime()});
      }
      std::sort(reap_candidates.begin(), reap_candidates.end(),
                [](ReapCandidate const& a, ReapCandidate const& b) {
                  return a.last_active > b.last_active;
                });

      for (size_t i = 0; i < need_to_keep_globally; ++i) {
        kept_idle_sessions.push_back(reap_candidates[i].session);
      }
      for (size_t i = need_to_keep_globally; i < reap_candidates.size(); ++i) {
        sessions_to_close.push_back(reap_candidates[i].session);
      }
    } else {
      kept_idle_sessions.insert(kept_idle_sessions.end(),
                                candidates_to_reap.begin(),
                                candidates_to_reap.end());
    }
  } else {
    sessions_to_close.insert(sessions_to_close.end(),
                             candidates_to_reap.begin(),
                             candidates_to_reap.end());
  }

  std::unordered_set<SessionImpl*> reaped_set;
  for (auto const& sess : sessions_to_close) {
    reaped_set.insert(sess.get());
  }

  std::vector<std::shared_ptr<SessionImpl>> verified_sessions_to_close;

  for (auto& info : groups) {
    std::lock_guard<std::mutex> l_shard(info.shard->mutex);
    if (info.shard->is_closed) continue;
    std::vector<SessionEntry> new_group_sessions;
    std::unordered_set<SessionImpl*> group_reaped_set;
    bool removed_any = false;
    for (auto& entry : info.group->sessions) {
      bool can_reap = false;
      if (entry.session != nullptr &&
          reaped_set.find(entry.session.get()) != reaped_set.end() &&
          entry.session->state() == SessionImpl::State::kReady &&
          entry.session->IsIdle(now, idle_timeout) &&
          entry.session->Acquire()) {
        if ((now - entry.session->GetLastActiveTime()) > idle_timeout) {
          can_reap = true;
        } else {
          entry.session->FinalizeActiveRpc(0);
        }
      }
      if (!can_reap) {
        new_group_sessions.push_back(entry);
      } else {
        auto it_h = info.shard->session_handles.find(entry.session.get());
        if (it_h != info.shard->session_handles.end()) {
          info.shard->session_list.OnSessionClosing(it_h->second);
        }
        info.group->active_sessions_count--;
        info.shard->ready_sessions_count.fetch_sub(1,
                                                   std::memory_order_relaxed);
        total_ready_sessions_--;
        info.shard->draining_sessions.insert(entry.session);
        info.shard->draining_since[entry.session.get()] = now;
        total_draining_sessions_++;
        group_reaped_set.insert(entry.session.get());
        verified_sessions_to_close.push_back(entry.session);
        removed_any = true;
      }
    }
    info.group->sessions = std::move(new_group_sessions);
    if (removed_any) {
      info.group->idle_queue.Remove(group_reaped_set);
      UpdateSessionsSnapshotLocked(info.group.get());
      if (info.group->sessions.empty()) {
        info.group->is_closed.store(true, std::memory_order_release);
        while (!info.group->pending_queue.empty()) {
          info.shard->pending_queue.push_back(
              std::move(info.group->pending_queue.front()));
          info.group->pending_queue.pop_front();
        }
        info.group->pending_calls_count.store(0, std::memory_order_release);
        info.shard->connection_groups.erase(info.group->peer_id);
        UpdateGroupsSnapshotLocked(*info.shard);
      }
    }
  }

  for (auto& sess : expired_draining_to_close) {
    if (sess != nullptr) {
      sess->Close();
    }
  }

  for (auto& sess : verified_sessions_to_close) {
    ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
        2, "SessionPoolImpl", "Reaping idle session.");
    sess->Close();
    if (sess->active_rpc_count() > 0) {
      sess->FinalizeActiveRpc(0);
    }
  }

  if (!verified_sessions_to_close.empty() && metrics_collector_) {
    metrics_collector_->SetActiveSessions(total_ready_sessions_.load());
  }
}

void SessionPoolImpl::RecordSessionCloseTelemetry(
    std::shared_ptr<SessionImpl> const& session,
    google::cloud::Status const& status, bool was_connecting, bool was_ready,
    bool was_draining) {
  if (session == nullptr || !session->TryMarkDurationRecorded()) {
    return;
  }
  SessionImpl::TimePoint const close_now = SessionImpl::Clock::now();
  double const duration_ms =
      std::max(0.0, std::chrono::duration<double, std::milli>(
                        close_now - session->created_at())
                        .count());
  std::string transport_type_str;
  std::string afe_location_str;
  if (auto pi = session->peer_info(); pi.has_value()) {
    transport_type_str =
        transport::PeerInfo::TransportTypeToString(pi->transport_type);
    afe_location_str = pi->application_frontend_subzone;
  }
  std::string const status_str = StatusCodeToString(status.code());
  if (was_connecting && !session->ready_at().has_value()) {
    telemetry::OpenLatencyAttributes open_attrs;
    open_attrs.transport_type = transport_type_str;
    open_attrs.status = status_str;
    open_attrs.afe_location = afe_location_str;
    session_tracer_.RecordSessionOpenLatencyMs(duration_ms, open_attrs);
  }
  telemetry::DurationAttributes dur_attrs;
  dur_attrs.transport_type = transport_type_str;
  dur_attrs.status = status_str;
  dur_attrs.afe_location = afe_location_str;
  dur_attrs.closing_reason = session->close_reason_str();
  dur_attrs.vrpcs = session->vrpcs_summary();
  dur_attrs.ready =
      (session->ready_at().has_value() || was_ready || was_draining) ? "true"
                                                                     : "false";
  session_tracer_.RecordSessionDurationMs(duration_ms, dur_attrs);
}

void SessionPoolImpl::ScheduleReaper() {
  if (scheduler_ == nullptr) return;

  scheduler_(std::chrono::minutes(1), [weak_self = weak_from_this()]() {
    if (auto self = weak_self.lock()) {
      if (!self->is_closed_.load()) {
        self->ReapIdleSessions();
        self->ScheduleReaper();
      }
    }
  });
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
