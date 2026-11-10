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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_SESSION_POOL_IMPL_TEST_HELPERS_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_SESSION_POOL_IMPL_TEST_HELPERS_H

#include "google/cloud/bigtable/internal/sessions/latch.h"
#include "google/cloud/bigtable/internal/sessions/pool/metrics_collector.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_pool_impl.h"
#include "google/cloud/bigtable/internal/sessions/session/session_impl.h"
#include "google/cloud/bigtable/internal/sessions/session/write_status.h"
#include "google/cloud/bigtable/internal/sessions/sessions_options.h"
#include "google/cloud/internal/completion_queue_impl.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/options.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/cloud/testing_util/mock_completion_queue_impl.h"
#include <google/bigtable/v2/session.pb.h>
#include <google/protobuf/duration.pb.h>
#include <gmock/gmock.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
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

// Test-only helpers shared by session_pool_impl_test.cc and
// session_pool_lifecycle_test.cc.

class SessionPoolImplTestPeer {
 public:
  static void SetLastConnectionErrorForTesting(SessionPoolImpl& pool,
                                               google::cloud::Status status) {
    std::lock_guard<std::mutex> l(pool.error_mutex_);
    pool.last_connection_error_ = status;
  }

  static std::shared_ptr<SessionPoolImpl::ConnectionGroup>
  GetOrCreateGroupLocked(SessionPoolImpl& pool, SessionPoolImpl::Shard& shard,
                         std::string const& peer_id) {
    auto it = shard.connection_groups.find(peer_id);
    if (it != shard.connection_groups.end()) {
      return it->second;
    }
    auto group = std::make_shared<SessionPoolImpl::ConnectionGroup>();
    group->peer_id = peer_id;
    shard.connection_groups[peer_id] = group;
    pool.UpdateGroupsSnapshotLocked(shard);
    return group;
  }

  static void AddGlobalPendingCall(
      SessionPoolImpl& pool, grpc::ClientContext* context,
      google::bigtable::v2::VirtualRpcRequest request,
      SessionPoolImpl::GenericCallback callback) {
    auto& shard = *pool.shards_[0];
    std::lock_guard<std::mutex> l(shard.mutex);
    shard.pending_queue.push_back(
        {context, std::move(request), std::move(callback)});
    shard.total_queued_calls++;
    pool.total_queued_calls_++;
    pool.total_active_calls_++;
  }

  static std::shared_ptr<SessionPoolImpl::ConnectionGroup> GetOrCreateGroup(
      SessionPoolImpl& pool, std::string const& peer_id) {
    auto& shard = *pool.shards_[0];
    std::lock_guard<std::mutex> l(shard.mutex);
    return GetOrCreateGroupLocked(pool, shard, peer_id);
  }

  static void AddGroupPendingCall(
      SessionPoolImpl& pool, std::string const& peer_id,
      grpc::ClientContext* context,
      google::bigtable::v2::VirtualRpcRequest request,
      SessionPoolImpl::GenericCallback callback) {
    auto& shard = *pool.shards_[0];
    std::lock_guard<std::mutex> l(shard.mutex);
    auto group = GetOrCreateGroupLocked(pool, shard, peer_id);
    group->pending_queue.push_back(
        {context, std::move(request), std::move(callback)});
    group->pending_calls_count++;
    shard.total_queued_calls++;
    pool.total_queued_calls_++;
    pool.total_active_calls_++;
  }

  static void SetPreWarmStarted(SessionPoolImpl& pool, bool started) {
    std::lock_guard<std::mutex> l(pool.pre_warm_mutex_);
    pool.pre_warm_started_ = started;
  }
  static void CloseGroup(SessionPoolImpl& pool, std::string const& peer_id) {
    auto& shard = *pool.shards_[0];
    std::lock_guard<std::mutex> l(shard.mutex);
    auto group = GetOrCreateGroupLocked(pool, shard, peer_id);
    group->is_closed = true;
  }
  static void ConfigureMockStubsForLoadBalancing(SessionPoolImpl& pool,
                                                 int count) {
    pool.stubs_.clear();
    for (int i = 0; i < count; ++i) {
      pool.stubs_.push_back(
          reinterpret_cast<google::bigtable::v2::Bigtable::Stub*>(0x1000 +
                                                                  i * 0x10));
    }
    pool.active_calls_per_stub_ = std::vector<std::atomic<int>>(count);
  }
  static void SetPickerToLeastInFlight(SessionPoolImpl& pool) {
    google::bigtable::v2::SessionClientConfiguration::SessionPoolConfiguration
        config;
    config.mutable_load_balancing_options()->mutable_least_in_flight();
    pool.picker_.UpdateConfig(config);
  }
  static void SetBudgetLimit(SessionPoolImpl& pool, int limit) {
    google::bigtable::v2::SessionClientConfiguration::SessionPoolConfiguration
        config;
    config.set_new_session_creation_budget(limit);
    pool.budget_.UpdateConfig(config);
  }
  static void SetBudgetConfig(SessionPoolImpl& pool, int limit,
                              std::chrono::nanoseconds penalty) {
    google::bigtable::v2::SessionClientConfiguration::SessionPoolConfiguration
        config;
    config.set_new_session_creation_budget(limit);
    auto* penalty_proto = config.mutable_new_session_creation_penalty();
    penalty_proto->set_seconds(
        std::chrono::duration_cast<std::chrono::seconds>(penalty).count());
    penalty_proto->set_nanos(static_cast<int32_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            penalty % std::chrono::seconds(1))
            .count()));
    pool.budget_.UpdateConfig(config);
  }
  static bool TryReserveBudget(SessionPoolImpl& pool) {
    return pool.budget_.TryReserve();
  }
  static void AddSessionToShard(SessionPoolImpl& pool, size_t shard_idx,
                                std::shared_ptr<SessionImpl> session,
                                std::string const& peer_id) {
    auto& shard = *pool.shards_[shard_idx];
    std::lock_guard<std::mutex> l(shard.mutex);
    if (session != nullptr) {
      auto [it, inserted] =
          shard.session_handles.try_emplace(session.get(), nullptr);
      if (inserted) {
        auto handle = shard.session_list.NewStartingHandle(
            reinterpret_cast<std::int64_t>(session.get()), shard_idx);
        shard.session_list.OnSessionReady(handle, 0);
        session->set_pool_handle(handle);
        it->second = std::move(handle);
      }
    }
    auto group = GetOrCreateGroupLocked(pool, shard, peer_id);
    group->sessions.push_back(SessionEntry{session});
    group->active_sessions_count++;
    shard.ready_sessions_count.fetch_add(1, std::memory_order_relaxed);
    pool.total_ready_sessions_++;
  }
  static void* GetShardAddressForCurrentThread(SessionPoolImpl& pool) {
    return &pool.GetShardForCurrentThread();
  }
  static void DebugPrintGroups(SessionPoolImpl& pool) {
    for (auto const& shard : pool.shards_) {
      std::lock_guard<std::mutex> l(shard->mutex);
      for (auto const& [id, group] : shard->connection_groups) {
        (void)id;
        (void)group;
      }
    }
  }
  static std::vector<std::shared_ptr<SessionImpl>> GetConnectingSessions(
      SessionPoolImpl& pool) {
    std::vector<std::shared_ptr<SessionImpl>> sessions;
    for (auto const& shard : pool.shards_) {
      std::lock_guard<std::mutex> l(shard->mutex);
      sessions.insert(sessions.end(), shard->connecting_sessions.begin(),
                      shard->connecting_sessions.end());
    }
    return sessions;
  }
  static std::vector<google::cloud::CompletionQueue> GetEventLoopCqs(
      SessionPoolImpl& pool) {
    std::vector<google::cloud::CompletionQueue> cqs;
    if (pool.executors_) {
      for (std::size_t i = 0; i < pool.executors_->num_event_loops(); ++i) {
        cqs.push_back(pool.executors_->cq(i));
      }
    }
    return cqs;
  }
  static size_t GetGroupPendingQueueSize(SessionPoolImpl& pool,
                                         std::string const& peer_id) {
    auto& shard = *pool.shards_[0];
    std::lock_guard<std::mutex> l(shard.mutex);
    auto it = shard.connection_groups.find(peer_id);
    if (it != shard.connection_groups.end()) {
      return it->second->pending_queue.size();
    }
    return 0;
  }

  static void RecordReadLatency(SessionPoolImpl& pool,
                                std::chrono::nanoseconds latency) {
    pool.read_latency_tracker_.OnRequestComplete(latency);
  }
  static void RecordWriteLatency(SessionPoolImpl& pool,
                                 std::chrono::nanoseconds latency) {
    pool.write_latency_tracker_.OnRequestComplete(latency);
  }
  static size_t GetSessionsHoldingBudgetCount(SessionPoolImpl& pool) {
    std::lock_guard<std::mutex> l(pool.budget_holding_mutex_);
    return pool.sessions_holding_budget_.size();
  }
  static void ResetLatencyTrackers(SessionPoolImpl& pool) {
    pool.read_latency_tracker_.Reset();
    pool.write_latency_tracker_.Reset();
  }
  static int GetActiveCallsForStubIndex(SessionPoolImpl& pool, size_t idx) {
    return pool.active_calls_per_stub_[idx].load();
  }
  static google::bigtable::v2::Bigtable::Stub* GetStub(SessionPoolImpl& pool,
                                                       size_t idx) {
    return pool.stubs_[idx];
  }
  static void SetActiveCallsForStubIndex(SessionPoolImpl& pool, size_t idx,
                                         int val) {
    pool.active_calls_per_stub_[idx].store(val);
  }
  static DynamicPicker& GetPicker(SessionPoolImpl& pool) {
    return pool.picker_;
  }
  static void ClearGroupsSnapshotOnShard0(SessionPoolImpl& pool) {
    auto& shard = *pool.shards_[0];
    std::atomic_store(
        &shard.groups_snapshot,
        std::shared_ptr<std::vector<
            std::shared_ptr<SessionPoolImpl::ConnectionGroup>> const>());
  }
  static void RegisterConnectingSessionOnShard(
      SessionPoolImpl& pool, size_t shard_idx,
      std::shared_ptr<SessionImpl> const& session) {
    auto& shard = *pool.shards_[shard_idx];
    std::lock_guard<std::mutex> l(shard.mutex);
    auto handle = shard.session_list.NewStartingHandle(
        reinterpret_cast<std::int64_t>(session.get()), shard_idx);
    session->set_pool_handle(handle);
    shard.session_handles[session.get()] = std::move(handle);
    shard.connecting_sessions.push_back(session);
    pool.total_connecting_sessions_++;
  }
};

class SessionImplTestPeer {
 public:
  static std::optional<google::cloud::CompletionQueue> GetEventCq(
      SessionImpl& session) {
    return session.event_cq_;
  }
};

class MockSessionImplForPool : public SessionImpl {
 public:
  MockSessionImplForPool() = default;
  using SessionImpl::StartVirtualRpc;

  void StartVirtualRpc(grpc::ClientContext* context,
                       ::google::bigtable::v2::VirtualRpcRequest request,
                       GenericCallback callback,
                       bool is_write_request) override {
    context_ = context;
    last_request_ = std::move(request);
    callback_ = std::move(callback);
    is_busy_.store(true);
    start_count_++;
    GetLatencyTracker()->OnRequestStart();
    if (block_start_ != nullptr) {
      block_start_->Wait();
    }
    if (!next_start_status_.ok()) {
      is_busy_.store(false);
      active_calls_--;
      GetLatencyTracker()->OnRequestComplete(std::chrono::nanoseconds(0));
      if (callback_) {
        callback_(next_start_status_);
      }
    }
  }

  void CompleteRpc(
      int64_t rpc_id, std::chrono::nanoseconds latency,
      google::cloud::StatusOr<::google::bigtable::v2::VirtualRpcResponse>
          response) {
    is_busy_.store(false);
    context_ = nullptr;
    if (active_calls_ > 0) {
      active_calls_--;
    }
    GetLatencyTracker()->OnRequestComplete(latency);

    GenericCallback cb = std::move(callback_);
    if (cb) {
      cb(response);
    }

    if (is_draining_.load() && active_calls_.load() == 0) {
      is_closed_.store(true);
      if (auto l = listener().lock()) {
        l->OnSessionClosed(shared_from_this(),
                           google::cloud::internal::CancelledError("closed"));
      }
    }
  }

  void CancelVirtualRpc(int64_t rpc_id) override {
    is_cancelled_.store(true);
    is_busy_.store(false);
    context_ = nullptr;
    if (active_calls_ > 0) {
      active_calls_--;
    }

    GenericCallback cb = std::move(callback_);
    if (cb) {
      cb(google::cloud::internal::CancelledError("cancelled"));
    }

    if (is_draining_.load() && active_calls_.load() == 0) {
      is_closed_.store(true);
      if (auto l = listener().lock()) {
        l->OnSessionClosed(shared_from_this(),
                           google::cloud::internal::CancelledError("closed"));
      }
    }
  }

  void Close() override {
    is_closed_.store(true);
    context_ = nullptr;

    GenericCallback cb = std::move(callback_);
    if (cb) {
      cb(google::cloud::internal::CancelledError("closed"));
    }

    if (auto l = listener().lock()) {
      l->OnSessionClosed(shared_from_this(),
                         google::cloud::internal::CancelledError("closed"));
    }
  }

  void Drain() override {
    is_draining_.store(true);
    if (active_calls_.load() == 0) {
      is_closed_.store(true);
      if (auto l = listener().lock()) {
        l->OnSessionClosed(shared_from_this(),
                           google::cloud::internal::CancelledError("closed"));
      }
    }
  }

  State state() const override { return state_; }
  bool HasActiveContext(grpc::ClientContext* context) const override {
    return context_ == context;
  }
  void CancelRpcByContext(grpc::ClientContext* context) override {
    if (context_ == context) {
      CancelVirtualRpc(last_request_.rpc_id());
    }
  }
  bool IsBusy() const override {
    if (is_busy_.load()) return true;
    if (is_write_congested_.load()) return true;
    return active_calls_.load() >= 1;
  }

  bool IsWriteCongested() const override { return is_write_congested_.load(); }
  bool IsIdle(SessionImpl::TimePoint now,
              std::chrono::nanoseconds timeout) const override {
    return is_idle_.load();
  }
  SessionImpl::TimePoint GetLastActiveTime() const override {
    return last_active_time_;
  }

  std::optional<std::string> GetPeerId() const override { return peer_id_; }

  int active_rpc_count() const override { return active_calls_.load(); }

  bool Acquire() override {
    int expected = 0;
    return active_calls_.compare_exchange_strong(expected, 1);
  }

  void FinalizeActiveRpc(int64_t rpc_id) override {
    SessionImpl::FinalizeActiveRpc(rpc_id);
    finalize_count_++;
    if (!finalize_done_.HasBeenNotified()) {
      finalize_done_.Notify();
    }
  }

  void SetListener(std::weak_ptr<SessionListener> listener) {
    SessionImpl::SetListener(listener);
  }

  std::atomic<int> finalize_count_{0};
  Latch finalize_done_;

  State state_ = State::kReady;
  std::atomic<bool> is_busy_{false};
  std::atomic<bool> is_cancelled_{false};
  std::atomic<int> active_calls_{0};
  google::cloud::Status next_start_status_ = google::cloud::Status();
  ::google::bigtable::v2::VirtualRpcRequest last_request_;
  std::atomic<int> start_count_{0};
  std::atomic<bool> is_closed_{false};
  std::atomic<bool> is_draining_{false};
  std::optional<std::string> peer_id_;
  std::optional<std::string>& afe_id_ = peer_id_;
  Latch* block_start_ = nullptr;
  GenericCallback callback_;
  grpc::ClientContext* context_ = nullptr;

  std::atomic<bool> is_write_congested_{false};
  std::atomic<bool> is_idle_{false};
  SessionImpl::TimePoint last_active_time_ = SessionImpl::TimePoint{};

  google::bigtable::v2::Bigtable::Stub* stub() const override {
    return mock_stub_;
  }
  google::bigtable::v2::Bigtable::Stub* mock_stub_ = nullptr;
};

class SessionPoolImplTest : public ::testing::Test {
 protected:
  void SetUp() override {
    sess1_ =
        std::shared_ptr<MockSessionImplForPool>(new MockSessionImplForPool());
    sess2_ =
        std::shared_ptr<MockSessionImplForPool>(new MockSessionImplForPool());
    pool_ = SessionPoolImpl::CreateForTesting({sess1_, sess2_}, 2);
    sess1_->SetListener(pool_);
    sess2_->SetListener(pool_);
    SessionPoolImplTestPeer::SetPickerToLeastInFlight(*pool_);
  }

  std::shared_ptr<MockSessionImplForPool> sess1_;
  std::shared_ptr<MockSessionImplForPool> sess2_;
  std::shared_ptr<SessionPoolImpl> pool_;
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_SESSION_POOL_IMPL_TEST_HELPERS_H
