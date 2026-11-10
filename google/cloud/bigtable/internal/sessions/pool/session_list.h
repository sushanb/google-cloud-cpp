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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_SESSION_LIST_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_SESSION_LIST_H

#include "google/cloud/bigtable/internal/sessions/session/session_state.h"
#include "google/cloud/status.h"
#include "google/cloud/version.h"
#include <google/bigtable/v2/request_stats.pb.h>
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace pool {

/// OK-gated Peak-EWMA latency tracker for backend connection groups.
///
/// Seeds `e2e_ewma` at 1.0ms (1000us) and `transport_ewma` at 0.5ms (500us).
/// Updates only on `status.ok()` so fast-failing peers never appear near-zero
/// latency, and subtracts client-side pool checkout wait (`pool_wait_us`).
class OkGatedPeakEwma {
 public:
  static constexpr double kDefaultE2eSeedUs = 1000.0;       // 1ms
  static constexpr double kDefaultTransportSeedUs = 500.0;  // 500us

  explicit OkGatedPeakEwma(double alpha = 0.2)
      : alpha_(alpha),
        e2e_ewma_us_(kDefaultE2eSeedUs),
        transport_ewma_us_(kDefaultTransportSeedUs) {}

  OkGatedPeakEwma(OkGatedPeakEwma const& other)
      : alpha_(other.alpha_),
        e2e_ewma_us_(other.e2e_ewma_us_.load(std::memory_order_relaxed)),
        transport_ewma_us_(
            other.transport_ewma_us_.load(std::memory_order_relaxed)) {}

  OkGatedPeakEwma& operator=(OkGatedPeakEwma const& other) {
    if (this != &other) {
      alpha_ = other.alpha_;
      e2e_ewma_us_.store(other.e2e_ewma_us_.load(std::memory_order_relaxed),
                         std::memory_order_relaxed);
      transport_ewma_us_.store(
          other.transport_ewma_us_.load(std::memory_order_relaxed),
          std::memory_order_relaxed);
    }
    return *this;
  }

  void RecordOutcome(Status const& status, double total_elapsed_us,
                     double pool_wait_us, double backend_latency_us) {
    if (!status.ok()) {
      // Non-OK responses must not update the latency EWMA so fast-failing peers
      // are not favored by the picker.
      return;
    }
    double const wire_e2e_us = (std::max)(0.0, total_elapsed_us - pool_wait_us);
    double const transport_us =
        (std::max)(0.0, wire_e2e_us - backend_latency_us);

    double const cur_e2e = e2e_ewma_us_.load(std::memory_order_relaxed);
    double const next_e2e =
        (wire_e2e_us > cur_e2e)
            ? wire_e2e_us
            : (alpha_ * wire_e2e_us + (1.0 - alpha_) * cur_e2e);
    e2e_ewma_us_.store(next_e2e, std::memory_order_relaxed);

    double const cur_trans = transport_ewma_us_.load(std::memory_order_relaxed);
    double const next_trans =
        (transport_us > cur_trans)
            ? transport_us
            : (alpha_ * transport_us + (1.0 - alpha_) * cur_trans);
    transport_ewma_us_.store(next_trans, std::memory_order_relaxed);
  }

  double e2e_ewma_us() const {
    return e2e_ewma_us_.load(std::memory_order_relaxed);
  }
  double transport_ewma_us() const {
    return transport_ewma_us_.load(std::memory_order_relaxed);
  }

 private:
  double alpha_;
  std::atomic<double> e2e_ewma_us_;
  std::atomic<double> transport_ewma_us_;
};

/// Two-tier per-peer session registry and creation-budget reservation tracker.
class SessionList {
 public:
  enum class HandleState {
    kNotRegistered = 0,
    kIdle = 1,
    kInFlight = 2,
    kClosing = 3,
    kClosed = 4,
  };

  struct SessionHandle {
    std::int64_t session_id = 0;
    std::int64_t peer_id = 0;
    std::size_t shard_index = 0;
    std::atomic<HandleState> state{HandleState::kNotRegistered};
    std::atomic<bool> in_expected_count{false};
    std::atomic<bool> counted_in_group_in_flight{false};
  };

  struct ConnectionGroupHandle {
    std::int64_t peer_id = 0;
    std::size_t ref_count = 0;
    std::size_t in_flight_count = 0;
    std::deque<std::shared_ptr<SessionHandle>> idle_sessions;
    OkGatedPeakEwma ewma;
  };
  using AfeHandle = ConnectionGroupHandle;

  /// Lock-free fast-path transition (`kIdle -> kInFlight`) used when a session
  /// is acquired directly from the RCU snapshot without locking `shard.mutex`
  /// or `SessionList::mu_`.
  static void MarkHandleInFlightFast(SessionHandle* handle) {
    if (!handle) return;
    HandleState expected = HandleState::kIdle;
    handle->state.compare_exchange_strong(expected, HandleState::kInFlight,
                                          std::memory_order_acq_rel);
  }

  /// Lock-free fast-path transition (`kInFlight -> kIdle`) verifying that
  /// `in_expected_count` is still true and `state == kInFlight`.
  static bool ReleaseToPoolFast(SessionHandle* handle) {
    if (!handle || !handle->in_expected_count.load(std::memory_order_acquire)) {
      return false;
    }
    HandleState expected = HandleState::kInFlight;
    return handle->state.compare_exchange_strong(expected, HandleState::kIdle,
                                                 std::memory_order_acq_rel);
  }

  /// Registers a new starting handle and reserves a creation budget slot
  /// tracked in `sessions_holding_budget_`.
  std::shared_ptr<SessionHandle> NewStartingHandle(
      std::int64_t session_id, std::size_t shard_index = 0) {
    std::lock_guard<std::mutex> lock(mu_);
    auto handle = std::make_shared<SessionHandle>();
    handle->session_id = session_id;
    handle->shard_index = shard_index;
    handle->state.store(HandleState::kNotRegistered, std::memory_order_relaxed);
    handle->in_expected_count.store(true, std::memory_order_relaxed);
    expected_capacity_++;
    sessions_holding_budget_.insert(handle);
    return handle;
  }

  /// Releases the creation budget reservation on handshake completion, binds
  /// `peer_id` (from `PeerInfo`), increments `group.ref_count`, and enqueues
  /// the handle into `group.idle_sessions`.
  void OnSessionReady(std::shared_ptr<SessionHandle> const& handle,
                      std::int64_t peer_id) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!handle) return;
    auto const st = handle->state.load(std::memory_order_acquire);
    if (st == HandleState::kClosed || st == HandleState::kClosing) {
      sessions_holding_budget_.erase(handle);
      return;
    }
    sessions_holding_budget_.erase(handle);
    handle->peer_id = peer_id;
    handle->state.store(HandleState::kIdle, std::memory_order_release);
    auto& group = connection_groups_[peer_id];
    group.peer_id = peer_id;
    group.ref_count++;
    group.idle_sessions.push_back(handle);
    ready_count_++;
  }

  /// Checks out an idle session for `peer_id` (`kIdle -> kInFlight`).
  std::shared_ptr<SessionHandle> CheckoutFromPeer(std::int64_t peer_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = connection_groups_.find(peer_id);
    if (it == connection_groups_.end() || it->second.idle_sessions.empty()) {
      return nullptr;
    }
    auto handle = it->second.idle_sessions.front();
    it->second.idle_sessions.pop_front();
    handle->state.store(HandleState::kInFlight, std::memory_order_release);
    handle->counted_in_group_in_flight.store(true, std::memory_order_release);
    it->second.in_flight_count++;
    ready_count_--;
    return handle;
  }

  std::shared_ptr<SessionHandle> CheckoutFromAfe(std::int64_t peer_id) {
    return CheckoutFromPeer(peer_id);
  }

  /// Marks a specific handle as `kInFlight` when checked out from a shard's
  /// lock-free or idle queue (`kIdle -> kInFlight`).
  void MarkHandleInFlight(std::shared_ptr<SessionHandle> const& handle) {
    MarkHandleInFlightFast(handle.get());
  }

  std::unordered_map<std::int64_t, ConnectionGroupHandle>
  GetConnectionGroupsSnapshot() const {
    std::lock_guard<std::mutex> lock(mu_);
    auto copy = connection_groups_;
    for (auto& [peer_id, cg] : copy) {
      (void)peer_id;
      std::deque<std::shared_ptr<SessionHandle>> still_idle;
      for (auto const& h : cg.idle_sessions) {
        if (!h) continue;
        auto const st = h->state.load(std::memory_order_acquire);
        if (st == HandleState::kInFlight) {
          cg.in_flight_count++;
        } else if (st == HandleState::kIdle) {
          still_idle.push_back(h);
        }
      }
      cg.idle_sessions = std::move(still_idle);
    }
    return copy;
  }

  /// Returns a completed in-flight session to its connection group's idle
  /// queue (`kInFlight -> kIdle`).
  ///
  /// Verifies both `handle->in_expected_count` and `handle->state ==
  /// kInFlight`. If `OnSessionClosing` (`GOAWAY`) occurred while the session
  /// was in flight, `in_expected_count` is `false` and the session is not
  /// re-added to `group.idle_sessions` (preventing a `WaitServerClose` retry
  /// storm).
  bool ReleaseToPool(std::shared_ptr<SessionHandle> const& handle) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!handle) return false;
    auto it = connection_groups_.find(handle->peer_id);
    if (handle->counted_in_group_in_flight.exchange(
            false, std::memory_order_acq_rel)) {
      if (it != connection_groups_.end() && it->second.in_flight_count > 0) {
        it->second.in_flight_count--;
      }
    }
    if (!handle->in_expected_count.load(std::memory_order_acquire) ||
        handle->state.load(std::memory_order_acquire) !=
            HandleState::kInFlight) {
      return false;
    }
    handle->state.store(HandleState::kIdle, std::memory_order_release);
    if (it != connection_groups_.end()) {
      auto& q = it->second.idle_sessions;
      if (std::find(q.begin(), q.end(), handle) == q.end()) {
        q.push_back(handle);
        ready_count_++;
      }
      return true;
    }
    return false;
  }

  /// Transitions a session to `kClosing` (e.g. when `GOAWAY` is received):
  /// removes the session from `idle_sessions` if currently idle, marks
  /// `in_expected_count = false`, and decrements `expected_capacity_`.
  ///
  /// Does not decrement `group.ref_count` here (only `OnSessionClosed`
  /// decrements `group.ref_count`), preventing spurious scale-up bursts during
  /// the `GOAWAY` drain window.
  void OnSessionClosing(std::shared_ptr<SessionHandle> const& handle) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!handle) return;
    auto const st = handle->state.load(std::memory_order_acquire);
    if (st == HandleState::kClosed || st == HandleState::kClosing) {
      return;
    }
    if (handle->in_expected_count.exchange(false, std::memory_order_acq_rel)) {
      if (expected_capacity_ > 0) expected_capacity_--;
    }
    auto it = connection_groups_.find(handle->peer_id);
    if (it != connection_groups_.end()) {
      if (handle->counted_in_group_in_flight.exchange(
              false, std::memory_order_acq_rel) &&
          it->second.in_flight_count > 0) {
        it->second.in_flight_count--;
      }
      auto& q = it->second.idle_sessions;
      auto const old_size = q.size();
      q.erase(std::remove(q.begin(), q.end(), handle), q.end());
      if (q.size() < old_size && ready_count_ > 0) ready_count_--;
    } else {
      handle->counted_in_group_in_flight.store(false,
                                               std::memory_order_release);
    }
    handle->state.store(HandleState::kClosing, std::memory_order_release);
  }

  /// Unconditionally releases any held creation budget slot
  /// (`sessions_holding_budget_`), removes the session from `idle_sessions` if
  /// needed, and decrements `group.ref_count`.
  void OnSessionClosed(std::shared_ptr<SessionHandle> const& handle) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!handle) return;
    auto const st = handle->state.load(std::memory_order_acquire);
    if (st == HandleState::kClosed) {
      return;
    }
    bool const was_registered = (st != HandleState::kNotRegistered);
    sessions_holding_budget_.erase(handle);
    if (handle->in_expected_count.exchange(false, std::memory_order_acq_rel)) {
      if (expected_capacity_ > 0) expected_capacity_--;
    }
    auto it = connection_groups_.find(handle->peer_id);
    if (it != connection_groups_.end()) {
      if (handle->counted_in_group_in_flight.exchange(
              false, std::memory_order_acq_rel) &&
          it->second.in_flight_count > 0) {
        it->second.in_flight_count--;
      }
      auto& q = it->second.idle_sessions;
      auto const old_size = q.size();
      q.erase(std::remove(q.begin(), q.end(), handle), q.end());
      if (q.size() < old_size && ready_count_ > 0) ready_count_--;
    } else {
      handle->counted_in_group_in_flight.store(false,
                                               std::memory_order_release);
    }
    if (was_registered && it != connection_groups_.end() &&
        it->second.ref_count > 0) {
      it->second.ref_count--;
      if (it->second.ref_count == 0 && it->second.idle_sessions.empty() &&
          it->second.in_flight_count == 0) {
        connection_groups_.erase(it);
      }
    }
    handle->state.store(HandleState::kClosed, std::memory_order_release);
  }

  std::size_t ready_count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return ready_count_;
  }

  std::size_t expected_capacity() const {
    std::lock_guard<std::mutex> lock(mu_);
    return expected_capacity_;
  }

  std::size_t sessions_holding_budget_count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return sessions_holding_budget_.size();
  }

  std::size_t peer_ref_count(std::int64_t peer_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = connection_groups_.find(peer_id);
    return it != connection_groups_.end() ? it->second.ref_count : 0;
  }

  std::size_t afe_ref_count(std::int64_t peer_id) const {
    return peer_ref_count(peer_id);
  }

 private:
  mutable std::mutex mu_;
  std::unordered_map<std::int64_t, ConnectionGroupHandle> connection_groups_;
  std::unordered_set<std::shared_ptr<SessionHandle>> sessions_holding_budget_;
  std::size_t ready_count_ = 0;
  std::size_t expected_capacity_ = 0;
};

/// Computes the target session pool capacity from active and pending load:
/// - `EffectivePending = ceil(pending_count / new_session_queue_length)`
/// - `SessionsInUse = in_use_count + EffectivePending`
/// - `IdleHeadroom = max(1, ceil(SessionsInUse * headroom_ratio))`
/// - `DesiredCapacity = clamp(SessionsInUse + IdleHeadroom, min_sessions,
/// max_sessions)`
inline std::size_t ComputeDesiredPoolCapacity(
    std::size_t in_use_count, std::size_t pending_count,
    std::size_t new_session_queue_length, double headroom_ratio,
    std::size_t min_sessions, std::size_t max_sessions) {
  std::size_t const q_len =
      (new_session_queue_length == 0) ? 10 : new_session_queue_length;
  std::size_t const effective_pending = (pending_count + q_len - 1) / q_len;
  std::size_t const sessions_in_use = in_use_count + effective_pending;
  auto const computed_headroom = static_cast<std::size_t>(
      std::ceil(static_cast<double>(sessions_in_use) * headroom_ratio));
  std::size_t const idle_headroom =
      (std::max<std::size_t>)(1, computed_headroom);
  std::size_t const raw_desired = sessions_in_use + idle_headroom;
  return (std::min)(max_sessions, (std::max)(min_sessions, raw_desired));
}

}  // namespace pool
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_SESSION_LIST_H
