// Copyright 2025 Google LLC
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

#include "google/cloud/bigtable/internal/query_plan.h"
#include "google/cloud/completion_queue.h"
#include "google/cloud/internal/time_utils.h"
#include "google/cloud/options.h"
#include "google/bigtable/v2/data.pb.h"
#include <algorithm>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace {
auto constexpr kRefreshDeadlineOffsetMs = std::chrono::milliseconds(1000);
auto constexpr kRefreshRetryDelayMs = std::chrono::milliseconds(250);
}  // namespace

std::shared_ptr<QueryPlan> QueryPlan::Create(
    CompletionQueue cq,
    StatusOr<google::bigtable::v2::PrepareQueryResponse> response, RefreshFn fn,
    std::shared_ptr<Clock> clock) {
  auto plan = std::shared_ptr<QueryPlan>(new QueryPlan(
      std::move(cq), std::move(clock), std::move(fn), std::move(response)));
  plan->Initialize();
  return plan;
}

QueryPlan::~QueryPlan() {
  future<void> timer;
  {
    std::unique_lock<std::mutex> lock(mu_);
    timer = std::move(refresh_timer_);
  }
  if (timer.valid()) timer.cancel();
}

void QueryPlan::Initialize() {
  std::unique_lock<std::mutex> lock(mu_);
  if (state_ == RefreshState::kDone) ScheduleRefresh(lock);
}

// ScheduleRefresh should only be called after updating response_.
void QueryPlan::ScheduleRefresh(std::unique_lock<std::mutex> const& lock) {
  if (!response_.ok()) return;
  // We want to start the refresh process before the query plan expires.
  auto refresh_deadline =
      internal::ToChronoTimePoint(response_->valid_until()) -
      kRefreshDeadlineOffsetMs;
  ScheduleRefreshAt(lock, refresh_deadline);
}

void QueryPlan::ScheduleRefreshAt(
    std::unique_lock<std::mutex> const&,
    std::chrono::system_clock::time_point deadline) {
  if (refresh_timer_.valid()) refresh_timer_.cancel();
  std::weak_ptr<QueryPlan> plan = weak_from_this();
  refresh_timer_ = cq_.MakeDeadlineTimer(deadline).then(
      [plan = std::move(plan), current = internal::SaveCurrentOptions()](
          future<StatusOr<std::chrono::system_clock::time_point>> result) {
        // Options are stored in a thread_local variable. When this timer
        // expires and this lambda is executed we need to restore the
        // Options that were saved in the capture group as a different
        // thread may be used.
        internal::OptionsSpan options_span(current);
        if (result.get().ok()) {
          if (auto p = plan.lock()) {
            p->ExpiredRefresh();
          }
        }
      });
}

void QueryPlan::ExpiredRefresh() {
  std::unique_lock<std::mutex> lock(mu_);
  if (state_ != RefreshState::kDone || !response_.ok()) return;
  StartRefresh(lock);
}

void QueryPlan::Invalidate(Status status,
                           std::string const& invalid_query_plan_id) {
  future<void> timer;
  {
    std::unique_lock<std::mutex> lock(mu_);
    // Only invalidate if the rejected plan ID matches the current active plan.
    // This ignores stale multi-hop invalidations for older plans while still
    // clearing response_ if the active plan expires during a slow proactive
    // refresh.
    if (response_.ok() &&
        response_->prepared_query() == invalid_query_plan_id) {
      response_ = std::move(status);
      timer = std::move(refresh_timer_);
      if (state_ == RefreshState::kPending) {
        invalidate_during_refresh_ = true;
      } else {
        state_ = RefreshState::kBegin;
      }
    }
  }
  if (timer.valid()) timer.cancel();
}

std::uint64_t QueryPlan::StartRefresh(std::unique_lock<std::mutex>& lock) {
  state_ = RefreshState::kPending;
  auto const id = ++next_refresh_id_;
  active_refresh_id_ = id;
  std::weak_ptr<QueryPlan> plan = weak_from_this();
  auto current = internal::SaveCurrentOptions();
  lock.unlock();
  (void)refresh_fn_().then(
      [plan = std::move(plan), current = std::move(current)](
          future<StatusOr<google::bigtable::v2::PrepareQueryResponse>> f) {
        internal::OptionsSpan options_span(current);
        auto result = f.get();
        if (auto p = plan.lock()) {
          p->OnRefreshCompleted(std::move(result));
        }
      });
  lock.lock();
  return id;
}

void QueryPlan::OnRefreshCompleted(
    StatusOr<google::bigtable::v2::PrepareQueryResponse> response) {
  bool notify_all_waiters = false;
  {
    std::unique_lock<std::mutex> lock(mu_);
    if (invalidate_during_refresh_) {
      invalidate_during_refresh_ = false;
      StartRefresh(lock);
      return;
    }

    active_refresh_has_leader_ = false;
    completed_refresh_id_ = active_refresh_id_;

    if (response.ok()) {
      response_ = std::move(response);
      state_ = RefreshState::kDone;
      notify_all_waiters = true;
      ScheduleRefresh(lock);
    } else if (response_.ok()) {
      // A proactive refresh failed transiently while the current plan has not
      // been invalidated. Preserve the existing plan so foreground callers can
      // continue using it during its remaining validity and server grace
      // window.
      state_ = RefreshState::kDone;
      notify_all_waiters = true;
      auto const valid_until =
          internal::ToChronoTimePoint(response_->valid_until());
      auto const now = clock_->Now();
      if (now < valid_until) {
        ScheduleRefreshAt(lock,
                          std::min(now + kRefreshRetryDelayMs, valid_until));
      }
    } else {
      // If there are no waiting threads that could call the refresh_fn, then
      // we accept that the refresh is in a failed state until the next caller
      // retries. If there are waiting threads, we wake one to try again while
      // avoiding a stampede of concurrent refresh RPCs.
      response_ = std::move(response);
      state_ = RefreshState::kBegin;
    }
  }
  refresh_cv_.notify_all();
  if (notify_all_waiters) {
    turn_cv_.notify_all();
  } else {
    turn_cv_.notify_one();
  }
}

StatusOr<google::bigtable::v2::PrepareQueryResponse> QueryPlan::response() {
  std::unique_lock<std::mutex> lock(mu_);
  if (response_.ok()) return response_;

  if (state_ == RefreshState::kDone) return response_;

  std::uint64_t target_id = 0;
  if (state_ == RefreshState::kPending && !active_refresh_has_leader_) {
    active_refresh_has_leader_ = true;
    target_id = active_refresh_id_;
  } else {
    turn_cv_.wait(lock, [this] {
      return state_ != RefreshState::kPending || !active_refresh_has_leader_;
    });
    if (response_.ok() || state_ == RefreshState::kDone) {
      return response_;
    }
    if (state_ == RefreshState::kPending) {
      active_refresh_has_leader_ = true;
      target_id = active_refresh_id_;
    } else {
      active_refresh_has_leader_ = true;
      target_id = StartRefresh(lock);
    }
  }

  refresh_cv_.wait(
      lock, [this, target_id] { return completed_refresh_id_ >= target_id; });
  return response_;
}

StatusOr<std::string> QueryPlan::prepared_query() {
  auto data = response();
  if (!data.ok()) return std::move(data).status();
  return std::move(*data->mutable_prepared_query());
}

StatusOr<google::bigtable::v2::ResultSetMetadata> QueryPlan::metadata() {
  auto data = response();
  if (!data.ok()) return std::move(data).status();
  return std::move(*data->mutable_metadata());
}

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
