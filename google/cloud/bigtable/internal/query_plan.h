
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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_QUERY_PLAN_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_QUERY_PLAN_H

#include "google/cloud/bigtable/version.h"
#include "google/cloud/completion_queue.h"
#include "google/cloud/future.h"
#include "google/cloud/internal/clock.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "google/bigtable/v2/bigtable.pb.h"
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN

class QueryPlan : public std::enable_shared_from_this<QueryPlan> {
 public:
  // Typically, a lambda capturing the original PrepareQueryRequest and
  // DataConnection pointer necessary to call the AsyncPrepareQuery RPC.
  using RefreshFn = std::function<
      future<StatusOr<google::bigtable::v2::PrepareQueryResponse>>()>;

  using Clock = ::google::cloud::internal::SystemClock;

  // Calls the constructor and then Initialize.
  static std::shared_ptr<QueryPlan> Create(
      CompletionQueue cq,
      StatusOr<google::bigtable::v2::PrepareQueryResponse> response,
      RefreshFn fn, std::shared_ptr<Clock> clock = std::make_shared<Clock>());

  ~QueryPlan();

  // Invalidates the current QueryPlan and triggers a refresh.
  void Invalidate(Status status, std::string const& invalid_query_plan_id);

  // Accessor for the prepared_query and metadata fields in response_.
  // Triggers a refresh if needed.
  StatusOr<google::bigtable::v2::PrepareQueryResponse> response();

  GOOGLE_CLOUD_CPP_DEPRECATED("Use response() instead")
  StatusOr<std::string> prepared_query();

  GOOGLE_CLOUD_CPP_DEPRECATED("Use response() instead")
  StatusOr<google::bigtable::v2::ResultSetMetadata> metadata();

 private:
  QueryPlan(CompletionQueue cq, std::shared_ptr<Clock> clock, RefreshFn fn,
            StatusOr<google::bigtable::v2::PrepareQueryResponse> response)
      : state_(response.ok() ? RefreshState::kDone : RefreshState::kBegin),
        cq_(std::move(cq)),
        clock_(clock ? std::move(clock) : std::make_shared<Clock>()),
        refresh_fn_(std::move(fn)),
        response_(std::move(response)) {}

  // Performs the first call to ScheduleRefresh and any other initialization not
  // possible in the constructor.
  void Initialize();

  // Calls MakeDeadlineTimer on the CompletionQueue with a continuation lambda
  // capturing a std::weak_ptr to this that calls ExpiredRefresh.
  void ScheduleRefresh(std::unique_lock<std::mutex> const& lock);
  void ScheduleRefreshAt(std::unique_lock<std::mutex> const& lock,
                         std::chrono::system_clock::time_point deadline);

  // Starts an asynchronous refresh attempt without blocking on its completion.
  // Unlocks `lock` while invoking `refresh_fn_()` and re-acquires `lock` before
  // returning the assigned refresh attempt ID.
  std::uint64_t StartRefresh(std::unique_lock<std::mutex>& lock);

  // Handles completion of an asynchronous refresh attempt.
  void OnRefreshCompleted(
      StatusOr<google::bigtable::v2::PrepareQueryResponse> response);

  void ExpiredRefresh();

  // State machine transitions:
  //   kDone -> kBegin      (Invalidate while idle)
  //   kDone -> kPending    (proactive ExpiredRefresh)
  //   kBegin -> kPending   (StartRefresh from response())
  //   kPending -> kBegin   (post-invalidation refresh failed)
  //   kPending -> kDone    (refresh succeeded, or proactive refresh failed
  //                         while the existing plan has not been invalidated)
  //   kPending -> kPending (Invalidate arrived during an in-flight refresh,
  //                         immediately starting a fresh refresh)
  enum class RefreshState {
    kBegin,    // waiting for a caller to start refreshing response_
    kPending,  // asynchronous refresh RPC is in flight
    kDone,     // response_ holds a usable query plan
  };
  RefreshState state_;

  CompletionQueue cq_;
  std::shared_ptr<Clock> clock_;
  RefreshFn refresh_fn_;
  future<void> refresh_timer_;
  mutable std::mutex mu_;
  std::condition_variable turn_cv_;
  std::condition_variable refresh_cv_;
  bool invalidate_during_refresh_ = false;
  bool active_refresh_has_leader_ = false;
  std::uint64_t next_refresh_id_ = 0;
  std::uint64_t active_refresh_id_ = 0;
  std::uint64_t completed_refresh_id_ = 0;
  StatusOr<google::bigtable::v2::PrepareQueryResponse>
      response_;  // GUARDED_BY(mu_)
};

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_QUERY_PLAN_H
