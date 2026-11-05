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
#include "google/cloud/internal/make_status.h"
#include "google/cloud/internal/time_utils.h"
#include "google/cloud/testing_util/fake_clock.h"
#include "google/cloud/testing_util/fake_completion_queue_impl.h"
#include "google/cloud/testing_util/is_proto_equal.h"
#include "google/cloud/testing_util/status_matchers.h"
#include "absl/synchronization/barrier.h"
#include "google/bigtable/v2/data.pb.h"
#include <google/protobuf/text_format.h>
#include <google/protobuf/util/time_util.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace {

using ::google::cloud::testing_util::FakeCompletionQueueImpl;
using ::google::cloud::testing_util::FakeSystemClock;
using ::google::cloud::testing_util::IsProtoEqual;
using ::google::cloud::testing_util::StatusIs;
using ::testing::IsEmpty;

TEST(QueryPlanTest, ResponseDataWithOriginalValidQueryPlan) {
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  CompletionQueue cq(fake_cq_impl);

  google::bigtable::v2::PrepareQueryResponse response;
  response.set_prepared_query("test-query");
  auto constexpr kResultMetadataText = R"pb(
    proto_schema {
      columns {
        name: "user_id"
        type { string_type {} }
      }
    }
  )pb";
  google::bigtable::v2::ResultSetMetadata metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(kResultMetadataText,
                                                            &metadata));
  *response.mutable_metadata() = metadata;

  auto plan = QueryPlan::Create(cq, response, [] {
    return make_ready_future(
        make_status_or(google::bigtable::v2::PrepareQueryResponse{}));
  });

  auto response_data = plan->response();
  ASSERT_STATUS_OK(response_data);
  EXPECT_EQ(response_data->prepared_query(), "test-query");
  EXPECT_THAT(response_data->metadata(), IsProtoEqual(metadata));

  // Cancel all pending operations, satisfying any remaining futures.
  fake_cq_impl->SimulateCompletion(false);
}

TEST(QueryPlanTest, RefreshExpiredPlan) {
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  auto fake_clock = std::make_shared<FakeSystemClock>();
  auto now = std::chrono::system_clock::now();
  fake_clock->SetTime(now);

  google::bigtable::v2::PrepareQueryResponse refresh_response;
  refresh_response.set_prepared_query("refreshed-query-plan");
  auto refresh_fn = [&]() {
    return make_ready_future(make_status_or(refresh_response));
  };

  google::bigtable::v2::PrepareQueryResponse response;
  response.set_prepared_query("original-query-plan");

  auto query_plan = QueryPlan::Create(CompletionQueue(fake_cq_impl), response,
                                      refresh_fn, fake_clock);

  auto data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "original-query-plan");

  fake_cq_impl->SimulateCompletion(true);

  data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "refreshed-query-plan");

  // Cancel all pending operations, satisfying any remaining futures.
  fake_cq_impl->SimulateCompletion(false);
}

TEST(QueryPlanTest, FailedRefreshExpiredPlan) {
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  auto fake_clock = std::make_shared<FakeSystemClock>();
  auto now = std::chrono::system_clock::now();
  fake_clock->SetTime(now);

  auto failed_refresh = internal::InternalError("oops!");
  auto refresh_fn = [&]() {
    return make_ready_future(
        StatusOr<google::bigtable::v2::PrepareQueryResponse>(failed_refresh));
  };

  google::bigtable::v2::PrepareQueryResponse response;
  response.set_prepared_query("original-query-plan");

  auto query_plan = QueryPlan::Create(CompletionQueue(fake_cq_impl), response,
                                      refresh_fn, fake_clock);

  auto data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "original-query-plan");

  // A failed proactive timer refresh preserves the existing plan rather than
  // clobbering it while the server may still accept it.
  fake_cq_impl->SimulateCompletion(true);

  data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "original-query-plan");

  // Once the server actually rejects the plan and Invalidate is called, a
  // failed refresh surfaces the refresh error.
  query_plan->Invalidate(internal::FailedPreconditionError("plan expired"),
                         "original-query-plan");
  data = query_plan->response();
  EXPECT_THAT(data.status(), StatusIs(StatusCode::kInternal, "oops!"));

  // Cancel all pending operations, satisfying any remaining futures.
  fake_cq_impl->SimulateCompletion(false);
}

TEST(QueryPlanTest, RefreshInvalidatedPlan) {
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  auto fake_clock = std::make_shared<FakeSystemClock>();
  auto now = std::chrono::system_clock::now();
  fake_clock->SetTime(now);

  google::bigtable::v2::PrepareQueryResponse refresh_response;
  refresh_response.set_prepared_query("refreshed-query-plan");
  auto refresh_fn = [&]() {
    return make_ready_future(make_status_or(refresh_response));
  };

  google::bigtable::v2::PrepareQueryResponse response;
  response.set_prepared_query("original-query-plan");

  auto query_plan = QueryPlan::Create(CompletionQueue(fake_cq_impl), response,
                                      refresh_fn, fake_clock);

  auto data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "original-query-plan");

  auto invalid_status = internal::InternalError("oops!");
  query_plan->Invalidate(invalid_status, data->prepared_query());

  data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "refreshed-query-plan");

  // Cancel all pending operations, satisfying any remaining futures.
  fake_cq_impl->SimulateCompletion(false);
}

TEST(QueryPlanTest, FailedRefreshInvalidatedPlan) {
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  auto fake_clock = std::make_shared<FakeSystemClock>();
  auto now = std::chrono::system_clock::now();
  fake_clock->SetTime(now);

  auto failed_refresh = internal::InternalError("oops again!");
  auto refresh_fn = [&]() {
    return make_ready_future(
        StatusOr<google::bigtable::v2::PrepareQueryResponse>(failed_refresh));
  };

  google::bigtable::v2::PrepareQueryResponse response;
  response.set_prepared_query("original-query-plan");

  auto query_plan = QueryPlan::Create(CompletionQueue(fake_cq_impl), response,
                                      refresh_fn, fake_clock);

  auto data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "original-query-plan");

  auto invalid_status = internal::InternalError("oops!");
  query_plan->Invalidate(invalid_status, data->prepared_query());

  data = query_plan->response();
  EXPECT_THAT(data.status(), StatusIs(StatusCode::kInternal, "oops again!"));

  // Cancel all pending operations, satisfying any remaining futures.
  fake_cq_impl->SimulateCompletion(false);
}

TEST(QueryPlanTest, CreateFailedPlanAndRefresh) {
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  auto fake_clock = std::make_shared<FakeSystemClock>();
  auto now = std::chrono::system_clock::now();
  fake_clock->SetTime(now);

  google::bigtable::v2::PrepareQueryResponse refresh_response;
  refresh_response.set_prepared_query("refreshed-query-plan");
  auto refresh_fn = [&]() {
    return make_ready_future(make_status_or(refresh_response));
  };

  StatusOr<google::bigtable::v2::PrepareQueryResponse> response =
      Status(StatusCode::kResourceExhausted, "I'm tired boss");

  auto query_plan = QueryPlan::Create(CompletionQueue(fake_cq_impl), response,
                                      refresh_fn, fake_clock);

  EXPECT_THAT(*fake_cq_impl, IsEmpty());

  auto data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "refreshed-query-plan");

  EXPECT_EQ(fake_cq_impl->size(), 1);
  // Cancel all pending operations, satisfying any remaining futures.
  fake_cq_impl->SimulateCompletion(false);
}

// TODO(#15695): For reasons not yet understood, the fedora m32 CI build has
//   failures not seen in m64 builds when the number of threads is "too" high.
//  These failures occur while trying to create more than 500 threads.
constexpr int LimitNumThreadsOn32Bit(int num_threads) {
#if INTPTR_MAX == INT32_MAX
  return std::min(num_threads, 200);
#else
  return num_threads;
#endif
}

TEST(QueryPlanMultithreadedTest, RefreshInvalidatedPlan) {
  using google::bigtable::v2::PrepareQueryResponse;
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  auto fake_clock = std::make_shared<FakeSystemClock>();
  auto now = std::chrono::system_clock::now();
  fake_clock->SetTime(now);

  PrepareQueryResponse refresh_response;
  refresh_response.set_prepared_query("refreshed-query-plan");
  int calls_to_refresh_fn = 0;
  std::mutex mu;
  auto refresh_fn = [&]() {
    std::lock_guard<std::mutex> lock(mu);
    ++calls_to_refresh_fn;
    return make_ready_future(make_status_or(refresh_response));
  };

  PrepareQueryResponse response;
  response.set_prepared_query("original-query-plan");

  auto query_plan = QueryPlan::Create(CompletionQueue(fake_cq_impl), response,
                                      refresh_fn, fake_clock);

  auto data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "original-query-plan");

  constexpr int kNumThreads = LimitNumThreadsOn32Bit(1000);
  std::vector<std::thread> threads(kNumThreads);
  std::array<promise<StatusOr<PrepareQueryResponse>>, kNumThreads>
      data_responses;
  std::array<future<StatusOr<PrepareQueryResponse>>, kNumThreads>
      data_response_futures;
  for (auto i = 0; i < kNumThreads; ++i) {
    data_responses[i] = promise<StatusOr<PrepareQueryResponse>>();
    data_response_futures[i] = data_responses[i].get_future();
  }

  auto barrier = std::make_shared<absl::Barrier>(kNumThreads + 1);
  auto thread_fn = [barrier, query_plan](
                       promise<StatusOr<PrepareQueryResponse>>* thread_data) {
    barrier->Block();
    thread_data->set_value(query_plan->response());
  };

  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(thread_fn, &(data_responses[i]));
  }

  auto invalid_status = internal::InternalError("oops!");
  query_plan->Invalidate(invalid_status, data->prepared_query());
  barrier->Block();

  for (auto& f : data_response_futures) {
    auto r = f.get();
    ASSERT_STATUS_OK(r);
    EXPECT_EQ(r->prepared_query(), "refreshed-query-plan");
  }

  for (auto& t : threads) {
    if (t.joinable()) t.join();
  }
  EXPECT_EQ(calls_to_refresh_fn, 1);

  // Cancel all pending operations, satisfying any remaining futures.
  fake_cq_impl->SimulateCompletion(false);
}

TEST(QueryPlanMultithreadedTest, RefreshInvalidatedPlanTransientFailures) {
  using google::bigtable::v2::PrepareQueryResponse;
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  auto fake_clock = std::make_shared<FakeSystemClock>();
  auto now = std::chrono::system_clock::now();
  fake_clock->SetTime(now);

  PrepareQueryResponse refresh_response;
  refresh_response.set_prepared_query("refreshed-query-plan");
  int calls_to_refresh_fn = 0;
  std::mutex mu;
  auto refresh_fn = [&]() {
    std::lock_guard<std::mutex> lock(mu);
    ++calls_to_refresh_fn;
    if (calls_to_refresh_fn <= 3) {
      return make_ready_future(StatusOr<PrepareQueryResponse>(
          Status(StatusCode::kUnavailable, "hello?")));
    }
    return make_ready_future(make_status_or(refresh_response));
  };

  PrepareQueryResponse response;
  response.set_prepared_query("original-query-plan");

  auto query_plan = QueryPlan::Create(CompletionQueue(fake_cq_impl), response,
                                      refresh_fn, fake_clock);

  auto data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "original-query-plan");

  constexpr int kNumThreads = LimitNumThreadsOn32Bit(1000);
  std::vector<std::thread> threads(kNumThreads);
  std::array<promise<StatusOr<PrepareQueryResponse>>, kNumThreads>
      data_responses;
  std::array<future<StatusOr<PrepareQueryResponse>>, kNumThreads>
      data_response_futures;
  for (auto i = 0; i < kNumThreads; ++i) {
    data_responses[i] = promise<StatusOr<PrepareQueryResponse>>();
    data_response_futures[i] = data_responses[i].get_future();
  }

  auto barrier = std::make_shared<absl::Barrier>(kNumThreads + 1);
  auto thread_fn = [barrier, query_plan](
                       promise<StatusOr<PrepareQueryResponse>>* thread_data) {
    barrier->Block();
    auto response = query_plan->response();
    while (!response.ok()) {
      std::this_thread::yield();
      response = query_plan->response();
    }
    thread_data->set_value(response);
  };

  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(thread_fn, &(data_responses[i]));
  }

  auto invalid_status = internal::InternalError("oops!");
  query_plan->Invalidate(invalid_status, data->prepared_query());
  barrier->Block();

  for (auto& f : data_response_futures) {
    auto r = f.get();
    ASSERT_STATUS_OK(r);
    EXPECT_EQ(r->prepared_query(), "refreshed-query-plan");
  }

  for (auto& t : threads) {
    if (t.joinable()) t.join();
  }

  EXPECT_EQ(calls_to_refresh_fn, 4);

  // Cancel all pending operations, satisfying any remaining futures.
  fake_cq_impl->SimulateCompletion(false);
}

TEST(QueryPlanMultithreadedTest, RefreshInvalidatedPlanAfterFailedRefresh) {
  using google::bigtable::v2::PrepareQueryResponse;
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  auto fake_clock = std::make_shared<FakeSystemClock>();
  auto now = std::chrono::system_clock::now();
  fake_clock->SetTime(now);

  constexpr int kNumFailingThreads = 100;
  PrepareQueryResponse refresh_response;
  refresh_response.set_prepared_query("refreshed-query-plan");
  int calls_to_refresh_fn = 0;
  std::mutex mu;
  auto refresh_fn = [&]() {
    std::lock_guard<std::mutex> lock(mu);
    ++calls_to_refresh_fn;
    if (calls_to_refresh_fn <= kNumFailingThreads) {
      return make_ready_future(StatusOr<PrepareQueryResponse>(
          Status(StatusCode::kUnavailable, "hello?")));
    }
    return make_ready_future(make_status_or(refresh_response));
  };

  PrepareQueryResponse response;
  response.set_prepared_query("original-query-plan");

  auto query_plan = QueryPlan::Create(CompletionQueue(fake_cq_impl), response,
                                      refresh_fn, fake_clock);

  auto data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "original-query-plan");

  std::vector<std::thread> failing_threads(kNumFailingThreads);
  std::array<StatusOr<PrepareQueryResponse>, kNumFailingThreads>
      failed_responses;

  auto fail_barrier = std::make_shared<absl::Barrier>(kNumFailingThreads + 1);
  auto fail_thread_fn =
      [fail_barrier, query_plan](StatusOr<PrepareQueryResponse>* thread_data) {
        fail_barrier->Block();
        *thread_data = query_plan->response();
      };

  for (int i = 0; i < kNumFailingThreads; ++i) {
    failed_responses[i] = StatusOr<PrepareQueryResponse>(
        Status(StatusCode::kNotFound, "not found"));
    failing_threads.emplace_back(fail_thread_fn, &(failed_responses[i]));
  }

  auto invalid_status = internal::InternalError("oops!");
  query_plan->Invalidate(invalid_status, data->prepared_query());
  fail_barrier->Block();

  for (auto& t : failing_threads) {
    if (t.joinable()) t.join();
  }

  EXPECT_EQ(calls_to_refresh_fn, kNumFailingThreads);
  for (auto const& r : failed_responses) {
    EXPECT_THAT(r.status(), StatusIs(StatusCode::kUnavailable, "hello?"));
  }

  // At this point, all the previous threads have run to completion and the
  // QueryPlan is left with a failed Status, not a valid response_. The state_
  // of QueryPlan is RefreshState::kBegin, so the first thread to call
  // response() should trigger a new refresh with the other threads waiting for
  // it to complete.
  constexpr int kNumThreads = LimitNumThreadsOn32Bit(1000);
  std::vector<std::thread> threads(kNumThreads);
  std::array<promise<StatusOr<PrepareQueryResponse>>, kNumThreads>
      data_responses;
  std::array<future<StatusOr<PrepareQueryResponse>>, kNumThreads>
      data_response_futures;
  for (auto i = 0; i < kNumThreads; ++i) {
    data_responses[i] = promise<StatusOr<PrepareQueryResponse>>();
    data_response_futures[i] = data_responses[i].get_future();
  }

  auto barrier = std::make_shared<absl::Barrier>(kNumThreads + 1);
  auto thread_fn = [barrier, query_plan](
                       promise<StatusOr<PrepareQueryResponse>>* thread_data) {
    barrier->Block();
    thread_data->set_value(query_plan->response());
  };

  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(thread_fn, &(data_responses[i]));
  }

  barrier->Block();

  for (auto& f : data_response_futures) {
    auto r = f.get();
    ASSERT_STATUS_OK(r);
    EXPECT_EQ(r->prepared_query(), "refreshed-query-plan");
  }

  for (auto& t : threads) {
    if (t.joinable()) t.join();
  }
  EXPECT_EQ(calls_to_refresh_fn, kNumFailingThreads + 1);

  // Cancel all pending operations, satisfying any remaining futures.
  fake_cq_impl->SimulateCompletion(false);
}

TEST(QueryPlanTest, ProactiveRefreshTransientFailureRetriesBeforeExpiry) {
  using google::bigtable::v2::PrepareQueryResponse;
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  auto fake_clock = std::make_shared<FakeSystemClock>();
  auto now = std::chrono::system_clock::now();
  fake_clock->SetTime(now);

  int calls = 0;
  auto refresh_fn = [&]() {
    ++calls;
    if (calls == 1 || calls == 3) {
      return make_ready_future(StatusOr<PrepareQueryResponse>(
          Status(StatusCode::kUnavailable, "transient failure")));
    }
    PrepareQueryResponse r;
    r.set_prepared_query(calls == 2 ? "plan-v2" : "plan-v3");
    *r.mutable_valid_until() =
        internal::ToProtoTimestamp(now + std::chrono::seconds(20));
    return make_ready_future(make_status_or(std::move(r)));
  };

  PrepareQueryResponse initial;
  initial.set_prepared_query("plan-v1");
  *initial.mutable_valid_until() =
      internal::ToProtoTimestamp(now + std::chrono::seconds(10));

  auto query_plan = QueryPlan::Create(CompletionQueue(fake_cq_impl), initial,
                                      refresh_fn, fake_clock);
  EXPECT_EQ(fake_cq_impl->size(), 1);

  // Proactive refresh fires before valid_until and fails transiently.
  fake_clock->AdvanceTime(std::chrono::seconds(9));
  fake_cq_impl->SimulateCompletion(true);
  EXPECT_EQ(calls, 1);

  // The valid plan is preserved and a retry timer is scheduled before expiry.
  auto data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "plan-v1");
  EXPECT_EQ(calls, 1);
  EXPECT_EQ(fake_cq_impl->size(), 1);

  // Retry timer fires and succeeds.
  fake_cq_impl->SimulateCompletion(true);
  EXPECT_EQ(calls, 2);
  data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "plan-v2");
  EXPECT_EQ(fake_cq_impl->size(), 1);

  // If a proactive refresh fails after valid_until has passed, the existing
  // plan is still preserved for the server grace window without scheduling
  // tight-loop timers past valid_until.
  fake_clock->AdvanceTime(std::chrono::seconds(20));
  fake_cq_impl->SimulateCompletion(true);
  EXPECT_EQ(calls, 3);
  data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "plan-v2");
  EXPECT_THAT(*fake_cq_impl, IsEmpty());

  // Reactive Invalidate refreshes to plan-v3.
  query_plan->Invalidate(internal::FailedPreconditionError("expired"),
                         "plan-v2");
  data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "plan-v3");
  EXPECT_EQ(calls, 4);

  fake_cq_impl->SimulateCompletion(false);
}

TEST(QueryPlanTest, AsyncProactiveRefreshDoesNotBlockTimerCompletion) {
  using google::bigtable::v2::PrepareQueryResponse;
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  auto fake_clock = std::make_shared<FakeSystemClock>();
  fake_clock->SetTime(std::chrono::system_clock::now());

  promise<StatusOr<PrepareQueryResponse>> refresh_promise;
  auto refresh_fn = [&]() { return refresh_promise.get_future(); };

  PrepareQueryResponse initial;
  initial.set_prepared_query("plan-v1");
  auto query_plan = QueryPlan::Create(CompletionQueue(fake_cq_impl), initial,
                                      refresh_fn, fake_clock);

  // Completing the timer on this thread must not block waiting on the
  // unsatisfied refresh future.
  fake_cq_impl->SimulateCompletion(true);

  // Foreground callers continue receiving the existing plan non-blockingly
  // while the proactive refresh is in flight.
  auto data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "plan-v1");

  PrepareQueryResponse refreshed;
  refreshed.set_prepared_query("plan-v2");
  refresh_promise.set_value(refreshed);

  data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "plan-v2");

  fake_cq_impl->SimulateCompletion(false);
}

TEST(QueryPlanTest, StaleMultiHopInvalidateDoesNotClobberCurrentPlan) {
  using google::bigtable::v2::PrepareQueryResponse;
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  auto fake_clock = std::make_shared<FakeSystemClock>();
  fake_clock->SetTime(std::chrono::system_clock::now());

  int calls = 0;
  auto refresh_fn = [&]() {
    ++calls;
    PrepareQueryResponse r;
    r.set_prepared_query("plan-v" + std::to_string(calls + 1));
    return make_ready_future(make_status_or(std::move(r)));
  };

  PrepareQueryResponse initial;
  initial.set_prepared_query("plan-v1");
  auto query_plan = QueryPlan::Create(CompletionQueue(fake_cq_impl), initial,
                                      refresh_fn, fake_clock);

  // Refresh twice: plan-v1 -> plan-v2 -> plan-v3.
  fake_cq_impl->SimulateCompletion(true);
  ASSERT_STATUS_OK(query_plan->response());
  EXPECT_EQ(query_plan->response()->prepared_query(), "plan-v2");

  fake_cq_impl->SimulateCompletion(true);
  ASSERT_STATUS_OK(query_plan->response());
  EXPECT_EQ(query_plan->response()->prepared_query(), "plan-v3");
  EXPECT_EQ(calls, 2);

  // Delayed callers reporting expiry for plan-v1 (multi-hop stale) or plan-v2
  // (single-hop stale) must not clobber plan-v3 or trigger extra refreshes.
  query_plan->Invalidate(internal::FailedPreconditionError("expired v1"),
                         "plan-v1");
  auto data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "plan-v3");
  EXPECT_EQ(calls, 2);

  query_plan->Invalidate(internal::FailedPreconditionError("expired v2"),
                         "plan-v2");
  data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "plan-v3");
  EXPECT_EQ(calls, 2);

  // Invalidating the active plan-v3 triggers a refresh to plan-v4.
  query_plan->Invalidate(internal::FailedPreconditionError("expired v3"),
                         "plan-v3");
  data = query_plan->response();
  ASSERT_STATUS_OK(data);
  EXPECT_EQ(data->prepared_query(), "plan-v4");
  EXPECT_EQ(calls, 3);

  fake_cq_impl->SimulateCompletion(false);
}

TEST(QueryPlanTest,
     InvalidateDuringPendingProactiveRefreshTriggersFreshRefresh) {
  using google::bigtable::v2::PrepareQueryResponse;
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  auto fake_clock = std::make_shared<FakeSystemClock>();
  fake_clock->SetTime(std::chrono::system_clock::now());

  promise<StatusOr<PrepareQueryResponse>> promise_1;
  promise<StatusOr<PrepareQueryResponse>> promise_2;
  promise<void> call_2_started;
  std::atomic<int> calls{0};

  auto refresh_fn = [&]() {
    int const c = ++calls;
    if (c == 1) return promise_1.get_future();
    call_2_started.set_value();
    return promise_2.get_future();
  };

  PrepareQueryResponse initial;
  initial.set_prepared_query("plan-v1");
  auto query_plan = QueryPlan::Create(CompletionQueue(fake_cq_impl), initial,
                                      refresh_fn, fake_clock);

  // Start slow proactive refresh #1.
  fake_cq_impl->SimulateCompletion(true);
  EXPECT_EQ(calls.load(), 1);

  // Server rejects plan-v1 while proactive refresh #1 is still in flight.
  query_plan->Invalidate(internal::FailedPreconditionError("expired v1"),
                         "plan-v1");
  // Duplicate Invalidate for the same expired plan is coalesced.
  query_plan->Invalidate(internal::FailedPreconditionError("expired v1 again"),
                         "plan-v1");
  EXPECT_EQ(calls.load(), 1);

  // A foreground caller requesting response() must wait rather than receiving
  // the known-expired plan-v1.
  promise<void> caller_entered;
  promise<StatusOr<PrepareQueryResponse>> caller_result;
  auto caller_future = caller_result.get_future();
  std::thread caller([&] {
    caller_entered.set_value();
    caller_result.set_value(query_plan->response());
  });
  caller_entered.get_future().get();
  EXPECT_FALSE(caller_future.is_ready());

  // When the pre-invalidation refresh #1 finishes, OnRefreshCompleted discards
  // it and immediately starts fresh refresh #2.
  PrepareQueryResponse stale_refresh;
  stale_refresh.set_prepared_query("stale-plan-v2");
  promise_1.set_value(stale_refresh);

  call_2_started.get_future().get();
  EXPECT_EQ(calls.load(), 2);
  EXPECT_FALSE(caller_future.is_ready());

  PrepareQueryResponse fresh_refresh;
  fresh_refresh.set_prepared_query("fresh-plan-v3");
  promise_2.set_value(fresh_refresh);

  auto result = caller_future.get();
  caller.join();
  ASSERT_STATUS_OK(result);
  EXPECT_EQ(result->prepared_query(), "fresh-plan-v3");
  EXPECT_EQ(calls.load(), 2);

  fake_cq_impl->SimulateCompletion(false);
}

TEST(QueryPlanTest,
     InvalidateDuringPendingProactiveRefreshHandlesFollowUpFailureAndRecovery) {
  using google::bigtable::v2::PrepareQueryResponse;
  auto fake_cq_impl = std::make_shared<FakeCompletionQueueImpl>();
  auto fake_clock = std::make_shared<FakeSystemClock>();
  fake_clock->SetTime(std::chrono::system_clock::now());

  promise<StatusOr<PrepareQueryResponse>> promise_1;
  promise<StatusOr<PrepareQueryResponse>> promise_2;
  int calls = 0;

  auto refresh_fn = [&]() {
    ++calls;
    if (calls == 1) return promise_1.get_future();
    if (calls == 2) return promise_2.get_future();
    if (calls == 3) {
      return make_ready_future(StatusOr<PrepareQueryResponse>(
          Status(StatusCode::kUnavailable, "retry failed")));
    }
    PrepareQueryResponse recovered;
    recovered.set_prepared_query("plan-v4");
    return make_ready_future(make_status_or(std::move(recovered)));
  };

  PrepareQueryResponse initial;
  initial.set_prepared_query("plan-v1");
  auto query_plan = QueryPlan::Create(CompletionQueue(fake_cq_impl), initial,
                                      refresh_fn, fake_clock);

  fake_cq_impl->SimulateCompletion(true);
  EXPECT_EQ(calls, 1);

  query_plan->Invalidate(internal::FailedPreconditionError("expired v1"),
                         "plan-v1");

  // Even when the pre-invalidation refresh fails, OnRefreshCompleted still
  // triggers a fresh post-invalidation refresh immediately.
  promise_1.set_value(
      StatusOr<PrepareQueryResponse>(Status(StatusCode::kUnavailable, "old")));
  EXPECT_EQ(calls, 2);

  // If the post-invalidation refresh also fails, state transitions to kBegin so
  // subsequent response() calls retry on demand.
  promise_2.set_value(StatusOr<PrepareQueryResponse>(
      Status(StatusCode::kUnavailable, "follow-up failed")));

  auto failed = query_plan->response();
  EXPECT_THAT(failed, StatusIs(StatusCode::kUnavailable, "retry failed"));
  EXPECT_EQ(calls, 3);

  auto recovered = query_plan->response();
  ASSERT_STATUS_OK(recovered);
  EXPECT_EQ(recovered->prepared_query(), "plan-v4");
  EXPECT_EQ(calls, 4);

  fake_cq_impl->SimulateCompletion(false);
}

}  // namespace
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
