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

#include "google/cloud/bigtable/internal/sessions/middleware/retrying_vrpc.h"
#include "google/cloud/bigtable/internal/sessions/session/write_status.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include <google/rpc/status.pb.h>
#include <gmock/gmock.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace middleware {
namespace {

using ::google::bigtable::v2::VirtualRpcResponse;

TEST(RetryingVRpcTest, CompletesImmediatelyOnSuccess) {
  int factory_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    cb(VirtualRpcResponse());
  };

  bool cb_invoked = false;
  RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_invoked = true;
        EXPECT_TRUE(resp.ok());
      },
      {3, true}, nullptr);

  EXPECT_EQ(factory_calls, 1);
  EXPECT_TRUE(cb_invoked);
}

TEST(RetryingVRpcTest, CompletesImmediatelyOnNonRetryableError) {
  int factory_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    cb(google::cloud::internal::InvalidArgumentError("Bad request format"));
  };

  bool cb_invoked = false;
  RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_invoked = true;
        EXPECT_EQ(resp.status().code(),
                  google::cloud::StatusCode::kInvalidArgument);
      },
      {3, true}, nullptr);

  EXPECT_EQ(factory_calls, 1);
  EXPECT_TRUE(cb_invoked);
}

TEST(RetryingVRpcTest, RetriesOnRetryableErrorsUpToMaxAttempts) {
  int factory_calls = 0;
  std::vector<std::chrono::nanoseconds> scheduled_delays;

  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    if (factory_calls < 3) {
      cb(google::cloud::internal::UnavailableError("Connection contended"));
    } else {
      cb(VirtualRpcResponse());
    }
  };

  auto scheduler = [&](std::chrono::nanoseconds delay,
                       std::function<void()> task) {
    scheduled_delays.push_back(delay);
    task();
  };

  bool cb_invoked = false;
  RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_invoked = true;
        EXPECT_TRUE(resp.ok());
      },
      {3, true}, scheduler);

  EXPECT_EQ(factory_calls, 3);
  EXPECT_TRUE(cb_invoked);
  EXPECT_EQ(scheduled_delays.size(), 2u);
  // Full jitter: attempt n waits in [0, min(1s, 10ms * 2^(n-1))).
  EXPECT_GE(scheduled_delays[0], std::chrono::nanoseconds::zero());
  EXPECT_LT(scheduled_delays[0], std::chrono::milliseconds(10));
  EXPECT_GE(scheduled_delays[1], std::chrono::nanoseconds::zero());
  EXPECT_LT(scheduled_delays[1], std::chrono::milliseconds(20));
}

TEST(RetryingVRpcTest, ExhaustsAttemptsAndReturnsLastError) {
  int factory_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    cb(google::cloud::internal::ResourceExhaustedError("Capacity saturated"));
  };

  auto scheduler = [&](std::chrono::nanoseconds /*delay*/,
                       std::function<void()> task) { task(); };

  bool cb_invoked = false;
  RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_invoked = true;
        EXPECT_EQ(resp.status().code(),
                  google::cloud::StatusCode::kResourceExhausted);
      },
      {3, true}, scheduler);

  EXPECT_EQ(factory_calls, 3);
  EXPECT_TRUE(cb_invoked);
}

TEST(RetryingVRpcTest, SynchronousFallbackWhenSchedulerIsNull) {
  int factory_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    if (factory_calls < 3) {
      cb(google::cloud::internal::UnavailableError("Transient socket error"));
    } else {
      cb(VirtualRpcResponse());
    }
  };

  bool cb_invoked = false;
  RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_invoked = true;
        EXPECT_TRUE(resp.ok());
      },
      {3, true}, nullptr);

  EXPECT_EQ(factory_calls, 3);
  EXPECT_TRUE(cb_invoked);
}

TEST(RetryingVRpcTest, NonIdempotentNoRetryIfMightHaveLeftClient) {
  int factory_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    cb(google::cloud::internal::UnavailableError(
        "Connection dropped after write completed"));
  };

  bool cb_invoked = false;
  RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_invoked = true;
        EXPECT_EQ(resp.status().code(),
                  google::cloud::StatusCode::kUnavailable);
      },
      {3, false}, nullptr);

  EXPECT_EQ(factory_calls, 1);
  EXPECT_TRUE(cb_invoked);
}

TEST(RetryingVRpcTest, NonIdempotRetryIfNeverLeftClient) {
  int factory_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    if (factory_calls < 3) {
      google::cloud::Status status =
          google::cloud::internal::UnavailableError("Write failed pre-socket");
      google::cloud::internal::SetPayload(status, kWriteStatusPayloadUrl, "0");
      cb(status);
    } else {
      cb(VirtualRpcResponse());
    }
  };

  auto scheduler = [&](std::chrono::nanoseconds /*delay*/,
                       std::function<void()> task) { task(); };

  bool cb_invoked = false;
  RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_invoked = true;
        EXPECT_TRUE(resp.ok());
      },
      {3, false}, scheduler);

  EXPECT_EQ(factory_calls, 3);
  EXPECT_TRUE(cb_invoked);
}

TEST(RetryingVRpcTest,
     IdempotentRetriesOnServerErrorWithRetryInfoAndHonorsServerDelay) {
  int factory_calls = 0;
  std::vector<std::chrono::nanoseconds> scheduled_delays;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    if (factory_calls == 1) {
      google::cloud::Status status =
          google::cloud::internal::UnavailableError("Server overloaded");
      MarkServerResult(status);
      SetServerRetryDelay(status, std::chrono::milliseconds(35));
      cb(status);
    } else {
      cb(VirtualRpcResponse());
    }
  };

  auto scheduler = [&](std::chrono::nanoseconds delay,
                       std::function<void()> task) {
    scheduled_delays.push_back(delay);
    task();
  };

  bool cb_invoked = false;
  RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_invoked = true;
        EXPECT_TRUE(resp.ok());
      },
      {3, /*is_idempotent=*/true}, scheduler);

  EXPECT_EQ(factory_calls, 2);
  EXPECT_TRUE(cb_invoked);
  ASSERT_EQ(scheduled_delays.size(), 1u);
  EXPECT_EQ(scheduled_delays[0], std::chrono::milliseconds(35));
}

TEST(RetryingVRpcTest,
     NonIdempotentDoesNotRetryOnServerErrorEvenWithRetryInfo) {
  int factory_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    google::cloud::Status status =
        google::cloud::internal::UnavailableError("Server overloaded");
    MarkServerResult(status);
    SetServerRetryDelay(status, std::chrono::milliseconds(35));
    cb(status);
  };

  bool cb_invoked = false;
  RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_invoked = true;
        EXPECT_EQ(resp.status().code(),
                  google::cloud::StatusCode::kUnavailable);
      },
      {3, /*is_idempotent=*/false},
      [](std::chrono::nanoseconds, std::function<void()> task) { task(); });

  EXPECT_EQ(factory_calls, 1);
  EXPECT_TRUE(cb_invoked);
}

TEST(RetryingVRpcTest, NonIdempotentDoesNotRetryOnServerErrorWithoutRetryInfo) {
  int factory_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    google::cloud::Status status =
        google::cloud::internal::UnavailableError("Server error without retry");
    MarkServerResult(status);
    cb(status);
  };

  bool cb_invoked = false;
  RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        cb_invoked = true;
        EXPECT_EQ(resp.status().code(),
                  google::cloud::StatusCode::kUnavailable);
      },
      {3, /*is_idempotent=*/false},
      [](std::chrono::nanoseconds, std::function<void()> task) { task(); });

  EXPECT_EQ(factory_calls, 1);
  EXPECT_TRUE(cb_invoked);
}

// --- Backoff math -----------------------------------------------------------

TEST(RetryingVRpcBackoffTest, FirstTurnoverIsFastAndJittered) {
  for (double u : {0.0, 0.25, 0.5, 0.999}) {
    auto d = RetryingVRpc::ComputeBackoff(1, /*session_turnover=*/true,
                                          std::chrono::nanoseconds::zero(), u);
    EXPECT_GE(d, std::chrono::nanoseconds::zero());
    EXPECT_LT(d, RetryingVRpc::kMaxTurnoverDelay);
  }
  // Jitter actually spreads the fleet (no lockstep).
  EXPECT_NE(RetryingVRpc::ComputeBackoff(1, true,
                                         std::chrono::nanoseconds::zero(), 0.1),
            RetryingVRpc::ComputeBackoff(
                1, true, std::chrono::nanoseconds::zero(), 0.9));
}

TEST(RetryingVRpcBackoffTest, RepeatedTurnoverFallsBackToExponential) {
  // A second consecutive turnover must not keep retrying at ~0ms.
  auto d = RetryingVRpc::ComputeBackoff(
      2, /*session_turnover=*/true, std::chrono::nanoseconds::zero(), 0.999);
  EXPECT_GT(d, RetryingVRpc::kMaxTurnoverDelay);
  EXPECT_LT(d, std::chrono::milliseconds(20));
}

TEST(RetryingVRpcBackoffTest, ExponentialCapIsTruncatedAtMax) {
  for (int attempt = 1; attempt <= 40; ++attempt) {
    auto d = RetryingVRpc::ComputeBackoff(
        attempt, false, std::chrono::nanoseconds::zero(), 0.999999);
    EXPECT_LT(d, RetryingVRpc::kMaxBackoff) << "attempt=" << attempt;
    EXPECT_GE(d, std::chrono::nanoseconds::zero());
  }
  // Cap doubles: attempt 3 -> 40ms.
  EXPECT_LT(RetryingVRpc::ComputeBackoff(
                3, false, std::chrono::nanoseconds::zero(), 0.999),
            std::chrono::milliseconds(40));
  EXPECT_GT(RetryingVRpc::ComputeBackoff(
                3, false, std::chrono::nanoseconds::zero(), 0.999),
            std::chrono::milliseconds(39));
}

TEST(RetryingVRpcBackoffTest, PolicyDelayIsUsedAsCap) {
  auto d = RetryingVRpc::ComputeBackoff(1, false,
                                        std::chrono::milliseconds(500), 0.5);
  EXPECT_EQ(d, std::chrono::milliseconds(250));
}

TEST(RetryingVRpcBackoffTest, OutOfRangeRandomIsClamped) {
  EXPECT_EQ(RetryingVRpc::ComputeBackoff(
                1, false, std::chrono::nanoseconds::zero(), -1.0),
            std::chrono::nanoseconds::zero());
  EXPECT_LT(RetryingVRpc::ComputeBackoff(1, false,
                                         std::chrono::nanoseconds::zero(), 5.0),
            std::chrono::milliseconds(10));
}

// --- Stop conditions --------------------------------------------------------

TEST(RetryingVRpcTest, ExpiredDeadlineStopsRetries) {
  int factory_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    cb(google::cloud::internal::UnavailableError("transient"));
  };
  RetryingVRpc::Options opts;
  opts.policy.deadline =
      std::chrono::system_clock::now() - std::chrono::seconds(1);
  google::cloud::Status final_status;
  RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
        final_status = r.status();
      },
      opts, [](std::chrono::nanoseconds, std::function<void()> t) { t(); });
  EXPECT_EQ(factory_calls, 1);
  EXPECT_EQ(final_status.code(), google::cloud::StatusCode::kUnavailable);
}

TEST(RetryingVRpcTest, UserRetryPolicyCanStopRetries) {
  int factory_calls = 0;
  int policy_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    cb(google::cloud::internal::UnavailableError("transient"));
  };
  RetryingVRpc::Options opts;
  opts.policy.on_failure = [&](google::cloud::Status const&) {
    return ++policy_calls < 2;  // allow exactly one retry
  };
  RetryingVRpc::Execute(
      factory, [](google::cloud::StatusOr<VirtualRpcResponse>) {}, opts,
      [](std::chrono::nanoseconds, std::function<void()> t) { t(); });
  EXPECT_EQ(factory_calls, 2);
  EXPECT_EQ(policy_calls, 2);
}

TEST(RetryingVRpcTest, UserBackoffPolicyIsConsulted) {
  int backoff_calls = 0;
  std::vector<std::chrono::nanoseconds> delays;
  int factory_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    if (++factory_calls < 3) {
      cb(google::cloud::internal::UnavailableError("transient"));
    } else {
      cb(VirtualRpcResponse());
    }
  };
  RetryingVRpc::Options opts;
  opts.policy.next_backoff = [&]() -> std::chrono::nanoseconds {
    ++backoff_calls;
    return std::chrono::milliseconds(4);
  };
  RetryingVRpc::Execute(
      factory, [](google::cloud::StatusOr<VirtualRpcResponse>) {}, opts,
      [&](std::chrono::nanoseconds d, std::function<void()> t) {
        delays.push_back(d);
        t();
      });
  EXPECT_EQ(backoff_calls, 2);
  for (auto d : delays) EXPECT_LT(d, std::chrono::milliseconds(4));
}

TEST(RetryingVRpcTest, DeadlineExceededRetriedOnlyBeforeOperationDeadline) {
  // Per-attempt timeout with plenty of operation budget left: retry.
  {
    int calls = 0;
    auto factory = [&](RetryingVRpc::GenericCallback cb) {
      if (++calls < 2) {
        cb(google::cloud::internal::DeadlineExceededError("attempt timeout"));
      } else {
        cb(VirtualRpcResponse());
      }
    };
    RetryingVRpc::Options opts;
    opts.policy.deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(30);
    bool ok = false;
    RetryingVRpc::Execute(
        factory,
        [&](google::cloud::StatusOr<VirtualRpcResponse> r) { ok = r.ok(); },
        opts, [](std::chrono::nanoseconds, std::function<void()> t) { t(); });
    EXPECT_EQ(calls, 2);
    EXPECT_TRUE(ok);
  }
  // Operation deadline already passed: surface DEADLINE_EXCEEDED immediately.
  {
    int calls = 0;
    auto factory = [&](RetryingVRpc::GenericCallback cb) {
      ++calls;
      cb(google::cloud::internal::DeadlineExceededError("user deadline"));
    };
    RetryingVRpc::Options opts;
    opts.policy.deadline =
        std::chrono::system_clock::now() - std::chrono::milliseconds(1);
    google::cloud::StatusCode code = google::cloud::StatusCode::kOk;
    RetryingVRpc::Execute(
        factory,
        [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
          code = r.status().code();
        },
        opts, [](std::chrono::nanoseconds, std::function<void()> t) { t(); });
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(code, google::cloud::StatusCode::kDeadlineExceeded);
  }
}

TEST(RetryingVRpcTest, RetryAmplificationIsBoundedByMaxAttempts) {
  // Even with turnover-marked errors (fast path) and a permissive user
  // policy, one operation may never exceed max_attempts vRPCs.
  int factory_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    auto s = google::cloud::internal::UnavailableError("session closed");
    MarkSessionTurnover(s);
    cb(s);
  };
  RetryingVRpc::Options opts;
  opts.max_attempts = 10;  // the oracle still caps at its own limit
  opts.policy.on_failure = [](google::cloud::Status const&) { return true; };
  auto loop = RetryingVRpc::Execute(
      factory, [](google::cloud::StatusOr<VirtualRpcResponse>) {}, opts,
      [](std::chrono::nanoseconds, std::function<void()> t) { t(); });
  EXPECT_LE(factory_calls, 3);
  EXPECT_EQ(loop->attempts_started(), factory_calls);
}

TEST(RetryingVRpcTest, CallbackInvokedExactlyOnceUnderCancelRace) {
  // Races Cancel() against attempt completion on another thread. The user
  // callback must fire exactly once regardless of interleaving.
  for (int iter = 0; iter < 200; ++iter) {
    std::atomic<int> callbacks{0};
    RetryingVRpc::GenericCallback pending;
    std::mutex mu;
    auto factory = [&](RetryingVRpc::GenericCallback cb) {
      std::lock_guard<std::mutex> l(mu);
      pending = std::move(cb);
    };
    auto loop = RetryingVRpc::Execute(
        factory,
        [&](google::cloud::StatusOr<VirtualRpcResponse>) { ++callbacks; },
        RetryingVRpc::Options{}, nullptr);
    RetryingVRpc::GenericCallback cb;
    {
      std::lock_guard<std::mutex> l(mu);
      cb = std::move(pending);
    }
    std::thread completer([&] { cb(VirtualRpcResponse()); });
    std::thread canceller([&] { loop->Cancel(); });
    completer.join();
    canceller.join();
    EXPECT_EQ(callbacks.load(), 1) << "iter=" << iter;
  }
}

TEST(RetryingVRpcTest, CancelBeforeRetryStopsLoop) {
  int factory_calls = 0;
  std::function<void()> scheduled;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    factory_calls++;
    cb(google::cloud::internal::UnavailableError("transient"));
  };
  google::cloud::StatusCode code = google::cloud::StatusCode::kOk;
  RetryingVRpc::Options opts;
  opts.policy.next_backoff = []() -> std::chrono::nanoseconds {
    return std::chrono::milliseconds(100);
  };
  auto loop = RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
        code = r.status().code();
      },
      opts,
      [&](std::chrono::nanoseconds, std::function<void()> t) {
        scheduled = std::move(t);
      });
  loop->Cancel();
  if (scheduled) scheduled();
  EXPECT_EQ(factory_calls, 1);
  EXPECT_EQ(code, google::cloud::StatusCode::kCancelled);
}

TEST(RetryingVRpcTest, DestroyedLoopFulfillsWithUnavailable) {
  google::cloud::StatusCode code = google::cloud::StatusCode::kOk;
  {
    auto loop = RetryingVRpc::Create(
        [](RetryingVRpc::GenericCallback) {},
        [&](google::cloud::StatusOr<VirtualRpcResponse> r) {
          code = r.status().code();
        },
        RetryingVRpc::Options{}, nullptr);
  }
  EXPECT_EQ(code, google::cloud::StatusCode::kUnavailable);
}

TEST(RetryingVRpcTest, UncommittedAbortedAndCancelledAreRetried) {
  for (auto const& base_err :
       {google::cloud::internal::AbortedError("session aborted pre-write"),
        google::cloud::internal::CancelledError("stream cancelled pre-write"),
        google::cloud::internal::InternalError("stream reset pre-write")}) {
    int factory_calls = 0;
    auto factory = [&](RetryingVRpc::GenericCallback cb) {
      if (++factory_calls == 1) {
        google::cloud::Status status = base_err;
        google::cloud::internal::SetPayload(status, kWriteStatusPayloadUrl,
                                            "0");
        cb(status);
      } else {
        cb(VirtualRpcResponse());
      }
    };

    bool ok = false;
    RetryingVRpc::Execute(
        factory,
        [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
          ok = resp.ok();
        },
        {3, /*is_idempotent=*/false},
        [](std::chrono::nanoseconds, std::function<void()> task) { task(); });
    EXPECT_EQ(factory_calls, 2) << base_err;
    EXPECT_TRUE(ok) << base_err;
  }
}

TEST(RetryingVRpcTest, ServerResultWithRetryInfoOnAbortedAndInternalIsRetried) {
  for (auto const& base_err :
       {google::cloud::internal::AbortedError("tablet abort with retry_info"),
        google::cloud::internal::InternalError(
            "transient internal with retry_info")}) {
    int factory_calls = 0;
    std::vector<std::chrono::nanoseconds> delays;
    auto factory = [&](RetryingVRpc::GenericCallback cb) {
      if (++factory_calls == 1) {
        google::cloud::Status status = base_err;
        MarkServerResult(status);
        SetServerRetryDelay(status, std::chrono::milliseconds(25));
        cb(status);
      } else {
        cb(VirtualRpcResponse());
      }
    };

    bool ok = false;
    RetryingVRpc::Execute(
        factory,
        [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
          ok = resp.ok();
        },
        {3, /*is_idempotent=*/true},
        [&](std::chrono::nanoseconds d, std::function<void()> task) {
          delays.push_back(d);
          task();
        });
    EXPECT_EQ(factory_calls, 2) << base_err;
    EXPECT_TRUE(ok) << base_err;
    ASSERT_EQ(delays.size(), 1u);
    EXPECT_EQ(delays[0], std::chrono::milliseconds(25));
  }
}

TEST(RetryingVRpcTest,
     ServerResultWithoutRetryInfoIsNotRetriedEvenOnUnavailable) {
  int factory_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    ++factory_calls;
    google::cloud::Status status = google::cloud::internal::UnavailableError(
        "Server ErrorResponse without RetryInfo");
    MarkServerResult(status);
    cb(status);
  };

  google::cloud::StatusCode final_code = google::cloud::StatusCode::kOk;
  RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) {
        final_code = resp.status().code();
      },
      {3, /*is_idempotent=*/true},
      [](std::chrono::nanoseconds, std::function<void()> task) { task(); });
  EXPECT_EQ(factory_calls, 1);
  EXPECT_EQ(final_code, google::cloud::StatusCode::kUnavailable);
}

TEST(RetryingVRpcTest,
     ZeroDurationServerRetryDelayHonoredWithoutClientBackoff) {
  int factory_calls = 0;
  int backoff_policy_calls = 0;
  int scheduler_calls = 0;
  auto factory = [&](RetryingVRpc::GenericCallback cb) {
    if (++factory_calls == 1) {
      google::cloud::Status status = google::cloud::internal::UnavailableError(
          "Immediate retry requested by server");
      MarkServerResult(status);
      SetServerRetryDelay(status, std::chrono::nanoseconds::zero());
      cb(status);
    } else {
      cb(VirtualRpcResponse());
    }
  };

  RetryingVRpc::Options opts;
  opts.max_attempts = 3;
  opts.is_idempotent = true;
  opts.policy.next_backoff = [&]() -> std::chrono::nanoseconds {
    ++backoff_policy_calls;
    return std::chrono::milliseconds(500);
  };

  bool ok = false;
  RetryingVRpc::Execute(
      factory,
      [&](google::cloud::StatusOr<VirtualRpcResponse> resp) { ok = resp.ok(); },
      opts,
      [&](std::chrono::nanoseconds, std::function<void()> task) {
        ++scheduler_calls;
        task();
      });

  EXPECT_EQ(factory_calls, 2);
  EXPECT_TRUE(ok);
  EXPECT_EQ(backoff_policy_calls, 0);
  EXPECT_EQ(scheduler_calls, 0);
}

}  // namespace
}  // namespace middleware
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
