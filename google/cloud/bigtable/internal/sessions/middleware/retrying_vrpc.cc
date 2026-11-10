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
#include "google/cloud/bigtable/internal/sessions/logging_helper.h"
#include "google/cloud/bigtable/internal/sessions/middleware/retry_oracle.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/internal/random.h"
#include "google/cloud/status.h"
#include "google/cloud/status_or.h"
#include "absl/strings/str_cat.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace middleware {

std::shared_ptr<RetryingVRpc> RetryingVRpc::Create(
    OperationFactory factory, GenericCallback user_callback, Options options,
    ScheduleFn scheduler) {
  return std::shared_ptr<RetryingVRpc>(
      new RetryingVRpc(std::move(factory), std::move(user_callback),
                       std::move(options), std::move(scheduler)));
}

std::shared_ptr<RetryingVRpc> RetryingVRpc::Execute(
    OperationFactory factory, GenericCallback user_callback, Options options,
    ScheduleFn scheduler) {
  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      2, "RetryingVRpc",
      absl::StrCat("Execute - Provisioning retry pipeline, max_attempts: ",
                   options.max_attempts,
                   ", idempotent: ", options.is_idempotent,
                   ", has_scheduler: ", scheduler != nullptr));
  auto interceptor = Create(std::move(factory), std::move(user_callback),
                            std::move(options), std::move(scheduler));
  interceptor->StartAttempt();
  return interceptor;
}

RetryingVRpc::RetryingVRpc(OperationFactory factory,
                           GenericCallback user_callback, Options options,
                           ScheduleFn scheduler)
    : factory_(std::move(factory)),
      user_callback_(std::move(user_callback)),
      options_(std::move(options)),
      scheduler_(std::move(scheduler)) {}

RetryingVRpc::~RetryingVRpc() {
  if (!fulfilled_) {
    Fulfill(google::cloud::internal::UnavailableError(
        "Retry loop terminated before completion"));
  }
}

void RetryingVRpc::Cancel() {
  cancelled_ = true;
  Fulfill(
      google::cloud::internal::CancelledError("Retry loop cancelled by user"));
}

void RetryingVRpc::Fulfill(
    google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse> result) {
  GenericCallback cb;
  OperationFactory factory_to_drop;
  ScheduleFn scheduler_to_drop;
  {
    std::lock_guard<std::mutex> l(mu_);
    if (fulfilled_) return;
    fulfilled_ = true;
    cb = std::move(user_callback_);
    // Break any reference cycles where `factory_` or `scheduler_` captures a
    // `shared_ptr<RetryingVRpc>` back to `this`.
    factory_to_drop = std::move(factory_);
    scheduler_to_drop = std::move(scheduler_);
  }
  if (cb != nullptr) {
    cb(std::move(result));
  }
}

void RetryingVRpc::StartAttempt() {
  if (cancelled_) {
    Fulfill(google::cloud::internal::CancelledError(
        "Retry loop cancelled by user"));
    return;
  }
  OperationFactory factory;
  {
    std::lock_guard<std::mutex> l(mu_);
    if (fulfilled_) return;
    factory = factory_;
  }
  current_attempt_.fetch_add(1, std::memory_order_acq_rel);
  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      2, "RetryingVRpc",
      absl::StrCat("StartAttempt - Initiating attempt: ",
                   current_attempt_.load(std::memory_order_relaxed), "/",
                   options_.max_attempts));

  auto interceptor_cb =
      [self = shared_from_this()](
          google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse>
              resp) mutable { self->HandleAttemptResult(std::move(resp)); };

  if (factory != nullptr) {
    factory(std::move(interceptor_cb));
  }
}

std::chrono::nanoseconds RetryingVRpc::ComputeBackoff(
    int attempt, bool session_turnover, std::chrono::nanoseconds policy_delay,
    double unit_random) {
  if (unit_random < 0.0) unit_random = 0.0;
  if (unit_random >= 1.0) unit_random = 0.999999;
  // A session turnover (GoAway / local close / transport EOF) on the first
  // failure is re-routed almost immediately to another session. The small
  // jitter prevents every client hit by the same GoAway wave from retrying
  // in lockstep. Repeated turnovers fall through to regular backoff so a
  // sustained backend outage cannot produce a tight retry loop.
  if (session_turnover && attempt <= 1) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::duration<double, std::nano>(kMaxTurnoverDelay) *
        unit_random);
  }
  std::chrono::nanoseconds cap = policy_delay;
  if (cap <= std::chrono::nanoseconds::zero()) {
    int shift = std::min(std::max(attempt - 1, 0), 16);
    cap = std::min(kMaxBackoff, kInitialBackoff * (std::int64_t{1} << shift));
  }
  // Full jitter: uniform in [0, cap).
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::duration<double, std::nano>(cap) * unit_random);
}

int RetryingVRpc::attempts_started() const {
  return current_attempt_.load(std::memory_order_acquire);
}

void RetryingVRpc::HandleAttemptResult(
    google::cloud::StatusOr<google::bigtable::v2::VirtualRpcResponse> result) {
  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      2, "RetryingVRpc",
      absl::StrCat("HandleAttemptResult - Attempt completed, ok: ", result.ok(),
                   ", status: ", result.status().message()));

  if (result.ok() || cancelled_.load(std::memory_order_acquire)) {
    Fulfill(std::move(result));
    return;
  }

  auto const& status = result.status();
  int const attempt = current_attempt_.load(std::memory_order_acquire);
  bool const turnover = IsSessionTurnover(status);

  // Classify the attempt outcome for the four-state retry oracle.
  session::AttemptOutcomeState outcome_state;
  bool has_retry_info = false;
  auto const server_retry_delay = GetServerRetryDelay(status);
  if (IsPoolRejected(status)) {
    outcome_state = session::AttemptOutcomeState::kRejected;
  } else if (NeverLeftClient(status)) {
    outcome_state = session::AttemptOutcomeState::kUncommitted;
  } else if (IsServerResult(status)) {
    outcome_state = session::AttemptOutcomeState::kServerResult;
    has_retry_info = server_retry_delay.has_value();
  } else if (IsRetryable(status)) {
    outcome_state = session::AttemptOutcomeState::kTransportFailure;
    has_retry_info = true;
  } else {
    outcome_state = session::AttemptOutcomeState::kServerResult;
  }

  // Next delay: if the server supplied a non-negative RetryInfo.retry_delay on
  // a kServerResult, honor it directly; otherwise consult user backoff policy
  // and apply full jitter.
  auto delay = std::chrono::nanoseconds::zero();
  if (server_retry_delay.has_value() &&
      *server_retry_delay >= std::chrono::nanoseconds::zero()) {
    delay = *server_retry_delay;
  } else {
    auto policy_delay = std::chrono::nanoseconds::zero();
    if (options_.policy.next_backoff) {
      policy_delay = options_.policy.next_backoff();
    }
    thread_local google::cloud::internal::DefaultPRNG gen =
        google::cloud::internal::MakeDefaultPRNG();
    double const unit = std::uniform_real_distribution<double>(0.0, 1.0)(gen);
    delay = ComputeBackoff(attempt, turnover, policy_delay, unit);
  }

  // Remaining time is measured against the real operation deadline, never a
  // hardcoded constant. An infinite deadline is clamped to a large finite
  // value for the oracle's arithmetic.
  std::chrono::nanoseconds remaining = std::chrono::hours(1);
  if (options_.policy.deadline !=
      (std::chrono::system_clock::time_point::max)()) {
    remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(
        options_.policy.deadline - std::chrono::system_clock::now());
  }
  auto decision =
      EvaluateRetryOracle(outcome_state, options_.is_idempotent, has_retry_info,
                          delay, attempt, remaining);
  bool can_retry = decision.should_retry;
  // The delay must fit inside the remaining deadline (`remaining - delay >
  // 1ms`), including for `kUncommitted` and `kTransportFailure` outcomes.
  if (can_retry && remaining - delay <= std::chrono::milliseconds(1)) {
    can_retry = false;
  }
  if (can_retry && options_.policy.on_failure &&
      !options_.policy.on_failure(status)) {
    can_retry = false;
  }
  if (attempt >= options_.max_attempts) can_retry = false;

  if (!can_retry) {
    ::google::cloud::bigtable_internal::sessions::internal::LogVerboseWarning(
        2, "RetryingVRpc",
        absl::StrCat("HandleAttemptResult - Terminating attempt iteration "
                     "cleanly, returning error to user callback, code: ",
                     status.message()));
    Fulfill(std::move(result));
    return;
  }

  ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
      2, "RetryingVRpc",
      absl::StrCat("HandleAttemptResult - Attempt failed with retryable error; "
                   "scheduling retry in ns: ",
                   delay.count()));

  ScheduleFn scheduler;
  {
    std::lock_guard<std::mutex> l(mu_);
    if (fulfilled_) return;
    scheduler = scheduler_;
  }
  if (scheduler != nullptr && delay > std::chrono::nanoseconds::zero()) {
    scheduler(delay, [self = shared_from_this()]() { self->StartAttempt(); });
  } else {
    StartAttempt();
  }
}

bool RetryingVRpc::IsRetryable(google::cloud::Status const& status) const {
  google::cloud::StatusCode code = status.code();
  if (code == google::cloud::StatusCode::kUnavailable ||
      code == google::cloud::StatusCode::kResourceExhausted) {
    return true;
  }
  // DEADLINE_EXCEEDED is only retryable while the *operation* deadline still
  // has room: it then denotes a per-attempt timeout, not user deadline expiry.
  if (code == google::cloud::StatusCode::kDeadlineExceeded) {
    return options_.policy.deadline ==
               (std::chrono::system_clock::time_point::max)() ||
           std::chrono::system_clock::now() < options_.policy.deadline;
  }
  return false;
}

}  // namespace middleware
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
