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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_MIDDLEWARE_RETRY_ORACLE_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_MIDDLEWARE_RETRY_ORACLE_H

#include "google/cloud/bigtable/internal/sessions/session/session_state.h"
#include "google/cloud/status.h"
#include "google/cloud/version.h"
#include <chrono>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace middleware {

/// Three-state retry oracle decision for a virtual RPC attempt.
struct RetryDecision {
  bool should_retry = false;
  std::chrono::nanoseconds backoff_delay = std::chrono::nanoseconds::zero();
};

/// Evaluates whether a virtual RPC attempt should be retried based on the
/// four-state outcome classification (`kUncommitted`, `kTransportFailure`,
/// `kServerResult`, `kRejected`), request idempotency, server `RetryInfo`, the
/// 3-attempt cap (`kMaxAttempts = 3`), and the deadline-fit check (`remaining -
/// delay > 1ms`).
inline RetryDecision EvaluateRetryOracle(
    session::AttemptOutcomeState outcome_state, bool is_idempotent,
    bool has_server_retry_info, std::chrono::nanoseconds server_retry_delay,
    int attempt_number, std::chrono::nanoseconds remaining_deadline) {
  constexpr int kMaxAttempts = 3;
  constexpr auto kMinDeadlineMargin = std::chrono::milliseconds(1);

  if (attempt_number >= kMaxAttempts) {
    return {false, std::chrono::nanoseconds::zero()};
  }

  switch (outcome_state) {
    case session::AttemptOutcomeState::kRejected:
      // Pool-level rejection (queue full, pool closed, terminal handshake
      // error, or circuit breaker open) -> never retry in RetryingVRpc.
      return {false, std::chrono::nanoseconds::zero()};

    case session::AttemptOutcomeState::kUncommitted:
      // Rejected by a session before leaving client -> retry unconditionally if
      // deadline permits.
      if (remaining_deadline > kMinDeadlineMargin) {
        return {true, std::chrono::nanoseconds::zero()};
      }
      return {false, std::chrono::nanoseconds::zero()};

    case session::AttemptOutcomeState::kTransportFailure:
      // Written to wire, no server response (e.g. missed heartbeat / EOF) ->
      // retry ONLY if the operation is idempotent.
      if (is_idempotent && remaining_deadline > kMinDeadlineMargin) {
        return {true, std::chrono::nanoseconds::zero()};
      }
      return {false, std::chrono::nanoseconds::zero()};

    case session::AttemptOutcomeState::kServerResult:
      // Server returned ErrorResponse -> retry ONLY if the operation is
      // idempotent, RetryInfo is present, and the delay fits within the
      // remaining deadline (`remaining - delay > 1ms`). Non-idempotent writes
      // must never be retried after leaving the client, even when the server
      // attaches RetryInfo on a transient error.
      if (is_idempotent && has_server_retry_info &&
          (remaining_deadline - server_retry_delay) > kMinDeadlineMargin) {
        return {true, server_retry_delay};
      }
      return {false, std::chrono::nanoseconds::zero()};
  }
  return {false, std::chrono::nanoseconds::zero()};
}

}  // namespace middleware
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_MIDDLEWARE_RETRY_ORACLE_H
