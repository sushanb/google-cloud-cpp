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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SESSION_WRITE_STATUS_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SESSION_WRITE_STATUS_H

#include "google/cloud/status.h"
#include "google/cloud/version.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include <chrono>
#include <cstdint>
#include <optional>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

/// Payload key attached to status failures when the request was not written to
/// the transport stream (`"0"`) or after write completion (`"1"`).
constexpr char kWriteStatusPayloadUrl[] =
    "type.googleapis.com/bigtable.client.sessions.WriteStatus";

/// Payload key attached to failures caused by session turnover (`GoAway`
/// rotation, local close, or transport termination) rather than server request
/// rejection, allowing the retry middleware to use a fast re-route delay
/// instead of exponential backoff.
constexpr char kSessionTurnoverPayloadUrl[] =
    "type.googleapis.com/bigtable.client.sessions.SessionTurnover";

/// Payload key attached when a failure came from an explicit
/// `SessionResponse::kError` frame sent by the server (`kServerResult`), rather
/// than a transport stream drop after `write_completed` (`kTransportFailure`).
constexpr char kServerResultPayloadUrl[] =
    "type.googleapis.com/bigtable.client.sessions.ServerResult";

/// Payload key carrying the nanosecond `google.rpc.RetryInfo.retry_delay`
/// extracted from `ErrorResponse.retry_info` when the server explicitly grants
/// retry permission on a `SessionResponse::kError` frame.
constexpr char kServerRetryDelayPayloadUrl[] =
    "type.googleapis.com/bigtable.client.sessions.ServerRetryDelayNs";

/// Payload key attached when the `SessionPool` rejects a request before it
/// reaches any session (waiter queue full, pool closed, terminal handshake
/// error, or circuit breaker open). Maps to `AttemptOutcomeState::kRejected`.
constexpr char kPoolRejectedPayloadUrl[] =
    "type.googleapis.com/bigtable.client.sessions.PoolRejected";

inline void MarkPoolRejected(google::cloud::Status& status) {
  if (status.ok()) return;
  google::cloud::internal::SetPayload(status, kWriteStatusPayloadUrl, "0");
  google::cloud::internal::SetPayload(status, kPoolRejectedPayloadUrl, "1");
}

inline bool IsPoolRejected(google::cloud::Status const& status) {
  if (status.ok()) return false;
  auto payload =
      google::cloud::internal::GetPayload(status, kPoolRejectedPayloadUrl);
  return payload.has_value() && *payload == "1";
}

inline void MarkSessionTurnover(google::cloud::Status& status) {
  if (status.ok()) return;
  google::cloud::internal::SetPayload(status, kSessionTurnoverPayloadUrl, "1");
}

inline bool IsSessionTurnover(google::cloud::Status const& status) {
  if (status.ok()) return false;
  auto payload =
      google::cloud::internal::GetPayload(status, kSessionTurnoverPayloadUrl);
  return payload.has_value() && *payload == "1";
}

inline void MarkServerResult(google::cloud::Status& status) {
  if (status.ok()) return;
  google::cloud::internal::SetPayload(status, kServerResultPayloadUrl, "1");
}

inline bool IsServerResult(google::cloud::Status const& status) {
  if (status.ok()) return false;
  auto payload =
      google::cloud::internal::GetPayload(status, kServerResultPayloadUrl);
  return payload.has_value() && *payload == "1";
}

inline void SetServerRetryDelay(google::cloud::Status& status,
                                std::chrono::nanoseconds delay) {
  if (status.ok()) return;
  google::cloud::internal::SetPayload(status, kServerRetryDelayPayloadUrl,
                                      absl::StrCat(delay.count()));
}

inline std::optional<std::chrono::nanoseconds> GetServerRetryDelay(
    google::cloud::Status const& status) {
  if (status.ok()) return std::nullopt;
  auto payload =
      google::cloud::internal::GetPayload(status, kServerRetryDelayPayloadUrl);
  if (!payload.has_value()) return std::nullopt;
  std::int64_t ns = 0;
  if (!absl::SimpleAtoi(*payload, &ns)) return std::nullopt;
  return std::chrono::nanoseconds(ns);
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SESSION_WRITE_STATUS_H
