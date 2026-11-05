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

#include "google/cloud/bigtable/internal/retry_traits.h"
#include "google/cloud/internal/status_payload_keys.h"
#include "google/cloud/status.h"
#include "absl/strings/match.h"
#include "google/rpc/error_details.pb.h"
#include "google/rpc/status.pb.h"

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace {

constexpr char kNonRetryableAfterResumeTokenPayloadKey[] =
    "gcloud-cpp.bigtable.query-plan-refresh.non-retryable";

enum class QueryPlanExpirationKind {
  kNotExpired,
  kExpiredRetryable,
  kExpiredPermanent,
};

QueryPlanExpirationKind ClassifyQueryPlanExpiration(Status const& s) {
  if (s.code() != StatusCode::kFailedPrecondition) {
    return QueryPlanExpirationKind::kNotExpired;
  }
  auto payload = google::cloud::internal::GetPayload(
      s, google::cloud::internal::StatusPayloadGrpcProto());
  google::rpc::Status proto;
  if (payload && proto.ParseFromString(*payload)) {
    bool has_precondition_violations = false;
    bool has_permanent_expiry = false;
    google::rpc::PreconditionFailure failure;
    for (google::protobuf::Any const& any : proto.details()) {
      if (any.UnpackTo(&failure)) {
        for (auto const& v : failure.violations()) {
          has_precondition_violations = true;
          if (v.type() == "PREPARED_QUERY_EXPIRED") {
            return QueryPlanExpirationKind::kExpiredRetryable;
          }
          if (v.type() == "PREPARED_QUERY_EXPIRED_FAIL_PERMANENTLY") {
            has_permanent_expiry = true;
          }
        }
      }
    }
    if (has_permanent_expiry) {
      return QueryPlanExpirationKind::kExpiredPermanent;
    }
    if (has_precondition_violations) {
      return QueryPlanExpirationKind::kNotExpired;
    }
  }
  if (absl::StrContains(s.message(),
                        "PREPARED_QUERY_EXPIRED_FAIL_PERMANENTLY")) {
    return QueryPlanExpirationKind::kExpiredPermanent;
  }
  if (absl::StrContains(s.message(), "PREPARED_QUERY_EXPIRED")) {
    return QueryPlanExpirationKind::kExpiredRetryable;
  }
  return QueryPlanExpirationKind::kNotExpired;
}

}  // namespace

bool ExecuteQueryPlanRefreshRetry::IsQueryPlanExpired(Status const& s) {
  if (google::cloud::internal::GetPayload(
          s, kNonRetryableAfterResumeTokenPayloadKey)
          .has_value()) {
    return false;
  }
  return ClassifyQueryPlanExpiration(s) ==
         QueryPlanExpirationKind::kExpiredRetryable;
}

bool ExecuteQueryPlanRefreshRetry::ShouldInvalidateQueryPlan(Status const& s) {
  return ClassifyQueryPlanExpiration(s) != QueryPlanExpirationKind::kNotExpired;
}

void ExecuteQueryPlanRefreshRetry::MarkNonRetryableAfterResumeToken(Status& s) {
  google::cloud::internal::SetPayload(
      s, kNonRetryableAfterResumeTokenPayloadKey, "true");
}

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
