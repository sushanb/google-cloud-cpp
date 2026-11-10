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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_CLIENT_SESSION_CLIENT_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_CLIENT_SESSION_CLIENT_H

#include "google/cloud/bigtable/internal/sessions/middleware/retry_oracle.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_list.h"
#include "google/cloud/status_or.h"
#include "google/cloud/version.h"
#include "absl/strings/str_cat.h"
#include <google/bigtable/v2/data.pb.h>
#include <string>
#include <string_view>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace client {

/// Resource types addressed by a session pool.
enum class SessionResourceType {
  kTable = 1,
  kAuthorizedView = 2,
  kMaterializedView = 3,
};

/// Access mode requested when opening a session pool.
enum class PoolPermission {
  kRead = 1,
  kWrite = 2,
};

/// Formats the canonical 4-part pool key
/// `"{table|av|mv}:<resource_name>:<app_profile>:{READ|WRITE}"`.
///
/// Materialized views are read-only; requesting `kWrite` returns
/// `kInvalidArgument` client-side without opening a pool.
inline StatusOr<std::string> MakePoolKey(SessionResourceType type,
                                         std::string_view resource_name,
                                         std::string_view app_profile_id,
                                         PoolPermission perm) {
  if (type == SessionResourceType::kMaterializedView &&
      perm == PoolPermission::kWrite) {
    return Status(StatusCode::kInvalidArgument,
                  "Writes are not supported on MaterializedViews");
  }
  std::string_view prefix = "table";
  if (type == SessionResourceType::kAuthorizedView) {
    prefix = "av";
  } else if (type == SessionResourceType::kMaterializedView) {
    prefix = "mv";
  }
  std::string_view const profile =
      app_profile_id.empty() ? "default" : app_profile_id;
  std::string_view const dir =
      (perm == PoolPermission::kRead) ? "READ" : "WRITE";
  return absl::StrCat(prefix, ":", resource_name, ":", profile, ":", dir);
}

/// Overload defaulting `app_profile_id` to `"default"`.
inline StatusOr<std::string> MakePoolKey(SessionResourceType type,
                                         std::string_view resource_name,
                                         PoolPermission perm) {
  return MakePoolKey(type, resource_name, "default", perm);
}

/// Evaluates whether a single `google::bigtable::v2::Mutation` is safe to
/// retry under `SafeIdempotentMutationPolicy` semantics.
inline bool IsIdempotentMutation(google::bigtable::v2::Mutation const& m) {
  switch (m.mutation_case()) {
    case google::bigtable::v2::Mutation::kSetCell:
      return m.set_cell().timestamp_micros() != -1;
    case google::bigtable::v2::Mutation::kDeleteFromColumn:
    case google::bigtable::v2::Mutation::kDeleteFromFamily:
    case google::bigtable::v2::Mutation::kDeleteFromRow:
      return true;
    case google::bigtable::v2::Mutation::kAddToCell:
    case google::bigtable::v2::Mutation::kMergeToCell:
    case google::bigtable::v2::Mutation::MUTATION_NOT_SET:
      return false;
  }
  return false;
}

}  // namespace client
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_CLIENT_SESSION_CLIENT_H
