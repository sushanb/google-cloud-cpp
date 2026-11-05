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

#include "google/cloud/bigtable/internal/partial_result_set_resume.h"
#include "google/cloud/internal/retry_loop_helpers.h"
#include "google/cloud/internal/status_payload_keys.h"
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN

void PartialResultSetResume::TryCancel() { reader_->TryCancel(); }

bool PartialResultSetResume::Read(
    std::optional<std::string> const& resume_token,
    UnownedPartialResultSet& result) {
  bool resumption = false;
  std::unique_ptr<BackoffPolicy> backoff;
  while (true) {
    if (reader_->Read(resume_token, result)) {
      // Let the caller know if we recreated the PartialResultSetReader using
      // the resume_token so that they might discard any previous results that
      // will be contained in the new stream.
      if (resumption) result.resumption = true;
      return true;
    }
    auto status = Finish();
    if (status.ok()) return false;
    if (!resume_token) {
      // Our caller has requested that we not try to resume the stream,
      // probably because they have already delivered previous results that
      // would otherwise be replayed.
      return false;
    }
    if (!backoff) backoff = backoff_policy_prototype_->clone();
    auto delay =
        internal::Backoff(status, "ExecuteQuery", *retry_policy_, *backoff,
                          idempotency_, enable_server_retries_);
    if (!delay) {
      last_status_ = std::move(delay).status();
      if (auto payload = internal::GetPayload(
              status, internal::StatusPayloadGrpcProto())) {
        internal::SetPayload(*last_status_, internal::StatusPayloadGrpcProto(),
                             *std::move(payload));
      }
      return false;
    }
    sleeper_(*delay);
    resumption = true;
    last_status_.reset();
    reader_ = factory_(*resume_token);
  }
}

grpc::ClientContext const& PartialResultSetResume::context() const {
  return reader_->context();
}

Status PartialResultSetResume::Finish() {
  // Finish() can be called only once, so cache the last result.
  if (last_status_.has_value()) {
    return *last_status_;
  }
  last_status_ = reader_->Finish();
  return *last_status_;
}

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
