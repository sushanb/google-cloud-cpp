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

#include "google/cloud/bigtable/internal/sessions/middleware/cancellable_vrpc.h"
#include "google/cloud/bigtable/internal/sessions/logging_helper.h"
#include "google/cloud/bigtable/internal/sessions/session/write_status.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/status.h"
#include <grpcpp/client_context.h>
#include <chrono>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace middleware {

void CancellableVRpc::Intercept(grpc::ClientContext* context,
                                OperationFn operation,
                                GenericCallback callback) {
  if (context == nullptr) {
    ::google::cloud::bigtable_internal::sessions::internal::LogVerbose(
        2, "CancellableVRpc",
        "Intercept - ClientContext is null; passing through "
        "downstream directly");
    if (operation != nullptr) {
      operation(std::move(callback));
    }
    return;
  }

  Intercept(context->deadline(), std::move(operation), std::move(callback));
}

void CancellableVRpc::Intercept(std::chrono::system_clock::time_point deadline,
                                OperationFn operation,
                                GenericCallback callback) {
  auto const max_time = (std::chrono::system_clock::time_point::max)();
  if (deadline < max_time) {
    if (std::chrono::system_clock::now() >= deadline) {
      ::google::cloud::bigtable_internal::sessions::internal::LogVerboseWarning(
          2, "CancellableVRpc",
          "Intercept - Client context deadline exceeded prior to operation "
          "dispatch; short-circuiting call");
      if (callback != nullptr) {
        google::cloud::Status status =
            google::cloud::internal::DeadlineExceededError(
                "Client context deadline exceeded prior to operation dispatch");
        google::cloud::internal::SetPayload(status, kWriteStatusPayloadUrl,
                                            "0");
        callback(status);
      }
      return;
    }
  }

  if (operation != nullptr) {
    operation(std::move(callback));
  }
}

}  // namespace middleware
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
