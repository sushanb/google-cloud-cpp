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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TRANSPORT_CURRENT_CQ_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TRANSPORT_CURRENT_CQ_H

#include "google/cloud/completion_queue.h"
#include "google/cloud/version.h"
#include <optional>

namespace google {
namespace cloud {
namespace internal {
class CompletionQueueImpl;
}  // namespace internal
}  // namespace cloud
}  // namespace google

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

/// Tracks the `CompletionQueueImpl` active on the current event-loop thread so
/// downstream layers can preserve thread-local shard affinity.
inline thread_local ::google::cloud::internal::CompletionQueueImpl*
    current_cq_impl = nullptr;

/// RAII scope guard that binds `current_cq_impl` to `cq` for the duration of a
/// callback.
class CurrentCqGuard {
 public:
  explicit CurrentCqGuard(::google::cloud::CompletionQueue const& cq)
      : old_cq_(current_cq_impl) {
    current_cq_impl =
        ::google::cloud::internal::GetCompletionQueueImpl(cq).get();
  }
  CurrentCqGuard(CurrentCqGuard const&) = delete;
  CurrentCqGuard& operator=(CurrentCqGuard const&) = delete;
  ~CurrentCqGuard() { current_cq_impl = old_cq_; }

 private:
  ::google::cloud::internal::CompletionQueueImpl* old_cq_;
};

/// Conditionally installs a `CurrentCqGuard` when an event `CompletionQueue` is
/// present.
class SessionCallbackGuard {
 public:
  explicit SessionCallbackGuard(
      std::optional<google::cloud::CompletionQueue> const& cq) {
    if (cq.has_value()) {
      cq_guard_.emplace(*cq);
    }
  }
  SessionCallbackGuard(SessionCallbackGuard const&) = delete;
  SessionCallbackGuard& operator=(SessionCallbackGuard const&) = delete;

 private:
  std::optional<CurrentCqGuard> cq_guard_;
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TRANSPORT_CURRENT_CQ_H
