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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_POOL_EXECUTORS_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_POOL_EXECUTORS_H

#include "google/cloud/bigtable/internal/sessions/latch.h"
#include "google/cloud/bigtable/internal/sessions/pool/lightweight_thread_pool.h"
#include "google/cloud/completion_queue.h"
#include "google/cloud/version.h"
#include <atomic>
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

/// Layer 3 (Pool): the I/O event loops and user-callback offload threads used
/// by session pools.
///
/// A `SessionsDataConnection` creates exactly one `PoolExecutors` and shares
/// it with every `{table, permission}` pool it creates (via
/// `SessionsPoolExecutorsOption`). Thread count is therefore bounded per
/// connection (`num_event_loops + num_callback_threads`) instead of growing
/// linearly with the number of tables.
///
/// Pools that do not receive shared executors create a private instance, which
/// preserves the previous standalone behavior (used by unit tests).
class PoolExecutors {
 public:
  /// Creates executors with `num_event_loops` single-threaded completion queues
  /// and a callback pool with `num_callback_threads` workers. Zero values are
  /// replaced with `std::thread::hardware_concurrency()` (or 4 if unknown).
  static std::shared_ptr<PoolExecutors> Create(
      std::size_t num_event_loops, std::size_t num_callback_threads);

  ~PoolExecutors();
  PoolExecutors(PoolExecutors const&) = delete;
  PoolExecutors& operator=(PoolExecutors const&) = delete;

  std::size_t num_event_loops() const { return cqs_.size(); }
  std::size_t num_callback_threads() const { return num_callback_threads_; }

  /// Completion queue for event loop `index % num_event_loops()`.
  google::cloud::CompletionQueue cq(std::size_t index) const;

  /// Shared user-callback offload pool. Never null while `*this` is alive.
  LightweightThreadPool* callback_pool() const { return callback_pool_.get(); }

  /// Cancels all pending operations on every event loop. Only the owner of
  /// the executors (the connection, or a pool with private executors) may
  /// call this, after all pools using the executors have been closed.
  void CancelAll();

  /// Shuts down the callback pool and all event loops. Idempotent.
  void Shutdown();

  /// Total number of OS threads owned by these executors.
  std::size_t thread_count() const {
    return cqs_.size() + num_callback_threads_;
  }

 private:
  struct EventLoop {
    google::cloud::CompletionQueue cq;
    std::thread thread;
    std::thread::id thread_id;
    Latch done;
  };

  PoolExecutors(std::size_t num_event_loops, std::size_t num_callback_threads);

  std::vector<std::unique_ptr<EventLoop>> event_loops_;
  std::vector<google::cloud::CompletionQueue> cqs_;
  std::vector<std::thread::id> loop_thread_ids_;
  std::size_t num_callback_threads_ = 0;
  std::shared_ptr<LightweightThreadPool> callback_pool_;
  std::atomic<bool> shutdown_{false};
};

/// Internal option carrying connection-scoped executors into
/// `SessionPoolImpl::Create`.
struct SessionsPoolExecutorsOption {
  using Type = std::shared_ptr<PoolExecutors>;
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_POOL_EXECUTORS_H
