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

#include "google/cloud/bigtable/internal/sessions/pool/pool_executors.h"
#include "google/cloud/bigtable/internal/sessions/latch.h"
#include "google/cloud/bigtable/internal/sessions/transport/current_cq.h"
#include <thread>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

namespace {
std::size_t DefaultThreads(std::size_t requested) {
  if (requested > 0) return requested;
  auto const hc = std::thread::hardware_concurrency();
  return hc == 0 ? 4 : hc;
}
}  // namespace

std::shared_ptr<PoolExecutors> PoolExecutors::Create(
    std::size_t num_event_loops, std::size_t num_callback_threads) {
  return std::shared_ptr<PoolExecutors>(
      new PoolExecutors(num_event_loops, num_callback_threads));
}

PoolExecutors::PoolExecutors(std::size_t num_event_loops,
                             std::size_t num_callback_threads)
    : num_callback_threads_(DefaultThreads(num_callback_threads)) {
  auto const loops = DefaultThreads(num_event_loops);
  event_loops_.reserve(loops);
  cqs_.reserve(loops);
  loop_thread_ids_.reserve(loops);
  for (std::size_t i = 0; i != loops; ++i) {
    auto loop = std::make_unique<EventLoop>();
    cqs_.push_back(loop->cq);
    Latch started;
    loop->thread = std::thread([loop_ptr = loop.get(), &started]() {
      CurrentCqGuard guard(loop_ptr->cq);
      loop_ptr->thread_id = std::this_thread::get_id();
      started.Notify();
      loop_ptr->cq.Run();
      loop_ptr->done.Notify();
    });
    started.Wait();
    loop_thread_ids_.push_back(loop->thread_id);
    event_loops_.push_back(std::move(loop));
  }
  callback_pool_ =
      std::make_shared<LightweightThreadPool>(num_callback_threads_, 0);
}

PoolExecutors::~PoolExecutors() { Shutdown(); }

void PoolExecutors::Shutdown() {
  if (shutdown_.exchange(true)) return;
  // Stop accepting/running callbacks before tearing down the I/O loops so no
  // offloaded callback observes a destroyed completion queue. If Shutdown() or
  // ~PoolExecutors() runs from inside one of its own callback worker threads or
  // event-loop threads, joining synchronously on that thread would self-join
  // (EDEADLK). Hand ownership to a helper thread so the join runs after the
  // current callback unwinds.
  if (callback_pool_) {
    bool const on_worker = callback_pool_->IsCurrentThreadWorker();
    callback_pool_->Shutdown();
    if (on_worker) {
      std::thread([pool = std::move(callback_pool_)]() mutable {
        pool->WaitForAllDone();
        pool.reset();
      }).detach();
    }
  }
  auto const self_id = std::this_thread::get_id();
  for (std::size_t i = 0; i != event_loops_.size(); ++i) {
    if (!event_loops_[i]) continue;
    event_loops_[i]->cq.CancelAll();
    event_loops_[i]->cq.Shutdown();
    event_loops_[i]->cq.CancelAll();
    if (loop_thread_ids_[i] == self_id) {
      std::thread([loop = std::move(event_loops_[i])]() mutable {
        loop->done.Wait();
        if (loop->thread.joinable()) {
          loop->thread.join();
        }
        loop.reset();
      }).detach();
    } else {
      event_loops_[i]->done.Wait();
      if (event_loops_[i]->thread.joinable()) {
        event_loops_[i]->thread.join();
      }
      event_loops_[i].reset();
    }
  }
}

google::cloud::CompletionQueue PoolExecutors::cq(std::size_t index) const {
  return cqs_[index % cqs_.size()];
}

void PoolExecutors::CancelAll() {
  for (auto& cq : cqs_) {
    cq.CancelAll();
  }
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
