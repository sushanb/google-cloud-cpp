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

#include "google/cloud/bigtable/internal/sessions/pool/lightweight_thread_pool.h"
#include <mutex>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

LightweightThreadPool::LightweightThreadPool(std::size_t num_threads,
                                             int spin_count)
    : num_threads_(num_threads), spin_count_(spin_count) {
  workers_.reserve(num_threads_);
  for (std::size_t i = 0; i < num_threads_; ++i) {
    workers_.push_back(std::make_shared<Worker>());
  }
  for (std::size_t i = 0; i < num_threads_; ++i) {
    workers_[i]->thread = std::thread(&LightweightThreadPool::WorkerLoop,
                                      workers_[i], spin_count_);
    workers_[i]->thread_id = workers_[i]->thread.get_id();
  }
}

LightweightThreadPool::~LightweightThreadPool() { Shutdown(); }

void LightweightThreadPool::Submit(Task task, std::size_t hint) {
  std::size_t const worker_idx = hint % num_threads_;
  Worker* w = workers_[worker_idx].get();

  {
    std::lock_guard<std::mutex> lock(w->queue_lock);
    w->queue.push(std::move(task));
    w->queue_size.fetch_add(1, std::memory_order_release);
  }

  {
    std::lock_guard<std::mutex> cv_lock(w->cv_mutex);
  }
  w->cv.notify_one();
}

void LightweightThreadPool::Shutdown() {
  if (global_shutdown_.exchange(true)) return;

  for (std::size_t i = 0; i < num_threads_; ++i) {
    Worker* w = workers_[i].get();
    {
      std::lock_guard<std::mutex> lock(w->cv_mutex);
      w->shutdown.store(true, std::memory_order_release);
    }
    w->cv.notify_all();
  }

  auto const self_id = std::this_thread::get_id();
  for (std::size_t i = 0; i < num_threads_; ++i) {
    if (workers_[i]->thread_id == self_id) {
      if (workers_[i]->thread.joinable()) {
        workers_[i]->thread.detach();
      }
    } else {
      workers_[i]->done.Wait();
      if (workers_[i]->thread.joinable()) {
        workers_[i]->thread.join();
      }
    }
  }
}

void LightweightThreadPool::WaitForAllDone() {
  for (std::size_t i = 0; i < num_threads_; ++i) {
    workers_[i]->done.Wait();
  }
}

bool LightweightThreadPool::IsCurrentThreadWorker() const {
  auto const self_id = std::this_thread::get_id();
  for (std::size_t i = 0; i < num_threads_; ++i) {
    if (workers_[i]->thread_id == self_id) return true;
  }
  return false;
}

void LightweightThreadPool::WorkerLoop(std::shared_ptr<Worker> w,
                                       int spin_count) {
  while (true) {
    Task task;

    // 1. Optional brief yield loop before acquiring the queue mutex.
    if (!w->shutdown.load(std::memory_order_acquire)) {
      for (int i = 0; i < spin_count; ++i) {
        if (w->queue_size.load(std::memory_order_acquire) > 0 ||
            w->shutdown.load(std::memory_order_acquire)) {
          break;
        }
        std::this_thread::yield();
      }
    }

    // 2. Pop the next queued task under the worker queue mutex.
    bool found = false;
    {
      std::lock_guard<std::mutex> lock(w->queue_lock);
      if (!w->queue.empty()) {
        task = std::move(w->queue.front());
        w->queue.pop();
        w->queue_size.fetch_sub(1, std::memory_order_release);
        found = true;
      }
    }

    if (found) {
      if (task) {
        task();
      }
      continue;
    }

    // 3. Block on condition variable until new work arrives or shutdown.
    {
      std::unique_lock<std::mutex> cv_lock(w->cv_mutex);

      auto is_queue_empty = [&w] {
        std::lock_guard<std::mutex> lock(w->queue_lock);
        return w->queue.empty();
      };

      bool const check_empty = is_queue_empty();
      if (w->shutdown.load(std::memory_order_acquire) && check_empty) {
        break;
      }

      if (check_empty) {
        w->is_sleeping.store(true, std::memory_order_release);
        w->cv.wait(cv_lock, [&w, &is_queue_empty] {
          return w->shutdown.load(std::memory_order_acquire) ||
                 !is_queue_empty();
        });
        w->is_sleeping.store(false, std::memory_order_release);
      }
    }
  }
  w->done.Notify();
}

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
