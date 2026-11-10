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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_LIGHTWEIGHT_THREAD_POOL_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_LIGHTWEIGHT_THREAD_POOL_H

#include "google/cloud/bigtable/internal/sessions/latch.h"
#include "google/cloud/version.h"
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <new>
#include <queue>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

/// Fixed-size worker pool with per-worker queues and inline task storage.
class LightweightThreadPool {
 public:
  explicit LightweightThreadPool(std::size_t num_threads, int spin_count = 0);
  ~LightweightThreadPool();

  /// Move-only task representation with fixed inline storage (no heap
  /// allocation).
  class Task {
   public:
    Task() = default;

    template <typename F, typename = std::enable_if_t<
                              !std::is_same_v<std::decay_t<F>, Task>>>
    Task(F&& f) {  // NOLINT(google-explicit-constructor)
      static_assert(sizeof(DecayF<F>) <= kInlineSize, "Task closure too large");
      new (&storage_) Impl<DecayF<F>>(std::forward<F>(f));
      impl_ = reinterpret_cast<Concept*>(&storage_);
    }

    Task(Task const&) = delete;
    Task& operator=(Task const&) = delete;

    Task(Task&& other) noexcept {
      if (other.impl_) {
        other.impl_->MoveTo(&storage_);
        impl_ = reinterpret_cast<Concept*>(&storage_);
        other.impl_->Destroy();
        other.impl_ = nullptr;
      } else {
        impl_ = nullptr;
      }
    }

    Task& operator=(Task&& other) noexcept {
      if (this != &other) {
        if (impl_) {
          impl_->Destroy();
        }
        if (other.impl_) {
          other.impl_->MoveTo(&storage_);
          impl_ = reinterpret_cast<Concept*>(&storage_);
          other.impl_->Destroy();
          other.impl_ = nullptr;
        } else {
          impl_ = nullptr;
        }
      }
      return *this;
    }

    ~Task() {
      if (impl_) {
        impl_->Destroy();
      }
    }

    void operator()() {
      if (impl_) {
        impl_->Execute();
      }
    }

    explicit operator bool() const { return impl_ != nullptr; }

   private:
    static constexpr std::size_t kStorageSize = 384;
    static constexpr std::size_t kInlineSize = kStorageSize - sizeof(void*);

    struct Concept {
      virtual void Execute() = 0;
      virtual void MoveTo(void* dest) = 0;
      virtual void Destroy() = 0;
      virtual ~Concept() = default;
    };

    template <typename F>
    struct Impl : Concept {
      explicit Impl(F&& f) : f_(std::move(f)) {}
      explicit Impl(F const& f) : f_(f) {}
      void Execute() override { f_(); }
      void MoveTo(void* dest) override { new (dest) Impl<F>(std::move(*this)); }
      void Destroy() override { this->~Impl(); }
      F f_;
    };

    template <typename F>
    using DecayF = typename std::decay<F>::type;

    Concept* impl_ = nullptr;
    alignas(alignof(std::max_align_t)) char storage_[kStorageSize]{};
  };

  void Submit(Task task, std::size_t hint);
  void Shutdown();
  void WaitForAllDone();
  bool IsCurrentThreadWorker() const;

  std::size_t total_queue_size() const {
    std::size_t total = 0;
    for (auto const& w : workers_) {
      int const sz = w->queue_size.load(std::memory_order_relaxed);
      total += (sz > 0 ? static_cast<std::size_t>(sz) : 0);
    }
    return total;
  }

 private:
  struct Worker {
    std::thread thread;
    std::thread::id thread_id;
    std::mutex queue_lock;
    std::mutex cv_mutex;
    std::condition_variable cv;
    std::queue<Task> queue;
    std::atomic<bool> shutdown{false};
    std::atomic<int> queue_size{0};
    std::atomic<bool> is_sleeping{false};
    Latch done;
  };

  static void WorkerLoop(std::shared_ptr<Worker> w, int spin_count);

  std::vector<std::shared_ptr<Worker>> workers_;
  std::size_t num_threads_;
  std::atomic<bool> global_shutdown_{false};
  int spin_count_ = 0;
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_LIGHTWEIGHT_THREAD_POOL_H
