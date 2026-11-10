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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_LATCH_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_LATCH_H

#include "google/cloud/version.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

/// Single-use thread-synchronization primitive analogous to
/// `absl::Notification` or C++20 `std::latch(1)`, built on standard C++17
/// primitives.
class Latch {
 public:
  Latch() = default;
  Latch(Latch const&) = delete;
  Latch& operator=(Latch const&) = delete;

  void Notify() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (notified_.load(std::memory_order_relaxed)) return;
      notified_.store(true, std::memory_order_release);
    }
    cv_.notify_all();
  }

  bool HasBeenNotified() const noexcept {
    return notified_.load(std::memory_order_acquire);
  }

  void Wait() {
    if (HasBeenNotified()) return;
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock,
             [this] { return notified_.load(std::memory_order_relaxed); });
  }

  template <typename Rep, typename Period>
  bool WaitFor(std::chrono::duration<Rep, Period> const& timeout) {
    if (HasBeenNotified()) return true;
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [this] {
      return notified_.load(std::memory_order_relaxed);
    });
  }

  template <typename Clock, typename Duration>
  bool WaitUntil(std::chrono::time_point<Clock, Duration> const& deadline) {
    if (HasBeenNotified()) return true;
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_until(lock, deadline, [this] {
      return notified_.load(std::memory_order_relaxed);
    });
  }

 private:
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::atomic<bool> notified_{false};
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_LATCH_H
