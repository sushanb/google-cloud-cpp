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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SHIM_DIVERTER_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SHIM_DIVERTER_H

#include "google/cloud/bigtable/internal/sessions/client/session_client.h"
#include "google/cloud/bigtable/internal/sessions/sessions_options.h"
#include "google/cloud/version.h"
#include <atomic>
#include <cmath>
#include <cstdint>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace shim {

/// Stochastic traffic diverter that routes a configurable fraction
/// (`session_load` in `[0.0, 1.0]`) of eligible operations to the Sessions
/// stack and the remainder to the classic gRPC stack.
class Diverter {
 public:
  explicit Diverter(double initial_session_load = 1.0)
      : session_load_(Clamp(initial_session_load)) {}

  void SetSessionLoad(double load) {
    if (std::isnan(load)) return;
    session_load_.store(Clamp(load), std::memory_order_release);
  }

  /// Trips the circuit breaker when the server reports `UNIMPLEMENTED`, routing
  /// eligible calls to the classic stack while preserving the underlying
  /// `session_load_` value for recovery if the breaker is reset.
  void TripUnimplementedBreaker() {
    unimplemented_tripped_.store(true, std::memory_order_release);
  }

  void ResetUnimplementedBreaker() {
    unimplemented_tripped_.store(false, std::memory_order_release);
  }

  bool unimplemented_tripped() const {
    return unimplemented_tripped_.load(std::memory_order_acquire);
  }

  double session_load() const {
    if (unimplemented_tripped_.load(std::memory_order_acquire)) {
      return 0.0;
    }
    return session_load_.load(std::memory_order_acquire);
  }

  /// Evaluates whether a call with uniform random sample `sample_0_to_1` in
  /// `[0.0, 1.0)` should route to the Sessions stack or classic stack.
  bool UseSession(double sample_0_to_1) {
    if (unimplemented_tripped_.load(std::memory_order_acquire)) {
      classic_picks_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    double threshold = session_load_.load(std::memory_order_relaxed);
    if (threshold > 0.0 && sample_0_to_1 < threshold) {
      session_picks_.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
    classic_picks_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  /// Evaluates whether the current call should route to the Sessions stack or
  /// classic stack using a thread-local uniform sample in `[0.0, 1.0)`.
  bool UseSession() {
    if (unimplemented_tripped_.load(std::memory_order_acquire)) {
      classic_picks_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    double threshold = session_load_.load(std::memory_order_relaxed);
    if (threshold >= 1.0) {
      session_picks_.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
    if (threshold <= 0.0) {
      classic_picks_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    thread_local std::uint64_t state = 0x9e3779b97f4a7c15ULL;
    state += 0x9e3779b97f4a7c15ULL;
    std::uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    z = z ^ (z >> 31);
    double sample = static_cast<double>(z >> 11) * (1.0 / 9007199254740992.0);
    return UseSession(sample);
  }

  std::uint64_t session_picks() const {
    return session_picks_.load(std::memory_order_relaxed);
  }

  std::uint64_t classic_picks() const {
    return classic_picks_.load(std::memory_order_relaxed);
  }

 private:
  static double Clamp(double v) {
    if (std::isnan(v) || v < 0.0) return 0.0;
    if (v > 1.0) return 1.0;
    return v;
  }

  std::atomic<double> session_load_{1.0};
  std::atomic<bool> unimplemented_tripped_{false};
  std::atomic<std::uint64_t> session_picks_{0};
  std::atomic<std::uint64_t> classic_picks_{0};
};

/// Classifies `Table` operations to determine whether they can be executed over
/// a session stream (`kReadRow`, `kApplyNonConditional`) or must be forwarded
/// to the classic gRPC `DataConnection`.
enum class TableOperationKind {
  kReadRow = 0,
  kApplyNonConditional = 1,
  kCheckAndMutateRowConditional = 2,
  kReadRowsScan = 3,
  kBulkApply = 4,
  kSampleRowKeys = 5,
  kReadModifyWriteRow = 6,
};

inline bool IsRoutableToSessions(TableOperationKind kind) {
  return kind == TableOperationKind::kReadRow ||
         kind == TableOperationKind::kApplyNonConditional;
}

}  // namespace shim
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_SHIM_DIVERTER_H
