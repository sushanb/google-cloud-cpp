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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_DYNAMIC_PICKER_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_DYNAMIC_PICKER_H

#include "google/cloud/bigtable/internal/sessions/session/session_impl.h"
#include "google/cloud/version.h"
#include <google/bigtable/v2/session.pb.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {

struct SessionEntry {
  std::shared_ptr<SessionImpl> session;
};

/// Selects ready sessions from a connection group using the configured
/// load-balancing strategy.
class DynamicPicker {
 public:
  enum class Strategy {
    kLeastInFlight,
    kRandom,
    kLeastLatency,
    kRoundRobin,
    kWeightedRoundRobin,
  };

  DynamicPicker();

  /// Updates the load-balancing policy from server configuration payloads.
  void UpdateConfig(google::bigtable::v2::SessionClientConfiguration::
                        SessionPoolConfiguration const& config);

  void SetStrategy(Strategy strategy) {
    strategy_.store(strategy, std::memory_order_relaxed);
  }
  Strategy strategy() const {
    return strategy_.load(std::memory_order_relaxed);
  }

  void SetRandomSubsetSize(std::int64_t subset_size) {
    random_subset_size_.store(subset_size > 0 ? subset_size : 0,
                              std::memory_order_relaxed);
  }
  std::int64_t random_subset_size() const {
    return random_subset_size_.load(std::memory_order_relaxed);
  }

  /// Selects an available, ready session to service a new virtual RPC, or
  /// returns `std::nullopt` if all candidate sessions are busy.
  std::optional<std::shared_ptr<SessionImpl>> PickSession(
      std::vector<SessionEntry> const& sessions,
      std::unordered_set<SessionImpl*> const& ignored = {},
      bool is_write_request = true) const;

 private:
  std::atomic<Strategy> strategy_{Strategy::kLeastInFlight};
  std::atomic<std::int64_t> random_subset_size_{2};
};

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_POOL_DYNAMIC_PICKER_H
