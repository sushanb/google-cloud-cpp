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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TELEMETRY_DEBUG_SNAPSHOT_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TELEMETRY_DEBUG_SNAPSHOT_H

#include "google/cloud/version.h"
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace telemetry {

/// Monotonic 5-state C++ session reactor lifecycle (`SessionImpl::State`):
/// `kConnecting (0) -> kReady (1) -> kDraining (2) -> kClosing (3) -> kClosed
/// (4)`
enum class SessionLifecycleState : int {
  kConnecting = 0,
  kReady = 1,
  kDraining = 2,
  kClosing = 3,
  kClosed = 4,
};

/// Four-state outcome classification for the retry oracle
/// (`EvaluateRetryOracle`).
enum class AttemptOutcomeState {
  /// Rejected by a session before leaving the client (e.g., session not Ready
  /// or busy). Always safe to retry on another session regardless of request
  /// idempotency.
  kUncommitted = 0,
  /// Written to wire, stream terminated before server response (e.g., missed
  /// heartbeat or EOF). Safe to retry ONLY if the request is idempotent.
  kTransportFailure = 1,
  /// Server returned an explicit `ErrorResponse` or `VirtualRpcResponse`.
  /// Retryable ONLY if `ErrorResponse.retry_info` is present (and operation is
  /// idempotent) within attempt and deadline limits.
  kServerResult = 2,
  /// Rejected by the SessionPool before reaching any session (e.g., waiter
  /// queue full, pool closed, terminal handshake failure, or circuit breaker
  /// open). Never left the client, and NEVER retried by `RetryingVRpc`.
  kRejected = 3,
};

/// Value-type diagnostic snapshots for sessions, peers, pools, and the traffic
/// diverter. Diagnostic consumers inspect state through copied value snapshots
/// without holding pool or session locks.
struct SessionSnapshot {
  std::int64_t session_id = 0;
  std::int64_t peer_id = 0;
  SessionLifecycleState state = SessionLifecycleState::kConnecting;
  bool has_active_rpc = false;
};

struct PeerSnapshotRow {
  std::int64_t peer_id = 0;
  std::size_t ref_count = 0;
  std::size_t idle_count = 0;
  std::size_t in_flight_count = 0;
  double e2e_ewma_us = 0.0;
  double transport_ewma_us = 0.0;
};
using AfeSnapshotRow = PeerSnapshotRow;

struct PoolSnapshot {
  std::string pool_key;
  std::size_t ready_sessions = 0;
  std::size_t expected_capacity = 0;
  std::size_t sessions_holding_budget = 0;
  std::vector<PeerSnapshotRow> peers;
  std::vector<SessionSnapshot> sessions;
};

struct DiverterSnapshot {
  double session_load = 0.0;
  std::uint64_t session_picks = 0;
  std::uint64_t classic_picks = 0;
};

class SessionDebugProvider {
 public:
  virtual ~SessionDebugProvider() = default;
  virtual PoolSnapshot GetPoolSnapshot() const = 0;
};

}  // namespace telemetry

namespace session {
using SessionLifecycleState = ::google::cloud::bigtable_internal::sessions::
    telemetry::SessionLifecycleState;
using AttemptOutcomeState = ::google::cloud::bigtable_internal::sessions::
    telemetry::AttemptOutcomeState;
}  // namespace session

}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TELEMETRY_DEBUG_SNAPSHOT_H
