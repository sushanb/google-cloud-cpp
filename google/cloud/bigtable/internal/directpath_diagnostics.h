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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_DIRECTPATH_DIAGNOSTICS_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_DIRECTPATH_DIAGNOSTICS_H

#include "google/cloud/bigtable/options.h"
#include "google/cloud/bigtable/version.h"
#include "google/cloud/completion_queue.h"
#include "google/cloud/future.h"
#include "google/cloud/options.h"
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
#include "google/cloud/bigtable/internal/client_schema_metrics.h"
#endif
#include "google/cloud/internal/detect_gcp.h"
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#ifndef _WIN32
#include <ifaddrs.h>
#endif

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN

enum class DiagnosticFailureReason {
  kNotInGcp,
  kMetadataUnreachable,
  kNoIpAssigned,
  kLoopbackMisconfigured,
  kLoopbackMisconfiguredIpv4,
  kLoopbackMisconfiguredIpv6,
  kMetadataMissing,
  kXdsReachabilityFailed,
  kXdsEdsFailed,
  kXdsMalformedEndpoint,
  kRouteUnreachable,
  kAltsHandshakeFailed,
  kTimeout,
  kUnknown,
};

std::string ToString(DiagnosticFailureReason reason);

/**
 * Interface for OS-specific networking and system operations used by
 * DirectPath diagnostics.
 */
class DirectPathNetworkSystem {
 public:
  virtual ~DirectPathNetworkSystem() = default;
  virtual bool CanConnectTcp(std::string const& host, std::uint16_t port,
                             std::chrono::milliseconds timeout) = 0;
  virtual DiagnosticFailureReason CheckLoopbackConfiguration() = 0;
};

constexpr char const* DefaultDirectPathMetadataHost() {
  return "169.254.169.254";
}
constexpr std::uint16_t DefaultDirectPathMetadataPort() { return 80; }
constexpr std::chrono::milliseconds DefaultDirectPathMetadataTimeout() {
  return std::chrono::seconds(2);
}

#ifndef _WIN32
/// For testing only.
DiagnosticFailureReason EvaluateLoopbackIfaddrs(struct ifaddrs const* ifaddr);
#endif

std::shared_ptr<DirectPathNetworkSystem> MakeDefaultDirectPathNetworkSystem();

class DirectPathDiagnostics {
 public:
  class CancelState;

  /**
   * Owns a background diagnostic run started off the `CompletionQueue` thread
   * and interrupts/joins it on destruction so connection teardown never blocks
   * waiting for TCP timeouts.
   */
  class AsyncTask {
   public:
    AsyncTask() = default;
    AsyncTask(std::shared_ptr<CancelState> cancel_state, std::thread worker);
    ~AsyncTask();

    AsyncTask(AsyncTask const&) = delete;
    AsyncTask& operator=(AsyncTask const&) = delete;
    AsyncTask(AsyncTask&&) noexcept = default;
    AsyncTask& operator=(AsyncTask&& other) noexcept;

    void CancelAndJoin();
    void Wait();

   private:
    std::shared_ptr<CancelState> cancel_state_;
    std::thread worker_;
  };

  static DiagnosticFailureReason RunDiagnostics(Options const& options);

  /// For testing only.
  static DiagnosticFailureReason RunDiagnostics(
      Options const& options,
      std::shared_ptr<internal::GcpDetector> const& detector,
      std::shared_ptr<DirectPathNetworkSystem> const& network_system,
      std::string const& metadata_host, std::uint16_t metadata_port);

#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
  static std::unique_ptr<AsyncTask> StartAsync(
      Options const& options,
      std::weak_ptr<DirectAccessCompatibility> direct_access_compatibility);

  /// For testing only.
  static std::unique_ptr<AsyncTask> StartAsync(
      Options const& options,
      std::weak_ptr<DirectAccessCompatibility> direct_access_compatibility,
      std::shared_ptr<internal::GcpDetector> detector,
      std::shared_ptr<DirectPathNetworkSystem> network_system,
      std::string const& metadata_host, std::uint16_t metadata_port);

  static future<void> RunAsync(CompletionQueue cq, Options const& options);

  static future<void> RunAsync(
      CompletionQueue cq, Options const& options,
      std::shared_ptr<DirectAccessCompatibility> direct_access_compatibility);

  /// For testing only.
  static future<void> RunAsync(
      CompletionQueue cq, Options const& options,
      std::shared_ptr<DirectAccessCompatibility> direct_access_compatibility,
      std::shared_ptr<internal::GcpDetector> detector,
      std::shared_ptr<DirectPathNetworkSystem> network_system,
      std::string const& metadata_host, std::uint16_t metadata_port);
#endif
};

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_DIRECTPATH_DIAGNOSTICS_H
