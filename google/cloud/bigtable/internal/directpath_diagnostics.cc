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

#include "google/cloud/bigtable/internal/directpath_diagnostics.h"
#include "google/cloud/bigtable/internal/defaults.h"
#include "google/cloud/bigtable/options.h"
#include "google/cloud/internal/detect_gcp.h"
#include "google/cloud/internal/getenv.h"
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
#include <opentelemetry/context/runtime_context.h>
#endif
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#ifndef _WIN32
#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN

namespace {

bool ParsePort(std::string_view s, std::uint16_t& port_out) {
  if (s.empty()) return false;
  unsigned int value = 0;
  auto const [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
  if (ec != std::errc{} || ptr != s.data() + s.size() || value == 0 ||
      value > 65535) {
    return false;
  }
  port_out = static_cast<std::uint16_t>(value);
  return true;
}

std::pair<std::string, std::uint16_t> ParseMetadataEndpoint(
    std::string_view raw) {
  if (raw.substr(0, 7) == "http://") {
    raw.remove_prefix(7);
  } else if (raw.substr(0, 8) == "https://") {
    raw.remove_prefix(8);
  }
  auto const slash = raw.find('/');
  if (slash != std::string_view::npos) {
    raw = raw.substr(0, slash);
  }
  if (raw.empty()) {
    return {DefaultDirectPathMetadataHost(), DefaultDirectPathMetadataPort()};
  }
  if (raw.front() == '[') {
    auto const close = raw.find(']');
    if (close == std::string_view::npos || close <= 1) {
      return {DefaultDirectPathMetadataHost(), DefaultDirectPathMetadataPort()};
    }
    std::string host(raw.substr(1, close - 1));
    std::string_view const rest = raw.substr(close + 1);
    if (rest.empty()) {
      return {std::move(host), DefaultDirectPathMetadataPort()};
    }
    std::uint16_t port = DefaultDirectPathMetadataPort();
    if (rest.front() == ':' && ParsePort(rest.substr(1), port)) {
      return {std::move(host), port};
    }
    return {DefaultDirectPathMetadataHost(), DefaultDirectPathMetadataPort()};
  }

  auto const first_colon = raw.find(':');
  if (first_colon == std::string_view::npos) {
    return {std::string(raw), DefaultDirectPathMetadataPort()};
  }
  auto const last_colon = raw.rfind(':');
  if (first_colon == last_colon) {
    std::string_view const host_part = raw.substr(0, first_colon);
    std::uint16_t port = DefaultDirectPathMetadataPort();
    if (!host_part.empty() && ParsePort(raw.substr(first_colon + 1), port)) {
      return {std::string(host_part), port};
    }
    return {DefaultDirectPathMetadataHost(), DefaultDirectPathMetadataPort()};
  }
  // Unbracketed IPv6 literal (e.g. ::1 or fd00:ec2::254).
  return {std::string(raw), DefaultDirectPathMetadataPort()};
}

std::pair<std::string, std::uint16_t> ResolveMetadataEndpoint() {
  auto env = internal::GetEnv("GCE_METADATA_HOST");
  if (env.has_value() && !env->empty()) {
    return ParseMetadataEndpoint(*env);
  }
  return {DefaultDirectPathMetadataHost(), DefaultDirectPathMetadataPort()};
}

}  // namespace

class DirectPathDiagnostics::CancelState {
 public:
  CancelState() {
#ifndef _WIN32
    if (pipe(pipe_fds_) == 0) {
      for (int fd : pipe_fds_) {
        int const flags = fcntl(fd, F_GETFL, 0);
        if (flags >= 0) {
          (void)fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        }
        int const fd_flags = fcntl(fd, F_GETFD, 0);
        if (fd_flags >= 0) {
          (void)fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC);
        }
      }
    }
#endif
  }

  ~CancelState() {
#ifndef _WIN32
    if (pipe_fds_[0] >= 0) close(pipe_fds_[0]);
    if (pipe_fds_[1] >= 0) close(pipe_fds_[1]);
#endif
  }

  CancelState(CancelState const&) = delete;
  CancelState& operator=(CancelState const&) = delete;

  void Cancel() {
    if (cancelled_.exchange(true, std::memory_order_acq_rel)) return;
#ifndef _WIN32
    if (pipe_fds_[1] >= 0) {
      char const byte = 1;
      if (write(pipe_fds_[1], &byte, 1) < 0) {
        // Best-effort non-blocking wakeup write.
      }
    }
#endif
  }

  bool IsCancelled() const {
    return cancelled_.load(std::memory_order_acquire);
  }

#ifndef _WIN32
  int wake_read_fd() const { return pipe_fds_[0]; }
#endif

 private:
  std::atomic<bool> cancelled_{false};
#ifndef _WIN32
  int pipe_fds_[2] = {-1, -1};
#endif
};

DirectPathDiagnostics::AsyncTask::AsyncTask(
    std::shared_ptr<CancelState> cancel_state, std::thread worker)
    : cancel_state_(std::move(cancel_state)), worker_(std::move(worker)) {}

DirectPathDiagnostics::AsyncTask::~AsyncTask() { CancelAndJoin(); }

DirectPathDiagnostics::AsyncTask& DirectPathDiagnostics::AsyncTask::operator=(
    AsyncTask&& other) noexcept {
  if (this != &other) {
    CancelAndJoin();
    cancel_state_ = std::move(other.cancel_state_);
    worker_ = std::move(other.worker_);
  }
  return *this;
}

void DirectPathDiagnostics::AsyncTask::CancelAndJoin() {
  if (cancel_state_ != nullptr) {
    cancel_state_->Cancel();
  }
  if (worker_.joinable()) {
    if (worker_.get_id() == std::this_thread::get_id()) {
      worker_.detach();
    } else {
      worker_.join();
    }
  }
}

void DirectPathDiagnostics::AsyncTask::Wait() {
  if (worker_.joinable() && worker_.get_id() != std::this_thread::get_id()) {
    worker_.join();
  }
}

std::string ToString(DiagnosticFailureReason reason) {
  switch (reason) {
    case DiagnosticFailureReason::kNotInGcp:
      return "not_in_gcp";
    case DiagnosticFailureReason::kMetadataUnreachable:
      return "metadata_unreachable";
    case DiagnosticFailureReason::kNoIpAssigned:
      return "no_ip_assigned";
    case DiagnosticFailureReason::kLoopbackMisconfigured:
      return "loopback_misconfigured";
    case DiagnosticFailureReason::kLoopbackMisconfiguredIpv4:
      return "loopback_misconfigured_ipv4";
    case DiagnosticFailureReason::kLoopbackMisconfiguredIpv6:
      return "loopback_misconfigured_ipv6";
    case DiagnosticFailureReason::kMetadataMissing:
      return "metadata_missing";
    case DiagnosticFailureReason::kXdsReachabilityFailed:
      return "xds_reachability_failed";
    case DiagnosticFailureReason::kXdsEdsFailed:
      return "xds_eds_failed";
    case DiagnosticFailureReason::kXdsMalformedEndpoint:
      return "xds_malformed_endpoint";
    case DiagnosticFailureReason::kRouteUnreachable:
      return "route_unreachable";
    case DiagnosticFailureReason::kAltsHandshakeFailed:
      return "alts_handshake_failed";
    case DiagnosticFailureReason::kTimeout:
      return "timeout";
    case DiagnosticFailureReason::kUnknown:
      return "unknown";
  }
  return "unknown";
}

#ifndef _WIN32
DiagnosticFailureReason EvaluateLoopbackIfaddrs(struct ifaddrs const* ifaddr) {
  bool has_ipv4_lo = false;
  bool has_ipv6_lo = false;

  for (struct ifaddrs const* ifa = ifaddr; ifa != nullptr;
       ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == nullptr) continue;
    if ((ifa->ifa_flags & IFF_LOOPBACK) != 0 &&
        (ifa->ifa_flags & IFF_UP) != 0) {
      if (ifa->ifa_addr->sa_family == AF_INET) {
        has_ipv4_lo = true;
      } else if (ifa->ifa_addr->sa_family == AF_INET6) {
        has_ipv6_lo = true;
      }
    }
  }

  if (!has_ipv4_lo && !has_ipv6_lo) {
    return DiagnosticFailureReason::kLoopbackMisconfigured;
  }
  if (!has_ipv4_lo) {
    return DiagnosticFailureReason::kLoopbackMisconfiguredIpv4;
  }
  if (!has_ipv6_lo) {
    return DiagnosticFailureReason::kLoopbackMisconfiguredIpv6;
  }
  return DiagnosticFailureReason::kUnknown;
}

namespace {

bool ConnectTcpWithCancel(
    std::string const& host, std::uint16_t port,
    std::chrono::milliseconds timeout,
    DirectPathDiagnostics::CancelState const* cancel_state) {
  if (host.empty() || port == 0 ||
      timeout <= std::chrono::milliseconds::zero()) {
    return false;
  }
  if (cancel_state != nullptr && cancel_state->IsCancelled()) {
    return false;
  }

  std::string effective_host = host;
  if (effective_host.size() >= 2 && effective_host.front() == '[' &&
      effective_host.back() == ']') {
    effective_host = effective_host.substr(1, effective_host.size() - 2);
  }
  if (effective_host.empty()) {
    return false;
  }

  auto const deadline = std::chrono::steady_clock::now() + timeout;

  struct addrinfo hints;
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  std::string const port_str = std::to_string(port);
  struct addrinfo* result = nullptr;
  if (getaddrinfo(effective_host.c_str(), port_str.c_str(), &hints, &result) !=
          0 ||
      result == nullptr) {
    return false;
  }
  std::unique_ptr<struct addrinfo, decltype(&freeaddrinfo)> addr_guard(
      result, &freeaddrinfo);

  std::chrono::steady_clock::rep remaining_addrs = 0;
  for (struct addrinfo const* rp = result; rp != nullptr; rp = rp->ai_next) {
    ++remaining_addrs;
  }

  for (struct addrinfo const* rp = result; rp != nullptr;
       rp = rp->ai_next, --remaining_addrs) {
    if (cancel_state != nullptr && cancel_state->IsCancelled()) {
      return false;
    }
    auto const start_now = std::chrono::steady_clock::now();
    if (start_now >= deadline) {
      return false;
    }

    int const sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (sock < 0) continue;

    int const flags = fcntl(sock, F_GETFL, 0);
    if (flags < 0 || fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
      close(sock);
      continue;
    }

    int const conn_res = connect(sock, rp->ai_addr, rp->ai_addrlen);
    if (conn_res == 0) {
      close(sock);
      return true;
    }
    if (errno != EINPROGRESS) {
      close(sock);
      continue;
    }

    auto const attempt_deadline =
        remaining_addrs <= 1
            ? deadline
            : start_now + (deadline - start_now) / remaining_addrs;

    bool connected = false;
    bool try_next = false;
    while (!connected && !try_next) {
      if (cancel_state != nullptr && cancel_state->IsCancelled()) {
        close(sock);
        return false;
      }
      auto const poll_now = std::chrono::steady_clock::now();
      if (poll_now >= attempt_deadline) {
        try_next = true;
        break;
      }
      auto const remaining_ms = static_cast<std::int64_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              attempt_deadline - poll_now)
              .count());
      int const poll_timeout_ms =
          remaining_ms > 0
              ? static_cast<int>(
                    (std::min)(remaining_ms,
                               static_cast<std::int64_t>(
                                   (std::numeric_limits<int>::max)())))
              : 1;

      struct pollfd pfds[2];
      pfds[0].fd = sock;
      pfds[0].events = POLLOUT;
      pfds[0].revents = 0;
      nfds_t nfds = 1;
      if (cancel_state != nullptr && cancel_state->wake_read_fd() >= 0) {
        pfds[1].fd = cancel_state->wake_read_fd();
        pfds[1].events = POLLIN;
        pfds[1].revents = 0;
        nfds = 2;
      }

      int const poll_res = poll(pfds, nfds, poll_timeout_ms);
      if (poll_res < 0) {
        if (errno == EINTR) continue;
        try_next = true;
        break;
      }
      if (poll_res == 0) {
        try_next = true;
        break;
      }
      if (nfds == 2 && (pfds[1].revents & (POLLIN | POLLHUP | POLLERR)) != 0) {
        close(sock);
        return false;
      }
      if ((pfds[0].revents & (POLLOUT | POLLERR | POLLHUP)) != 0) {
        int so_error = 0;
        socklen_t len = sizeof(so_error);
        if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &so_error, &len) == 0 &&
            so_error == 0) {
          connected = true;
        } else {
          try_next = true;
        }
      }
    }

    close(sock);
    if (connected) return true;
  }

  return false;
}

class DefaultDirectPathNetworkSystem : public DirectPathNetworkSystem {
 public:
  explicit DefaultDirectPathNetworkSystem(
      std::shared_ptr<DirectPathDiagnostics::CancelState> cancel_state =
          nullptr)
      : cancel_state_(std::move(cancel_state)) {}

  bool CanConnectTcp(std::string const& host, std::uint16_t port,
                     std::chrono::milliseconds timeout) override {
    return ConnectTcpWithCancel(host, port, timeout, cancel_state_.get());
  }

  DiagnosticFailureReason CheckLoopbackConfiguration() override {
    struct ifaddrs* ifaddr = nullptr;
    if (getifaddrs(&ifaddr) == -1) return DiagnosticFailureReason::kUnknown;
    DiagnosticFailureReason const reason = EvaluateLoopbackIfaddrs(ifaddr);
    freeifaddrs(ifaddr);
    return reason;
  }

 private:
  std::shared_ptr<DirectPathDiagnostics::CancelState> cancel_state_;
};

}  // namespace

std::shared_ptr<DirectPathNetworkSystem> MakeDefaultDirectPathNetworkSystem() {
  return std::make_shared<DefaultDirectPathNetworkSystem>();
}

#else

namespace {

class DefaultDirectPathNetworkSystem : public DirectPathNetworkSystem {
 public:
  explicit DefaultDirectPathNetworkSystem(
      std::shared_ptr<DirectPathDiagnostics::CancelState> = nullptr) {}

  bool CanConnectTcp(std::string const&, std::uint16_t,
                     std::chrono::milliseconds) override {
    return false;
  }
  DiagnosticFailureReason CheckLoopbackConfiguration() override {
    return DiagnosticFailureReason::kUnknown;
  }
};

}  // namespace

std::shared_ptr<DirectPathNetworkSystem> MakeDefaultDirectPathNetworkSystem() {
  return std::make_shared<DefaultDirectPathNetworkSystem>();
}
#endif

DiagnosticFailureReason DirectPathDiagnostics::RunDiagnostics(
    Options const& options) {
  auto const [metadata_host, metadata_port] = ResolveMetadataEndpoint();
  return RunDiagnostics(options, internal::MakeGcpDetector(),
                        MakeDefaultDirectPathNetworkSystem(), metadata_host,
                        metadata_port);
}

DiagnosticFailureReason DirectPathDiagnostics::RunDiagnostics(
    Options const& options,
    std::shared_ptr<internal::GcpDetector> const& detector,
    std::shared_ptr<DirectPathNetworkSystem> const& network_system,
    std::string const& metadata_host, std::uint16_t metadata_port) {
  // Step 1: Check platform (GCP VM)
  auto effective_detector = detector;
  if (effective_detector == nullptr) {
    effective_detector = internal::MakeGcpDetector();
  }
  if (!effective_detector->IsGoogleCloudBios()) {
    return DiagnosticFailureReason::kNotInGcp;
  }

  auto effective_network_system = network_system;
  if (effective_network_system == nullptr) {
    effective_network_system = MakeDefaultDirectPathNetworkSystem();
  }

  // Step 2: Metadata server reachability (bounded to the per-step metadata
  // probe timeout so unreachable metadata never stalls diagnostics for 60s).
  std::chrono::milliseconds timeout =
      options.get<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>();
  if (timeout <= std::chrono::milliseconds::zero()) {
    timeout = bigtable::internal::DefaultDirectPathDiagnosticsTimeout();
  }
  std::chrono::milliseconds const metadata_timeout =
      (std::min)(timeout, DefaultDirectPathMetadataTimeout());

  if (!effective_network_system->CanConnectTcp(metadata_host, metadata_port,
                                               metadata_timeout)) {
    return DiagnosticFailureReason::kMetadataUnreachable;
  }

  // Step 3 & 4: Loopback configuration check
  DiagnosticFailureReason const lo_result =
      effective_network_system->CheckLoopbackConfiguration();
  if (lo_result != DiagnosticFailureReason::kUnknown) {
    return lo_result;
  }

  // Step 5: Route resolution or fallback
  return DiagnosticFailureReason::kUnknown;
}

#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
std::unique_ptr<DirectPathDiagnostics::AsyncTask>
DirectPathDiagnostics::StartAsync(
    Options const& options,
    std::weak_ptr<DirectAccessCompatibility> direct_access_compatibility) {
  auto const [metadata_host, metadata_port] = ResolveMetadataEndpoint();
  return StartAsync(options, std::move(direct_access_compatibility),
                    internal::MakeGcpDetector(), nullptr, metadata_host,
                    metadata_port);
}

std::unique_ptr<DirectPathDiagnostics::AsyncTask>
DirectPathDiagnostics::StartAsync(
    Options const& options,
    std::weak_ptr<DirectAccessCompatibility> direct_access_compatibility,
    std::shared_ptr<internal::GcpDetector> detector,
    std::shared_ptr<DirectPathNetworkSystem> network_system,
    std::string const& metadata_host, std::uint16_t metadata_port) {
  auto run_options = options;
  if (run_options
          .get<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>() <=
      std::chrono::milliseconds::zero()) {
    run_options.set<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>(
        bigtable::internal::DefaultDirectPathDiagnosticsTimeout());
  }

  auto cancel_state = std::make_shared<CancelState>();
  if (detector == nullptr) {
    detector = internal::MakeGcpDetector();
  }
  if (network_system == nullptr) {
    network_system =
        std::make_shared<DefaultDirectPathNetworkSystem>(cancel_state);
  }

  std::thread worker([run_options = std::move(run_options),
                      direct_access_compatibility =
                          std::move(direct_access_compatibility),
                      detector = std::move(detector),
                      network_system = std::move(network_system), metadata_host,
                      metadata_port, cancel_state]() {
    if (cancel_state->IsCancelled()) return;
    DiagnosticFailureReason const reason =
        DirectPathDiagnostics::RunDiagnostics(run_options, detector,
                                              network_system, metadata_host,
                                              metadata_port);
    if (cancel_state->IsCancelled()) return;
    if (auto compat = direct_access_compatibility.lock()) {
      compat->Record(opentelemetry::context::RuntimeContext::GetCurrent(), 0,
                     DirectAccessCompatibilityLabels{"", ToString(reason)});
    }
  });

  return std::make_unique<AsyncTask>(std::move(cancel_state),
                                     std::move(worker));
}

future<void> DirectPathDiagnostics::RunAsync(CompletionQueue cq,
                                             Options const& options) {
  return RunAsync(std::move(cq), options, nullptr);
}

future<void> DirectPathDiagnostics::RunAsync(
    CompletionQueue cq, Options const& options,
    std::shared_ptr<DirectAccessCompatibility> direct_access_compatibility) {
  auto const [metadata_host, metadata_port] = ResolveMetadataEndpoint();
  return RunAsync(
      std::move(cq), options, std::move(direct_access_compatibility),
      internal::MakeGcpDetector(), nullptr, metadata_host, metadata_port);
}

future<void> DirectPathDiagnostics::RunAsync(
    CompletionQueue cq, Options const& options,
    std::shared_ptr<DirectAccessCompatibility> direct_access_compatibility,
    std::shared_ptr<internal::GcpDetector> detector,
    std::shared_ptr<DirectPathNetworkSystem> network_system,
    std::string const& metadata_host, std::uint16_t metadata_port) {
  (void)cq;
  auto run_options = options;
  if (run_options
          .get<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>() <=
      std::chrono::milliseconds::zero()) {
    run_options.set<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>(
        bigtable::internal::DefaultDirectPathDiagnosticsTimeout());
  }

  auto cancel_state = std::make_shared<CancelState>();
  if (detector == nullptr) {
    detector = internal::MakeGcpDetector();
  }
  if (network_system == nullptr) {
    network_system =
        std::make_shared<DefaultDirectPathNetworkSystem>(cancel_state);
  }

  struct SharedWorker {
    std::mutex mu;
    std::thread worker;
    std::atomic<bool> finished{false};

    ~SharedWorker() {
      std::lock_guard<std::mutex> lk(mu);
      if (!worker.joinable()) return;
      if (finished.load(std::memory_order_acquire) &&
          worker.get_id() != std::this_thread::get_id()) {
        worker.join();
      } else {
        worker.detach();
      }
    }
  };

  auto shared_worker = std::make_shared<SharedWorker>();
  promise<void> done_promise(
      [cancel_state, shared_worker] { cancel_state->Cancel(); });
  future<void> done_future = done_promise.get_future();

  {
    std::lock_guard<std::mutex> lk(shared_worker->mu);
    shared_worker->worker = std::thread(
        [run_options = std::move(run_options),
         direct_access_compatibility = std::move(direct_access_compatibility),
         detector = std::move(detector),
         network_system = std::move(network_system), metadata_host,
         metadata_port, cancel_state,
         weak_worker = std::weak_ptr<SharedWorker>(shared_worker),
         done_promise = std::move(done_promise)]() mutable {
          {
            auto opts = std::move(run_options);
            auto compat = std::move(direct_access_compatibility);
            auto det = std::move(detector);
            auto net = std::move(network_system);
            auto host = std::move(metadata_host);
            if (!cancel_state->IsCancelled()) {
              DiagnosticFailureReason const reason =
                  DirectPathDiagnostics::RunDiagnostics(opts, det, net, host,
                                                        metadata_port);
              if (!cancel_state->IsCancelled() && compat != nullptr) {
                compat->Record(
                    opentelemetry::context::RuntimeContext::GetCurrent(), 0,
                    DirectAccessCompatibilityLabels{"", ToString(reason)});
              }
            }
          }
          if (auto sw = weak_worker.lock()) {
            sw->finished.store(true, std::memory_order_release);
          }
          auto local_p = std::move(done_promise);
          local_p.set_value();
        });
  }

  return done_future;
}
#endif

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
