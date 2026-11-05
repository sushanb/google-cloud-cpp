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
#include "google/cloud/bigtable/options.h"
#include "google/cloud/completion_queue.h"
#include "google/cloud/internal/detect_gcp.h"
#include "google/cloud/testing_util/scoped_environment.h"
#include <gmock/gmock.h>
#include <chrono>
#include <cstring>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#ifndef _WIN32
#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <ifaddrs.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#if !defined(_WIN32) && !defined(__MACH__)
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
#include "google/cloud/bigtable/internal/client_schema_metrics.h"
#include "google/cloud/testing_util/mock_opentelemetry_metrics.h"
#include "google/cloud/testing_util/opentelemetry_attributes.h"
#endif
#endif

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace {

using ::testing::AnyOf;
using ::testing::Eq;

#if !defined(_WIN32) && !defined(__MACH__)
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
using ::google::cloud::testing_util::MakeAttributesMap;
#if OPENTELEMETRY_ABI_VERSION_NO >= 2
using ::google::cloud::testing_util::MockGauge;
using ::testing::_;
#endif
using ::google::cloud::testing_util::MockMeter;
using ::google::cloud::testing_util::MockMeterProvider;
using ::google::cloud::testing_util::MockObservableInstrument;
using ::google::cloud::testing_util::MockObserverResult;
using ::testing::A;
using ::testing::AnyNumber;
using ::testing::Contains;
using ::testing::Key;
using ::testing::NotNull;
using ::testing::Pair;
#endif
#endif

class MockGcpDetector : public internal::GcpDetector {
 public:
  MOCK_METHOD(bool, IsGoogleCloudBios, (), (override));
  MOCK_METHOD(bool, IsGoogleCloudServerless, (), (override));
};

class MockDirectPathNetworkSystem : public DirectPathNetworkSystem {
 public:
  MOCK_METHOD(bool, CanConnectTcp,
              (std::string const& host, std::uint16_t port,
               std::chrono::milliseconds timeout),
              (override));
  MOCK_METHOD(DiagnosticFailureReason, CheckLoopbackConfiguration, (),
              (override));
};

TEST(DirectPathDiagnosticsTest, ToString) {
  EXPECT_THAT(ToString(DiagnosticFailureReason::kNotInGcp), Eq("not_in_gcp"));
  EXPECT_THAT(ToString(DiagnosticFailureReason::kMetadataUnreachable),
              Eq("metadata_unreachable"));
  EXPECT_THAT(ToString(DiagnosticFailureReason::kNoIpAssigned),
              Eq("no_ip_assigned"));
  EXPECT_THAT(ToString(DiagnosticFailureReason::kLoopbackMisconfigured),
              Eq("loopback_misconfigured"));
  EXPECT_THAT(ToString(DiagnosticFailureReason::kLoopbackMisconfiguredIpv4),
              Eq("loopback_misconfigured_ipv4"));
  EXPECT_THAT(ToString(DiagnosticFailureReason::kLoopbackMisconfiguredIpv6),
              Eq("loopback_misconfigured_ipv6"));
  EXPECT_THAT(ToString(DiagnosticFailureReason::kMetadataMissing),
              Eq("metadata_missing"));
  EXPECT_THAT(ToString(DiagnosticFailureReason::kXdsReachabilityFailed),
              Eq("xds_reachability_failed"));
  EXPECT_THAT(ToString(DiagnosticFailureReason::kXdsEdsFailed),
              Eq("xds_eds_failed"));
  EXPECT_THAT(ToString(DiagnosticFailureReason::kXdsMalformedEndpoint),
              Eq("xds_malformed_endpoint"));
  EXPECT_THAT(ToString(DiagnosticFailureReason::kRouteUnreachable),
              Eq("route_unreachable"));
  EXPECT_THAT(ToString(DiagnosticFailureReason::kAltsHandshakeFailed),
              Eq("alts_handshake_failed"));
  EXPECT_THAT(ToString(DiagnosticFailureReason::kTimeout), Eq("timeout"));
  EXPECT_THAT(ToString(DiagnosticFailureReason::kUnknown), Eq("unknown"));
  EXPECT_THAT(ToString(static_cast<DiagnosticFailureReason>(99)),
              Eq("unknown"));
}

TEST(DirectPathDiagnosticsTest, RunDiagnosticsDefaultOptions) {
  DiagnosticFailureReason const reason =
      DirectPathDiagnostics::RunDiagnostics(Options{});
  EXPECT_THAT(reason,
              AnyOf(Eq(DiagnosticFailureReason::kNotInGcp),
                    Eq(DiagnosticFailureReason::kMetadataUnreachable),
                    Eq(DiagnosticFailureReason::kLoopbackMisconfigured),
                    Eq(DiagnosticFailureReason::kLoopbackMisconfiguredIpv4),
                    Eq(DiagnosticFailureReason::kLoopbackMisconfiguredIpv6),
                    Eq(DiagnosticFailureReason::kUnknown)));
}

TEST(DirectPathDiagnosticsTest, RunDiagnosticsWithTimeoutOption) {
  Options const options =
      Options{}.set<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>(
          std::chrono::milliseconds(100));
  DiagnosticFailureReason const reason =
      DirectPathDiagnostics::RunDiagnostics(options);
  EXPECT_THAT(reason,
              AnyOf(Eq(DiagnosticFailureReason::kNotInGcp),
                    Eq(DiagnosticFailureReason::kMetadataUnreachable),
                    Eq(DiagnosticFailureReason::kLoopbackMisconfigured),
                    Eq(DiagnosticFailureReason::kLoopbackMisconfiguredIpv4),
                    Eq(DiagnosticFailureReason::kLoopbackMisconfiguredIpv6),
                    Eq(DiagnosticFailureReason::kUnknown)));
}

TEST(DirectPathDiagnosticsTest, RunDiagnosticsNotInGcp) {
  auto mock_detector = std::make_shared<MockGcpDetector>();
  EXPECT_CALL(*mock_detector, IsGoogleCloudBios)
      .WillOnce(testing::Return(false));

  DiagnosticFailureReason const reason = DirectPathDiagnostics::RunDiagnostics(
      Options{}, mock_detector, nullptr, "127.0.0.1", 80);
  EXPECT_THAT(reason, Eq(DiagnosticFailureReason::kNotInGcp));
}

TEST(DirectPathDiagnosticsTest, RunDiagnosticsMetadataUnreachable) {
  auto mock_detector = std::make_shared<MockGcpDetector>();
  EXPECT_CALL(*mock_detector, IsGoogleCloudBios)
      .WillOnce(testing::Return(true));

  auto mock_network = std::make_shared<MockDirectPathNetworkSystem>();
  EXPECT_CALL(*mock_network, CanConnectTcp).WillOnce(testing::Return(false));

  Options const options =
      Options{}.set<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>(
          std::chrono::milliseconds(50));
  DiagnosticFailureReason const reason = DirectPathDiagnostics::RunDiagnostics(
      options, mock_detector, mock_network, "127.0.0.1", 80);
  EXPECT_THAT(reason, Eq(DiagnosticFailureReason::kMetadataUnreachable));
}

TEST(DirectPathDiagnosticsTest, RunDiagnosticsLoopbackMisconfigured) {
  auto mock_detector = std::make_shared<MockGcpDetector>();
  EXPECT_CALL(*mock_detector, IsGoogleCloudBios)
      .WillOnce(testing::Return(true));

  auto mock_network = std::make_shared<MockDirectPathNetworkSystem>();
  EXPECT_CALL(*mock_network, CanConnectTcp).WillOnce(testing::Return(true));
  EXPECT_CALL(*mock_network, CheckLoopbackConfiguration)
      .WillOnce(
          testing::Return(DiagnosticFailureReason::kLoopbackMisconfigured));

  DiagnosticFailureReason const reason = DirectPathDiagnostics::RunDiagnostics(
      Options{}, mock_detector, mock_network, "127.0.0.1", 80);
  EXPECT_THAT(reason, Eq(DiagnosticFailureReason::kLoopbackMisconfigured));
}

TEST(DirectPathDiagnosticsTest, RunDiagnosticsLoopbackMisconfiguredIpv4) {
  auto mock_detector = std::make_shared<MockGcpDetector>();
  EXPECT_CALL(*mock_detector, IsGoogleCloudBios)
      .WillOnce(testing::Return(true));

  auto mock_network = std::make_shared<MockDirectPathNetworkSystem>();
  EXPECT_CALL(*mock_network, CanConnectTcp).WillOnce(testing::Return(true));
  EXPECT_CALL(*mock_network, CheckLoopbackConfiguration)
      .WillOnce(
          testing::Return(DiagnosticFailureReason::kLoopbackMisconfiguredIpv4));

  DiagnosticFailureReason const reason = DirectPathDiagnostics::RunDiagnostics(
      Options{}, mock_detector, mock_network, "127.0.0.1", 80);
  EXPECT_THAT(reason, Eq(DiagnosticFailureReason::kLoopbackMisconfiguredIpv4));
}

TEST(DirectPathDiagnosticsTest, RunDiagnosticsLoopbackMisconfiguredIpv6) {
  auto mock_detector = std::make_shared<MockGcpDetector>();
  EXPECT_CALL(*mock_detector, IsGoogleCloudBios)
      .WillOnce(testing::Return(true));

  auto mock_network = std::make_shared<MockDirectPathNetworkSystem>();
  EXPECT_CALL(*mock_network, CanConnectTcp).WillOnce(testing::Return(true));
  EXPECT_CALL(*mock_network, CheckLoopbackConfiguration)
      .WillOnce(
          testing::Return(DiagnosticFailureReason::kLoopbackMisconfiguredIpv6));

  DiagnosticFailureReason const reason = DirectPathDiagnostics::RunDiagnostics(
      Options{}, mock_detector, mock_network, "127.0.0.1", 80);
  EXPECT_THAT(reason, Eq(DiagnosticFailureReason::kLoopbackMisconfiguredIpv6));
}

TEST(DirectPathDiagnosticsTest, RunDiagnosticsHealthyLoopbackReturnsUnknown) {
  auto mock_detector = std::make_shared<MockGcpDetector>();
  EXPECT_CALL(*mock_detector, IsGoogleCloudBios)
      .WillOnce(testing::Return(true));

  auto mock_network = std::make_shared<MockDirectPathNetworkSystem>();
  EXPECT_CALL(*mock_network, CanConnectTcp).WillOnce(testing::Return(true));
  EXPECT_CALL(*mock_network, CheckLoopbackConfiguration)
      .WillOnce(testing::Return(DiagnosticFailureReason::kUnknown));

  DiagnosticFailureReason const reason = DirectPathDiagnostics::RunDiagnostics(
      Options{}, mock_detector, mock_network, "127.0.0.1", 80);
  EXPECT_THAT(reason, Eq(DiagnosticFailureReason::kUnknown));
}

TEST(DirectPathDiagnosticsTest,
     RunDiagnosticsCapsMetadataTimeoutToDefaultMetadataTimeout) {
  auto mock_detector = std::make_shared<MockGcpDetector>();
  EXPECT_CALL(*mock_detector, IsGoogleCloudBios)
      .Times(3)
      .WillRepeatedly(testing::Return(true));

  auto mock_network = std::make_shared<MockDirectPathNetworkSystem>();
  EXPECT_CALL(*mock_network,
              CanConnectTcp(Eq("127.0.0.1"), Eq(static_cast<std::uint16_t>(80)),
                            Eq(DefaultDirectPathMetadataTimeout())))
      .Times(2)
      .WillRepeatedly(testing::Return(false));
  EXPECT_CALL(*mock_network,
              CanConnectTcp(Eq("127.0.0.1"), Eq(static_cast<std::uint16_t>(80)),
                            Eq(std::chrono::milliseconds(50))))
      .WillOnce(testing::Return(false));

  EXPECT_THAT(DirectPathDiagnostics::RunDiagnostics(
                  Options{}, mock_detector, mock_network, "127.0.0.1", 80),
              Eq(DiagnosticFailureReason::kMetadataUnreachable));

  Options const long_timeout =
      Options{}.set<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>(
          std::chrono::seconds(60));
  EXPECT_THAT(DirectPathDiagnostics::RunDiagnostics(
                  long_timeout, mock_detector, mock_network, "127.0.0.1", 80),
              Eq(DiagnosticFailureReason::kMetadataUnreachable));

  Options const short_timeout =
      Options{}.set<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>(
          std::chrono::milliseconds(50));
  EXPECT_THAT(DirectPathDiagnostics::RunDiagnostics(
                  short_timeout, mock_detector, mock_network, "127.0.0.1", 80),
              Eq(DiagnosticFailureReason::kMetadataUnreachable));
}

#ifndef _WIN32
class ScopedTcpListener {
 public:
  ScopedTcpListener() {
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) return;
    int const opt = 1;
    setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) !=
        0) {
      close(fd_);
      fd_ = -1;
      return;
    }
    if (listen(fd_, 4) != 0) {
      close(fd_);
      fd_ = -1;
      return;
    }
    socklen_t len = sizeof(addr);
    if (getsockname(fd_, reinterpret_cast<struct sockaddr*>(&addr), &len) ==
        0) {
      port_ = ntohs(addr.sin_port);
    }
  }

  ~ScopedTcpListener() {
    if (fd_ >= 0) close(fd_);
  }

  std::uint16_t port() const { return port_; }
  bool is_valid() const { return fd_ >= 0 && port_ > 0; }

 private:
  int fd_ = -1;
  std::uint16_t port_ = 0;
};

class ScopedTcpListenerV6 {
 public:
  ScopedTcpListenerV6() {
    fd_ = socket(AF_INET6, SOCK_STREAM, 0);
    if (fd_ < 0) return;
    int const opt = 1;
    setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in6 addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin6_family = AF_INET6;
    addr.sin6_addr = in6addr_loopback;
    addr.sin6_port = 0;
    if (bind(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) !=
        0) {
      close(fd_);
      fd_ = -1;
      return;
    }
    if (listen(fd_, 4) != 0) {
      close(fd_);
      fd_ = -1;
      return;
    }
    socklen_t len = sizeof(addr);
    if (getsockname(fd_, reinterpret_cast<struct sockaddr*>(&addr), &len) ==
        0) {
      port_ = ntohs(addr.sin6_port);
    }
  }

  ~ScopedTcpListenerV6() {
    if (fd_ >= 0) close(fd_);
  }

  std::uint16_t port() const { return port_; }
  bool is_valid() const { return fd_ >= 0 && port_ > 0; }

 private:
  int fd_ = -1;
  std::uint16_t port_ = 0;
};

TEST(DirectPathDiagnosticsTest, DefaultNetworkSystemCanConnectTcp) {
  ScopedTcpListener listener;
  ASSERT_TRUE(listener.is_valid());

  auto network_system = MakeDefaultDirectPathNetworkSystem();
  ASSERT_THAT(network_system, testing::NotNull());

  EXPECT_TRUE(network_system->CanConnectTcp("127.0.0.1", listener.port(),
                                            std::chrono::milliseconds(500)));
  EXPECT_TRUE(network_system->CanConnectTcp("localhost", listener.port(),
                                            std::chrono::milliseconds(500)));
  EXPECT_FALSE(network_system->CanConnectTcp("127.0.0.1", 1,
                                             std::chrono::milliseconds(50)));
  EXPECT_FALSE(network_system->CanConnectTcp(
      "invalid.ip.address", listener.port(), std::chrono::milliseconds(50)));
  EXPECT_FALSE(network_system->CanConnectTcp("", listener.port(),
                                             std::chrono::milliseconds(50)));
  EXPECT_FALSE(network_system->CanConnectTcp("127.0.0.1", 0,
                                             std::chrono::milliseconds(50)));
  EXPECT_FALSE(network_system->CanConnectTcp(
      "127.0.0.1", listener.port(), std::chrono::milliseconds::zero()));

  ScopedTcpListenerV6 listener_v6;
  if (listener_v6.is_valid()) {
    EXPECT_TRUE(network_system->CanConnectTcp("::1", listener_v6.port(),
                                              std::chrono::milliseconds(500)));
    EXPECT_TRUE(network_system->CanConnectTcp("[::1]", listener_v6.port(),
                                              std::chrono::milliseconds(500)));
  }
}

TEST(DirectPathDiagnosticsTest, RunDiagnosticsWithGceMetadataHostOverride) {
  ScopedTcpListener listener;
  ASSERT_TRUE(listener.is_valid());

  testing_util::ScopedEnvironment env(
      "GCE_METADATA_HOST", "127.0.0.1:" + std::to_string(listener.port()));
  DiagnosticFailureReason const reason =
      DirectPathDiagnostics::RunDiagnostics(Options{});
  EXPECT_THAT(reason,
              AnyOf(Eq(DiagnosticFailureReason::kNotInGcp),
                    Eq(DiagnosticFailureReason::kLoopbackMisconfigured),
                    Eq(DiagnosticFailureReason::kLoopbackMisconfiguredIpv4),
                    Eq(DiagnosticFailureReason::kLoopbackMisconfiguredIpv6),
                    Eq(DiagnosticFailureReason::kUnknown)));
}

TEST(DirectPathDiagnosticsTest, DefaultNetworkSystemCheckLoopback) {
  auto network_system = MakeDefaultDirectPathNetworkSystem();
  ASSERT_THAT(network_system, testing::NotNull());

  DiagnosticFailureReason const reason =
      network_system->CheckLoopbackConfiguration();
  EXPECT_THAT(reason,
              AnyOf(Eq(DiagnosticFailureReason::kUnknown),
                    Eq(DiagnosticFailureReason::kLoopbackMisconfigured),
                    Eq(DiagnosticFailureReason::kLoopbackMisconfiguredIpv4),
                    Eq(DiagnosticFailureReason::kLoopbackMisconfiguredIpv6)));
}

TEST(DirectPathDiagnosticsTest, EvaluateLoopbackIfaddrsUsesFlagsNotName) {
  struct sockaddr_in v4_addr;
  std::memset(&v4_addr, 0, sizeof(v4_addr));
  v4_addr.sin_family = AF_INET;

  struct sockaddr_in6 v6_addr;
  std::memset(&v6_addr, 0, sizeof(v6_addr));
  v6_addr.sin6_family = AF_INET6;

  EXPECT_THAT(EvaluateLoopbackIfaddrs(nullptr),
              Eq(DiagnosticFailureReason::kLoopbackMisconfigured));

  char name_lo0[] = "lo0";
  char name_lo[] = "lo";
  char name_eth0[] = "eth0";

  // Non-"lo" name ("lo0") with IFF_LOOPBACK | IFF_UP and both IPv4 + IPv6.
  struct ifaddrs ifa_v6{};
  ifa_v6.ifa_name = name_lo0;
  ifa_v6.ifa_flags = IFF_LOOPBACK | IFF_UP;
  ifa_v6.ifa_addr = reinterpret_cast<struct sockaddr*>(&v6_addr);

  struct ifaddrs ifa_v4{};
  ifa_v4.ifa_next = &ifa_v6;
  ifa_v4.ifa_name = name_lo0;
  ifa_v4.ifa_flags = IFF_LOOPBACK | IFF_UP;
  ifa_v4.ifa_addr = reinterpret_cast<struct sockaddr*>(&v4_addr);

  EXPECT_THAT(EvaluateLoopbackIfaddrs(&ifa_v4),
              Eq(DiagnosticFailureReason::kUnknown));

  // IPv4 only on up loopback -> kLoopbackMisconfiguredIpv6.
  ifa_v4.ifa_next = nullptr;
  EXPECT_THAT(EvaluateLoopbackIfaddrs(&ifa_v4),
              Eq(DiagnosticFailureReason::kLoopbackMisconfiguredIpv6));

  // IPv6 only on up loopback -> kLoopbackMisconfiguredIpv4.
  EXPECT_THAT(EvaluateLoopbackIfaddrs(&ifa_v6),
              Eq(DiagnosticFailureReason::kLoopbackMisconfiguredIpv4));

  // Loopback named "lo" that is down (!IFF_UP) -> kLoopbackMisconfigured.
  struct ifaddrs down_v6{};
  down_v6.ifa_name = name_lo;
  down_v6.ifa_flags = IFF_LOOPBACK;
  down_v6.ifa_addr = reinterpret_cast<struct sockaddr*>(&v6_addr);

  struct ifaddrs down_v4{};
  down_v4.ifa_next = &down_v6;
  down_v4.ifa_name = name_lo;
  down_v4.ifa_flags = IFF_LOOPBACK;
  down_v4.ifa_addr = reinterpret_cast<struct sockaddr*>(&v4_addr);

  EXPECT_THAT(EvaluateLoopbackIfaddrs(&down_v4),
              Eq(DiagnosticFailureReason::kLoopbackMisconfigured));

  // Non-loopback interface ("eth0") that is up (!IFF_LOOPBACK) -> ignored.
  struct ifaddrs eth_v6{};
  eth_v6.ifa_name = name_eth0;
  eth_v6.ifa_flags = IFF_UP;
  eth_v6.ifa_addr = reinterpret_cast<struct sockaddr*>(&v6_addr);

  struct ifaddrs eth_v4{};
  eth_v4.ifa_next = &eth_v6;
  eth_v4.ifa_name = name_eth0;
  eth_v4.ifa_flags = IFF_UP;
  eth_v4.ifa_addr = reinterpret_cast<struct sockaddr*>(&v4_addr);

  struct ifaddrs null_addr{};
  null_addr.ifa_next = &eth_v4;
  null_addr.ifa_name = name_lo;
  null_addr.ifa_flags = IFF_LOOPBACK | IFF_UP;
  null_addr.ifa_addr = nullptr;

  EXPECT_THAT(EvaluateLoopbackIfaddrs(&null_addr),
              Eq(DiagnosticFailureReason::kLoopbackMisconfigured));
}
#endif

#if !defined(_WIN32) && !defined(__MACH__)
#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS

TEST(DirectPathDiagnosticsTest, RunAsyncNullDirectAccessCompatibility) {
  google::cloud::CompletionQueue cq;
  std::thread t([&cq] { cq.Run(); });

  auto done = DirectPathDiagnostics::RunAsync(cq, Options{});
  ASSERT_THAT(done.wait_for(std::chrono::seconds(10)),
              Eq(std::future_status::ready));

  cq.CancelAll();
  cq.Shutdown();
  t.join();
}

TEST(DirectPathDiagnosticsTest, RunAsyncDefaultTimeoutWhenNonPositive) {
  google::cloud::CompletionQueue cq;
  std::thread t([&cq] { cq.Run(); });

  Options const options =
      Options{}.set<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>(
          std::chrono::milliseconds(-10));
  auto done = DirectPathDiagnostics::RunAsync(cq, options);
  ASSERT_THAT(done.wait_for(std::chrono::seconds(10)),
              Eq(std::future_status::ready));

  cq.CancelAll();
  cq.Shutdown();
  t.join();
}

TEST(DirectPathDiagnosticsTest, RunAsyncRecordsDiagnosticMetric) {
#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  auto mock_gauge = std::make_unique<MockGauge<std::int64_t>>();
  auto* mock_gauge_ptr = mock_gauge.get();
  EXPECT_CALL(
      *mock_gauge_ptr,
      Record(Eq(0), A<opentelemetry::common::KeyValueIterable const&>(), _))
      .WillOnce([](std::int64_t value,
                   opentelemetry::common::KeyValueIterable const& attrs,
                   opentelemetry::context::Context const&) {
        EXPECT_THAT(value, Eq(0));
        auto const map = MakeAttributesMap(attrs);
        EXPECT_THAT(map, Contains(Key("reason")));
        EXPECT_THAT(map, Contains(Pair("ip_preference", "")));
      });

  auto mock_meter = std::make_shared<MockMeter>();
  EXPECT_CALL(*mock_meter, CreateInt64Gauge)
      .WillOnce([mock = std::move(mock_gauge)](
                    opentelemetry::nostd::string_view name,
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view) mutable {
        EXPECT_THAT(name, Eq("direct_access/compatible"));
        return std::move(mock);
      });

  auto mock_provider = std::make_shared<MockMeterProvider>();
  EXPECT_CALL(*mock_provider, GetMeter)
      .WillOnce([&mock_meter](opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::common::KeyValueIterable const*) {
        return mock_meter;
      });
#else
  auto mock_observable_gauge = std::make_shared<MockObservableInstrument>();
  opentelemetry::metrics::ObservableCallbackPtr saved_callback = nullptr;
  void* saved_state = nullptr;
  EXPECT_CALL(*mock_observable_gauge, AddCallback)
      .WillRepeatedly(
          [&](opentelemetry::metrics::ObservableCallbackPtr cb, void* state) {
            saved_callback = cb;
            saved_state = state;
          });
  EXPECT_CALL(*mock_observable_gauge, RemoveCallback).Times(AnyNumber());

  auto mock_meter = std::make_shared<MockMeter>();
  EXPECT_CALL(*mock_meter, CreateInt64ObservableGauge)
      .WillOnce([mock = mock_observable_gauge](
                    opentelemetry::nostd::string_view name,
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view) mutable {
        EXPECT_THAT(name, Eq("direct_access/compatible"));
        return mock;
      });

  auto mock_provider = std::make_shared<MockMeterProvider>();
  EXPECT_CALL(*mock_provider, GetMeter)
      .WillOnce([&mock_meter](opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view) {
        return mock_meter;
      });
#endif

  auto direct_access = std::make_shared<DirectAccessCompatibility>(
      "test-instrumentation-scope", mock_provider, ClientResourceLabels{});

  Options const options =
      Options{}.set<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>(
          std::chrono::milliseconds(500));

  google::cloud::CompletionQueue cq;
  std::thread t([&cq] { cq.Run(); });

  auto done = DirectPathDiagnostics::RunAsync(cq, options, direct_access);
  ASSERT_THAT(done.wait_for(std::chrono::seconds(10)),
              Eq(std::future_status::ready));

  cq.CancelAll();
  cq.Shutdown();
  t.join();

#if OPENTELEMETRY_ABI_VERSION_NO < 2
  auto mock_observer = std::make_shared<MockObserverResult<std::int64_t>>();
  EXPECT_CALL(
      *mock_observer,
      Observe(Eq(0), A<opentelemetry::common::KeyValueIterable const&>()))
      .WillOnce([](std::int64_t value,
                   opentelemetry::common::KeyValueIterable const& attrs) {
        EXPECT_THAT(value, Eq(0));
        auto const map = MakeAttributesMap(attrs);
        EXPECT_THAT(map, Contains(Key("reason")));
        EXPECT_THAT(map, Contains(Pair("ip_preference", "")));
      });

  ASSERT_THAT(saved_callback, NotNull());
  opentelemetry::metrics::ObserverResult observer_result =
      opentelemetry::nostd::shared_ptr<
          opentelemetry::metrics::ObserverResultT<std::int64_t>>(mock_observer);
  saved_callback(observer_result, saved_state);
#endif
}

TEST(DirectPathDiagnosticsTest, RunAsyncRecordsMetadataUnreachableReason) {
  auto mock_detector = std::make_shared<MockGcpDetector>();
  EXPECT_CALL(*mock_detector, IsGoogleCloudBios)
      .WillOnce(testing::Return(true));

  auto mock_network = std::make_shared<MockDirectPathNetworkSystem>();
  EXPECT_CALL(*mock_network, CanConnectTcp).WillOnce(testing::Return(false));

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  auto mock_gauge = std::make_unique<MockGauge<std::int64_t>>();
  auto* mock_gauge_ptr = mock_gauge.get();
  EXPECT_CALL(
      *mock_gauge_ptr,
      Record(Eq(0), A<opentelemetry::common::KeyValueIterable const&>(), _))
      .WillOnce([](std::int64_t value,
                   opentelemetry::common::KeyValueIterable const& attrs,
                   opentelemetry::context::Context const&) {
        EXPECT_THAT(value, Eq(0));
        auto const map = MakeAttributesMap(attrs);
        EXPECT_THAT(map, Contains(Pair("reason", "metadata_unreachable")));
        EXPECT_THAT(map, Contains(Pair("ip_preference", "")));
      });

  auto mock_meter = std::make_shared<MockMeter>();
  EXPECT_CALL(*mock_meter, CreateInt64Gauge)
      .WillOnce([mock = std::move(mock_gauge)](
                    opentelemetry::nostd::string_view name,
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view) mutable {
        EXPECT_THAT(name, Eq("direct_access/compatible"));
        return std::move(mock);
      });

  auto mock_provider = std::make_shared<MockMeterProvider>();
  EXPECT_CALL(*mock_provider, GetMeter)
      .WillOnce([&mock_meter](opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::common::KeyValueIterable const*) {
        return mock_meter;
      });
#else
  auto mock_observable_gauge = std::make_shared<MockObservableInstrument>();
  opentelemetry::metrics::ObservableCallbackPtr saved_callback = nullptr;
  void* saved_state = nullptr;
  EXPECT_CALL(*mock_observable_gauge, AddCallback)
      .WillRepeatedly(
          [&](opentelemetry::metrics::ObservableCallbackPtr cb, void* state) {
            saved_callback = cb;
            saved_state = state;
          });
  EXPECT_CALL(*mock_observable_gauge, RemoveCallback).Times(AnyNumber());

  auto mock_meter = std::make_shared<MockMeter>();
  EXPECT_CALL(*mock_meter, CreateInt64ObservableGauge)
      .WillOnce([mock = mock_observable_gauge](
                    opentelemetry::nostd::string_view name,
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view) mutable {
        EXPECT_THAT(name, Eq("direct_access/compatible"));
        return mock;
      });

  auto mock_provider = std::make_shared<MockMeterProvider>();
  EXPECT_CALL(*mock_provider, GetMeter)
      .WillOnce([&mock_meter](opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view) {
        return mock_meter;
      });
#endif

  auto direct_access = std::make_shared<DirectAccessCompatibility>(
      "test-instrumentation-scope", mock_provider, ClientResourceLabels{});

  Options const options =
      Options{}.set<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>(
          std::chrono::milliseconds(50));

  google::cloud::CompletionQueue cq;
  std::thread t([&cq] { cq.Run(); });

  auto done = DirectPathDiagnostics::RunAsync(
      cq, options, direct_access, mock_detector, mock_network, "127.0.0.1",
      static_cast<std::uint16_t>(80));
  ASSERT_THAT(done.wait_for(std::chrono::seconds(10)),
              Eq(std::future_status::ready));

  cq.CancelAll();
  cq.Shutdown();
  t.join();

#if OPENTELEMETRY_ABI_VERSION_NO < 2
  auto mock_observer = std::make_shared<MockObserverResult<std::int64_t>>();
  EXPECT_CALL(
      *mock_observer,
      Observe(Eq(0), A<opentelemetry::common::KeyValueIterable const&>()))
      .WillOnce([](std::int64_t value,
                   opentelemetry::common::KeyValueIterable const& attrs) {
        EXPECT_THAT(value, Eq(0));
        auto const map = MakeAttributesMap(attrs);
        EXPECT_THAT(map, Contains(Pair("reason", "metadata_unreachable")));
        EXPECT_THAT(map, Contains(Pair("ip_preference", "")));
      });

  ASSERT_THAT(saved_callback, NotNull());
  opentelemetry::metrics::ObserverResult observer_result =
      opentelemetry::nostd::shared_ptr<
          opentelemetry::metrics::ObserverResultT<std::int64_t>>(mock_observer);
  saved_callback(observer_result, saved_state);
#endif
}

TEST(DirectPathDiagnosticsTest, RunAsyncRecordsLoopbackMisconfiguredReason) {
  auto mock_detector = std::make_shared<MockGcpDetector>();
  EXPECT_CALL(*mock_detector, IsGoogleCloudBios)
      .WillOnce(testing::Return(true));

  auto mock_network = std::make_shared<MockDirectPathNetworkSystem>();
  EXPECT_CALL(*mock_network, CanConnectTcp).WillOnce(testing::Return(true));
  EXPECT_CALL(*mock_network, CheckLoopbackConfiguration)
      .WillOnce(
          testing::Return(DiagnosticFailureReason::kLoopbackMisconfiguredIpv6));

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  auto mock_gauge = std::make_unique<MockGauge<std::int64_t>>();
  auto* mock_gauge_ptr = mock_gauge.get();
  EXPECT_CALL(
      *mock_gauge_ptr,
      Record(Eq(0), A<opentelemetry::common::KeyValueIterable const&>(), _))
      .WillOnce([](std::int64_t value,
                   opentelemetry::common::KeyValueIterable const& attrs,
                   opentelemetry::context::Context const&) {
        EXPECT_THAT(value, Eq(0));
        auto const map = MakeAttributesMap(attrs);
        EXPECT_THAT(map,
                    Contains(Pair("reason", "loopback_misconfigured_ipv6")));
        EXPECT_THAT(map, Contains(Pair("ip_preference", "")));
      });

  auto mock_meter = std::make_shared<MockMeter>();
  EXPECT_CALL(*mock_meter, CreateInt64Gauge)
      .WillOnce([mock = std::move(mock_gauge)](
                    opentelemetry::nostd::string_view name,
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view) mutable {
        EXPECT_THAT(name, Eq("direct_access/compatible"));
        return std::move(mock);
      });

  auto mock_provider = std::make_shared<MockMeterProvider>();
  EXPECT_CALL(*mock_provider, GetMeter)
      .WillOnce([&mock_meter](opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::common::KeyValueIterable const*) {
        return mock_meter;
      });
#else
  auto mock_observable_gauge = std::make_shared<MockObservableInstrument>();
  opentelemetry::metrics::ObservableCallbackPtr saved_callback = nullptr;
  void* saved_state = nullptr;
  EXPECT_CALL(*mock_observable_gauge, AddCallback)
      .WillRepeatedly(
          [&](opentelemetry::metrics::ObservableCallbackPtr cb, void* state) {
            saved_callback = cb;
            saved_state = state;
          });
  EXPECT_CALL(*mock_observable_gauge, RemoveCallback).Times(AnyNumber());

  auto mock_meter = std::make_shared<MockMeter>();
  EXPECT_CALL(*mock_meter, CreateInt64ObservableGauge)
      .WillOnce([mock = mock_observable_gauge](
                    opentelemetry::nostd::string_view name,
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view) mutable {
        EXPECT_THAT(name, Eq("direct_access/compatible"));
        return mock;
      });

  auto mock_provider = std::make_shared<MockMeterProvider>();
  EXPECT_CALL(*mock_provider, GetMeter)
      .WillOnce([&mock_meter](opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view) {
        return mock_meter;
      });
#endif

  auto direct_access = std::make_shared<DirectAccessCompatibility>(
      "test-instrumentation-scope", mock_provider, ClientResourceLabels{});

  Options const options =
      Options{}.set<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>(
          std::chrono::milliseconds(50));

  google::cloud::CompletionQueue cq;
  std::thread t([&cq] { cq.Run(); });

  auto done = DirectPathDiagnostics::RunAsync(
      cq, options, direct_access, mock_detector, mock_network, "127.0.0.1",
      static_cast<std::uint16_t>(80));
  ASSERT_THAT(done.wait_for(std::chrono::seconds(10)),
              Eq(std::future_status::ready));

  cq.CancelAll();
  cq.Shutdown();
  t.join();

#if OPENTELEMETRY_ABI_VERSION_NO < 2
  auto mock_observer = std::make_shared<MockObserverResult<std::int64_t>>();
  EXPECT_CALL(
      *mock_observer,
      Observe(Eq(0), A<opentelemetry::common::KeyValueIterable const&>()))
      .WillOnce([](std::int64_t value,
                   opentelemetry::common::KeyValueIterable const& attrs) {
        EXPECT_THAT(value, Eq(0));
        auto const map = MakeAttributesMap(attrs);
        EXPECT_THAT(map,
                    Contains(Pair("reason", "loopback_misconfigured_ipv6")));
        EXPECT_THAT(map, Contains(Pair("ip_preference", "")));
      });

  ASSERT_THAT(saved_callback, NotNull());
  opentelemetry::metrics::ObserverResult observer_result =
      opentelemetry::nostd::shared_ptr<
          opentelemetry::metrics::ObserverResultT<std::int64_t>>(mock_observer);
  saved_callback(observer_result, saved_state);
#endif
}

TEST(DirectPathDiagnosticsTest,
     RunAsyncDoesNotBlockSingleThreadCompletionQueue) {
  std::promise<void> connect_entered_promise;
  auto connect_entered_future = connect_entered_promise.get_future();
  std::promise<void> unblock_connect_promise;
  auto unblock_connect_future = unblock_connect_promise.get_future().share();

  auto mock_detector = std::make_shared<MockGcpDetector>();
  EXPECT_CALL(*mock_detector, IsGoogleCloudBios)
      .WillOnce(testing::Return(true));

  auto mock_network = std::make_shared<MockDirectPathNetworkSystem>();
  EXPECT_CALL(*mock_network, CanConnectTcp)
      .WillOnce(
          [&connect_entered_promise, unblock_connect_future](
              std::string const&, std::uint16_t, std::chrono::milliseconds) {
            connect_entered_promise.set_value();
            unblock_connect_future.wait();
            return false;
          });

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  auto mock_gauge = std::make_unique<MockGauge<std::int64_t>>();
  auto* mock_gauge_ptr = mock_gauge.get();
  EXPECT_CALL(
      *mock_gauge_ptr,
      Record(Eq(0), A<opentelemetry::common::KeyValueIterable const&>(), _))
      .WillOnce([](std::int64_t value,
                   opentelemetry::common::KeyValueIterable const& attrs,
                   opentelemetry::context::Context const&) {
        EXPECT_THAT(value, Eq(0));
        auto const map = MakeAttributesMap(attrs);
        EXPECT_THAT(map, Contains(Pair("reason", "metadata_unreachable")));
      });

  auto mock_meter = std::make_shared<MockMeter>();
  EXPECT_CALL(*mock_meter, CreateInt64Gauge)
      .WillOnce([mock = std::move(mock_gauge)](
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view) mutable {
        return std::move(mock);
      });

  auto mock_provider = std::make_shared<MockMeterProvider>();
  EXPECT_CALL(*mock_provider, GetMeter)
      .WillOnce([&mock_meter](opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::common::KeyValueIterable const*) {
        return mock_meter;
      });
#else
  auto mock_observable_gauge = std::make_shared<MockObservableInstrument>();
  EXPECT_CALL(*mock_observable_gauge, AddCallback).Times(AnyNumber());
  EXPECT_CALL(*mock_observable_gauge, RemoveCallback).Times(AnyNumber());

  auto mock_meter = std::make_shared<MockMeter>();
  EXPECT_CALL(*mock_meter, CreateInt64ObservableGauge)
      .WillOnce(
          [mock = mock_observable_gauge](
              opentelemetry::nostd::string_view,
              opentelemetry::nostd::string_view,
              opentelemetry::nostd::string_view) mutable { return mock; });

  auto mock_provider = std::make_shared<MockMeterProvider>();
  EXPECT_CALL(*mock_provider, GetMeter)
      .WillOnce([&mock_meter](opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view) {
        return mock_meter;
      });
#endif

  auto direct_access = std::make_shared<DirectAccessCompatibility>(
      "test-instrumentation-scope", mock_provider, ClientResourceLabels{});

  google::cloud::CompletionQueue cq;
  // Intentionally use a single CQ worker thread matching the default
  // AutomaticallyCreatedBackgroundThreads pool size of 1.
  std::thread t([&cq] { cq.Run(); });

  auto done = DirectPathDiagnostics::RunAsync(
      cq, Options{}, direct_access, mock_detector, mock_network, "127.0.0.1",
      static_cast<std::uint16_t>(80));

  ASSERT_THAT(connect_entered_future.wait_for(std::chrono::seconds(10)),
              Eq(std::future_status::ready));

  // While CanConnectTcp is actively blocked, the single CQ thread must still
  // execute RunAsync callbacks and timers without stalling.
  std::promise<void> cq_work_promise;
  auto cq_work_future = cq_work_promise.get_future();
  cq.RunAsync([&cq_work_promise] { cq_work_promise.set_value(); });
  auto timer_future = cq.MakeRelativeTimer(std::chrono::milliseconds(5));

  EXPECT_THAT(cq_work_future.wait_for(std::chrono::seconds(5)),
              Eq(std::future_status::ready));
  EXPECT_THAT(timer_future.wait_for(std::chrono::seconds(5)),
              Eq(std::future_status::ready));

  unblock_connect_promise.set_value();
  ASSERT_THAT(done.wait_for(std::chrono::seconds(10)),
              Eq(std::future_status::ready));

  cq.CancelAll();
  cq.Shutdown();
  t.join();
}

TEST(DirectPathDiagnosticsTest,
     StartAsyncSkipsRecordWhenWeakCompatibilityExpires) {
  std::promise<void> connect_entered_promise;
  auto connect_entered_future = connect_entered_promise.get_future();
  std::promise<void> unblock_connect_promise;
  auto unblock_connect_future = unblock_connect_promise.get_future().share();

  auto mock_detector = std::make_shared<MockGcpDetector>();
  EXPECT_CALL(*mock_detector, IsGoogleCloudBios)
      .WillOnce(testing::Return(true));

  auto mock_network = std::make_shared<MockDirectPathNetworkSystem>();
  EXPECT_CALL(*mock_network, CanConnectTcp)
      .WillOnce(
          [&connect_entered_promise, unblock_connect_future](
              std::string const&, std::uint16_t, std::chrono::milliseconds) {
            connect_entered_promise.set_value();
            unblock_connect_future.wait();
            return false;
          });

#if OPENTELEMETRY_ABI_VERSION_NO >= 2
  auto mock_gauge = std::make_unique<MockGauge<std::int64_t>>();
  auto* mock_gauge_ptr = mock_gauge.get();
  EXPECT_CALL(*mock_gauge_ptr, Record).Times(0);

  auto mock_meter = std::make_shared<MockMeter>();
  EXPECT_CALL(*mock_meter, CreateInt64Gauge)
      .WillOnce([mock = std::move(mock_gauge)](
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view,
                    opentelemetry::nostd::string_view) mutable {
        return std::move(mock);
      });

  auto mock_provider = std::make_shared<MockMeterProvider>();
  EXPECT_CALL(*mock_provider, GetMeter)
      .WillOnce([&mock_meter](opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::common::KeyValueIterable const*) {
        return mock_meter;
      });
#else
  auto mock_observable_gauge = std::make_shared<MockObservableInstrument>();
  EXPECT_CALL(*mock_observable_gauge, AddCallback).Times(AnyNumber());
  EXPECT_CALL(*mock_observable_gauge, RemoveCallback).Times(AnyNumber());

  auto mock_meter = std::make_shared<MockMeter>();
  EXPECT_CALL(*mock_meter, CreateInt64ObservableGauge)
      .WillOnce(
          [mock = mock_observable_gauge](
              opentelemetry::nostd::string_view,
              opentelemetry::nostd::string_view,
              opentelemetry::nostd::string_view) mutable { return mock; });

  auto mock_provider = std::make_shared<MockMeterProvider>();
  EXPECT_CALL(*mock_provider, GetMeter)
      .WillOnce([&mock_meter](opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view,
                              opentelemetry::nostd::string_view) {
        return mock_meter;
      });
#endif

  auto direct_access = std::make_shared<DirectAccessCompatibility>(
      "test-instrumentation-scope", mock_provider, ClientResourceLabels{});

  auto task = DirectPathDiagnostics::StartAsync(
      Options{}, std::weak_ptr<DirectAccessCompatibility>(direct_access),
      mock_detector, mock_network, "127.0.0.1", static_cast<std::uint16_t>(80));

  ASSERT_THAT(connect_entered_future.wait_for(std::chrono::seconds(10)),
              Eq(std::future_status::ready));

  // Simulate DataConnection destruction releasing DirectAccessCompatibility
  // while diagnostics is still in flight.
  direct_access.reset();
  unblock_connect_promise.set_value();
  task->Wait();
}

TEST(DirectPathDiagnosticsTest, StartAsyncCancelAndJoinStopsPromptly) {
  auto mock_detector = std::make_shared<MockGcpDetector>();
  EXPECT_CALL(*mock_detector, IsGoogleCloudBios)
      .Times(AnyNumber())
      .WillRepeatedly(testing::Return(true));

  Options const options =
      Options{}.set<bigtable::experimental::DirectPathDiagnosticsTimeoutOption>(
          std::chrono::seconds(30));

  // Pass network_system=nullptr so StartAsync wires CancelState into the real
  // DefaultDirectPathNetworkSystem and non-blocking poll() wakeup pipe.
  auto task = DirectPathDiagnostics::StartAsync(
      options, std::weak_ptr<DirectAccessCompatibility>{}, mock_detector,
      nullptr, "192.0.2.1", static_cast<std::uint16_t>(80));

  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  auto const cancel_start = std::chrono::steady_clock::now();
  task->CancelAndJoin();
  auto const cancel_elapsed = std::chrono::steady_clock::now() - cancel_start;
  EXPECT_LT(cancel_elapsed, std::chrono::milliseconds(1500));
}

#endif  // GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
#endif  // !defined(_WIN32) && !defined(__MACH__)

}  // namespace
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
