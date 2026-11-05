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

#include "google/cloud/bigtable/internal/directpath_prober.h"
#include "google/cloud/bigtable/instance_resource.h"
#include "google/cloud/bigtable/internal/bigtable_stub.h"
#include "google/cloud/bigtable/internal/bigtable_stub_factory.h"
#include "google/cloud/bigtable/internal/data_connection_impl.h"
#include "google/cloud/bigtable/internal/defaults.h"
#include "google/cloud/bigtable/internal/endpoint_options.h"
#include "google/cloud/bigtable/internal/operation_context.h"
#include "google/cloud/bigtable/options.h"
#include "google/cloud/common_options.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/internal/make_status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "google/bigtable/v2/bigtable.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace {

IpPreference ExtractIpPreference(std::string_view peer_address) {
  if (absl::StartsWith(peer_address, "ipv4:")) {
    return IpPreference::kIpv4;
  }
  if (absl::StartsWith(peer_address, "ipv6:")) {
    return IpPreference::kIpv6;
  }
  return IpPreference::kNone;
}

bool IsAltsNegotiated(grpc::AuthContext const& auth_ctx) {
  if (!auth_ctx.IsPeerAuthenticated()) return false;
  std::vector<grpc::string_ref> const sec_types =
      auth_ctx.FindPropertyValues("transport_security_type");
  return std::any_of(sec_types.begin(), sec_types.end(),
                     [](grpc::string_ref const& st) { return st == "alts"; });
}

bool IsAltsNegotiated(grpc::ClientContext const& context) {
  std::shared_ptr<grpc::AuthContext const> const auth_ctx =
      context.auth_context();
  if (auth_ctx == nullptr) return false;
  return IsAltsNegotiated(*auth_ctx);
}

bool IsPeerAuthenticated(grpc::ClientContext const& context) {
  std::shared_ptr<grpc::AuthContext const> const auth_ctx =
      context.auth_context();
  if (auth_ctx == nullptr) return false;
  return auth_ctx->IsPeerAuthenticated();
}

bool ParsePort(std::string_view port_str) {
  if (port_str.empty() || port_str.size() > 5) return false;
  std::uint32_t port = 0;
  for (char const c : port_str) {
    if (c < '0' || c > '9') return false;
    port = port * 10 + static_cast<std::uint32_t>(c - '0');
  }
  return port <= 65535U;
}

bool ParseIpv4(std::string_view ip_str, std::array<std::uint8_t, 4>& out) {
  if (ip_str.empty()) return false;
  std::vector<std::string_view> const octets = absl::StrSplit(ip_str, '.');
  if (octets.size() != 4) return false;
  for (std::size_t i = 0; i < 4; ++i) {
    std::string_view const octet = octets[i];
    if (octet.empty() || octet.size() > 3) return false;
    if (octet.size() > 1 && octet[0] == '0') return false;
    std::uint32_t val = 0;
    for (char const c : octet) {
      if (c < '0' || c > '9') return false;
      val = val * 10 + static_cast<std::uint32_t>(c - '0');
    }
    if (val > 255U) return false;
    out[i] = static_cast<std::uint8_t>(val);
  }
  return true;
}

bool ParseIpv6Hextets(std::string_view part, bool allow_ipv4_tail,
                      std::vector<std::uint16_t>& hextets) {
  if (part.empty()) return true;
  std::vector<std::string_view> const groups = absl::StrSplit(part, ':');
  for (std::size_t i = 0; i < groups.size(); ++i) {
    std::string_view const group = groups[i];
    if (group.empty()) return false;
    if (allow_ipv4_tail && i + 1 == groups.size() &&
        group.find('.') != std::string_view::npos) {
      std::array<std::uint8_t, 4> v4{};
      if (!ParseIpv4(group, v4)) return false;
      hextets.push_back(static_cast<std::uint16_t>(
          (static_cast<std::uint16_t>(v4[0]) << 8U) | v4[1]));
      hextets.push_back(static_cast<std::uint16_t>(
          (static_cast<std::uint16_t>(v4[2]) << 8U) | v4[3]));
      continue;
    }
    if (group.size() > 4) return false;
    std::uint16_t val = 0;
    for (char const c : group) {
      std::uint16_t digit = 0;
      if (c >= '0' && c <= '9') {
        digit = static_cast<std::uint16_t>(c - '0');
      } else if (c >= 'a' && c <= 'f') {
        digit = static_cast<std::uint16_t>(10 + (c - 'a'));
      } else if (c >= 'A' && c <= 'F') {
        digit = static_cast<std::uint16_t>(10 + (c - 'A'));
      } else {
        return false;
      }
      val = static_cast<std::uint16_t>((val << 4U) | digit);
    }
    hextets.push_back(val);
  }
  return true;
}

bool ParseIpv6(std::string_view ip_str, std::array<std::uint8_t, 16>& out) {
  out.fill(0);
  auto const pct = ip_str.find('%');
  if (pct != std::string_view::npos) {
    if (pct + 1 == ip_str.size()) return false;
    ip_str = ip_str.substr(0, pct);
  }
  if (ip_str.empty()) return false;

  auto const double_colon = ip_str.find("::");
  if (double_colon == std::string_view::npos) {
    std::vector<std::uint16_t> hextets;
    hextets.reserve(8);
    if (!ParseIpv6Hextets(ip_str, /*allow_ipv4_tail=*/true, hextets) ||
        hextets.size() != 8) {
      return false;
    }
    for (std::size_t i = 0; i < 8; ++i) {
      out[2 * i] = static_cast<std::uint8_t>(hextets[i] >> 8U);
      out[2 * i + 1] = static_cast<std::uint8_t>(hextets[i] & 0xFFU);
    }
    return true;
  }

  if (ip_str.find("::", double_colon + 1) != std::string_view::npos) {
    return false;
  }
  std::string_view const head_str = ip_str.substr(0, double_colon);
  std::string_view const tail_str = ip_str.substr(double_colon + 2);
  std::vector<std::uint16_t> head;
  std::vector<std::uint16_t> tail;
  if (!ParseIpv6Hextets(head_str, /*allow_ipv4_tail=*/false, head) ||
      !ParseIpv6Hextets(tail_str, /*allow_ipv4_tail=*/true, tail) ||
      head.size() + tail.size() >= 8) {
    return false;
  }
  for (std::size_t i = 0; i < head.size(); ++i) {
    out[2 * i] = static_cast<std::uint8_t>(head[i] >> 8U);
    out[2 * i + 1] = static_cast<std::uint8_t>(head[i] & 0xFFU);
  }
  std::size_t const tail_offset = 16 - 2 * tail.size();
  for (std::size_t i = 0; i < tail.size(); ++i) {
    out[tail_offset + 2 * i] = static_cast<std::uint8_t>(tail[i] >> 8U);
    out[tail_offset + 2 * i + 1] = static_cast<std::uint8_t>(tail[i] & 0xFFU);
  }
  return true;
}

// DirectPath IPv4 addresses fall within 34.126.0.0/18 (0x227E0000/18).
bool IsDirectPathIpv4Bytes(std::array<std::uint8_t, 4> const& ipv4) {
  std::uint32_t const addr = (static_cast<std::uint32_t>(ipv4[0]) << 24U) |
                             (static_cast<std::uint32_t>(ipv4[1]) << 16U) |
                             (static_cast<std::uint32_t>(ipv4[2]) << 8U) |
                             static_cast<std::uint32_t>(ipv4[3]);
  return (addr & 0xFFFFC000U) == 0x227E0000U;
}

// DirectPath IPv6 addresses fall within 2001:4860:8040::/42, or IPv4-mapped
// IPv6 ::ffff:34.126.0.0/114 (::ffff:0:0/96 wrapping 34.126.0.0/18).
bool IsDirectPathIpv6Bytes(std::array<std::uint8_t, 16> const& ipv6) {
  if (ipv6[0] == 0x20 && ipv6[1] == 0x01 && ipv6[2] == 0x48 &&
      ipv6[3] == 0x60 && ipv6[4] == 0x80 && (ipv6[5] & 0xC0U) == 0x40U) {
    return true;
  }
  bool const is_v4_mapped =
      std::all_of(ipv6.begin(), ipv6.begin() + 10,
                  [](std::uint8_t b) { return b == 0; }) &&
      ipv6[10] == 0xFF && ipv6[11] == 0xFF;
  if (is_v4_mapped) {
    return IsDirectPathIpv4Bytes({ipv6[12], ipv6[13], ipv6[14], ipv6[15]});
  }
  return false;
}

// Returns true if the peer IP address belongs to a DirectPath subnet.
// IPv6 connections over DirectPath use the 2001:4860:8040::/42 CIDR block,
// while IPv4 connections route through the 34.126.0.0/18 CIDR block.
bool IsDirectPathIp(std::string_view peer_address) {
  if (absl::StartsWith(peer_address, "ipv6:")) {
    std::string_view const rest = peer_address.substr(5);
    std::string_view ip_str;
    if (absl::StartsWith(rest, "[")) {
      auto const close = rest.find(']');
      if (close == std::string_view::npos) return false;
      if (close + 1 < rest.size()) {
        if (rest[close + 1] != ':' || !ParsePort(rest.substr(close + 2))) {
          return false;
        }
      }
      ip_str = rest.substr(1, close - 1);
    } else {
      ip_str = rest;
    }
    std::array<std::uint8_t, 16> ipv6{};
    return ParseIpv6(ip_str, ipv6) && IsDirectPathIpv6Bytes(ipv6);
  }
  if (absl::StartsWith(peer_address, "ipv4:")) {
    std::string_view const rest = peer_address.substr(5);
    auto const colon = rest.find(':');
    std::string_view ip_str = rest;
    if (colon != std::string_view::npos) {
      if (!ParsePort(rest.substr(colon + 1))) return false;
      ip_str = rest.substr(0, colon);
    }
    std::array<std::uint8_t, 4> ipv4{};
    return ParseIpv4(ip_str, ipv4) && IsDirectPathIpv4Bytes(ipv4);
  }
  return false;
}

}  // namespace

std::string ToString(IpPreference preference) {
  switch (preference) {
    case IpPreference::kIpv4:
      return "ipv4";
    case IpPreference::kIpv6:
      return "ipv6";
    case IpPreference::kNone:
      return "";
  }
  return "";
}

StatusOr<DirectPathProbeResult> DirectPathProber::Probe(
    std::shared_ptr<internal::GrpcAuthenticationStrategy> const& auth,
    bigtable::InstanceResource const& instance_resource, Options const& options,
    CompletionQueue const& cq) {
  return Probe(auth, instance_resource, options, cq, nullptr, "", nullptr);
}

StatusOr<DirectPathProbeResult> DirectPathProber::Probe(
    std::shared_ptr<internal::GrpcAuthenticationStrategy> const& auth,
    bigtable::InstanceResource const& instance_resource, Options const& options,
    CompletionQueue const& cq, std::shared_ptr<BigtableStub> const& stub,
    std::string const& peer_address,
    std::shared_ptr<grpc::AuthContext const> const& auth_context) {
  std::shared_ptr<BigtableStub> effective_stub = stub;
  if (!effective_stub) {
    if (!auth) {
      return internal::InternalError("Auth strategy cannot be null",
                                     GCP_ERROR_INFO());
    }

    Options probe_options = options;
    probe_options.unset<DirectPathFallbackToCloudPathOption>();
    probe_options.set<::google::cloud::bigtable_internal::DataEndpointOption>(
        bigtable::internal::DefaultDirectPathDataEndpoint());
    probe_options.set<EndpointOption>(
        bigtable::internal::DefaultDirectPathDataEndpoint());
    probe_options.set<AuthorityOption>(
        bigtable::internal::DefaultDirectPathAuthority());
    probe_options.set<bigtable::experimental::DirectPathModeOption>(
        bigtable::experimental::DirectPathMode::kEnabled);
    probe_options.set<GrpcNumChannelsOption>(1);
    probe_options.set<bigtable::MinConnectionRefreshOption>(
        std::chrono::milliseconds::zero());
    probe_options.set<bigtable::MaxConnectionRefreshOption>(
        std::chrono::milliseconds::zero());

    auto stub_factory = [](std::shared_ptr<grpc::Channel> channel) {
      return std::make_shared<DefaultBigtableStub>(
          google::bigtable::v2::Bigtable::NewStub(std::move(channel)));
    };
    effective_stub =
        CreateDecoratedStubs(auth, cq, probe_options, stub_factory);
  }

  std::chrono::milliseconds timeout =
      options.get<bigtable::experimental::DirectPathProbeTimeoutOption>();
  if (timeout <= std::chrono::milliseconds::zero()) {
    timeout = bigtable::internal::DefaultDirectPathProbeTimeout();
  }

  grpc::ClientContext client_context;
  client_context.set_deadline(std::chrono::system_clock::now() + timeout);

  google::bigtable::v2::PingAndWarmRequest request;
  request.set_name(instance_resource.FullName());
  if (options.has<bigtable::AppProfileIdOption>()) {
    request.set_app_profile_id(options.get<bigtable::AppProfileIdOption>());
  }
  OperationContext op_ctx;
  StatusOr<google::bigtable::v2::PingAndWarmResponse> response =
      effective_stub->PingAndWarm(client_context, options, request, op_ctx);
  if (!response.ok()) return response.status();

  DirectPathProbeResult result;
  result.peer_address =
      peer_address.empty() ? client_context.peer() : peer_address;
  bool const is_alts = auth_context != nullptr
                           ? IsAltsNegotiated(*auth_context)
                           : IsAltsNegotiated(client_context);
  bool const is_auth = auth_context != nullptr
                           ? auth_context->IsPeerAuthenticated()
                           : IsPeerAuthenticated(client_context);
  bool const is_dp_ip = IsDirectPathIp(result.peer_address);
  result.success = is_alts || (is_auth && is_dp_ip);
  result.ip_preference = result.success
                             ? ExtractIpPreference(result.peer_address)
                             : IpPreference::kNone;

  return result;
}

GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
