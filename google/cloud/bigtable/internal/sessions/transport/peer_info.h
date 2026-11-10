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

#ifndef GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TRANSPORT_PEER_INFO_H
#define GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TRANSPORT_PEER_INFO_H

#include "google/cloud/version.h"
#include "absl/strings/escaping.h"
#include "absl/strings/numbers.h"
#include <google/bigtable/v2/peer_info.pb.h>
#include <cstdint>
#include <map>
#include <string>
#include <utility>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace transport {

/// Parsed representation of the `bigtable-peer-info` initial metadata header.
///
/// Resolved synchronously before `OnActive` announces the session to the pool.
struct PeerInfo {
  enum class TransportType {
    kUnknown = 0,
    kExternal = 1,
    kCloudPath = 2,
    kDirectAccess = 3,
    kSessionUnknown = 4,
    kSessionExternal = 5,
    kSessionCloudPath = 6,
    kSessionDirectAccess = 7,
  };

  std::int64_t google_frontend_id = 0;
  std::int64_t application_frontend_id =
      0;  // Backend peer ID (0 indicates the default unknown bucket).
  std::string application_frontend_region;
  std::string application_frontend_zone;
  std::string application_frontend_subzone;
  TransportType transport_type = TransportType::kUnknown;
  bool present_in_headers = false;

  std::int64_t peer_id() const { return application_frontend_id; }

  bool IsDirectAccess() const {
    return transport_type == TransportType::kSessionDirectAccess ||
           transport_type == TransportType::kDirectAccess;
  }

  static TransportType MapProtoTransportType(
      google::bigtable::v2::PeerInfo::TransportType proto_type) {
    switch (proto_type) {
      case google::bigtable::v2::PeerInfo::TRANSPORT_TYPE_EXTERNAL:
        return TransportType::kExternal;
      case google::bigtable::v2::PeerInfo::TRANSPORT_TYPE_CLOUD_PATH:
        return TransportType::kCloudPath;
      case google::bigtable::v2::PeerInfo::TRANSPORT_TYPE_DIRECT_ACCESS:
        return TransportType::kDirectAccess;
      case google::bigtable::v2::PeerInfo::TRANSPORT_TYPE_SESSION_UNKNOWN:
        return TransportType::kSessionUnknown;
      case google::bigtable::v2::PeerInfo::TRANSPORT_TYPE_SESSION_EXTERNAL:
        return TransportType::kSessionExternal;
      case google::bigtable::v2::PeerInfo::TRANSPORT_TYPE_SESSION_CLOUD_PATH:
        return TransportType::kSessionCloudPath;
      case google::bigtable::v2::PeerInfo::TRANSPORT_TYPE_SESSION_DIRECT_ACCESS:
        return TransportType::kSessionDirectAccess;
      case google::bigtable::v2::PeerInfo::TRANSPORT_TYPE_UNKNOWN:
      default:
        return TransportType::kUnknown;
    }
  }

  static std::string TransportTypeToString(TransportType type) {
    switch (type) {
      case TransportType::kExternal:
        return "transport_type_external";
      case TransportType::kCloudPath:
        return "transport_type_cloud_path";
      case TransportType::kDirectAccess:
        return "transport_type_direct_access";
      case TransportType::kSessionUnknown:
        return "transport_type_session_unknown";
      case TransportType::kSessionExternal:
        return "transport_type_session_external";
      case TransportType::kSessionCloudPath:
        return "transport_type_session_cloud_path";
      case TransportType::kSessionDirectAccess:
        return "transport_type_session_direct_access";
      case TransportType::kUnknown:
      default:
        return "transport_type_unknown";
    }
  }

  /// Parses `PeerInfo` from initial metadata headers (`bigtable-peer-info` or
  /// fallback `OpenSessionResponse.backend` fields).
  static PeerInfo FromHeaders(
      std::multimap<std::string, std::string> const& headers) {
    return FromHeaders(headers, 0, "");
  }

  static PeerInfo FromHeaders(
      std::multimap<std::string, std::string> const& headers,
      std::int64_t fallback_peer_id, std::string fallback_zone) {
    PeerInfo info;
    info.application_frontend_id = fallback_peer_id;
    info.application_frontend_subzone = std::move(fallback_zone);

    auto apply_proto = [&](google::bigtable::v2::PeerInfo const& proto_info) {
      info.present_in_headers = true;
      info.google_frontend_id = proto_info.google_frontend_id();
      info.application_frontend_id = proto_info.application_frontend_id();
      if (!proto_info.application_frontend_region().empty()) {
        info.application_frontend_region =
            proto_info.application_frontend_region();
      }
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
      if (!proto_info.application_frontend_zone().empty()) {
        info.application_frontend_zone = proto_info.application_frontend_zone();
        if (info.application_frontend_subzone.empty()) {
          info.application_frontend_subzone =
              proto_info.application_frontend_zone();
        }
      }
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
      if (!proto_info.application_frontend_subzone().empty()) {
        info.application_frontend_subzone =
            proto_info.application_frontend_subzone();
      }
      if (proto_info.transport_type() !=
          google::bigtable::v2::PeerInfo::TRANSPORT_TYPE_UNKNOWN) {
        info.transport_type =
            MapProtoTransportType(proto_info.transport_type());
      }
    };

    auto b64_it = headers.find("bigtable-peer-info");
    if (b64_it != headers.end() && !b64_it->second.empty()) {
      std::string decoded;
      if (absl::WebSafeBase64Unescape(b64_it->second, &decoded) ||
          absl::Base64Unescape(b64_it->second, &decoded)) {
        google::bigtable::v2::PeerInfo proto_info;
        if (proto_info.ParseFromString(decoded)) {
          apply_proto(proto_info);
        }
      }
    } else {
      auto bin_it = headers.find("bigtable-peer-info-bin");
      if (bin_it != headers.end() && !bin_it->second.empty()) {
        google::bigtable::v2::PeerInfo proto_info;
        if (proto_info.ParseFromString(bin_it->second)) {
          apply_proto(proto_info);
        }
      }
    }

    auto gfe_it = headers.find("bigtable-peer-info-gfe-id");
    if (gfe_it != headers.end()) {
      std::int64_t parsed = 0;
      if (absl::SimpleAtoi(gfe_it->second, &parsed)) {
        info.google_frontend_id = parsed;
        info.present_in_headers = true;
      }
    }
    auto it = headers.find("bigtable-peer-info-afe-id");
    if (it != headers.end()) {
      std::int64_t parsed = 0;
      if (absl::SimpleAtoi(it->second, &parsed)) {
        info.application_frontend_id = parsed;
        info.present_in_headers = true;
      }
    }
    auto zone_it = headers.find("bigtable-peer-info-zone");
    if (zone_it != headers.end()) {
      info.application_frontend_zone = zone_it->second;
      if (info.application_frontend_subzone.empty()) {
        info.application_frontend_subzone = zone_it->second;
      }
      info.present_in_headers = true;
    }
    auto subzone_it = headers.find("bigtable-peer-info-subzone");
    if (subzone_it != headers.end()) {
      info.application_frontend_subzone = subzone_it->second;
      info.present_in_headers = true;
    }
    auto region_it = headers.find("bigtable-peer-info-region");
    if (region_it != headers.end()) {
      info.application_frontend_region = region_it->second;
      info.present_in_headers = true;
    }
    auto transport_it = headers.find("bigtable-peer-info-transport");
    if (transport_it != headers.end()) {
      info.present_in_headers = true;
      if (transport_it->second == "direct_access") {
        info.transport_type = TransportType::kSessionDirectAccess;
      } else if (transport_it->second == "cloud_path") {
        info.transport_type = TransportType::kSessionCloudPath;
      } else if (transport_it->second == "external") {
        info.transport_type = TransportType::kSessionExternal;
      }
    }
    return info;
  }
};

}  // namespace transport
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google

#endif  // GOOGLE_CLOUD_CPP_GOOGLE_CLOUD_BIGTABLE_INTERNAL_SESSIONS_TRANSPORT_PEER_INFO_H
