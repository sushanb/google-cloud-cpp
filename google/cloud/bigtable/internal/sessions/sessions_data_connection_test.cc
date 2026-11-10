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

#include "google/cloud/bigtable/internal/sessions/sessions_data_connection.h"
#include "google/cloud/bigtable/internal/defaults.h"
#include "google/cloud/bigtable/internal/sessions/pool/session_pool_impl_test_helpers.h"
#include "google/cloud/bigtable/internal/sessions/session/write_status.h"
#include "google/cloud/bigtable/mocks/mock_data_connection.h"
#include "google/cloud/bigtable/options.h"
#include "google/cloud/credentials.h"
#include "google/cloud/internal/make_status.h"
#include <google/bigtable/v2/data.pb.h>
#include <google/bigtable/v2/session.pb.h>
#include <gmock/gmock.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace sessions {
namespace {

using ::google::bigtable::v2::OpenTableRequest;
using ::google::bigtable::v2::TableRequest;
using ::google::bigtable::v2::TableResponse;
using ::google::bigtable::v2::VirtualRpcRequest;
using ::google::bigtable::v2::VirtualRpcResponse;
using ::testing::_;
using ::testing::Return;

constexpr char kTableName[] = "projects/p/instances/i/tables/t1";

class FakeResponderPool : public SessionPoolImpl {
 public:
  using HandlerFn = std::function<google::cloud::StatusOr<VirtualRpcResponse>(
      VirtualRpcRequest const&, bool)>;

  explicit FakeResponderPool(HandlerFn handler)
      : handler_(std::move(handler)) {}

  google::cloud::Status StartVirtualRpc(grpc::ClientContext* /*context*/,
                                        VirtualRpcRequest request,
                                        GenericCallback callback,
                                        bool is_write_request) override {
    ++calls_;
    auto resp = handler_(request, is_write_request);
    auto const status = resp.status();
    if (callback) {
      std::move(callback)(std::move(resp));
    }
    return status;
  }

  void Close() override { closed_.store(true, std::memory_order_release); }

  std::atomic<int> calls_{0};
  std::atomic<bool> closed_{false};

 private:
  HandlerFn handler_;
};

Options MakeTestOptions() {
  return google::cloud::bigtable::internal::DefaultDataOptions(
      Options{}
          .set<UnifiedCredentialsOption>(MakeInsecureCredentials())
          .set<google::cloud::bigtable::experimental::EnableSessionsOption>(
              true));
}

TEST(SessionsDataConnectionTest,
     ReadRowAndAsyncReadRowTranslateCellsAndAbsentRows) {
  auto classic = std::make_shared<bigtable_mocks::MockDataConnection>();
  EXPECT_CALL(*classic, options).WillRepeatedly(Return(MakeTestOptions()));
  EXPECT_CALL(*classic, ReadRow).Times(0);
  EXPECT_CALL(*classic, AsyncReadRow).Times(0);

  std::shared_ptr<FakeResponderPool> read_pool;
  auto factory = [&](std::string_view /*table*/, std::string_view /*profile*/,
                     OpenTableRequest::Permission perm, Options /*opts*/,
                     Client::ScheduleFn scheduler) {
    EXPECT_EQ(perm, OpenTableRequest::PERMISSION_READ);
    read_pool = std::make_shared<FakeResponderPool>(
        [](VirtualRpcRequest const& vreq,
           bool is_write) -> google::cloud::StatusOr<VirtualRpcResponse> {
          EXPECT_FALSE(is_write);
          TableRequest treq;
          EXPECT_TRUE(treq.ParseFromString(vreq.payload()));
          TableResponse tresp;
          if (treq.read_row().key() == "found") {
            auto* r = tresp.mutable_read_row()->mutable_row();
            r->set_key("found");
            auto* fam = r->add_families();
            fam->set_name("cf");
            auto* col = fam->add_columns();
            col->set_qualifier("q1");
            auto* cell = col->add_cells();
            cell->set_timestamp_micros(123456);
            cell->set_value("val1");
          } else {
            tresp.mutable_read_row();
          }
          VirtualRpcResponse vresp;
          vresp.set_rpc_id(vreq.rpc_id());
          EXPECT_TRUE(tresp.SerializeToString(vresp.mutable_payload()));
          return vresp;
        });
    return Client::CreateWithPool(read_pool, std::move(scheduler));
  };

  auto conn = SessionsDataConnection::CreateForTesting(
      classic, nullptr, {}, MakeTestOptions(), factory);
  google::cloud::internal::OptionsSpan span(conn->options());

  auto hit =
      conn->ReadRow(kTableName, "found", bigtable::Filter::PassAllFilter());
  ASSERT_TRUE(hit.ok()) << hit.status();
  ASSERT_TRUE(hit->first);
  EXPECT_EQ(hit->second.row_key(), "found");
  ASSERT_EQ(hit->second.cells().size(), 1u);
  EXPECT_EQ(hit->second.cells()[0].family_name(), "cf");
  EXPECT_EQ(hit->second.cells()[0].column_qualifier(), "q1");
  EXPECT_EQ(hit->second.cells()[0].value(), "val1");

  auto miss = conn->AsyncReadRow(kTableName, "missing",
                                 bigtable::Filter::PassAllFilter())
                  .get();
  ASSERT_TRUE(miss.ok()) << miss.status();
  EXPECT_FALSE(miss->first);
  EXPECT_EQ(conn->directory_size(), 1u);
}

TEST(SessionsDataConnectionTest,
     SeparateReadAndWritePoolsAndPerScopeDiversionRouting) {
  auto classic = std::make_shared<bigtable_mocks::MockDataConnection>();
  EXPECT_CALL(*classic, options).WillRepeatedly(Return(MakeTestOptions()));
  EXPECT_CALL(*classic, Apply(kTableName, _)).WillOnce(Return(Status{}));

  int read_pools_created = 0;
  int write_pools_created = 0;
  auto factory = [&](std::string_view /*table*/, std::string_view /*profile*/,
                     OpenTableRequest::Permission perm, Options /*opts*/,
                     Client::ScheduleFn scheduler) {
    if (perm == OpenTableRequest::PERMISSION_READ) {
      ++read_pools_created;
    } else {
      ++write_pools_created;
    }
    auto pool = std::make_shared<FakeResponderPool>(
        [](VirtualRpcRequest const& vreq,
           bool is_write) -> google::cloud::StatusOr<VirtualRpcResponse> {
          TableResponse tresp;
          if (is_write) {
            tresp.mutable_mutate_row();
          } else {
            tresp.mutable_read_row();
          }
          VirtualRpcResponse vresp;
          vresp.set_rpc_id(vreq.rpc_id());
          EXPECT_TRUE(tresp.SerializeToString(vresp.mutable_payload()));
          return vresp;
        });
    return Client::CreateWithPool(pool, std::move(scheduler));
  };

  auto conn = SessionsDataConnection::CreateForTesting(
      classic, nullptr, {}, MakeTestOptions(), factory);
  // Route 100% of ReadRow to Sessions and 0% of MutateRow (Apply) to Sessions.
  conn->SetDiversionLoadsForTesting({1.0, 0.0});
  google::cloud::internal::OptionsSpan span(conn->options());

  auto r = conn->ReadRow(kTableName, "rk", bigtable::Filter::PassAllFilter());
  EXPECT_TRUE(r.ok());
  EXPECT_EQ(read_pools_created, 1);
  EXPECT_EQ(write_pools_created, 0);

  bigtable::SingleRowMutation mut(
      "rk", bigtable::SetCell("cf", "q", std::chrono::milliseconds(10), "v"));
  EXPECT_TRUE(conn->Apply(kTableName, std::move(mut)).ok());
  // Write pool was never even allocated because mutate_row diversion load is 0!
  EXPECT_EQ(write_pools_created, 0);

  // Flip write diversion to 1.0 -> opens separate write pool.
  conn->SetDiversionLoadsForTesting({1.0, 1.0});
  bigtable::SingleRowMutation mut2(
      "rk", bigtable::SetCell("cf", "q", std::chrono::milliseconds(10), "v2"));
  EXPECT_TRUE(conn->AsyncApply(kTableName, std::move(mut2)).get().ok());
  EXPECT_EQ(write_pools_created, 1);
  EXPECT_EQ(conn->directory_size(), 2u);
}

TEST(SessionsDataConnectionTest,
     UnimplementedTripsStickyBreakerAndHonorsNonIdempotentFallbackGate) {
  // Case 1: Non-idempotent write that MIGHT have left client -> trips breaker
  // for future RPCs, but MUST NOT re-execute this mutation on classic!
  {
    auto classic = std::make_shared<bigtable_mocks::MockDataConnection>();
    EXPECT_CALL(*classic, options).WillRepeatedly(Return(MakeTestOptions()));
    EXPECT_CALL(*classic, Apply).Times(0);

    auto factory = [&](std::string_view, std::string_view,
                       OpenTableRequest::Permission, Options,
                       Client::ScheduleFn scheduler) {
      auto pool = std::make_shared<FakeResponderPool>(
          [](VirtualRpcRequest const&,
             bool) -> google::cloud::StatusOr<VirtualRpcResponse> {
            return google::cloud::internal::UnimplementedError("no sessions");
          });
      return Client::CreateWithPool(pool, std::move(scheduler));
    };

    auto conn = SessionsDataConnection::CreateForTesting(
        classic, nullptr, {}, MakeTestOptions(), factory);
    google::cloud::internal::OptionsSpan span(conn->options());

    // ServerSetTimestamp (-1) is non-idempotent!
    bigtable::SingleRowMutation non_idem(
        "rk", bigtable::SetCell("cf", "q", "server-ts-val"));
    auto s = conn->Apply(kTableName, std::move(non_idem));
    EXPECT_EQ(s.code(), StatusCode::kUnimplemented);
    EXPECT_TRUE(conn->mutate_row_diverter().unimplemented_tripped());
    EXPECT_TRUE(conn->read_row_diverter().unimplemented_tripped());
  }

  // Case 2: Non-idempotent write tagged NeverLeftClient ("0") -> safe to fall
  // back to classic_connection_!
  {
    auto classic = std::make_shared<bigtable_mocks::MockDataConnection>();
    EXPECT_CALL(*classic, options).WillRepeatedly(Return(MakeTestOptions()));
    EXPECT_CALL(*classic, Apply(kTableName, _)).WillOnce(Return(Status{}));

    auto factory = [&](std::string_view, std::string_view,
                       OpenTableRequest::Permission, Options,
                       Client::ScheduleFn scheduler) {
      auto pool = std::make_shared<FakeResponderPool>(
          [](VirtualRpcRequest const&,
             bool) -> google::cloud::StatusOr<VirtualRpcResponse> {
            auto st =
                google::cloud::internal::UnimplementedError("handshake unimp");
            MarkPoolRejected(st);
            return st;
          });
      return Client::CreateWithPool(pool, std::move(scheduler));
    };

    auto conn = SessionsDataConnection::CreateForTesting(
        classic, nullptr, {}, MakeTestOptions(), factory);
    google::cloud::internal::OptionsSpan span(conn->options());

    bigtable::SingleRowMutation non_idem(
        "rk", bigtable::SetCell("cf", "q", "server-ts-val"));
    EXPECT_TRUE(conn->Apply(kTableName, std::move(non_idem)).ok());
    EXPECT_TRUE(conn->mutate_row_diverter().unimplemented_tripped());
  }
}

TEST(SessionsDataConnectionTest,
     PerCallCredentialsFailedOpenAndDirectoryCapFallbackToClassic) {
  auto classic = std::make_shared<bigtable_mocks::MockDataConnection>();
  EXPECT_CALL(*classic, options).WillRepeatedly(Return(MakeTestOptions()));
  EXPECT_CALL(*classic, ReadRow)
      .Times(3)
      .WillRepeatedly(Return(std::make_pair(false, bigtable::Row("", {}))));

  int factory_calls = 0;
  auto factory = [&](std::string_view, std::string_view,
                     OpenTableRequest::Permission, Options,
                     Client::ScheduleFn scheduler) -> std::shared_ptr<Client> {
    if (++factory_calls == 1) {
      // Failed open returns nullptr and MUST NOT be cached.
      return nullptr;
    }
    auto pool = std::make_shared<FakeResponderPool>(
        [](VirtualRpcRequest const& vreq,
           bool) -> google::cloud::StatusOr<VirtualRpcResponse> {
          TableResponse tresp;
          tresp.mutable_read_row();
          VirtualRpcResponse vresp;
          vresp.set_rpc_id(vreq.rpc_id());
          EXPECT_TRUE(tresp.SerializeToString(vresp.mutable_payload()));
          return vresp;
        });
    return Client::CreateWithPool(pool, std::move(scheduler));
  };

  auto conn = SessionsDataConnection::CreateForTesting(
      classic, nullptr, {}, MakeTestOptions(), factory,
      /*max_directory_entries=*/1);

  // 1. Per-call UnifiedCredentialsOption override -> bypasses sessions.
  {
    auto per_call = conn->options().set<UnifiedCredentialsOption>(
        MakeInsecureCredentials());
    google::cloud::internal::OptionsSpan span(per_call);
    EXPECT_TRUE(
        conn->ReadRow(kTableName, "r1", bigtable::Filter::PassAllFilter())
            .ok());
    EXPECT_EQ(factory_calls, 0);
  }

  google::cloud::internal::OptionsSpan span(conn->options());
  // 2. First open fails -> falls back to classic and is NOT cached.
  EXPECT_TRUE(
      conn->ReadRow(kTableName, "r2", bigtable::Filter::PassAllFilter()).ok());
  EXPECT_EQ(conn->directory_size(), 0u);

  // 3. Second call retries open -> succeeds and caches 1 entry (reaching
  // cap=1).
  EXPECT_TRUE(
      conn->ReadRow(kTableName, "r3", bigtable::Filter::PassAllFilter()).ok());
  EXPECT_EQ(conn->directory_size(), 1u);

  // 4. Distinct second table exceeds max_directory_entries=1 -> falls back to
  // classic.
  EXPECT_TRUE(conn->ReadRow("projects/p/instances/i/tables/t2", "r4",
                            bigtable::Filter::PassAllFilter())
                  .ok());
  EXPECT_EQ(conn->directory_size(), 1u);
}

}  // namespace
}  // namespace sessions
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
