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

#include "google/cloud/bigtable/internal/bigtable_random_two_least_used_decorator.h"
#include "google/cloud/bigtable/internal/dynamic_channel_pool.h"
#include "google/cloud/bigtable/internal/operation_context.h"
#include "google/cloud/bigtable/internal/table_schema_metrics.h"
#include "google/cloud/bigtable/testing/mock_bigtable_stub.h"
#include "google/cloud/internal/make_status.h"
#include "google/cloud/testing_util/fake_completion_queue_impl.h"
#include "google/cloud/testing_util/status_matchers.h"
#include <gmock/gmock.h>

namespace google {
namespace cloud {
namespace bigtable_internal {
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_BEGIN
namespace {

using ::google::cloud::bigtable::testing::MockBigtableStub;
using ::google::cloud::testing_util::FakeCompletionQueueImpl;
using ::google::cloud::testing_util::IsOkAndHolds;
using ::google::cloud::testing_util::StatusIs;
using ::testing::Eq;
using ::testing::MockFunction;

class BigtableRandomTwoLeastUsedTest : public ::testing::Test {
 protected:
  BigtableRandomTwoLeastUsedTest() {
    mock_stub_ = std::make_shared<MockBigtableStub>();

    fake_cq_impl_ = std::make_shared<FakeCompletionQueueImpl>();
    cq_ = CompletionQueue(fake_cq_impl_);
    auto instance_name =
        bigtable::InstanceResource(Project("my-project"), "my-instance")
            .FullName();
    bigtable::experimental::DynamicChannelPoolSizingPolicy sizing_policy;
    auto refresh_state = std::make_shared<ConnectionRefreshState>(
        fake_cq_impl_, std::chrono::milliseconds(1),
        std::chrono::milliseconds(10));

    channel_ = std::make_shared<ChannelUsage<BigtableStub>>(mock_stub_, 0);
    std::vector<std::shared_ptr<ChannelUsage<BigtableStub>>> channels{channel_};

    pool_ = DynamicChannelPool<BigtableStub>::Create(
        instance_name, cq_, channels, refresh_state,
        stub_factory_fn_.AsStdFunction(), sizing_policy,
        TransportType::kCloudPath);
  }

  ~BigtableRandomTwoLeastUsedTest() override {
    fake_cq_impl_->SimulateCompletion(false);
  }

  std::shared_ptr<MockBigtableStub> mock_stub_;
  std::shared_ptr<ChannelUsage<BigtableStub>> channel_;
  std::shared_ptr<DynamicChannelPool<BigtableStub>> pool_;
  MockFunction<StatusOr<std::shared_ptr<ChannelUsage<BigtableStub>>>(
      std::uint32_t, std::string const&, StubManager::Priming)>
      stub_factory_fn_;
  CompletionQueue cq_;
  std::shared_ptr<FakeCompletionQueueImpl> fake_cq_impl_;
  std::shared_ptr<OperationContext> op_ctx_ =
      std::make_shared<OperationContext>();
};

TEST_F(BigtableRandomTwoLeastUsedTest, ReadRows) {
  EXPECT_CALL(*mock_stub_, ReadRows).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  auto client_context = std::make_shared<grpc::ClientContext>();
  google::bigtable::v2::ReadRowsRequest request;
  auto response = decorator->ReadRows(client_context, {}, request, op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, SampleRowKeys) {
  EXPECT_CALL(*mock_stub_, SampleRowKeys).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  auto client_context = std::make_shared<grpc::ClientContext>();
  google::bigtable::v2::SampleRowKeysRequest request;
  auto response =
      decorator->SampleRowKeys(client_context, {}, request, op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, MutateRow) {
  EXPECT_CALL(*mock_stub_, MutateRow).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  grpc::ClientContext client_context;
  google::bigtable::v2::MutateRowRequest request;
  auto response = decorator->MutateRow(client_context, {}, request, *op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, MutateRows) {
  EXPECT_CALL(*mock_stub_, MutateRows).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  auto client_context = std::make_shared<grpc::ClientContext>();
  google::bigtable::v2::MutateRowsRequest request;
  auto response = decorator->MutateRows(client_context, {}, request, op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, CheckAndMutateRow) {
  EXPECT_CALL(*mock_stub_, CheckAndMutateRow).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  grpc::ClientContext client_context;
  google::bigtable::v2::CheckAndMutateRowRequest request;
  auto response =
      decorator->CheckAndMutateRow(client_context, {}, request, *op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, PingAndWarm) {
  EXPECT_CALL(*mock_stub_, PingAndWarm).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  grpc::ClientContext client_context;
  google::bigtable::v2::PingAndWarmRequest request;
  auto response = decorator->PingAndWarm(client_context, {}, request, *op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, ReadModifyWriteRow) {
  EXPECT_CALL(*mock_stub_, ReadModifyWriteRow).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  grpc::ClientContext client_context;
  google::bigtable::v2::ReadModifyWriteRowRequest request;
  auto response =
      decorator->ReadModifyWriteRow(client_context, {}, request, *op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, PrepareQuery) {
  EXPECT_CALL(*mock_stub_, PrepareQuery).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  grpc::ClientContext client_context;
  google::bigtable::v2::PrepareQueryRequest request;
  auto response =
      decorator->PrepareQuery(client_context, {}, request, *op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, ExecuteQuery) {
  EXPECT_CALL(*mock_stub_, ExecuteQuery).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  auto client_context = std::make_shared<grpc::ClientContext>();
  google::bigtable::v2::ExecuteQueryRequest request;
  auto response = decorator->ExecuteQuery(client_context, {}, request, op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, AsyncReadRows) {
  EXPECT_CALL(*mock_stub_, AsyncReadRows).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  auto client_context = std::make_shared<grpc::ClientContext>();
  google::bigtable::v2::ReadRowsRequest request;
  auto response =
      decorator->AsyncReadRows(cq_, client_context, {}, request, op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, AsyncSampleRowKeys) {
  EXPECT_CALL(*mock_stub_, AsyncSampleRowKeys).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  auto client_context = std::make_shared<grpc::ClientContext>();
  google::bigtable::v2::SampleRowKeysRequest request;
  auto response =
      decorator->AsyncSampleRowKeys(cq_, client_context, {}, request, op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, AsyncMutateRow) {
  EXPECT_CALL(*mock_stub_, AsyncMutateRow).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  auto client_context = std::make_shared<grpc::ClientContext>();
  google::bigtable::v2::MutateRowRequest request;
  auto response =
      decorator->AsyncMutateRow(cq_, client_context, {}, request, op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, AsyncMutateRows) {
  EXPECT_CALL(*mock_stub_, AsyncMutateRows).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  auto client_context = std::make_shared<grpc::ClientContext>();
  google::bigtable::v2::MutateRowsRequest request;
  auto response =
      decorator->AsyncMutateRows(cq_, client_context, {}, request, op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, AsyncCheckAndMutateRow) {
  EXPECT_CALL(*mock_stub_, AsyncCheckAndMutateRow).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  auto client_context = std::make_shared<grpc::ClientContext>();
  google::bigtable::v2::CheckAndMutateRowRequest request;
  auto response = decorator->AsyncCheckAndMutateRow(cq_, client_context, {},
                                                    request, op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, AsyncPingAndWarm) {
  EXPECT_CALL(*mock_stub_, AsyncPingAndWarm).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  auto client_context = std::make_shared<grpc::ClientContext>();
  google::bigtable::v2::PingAndWarmRequest request;
  auto response =
      decorator->AsyncPingAndWarm(cq_, client_context, {}, request, op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, AsyncReadModifyWriteRow) {
  EXPECT_CALL(*mock_stub_, AsyncReadModifyWriteRow).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  auto client_context = std::make_shared<grpc::ClientContext>();
  google::bigtable::v2::ReadModifyWriteRowRequest request;
  auto response = decorator->AsyncReadModifyWriteRow(cq_, client_context, {},
                                                     request, op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest, AsyncPrepareQuery) {
  EXPECT_CALL(*mock_stub_, AsyncPrepareQuery).Times(1);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  auto client_context = std::make_shared<grpc::ClientContext>();
  google::bigtable::v2::PrepareQueryRequest request;
  auto response =
      decorator->AsyncPrepareQuery(cq_, client_context, {}, request, op_ctx_);
}

TEST_F(BigtableRandomTwoLeastUsedTest,
       AsyncMutateRowHoldsChannelUntilFutureResolves) {
  promise<StatusOr<google::bigtable::v2::MutateRowResponse>> p1;
  promise<StatusOr<google::bigtable::v2::MutateRowResponse>> p2;
  EXPECT_CALL(*mock_stub_, AsyncMutateRow)
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::MutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p1.get_future(); })
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::MutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p2.get_future(); });

  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(0));

  google::bigtable::v2::MutateRowRequest request;
  auto f1 = decorator->AsyncMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);
  EXPECT_FALSE(f1.is_ready());
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(1));

  auto f2 = decorator->AsyncMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);
  EXPECT_FALSE(f2.is_ready());
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(2));

  p1.set_value(google::bigtable::v2::MutateRowResponse{});
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(1));
  ASSERT_TRUE(f1.is_ready());
  EXPECT_STATUS_OK(f1.get());

  p2.set_value(internal::UnavailableError("transient"));
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(0));
  ASSERT_TRUE(f2.is_ready());
  EXPECT_THAT(f2.get(), StatusIs(StatusCode::kUnavailable));
}

TEST_F(BigtableRandomTwoLeastUsedTest,
       AsyncCheckAndMutateRowHoldsChannelUntilFutureResolves) {
  promise<StatusOr<google::bigtable::v2::CheckAndMutateRowResponse>> p;
  EXPECT_CALL(*mock_stub_, AsyncCheckAndMutateRow)
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::CheckAndMutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p.get_future(); });

  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  google::bigtable::v2::CheckAndMutateRowRequest request;
  auto fut = decorator->AsyncCheckAndMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);
  EXPECT_FALSE(fut.is_ready());
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(1));

  google::bigtable::v2::CheckAndMutateRowResponse expected;
  expected.set_predicate_matched(true);
  p.set_value(expected);
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(0));
  auto result = fut.get();
  ASSERT_STATUS_OK(result);
  EXPECT_TRUE(result->predicate_matched());
}

TEST_F(BigtableRandomTwoLeastUsedTest,
       AsyncPingAndWarmHoldsChannelUntilFutureResolves) {
  promise<StatusOr<google::bigtable::v2::PingAndWarmResponse>> p;
  EXPECT_CALL(*mock_stub_, AsyncPingAndWarm)
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::PingAndWarmRequest const&,
              std::shared_ptr<OperationContext>) { return p.get_future(); });

  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  google::bigtable::v2::PingAndWarmRequest request;
  auto fut = decorator->AsyncPingAndWarm(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);
  EXPECT_FALSE(fut.is_ready());
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(1));

  p.set_value(google::bigtable::v2::PingAndWarmResponse{});
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(0));
  EXPECT_STATUS_OK(fut.get());
}

TEST_F(BigtableRandomTwoLeastUsedTest,
       AsyncReadModifyWriteRowHoldsChannelUntilFutureResolves) {
  promise<StatusOr<google::bigtable::v2::ReadModifyWriteRowResponse>> p;
  EXPECT_CALL(*mock_stub_, AsyncReadModifyWriteRow)
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::ReadModifyWriteRowRequest const&,
              std::shared_ptr<OperationContext>) { return p.get_future(); });

  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  google::bigtable::v2::ReadModifyWriteRowRequest request;
  auto fut = decorator->AsyncReadModifyWriteRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);
  EXPECT_FALSE(fut.is_ready());
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(1));

  p.set_value(google::bigtable::v2::ReadModifyWriteRowResponse{});
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(0));
  EXPECT_STATUS_OK(fut.get());
}

TEST_F(BigtableRandomTwoLeastUsedTest,
       AsyncPrepareQueryHoldsChannelUntilFutureResolves) {
  promise<StatusOr<google::bigtable::v2::PrepareQueryResponse>> p;
  EXPECT_CALL(*mock_stub_, AsyncPrepareQuery)
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::PrepareQueryRequest const&,
              std::shared_ptr<OperationContext>) { return p.get_future(); });

  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  google::bigtable::v2::PrepareQueryRequest request;
  auto fut = decorator->AsyncPrepareQuery(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);
  EXPECT_FALSE(fut.is_ready());
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(1));

  p.set_value(google::bigtable::v2::PrepareQueryResponse{});
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(0));
  EXPECT_STATUS_OK(fut.get());
}

TEST_F(BigtableRandomTwoLeastUsedTest, AsyncHelperPreservesFutureCancellation) {
  bool cancelled = false;
  promise<StatusOr<google::bigtable::v2::MutateRowResponse>> p(
      [&cancelled] { cancelled = true; });
  EXPECT_CALL(*mock_stub_, AsyncMutateRow)
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::MutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p.get_future(); });

  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  google::bigtable::v2::MutateRowRequest request;
  auto fut = decorator->AsyncMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);

  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(1));
  EXPECT_TRUE(fut.cancel());
  EXPECT_TRUE(cancelled);
  // Channel stays acquired until the cancelled RPC actually completes.
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(1));

  p.set_value(internal::CancelledError("cancelled"));
  EXPECT_THAT(channel_->instant_outstanding_rpcs(), IsOkAndHolds(0));
  EXPECT_THAT(fut.get(), StatusIs(StatusCode::kCancelled));
}

TEST_F(BigtableRandomTwoLeastUsedTest,
       PendingAsyncUnaryBiasesP2CSelectionAwayFromBusyChannel) {
  auto instance_name =
      bigtable::InstanceResource(Project("my-project"), "my-instance")
          .FullName();
  bigtable::experimental::DynamicChannelPoolSizingPolicy sizing_policy;
  sizing_policy.minimum_channel_pool_size = 2;
  sizing_policy.maximum_channel_pool_size = 2;
  auto refresh_state = std::make_shared<ConnectionRefreshState>(
      fake_cq_impl_, std::chrono::milliseconds(1),
      std::chrono::milliseconds(10));

  auto stub_0 = std::make_shared<MockBigtableStub>();
  auto stub_1 = std::make_shared<MockBigtableStub>();
  auto ch_0 = std::make_shared<ChannelUsage<BigtableStub>>(stub_0, 0);
  auto ch_1 = std::make_shared<ChannelUsage<BigtableStub>>(stub_1, 0);

  promise<StatusOr<google::bigtable::v2::MutateRowResponse>> p_0;
  promise<StatusOr<google::bigtable::v2::MutateRowResponse>> p_1;
  EXPECT_CALL(*stub_0, AsyncMutateRow)
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::MutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p_0.get_future(); });
  EXPECT_CALL(*stub_1, AsyncMutateRow)
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::MutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p_1.get_future(); });

  auto pool = DynamicChannelPool<BigtableStub>::Create(
      instance_name, cq_, {ch_0, ch_1}, refresh_state,
      stub_factory_fn_.AsStdFunction(), sizing_policy,
      TransportType::kCloudPath);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool);

  google::bigtable::v2::MutateRowRequest request;
  // Two concurrent AsyncMutateRow calls must spread across both channels
  // because the first pending future keeps its channel's outstanding_rpcs at 1.
  auto f_a = decorator->AsyncMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);
  auto f_b = decorator->AsyncMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);

  EXPECT_THAT(ch_0->instant_outstanding_rpcs(), IsOkAndHolds(1));
  EXPECT_THAT(ch_1->instant_outstanding_rpcs(), IsOkAndHolds(1));

  // Resolving stub_0's RPC drops ch_0 back to 0 while ch_1 remains at 1, so a
  // subsequent sync MutateRow must deterministically route to stub_0.
  p_0.set_value(google::bigtable::v2::MutateRowResponse{});
  EXPECT_THAT(ch_0->instant_outstanding_rpcs(), IsOkAndHolds(0));
  EXPECT_THAT(ch_1->instant_outstanding_rpcs(), IsOkAndHolds(1));

  EXPECT_CALL(*stub_0, MutateRow)
      .WillOnce([](grpc::ClientContext&, Options const&,
                   google::bigtable::v2::MutateRowRequest const&,
                   OperationContext&) {
        return google::bigtable::v2::MutateRowResponse{};
      });
  EXPECT_CALL(*stub_1, MutateRow).Times(0);
  grpc::ClientContext sync_ctx;
  EXPECT_STATUS_OK(decorator->MutateRow(sync_ctx, {}, request, *op_ctx_));

  p_1.set_value(google::bigtable::v2::MutateRowResponse{});
  EXPECT_THAT(ch_1->instant_outstanding_rpcs(), IsOkAndHolds(0));
  EXPECT_STATUS_OK(f_a.get());
  EXPECT_STATUS_OK(f_b.get());
}

TEST_F(BigtableRandomTwoLeastUsedTest, PendingAsyncUnaryTriggersPoolScaleUp) {
  auto instance_name =
      bigtable::InstanceResource(Project("my-project"), "my-instance")
          .FullName();
  bigtable::experimental::DynamicChannelPoolSizingPolicy sizing_policy;
  sizing_policy.minimum_channel_pool_size = 1;
  sizing_policy.maximum_channel_pool_size = 2;
  sizing_policy.maximum_average_outstanding_rpcs_per_channel = 1;
  sizing_policy.minimum_average_outstanding_rpcs_per_channel = 0;
  auto refresh_state = std::make_shared<ConnectionRefreshState>(
      fake_cq_impl_, std::chrono::milliseconds(1),
      std::chrono::milliseconds(10));

  auto stub_0 = std::make_shared<MockBigtableStub>();
  auto stub_1 = std::make_shared<MockBigtableStub>();
  auto ch_0 = std::make_shared<ChannelUsage<BigtableStub>>(stub_0, 0);
  auto ch_1 = std::make_shared<ChannelUsage<BigtableStub>>(stub_1, 0);

  EXPECT_CALL(stub_factory_fn_, Call)
      .WillOnce([&](std::uint32_t, std::string const&, StubManager::Priming) {
        return ch_1;
      });

  auto pool = DynamicChannelPool<BigtableStub>::Create(
      instance_name, cq_, {ch_0}, refresh_state,
      stub_factory_fn_.AsStdFunction(), sizing_policy,
      TransportType::kCloudPath);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool);
  ASSERT_EQ(pool->size(), 1U);

  promise<StatusOr<google::bigtable::v2::MutateRowResponse>> p1;
  promise<StatusOr<google::bigtable::v2::MutateRowResponse>> p2;
  promise<StatusOr<google::bigtable::v2::MutateRowResponse>> p3;
  EXPECT_CALL(*stub_0, AsyncMutateRow)
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::MutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p1.get_future(); })
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::MutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p2.get_future(); })
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::MutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p3.get_future(); });

  google::bigtable::v2::MutateRowRequest request;
  auto f1 = decorator->AsyncMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);
  auto f2 = decorator->AsyncMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);
  EXPECT_THAT(ch_0->instant_outstanding_rpcs(), IsOkAndHolds(2));

  // Third call sees average_rpcs_per_channel == 2 > 1 and schedules scale-up.
  auto f3 = decorator->AsyncMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);
  fake_cq_impl_->SimulateCompletion(true);
  EXPECT_EQ(pool->size(), 2U);

  p1.set_value(google::bigtable::v2::MutateRowResponse{});
  p2.set_value(google::bigtable::v2::MutateRowResponse{});
  p3.set_value(google::bigtable::v2::MutateRowResponse{});
  EXPECT_THAT(ch_0->instant_outstanding_rpcs(), IsOkAndHolds(0));
  EXPECT_STATUS_OK(f1.get());
  EXPECT_STATUS_OK(f2.get());
  EXPECT_STATUS_OK(f3.get());
}

TEST_F(BigtableRandomTwoLeastUsedTest,
       PendingAsyncUnaryKeepsDrainingChannelAliveUntilResolved) {
  auto instance_name =
      bigtable::InstanceResource(Project("my-project"), "my-instance")
          .FullName();
  bigtable::experimental::DynamicChannelPoolSizingPolicy sizing_policy;
  sizing_policy.minimum_channel_pool_size = 1;
  sizing_policy.maximum_channel_pool_size = 2;
  sizing_policy.maximum_average_outstanding_rpcs_per_channel = 25;
  sizing_policy.minimum_average_outstanding_rpcs_per_channel = 10;
  auto refresh_state = std::make_shared<ConnectionRefreshState>(
      fake_cq_impl_, std::chrono::milliseconds(1),
      std::chrono::milliseconds(10));

  auto stub_0 = std::make_shared<MockBigtableStub>();
  auto stub_1 = std::make_shared<MockBigtableStub>();
  auto ch_0 = std::make_shared<ChannelUsage<BigtableStub>>(stub_0, 0);
  auto ch_1 = std::make_shared<ChannelUsage<BigtableStub>>(stub_1, 0);

  std::weak_ptr<ChannelUsage<BigtableStub>> weak_ch_0 = ch_0;
  std::weak_ptr<ChannelUsage<BigtableStub>> weak_ch_1 = ch_1;
  auto pool = DynamicChannelPool<BigtableStub>::Create(
      instance_name, cq_, {std::move(ch_0), std::move(ch_1)}, refresh_state,
      stub_factory_fn_.AsStdFunction(), sizing_policy,
      TransportType::kCloudPath);
  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool);
  ASSERT_EQ(pool->size(), 2U);

  promise<StatusOr<google::bigtable::v2::MutateRowResponse>> p0;
  promise<StatusOr<google::bigtable::v2::MutateRowResponse>> p1;
  EXPECT_CALL(*stub_0, AsyncMutateRow)
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::MutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p0.get_future(); });
  EXPECT_CALL(*stub_1, AsyncMutateRow)
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::MutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p1.get_future(); });

  google::bigtable::v2::MutateRowRequest request;
  auto f0 = decorator->AsyncMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);
  auto f1 = decorator->AsyncMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx_);
  ASSERT_FALSE(weak_ch_0.expired());
  ASSERT_FALSE(weak_ch_1.expired());
  EXPECT_THAT(weak_ch_0.lock()->instant_outstanding_rpcs(), IsOkAndHolds(1));
  EXPECT_THAT(weak_ch_1.lock()->instant_outstanding_rpcs(), IsOkAndHolds(1));

  // Expire the initial scale-down cooldown timer.
  fake_cq_impl_->SimulateCompletion(true);
  ASSERT_TRUE(fake_cq_impl_->empty());

  // Trigger scale-down (average == (1 + 1) / 2 == 1 < 10, cooldown ready),
  // moving one of the two busy channels into draining_channels_.
  int sync_calls = 0;
  auto sync_handler = [&](grpc::ClientContext&, Options const&,
                          google::bigtable::v2::MutateRowRequest const&,
                          OperationContext&) {
    ++sync_calls;
    return google::bigtable::v2::MutateRowResponse{};
  };
  EXPECT_CALL(*stub_0, MutateRow)
      .Times(::testing::AtMost(1))
      .WillRepeatedly(sync_handler);
  EXPECT_CALL(*stub_1, MutateRow)
      .Times(::testing::AtMost(1))
      .WillRepeatedly(sync_handler);
  grpc::ClientContext sync_ctx;
  EXPECT_STATUS_OK(decorator->MutateRow(sync_ctx, {}, request, *op_ctx_));
  EXPECT_EQ(sync_calls, 1);
  EXPECT_EQ(pool->size(), 1U);

  // Firing the scheduled timers runs RemoveChannels(); the draining channel
  // must remain alive while its in-flight async unary RPC has not resolved.
  while (!fake_cq_impl_->empty()) {
    fake_cq_impl_->SimulateCompletion(true);
  }
  EXPECT_FALSE(weak_ch_0.expired());
  EXPECT_FALSE(weak_ch_1.expired());
  EXPECT_THAT(weak_ch_0.lock()->instant_outstanding_rpcs(), IsOkAndHolds(1));
  EXPECT_THAT(weak_ch_1.lock()->instant_outstanding_rpcs(), IsOkAndHolds(1));

  // Once the in-flight async unary RPCs resolve, both channels release cleanly.
  p0.set_value(google::bigtable::v2::MutateRowResponse{});
  p1.set_value(google::bigtable::v2::MutateRowResponse{});
  EXPECT_THAT(weak_ch_0.lock()->instant_outstanding_rpcs(), IsOkAndHolds(0));
  EXPECT_THAT(weak_ch_1.lock()->instant_outstanding_rpcs(), IsOkAndHolds(0));

  EXPECT_STATUS_OK(f0.get());
  EXPECT_STATUS_OK(f1.get());
}

#ifdef GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS
class MockMetric : public TableSchemaMetric {
 public:
  MOCK_METHOD(void, StubSelection,
              (opentelemetry::context::Context const&,
               StubSelectionParams const&),
              (override));
  MOCK_METHOD(std::unique_ptr<TableSchemaMetric>, clone,
              (TableResourceLabels const&, TableDataLabels const&),
              (const, override));
};

TEST_F(BigtableRandomTwoLeastUsedTest,
       AsyncUnaryRecordsInFlightOutstandingRpcsInMetrics) {
  auto mock_metric = std::make_shared<MockMetric>();
  auto op_ctx = std::make_shared<OperationContext>(
      std::vector<std::shared_ptr<Metric>>{mock_metric},
      std::make_shared<OperationContext::Clock>());

  std::vector<std::int64_t> recorded_outstanding;
  EXPECT_CALL(*mock_metric, StubSelection)
      .Times(3)
      .WillRepeatedly([&](opentelemetry::context::Context const&,
                          StubSelectionParams const& p) {
        recorded_outstanding.push_back(p.outstanding_rpcs);
        EXPECT_EQ(p.channel_pool_lb_policy,
                  ChannelPoolLbPolicy::kRandomTwoLeastUsed);
        EXPECT_EQ(p.streaming, RpcType::kUnary);
      });

  promise<StatusOr<google::bigtable::v2::MutateRowResponse>> p1;
  promise<StatusOr<google::bigtable::v2::MutateRowResponse>> p2;
  promise<StatusOr<google::bigtable::v2::MutateRowResponse>> p3;
  EXPECT_CALL(*mock_stub_, AsyncMutateRow)
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::MutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p1.get_future(); })
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::MutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p2.get_future(); })
      .WillOnce(
          [&](CompletionQueue&, std::shared_ptr<grpc::ClientContext>,
              internal::ImmutableOptions,
              google::bigtable::v2::MutateRowRequest const&,
              std::shared_ptr<OperationContext>) { return p3.get_future(); });

  auto decorator = std::make_shared<BigtableRandomTwoLeastUsed>(pool_);
  google::bigtable::v2::MutateRowRequest request;

  auto f1 = decorator->AsyncMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx);
  auto f2 = decorator->AsyncMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx);

  p1.set_value(google::bigtable::v2::MutateRowResponse{});
  auto f3 = decorator->AsyncMutateRow(
      cq_, std::make_shared<grpc::ClientContext>(), {}, request, op_ctx);

  EXPECT_THAT(recorded_outstanding, ::testing::ElementsAre(0, 1, 1));

  p2.set_value(google::bigtable::v2::MutateRowResponse{});
  p3.set_value(google::bigtable::v2::MutateRowResponse{});
  EXPECT_STATUS_OK(f1.get());
  EXPECT_STATUS_OK(f2.get());
  EXPECT_STATUS_OK(f3.get());
}
#endif  // GOOGLE_CLOUD_CPP_BIGTABLE_WITH_OTEL_METRICS

}  // namespace
GOOGLE_CLOUD_CPP_INLINE_NAMESPACE_END
}  // namespace bigtable_internal
}  // namespace cloud
}  // namespace google
