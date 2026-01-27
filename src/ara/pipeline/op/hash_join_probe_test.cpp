#include "hash_join_probe.h"
#include "hash_join.h"
#include "hash_join_build.h"
#include "op_output.h"

#include <ara/common/query_context.h>
#include <ara/pipeline/pipeline_context.h>
#include <ara/pipeline/pipeline_observer.h>
#include <ara/schedule/async_dual_pool_scheduler.h>
#include <ara/schedule/schedule_context.h>
#include <ara/schedule/schedule_observer.h>
#include <ara/task/task_status.h>

#include <arrow/acero/query_context.h>
#include <arrow/acero/test_util_internal.h>
#include <arrow/testing/gtest_util.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>

using namespace ara;
using namespace ara::pipeline;
using namespace ara::schedule;
using namespace ara::task;

using ara::pipeline::detail::HashJoin;

class HashJoinProbeTest : public testing::Test {
 protected:
  size_t dop_;
  std::unique_ptr<arrow::acero::QueryContext> arrow_query_ctx_;
  QueryContext query_ctx_;
  PipelineContext pipeline_ctx_{&query_ctx_};
  std::shared_ptr<HashJoin> hash_join_ = std::make_shared<HashJoin>();
  HashJoinBuild hash_join_build_{"HashJoinBuild", ""};
  HashJoinProbe hash_join_probe_{"HashJoinProbe", ""};
  std::unique_ptr<SourceOp> hash_join_scan_source_ = nullptr;
  ScheduleContext schedule_ctx_;
  folly::CPUThreadPoolExecutor cpu_executor{1};
  AsyncDualPoolScheduler scheduler{&cpu_executor, nullptr};

  void Init(size_t dop, size_t source_batch_length, size_t pipe_batch_length,
            size_t mini_batch_length, const arrow::acero::HashJoinNodeOptions& options,
            const arrow::Schema& left_schema, const arrow::Schema& right_schema) {
    dop_ = dop;
    arrow_query_ctx_ = std::make_unique<arrow::acero::QueryContext>(
        arrow::acero::QueryOptions{}, arrow::compute::ExecContext());
    query_ctx_.options.source_max_batch_length = source_batch_length;
    query_ctx_.options.pipe_max_batch_length = pipe_batch_length;
    query_ctx_.options.mini_batch_length = mini_batch_length;
    ASSERT_OK(arrow_query_ctx_->Init(nullptr));
    ASSERT_OK(hash_join_->Init(pipeline_ctx_, dop, arrow_query_ctx_.get(), options,
                               left_schema, right_schema));
    ASSERT_OK(hash_join_build_.Init(pipeline_ctx_, hash_join_));
    ASSERT_OK(hash_join_probe_.Init(pipeline_ctx_, hash_join_));
  }

  void RunHashJoinBuild(const Batches& batches) {
    {
      Task sink_task("HashJoinBuild::SinkTask", "",
                     [&](const TaskContext& task_ctx, ThreadId thread_id) -> TaskResult {
                       for (auto& batch : batches) {
                         ARA_ASSIGN_OR_RAISE(
                             auto op_result,
                             hash_join_build_.Sink(pipeline_ctx_)(pipeline_ctx_, task_ctx,
                                                                  thread_id, batch));
                         if (!op_result.IsPipeSinkNeedsMore()) {
                           return Status::UnknownError("Unexpected result");
                         }
                       }
                       return TaskStatus::Finished();
                     });
      TaskGroup sink_tg("HashJoinBuild::Sink", "", std::move(sink_task), dop_,
                        std::nullopt, nullptr);
      auto result = scheduler.Schedule(schedule_ctx_, sink_tg);
      ASSERT_OK(result);
      auto tg_result = result.ValueOrDie()->Wait(schedule_ctx_);
      ASSERT_OK(tg_result);
      ASSERT_TRUE(tg_result->IsFinished()) << tg_result->ToString();
    }

    {
      auto build_tgs = hash_join_build_.Frontend(pipeline_ctx_);
      for (const auto& tg : build_tgs) {
        auto result = scheduler.Schedule(schedule_ctx_, tg);
        ASSERT_OK(result);
        auto tg_result = result.ValueOrDie()->Wait(schedule_ctx_);
        ASSERT_OK(tg_result);
        ASSERT_TRUE(tg_result->IsFinished()) << tg_result->ToString();
      }
    }
  }

  OpResult Probe(ThreadId thread_id, std::optional<Batch> input) {
    return hash_join_probe_.Pipe(pipeline_ctx_)(pipeline_ctx_, TaskContext{}, thread_id,
                                                std::move(input));
  }

  OpResult Drain(ThreadId thread_id) {
    return hash_join_probe_.Drain(pipeline_ctx_)(pipeline_ctx_, TaskContext{}, thread_id);
  }

  void StartScan() {
    hash_join_scan_source_ = hash_join_probe_.ImplicitSource(pipeline_ctx_);
    ASSERT_NE(hash_join_scan_source_, nullptr);
    ASSERT_FALSE(hash_join_scan_source_->Backend(pipeline_ctx_).has_value());
    auto fe = hash_join_scan_source_->Frontend(pipeline_ctx_);
    ASSERT_EQ(fe.size(), 1);
    auto result = scheduler.Schedule(schedule_ctx_, fe[0]);
    ASSERT_OK(result);
    auto fe_result = result.ValueOrDie()->Wait(schedule_ctx_);
    ASSERT_OK(fe_result);
    ASSERT_TRUE(fe_result->IsFinished()) << fe_result->ToString();
  }

  OpResult Scan(ThreadId thread_id) {
    return hash_join_scan_source_->Source(pipeline_ctx_)(pipeline_ctx_, TaskContext{},
                                                         thread_id);
  }
};

TEST_F(HashJoinProbeTest, InnerMiniBatchHasMore) {
  size_t dop = 4;
  size_t source_batch_length = 1024;
  size_t pipe_batch_length = 3;
  size_t mini_batch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::INNER, {{0}}, {{0}}, {{0}, {1}}, {}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, schema_with_batch.batches[0]);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_EQ(result->GetBatch()->length, 3);
  }

  {
    auto result = Probe(0, std::nullopt);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore()) << result->ToString();
  }

  {
    auto result = Drain(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }
}

TEST_F(HashJoinProbeTest, InnerMatchHasMore) {
  size_t dop = 4;
  size_t source_batch_length = 1024;
  size_t pipe_batch_length = 1;
  size_t mini_batch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::INNER, {{0}}, {{0}}, {{0}, {1}}, {}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, schema_with_batch.batches[0]);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_EQ(result->GetBatch()->length, 1);
  }

  for (size_t i = 0; i < 3; ++i) {
    auto result = Probe(0, std::nullopt);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_EQ(result->GetBatch()->length, 1);
  }

  {
    auto result = Probe(0, std::nullopt);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore()) << result->ToString();
  }

  {
    auto result = Drain(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_FALSE(result->GetBatch().has_value());
  }
}

TEST_F(HashJoinProbeTest, InnerEven) {
  size_t dop = 4;
  size_t source_batch_length = 1024;
  size_t pipe_batch_length = 4;
  size_t mini_batch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::INNER, {{0}}, {{0}}, {{0}, {1}}, {}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  // TODO: This should be an Even, but current JoinMatchIterator always requires a
  // GetNextMatch() call to tell the current batch is done so the best we can get is a
  // HasMore.
  {
    auto result = Probe(0, schema_with_batch.batches[0]);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 4);
  }

  {
    auto result = Probe(0, std::nullopt);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore()) << result->ToString();
  }

  {
    auto result = Drain(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_FALSE(result->GetBatch().has_value());
  }
}

TEST_F(HashJoinProbeTest, InnerNeedsMore) {
  size_t dop = 4;
  size_t source_batch_length = 1024;
  size_t pipe_batch_length = 5;
  size_t mini_batch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::INNER, {{0}}, {{0}}, {{0}, {1}}, {}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, schema_with_batch.batches[0]);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore()) << result->ToString();
  }

  {
    auto result = Drain(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 4);
  }
}

TEST_F(HashJoinProbeTest, LeftOuterMiniBatchHasMore) {
  size_t dop = 4;
  size_t source_batch_length = 1024;
  size_t pipe_batch_length = 5;
  size_t mini_batch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  auto probe_batch = arrow::acero::ExecBatchFromJSON(
      {arrow::int32(), arrow::boolean()}, "[[null, true], [null, false], [4, false]]");
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::LEFT_OUTER, {{0}}, {{0}}, {{0}, {1}}, {}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, probe_batch);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 5);
  }

  {
    auto result = Probe(0, std::nullopt);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore()) << result->ToString();
  }

  {
    auto result = Drain(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }
}

TEST_F(HashJoinProbeTest, LeftOuterMatchHasMore) {
  size_t dop = 4;
  size_t source_batch_length = 1024;
  size_t pipe_batch_length = 1;
  size_t mini_batch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::LEFT_OUTER, {{0}}, {{0}}, {{0}, {1}}, {}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, schema_with_batch.batches[0]);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }

  for (size_t i = 0; i < 4; ++i) {
    auto result = Probe(0, std::nullopt);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }

  {
    auto result = Probe(0, std::nullopt);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore()) << result->ToString();
  }

  {
    auto result = Drain(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_FALSE(result->GetBatch().has_value());
  }
}

TEST_F(HashJoinProbeTest, LeftOuterEven) {
  size_t dop = 4;
  size_t source_batch_length = 1024;
  size_t pipe_batch_length = 5;
  size_t mini_batch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  auto probe_batch = arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                                     "[[4, false], [null, true]]");
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::LEFT_OUTER, {{0}}, {{0}}, {{0}, {1}}, {}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, probe_batch);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeEven()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 5);
  }

  {
    auto result = Drain(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_FALSE(result->GetBatch().has_value());
  }
}

TEST_F(HashJoinProbeTest, LeftOuterNeedsMore) {
  size_t dop = 4;
  size_t source_batch_length = 1024;
  size_t pipe_batch_length = 6;
  size_t mini_batch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::LEFT_OUTER, {{0}}, {{0}}, {{0}, {1}}, {}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, schema_with_batch.batches[0]);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore()) << result->ToString();
  }

  {
    auto result = Drain(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 5);
  }
}

TEST_F(HashJoinProbeTest, LeftSemiHasMoreAndEven) {
  size_t dop = 4;
  size_t source_batch_length = 1024;
  size_t pipe_batch_length = 1;
  size_t mini_batch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::LEFT_SEMI, {{0}}, {{0}}, {{0}, {1}}, {}};
  auto probe_batch = arrow::acero::ExecBatchFromJSON(
      {arrow::int32(), arrow::boolean()}, "[[null, true], [4, false], [4, true]]");
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, probe_batch);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }

  {
    auto result = Probe(0, std::nullopt);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeEven()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }

  {
    auto result = Drain(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_FALSE(result->GetBatch().has_value());
  }
}

TEST_F(HashJoinProbeTest, LeftSemiNeedsMore) {
  size_t dop = 4;
  size_t source_batch_length = 1024;
  size_t pipe_batch_length = 2;
  size_t mini_batch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::LEFT_SEMI, {{0}}, {{0}}, {{0}, {1}}, {}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, schema_with_batch.batches[0]);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore()) << result->ToString();
  }

  {
    auto result = Drain(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }
}

TEST_F(HashJoinProbeTest, LeftAntiHasMoreAndEven) {
  size_t dop = 4;
  size_t source_batch_length = 1024;
  size_t pipe_batch_length = 1;
  size_t mini_batch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::LEFT_ANTI, {{0}}, {{0}}, {{0}, {1}}, {}};
  auto probe_batch = arrow::acero::ExecBatchFromJSON(
      {arrow::int32(), arrow::boolean()}, "[[4, false], [null, true], [null, false]]");
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, probe_batch);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }

  {
    auto result = Probe(0, std::nullopt);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeEven()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }

  {
    auto result = Drain(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_FALSE(result->GetBatch().has_value());
  }
}

TEST_F(HashJoinProbeTest, LeftAntiNeedsMore) {
  size_t dop = 4;
  size_t source_batch_length = 1024;
  size_t pipe_batch_length = 2;
  size_t mini_batch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::LEFT_ANTI, {{0}}, {{0}}, {{0}, {1}}, {}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, schema_with_batch.batches[0]);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore()) << result->ToString();
  }

  {
    auto result = Drain(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }
}

TEST_F(HashJoinProbeTest, RightOuter) {
  size_t dop = 4;
  size_t source_batch_length = 3;
  size_t pipe_batch_length = 16;
  size_t mini_batch_length = 1;

  auto schema_with_batch = []() {
    arrow::acero::BatchesWithSchema out;
    out.batches = {
        arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                        "[[null, true], [0, false], [1, false]]"),
        arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                        "[[2, null], [3, false], [4, false]]")};
    out.schema = arrow::schema(
        {arrow::field("i32", arrow::int32()), arrow::field("bool", arrow::boolean())});
    return out;
  }();
  auto probe_batch =
      arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                      "[[0, false], [0, true], [1, false], [2, null]]");
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::RIGHT_OUTER, {{0}}, {{0}}, {{0}, {1}}, {{0}, {1}}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, probe_batch);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 16);
  }

  StartScan();

  {
    auto result = Scan(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }

  {
    auto result = Scan(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }

  {
    auto result = Scan(1);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_FALSE(result->GetBatch().has_value());
  }

  {
    auto result = Scan(2);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 2);
  }

  {
    auto result = Scan(3);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }

  {
    auto result = Scan(3);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }
}

TEST_F(HashJoinProbeTest, RightOuterDrainInScan) {
  size_t dop = 4;
  size_t source_batch_length = 17;
  size_t pipe_batch_length = 17;
  size_t mini_batch_length = 1;

  auto schema_with_batch = []() {
    arrow::acero::BatchesWithSchema out;
    out.batches = {
        arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                        "[[null, true], [0, false], [1, false]]"),
        arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                        "[[2, null], [3, false], [4, false]]")};
    out.schema = arrow::schema(
        {arrow::field("i32", arrow::int32()), arrow::field("bool", arrow::boolean())});
    return out;
  }();
  auto probe_batch =
      arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                      "[[1, false], [2, true], [3, false], [4, null]]");
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::RIGHT_OUTER, {{0}}, {{0}}, {{0}, {1}}, {{0}, {1}}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, probe_batch);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore()) << result->ToString();
  }

  StartScan();

  {
    auto result = Scan(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 17);
  }

  {
    auto result = Scan(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
  }
}

TEST_F(HashJoinProbeTest, RightOuterDrainBeforeScan) {
  size_t dop = 4;
  size_t source_batch_length = 3;
  size_t pipe_batch_length = 17;
  size_t mini_batch_length = 1;

  auto schema_with_batch = []() {
    arrow::acero::BatchesWithSchema out;
    out.batches = {
        arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                        "[[null, true], [0, false], [1, false]]"),
        arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                        "[[2, null], [3, false], [4, false]]")};
    out.schema = arrow::schema(
        {arrow::field("i32", arrow::int32()), arrow::field("bool", arrow::boolean())});
    return out;
  }();
  auto probe_batch =
      arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                      "[[1, false], [2, true], [3, false], [4, null]]");
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::RIGHT_OUTER, {{0}}, {{0}}, {{0}, {1}}, {{0}, {1}}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, probe_batch);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore()) << result->ToString();
  }

  StartScan();

  {
    auto result = Scan(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 16);
  }

  {
    auto result = Scan(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
  }
}

TEST_F(HashJoinProbeTest, RightSemi) {
  size_t dop = 4;
  size_t source_batch_length = 3;
  size_t pipe_batch_length = 16;
  size_t mini_batch_length = 1;

  auto schema_with_batch = []() {
    arrow::acero::BatchesWithSchema out;
    out.batches = {
        arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                        "[[null, true], [0, false], [1, false]]"),
        arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                        "[[2, null], [3, false], [4, false]]")};
    out.schema = arrow::schema(
        {arrow::field("i32", arrow::int32()), arrow::field("bool", arrow::boolean())});
    return out;
  }();
  auto probe_batch =
      arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                      "[[0, false], [0, true], [1, false], [2, null]]");
  arrow::acero::HashJoinNodeOptions options{arrow::acero::JoinType::RIGHT_SEMI,
                                            {{0}},
                                            {{0}},
                                            std::vector<arrow::FieldRef>{},
                                            {{0}, {1}}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, probe_batch);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore()) << result->ToString();
  }

  StartScan();

  {
    auto result = Scan(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 2);
  }

  {
    auto result = Scan(1);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }

  {
    auto result = Scan(1);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }

  {
    auto result = Scan(2);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }

  {
    auto result = Scan(2);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }

  {
    auto result = Scan(3);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_FALSE(result->GetBatch().has_value());
  }
}

TEST_F(HashJoinProbeTest, RightAnti) {
  size_t dop = 4;
  size_t source_batch_length = 3;
  size_t pipe_batch_length = 16;
  size_t mini_batch_length = 1;

  auto schema_with_batch = []() {
    arrow::acero::BatchesWithSchema out;
    out.batches = {
        arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                        "[[null, true], [0, false], [1, false]]"),
        arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                        "[[2, null], [3, false], [4, false]]")};
    out.schema = arrow::schema(
        {arrow::field("i32", arrow::int32()), arrow::field("bool", arrow::boolean())});
    return out;
  }();
  auto probe_batch =
      arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                      "[[0, false], [0, true], [1, false], [2, null]]");
  arrow::acero::HashJoinNodeOptions options{arrow::acero::JoinType::RIGHT_ANTI,
                                            {{0}},
                                            {{0}},
                                            std::vector<arrow::FieldRef>{},
                                            {{0}, {1}}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, probe_batch);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeSinkNeedsMore()) << result->ToString();
  }

  StartScan();

  {
    auto result = Scan(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }

  {
    auto result = Scan(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }

  {
    auto result = Scan(1);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_FALSE(result->GetBatch().has_value());
  }

  {
    auto result = Scan(2);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 2);
  }

  {
    auto result = Scan(3);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }

  {
    auto result = Scan(3);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }
}

TEST_F(HashJoinProbeTest, FullOuter) {
  size_t dop = 4;
  size_t source_batch_length = 3;
  size_t pipe_batch_length = 3;
  size_t mini_batch_length = 1;

  auto schema_with_batch = []() {
    arrow::acero::BatchesWithSchema out;
    out.batches = {
        arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                        "[[null, true], [0, false], [1, false]]"),
        arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::boolean()},
                                        "[[2, null], [3, false], [4, false]]")};
    out.schema = arrow::schema(
        {arrow::field("i32", arrow::int32()), arrow::field("bool", arrow::boolean())});
    return out;
  }();
  auto probe_batch = arrow::acero::ExecBatchFromJSON(
      {arrow::int32(), arrow::boolean()},
      "[[0, false], [0, true], [null, null], [1, false], [2, null], [5, true]]");
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::FULL_OUTER, {{0}}, {{0}}, {{0}, {1}}, {{0}, {1}}};
  Init(dop, source_batch_length, pipe_batch_length, mini_batch_length, options,
       *schema_with_batch.schema, *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  {
    auto result = Probe(0, probe_batch);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }

  for (size_t i = 0; i < 4; ++i) {
    auto result = Probe(0, std::nullopt);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }

  {
    auto result = Probe(0, std::nullopt);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsPipeEven()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }

  StartScan();

  {
    auto result = Scan(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }

  {
    auto result = Scan(0);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 1);
  }

  {
    auto result = Scan(1);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_FALSE(result->GetBatch().has_value());
  }

  {
    auto result = Scan(2);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 2);
  }

  {
    auto result = Scan(3);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsSourcePipeHasMore()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }

  {
    auto result = Scan(3);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_TRUE(result->GetBatch().has_value());
    ASSERT_EQ(result->GetBatch()->length, 3);
  }
}
