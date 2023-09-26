#include "hash_join_probe.h"
#include "hash_join.h"
#include "hash_join_build.h"
#include "op_output.h"

#include <ara/common/query_context.h>
#include <ara/pipeline/pipeline_context.h>
#include <ara/pipeline/pipeline_observer.h>
#include <ara/schedule/naive_parallel_scheduler.h>
#include <ara/schedule/schedule_context.h>
#include <ara/schedule/schedule_observer.h>
#include <ara/task/task_status.h>

#include <arrow/acero/query_context.h>
#include <arrow/acero/test_util_internal.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

using namespace ara;
using namespace ara::task;
using namespace ara::pipeline;
using namespace ara::schedule;

using ara::pipeline::detail::HashJoin;

class HashJoinProbeTest : public testing::Test {
 protected:
  size_t dop_;
  std::unique_ptr<arrow::acero::QueryContext> arrow_query_ctx_;
  QueryContext query_ctx_{Options{8, 8, 4}};
  PipelineContext pipeline_ctx_{&query_ctx_};
  std::shared_ptr<HashJoin> hash_join_ = std::make_shared<HashJoin>();
  HashJoinBuild hash_join_build_{"HashJoinBuild", ""};
  HashJoinProbe hash_join_probe_{"HashJoinProbe", ""};
  ScheduleContext schedule_ctx_;
  NaiveParallelScheduler scheduler;

  void Init(size_t dop, size_t batch_length, size_t minibatch_length,
            const arrow::acero::HashJoinNodeOptions& options,
            const arrow::Schema& left_schema, const arrow::Schema& right_schema) {
    dop_ = dop;
    arrow_query_ctx_ = std::make_unique<arrow::acero::QueryContext>(
        arrow::acero::QueryOptions{}, arrow::compute::ExecContext());
    query_ctx_.options.pipe_max_batch_length = batch_length;
    query_ctx_.options.pipe_minibatch_length = minibatch_length;
    ASSERT_OK(arrow_query_ctx_->Init(dop, nullptr));
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
      ASSERT_TRUE(tg_result->IsFinished());
    }

    {
      auto build_tgs = hash_join_build_.Frontend(pipeline_ctx_);
      for (const auto& tg : build_tgs) {
        auto result = scheduler.Schedule(schedule_ctx_, tg);
        ASSERT_OK(result);
        auto tg_result = result.ValueOrDie()->Wait(schedule_ctx_);
        ASSERT_OK(tg_result);
        ASSERT_TRUE(tg_result->IsFinished());
      }
    }
  }

  OpResult ProbeOne(ThreadId thread_id, std::optional<Batch> input) {
    return hash_join_probe_.Pipe(pipeline_ctx_)(pipeline_ctx_, TaskContext{}, thread_id,
                                                std::move(input));
  }
};

TEST_F(HashJoinProbeTest, InnerBatchHasMore) {
  size_t dop = 4;
  size_t batch_length = 3;
  size_t minibatch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::INNER, {{0}}, {{0}}, {{0}, {1}}, {}};
  Init(dop, batch_length, minibatch_length, options, *schema_with_batch.schema,
       *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  auto probe_result = ProbeOne(0, schema_with_batch.batches[0]);
  ASSERT_OK(probe_result);
  ASSERT_TRUE(probe_result->IsSourcePipeHasMore());
  ASSERT_EQ(probe_result->GetBatch()->length, 3);
}

TEST_F(HashJoinProbeTest, InnerEven) {
  size_t dop = 4;
  size_t batch_length = 4;
  size_t minibatch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::INNER, {{0}}, {{0}}, {{0}, {1}}, {}};
  Init(dop, batch_length, minibatch_length, options, *schema_with_batch.schema,
       *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  // TODO: This should be an Even, but current JoinMatchIterator always requires a
  // GetNextMatch() call to tell the current batch is done so the best we can get is a
  // HasMore.
  {
    auto probe_result = ProbeOne(0, schema_with_batch.batches[0]);
    ASSERT_OK(probe_result);
    ASSERT_TRUE(probe_result->IsSourcePipeHasMore());
    ASSERT_EQ(probe_result->GetBatch()->length, 4);
  }

  {
    auto probe_result = ProbeOne(0, std::nullopt);
    ASSERT_OK(probe_result);
    ASSERT_TRUE(probe_result->IsPipeSinkNeedsMore());
  }
}

TEST_F(HashJoinProbeTest, InnerNeedsMore) {
  size_t dop = 4;
  size_t batch_length = 5;
  size_t minibatch_length = 1;

  auto schema_with_batch = arrow::acero::MakeBasicBatches();
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::INNER, {{0}}, {{0}}, {{0}, {1}}, {}};
  Init(dop, batch_length, minibatch_length, options, *schema_with_batch.schema,
       *schema_with_batch.schema);
  RunHashJoinBuild(schema_with_batch.batches);

  auto probe_result = ProbeOne(0, schema_with_batch.batches[0]);
  ASSERT_OK(probe_result);
  ASSERT_TRUE(probe_result->IsPipeSinkNeedsMore());
}
