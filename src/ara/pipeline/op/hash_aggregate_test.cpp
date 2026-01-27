#include "hash_aggregate.h"
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
#include <arrow/testing/builder.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>

using namespace ara;
using namespace ara::pipeline;
using namespace ara::schedule;
using namespace ara::task;

class HashAggregateTest : public testing::Test {
 protected:
  size_t dop_ = 4;
  std::unique_ptr<arrow::acero::QueryContext> arrow_query_ctx_;
  QueryContext query_ctx_;
  PipelineContext pipeline_ctx_{&query_ctx_};
  ScheduleContext schedule_ctx_;
  folly::CPUThreadPoolExecutor cpu_executor_{4};
  AsyncDualPoolScheduler scheduler{&cpu_executor_, nullptr};

  void SetUp() override {
    arrow_query_ctx_ = std::make_unique<arrow::acero::QueryContext>(
        arrow::acero::QueryOptions{}, arrow::compute::ExecContext());
    ASSERT_OK(arrow_query_ctx_->Init(nullptr));
  }

  Result<std::vector<Batch>> Run(const arrow::acero::AggregateNodeOptions& options,
                                 const std::shared_ptr<arrow::Schema>& input_schema,
                                 const Batch& batch) {
    HashAggregate sa("TestHashAggregate", "For test");
    ARA_RETURN_NOT_OK(
        sa.Init(pipeline_ctx_, dop_, arrow_query_ctx_.get(), options, *input_schema));

    {
      Task sink_task(
          "HashAggregate::SinkTask", "",
          [&](const TaskContext& task_ctx, ThreadId thread_id) -> TaskResult {
            ARA_ASSIGN_OR_RAISE(
                auto op_result,
                sa.Sink(pipeline_ctx_)(pipeline_ctx_, task_ctx, thread_id, batch));
            if (!op_result.IsPipeSinkNeedsMore()) {
              return Status::UnknownError("Unexpected result of HashAggregate::Sink");
            }
            return TaskStatus::Finished();
          });
      TaskGroup sink_tg("HashAggregate::Sink", "", std::move(sink_task), dop_,
                        std::nullopt, nullptr);
      auto result = scheduler.Schedule(schedule_ctx_, sink_tg);
      ARA_RETURN_NOT_OK(result);
      auto tg_result = result.ValueOrDie()->Wait(schedule_ctx_);
      ARA_RETURN_NOT_OK(tg_result);
      ARA_RETURN_IF(!tg_result->IsFinished(),
                    Status::UnknownError("Unexpected result of HashAggregate::Sink"));
    }

    {
      auto tgs = sa.Frontend(pipeline_ctx_);
      for (const auto& tg : tgs) {
        auto result = scheduler.Schedule(schedule_ctx_, tg);
        ARA_RETURN_NOT_OK(result);
        auto tg_result = result.ValueOrDie()->Wait(schedule_ctx_);
        ARA_RETURN_NOT_OK(tg_result);
        ARA_RETURN_IF(!tg_result->IsFinished(),
                      Status::UnknownError("Unexpected result of HashAggregate::Sink"));
      }
    }

    auto source = sa.ImplicitSource(pipeline_ctx_);
    std::vector<Batch> batches;
    for (size_t i = 0; i < dop_; ++i) {
      auto result = source->Source(pipeline_ctx_)(pipeline_ctx_, TaskContext{}, i);
      ARA_RETURN_IF(!result->IsFinished() || !result->GetBatch().has_value(),
                    Status::UnknownError("Unexpected result of HashAggregate::Sink"));
      batches.push_back(std::move(result->GetBatch().value()));
    }
    return std::move(batches);
  }
};

TEST_F(HashAggregateTest, Count) {
  arrow::compute::Aggregate agg("hash_count", nullptr, std::vector<arrow::FieldRef>{{1}},
                                "count");
  arrow::acero::AggregateNodeOptions options({std::move(agg)});
  auto input_schema = arrow::schema(
      {arrow::field("key_i32", arrow::int32()), arrow::field("agg_i32", arrow::int32())});

  auto result = Run(
      options, input_schema,
      arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::int32()},
                                      "[[1, 11], [1, 12], [1, 13], [1, 14], [2, 21], "
                                      "[2, 22], [2, 23], [3, 31], [3, 32], [4, 41]]"));
  ASSERT_OK(result);
  auto batches = result.ValueOrDie();

  auto output_schema = arrow::schema(
      {arrow::field("key_i32", arrow::int32()), arrow::field("count", arrow::int64())});
  std::stringstream ss;
  ss << "[[1, " << 4 * dop_ << "], [2, " << 3 * dop_ << "], [3, " << 2 * dop_ << "], [4, "
     << 1 * dop_ << "]]";
  auto exp_batch =
      arrow::acero::ExecBatchFromJSON({arrow::int32(), arrow::int64()}, ss.str());
  arrow::acero::AssertExecBatchesEqualIgnoringOrder(output_schema, {exp_batch}, batches);
}
