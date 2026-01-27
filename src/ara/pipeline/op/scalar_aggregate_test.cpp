#include "scalar_aggregate.h"
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

class ScalarAggregateTest : public testing::Test {
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

  OpResult Run(const arrow::acero::AggregateNodeOptions& options,
               const std::shared_ptr<arrow::Schema>& input_schema, const Batch& batch) {
    ScalarAggregate sa("TestScalarAggregate", "For test");
    ARA_RETURN_NOT_OK(
        sa.Init(pipeline_ctx_, dop_, arrow_query_ctx_.get(), options, *input_schema));

    {
      Task sink_task(
          "ScalarAggregate::SinkTask", "",
          [&](const TaskContext& task_ctx, ThreadId thread_id) -> TaskResult {
            ARA_ASSIGN_OR_RAISE(
                auto op_result,
                sa.Sink(pipeline_ctx_)(pipeline_ctx_, task_ctx, thread_id, batch));
            if (!op_result.IsPipeSinkNeedsMore()) {
              return Status::UnknownError("Unexpected result of ScalarAggregate::Sink");
            }
            return TaskStatus::Finished();
          });
      TaskGroup sink_tg("ScalarAggregate::Sink", "", std::move(sink_task), dop_,
                        std::nullopt, nullptr);
      auto result = scheduler.Schedule(schedule_ctx_, sink_tg);
      ARA_RETURN_NOT_OK(result);
      auto tg_result = result.ValueOrDie()->Wait(schedule_ctx_);
      ARA_RETURN_NOT_OK(tg_result);
      ARA_RETURN_IF(!tg_result->IsFinished(),
                    Status::UnknownError("Unexpected result of ScalarAggregate::Sink"));
    }

    {
      auto tgs = sa.Frontend(pipeline_ctx_);
      for (const auto& tg : tgs) {
        auto result = scheduler.Schedule(schedule_ctx_, tg);
        ARA_RETURN_NOT_OK(result);
        auto tg_result = result.ValueOrDie()->Wait(schedule_ctx_);
        ARA_RETURN_NOT_OK(tg_result);
        ARA_RETURN_IF(!tg_result->IsFinished(),
                      Status::UnknownError("Unexpected result of ScalarAggregate::Sink"));
      }
    }

    auto source = sa.ImplicitSource(pipeline_ctx_);
    auto result = source->Source(pipeline_ctx_)(pipeline_ctx_, TaskContext{}, 0);
    ARA_RETURN_IF(!result->IsFinished(),
                  Status::UnknownError("Unexpected result of ScalarAggregate::Sink"));
    return std::move(result);
  }
};

TEST_F(ScalarAggregateTest, Count) {
  arrow::compute::Aggregate agg("count", nullptr, std::vector<arrow::FieldRef>{{0}},
                                "count");
  arrow::acero::AggregateNodeOptions options({std::move(agg)});
  auto input_schema = arrow::schema({arrow::field("i32", arrow::int32())});

  auto result =
      Run(options, input_schema,
          arrow::acero::ExecBatchFromJSON(
              {arrow::int32()}, "[[1], [2], [3], [4], [5], [6], [7], [8], [9], [10]]"));
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());

  auto batch = result->GetBatch();
  ASSERT_TRUE(batch.has_value());
  ASSERT_EQ(batch->length, 1);
  ASSERT_EQ(batch->num_values(), 1);
  ASSERT_TRUE(batch->values[0].is_scalar());
  ASSERT_TRUE(batch->values[0].scalar_as<arrow::Int64Scalar>().is_valid);
  ASSERT_EQ(batch->values[0].scalar_as<arrow::Int64Scalar>().value, 10 * dop_);
}
