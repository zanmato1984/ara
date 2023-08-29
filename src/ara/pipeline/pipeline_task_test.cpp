#include "pipeline_task.h"
#include "logical_pipeline.h"
#include "op/op.h"
#include "physical_pipeline.h"
#include "pipeline_context.h"
#include "pipeline_observer.h"

#include <ara/schedule/async_dual_pool_scheduler.h>
#include <ara/schedule/schedule_context.h>
#include <ara/schedule/schedule_observer.h>

#include <arrow/testing/gtest_util.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest.h>

using namespace ara;
using namespace ara::pipeline;
using namespace ara::task;
using namespace ara::schedule;

class FooSource : public SourceOp {
 public:
  FooSource() : SourceOp("FooSource", "Do nothing") {}

  PipelineSource Source() override {
    return [](const PipelineContext&, const TaskContext&, ThreadId) -> OpResult {
      return OpOutput::Finished();
    };
  }

  TaskGroups Frontend(const PipelineContext&) override { return {}; }

  std::optional<TaskGroup> Backend(const PipelineContext&) override {
    return std::nullopt;
  }
};

class FooPipe : public PipeOp {
 public:
  FooPipe(std::optional<PipelineDrain> drain = std::nullopt,
          std::unique_ptr<SourceOp> implicit_source = nullptr)
      : PipeOp("FooPipe", "Do nothing"),
        drain_(std::move(drain)),
        implicit_source_(std::move(implicit_source)) {}

  PipelinePipe Pipe() override {
    return [](const PipelineContext&, const TaskContext&, ThreadId,
              std::optional<Batch>) -> OpResult { return OpOutput::PipeEven({}); };
  }

  std::optional<PipelineDrain> Drain() override { return drain_; }

  std::unique_ptr<SourceOp> ImplicitSource() override {
    return std::move(implicit_source_);
  }

 private:
  std::optional<PipelineDrain> drain_;
  std::unique_ptr<SourceOp> implicit_source_;
};

class FooSink : public SinkOp {
 public:
  FooSink() : SinkOp("FooSink", "Do nothing") {}

  PipelineSink Sink() override {
    return [](const PipelineContext&, const TaskContext&, ThreadId,
              std::optional<Batch>) -> OpResult { return OpOutput::PipeSinkNeedsMore(); };
  }

  TaskGroups Frontend(const PipelineContext&) override { return {}; }

  std::optional<TaskGroup> Backend(const PipelineContext&) override {
    return std::nullopt;
  }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }
};

TEST(PipelineTaskTest, TaskBasic) {
  PipelineContext pipeline_context;
  FooSource source;
  FooSink sink;
  LogicalPipeline logical("FooPipeline", {{&source, {}}}, &sink);
  auto pipelines = CompilePipeline(pipeline_context, logical);
  ASSERT_EQ(pipelines.size(), 1);
  size_t dop = 4;
  PipelineTask pipeline_task(pipelines[0], dop);
  Task task(pipeline_task.Name(), pipeline_task.Desc(),
            [&pipeline_context, &pipeline_task](const TaskContext& task_context,
                                                TaskId task_id) -> TaskResult {
              return pipeline_task(pipeline_context, task_context, task_id);
            });
  TaskGroup task_group("FooPipelineTaskGroup", "Do nonthing", std::move(task), dop,
                       std::nullopt, std::nullopt);

  ScheduleContext schedule_context;
  folly::CPUThreadPoolExecutor cpu_executor(4);
  folly::IOThreadPoolExecutor io_executor(2);
  AsyncDualPoolScheduler scheduler(&cpu_executor, &io_executor);

  auto handle = scheduler.Schedule(schedule_context, task_group);
  ASSERT_OK(handle);
  auto result = (*handle)->Wait(schedule_context);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
}
