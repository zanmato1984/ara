#include "logical_pipeline.h"
#include "op/op.h"
#include "physical_pipeline.h"
#include "pipeline_context.h"
#include "pipeline_observer.h"
#include "pipeline_task.h"

#include <ara/schedule/async_double_pool_scheduler.h>
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

TEST(CompilePipelineTest, EmptyPipeline) {
  PipelineContext context;
  FooSink sink;
  LogicalPipeline logical_pipeline("EmptyPipeline", {}, &sink);
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 0);
}

TEST(CompilePipelineTest, SinglePlexPipeline) {
  PipelineContext context;
  FooSource source;
  FooPipe pipe;
  FooSink sink;
  LogicalPipeline logical_pipeline("SinglePlexPipeline", {{&source, {&pipe}}}, &sink);
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 1);
  ASSERT_TRUE(physical_pipelines[0].ImplicitSources().empty());
  ASSERT_EQ(physical_pipelines[0].Plexes().size(), 1);
}

TEST(CompilePipelineTest, DoublePlexPipeline) {
  PipelineContext context;
  FooSource source1, source2;
  FooPipe pipe;
  FooSink sink;
  LogicalPipeline logical_pipeline("DoublePlexPipeline",
                                   {{&source1, {&pipe}}, {&source2, {&pipe}}}, &sink);
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 1);
  ASSERT_TRUE(physical_pipelines[0].ImplicitSources().empty());
  ASSERT_EQ(physical_pipelines[0].Plexes().size(), 2);
}

TEST(CompilePipelineTest, DoublePhysicalPipeline) {
  PipelineContext context;
  FooSource source;
  auto implicit_source_ptr = std::make_unique<FooSource>();
  auto implicit_source = implicit_source_ptr.get();
  FooPipe pipe(std::nullopt, std::move(implicit_source_ptr));
  FooSink sink;
  LogicalPipeline logical_pipeline("DoublePhysicalPipeline", {{&source, {&pipe}}}, &sink);
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 2);
  ASSERT_TRUE(physical_pipelines[0].ImplicitSources().empty());
  ASSERT_EQ(physical_pipelines[1].ImplicitSources().size(), 1);
  ASSERT_EQ(physical_pipelines[1].ImplicitSources()[0].get(), implicit_source);
  ASSERT_EQ(physical_pipelines[0].Plexes().size(), 1);
  ASSERT_EQ(physical_pipelines[1].Plexes().size(), 1);
}

TEST(CompilePipelineTest, DoublePhysicalDoublePlexPipeline) {
  PipelineContext context;
  FooSource source1, source2;
  auto implicit_source1_ptr = std::make_unique<FooSource>();
  auto implicit_source2_ptr = std::make_unique<FooSource>();
  auto implicit_source1 = implicit_source1_ptr.get();
  auto implicit_source2 = implicit_source2_ptr.get();
  FooPipe pipe1(std::nullopt, std::move(implicit_source1_ptr)),
      pipe2(std::nullopt, std::move(implicit_source2_ptr));
  FooSink sink;
  LogicalPipeline logical_pipeline("DoubleDoublePipeline",
                                   {{&source1, {&pipe1}}, {&source2, {&pipe2}}}, &sink);
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 2);
  ASSERT_TRUE(physical_pipelines[0].ImplicitSources().empty());
  ASSERT_EQ(physical_pipelines[1].ImplicitSources().size(), 2);
  if (physical_pipelines[1].ImplicitSources()[0].get() == implicit_source1) {
    ASSERT_EQ(physical_pipelines[1].ImplicitSources()[1].get(), implicit_source2);
  } else {
    ASSERT_EQ(physical_pipelines[1].ImplicitSources()[0].get(), implicit_source2);
    ASSERT_EQ(physical_pipelines[1].ImplicitSources()[1].get(), implicit_source1);
  }
  ASSERT_EQ(physical_pipelines[0].Plexes().size(), 2);
  ASSERT_EQ(physical_pipelines[1].Plexes().size(), 2);
}

TEST(CompilePipelineTest, TripplePhysicalPipeline) {
  PipelineContext context;
  FooSource source1, source2;
  auto implicit_source1_ptr = std::make_unique<FooSource>();
  auto implicit_source2_ptr = std::make_unique<FooSource>();
  auto implicit_source3_ptr = std::make_unique<FooSource>();
  auto implicit_source1 = implicit_source1_ptr.get();
  auto implicit_source2 = implicit_source2_ptr.get();
  auto implicit_source3 = implicit_source3_ptr.get();
  FooPipe pipe1(std::nullopt, std::move(implicit_source1_ptr)),
      pipe2(std::nullopt, std::move(implicit_source2_ptr)),
      pipe3(std::nullopt, std::move(implicit_source3_ptr));
  FooSink sink;
  LogicalPipeline logical_pipeline(
      "TripplePhysicalPipeline",
      {{&source1, {&pipe1, &pipe3}}, {&source2, {&pipe2, &pipe3}}}, &sink);
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 3);
  ASSERT_TRUE(physical_pipelines[0].ImplicitSources().empty());
  ASSERT_EQ(physical_pipelines[1].ImplicitSources().size(), 2);
  ASSERT_EQ(physical_pipelines[2].ImplicitSources().size(), 1);
  if (physical_pipelines[1].ImplicitSources()[0].get() == implicit_source1) {
    ASSERT_EQ(physical_pipelines[1].ImplicitSources()[1].get(), implicit_source2);
  } else {
    ASSERT_EQ(physical_pipelines[1].ImplicitSources()[0].get(), implicit_source2);
    ASSERT_EQ(physical_pipelines[1].ImplicitSources()[1].get(), implicit_source1);
  }
  ASSERT_EQ(physical_pipelines[2].ImplicitSources()[0].get(), implicit_source3);
  ASSERT_EQ(physical_pipelines[0].Plexes().size(), 2);
  ASSERT_EQ(physical_pipelines[1].Plexes().size(), 2);
  ASSERT_EQ(physical_pipelines[2].Plexes().size(), 1);
}

TEST(CompilePipelineTest, OddQuadroStagePipeline) {
  PipelineContext context;
  FooSource source1, source2, source3, source4;
  auto implicit_source1_ptr = std::make_unique<FooSource>();
  auto implicit_source2_ptr = std::make_unique<FooSource>();
  auto implicit_source3_ptr = std::make_unique<FooSource>();
  auto implicit_source4_ptr = std::make_unique<FooSource>();
  auto implicit_source1 = implicit_source1_ptr.get();
  auto implicit_source2 = implicit_source2_ptr.get();
  auto implicit_source3 = implicit_source3_ptr.get();
  auto implicit_source4 = implicit_source4_ptr.get();
  FooPipe pipe1(std::nullopt, std::move(implicit_source1_ptr)),
      pipe2(std::nullopt, std::move(implicit_source2_ptr)),
      pipe3(std::nullopt, std::move(implicit_source3_ptr)),
      pipe4(std::nullopt, std::move(implicit_source4_ptr));
  FooSink sink;
  LogicalPipeline logical_pipeline("OddQuadroStagePipeline",
                                   {{&source1, {&pipe1, &pipe2, &pipe4}},
                                    {&source2, {&pipe2, &pipe4}},
                                    {&source3, {&pipe3, &pipe4}},
                                    {&source4, {&pipe4}}},
                                   &sink);
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 4);
  ASSERT_TRUE(physical_pipelines[0].ImplicitSources().empty());
  ASSERT_EQ(physical_pipelines[1].ImplicitSources().size(), 2);
  ASSERT_EQ(physical_pipelines[2].ImplicitSources().size(), 1);
  ASSERT_EQ(physical_pipelines[3].ImplicitSources().size(), 1);

  if (physical_pipelines[1].ImplicitSources()[0].get() == implicit_source1) {
    ASSERT_EQ(physical_pipelines[1].ImplicitSources()[1].get(), implicit_source3);
  } else {
    ASSERT_EQ(physical_pipelines[1].ImplicitSources()[0].get(), implicit_source3);
    ASSERT_EQ(physical_pipelines[1].ImplicitSources()[1].get(), implicit_source1);
  }

  ASSERT_EQ(physical_pipelines[0].Plexes().size(), 4);
  ASSERT_EQ(physical_pipelines[1].Plexes().size(), 2);
  ASSERT_EQ(physical_pipelines[2].Plexes().size(), 1);
  ASSERT_EQ(physical_pipelines[3].Plexes().size(), 1);
}

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
  AsyncDoublePoolScheduler scheduler(&cpu_executor, &io_executor);

  auto handle = scheduler.Schedule(schedule_context, task_group);
  ASSERT_OK(handle);
  auto result = (*handle)->Wait(schedule_context);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished());
}
