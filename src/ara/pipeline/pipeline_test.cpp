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
  FooPipe() : PipeOp("FooPipe", "Do nothing") {}

  PipelinePipe Pipe() override {
    return [](const PipelineContext&, const TaskContext&, ThreadId,
              std::optional<Batch>) -> OpResult { return OpOutput::PipeEven({}); };
  }

  std::optional<PipelineDrain> Drain() override { return std::nullopt; }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }
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
  LogicalPipeline logical_pipeline{"EmptyPipeline", {}, &sink};
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 0);
}

TEST(CompilePipelineTest, SinglePlexPipeline) {
  PipelineContext context;
  FooSource source;
  FooPipe pipe;
  FooSink sink;
  LogicalPipeline logical_pipeline{"SinglePlexPipeline", {{&source, {&pipe}}}, &sink};
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 1);
  ASSERT_TRUE(physical_pipelines[0].ImplicitSources().empty());
  ASSERT_EQ(physical_pipelines[0].Plexes.size(), 1);
}

// TEST(CompilePipelineTest, DoublePlexPipeline) {
//   size_t dop = 8;
//   InfiniteSource source_1({}), source_2({});
//   IdentityPipe pipe;
//   BlackHoleSink sink;
//   LogicalPipeline pipeline{{{&source_1, {&pipe}}, {&source_2, {&pipe}}}, &sink};
//   auto stages = PipelineStageBuilder(pipeline).Build();
//   ASSERT_EQ(stages.size(), 1);
//   ASSERT_TRUE(stages[0].sources.empty());
//   ASSERT_EQ(stages[0].pipeline.plexes.size(), 2);
// }

// TEST(CompilePipelineTest, DoubleStagePipeline) {
//   size_t dop = 8;
//   InfiniteSource source({});
//   IdentityWithAnotherSourcePipe pipe(std::make_unique<InfiniteSource>(Batch{}));
//   BlackHoleSink sink;
//   LogicalPipeline pipeline{{{&source, {&pipe}}}, &sink};
//   auto stages = PipelineStageBuilder(pipeline).Build();
//   ASSERT_EQ(stages.size(), 2);
//   ASSERT_TRUE(stages[0].sources.empty());
//   ASSERT_EQ(stages[1].sources.size(), 1);
//   ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].sources[0].get()), nullptr);
//   ASSERT_EQ(stages[0].pipeline.plexes.size(), 1);
//   ASSERT_EQ(stages[1].pipeline.plexes.size(), 1);
// }

// TEST(CompilePipelineTest, DoubleStageDoublePlexPipeline) {
//   size_t dop = 8;
//   InfiniteSource source_1({}), source_2({});
//   IdentityWithAnotherSourcePipe pipe_1(std::make_unique<InfiniteSource>(Batch{})),
//       pipe_2(std::make_unique<MemorySource>(std::list<Batch>()));
//   auto pipe_source_1 = pipe_1.source_.get(), pipe_source_2 = pipe_2.source_.get();
//   BlackHoleSink sink;
//   LogicalPipeline pipeline{{{&source_1, {&pipe_1}}, {&source_2, {&pipe_2}}}, &sink};
//   auto stages = PipelineStageBuilder(pipeline).Build();
//   ASSERT_EQ(stages.size(), 2);
//   ASSERT_TRUE(stages[0].sources.empty());
//   ASSERT_EQ(stages[1].sources.size(), 2);
//   if (stages[1].sources[0].get() == pipe_source_1) {
//     ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].sources[0].get()), nullptr);
//     ASSERT_NE(dynamic_cast<MemorySource*>(stages[1].sources[1].get()), nullptr);
//     ASSERT_EQ(stages[1].sources[1].get(), pipe_source_2);
//   } else {
//     ASSERT_NE(dynamic_cast<MemorySource*>(stages[1].sources[0].get()), nullptr);
//     ASSERT_EQ(stages[1].sources[0].get(), pipe_source_2);
//     ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].sources[1].get()), nullptr);
//     ASSERT_EQ(stages[1].sources[1].get(), pipe_source_1);
//   }
//   ASSERT_EQ(stages[0].pipeline.plexes.size(), 2);
//   ASSERT_EQ(stages[1].pipeline.plexes.size(), 2);
// }

// TEST(CompilePipelineTest, TrippleStagePipeline) {
//   size_t dop = 8;
//   InfiniteSource source_1({}), source_2({});
//   IdentityWithAnotherSourcePipe pipe_1(std::make_unique<InfiniteSource>(Batch{})),
//       pipe_2(std::make_unique<MemorySource>(std::list<Batch>())),
//       pipe_3(std::make_unique<DistributedMemorySource>(dop, std::list<Batch>()));
//   auto pipe_source_1 = pipe_1.source_.get(), pipe_source_2 = pipe_2.source_.get(),
//        pipe_source_3 = pipe_3.source_.get();
//   BlackHoleSink sink;
//   LogicalPipeline pipeline{
//       {{&source_1, {&pipe_1, &pipe_3}}, {&source_2, {&pipe_2, &pipe_3}}}, &sink};
//   auto stages = PipelineStageBuilder(pipeline).Build();
//   ASSERT_EQ(stages.size(), 3);
//   ASSERT_TRUE(stages[0].sources.empty());
//   ASSERT_EQ(stages[1].sources.size(), 2);
//   ASSERT_EQ(stages[2].sources.size(), 1);
//   if (stages[1].sources[0].get() == pipe_source_1) {
//     ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].sources[0].get()), nullptr);
//     ASSERT_NE(dynamic_cast<MemorySource*>(stages[1].sources[1].get()), nullptr);
//     ASSERT_EQ(stages[1].sources[1].get(), pipe_source_2);
//   } else {
//     ASSERT_NE(dynamic_cast<MemorySource*>(stages[1].sources[0].get()), nullptr);
//     ASSERT_EQ(stages[1].sources[0].get(), pipe_source_2);
//     ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].sources[1].get()), nullptr);
//     ASSERT_EQ(stages[1].sources[1].get(), pipe_source_1);
//   }
//   ASSERT_NE(dynamic_cast<DistributedMemorySource*>(stages[2].sources[0].get()), nullptr);
//   ASSERT_EQ(stages[2].sources[0].get(), pipe_source_3);
//   ASSERT_EQ(stages[0].pipeline.plexes.size(), 2);
//   ASSERT_EQ(stages[1].pipeline.plexes.size(), 2);
//   ASSERT_EQ(stages[2].pipeline.plexes.size(), 1);
// }

// TEST(CompilePipelineTest, OddQuadroStagePipeline) {
//   size_t dop = 8;
//   InfiniteSource source_1({}), source_2({}), source_3({}), source_4({});
//   IdentityWithAnotherSourcePipe pipe_1_1(std::make_unique<InfiniteSource>(Batch{})),
//       pipe_1_2(std::make_unique<InfiniteSource>(Batch{}));
//   IdentityWithAnotherSourcePipe pipe_2_1(
//       std::make_unique<MemorySource>(std::list<Batch>()));
//   IdentityWithAnotherSourcePipe pipe_3_1(
//       std::make_unique<DistributedMemorySource>(dop, std::list<Batch>()));
//   BlackHoleSink sink;
//   LogicalPipeline pipeline{{{&source_1, {&pipe_1_1, &pipe_2_1, &pipe_3_1}},
//                             {&source_2, {&pipe_2_1, &pipe_3_1}},
//                             {&source_3, {&pipe_1_2, &pipe_3_1}},
//                             {&source_4, {&pipe_3_1}}},
//                            &sink};
//   auto stages = PipelineStageBuilder(pipeline).Build();

//   ASSERT_EQ(stages.size(), 4);
//   ASSERT_TRUE(stages[0].sources.empty());
//   ASSERT_EQ(stages[1].sources.size(), 2);
//   ASSERT_EQ(stages[2].sources.size(), 1);
//   ASSERT_EQ(stages[3].sources.size(), 1);

//   ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].sources[0].get()), nullptr);
//   ASSERT_NE(dynamic_cast<InfiniteSource*>(stages[1].sources[1].get()), nullptr);
//   ASSERT_NE(dynamic_cast<MemorySource*>(stages[2].sources[0].get()), nullptr);
//   ASSERT_NE(dynamic_cast<DistributedMemorySource*>(stages[3].sources[0].get()), nullptr);

//   ASSERT_EQ(stages[0].pipeline.plexes.size(), 4);
//   ASSERT_EQ(stages[1].pipeline.plexes.size(), 2);
//   ASSERT_EQ(stages[2].pipeline.plexes.size(), 1);
//   ASSERT_EQ(stages[3].pipeline.plexes.size(), 1);
// }

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
