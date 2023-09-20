#include "logical_pipeline.h"
#include "op/op.h"
#include "op/op_output.h"
#include "physical_pipeline.h"
#include "pipeline_context.h"
#include "pipeline_observer.h"

#include <gtest/gtest.h>

using namespace ara;
using namespace ara::pipeline;
using namespace ara::task;

class FooSource : public SourceOp {
 public:
  FooSource() : SourceOp("FooSource", "Do nothing") {}

  PipelineSource Source(const PipelineContext&) override {
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
  FooPipe(PipelineDrain drain = nullptr,
          std::unique_ptr<SourceOp> implicit_source = nullptr)
      : PipeOp("FooPipe", "Do nothing"),
        drain_(std::move(drain)),
        implicit_source_(std::move(implicit_source)) {}

  PipelinePipe Pipe(const PipelineContext&) override {
    return [](const PipelineContext&, const TaskContext&, ThreadId,
              std::optional<Batch>) -> OpResult { return OpOutput::PipeEven({}); };
  }

  PipelineDrain Drain(const PipelineContext&) override { return drain_; }

  std::unique_ptr<SourceOp> ImplicitSource(const PipelineContext&) override {
    return std::move(implicit_source_);
  }

 private:
  PipelineDrain drain_;
  std::unique_ptr<SourceOp> implicit_source_;
};

class FooSink : public SinkOp {
 public:
  FooSink() : SinkOp("FooSink", "Do nothing") {}

  PipelineSink Sink(const PipelineContext&) override {
    return [](const PipelineContext&, const TaskContext&, ThreadId,
              std::optional<Batch>) -> OpResult { return OpOutput::PipeSinkNeedsMore(); };
  }

  TaskGroups Frontend(const PipelineContext&) override { return {}; }

  std::optional<TaskGroup> Backend(const PipelineContext&) override {
    return std::nullopt;
  }

  std::unique_ptr<SourceOp> ImplicitSource(const PipelineContext&) override {
    return nullptr;
  }
};

TEST(CompilePipelineTest, EmptyPipeline) {
  PipelineContext context;
  FooSink sink;
  LogicalPipeline logical_pipeline("EmptyPipeline", {}, &sink);
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 0);

  ASSERT_TRUE(logical_pipeline.Desc().empty());
}

TEST(CompilePipelineTest, SingleChannelPipeline) {
  PipelineContext context;
  FooSource source;
  FooPipe pipe;
  FooSink sink;
  LogicalPipeline logical_pipeline("SingleChannelPipeline", {{&source, {&pipe}}}, &sink);
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 1);
  ASSERT_TRUE(physical_pipelines[0].ImplicitSources().empty());
  ASSERT_EQ(physical_pipelines[0].Channels().size(), 1);

  std::string desc_exp = "Channel0: FooSource -> FooPipe -> FooSink";
  ASSERT_EQ(logical_pipeline.Desc(), desc_exp);
  ASSERT_EQ(physical_pipelines[0].Desc(), desc_exp);
}

TEST(CompilePipelineTest, DoubleChannelPipeline) {
  PipelineContext context;
  FooSource source1, source2;
  FooPipe pipe;
  FooSink sink;
  LogicalPipeline logical_pipeline("DoubleChannelPipeline",
                                   {{&source1, {&pipe}}, {&source2, {&pipe}}}, &sink);
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 1);
  ASSERT_TRUE(physical_pipelines[0].ImplicitSources().empty());
  ASSERT_EQ(physical_pipelines[0].Channels().size(), 2);

  std::string desc_exp =
      "Channel0: FooSource -> FooPipe -> FooSink\n"
      "Channel1: FooSource -> FooPipe -> FooSink";
  ASSERT_EQ(logical_pipeline.Desc(), desc_exp);
  ASSERT_EQ(physical_pipelines[0].Desc(), desc_exp);
}

TEST(CompilePipelineTest, DoublePhysicalPipeline) {
  PipelineContext context;
  FooSource source;
  auto implicit_source_ptr = std::make_unique<FooSource>();
  auto implicit_source = implicit_source_ptr.get();
  FooPipe pipe(nullptr, std::move(implicit_source_ptr));
  FooSink sink;
  LogicalPipeline logical_pipeline("DoublePhysicalPipeline", {{&source, {&pipe}}}, &sink);
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 2);
  ASSERT_TRUE(physical_pipelines[0].ImplicitSources().empty());
  ASSERT_EQ(physical_pipelines[1].ImplicitSources().size(), 1);
  ASSERT_EQ(physical_pipelines[1].ImplicitSources()[0].get(), implicit_source);
  ASSERT_EQ(physical_pipelines[0].Channels().size(), 1);
  ASSERT_EQ(physical_pipelines[1].Channels().size(), 1);

  std::string logical_desc_exp = "Channel0: FooSource -> FooPipe -> FooSink";
  ASSERT_EQ(logical_pipeline.Desc(), logical_desc_exp);
  std::string physical_desc_exp0 = "Channel0: FooSource -> FooPipe -> FooSink";
  std::string physical_desc_exp1 = "Channel0: FooSource -> FooSink";
  ASSERT_EQ(physical_pipelines[0].Desc(), physical_desc_exp0);
  ASSERT_EQ(physical_pipelines[1].Desc(), physical_desc_exp1);
}

TEST(CompilePipelineTest, DoublePhysicalDoubleChannelPipeline) {
  PipelineContext context;
  FooSource source1, source2;
  auto implicit_source1_ptr = std::make_unique<FooSource>();
  auto implicit_source2_ptr = std::make_unique<FooSource>();
  auto implicit_source1 = implicit_source1_ptr.get();
  auto implicit_source2 = implicit_source2_ptr.get();
  FooPipe pipe1(nullptr, std::move(implicit_source1_ptr)),
      pipe2(nullptr, std::move(implicit_source2_ptr));
  FooSink sink;
  LogicalPipeline logical_pipeline("DoubleDoublePipeline",
                                   {{&source1, {&pipe1}}, {&source2, {&pipe2}}}, &sink);
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 2);
  ASSERT_TRUE(physical_pipelines[0].ImplicitSources().empty());
  ASSERT_EQ(physical_pipelines[1].ImplicitSources().size(), 2);
  ASSERT_EQ(physical_pipelines[1].ImplicitSources()[0].get(), implicit_source1);
  ASSERT_EQ(physical_pipelines[1].ImplicitSources()[1].get(), implicit_source2);
  ASSERT_EQ(physical_pipelines[0].Channels().size(), 2);
  ASSERT_EQ(physical_pipelines[1].Channels().size(), 2);

  std::string logical_desc_exp =
      "Channel0: FooSource -> FooPipe -> FooSink\n"
      "Channel1: FooSource -> FooPipe -> FooSink";
  ASSERT_EQ(logical_pipeline.Desc(), logical_desc_exp);
  std::string physical_desc_exp0 =
      "Channel0: FooSource -> FooPipe -> FooSink\n"
      "Channel1: FooSource -> FooPipe -> FooSink";
  std::string physical_desc_exp1 =
      "Channel0: FooSource -> FooSink\n"
      "Channel1: FooSource -> FooSink";
  ASSERT_EQ(physical_pipelines[0].Desc(), physical_desc_exp0);
  ASSERT_EQ(physical_pipelines[1].Desc(), physical_desc_exp1);
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
  FooPipe pipe1(nullptr, std::move(implicit_source1_ptr)),
      pipe2(nullptr, std::move(implicit_source2_ptr)),
      pipe3(nullptr, std::move(implicit_source3_ptr));
  FooSink sink;
  LogicalPipeline logical_pipeline(
      "TripplePhysicalPipeline",
      {{&source1, {&pipe1, &pipe3}}, {&source2, {&pipe2, &pipe3}}}, &sink);
  auto physical_pipelines = CompilePipeline(context, logical_pipeline);
  ASSERT_EQ(physical_pipelines.size(), 3);
  ASSERT_TRUE(physical_pipelines[0].ImplicitSources().empty());
  ASSERT_EQ(physical_pipelines[1].ImplicitSources().size(), 2);
  ASSERT_EQ(physical_pipelines[2].ImplicitSources().size(), 1);
  ASSERT_EQ(physical_pipelines[1].ImplicitSources()[0].get(), implicit_source1);
  ASSERT_EQ(physical_pipelines[1].ImplicitSources()[1].get(), implicit_source2);
  ASSERT_EQ(physical_pipelines[2].ImplicitSources()[0].get(), implicit_source3);
  ASSERT_EQ(physical_pipelines[0].Channels().size(), 2);
  ASSERT_EQ(physical_pipelines[1].Channels().size(), 2);
  ASSERT_EQ(physical_pipelines[2].Channels().size(), 1);

  std::string logical_desc_exp =
      "Channel0: FooSource -> FooPipe -> FooPipe -> FooSink\n"
      "Channel1: FooSource -> FooPipe -> FooPipe -> FooSink";
  ASSERT_EQ(logical_pipeline.Desc(), logical_desc_exp);
  std::string physical_desc_exp0 =
      "Channel0: FooSource -> FooPipe -> FooPipe -> FooSink\n"
      "Channel1: FooSource -> FooPipe -> FooPipe -> FooSink";
  std::string physical_desc_exp1 =
      "Channel0: FooSource -> FooPipe -> FooSink\n"
      "Channel1: FooSource -> FooPipe -> FooSink";
  std::string physical_desc_exp2 = "Channel0: FooSource -> FooSink";
  ASSERT_EQ(physical_pipelines[0].Desc(), physical_desc_exp0);
  ASSERT_EQ(physical_pipelines[1].Desc(), physical_desc_exp1);
  ASSERT_EQ(physical_pipelines[2].Desc(), physical_desc_exp2);
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
  FooPipe pipe1(nullptr, std::move(implicit_source1_ptr)),
      pipe2(nullptr, std::move(implicit_source2_ptr)),
      pipe3(nullptr, std::move(implicit_source3_ptr)),
      pipe4(nullptr, std::move(implicit_source4_ptr));
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
  ASSERT_EQ(physical_pipelines[1].ImplicitSources()[0].get(), implicit_source1);
  ASSERT_EQ(physical_pipelines[1].ImplicitSources()[1].get(), implicit_source3);
  ASSERT_EQ(physical_pipelines[2].ImplicitSources()[0].get(), implicit_source2);
  ASSERT_EQ(physical_pipelines[3].ImplicitSources()[0].get(), implicit_source4);
  ASSERT_EQ(physical_pipelines[0].Channels().size(), 4);
  ASSERT_EQ(physical_pipelines[1].Channels().size(), 2);
  ASSERT_EQ(physical_pipelines[2].Channels().size(), 1);
  ASSERT_EQ(physical_pipelines[3].Channels().size(), 1);

  std::string logical_desc_exp =
      "Channel0: FooSource -> FooPipe -> FooPipe -> FooPipe -> FooSink\n"
      "Channel1: FooSource -> FooPipe -> FooPipe -> FooSink\n"
      "Channel2: FooSource -> FooPipe -> FooPipe -> FooSink\n"
      "Channel3: FooSource -> FooPipe -> FooSink";
  ASSERT_EQ(logical_pipeline.Desc(), logical_desc_exp);
  std::string physical_desc_exp0 =
      "Channel0: FooSource -> FooPipe -> FooPipe -> FooPipe -> FooSink\n"
      "Channel1: FooSource -> FooPipe -> FooPipe -> FooSink\n"
      "Channel2: FooSource -> FooPipe -> FooPipe -> FooSink\n"
      "Channel3: FooSource -> FooPipe -> FooSink";
  std::string physical_desc_exp1 =
      "Channel0: FooSource -> FooPipe -> FooPipe -> FooSink\n"
      "Channel1: FooSource -> FooPipe -> FooSink";
  std::string physical_desc_exp2 = "Channel0: FooSource -> FooPipe -> FooSink";
  std::string physical_desc_exp3 = "Channel0: FooSource -> FooSink";
  ASSERT_EQ(physical_pipelines[0].Desc(), physical_desc_exp0);
  ASSERT_EQ(physical_pipelines[1].Desc(), physical_desc_exp1);
  ASSERT_EQ(physical_pipelines[2].Desc(), physical_desc_exp2);
  ASSERT_EQ(physical_pipelines[3].Desc(), physical_desc_exp3);
}
