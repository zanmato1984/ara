#include "pipeline_task.h"
#include "logical_pipeline.h"
#include "op/op.h"
#include "physical_pipeline.h"
#include "pipeline_context.h"
#include "pipeline_observer.h"

#include <ara/schedule/async_dual_pool_scheduler.h>
#include <ara/schedule/naive_parallel_scheduler.h>
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

namespace pipelang {

using ImperativeInstruction = OpResult;

struct ImperativeTrace {
  std::string op;
  std::string method;
  std::optional<std::string> payload;

  bool operator==(const ImperativeTrace& other) const {
    return op == other.op && method == other.method && payload == other.payload;
  }
};

class ImperativeOp;
class ImperativeSource;
class ImperativePipe;
class ImperativeSink;

class ImperativePipeline {
 public:
  explicit ImperativePipeline(std::string name, size_t dop = 1)
      : name_(std::move(name)), dop_(dop) {}

  ImperativeSource* DeclSource(std::string);

  ImperativePipe* DeclPipe(std::string, ImperativeOp*);

  ImperativeSink* DeclSink(std::string, ImperativeOp*);

  ImperativeSource* DeclImplicitSource(std::string, ImperativePipe*);

  const std::vector<std::shared_ptr<ImperativeSource>>& Sources() const {
    return sources_;
  }

  const std::vector<ImperativeTrace>& Traces() const { return traces_; }

  LogicalPipeline ToLogicalPipeline() const;

  size_t Dop() const { return dop_; }

 private:
  std::string name_;
  size_t dop_;

  std::vector<std::shared_ptr<ImperativeSource>> sources_;
  std::vector<ImperativeTrace> traces_;

  friend class ImperativeOp;
};

class ImperativeOp : public internal::Meta {
 public:
  explicit ImperativeOp(std::string name, std::string desc, ImperativePipeline* pipeline)
      : Meta(std::move(name), std::move(desc)),
        pipeline_(pipeline),
        thread_locals_(pipeline->dop_) {}
  virtual ~ImperativeOp() = default;

  ImperativeOp* GetChild() { return child_.get(); }
  void SetChild(std::shared_ptr<ImperativeOp> child) { child_ = std::move(child); }

 protected:
  void InstructAndTrace(ImperativeInstruction instruction, std::string method) {
    ImperativeTrace trace{Meta::Name(), std::move(method), instruction->ToString()};
    instructions_.emplace_back(std::move(instruction));
    pipeline_->traces_.emplace_back(std::move(trace));
  }

  ImperativeInstruction Execute(ThreadId thread_id) {
    return instructions_[thread_locals_[thread_id].id++];
  }

 protected:
  ImperativePipeline* pipeline_;
  std::shared_ptr<ImperativeOp> child_;

  std::vector<ImperativeInstruction> instructions_;

  struct ThreadLocal {
    size_t id = 0;
  };
  std::vector<ThreadLocal> thread_locals_;
};

class ImperativeSource : public ImperativeOp, public SourceOp {
 public:
  explicit ImperativeSource(std::string name, ImperativePipeline* pipeline)
      : ImperativeOp(std::move(name), "ImperativeSource", pipeline),
        SourceOp(ImperativeOp::Name(), ImperativeOp::Desc()) {}

  void NotReady() { InstructAndTrace(OpOutput::SourceNotReady(), "Source"); }
  void HasMore() { InstructAndTrace(OpOutput::SourcePipeHasMore({}), "Source"); }
  void Finished() { InstructAndTrace(OpOutput::Finished(), "Source"); }

  PipelineSource Source() override {
    return [&](const PipelineContext&, const TaskContext&,
               ThreadId thread_id) -> OpResult { return Execute(thread_id); };
  }

  TaskGroups Frontend(const PipelineContext&) override { return {}; }

  std::optional<TaskGroup> Backend(const PipelineContext&) override {
    return std::nullopt;
  }
};

class ImperativePipe : public ImperativeOp, public PipeOp {
 public:
  explicit ImperativePipe(std::string name, ImperativePipeline* pipeline)
      : ImperativeOp(std::move(name), "ImperativePipe", pipeline),
        PipeOp(ImperativeOp::Name(), ImperativeOp::Desc()),
        implicit_source_(nullptr),
        pipe_id_(0),
        drain_id_(0) {}

  void SetImplicitSource(std::unique_ptr<ImperativeSource> implicit_source) {
    implicit_source_ = std::move(implicit_source);
  }

  void PipeEven() { InstructAndTrace(OpOutput::PipeEven({}), "Pipe"); }
  void PipeNeedsMore() { InstructAndTrace(OpOutput::PipeSinkNeedsMore(), "Pipe"); }
  void PipeHasMore() { InstructAndTrace(OpOutput::SourcePipeHasMore({}), "Pipe"); }
  void PipeYield() { InstructAndTrace(OpOutput::PipeYield(), "Pipe"); }
  void PipeFinished() { InstructAndTrace(OpOutput::Finished(), "Pipe"); }

  void DrainHasMore() { InstructAndTrace(OpOutput::SourcePipeHasMore({}), "Drain"); }
  void DrainYield() { InstructAndTrace(OpOutput::PipeYield(), "Drain"); }
  void DrainFinished() { InstructAndTrace(OpOutput::Finished(), "Drain"); }

  PipelinePipe Pipe() override {
    return [&](const PipelineContext&, const TaskContext&, ThreadId thread_id,
               std::optional<Batch>) -> OpResult { return Execute(thread_id); };
  }

  std::optional<PipelineDrain> Drain() override {
    if (drain_instructions_.empty()) {
      return std::nullopt;
    }
    return [&](const PipelineContext&, const TaskContext&,
               ThreadId thread_id) -> OpResult { return Execute(thread_id); };
  }

  std::unique_ptr<SourceOp> ImplicitSource() override {
    return std::move(implicit_source_);
  }

 private:
  std::unique_ptr<ImperativeSource> implicit_source_;
  std::vector<ImperativeInstruction> pipe_instructions_, drain_instructions_;
  size_t pipe_id_, drain_id_;
};

class ImperativeSink : public ImperativeOp, public SinkOp {
 public:
  explicit ImperativeSink(std::string name, ImperativePipeline* pipeline)
      : ImperativeOp(std::move(name), "ImperativeSink", pipeline),
        SinkOp(ImperativeOp::Name(), ImperativeOp::Desc()),
        id_(0) {}

  void NeedsMore() { InstructAndTrace(OpOutput::PipeSinkNeedsMore(), "Sink"); }
  void Backpressure(Backpressure backpressure) {
    InstructAndTrace(OpOutput::SinkBackpressure(std::move(backpressure)), "Sink");
  }

  PipelineSink Sink() override {
    return [&](const PipelineContext&, const TaskContext&, ThreadId thread_id,
               std::optional<Batch>) -> OpResult { return Execute(thread_id); };
  }

  TaskGroups Frontend(const PipelineContext&) override { return {}; }

  std::optional<TaskGroup> Backend(const PipelineContext&) override {
    return std::nullopt;
  }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }

 private:
  std::vector<ImperativeInstruction> instructions_;
  size_t id_;
};

ImperativeSource* ImperativePipeline::DeclSource(std::string name) {
  auto source = std::make_shared<ImperativeSource>(std::move(name), this);
  sources_.emplace_back(source);
  return source.get();
}

ImperativePipe* ImperativePipeline::DeclPipe(std::string name, ImperativeOp* parent) {
  auto pipe = std::make_shared<ImperativePipe>(std::move(name), this);
  parent->SetChild(pipe);
  return pipe.get();
}

ImperativeSink* ImperativePipeline::DeclSink(std::string name, ImperativeOp* parent) {
  auto sink = std::make_shared<ImperativeSink>(std::move(name), this);
  parent->SetChild(sink);
  return sink.get();
}

ImperativeSource* ImperativePipeline::DeclImplicitSource(std::string name,
                                                         ImperativePipe* pipe) {
  auto source = std::make_unique<ImperativeSource>(std::move(name), this);
  auto p = source.get();
  pipe->SetImplicitSource(std::move(source));
  return p;
}

LogicalPipeline ImperativePipeline::ToLogicalPipeline() const {
  std::vector<LogicalPipeline::Plex> plexes;
  ImperativeSink* sink = nullptr;
  for (auto& source : sources_) {
    std::vector<PipeOp*> pipe_ops;
    auto op = source->GetChild();
    while (dynamic_cast<ImperativePipe*>(op)) {
      pipe_ops.emplace_back(dynamic_cast<ImperativePipe*>(op));
      op = op->GetChild();
    }
    if (sink == nullptr) {
      ARA_CHECK(sink = dynamic_cast<ImperativeSink*>(op));
    } else {
      ARA_CHECK(sink == dynamic_cast<ImperativeSink*>(op));
    }
    plexes.emplace_back(LogicalPipeline::Plex{source.get(), std::move(pipe_ops)});
  }
  return LogicalPipeline(name_, std::move(plexes), sink);
}

class ImperativeTracer : public PipelineObserver {
 public:
  ImperativeTracer(size_t dop = 1) : traces_(dop) {}

  Status OnPipelineSourceEnd(const PipelineTask& pipeline_task, size_t plex,
                             const PipelineContext&, const task::TaskContext&,
                             ThreadId thread_id, const OpResult& result) override {
    auto source_name = pipeline_task.Pipeline().Plexes()[plex].source_op->Name();
    traces_[thread_id].push_back(
        ImperativeTrace{std::move(source_name), "Source", result->ToString()});
    return Status::OK();
  }

  Status OnPipelinePipeEnd(const PipelineTask& pipeline_task, size_t plex, size_t pipe,
                           const PipelineContext&, const task::TaskContext&,
                           ThreadId thread_id, const OpResult& result) override {
    auto pipe_name = pipeline_task.Pipeline().Plexes()[plex].pipe_ops[pipe]->Name();
    traces_[thread_id].push_back(
        ImperativeTrace{std::move(pipe_name), "Pipe", result->ToString()});
    return Status::OK();
  }

  Status OnPipelineDrainEnd(const PipelineTask& pipeline_task, size_t plex, size_t pipe,
                            const PipelineContext&, const task::TaskContext&,
                            ThreadId thread_id, const OpResult& result) override {
    auto pipe_name = pipeline_task.Pipeline().Plexes()[plex].pipe_ops[pipe]->Name();
    traces_[thread_id].push_back(
        ImperativeTrace{std::move(pipe_name), "Drain", result->ToString()});
    return Status::OK();
  }

  Status OnPipelineSinkEnd(const PipelineTask& pipeline_task, size_t plex,
                           const PipelineContext&, const task::TaskContext&,
                           ThreadId thread_id, const OpResult& result) override {
    auto sink_name = pipeline_task.Pipeline().Plexes()[plex].sink_op->Name();
    traces_[thread_id].push_back(
        ImperativeTrace{std::move(sink_name), "Sink", result->ToString()});
    return Status::OK();
  }

  const std::vector<std::vector<ImperativeTrace>>& Traces() const { return traces_; }

 private:
  std::vector<std::vector<ImperativeTrace>> traces_;
};

}  // namespace pipelang

struct AsyncDualPoolSchedulerHolder {
  folly::CPUThreadPoolExecutor cpu_executor{4};
  folly::IOThreadPoolExecutor io_executor{2};
  AsyncDualPoolScheduler scheduler{&cpu_executor, &io_executor};
};

struct NaiveParallelSchedulerHolder {
  NaiveParallelScheduler scheduler;
};

template <typename SchedulerType>
class PipelineTaskTest : public testing::Test {
 protected:
  void TestTracePipeline(const pipelang::ImperativePipeline& pipeline) {
    auto dop = pipeline.Dop();

    PipelineContext pipeline_context;
    pipelang::ImperativeTracer* tracer = nullptr;
    {
      auto tracer_up = std::make_unique<pipelang::ImperativeTracer>(dop);
      tracer = tracer_up.get();
      std::vector<std::unique_ptr<PipelineObserver>> observers;
      observers.push_back(std::move(tracer_up));
      pipeline_context.pipeline_observer =
          std::make_unique<ChainedObserver<PipelineObserver>>(std::move(observers));
    }

    auto logical_pipeline = pipeline.ToLogicalPipeline();
    auto physical_pipelines = CompilePipeline(pipeline_context, logical_pipeline);
    for (const auto& physical_pipeline : physical_pipelines) {
      PipelineTask pipeline_task(physical_pipeline, dop);
      Task task(pipeline_task.Name(), pipeline_task.Desc(),
                [&pipeline_context, &pipeline_task](const TaskContext& task_context,
                                                    TaskId task_id) -> TaskResult {
                  return pipeline_task(pipeline_context, task_context, task_id);
                });
      TaskGroup task_group(pipeline_task.Name(), pipeline_task.Desc(), std::move(task),
                           dop, std::nullopt, std::nullopt);

      ScheduleContext schedule_context;
      SchedulerType holder;
      auto handle = holder.scheduler.Schedule(schedule_context, task_group);
      ASSERT_OK(handle);
      auto result = (*handle)->Wait(schedule_context);
      ASSERT_OK(result);
      ASSERT_TRUE(result->IsFinished());
    }

    auto& traces_act = tracer->Traces();
    auto& traces_exp = pipeline.Traces();
    for (size_t i = 0; i < dop; ++i) {
      ASSERT_EQ(traces_act[i], traces_exp);
    }
  }
};

using SchedulerTypes =
    ::testing::Types<AsyncDualPoolSchedulerHolder, NaiveParallelSchedulerHolder>;
TYPED_TEST_SUITE(PipelineTaskTest, SchedulerTypes);

void MakeEmptySourcePipeline(size_t dop,
                             std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("EmptySource", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto sink = pipeline.DeclSink("Sink", source);
  source->Finished();

  auto logical = pipeline.ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), "EmptySource");
  ASSERT_EQ(logical.Plexes().size(), 1);
  ASSERT_EQ(logical.Plexes()[0].source_op, source);
  ASSERT_TRUE(logical.Plexes()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline.Traces().size(), 1);
  ASSERT_EQ(
      pipeline.Traces()[0],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, EmptySource) {
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeEmptySourcePipeline(4, pipeline);
  this->TestTracePipeline(*pipeline);
}

void MakeOnePassPipeline(size_t dop,
                         std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("OnePass", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto sink = pipeline.DeclSink("Sink", source);
  source->HasMore();
  sink->NeedsMore();
  source->Finished();

  auto logical = pipeline.ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), "OnePass");
  ASSERT_EQ(logical.Plexes().size(), 1);
  ASSERT_EQ(logical.Plexes()[0].source_op, source);
  ASSERT_TRUE(logical.Plexes()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline.Traces().size(), 3);
  ASSERT_EQ(pipeline.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[1],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[2],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, OnePass) {
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeOnePassPipeline(4, pipeline);
  this->TestTracePipeline(*pipeline);
}

void MakeOnePassWithPipePipeline(size_t dop,
                                 std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("OnePassWithPipe", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto pipe = pipeline.DeclPipe("Pipe", source);
  auto sink = pipeline.DeclSink("Sink", pipe);
  source->HasMore();
  pipe->PipeEven();
  sink->NeedsMore();
  source->Finished();

  auto logical = pipeline.ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), "OnePassWithPipe");
  ASSERT_EQ(logical.Plexes().size(), 1);
  ASSERT_EQ(logical.Plexes()[0].source_op, source);
  ASSERT_EQ(logical.Plexes()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Plexes()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline.Traces().size(), 4);
  ASSERT_EQ(pipeline.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[1],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[3],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, OnePassWithPipe) {
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeOnePassWithPipePipeline(4, pipeline);
  this->TestTracePipeline(*pipeline);
}
