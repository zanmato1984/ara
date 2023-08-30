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

namespace pipelang {

using ImperativeInstruction = OpResult;
using ImperativeTrace = std::string;

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

 private:
  std::string name_;
  size_t dop_;

  std::vector<std::shared_ptr<ImperativeSource>> sources_;
  std::vector<ImperativeTrace> traces_;

  friend class ImperativeOp;
};

class ImperativeOp {
 public:
  explicit ImperativeOp(ImperativePipeline* pipeline)
      : pipeline_(pipeline), thread_locals_(pipeline->dop_) {}
  virtual ~ImperativeOp() = default;

  ImperativeOp* GetChild() { return child_.get(); }
  void SetChild(std::shared_ptr<ImperativeOp> child) { child_ = std::move(child); }

 protected:
  void Instruct(ImperativeInstruction instruction, ImperativeTrace trace) {
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
  explicit ImperativeSource(ImperativePipeline* pipeline, std::string name)
      : ImperativeOp(pipeline), SourceOp(std::move(name), "ImperativeSource") {}

  void NotReady() { Instruct(OpOutput::SourceNotReady(), "NotReady"); }
  void HasMore() { Instruct(OpOutput::SourcePipeHasMore({}), "HasMore"); }
  void Finished() { Instruct(OpOutput::Finished(), "Finished"); }

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
  explicit ImperativePipe(ImperativePipeline* pipeline, std::string name)
      : ImperativeOp(pipeline),
        PipeOp(std::move(name), "ImperativePipe"),
        implicit_source_(nullptr),
        pipe_id_(0),
        drain_id_(0) {}

  void SetImplicitSource(std::unique_ptr<ImperativeSource> implicit_source) {
    implicit_source_ = std::move(implicit_source);
  }

  void Even() { Instruct(OpOutput::PipeEven({}), ""); }
  void PipeNeedsMore() { Instruct(OpOutput::PipeSinkNeedsMore(), ""); }
  void PipeHasMore() { Instruct(OpOutput::SourcePipeHasMore({}), ""); }
  void PipeYield() { Instruct(OpOutput::PipeYield(), ""); }
  void PipeFinished() { Instruct(OpOutput::Finished(), ""); }

  void DrainHasMore() { Instruct(OpOutput::SourcePipeHasMore({}), ""); }
  void DrainYield() { Instruct(OpOutput::PipeYield(), ""); }
  void DrainFinished() { Instruct(OpOutput::Finished(), ""); }

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
  explicit ImperativeSink(ImperativePipeline* pipeline, std::string name)
      : ImperativeOp(pipeline), SinkOp(std::move(name), "ImperativeSink"), id_(0) {}

  void NeedsMore() { Instruct(OpOutput::PipeSinkNeedsMore(), ""); }
  void Backpressure(Backpressure backpressure) {
    Instruct(OpOutput::SinkBackpressure(std::move(backpressure)), "");
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
  auto source = std::make_shared<ImperativeSource>(this, std::move(name));
  sources_.emplace_back(source);
  return source.get();
}

ImperativePipe* ImperativePipeline::DeclPipe(std::string name, ImperativeOp* parent) {
  auto pipe = std::make_shared<ImperativePipe>(this, std::move(name));
  parent->SetChild(pipe);
  return pipe.get();
}

ImperativeSink* ImperativePipeline::DeclSink(std::string name, ImperativeOp* parent) {
  auto sink = std::make_shared<ImperativeSink>(this, std::move(name));
  parent->SetChild(sink);
  return sink.get();
}

ImperativeSource* ImperativePipeline::DeclImplicitSource(std::string name,
                                                         ImperativePipe* pipe) {
  auto source = std::make_unique<ImperativeSource>(this, std::move(name));
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

TEST(PipelangTest, EmptySource) {
  ImperativePipeline pipeline{"P"};
  auto source = pipeline.DeclSource("Source");
  auto sink = pipeline.DeclSink("Sink", source);
  source->Finished();

  auto logical = pipeline.ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), "P");
  ASSERT_EQ(logical.Plexes().size(), 1);
  ASSERT_EQ(logical.Plexes()[0].source_op, source);
  ASSERT_TRUE(logical.Plexes()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline.Traces().size(), 1);
}

}  // namespace pipelang

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
