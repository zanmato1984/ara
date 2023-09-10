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

std::string TaskName(const std::string& pipeline_name, size_t id = 0) {
  return "Task of PhysicalPipeline" + std::to_string(id) + "(" + pipeline_name + ")";
}

namespace pipelang {

using ImperativeInstruction = OpResult;

struct ImperativeTrace {
  std::string entity;
  std::string method;
  std::string payload;

  bool operator==(const ImperativeTrace& other) const {
    return entity == other.entity && method == other.method && payload == other.payload;
  }

  friend void PrintTo(const ImperativeTrace& trace, std::ostream* os) {
    *os << trace.entity << "::" << trace.method << "(" << trace.payload << ")";
  }
};

class ImperativeOp;
class ImperativeSource;
class ImperativePipe;
class ImperativeSink;

class ImperativeContext {
 public:
  void Trace(ImperativeTrace trace) { traces_.push_back(std::move(trace)); }

  void IncreaseProgramCounter() { ++pc_; }

  const std::vector<ImperativeTrace>& Traces() const { return traces_; }

  size_t ProgramCounter() const { return pc_; }

 private:
  std::vector<ImperativeTrace> traces_;
  size_t pc_ = 0;
};

class ImperativePipeline {
 public:
  explicit ImperativePipeline(std::string name, size_t dop = 1)
      : name_(std::move(name)), dop_(dop), thread_locals_(dop) {}

  ImperativeSource* DeclSource(std::string);

  ImperativePipe* DeclPipe(std::string, std::initializer_list<ImperativeOp*>);

  ImperativeSink* DeclSink(std::string, std::initializer_list<ImperativeOp*>);

  ImperativeSource* DeclImplicitSource(std::string, ImperativePipe*);

  void Resume(ImperativeContext& context, std::string op_name) {
    resume_instructions_.emplace(context.ProgramCounter(), std::move(op_name));
  }

  void ChannelFinished(ImperativeContext& context, size_t task_id = 0) {
    context.Trace(ImperativeTrace{TaskName(name_, task_id), "Run",
                                  OpOutput::Finished().ToString()});
  }

  LogicalPipeline ToLogicalPipeline() const;

  const std::string& Name() const { return name_; }
  size_t Dop() const { return dop_; }

 private:
  ImperativeInstruction Execute(std::string op_name, const TaskContext& task_context,
                                ThreadId thread_id, ImperativeInstruction instruction) {
    ImperativeInstruction result;

    if (instruction.ok() && instruction->IsBlocked()) {
      ARA_CHECK(task_context.resumer_factory != nullptr);
      ARA_ASSIGN_OR_RAISE(auto resumer, task_context.resumer_factory());
      ARA_CHECK(thread_locals_[thread_id].resumers.count(op_name) == 0);
      thread_locals_[thread_id].resumers.emplace(std::move(op_name), resumer);
      result = OpOutput::Blocked(std::move(resumer));
    } else {
      result = std::move(instruction);
    }

    size_t pc = ++thread_locals_[thread_id].pc;

    if (resume_instructions_.count(pc) != 0) {
      const auto& op_to_resume = resume_instructions_[pc];
      ARA_CHECK(thread_locals_[thread_id].resumers.count(op_to_resume) != 0);
      auto resumer = thread_locals_[thread_id].resumers[op_to_resume];
      thread_locals_[thread_id].resumers.erase(op_to_resume);
      ARA_CHECK(resumer != nullptr);
      resumer->Resume();
    }

    return result;
  }

 private:
  std::string name_;
  size_t dop_;

  std::vector<std::shared_ptr<ImperativeSource>> sources_;
  std::unordered_map<size_t, std::string> resume_instructions_;

  struct ThreadLocal {
    std::unordered_map<std::string, ResumerPtr> resumers;
    size_t pc = 0;
  };
  std::vector<ThreadLocal> thread_locals_;

  friend class ImperativeOp;
};

class ImperativeOp : public internal::Meta {
 public:
  explicit ImperativeOp(std::string name, std::string desc, ImperativePipeline* pipeline)
      : Meta(std::move(name), std::move(desc)),
        pipeline_(pipeline),
        thread_locals_(pipeline->Dop()) {}
  virtual ~ImperativeOp() = default;

  ImperativeOp* GetChild() { return child_.get(); }
  void SetChild(std::shared_ptr<ImperativeOp> child) { child_ = std::move(child); }

 protected:
  void OpInstructAndTrace(ImperativeContext& context, ImperativeInstruction instruction,
                          std::string method) {
    ImperativeTrace trace{Meta::Name(), std::move(method), instruction->ToString()};
    instructions_.emplace_back(std::move(instruction));
    context.IncreaseProgramCounter();
    context.Trace(std::move(trace));
  }

  void PipelineTrace(ImperativeContext& context, ImperativeInstruction instruction,
                     size_t pipeline_id = 0) {
    context.Trace(ImperativeTrace{TaskName(pipeline_->Name(), pipeline_id), "Run",
                                  instruction->ToString()});
  }

  ImperativeInstruction Fetch(ThreadId thread_id) {
    return instructions_[thread_locals_[thread_id].pc++];
  }

  ImperativeInstruction Execute(const TaskContext& task_context, ThreadId thread_id,
                                ImperativeInstruction instruction) {
    return pipeline_->Execute(Meta::Name(), task_context, thread_id,
                              std::move(instruction));
  }

 protected:
  ImperativePipeline* pipeline_;
  std::shared_ptr<ImperativeOp> child_;

  std::vector<ImperativeInstruction> instructions_;

  struct ThreadLocal {
    size_t pc = 0;
  };
  std::vector<ThreadLocal> thread_locals_;
};

class ImperativeSource : public ImperativeOp, public SourceOp {
 public:
  explicit ImperativeSource(std::string name, ImperativePipeline* pipeline)
      : ImperativeOp(std::move(name), "ImperativeSource", pipeline),
        SourceOp(ImperativeOp::Name(), ImperativeOp::Desc()) {}

  void HasMore(ImperativeContext& context) {
    OpInstructAndTrace(context, OpOutput::SourcePipeHasMore(Batch{}), "Source");
  }
  void Blocked(ImperativeContext& context, size_t task_id = 0) {
    OpInstructAndTrace(context, OpOutput::Blocked(nullptr), "Source");
    PipelineTrace(context, OpOutput::Blocked(nullptr), task_id);
  }
  void Finished(ImperativeContext& context, std::optional<Batch> batch = std::nullopt) {
    OpInstructAndTrace(context, OpOutput::Finished(std::move(batch)), "Source");
  }

  PipelineSource Source() override {
    return [&](const PipelineContext&, const TaskContext& task_context,
               ThreadId thread_id) -> OpResult {
      auto instruction = Fetch(thread_id);
      return Execute(task_context, thread_id, std::move(instruction));
    };
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
        has_drain_(false) {}

  void SetImplicitSource(std::unique_ptr<ImperativeSource> implicit_source) {
    implicit_source_ = std::move(implicit_source);
  }

  void PipeEven(ImperativeContext& context) {
    OpInstructAndTrace(context, OpOutput::PipeEven(Batch{}), "Pipe");
  }
  void PipeNeedsMore(ImperativeContext& context, size_t task_id = 0) {
    OpInstructAndTrace(context, OpOutput::PipeSinkNeedsMore(), "Pipe");
    PipelineTrace(context, OpOutput::PipeSinkNeedsMore(), task_id);
  }
  void PipeHasMore(ImperativeContext& context) {
    OpInstructAndTrace(context, OpOutput::SourcePipeHasMore(Batch{}), "Pipe");
  }
  void PipeYield(ImperativeContext& context, size_t task_id = 0) {
    OpInstructAndTrace(context, OpOutput::PipeYield(), "Pipe");
    PipelineTrace(context, OpOutput::PipeYield(), task_id);
  }
  void PipeYieldBack(ImperativeContext& context, size_t task_id = 0) {
    OpInstructAndTrace(context, OpOutput::PipeYieldBack(), "Pipe");
    PipelineTrace(context, OpOutput::PipeYieldBack());
  }
  void PipeBlocked(ImperativeContext& context, size_t task_id = 0) {
    OpInstructAndTrace(context, OpOutput::Blocked(nullptr), "Pipe");
    PipelineTrace(context, OpOutput::Blocked(nullptr), task_id);
  }

  void DrainHasMore(ImperativeContext& context) {
    has_drain_ = true;
    OpInstructAndTrace(context, OpOutput::SourcePipeHasMore(Batch{}), "Drain");
  }
  void DrainYield(ImperativeContext& context, size_t task_id = 0) {
    has_drain_ = true;
    OpInstructAndTrace(context, OpOutput::PipeYield(), "Drain");
    PipelineTrace(context, OpOutput::PipeYield(), task_id);
  }
  void DrainYieldBack(ImperativeContext& context, size_t task_id = 0) {
    has_drain_ = true;
    OpInstructAndTrace(context, OpOutput::PipeYieldBack(), "Drain");
    PipelineTrace(context, OpOutput::PipeYieldBack(), task_id);
  }
  void DrainBlocked(ImperativeContext& context, size_t task_id = 0) {
    has_drain_ = true;
    OpInstructAndTrace(context, OpOutput::Blocked(nullptr), "Drain");
    PipelineTrace(context, OpOutput::Blocked(nullptr), task_id);
  }
  void DrainFinished(ImperativeContext& context,
                     std::optional<Batch> batch = std::nullopt) {
    has_drain_ = true;
    OpInstructAndTrace(context, OpOutput::Finished(std::move(batch)), "Drain");
  }

  PipelinePipe Pipe() override {
    return [&](const PipelineContext&, const TaskContext& task_context,
               ThreadId thread_id, std::optional<Batch>) -> OpResult {
      auto instruction = Fetch(thread_id);
      return Execute(task_context, thread_id, std::move(instruction));
    };
  }

  PipelineDrain Drain() override {
    if (!has_drain_) {
      return nullptr;
    }
    return [&](const PipelineContext&, const TaskContext& task_context,
               ThreadId thread_id) -> OpResult {
      auto instruction = Fetch(thread_id);
      return Execute(task_context, thread_id, std::move(instruction));
    };
  }

  std::unique_ptr<SourceOp> ImplicitSource() override {
    return std::move(implicit_source_);
  }

 private:
  std::unique_ptr<ImperativeSource> implicit_source_;
  bool has_drain_;
};

class ImperativeSink : public ImperativeOp, public SinkOp {
 public:
  explicit ImperativeSink(std::string name, ImperativePipeline* pipeline)
      : ImperativeOp(std::move(name), "ImperativeSink", pipeline),
        SinkOp(ImperativeOp::Name(), ImperativeOp::Desc()) {}

  void NeedsMore(ImperativeContext& context, size_t task_id = 0) {
    OpInstructAndTrace(context, OpOutput::PipeSinkNeedsMore(), "Sink");
    PipelineTrace(context, OpOutput::PipeSinkNeedsMore(), task_id);
  }
  void Blocked(ImperativeContext& context, size_t task_id = 0) {
    OpInstructAndTrace(context, OpOutput::Blocked(nullptr), "Sink");
    PipelineTrace(context, OpOutput::Blocked(nullptr), task_id);
  }

  PipelineSink Sink() override {
    return [&](const PipelineContext&, const TaskContext& task_context,
               ThreadId thread_id, std::optional<Batch>) -> OpResult {
      auto instruction = Fetch(thread_id);
      return Execute(task_context, thread_id, std::move(instruction));
    };
  }

  TaskGroups Frontend(const PipelineContext&) override { return {}; }

  std::optional<TaskGroup> Backend(const PipelineContext&) override {
    return std::nullopt;
  }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }
};

ImperativeSource* ImperativePipeline::DeclSource(std::string name) {
  auto source = std::make_shared<ImperativeSource>(std::move(name), this);
  sources_.emplace_back(source);
  return source.get();
}

ImperativePipe* ImperativePipeline::DeclPipe(
    std::string name, std::initializer_list<ImperativeOp*> parents) {
  auto pipe = std::make_shared<ImperativePipe>(std::move(name), this);
  for (auto parent : parents) {
    parent->SetChild(pipe);
  }
  return pipe.get();
}

ImperativeSink* ImperativePipeline::DeclSink(
    std::string name, std::initializer_list<ImperativeOp*> parents) {
  auto sink = std::make_shared<ImperativeSink>(std::move(name), this);
  for (auto parent : parents) {
    parent->SetChild(sink);
  }
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
  std::vector<LogicalPipeline::Channel> channels;
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
    channels.emplace_back(LogicalPipeline::Channel{source.get(), std::move(pipe_ops)});
  }
  return LogicalPipeline(name_, std::move(channels), sink);
}

class ImperativeTracer : public PipelineObserver {
 public:
  ImperativeTracer(size_t dop = 1) : traces_(dop) {}

  Status OnPipelineTaskEnd(const PipelineTask& pipeline_task, size_t channel,
                           const PipelineContext&, const task::TaskContext&,
                           ThreadId thread_id, const OpResult& result) override {
    traces_[thread_id].push_back(
        ImperativeTrace{pipeline_task.Name(), "Run", result->ToString()});
    return Status::OK();
  }

  Status OnPipelineSourceEnd(const PipelineTask& pipeline_task, size_t channel,
                             const PipelineContext&, const TaskContext&,
                             ThreadId thread_id, const OpResult& result) override {
    auto source_name = pipeline_task.Pipeline().Channels()[channel].source_op->Name();
    traces_[thread_id].push_back(
        ImperativeTrace{std::move(source_name), "Source", result->ToString()});
    return Status::OK();
  }

  Status OnPipelinePipeEnd(const PipelineTask& pipeline_task, size_t channel, size_t pipe,
                           const PipelineContext&, const TaskContext&, ThreadId thread_id,
                           const OpResult& result) override {
    auto pipe_name = pipeline_task.Pipeline().Channels()[channel].pipe_ops[pipe]->Name();
    traces_[thread_id].push_back(
        ImperativeTrace{std::move(pipe_name), "Pipe", result->ToString()});
    return Status::OK();
  }

  Status OnPipelineDrainEnd(const PipelineTask& pipeline_task, size_t channel,
                            size_t pipe, const PipelineContext&, const TaskContext&,
                            ThreadId thread_id, const OpResult& result) override {
    auto pipe_name = pipeline_task.Pipeline().Channels()[channel].pipe_ops[pipe]->Name();
    traces_[thread_id].push_back(
        ImperativeTrace{std::move(pipe_name), "Drain", result->ToString()});
    return Status::OK();
  }

  Status OnPipelineSinkEnd(const PipelineTask& pipeline_task, size_t channel,
                           const PipelineContext&, const TaskContext&, ThreadId thread_id,
                           const OpResult& result) override {
    auto sink_name = pipeline_task.Pipeline().Channels()[channel].sink_op->Name();
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
  void TestTracePipeline(const pipelang::ImperativeContext& context,
                         const pipelang::ImperativePipeline& pipeline) {
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

    ScheduleContext schedule_context;
    SchedulerType holder;
    Scheduler& scheduler = holder.scheduler;

    auto logical_pipeline = pipeline.ToLogicalPipeline();
    auto physical_pipelines = CompilePipeline(pipeline_context, logical_pipeline);

    ASSERT_TRUE(logical_pipeline.SinkOp()->Frontend(pipeline_context).empty());
    ASSERT_FALSE(logical_pipeline.SinkOp()->Backend(pipeline_context).has_value());

    for (const auto& physical_pipeline : physical_pipelines) {
      PipelineTask pipeline_task(physical_pipeline, dop);

      for (const auto& channel : physical_pipeline.Channels()) {
        ASSERT_TRUE(channel.source_op->Frontend(pipeline_context).empty());
        ASSERT_TRUE(!channel.source_op->Backend(pipeline_context).has_value());
      }

      Task task(pipeline_task.Name(), pipeline_task.Desc(),
                [&pipeline_context, &pipeline_task](const TaskContext& task_context,
                                                    TaskId task_id) -> TaskResult {
                  return pipeline_task(pipeline_context, task_context, task_id);
                });
      TaskGroup task_group(pipeline_task.Name(), pipeline_task.Desc(), std::move(task),
                           dop, std::nullopt, nullptr);

      auto handle = scheduler.Schedule(schedule_context, task_group);
      ASSERT_OK(handle);
      auto result = (*handle)->Wait(schedule_context);
      ASSERT_OK(result);
      ASSERT_TRUE(result->IsFinished());
    }

    auto& traces_act = tracer->Traces();
    auto& traces_exp = context.Traces();
    for (size_t i = 0; i < dop; ++i) {
      ASSERT_EQ(traces_act[i].size(), traces_exp.size());
      for (size_t j = 0; j < traces_exp.size(); ++j) {
        ASSERT_EQ(traces_act[i][j], traces_exp[j])
            << "thread_id=" << i << ", trace_id=" << j;
      }
    }
  }
};

using SchedulerTypes =
    ::testing::Types<AsyncDualPoolSchedulerHolder, NaiveParallelSchedulerHolder>;
TYPED_TEST_SUITE(PipelineTaskTest, SchedulerTypes);

void MakeEmptySourcePipeline(pipelang::ImperativeContext& context, size_t dop,
                             std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "EmptySource";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto sink = pipeline->DeclSink("Sink", {source});
  source->Finished(context);
  pipeline->ChannelFinished(context);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(context.Traces().size(), 2);
  ASSERT_EQ(
      context.Traces()[0],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(context.Traces()[1],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, EmptySource) {
  pipelang::ImperativeContext context;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeEmptySourcePipeline(context, 4, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeEmptySourceNotReadyPipeline(
    pipelang::ImperativeContext& context, size_t dop,
    std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "EmptySourceNotReady";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto sink = pipeline->DeclSink("Sink", {source});
  source->Blocked(context);
  pipeline->Resume(context, "Source");
  source->Finished(context);
  pipeline->ChannelFinished(context);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(context.Traces().size(), 4);
  ASSERT_EQ(context.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(context.Traces()[1],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(
      context.Traces()[2],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(context.Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, EmptySourceNotReady) {
  pipelang::ImperativeContext context;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeEmptySourceNotReadyPipeline(context, 1, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeTwoSourcesOneNotReadyPipeline(
    pipelang::ImperativeContext& context, size_t dop,
    std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "TwoSourceOneNotReady";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source1 = pipeline->DeclSource("Source1");
  auto source2 = pipeline->DeclSource("Source2");
  auto sink = pipeline->DeclSink("Sink", {source1, source2});
  source1->Blocked(context);
  source2->Finished(context);
  pipeline->ChannelFinished(context);
  pipeline->Resume(context, "Source1");
  source1->Finished(context);
  pipeline->ChannelFinished(context);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 2);
  ASSERT_EQ(logical.Channels()[0].source_op, source1);
  ASSERT_EQ(logical.Channels()[1].source_op, source2);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(context.Traces().size(), 6);
  ASSERT_EQ(context.Traces()[0],
            (pipelang::ImperativeTrace{"Source1", "Source",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(context.Traces()[1],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(
      context.Traces()[2],
      (pipelang::ImperativeTrace{"Source2", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(context.Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
  ASSERT_EQ(
      context.Traces()[4],
      (pipelang::ImperativeTrace{"Source1", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(context.Traces()[5],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, TwoSourceOneNotReady) {
  pipelang::ImperativeContext context;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeTwoSourcesOneNotReadyPipeline(context, 4, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeOnePassPipeline(pipelang::ImperativeContext& context, size_t dop,
                         std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "OnePass";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto sink = pipeline->DeclSink("Sink", {source});
  source->HasMore(context);
  sink->NeedsMore(context);
  source->Finished(context);
  pipeline->ChannelFinished(context);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(context.Traces().size(), 5);
  ASSERT_EQ(context.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(context.Traces()[1],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[2],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      context.Traces()[3],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(context.Traces()[4],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, OnePass) {
  pipelang::ImperativeContext context;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeOnePassPipeline(context, 4, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeOnePassDirectFinishPipeline(
    pipelang::ImperativeContext& context, size_t dop,
    std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "OnePassDirectFinish";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto sink = pipeline->DeclSink("Sink", {source});
  source->Finished(context, Batch{});
  sink->NeedsMore(context);
  pipeline->ChannelFinished(context);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(context.Traces().size(), 4);
  ASSERT_EQ(
      context.Traces()[0],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(context.Traces()[1],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[2],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, OnePassDirectFinish) {
  pipelang::ImperativeContext context;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeOnePassDirectFinishPipeline(context, 4, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeOnePassWithPipePipeline(pipelang::ImperativeContext& context, size_t dop,
                                 std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "OnePassWithPipe";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore(context);
  pipe->PipeEven(context);
  sink->NeedsMore(context);
  source->Finished(context);
  pipeline->ChannelFinished(context);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(context.Traces().size(), 6);
  ASSERT_EQ(context.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(context.Traces()[1], (pipelang::ImperativeTrace{
                                     "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(context.Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      context.Traces()[4],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(context.Traces()[5],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, OnePassWithPipe) {
  pipelang::ImperativeContext context;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeOnePassWithPipePipeline(context, 4, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakePipeNeedsMorePipeline(pipelang::ImperativeContext& context, size_t dop,
                               std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "PipeNeedsMore";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore(context);
  pipe->PipeNeedsMore(context);
  source->Finished(context, Batch{});
  pipe->PipeEven(context);
  sink->NeedsMore(context);
  pipeline->ChannelFinished(context);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(context.Traces().size(), 8);
  ASSERT_EQ(context.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(context.Traces()[1],
            (pipelang::ImperativeTrace{"Pipe", "Pipe",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[2],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      context.Traces()[3],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(context.Traces()[4], (pipelang::ImperativeTrace{
                                     "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(context.Traces()[5],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[6],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[7],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, PipeNeedsMore) {
  pipelang::ImperativeContext context;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakePipeNeedsMorePipeline(context, 4, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakePipeHasMorePipeline(pipelang::ImperativeContext& context, size_t dop,
                             std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "PipeHasMore";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore(context);
  pipe->PipeHasMore(context);
  sink->NeedsMore(context);
  pipe->PipeEven(context);
  sink->NeedsMore(context);
  source->Finished(context);
  pipeline->ChannelFinished(context);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(context.Traces().size(), 9);
  ASSERT_EQ(context.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(context.Traces()[1],
            (pipelang::ImperativeTrace{"Pipe", "Pipe",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(context.Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[4], (pipelang::ImperativeTrace{
                                     "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(context.Traces()[5],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[6],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      context.Traces()[7],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(context.Traces()[8],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, PipeHasMore) {
  pipelang::ImperativeContext context;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakePipeHasMorePipeline(context, 4, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakePipeYieldPipeline(pipelang::ImperativeContext& context, size_t dop,
                           std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "PipeYield";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore(context);
  pipe->PipeYield(context);
  pipe->PipeYieldBack(context);
  pipe->PipeNeedsMore(context);
  source->Finished(context);
  pipeline->ChannelFinished(context);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(context.Traces().size(), 9);
  ASSERT_EQ(context.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(context.Traces()[1], (pipelang::ImperativeTrace{
                                     "Pipe", "Pipe", OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(context.Traces()[2],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(
      context.Traces()[3],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(context.Traces()[4],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(context.Traces()[5],
            (pipelang::ImperativeTrace{"Pipe", "Pipe",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[6],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      context.Traces()[7],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(context.Traces()[8],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, PipeYield) {
  pipelang::ImperativeContext context;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakePipeYieldPipeline(context, 4, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeDrainPipeline(pipelang::ImperativeContext& context, size_t dop,
                       std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "Drain";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore(context);
  pipe->PipeEven(context);
  sink->NeedsMore(context);
  source->Finished(context, Batch{});
  pipe->PipeEven(context);
  sink->NeedsMore(context);
  pipe->DrainHasMore(context);
  sink->NeedsMore(context);
  pipe->DrainYield(context);
  pipe->DrainYieldBack(context);
  pipe->DrainHasMore(context);
  sink->NeedsMore(context);
  pipe->DrainFinished(context, Batch{});
  sink->NeedsMore(context);
  pipeline->ChannelFinished(context);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(context.Traces().size(), 22);
  ASSERT_EQ(context.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(context.Traces()[1], (pipelang::ImperativeTrace{
                                     "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(context.Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      context.Traces()[4],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(context.Traces()[5], (pipelang::ImperativeTrace{
                                     "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(context.Traces()[6],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[7],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[8],
            (pipelang::ImperativeTrace{"Pipe", "Drain",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(context.Traces()[9],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[10],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      context.Traces()[11],
      (pipelang::ImperativeTrace{"Pipe", "Drain", OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(context.Traces()[12],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(
      context.Traces()[13],
      (pipelang::ImperativeTrace{"Pipe", "Drain", OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(context.Traces()[14],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(context.Traces()[15],
            (pipelang::ImperativeTrace{"Pipe", "Drain",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(context.Traces()[16],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[17],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      context.Traces()[18],
      (pipelang::ImperativeTrace{"Pipe", "Drain", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(context.Traces()[19],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[20],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[21],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, Drain) {
  pipelang::ImperativeContext context;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeDrainPipeline(context, 4, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeImplicitSourcePipeline(pipelang::ImperativeContext& context, size_t dop,
                                std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "ImplicitSource";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto implicit_source = pipeline->DeclImplicitSource("ImplicitSource", pipe);
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->Finished(context, Batch{});
  pipe->PipeEven(context);
  sink->NeedsMore(context);
  pipeline->ChannelFinished(context);
  implicit_source->Finished(context, Batch{});
  sink->NeedsMore(context, 1);
  pipeline->ChannelFinished(context, 1);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(context.Traces().size(), 9);
  ASSERT_EQ(
      context.Traces()[0],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(context.Traces()[1], (pipelang::ImperativeTrace{
                                     "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(context.Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[4],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
  ASSERT_EQ(context.Traces()[5],
            (pipelang::ImperativeTrace{"ImplicitSource", "Source",
                                       OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(context.Traces()[6],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[7],
            (pipelang::ImperativeTrace{TaskName(name, 1), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[8],
            (pipelang::ImperativeTrace{TaskName(name, 1), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, ImplicitSource) {
  pipelang::ImperativeContext context;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeImplicitSourcePipeline(context, 4, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeBackpressurePipeline(pipelang::ImperativeContext& context, size_t dop,
                              std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "Backpressure";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore(context);
  pipe->PipeEven(context);
  sink->Blocked(context);
  pipeline->Resume(context, "Sink");
  sink->Blocked(context);
  pipeline->Resume(context, "Sink");
  sink->NeedsMore(context);
  source->Finished(context, Batch{});
  pipe->PipeEven(context);
  sink->NeedsMore(context);
  pipeline->ChannelFinished(context);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(context.Traces().size(), 13);
  ASSERT_EQ(context.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(context.Traces()[1], (pipelang::ImperativeTrace{
                                     "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(
      context.Traces()[2],
      (pipelang::ImperativeTrace{"Sink", "Sink", OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(context.Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(
      context.Traces()[4],
      (pipelang::ImperativeTrace{"Sink", "Sink", OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(context.Traces()[5],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(context.Traces()[6],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[7],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      context.Traces()[8],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(context.Traces()[9], (pipelang::ImperativeTrace{
                                     "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(context.Traces()[10],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[11],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(context.Traces()[12],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, Backpressure) {
  pipelang::ImperativeContext context;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeBackpressurePipeline(context, 4, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

TYPED_TEST(PipelineTaskTest, MultiPipe) {
  pipelang::ImperativeContext context;
  size_t dop = 4;
  auto name = "MultiPipe";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe1 = pipeline->DeclPipe("Pipe1", {source});
  auto pipe2 = pipeline->DeclPipe("Pipe2", {pipe1});
  auto sink = pipeline->DeclSink("Sink", {pipe2});
  source->HasMore(context);
  pipe1->PipeEven(context);
  pipe2->PipeEven(context);
  sink->NeedsMore(context);

  source->HasMore(context);
  pipe1->PipeNeedsMore(context);

  source->HasMore(context);
  pipe1->PipeNeedsMore(context);

  source->Blocked(context);
  pipeline->Resume(context, "Source");

  source->HasMore(context);
  pipe1->PipeHasMore(context);
  pipe2->PipeYield(context);
  pipe2->PipeYieldBack(context);
  pipe2->PipeNeedsMore(context);

  pipe1->PipeYield(context);
  pipe1->PipeYieldBack(context);
  pipe1->PipeHasMore(context);
  pipe2->PipeNeedsMore(context);

  pipe1->PipeEven(context);
  pipe2->PipeEven(context);
  sink->NeedsMore(context);

  source->HasMore(context);
  pipe1->PipeHasMore(context);
  pipe2->PipeHasMore(context);
  sink->NeedsMore(context);

  pipe2->PipeHasMore(context);
  sink->NeedsMore(context);

  pipe2->PipeEven(context);
  sink->NeedsMore(context);

  pipe1->PipeEven(context);
  pipe2->PipeHasMore(context);
  sink->NeedsMore(context);

  pipe2->PipeEven(context);
  sink->NeedsMore(context);

  source->Finished(context, Batch{});
  pipe1->PipeEven(context);
  pipe2->PipeEven(context);
  sink->NeedsMore(context);

  pipeline->ChannelFinished(context);

  this->TestTracePipeline(context, *pipeline);
}

TYPED_TEST(PipelineTaskTest, MultiDrain) {
  pipelang::ImperativeContext context;
  size_t dop = 4;
  auto name = "MultiDrain";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe1 = pipeline->DeclPipe("Pipe1", {source});
  auto pipe2 = pipeline->DeclPipe("Pipe2", {pipe1});
  auto sink = pipeline->DeclSink("Sink", {pipe2});
  source->Finished(context);

  pipe1->DrainHasMore(context);
  pipe2->PipeYield(context);
  pipe2->PipeYieldBack(context);
  pipe2->PipeNeedsMore(context);

  pipe1->DrainHasMore(context);
  pipe2->PipeEven(context);
  sink->NeedsMore(context);

  pipe1->DrainFinished(context);
  pipe2->DrainHasMore(context);
  sink->NeedsMore(context);

  pipe2->DrainYield(context);
  pipe2->DrainYieldBack(context);
  pipe2->DrainHasMore(context);
  sink->NeedsMore(context);

  pipe2->DrainFinished(context);
  pipeline->ChannelFinished(context);

  this->TestTracePipeline(context, *pipeline);
}

TYPED_TEST(PipelineTaskTest, MultiChannel) {
  pipelang::ImperativeContext context;
  size_t dop = 1;
  auto name = "MultiChannel";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source1 = pipeline->DeclSource("Source1");
  auto source2 = pipeline->DeclSource("Source2");
  auto pipe1 = pipeline->DeclPipe("Pipe1", {source1});
  auto pipe2 = pipeline->DeclPipe("Pipe2", {source2});
  auto sink = pipeline->DeclSink("Sink", {pipe1, pipe2});

  source1->Blocked(context);
  source2->Blocked(context);
  pipeline->Resume(context, "Source1");

  source1->HasMore(context);
  pipe1->PipeEven(context);
  sink->NeedsMore(context);

  source1->Blocked(context);
  pipeline->Resume(context, "Source2");
  source2->HasMore(context);
  pipe2->PipeEven(context);
  sink->NeedsMore(context);
  pipeline->Resume(context, "Source1");

  // TODO: More.

  source1->Finished(context);
  pipeline->ChannelFinished(context);
  source2->Finished(context);
  pipeline->ChannelFinished(context);

  this->TestTracePipeline(context, *pipeline);
}

// TODO: Pipe/Drain after implicit sources.
// TODO: Multi-channel.
// TODO: Multi-physical.
// TODO: Backpressure everywhere.
