#include "pipeline_task.h"
#include "logical_pipeline.h"
#include "op/op.h"
#include "physical_pipeline.h"
#include "pipeline_context.h"
#include "pipeline_observer.h"

#include <ara/schedule/async_dual_pool_scheduler.h>
#include <ara/schedule/naive_parallel_scheduler.h>
#include <ara/schedule/parallel_coro_scheduler.h>
#include <ara/schedule/schedule_context.h>
#include <ara/schedule/schedule_observer.h>
#include <ara/schedule/sequential_coro_scheduler.h>

#include <arrow/testing/gtest_util.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest.h>

using namespace ara;
using namespace ara::pipeline;
using namespace ara::task;
using namespace ara::schedule;

std::string ChannelName(const std::string& pipeline_name, size_t id = 0) {
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

  void Resume(ImperativeContext& ctx, std::string op_name) {
    resume_instructions_.emplace(ctx.ProgramCounter(), std::move(op_name));
  }

  void ChannelFinished(ImperativeContext& ctx, size_t channel_id = 0) {
    ctx.Trace(ImperativeTrace{ChannelName(name_, channel_id), "Run",
                              OpOutput::Finished().ToString()});
  }

  LogicalPipeline ToLogicalPipeline() const;

  const std::string& Name() const { return name_; }
  size_t Dop() const { return dop_; }

 private:
  ImperativeInstruction Execute(std::string op_name, const TaskContext& task_ctx,
                                ThreadId thread_id, ImperativeInstruction instruction) {
    ImperativeInstruction result;

    if (instruction.ok() && instruction->IsBlocked()) {
      ARA_CHECK(task_ctx.resumer_factory != nullptr);
      ARA_ASSIGN_OR_RAISE(auto resumer, task_ctx.resumer_factory());
      thread_locals_[thread_id].resumers[op_name].emplace_back(resumer);
      result = OpOutput::Blocked(std::move(resumer));
    } else {
      result = std::move(instruction);
    }

    size_t pc = ++thread_locals_[thread_id].pc;

    if (resume_instructions_.count(pc) != 0) {
      const auto& op_to_resume = resume_instructions_[pc];
      ARA_CHECK(!thread_locals_[thread_id].resumers[op_to_resume].empty());
      auto resumer = thread_locals_[thread_id].resumers[op_to_resume].front();
      thread_locals_[thread_id].resumers[op_to_resume].pop_front();
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
    std::unordered_map<std::string, std::list<ResumerPtr>> resumers;
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
        error_pc_(-1),
        thread_locals_(pipeline->Dop()),
        error_done_(false) {}
  virtual ~ImperativeOp() = default;

  ImperativeOp* GetChild() { return child_.get(); }
  void SetChild(std::shared_ptr<ImperativeOp> child) { child_ = std::move(child); }

  void Error(ImperativeContext& ctx, std::string msg) {
    ARA_CHECK(error_pc_ == -1);
    error_pc_ = instructions_.size();
    error_msg_ = std::move(msg);
  }

 protected:
  void OpInstructAndTrace(ImperativeContext& ctx, ImperativeInstruction instruction,
                          std::string method) {
    ImperativeTrace trace{Meta::Name(), std::move(method), instruction->ToString()};
    instructions_.emplace_back(std::move(instruction));
    ctx.IncreaseProgramCounter();
    ctx.Trace(std::move(trace));
  }

  void Sync(ImperativeContext& ctx, std::string method, size_t channel_id = 0) {
    OpInstructAndTrace(ctx, OpOutput::Blocked(nullptr), std::move(method));
    ChannelResult(ctx, OpOutput::Blocked(nullptr), channel_id);
    sync_instructions_.emplace(instructions_.size());
  }

  void ChannelResult(ImperativeContext& ctx, ImperativeInstruction instruction,
                     size_t channel_id = 0) {
    ctx.Trace(ImperativeTrace{ChannelName(pipeline_->Name(), channel_id), "Run",
                              instruction->ToString()});
  }

  ImperativeInstruction Fetch(ThreadId thread_id) {
    if (thread_locals_[thread_id].pc == error_pc_) {
      return Status::UnknownError(error_msg_);
    }
    return instructions_[thread_locals_[thread_id].pc++];
  }

  ImperativeInstruction Execute(const TaskContext& task_ctx, ThreadId thread_id,
                                ImperativeInstruction instruction) {
    if (!instruction.ok()) {
      if (!error_done_.exchange(true)) {
        return std::move(instruction);
      } else {
        ARA_CHECK(task_ctx.resumer_factory != nullptr);
        ARA_ASSIGN_OR_RAISE(auto resumer, task_ctx.resumer_factory());
        resumer->Resume();
        return OpOutput::Blocked(std::move(resumer));
      }
    }
    if (sync_instructions_.count(thread_locals_[thread_id].pc) != 0) {
      ARA_CHECK(task_ctx.resumer_factory != nullptr);
      ARA_ASSIGN_OR_RAISE(auto resumer, task_ctx.resumer_factory());
      {
        std::lock_guard<std::mutex> lock(sync_mutex_);
        resumers_.push_back(resumer);
        if (resumers_.size() == pipeline_->Dop()) {
          for (auto& resumer : resumers_) {
            resumer->Resume();
          }
          resumers_.clear();
        }
      }
      // TODO: Dummy, just increase pc.
      std::ignore =
          pipeline_->Execute(Meta::Name(), task_ctx, thread_id, OpOutput::Finished());
      return OpOutput::Blocked(std::move(resumer));
    }
    return pipeline_->Execute(Meta::Name(), task_ctx, thread_id, std::move(instruction));
  }

 protected:
  ImperativePipeline* pipeline_;
  std::shared_ptr<ImperativeOp> child_;

  std::vector<ImperativeInstruction> instructions_;
  std::unordered_set<size_t> sync_instructions_;
  int error_pc_;
  std::string error_msg_;

  struct ThreadLocal {
    size_t pc = 0;
  };
  std::vector<ThreadLocal> thread_locals_;
  std::mutex sync_mutex_;
  Resumers resumers_;
  std::atomic_bool error_done_;
};

class ImperativeSource : public ImperativeOp, public SourceOp {
 public:
  explicit ImperativeSource(std::string name, ImperativePipeline* pipeline)
      : ImperativeOp(std::move(name), "ImperativeSource", pipeline),
        SourceOp(ImperativeOp::Name(), ImperativeOp::Desc()) {}

  void Sync(ImperativeContext& ctx) { ImperativeOp::Sync(ctx, "Source"); }
  void HasMore(ImperativeContext& ctx) {
    OpInstructAndTrace(ctx, OpOutput::SourcePipeHasMore(Batch{}), "Source");
  }
  void Blocked(ImperativeContext& ctx, size_t channel_id = 0) {
    OpInstructAndTrace(ctx, OpOutput::Blocked(nullptr), "Source");
    ChannelResult(ctx, OpOutput::Blocked(nullptr), channel_id);
  }
  void Finished(ImperativeContext& ctx, std::optional<Batch> batch = std::nullopt) {
    OpInstructAndTrace(ctx, OpOutput::Finished(std::move(batch)), "Source");
  }

  PipelineSource Source(const PipelineContext&) override {
    return [&](const PipelineContext&, const TaskContext& task_ctx,
               ThreadId thread_id) -> OpResult {
      auto instruction = Fetch(thread_id);
      return Execute(task_ctx, thread_id, std::move(instruction));
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

  void PipeSync(ImperativeContext& ctx) { Sync(ctx, "Pipe"); }
  void PipeEven(ImperativeContext& ctx) {
    OpInstructAndTrace(ctx, OpOutput::PipeEven(Batch{}), "Pipe");
  }
  void PipeNeedsMore(ImperativeContext& ctx, size_t channel_id = 0) {
    OpInstructAndTrace(ctx, OpOutput::PipeSinkNeedsMore(), "Pipe");
    ChannelResult(ctx, OpOutput::PipeSinkNeedsMore(), channel_id);
  }
  void PipeHasMore(ImperativeContext& ctx) {
    OpInstructAndTrace(ctx, OpOutput::SourcePipeHasMore(Batch{}), "Pipe");
  }
  void PipeYield(ImperativeContext& ctx, size_t channel_id = 0) {
    OpInstructAndTrace(ctx, OpOutput::PipeYield(), "Pipe");
    ChannelResult(ctx, OpOutput::PipeYield(), channel_id);
  }
  void PipeYieldBack(ImperativeContext& ctx, size_t channel_id = 0) {
    OpInstructAndTrace(ctx, OpOutput::PipeYieldBack(), "Pipe");
    ChannelResult(ctx, OpOutput::PipeYieldBack());
  }
  void PipeBlocked(ImperativeContext& ctx, size_t channel_id = 0) {
    OpInstructAndTrace(ctx, OpOutput::Blocked(nullptr), "Pipe");
    ChannelResult(ctx, OpOutput::Blocked(nullptr), channel_id);
  }

  void DrainSync(ImperativeContext& ctx) {
    has_drain_ = true;
    Sync(ctx, "Drain");
  }
  void DrainHasMore(ImperativeContext& ctx) {
    has_drain_ = true;
    OpInstructAndTrace(ctx, OpOutput::SourcePipeHasMore(Batch{}), "Drain");
  }
  void DrainYield(ImperativeContext& ctx, size_t channel_id = 0) {
    has_drain_ = true;
    OpInstructAndTrace(ctx, OpOutput::PipeYield(), "Drain");
    ChannelResult(ctx, OpOutput::PipeYield(), channel_id);
  }
  void DrainYieldBack(ImperativeContext& ctx, size_t channel_id = 0) {
    has_drain_ = true;
    OpInstructAndTrace(ctx, OpOutput::PipeYieldBack(), "Drain");
    ChannelResult(ctx, OpOutput::PipeYieldBack(), channel_id);
  }
  void DrainBlocked(ImperativeContext& ctx, size_t channel_id = 0) {
    has_drain_ = true;
    OpInstructAndTrace(ctx, OpOutput::Blocked(nullptr), "Drain");
    ChannelResult(ctx, OpOutput::Blocked(nullptr), channel_id);
  }
  void DrainFinished(ImperativeContext& ctx, std::optional<Batch> batch = std::nullopt) {
    has_drain_ = true;
    OpInstructAndTrace(ctx, OpOutput::Finished(std::move(batch)), "Drain");
  }

  PipelinePipe Pipe(const PipelineContext&) override {
    return [&](const PipelineContext&, const TaskContext& task_ctx, ThreadId thread_id,
               std::optional<Batch>) -> OpResult {
      auto instruction = Fetch(thread_id);
      return Execute(task_ctx, thread_id, std::move(instruction));
    };
  }

  PipelineDrain Drain(const PipelineContext&) override {
    if (!has_drain_) {
      return nullptr;
    }
    return [&](const PipelineContext&, const TaskContext& task_ctx,
               ThreadId thread_id) -> OpResult {
      auto instruction = Fetch(thread_id);
      return Execute(task_ctx, thread_id, std::move(instruction));
    };
  }

  std::unique_ptr<SourceOp> ImplicitSource(const PipelineContext&) override {
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

  void Sync(ImperativeContext& ctx) { ImperativeOp::Sync(ctx, "Sink"); }
  void NeedsMore(ImperativeContext& ctx, size_t channel_id = 0) {
    OpInstructAndTrace(ctx, OpOutput::PipeSinkNeedsMore(), "Sink");
    ChannelResult(ctx, OpOutput::PipeSinkNeedsMore(), channel_id);
  }
  void Blocked(ImperativeContext& ctx, size_t channel_id = 0) {
    OpInstructAndTrace(ctx, OpOutput::Blocked(nullptr), "Sink");
    ChannelResult(ctx, OpOutput::Blocked(nullptr), channel_id);
  }

  PipelineSink Sink(const PipelineContext&) override {
    return [&](const PipelineContext&, const TaskContext& task_ctx, ThreadId thread_id,
               std::optional<Batch>) -> OpResult {
      auto instruction = Fetch(thread_id);
      return Execute(task_ctx, thread_id, std::move(instruction));
    };
  }

  TaskGroups Frontend(const PipelineContext&) override { return {}; }

  std::optional<TaskGroup> Backend(const PipelineContext&) override {
    return std::nullopt;
  }

  std::unique_ptr<SourceOp> ImplicitSource(const PipelineContext&) override {
    return nullptr;
  }
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
    if (result.ok() && !result->IsCancelled()) {
      traces_[thread_id].push_back(
          ImperativeTrace{pipeline_task.Name(), "Run", result->ToString()});
    }
    return Status::OK();
  }

  Status OnPipelineSourceEnd(const PipelineTask& pipeline_task, size_t channel,
                             const PipelineContext&, const TaskContext&,
                             ThreadId thread_id, const OpResult& result) override {
    if (result.ok() && !result->IsCancelled()) {
      auto source_name = pipeline_task.Pipeline().Channels()[channel].source_op->Name();
      traces_[thread_id].push_back(
          ImperativeTrace{std::move(source_name), "Source", result->ToString()});
    }
    return Status::OK();
  }

  Status OnPipelinePipeEnd(const PipelineTask& pipeline_task, size_t channel, size_t pipe,
                           const PipelineContext&, const TaskContext&, ThreadId thread_id,
                           const OpResult& result) override {
    if (result.ok() && !result->IsCancelled()) {
      auto pipe_name =
          pipeline_task.Pipeline().Channels()[channel].pipe_ops[pipe]->Name();
      traces_[thread_id].push_back(
          ImperativeTrace{std::move(pipe_name), "Pipe", result->ToString()});
    }
    return Status::OK();
  }

  Status OnPipelineDrainEnd(const PipelineTask& pipeline_task, size_t channel,
                            size_t pipe, const PipelineContext&, const TaskContext&,
                            ThreadId thread_id, const OpResult& result) override {
    if (result.ok() && !result->IsCancelled()) {
      auto pipe_name =
          pipeline_task.Pipeline().Channels()[channel].pipe_ops[pipe]->Name();
      traces_[thread_id].push_back(
          ImperativeTrace{std::move(pipe_name), "Drain", result->ToString()});
    }
    return Status::OK();
  }

  Status OnPipelineSinkEnd(const PipelineTask& pipeline_task, size_t channel,
                           const PipelineContext&, const TaskContext&, ThreadId thread_id,
                           const OpResult& result) override {
    if (result.ok() && !result->IsCancelled()) {
      auto sink_name = pipeline_task.Pipeline().Channels()[channel].sink_op->Name();
      traces_[thread_id].push_back(
          ImperativeTrace{std::move(sink_name), "Sink", result->ToString()});
    }
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

struct SequentialCoroSchedulerHolder {
  SequentialCoroScheduler scheduler;
};

struct ParallelCoroSchedulerHolder {
  ParallelCoroScheduler scheduler;
};

template <typename SchedulerType>
class PipelineTaskTest : public testing::Test {
 protected:
  void TestTracePipeline(const pipelang::ImperativeContext& ctx,
                         const pipelang::ImperativePipeline& pipeline) {
    size_t dop = pipeline.Dop();
    const auto& [result, act] = RunPipeline(ctx, pipeline);
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    auto& exp = ctx.Traces();
    CompareTraces(act, exp);
  }

  void TestTracePipelineWithUnknownError(const pipelang::ImperativeContext& ctx,
                                         const pipelang::ImperativePipeline& pipeline,
                                         const std::string& exp_msg) {
    size_t dop = pipeline.Dop();
    const auto& [result, act] = RunPipeline(ctx, pipeline);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsUnknownError()) << result.status().ToString();
    ASSERT_EQ(result.status().message(), exp_msg);
    auto& exp = ctx.Traces();
    CompareTracesForError(act, exp);
  }

 private:
  std::tuple<TaskResult, std::vector<std::vector<pipelang::ImperativeTrace>>> RunPipeline(
      const pipelang::ImperativeContext& ctx,
      const pipelang::ImperativePipeline& pipeline) {
    auto dop = pipeline.Dop();

    PipelineContext pipeline_ctx;
    pipelang::ImperativeTracer* tracer = nullptr;
    {
      auto tracer_up = std::make_unique<pipelang::ImperativeTracer>(dop);
      tracer = tracer_up.get();
      std::vector<std::unique_ptr<PipelineObserver>> observers;
      observers.push_back(std::move(tracer_up));
      pipeline_ctx.pipeline_observer =
          std::make_unique<ChainedObserver<PipelineObserver>>(std::move(observers));
    }

    ScheduleContext schedule_context;
    SchedulerType holder;
    Scheduler& scheduler = holder.scheduler;

    auto logical_pipeline = pipeline.ToLogicalPipeline();
    auto physical_pipelines = CompilePipeline(pipeline_ctx, logical_pipeline);

    ARA_CHECK(logical_pipeline.SinkOp()->Frontend(pipeline_ctx).empty());
    ARA_CHECK(!logical_pipeline.SinkOp()->Backend(pipeline_ctx).has_value());

    TaskResult result;
    for (const auto& physical_pipeline : physical_pipelines) {
      PipelineTask pipeline_task(pipeline_ctx, physical_pipeline, dop);

      for (const auto& channel : physical_pipeline.Channels()) {
        ARA_CHECK(channel.source_op->Frontend(pipeline_ctx).empty());
        ARA_CHECK(!channel.source_op->Backend(pipeline_ctx).has_value());
      }

      Task task(pipeline_task.Name(), pipeline_task.Desc(),
                [&pipeline_ctx, &pipeline_task](const TaskContext& task_ctx,
                                                TaskId task_id) -> TaskResult {
                  return pipeline_task(pipeline_ctx, task_ctx, task_id);
                });
      TaskGroup task_group(pipeline_task.Name(), pipeline_task.Desc(), std::move(task),
                           dop, std::nullopt, nullptr);

      auto handle = scheduler.Schedule(schedule_context, task_group);
      ARA_CHECK(handle.ok());
      result = (*handle)->Wait(schedule_context);
      if (!result.ok()) {
        break;
      }
    }
    return std::make_tuple(std::move(result), std::move(tracer->Traces()));
  }

  void CompareTraces(const std::vector<std::vector<pipelang::ImperativeTrace>>& act,
                     const std::vector<pipelang::ImperativeTrace>& exp) {
    for (size_t i = 0; i < act.size(); ++i) {
      ASSERT_EQ(act[i].size(), exp.size()) << "thread_id=" << i;
      for (size_t j = 0; j < exp.size(); ++j) {
        ASSERT_EQ(act[i][j], exp[j]) << "thread_id=" << i << ", trace_id=" << j;
      }
    }
  }

  void CompareTracesForError(
      const std::vector<std::vector<pipelang::ImperativeTrace>>& act,
      const std::vector<pipelang::ImperativeTrace>& exp) {
    for (size_t i = 0; i < act.size(); ++i) {
      ASSERT_GE(act[i].size(), exp.size()) << "thread_id=" << i;
      for (size_t j = 0; j < exp.size(); ++j) {
        ASSERT_EQ(act[i][j], exp[j]) << "thread_id=" << i << ", trace_id=" << j;
      }
      for (size_t j = exp.size(); j < act[i].size(); ++j) {
        ASSERT_EQ(act[i][j].payload, "BLOCKED")
            << "thread_id=" << i << ", trace_id=" << j;
      }
    }
  }
};

using SchedulerTypes =
    ::testing::Types<AsyncDualPoolSchedulerHolder, NaiveParallelSchedulerHolder,
                     SequentialCoroSchedulerHolder, ParallelCoroSchedulerHolder>;
TYPED_TEST_SUITE(PipelineTaskTest, SchedulerTypes);

void MakeEmptySourcePipeline(pipelang::ImperativeContext& ctx, size_t dop,
                             std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "EmptySource";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto sink = pipeline->DeclSink("Sink", {source});
  source->Finished(ctx);
  pipeline->ChannelFinished(ctx);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(ctx.Traces().size(), 2);
  ASSERT_EQ(ctx.Traces()[0], (pipelang::ImperativeTrace{
                                 "Source", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(ctx.Traces()[1],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, EmptySource) {
  pipelang::ImperativeContext ctx;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeEmptySourcePipeline(ctx, 4, pipeline);
  this->TestTracePipeline(ctx, *pipeline);
}

void MakeEmptySourceNotReadyPipeline(
    pipelang::ImperativeContext& ctx, size_t dop,
    std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "EmptySourceNotReady";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto sink = pipeline->DeclSink("Sink", {source});
  source->Blocked(ctx);
  pipeline->Resume(ctx, "Source");
  source->Finished(ctx);
  pipeline->ChannelFinished(ctx);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(ctx.Traces().size(), 4);
  ASSERT_EQ(ctx.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(ctx.Traces()[1],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(ctx.Traces()[2], (pipelang::ImperativeTrace{
                                 "Source", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(ctx.Traces()[3],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, EmptySourceNotReady) {
  pipelang::ImperativeContext ctx;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeEmptySourceNotReadyPipeline(ctx, 1, pipeline);
  this->TestTracePipeline(ctx, *pipeline);
}

void MakeTwoSourcesOneNotReadyPipeline(
    pipelang::ImperativeContext& ctx, size_t dop,
    std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "TwoSourceOneNotReady";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source1 = pipeline->DeclSource("Source1");
  auto source2 = pipeline->DeclSource("Source2");
  auto sink = pipeline->DeclSink("Sink", {source1, source2});
  source1->Blocked(ctx);
  source2->Finished(ctx);
  pipeline->ChannelFinished(ctx);
  pipeline->Resume(ctx, "Source1");
  source1->Finished(ctx);
  pipeline->ChannelFinished(ctx);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 2);
  ASSERT_EQ(logical.Channels()[0].source_op, source1);
  ASSERT_EQ(logical.Channels()[1].source_op, source2);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(ctx.Traces().size(), 6);
  ASSERT_EQ(ctx.Traces()[0],
            (pipelang::ImperativeTrace{"Source1", "Source",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(ctx.Traces()[1],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(ctx.Traces()[2], (pipelang::ImperativeTrace{
                                 "Source2", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(ctx.Traces()[3],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
  ASSERT_EQ(ctx.Traces()[4], (pipelang::ImperativeTrace{
                                 "Source1", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(ctx.Traces()[5],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, TwoSourceOneNotReady) {
  pipelang::ImperativeContext ctx;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeTwoSourcesOneNotReadyPipeline(ctx, 4, pipeline);
  this->TestTracePipeline(ctx, *pipeline);
}

void MakeOnePassPipeline(pipelang::ImperativeContext& ctx, size_t dop,
                         std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "OnePass";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto sink = pipeline->DeclSink("Sink", {source});
  source->HasMore(ctx);
  sink->NeedsMore(ctx);
  source->Finished(ctx);
  pipeline->ChannelFinished(ctx);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(ctx.Traces().size(), 5);
  ASSERT_EQ(ctx.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[1],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[2],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[3], (pipelang::ImperativeTrace{
                                 "Source", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(ctx.Traces()[4],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, OnePass) {
  pipelang::ImperativeContext ctx;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeOnePassPipeline(ctx, 4, pipeline);
  this->TestTracePipeline(ctx, *pipeline);
}

void MakeOnePassDirectFinishPipeline(
    pipelang::ImperativeContext& ctx, size_t dop,
    std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "OnePassDirectFinish";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto sink = pipeline->DeclSink("Sink", {source});
  source->Finished(ctx, Batch{});
  sink->NeedsMore(ctx);
  pipeline->ChannelFinished(ctx);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(ctx.Traces().size(), 4);
  ASSERT_EQ(ctx.Traces()[0], (pipelang::ImperativeTrace{
                                 "Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[1],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[2],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[3],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, OnePassDirectFinish) {
  pipelang::ImperativeContext ctx;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeOnePassDirectFinishPipeline(ctx, 4, pipeline);
  this->TestTracePipeline(ctx, *pipeline);
}

void MakeOnePassWithPipePipeline(pipelang::ImperativeContext& ctx, size_t dop,
                                 std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "OnePassWithPipe";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore(ctx);
  pipe->PipeEven(ctx);
  sink->NeedsMore(ctx);
  source->Finished(ctx);
  pipeline->ChannelFinished(ctx);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(ctx.Traces().size(), 6);
  ASSERT_EQ(ctx.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[1], (pipelang::ImperativeTrace{
                                 "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[3],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[4], (pipelang::ImperativeTrace{
                                 "Source", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(ctx.Traces()[5],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, OnePassWithPipe) {
  pipelang::ImperativeContext ctx;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeOnePassWithPipePipeline(ctx, 4, pipeline);
  this->TestTracePipeline(ctx, *pipeline);
}

void MakePipeNeedsMorePipeline(pipelang::ImperativeContext& ctx, size_t dop,
                               std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "PipeNeedsMore";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore(ctx);
  pipe->PipeNeedsMore(ctx);
  source->Finished(ctx, Batch{});
  pipe->PipeEven(ctx);
  sink->NeedsMore(ctx);
  pipeline->ChannelFinished(ctx);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(ctx.Traces().size(), 8);
  ASSERT_EQ(ctx.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[1],
            (pipelang::ImperativeTrace{"Pipe", "Pipe",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[2],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[3], (pipelang::ImperativeTrace{
                                 "Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[4], (pipelang::ImperativeTrace{
                                 "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[5],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[6],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[7],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, PipeNeedsMore) {
  pipelang::ImperativeContext ctx;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakePipeNeedsMorePipeline(ctx, 4, pipeline);
  this->TestTracePipeline(ctx, *pipeline);
}

void MakePipeHasMorePipeline(pipelang::ImperativeContext& ctx, size_t dop,
                             std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "PipeHasMore";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore(ctx);
  pipe->PipeHasMore(ctx);
  sink->NeedsMore(ctx);
  pipe->PipeEven(ctx);
  sink->NeedsMore(ctx);
  source->Finished(ctx);
  pipeline->ChannelFinished(ctx);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(ctx.Traces().size(), 9);
  ASSERT_EQ(ctx.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[1],
            (pipelang::ImperativeTrace{"Pipe", "Pipe",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[3],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[4], (pipelang::ImperativeTrace{
                                 "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[5],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[6],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[7], (pipelang::ImperativeTrace{
                                 "Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[8],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, PipeHasMore) {
  pipelang::ImperativeContext ctx;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakePipeHasMorePipeline(ctx, 4, pipeline);
  this->TestTracePipeline(ctx, *pipeline);
}

void MakePipeYieldPipeline(pipelang::ImperativeContext& ctx, size_t dop,
                           std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "PipeYield";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore(ctx);
  pipe->PipeYield(ctx);
  pipe->PipeYieldBack(ctx);
  pipe->PipeNeedsMore(ctx);
  source->Finished(ctx);
  pipeline->ChannelFinished(ctx);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(ctx.Traces().size(), 9);
  ASSERT_EQ(ctx.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[1], (pipelang::ImperativeTrace{
                                 "Pipe", "Pipe", OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(ctx.Traces()[2],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(ctx.Traces()[3], (pipelang::ImperativeTrace{
                                 "Pipe", "Pipe", OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(ctx.Traces()[4],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(ctx.Traces()[5],
            (pipelang::ImperativeTrace{"Pipe", "Pipe",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[6],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[7], (pipelang::ImperativeTrace{
                                 "Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[8],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, PipeYield) {
  pipelang::ImperativeContext ctx;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakePipeYieldPipeline(ctx, 4, pipeline);
  this->TestTracePipeline(ctx, *pipeline);
}

void MakePipeAsyncSpillPipeline(pipelang::ImperativeContext& ctx, size_t dop,
                                std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "PipeAsyncSpill";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore(ctx);
  pipe->PipeBlocked(ctx);
  pipeline->Resume(ctx, "Pipe");
  pipe->PipeNeedsMore(ctx);
  source->Finished(ctx);
  pipeline->ChannelFinished(ctx);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(ctx.Traces().size(), 7);
  ASSERT_EQ(ctx.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[1], (pipelang::ImperativeTrace{
                                 "Pipe", "Pipe", OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(ctx.Traces()[2],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(ctx.Traces()[3],
            (pipelang::ImperativeTrace{"Pipe", "Pipe",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[4],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[5], (pipelang::ImperativeTrace{
                                 "Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[6],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, PipeAsyncSpill) {
  pipelang::ImperativeContext ctx;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakePipeAsyncSpillPipeline(ctx, 4, pipeline);
  this->TestTracePipeline(ctx, *pipeline);
}

void MakeDrainPipeline(pipelang::ImperativeContext& ctx, size_t dop,
                       std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "Drain";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore(ctx);
  pipe->PipeEven(ctx);
  sink->NeedsMore(ctx);
  source->Finished(ctx, Batch{});
  pipe->PipeEven(ctx);
  sink->NeedsMore(ctx);
  pipe->DrainHasMore(ctx);
  sink->NeedsMore(ctx);
  pipe->DrainYield(ctx);
  pipe->DrainYieldBack(ctx);
  pipe->DrainHasMore(ctx);
  sink->NeedsMore(ctx);
  pipe->DrainBlocked(ctx);
  pipeline->Resume(ctx, "Pipe");
  pipe->DrainFinished(ctx, Batch{});
  sink->NeedsMore(ctx);
  pipeline->ChannelFinished(ctx);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(ctx.Traces().size(), 24);
  ASSERT_EQ(ctx.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[1], (pipelang::ImperativeTrace{
                                 "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[3],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[4], (pipelang::ImperativeTrace{
                                 "Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[5], (pipelang::ImperativeTrace{
                                 "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[6],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[7],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[8],
            (pipelang::ImperativeTrace{"Pipe", "Drain",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[9],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[10],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[11], (pipelang::ImperativeTrace{
                                  "Pipe", "Drain", OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(ctx.Traces()[12],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(
      ctx.Traces()[13],
      (pipelang::ImperativeTrace{"Pipe", "Drain", OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(ctx.Traces()[14],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(ctx.Traces()[15],
            (pipelang::ImperativeTrace{"Pipe", "Drain",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[16],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[17],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[18],
            (pipelang::ImperativeTrace{"Pipe", "Drain",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(ctx.Traces()[19],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(ctx.Traces()[20], (pipelang::ImperativeTrace{
                                  "Pipe", "Drain", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[21],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[22],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[23],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, Drain) {
  pipelang::ImperativeContext ctx;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeDrainPipeline(ctx, 4, pipeline);
  this->TestTracePipeline(ctx, *pipeline);
}

void MakeImplicitSourcePipeline(pipelang::ImperativeContext& ctx, size_t dop,
                                std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "ImplicitSource";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto implicit_source = pipeline->DeclImplicitSource("ImplicitSource", pipe);
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->Finished(ctx, Batch{});
  pipe->PipeEven(ctx);
  sink->NeedsMore(ctx);
  pipeline->ChannelFinished(ctx);
  implicit_source->Finished(ctx, Batch{});
  sink->NeedsMore(ctx, 1);
  pipeline->ChannelFinished(ctx, 1);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(ctx.Traces().size(), 9);
  ASSERT_EQ(ctx.Traces()[0], (pipelang::ImperativeTrace{
                                 "Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[1], (pipelang::ImperativeTrace{
                                 "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[3],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[4],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
  ASSERT_EQ(ctx.Traces()[5],
            (pipelang::ImperativeTrace{"ImplicitSource", "Source",
                                       OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[6],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[7],
            (pipelang::ImperativeTrace{ChannelName(name, 1), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[8],
            (pipelang::ImperativeTrace{ChannelName(name, 1), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, ImplicitSource) {
  pipelang::ImperativeContext ctx;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeImplicitSourcePipeline(ctx, 4, pipeline);
  this->TestTracePipeline(ctx, *pipeline);
}

void MakeBackpressurePipeline(pipelang::ImperativeContext& ctx, size_t dop,
                              std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "Backpressure";
  result = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore(ctx);
  pipe->PipeEven(ctx);
  sink->Blocked(ctx);
  pipeline->Resume(ctx, "Sink");
  sink->Blocked(ctx);
  pipeline->Resume(ctx, "Sink");
  sink->NeedsMore(ctx);
  source->Finished(ctx, Batch{});
  pipe->PipeEven(ctx);
  sink->NeedsMore(ctx);
  pipeline->ChannelFinished(ctx);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(ctx.Traces().size(), 13);
  ASSERT_EQ(ctx.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[1], (pipelang::ImperativeTrace{
                                 "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[2], (pipelang::ImperativeTrace{
                                 "Sink", "Sink", OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(ctx.Traces()[3],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(ctx.Traces()[4], (pipelang::ImperativeTrace{
                                 "Sink", "Sink", OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(ctx.Traces()[5],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(ctx.Traces()[6],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[7],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[8], (pipelang::ImperativeTrace{
                                 "Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[9], (pipelang::ImperativeTrace{
                                 "Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(ctx.Traces()[10],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[11],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(ctx.Traces()[12],
            (pipelang::ImperativeTrace{ChannelName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, Backpressure) {
  pipelang::ImperativeContext ctx;
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeBackpressurePipeline(ctx, 4, pipeline);
  this->TestTracePipeline(ctx, *pipeline);
}

TYPED_TEST(PipelineTaskTest, MultiPipe) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "MultiPipe";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe1 = pipeline->DeclPipe("Pipe1", {source});
  auto pipe2 = pipeline->DeclPipe("Pipe2", {pipe1});
  auto sink = pipeline->DeclSink("Sink", {pipe2});
  source->HasMore(ctx);
  pipe1->PipeEven(ctx);
  pipe2->PipeEven(ctx);
  sink->NeedsMore(ctx);

  source->HasMore(ctx);
  pipe1->PipeNeedsMore(ctx);

  source->HasMore(ctx);
  pipe1->PipeNeedsMore(ctx);

  source->Blocked(ctx);
  pipeline->Resume(ctx, "Source");

  source->HasMore(ctx);
  pipe1->PipeHasMore(ctx);
  pipe2->PipeYield(ctx);
  pipe2->PipeYieldBack(ctx);
  pipe2->PipeNeedsMore(ctx);

  pipe1->PipeYield(ctx);
  pipe1->PipeYieldBack(ctx);
  pipe1->PipeHasMore(ctx);
  pipe2->PipeBlocked(ctx);
  pipeline->Resume(ctx, "Pipe2");
  pipe2->PipeNeedsMore(ctx);

  pipe1->PipeBlocked(ctx);
  pipeline->Resume(ctx, "Pipe1");
  pipe1->PipeEven(ctx);
  pipe2->PipeEven(ctx);
  sink->NeedsMore(ctx);

  source->HasMore(ctx);
  pipe1->PipeBlocked(ctx);
  pipeline->Resume(ctx, "Pipe1");
  pipe1->PipeHasMore(ctx);
  pipe2->PipeBlocked(ctx);
  pipeline->Resume(ctx, "Pipe2");
  pipe2->PipeHasMore(ctx);
  sink->NeedsMore(ctx);

  pipe2->PipeHasMore(ctx);
  sink->Blocked(ctx);
  pipeline->Resume(ctx, "Sink");
  sink->NeedsMore(ctx);

  pipe2->PipeEven(ctx);
  sink->NeedsMore(ctx);

  pipe1->PipeEven(ctx);
  pipe2->PipeHasMore(ctx);
  sink->NeedsMore(ctx);

  pipe2->PipeEven(ctx);
  sink->NeedsMore(ctx);

  source->Finished(ctx, Batch{});
  pipe1->PipeEven(ctx);
  pipe2->PipeEven(ctx);
  sink->NeedsMore(ctx);

  pipeline->ChannelFinished(ctx);

  this->TestTracePipeline(ctx, *pipeline);
}

TYPED_TEST(PipelineTaskTest, MultiDrain) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "MultiDrain";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe1 = pipeline->DeclPipe("Pipe1", {source});
  auto pipe2 = pipeline->DeclPipe("Pipe2", {pipe1});
  auto sink = pipeline->DeclSink("Sink", {pipe2});
  source->Finished(ctx);

  pipe1->DrainBlocked(ctx);
  pipeline->Resume(ctx, "Pipe1");
  pipe1->DrainHasMore(ctx);
  pipe2->PipeYield(ctx);
  pipe2->PipeYieldBack(ctx);
  pipe2->PipeBlocked(ctx);
  pipeline->Resume(ctx, "Pipe2");
  pipe2->PipeNeedsMore(ctx);

  pipe1->DrainHasMore(ctx);
  pipe2->PipeEven(ctx);
  sink->Blocked(ctx);
  pipeline->Resume(ctx, "Sink");
  sink->NeedsMore(ctx);

  pipe1->DrainFinished(ctx);
  pipe2->DrainBlocked(ctx);
  pipeline->Resume(ctx, "Pipe2");
  pipe2->DrainHasMore(ctx);
  sink->Blocked(ctx);
  pipeline->Resume(ctx, "Sink");
  sink->NeedsMore(ctx);

  pipe2->DrainYield(ctx);
  pipe2->DrainYieldBack(ctx);
  pipe2->DrainHasMore(ctx);
  sink->NeedsMore(ctx);

  pipe2->DrainFinished(ctx);
  pipeline->ChannelFinished(ctx);

  this->TestTracePipeline(ctx, *pipeline);
}

TYPED_TEST(PipelineTaskTest, MultiChannel) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "MultiChannel";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source1 = pipeline->DeclSource("Source1");
  auto source2 = pipeline->DeclSource("Source2");
  auto pipe1 = pipeline->DeclPipe("Pipe1", {source1});
  auto pipe2 = pipeline->DeclPipe("Pipe2", {source2});
  auto sink = pipeline->DeclSink("Sink", {pipe1, pipe2});

  source1->Blocked(ctx);
  source2->Blocked(ctx);
  pipeline->Resume(ctx, "Source1");

  source1->HasMore(ctx);
  pipe1->PipeEven(ctx);
  sink->NeedsMore(ctx);

  // Reentrant wait.
  source1->Blocked(ctx);
  pipeline->Resume(ctx, "Source1");
  source1->HasMore(ctx);
  pipe1->PipeEven(ctx);
  sink->NeedsMore(ctx);

  source1->Blocked(ctx);
  pipeline->Resume(ctx, "Source2");
  source2->HasMore(ctx);
  pipe2->PipeEven(ctx);
  sink->NeedsMore(ctx);

  source2->HasMore(ctx);
  pipe2->PipeBlocked(ctx);
  pipeline->Resume(ctx, "Source1");

  source1->HasMore(ctx);
  pipeline->Resume(ctx, "Pipe2");
  pipe1->PipeNeedsMore(ctx);

  source1->HasMore(ctx);
  pipe1->PipeEven(ctx);
  sink->NeedsMore(ctx);

  source1->HasMore(ctx);
  pipe1->PipeBlocked(ctx);

  pipe2->PipeEven(ctx);
  sink->NeedsMore(ctx);

  source2->Blocked(ctx);
  pipeline->Resume(ctx, "Pipe1");

  pipe1->PipeEven(ctx);
  sink->NeedsMore(ctx);
  pipeline->Resume(ctx, "Source2");

  source1->HasMore(ctx);
  pipe1->PipeBlocked(ctx);

  source2->HasMore(ctx);
  pipe2->PipeEven(ctx);
  sink->Blocked(ctx);
  pipeline->Resume(ctx, "Pipe1");

  pipe1->PipeEven(ctx);
  sink->Blocked(ctx);
  pipeline->Resume(ctx, "Sink");

  sink->NeedsMore(ctx);
  source2->Blocked(ctx);
  pipeline->Resume(ctx, "Sink");

  sink->NeedsMore(ctx);
  source1->HasMore(ctx);
  pipe1->PipeEven(ctx);
  sink->NeedsMore(ctx);

  source1->Blocked(ctx);
  pipeline->Resume(ctx, "Source2");

  source2->HasMore(ctx);
  pipeline->Resume(ctx, "Source1");
  pipe2->PipeEven(ctx);
  sink->NeedsMore(ctx);

  source1->Finished(ctx);
  pipeline->ChannelFinished(ctx);

  source2->HasMore(ctx);
  pipe2->PipeEven(ctx);
  sink->Blocked(ctx);
  pipeline->Resume(ctx, "Sink");

  sink->NeedsMore(ctx);

  source2->Finished(ctx);
  pipeline->ChannelFinished(ctx);

  this->TestTracePipeline(ctx, *pipeline);
}

TYPED_TEST(PipelineTaskTest, DirectSourceError) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "DirectSourceError";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->Sync(ctx);
  source->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

TYPED_TEST(PipelineTaskTest, SourceErrorAfterBlocked) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "SourceErrorAfterBlocked";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->Blocked(ctx);
  pipeline->Resume(ctx, "Source");
  source->Sync(ctx);
  source->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

TYPED_TEST(PipelineTaskTest, SourceError) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "SourceError";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->HasMore(ctx);
  pipe->PipeEven(ctx);
  sink->NeedsMore(ctx);
  source->Sync(ctx);
  source->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

TYPED_TEST(PipelineTaskTest, PipeError) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "PipeError";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->HasMore(ctx);
  pipe->PipeSync(ctx);
  pipe->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

TYPED_TEST(PipelineTaskTest, PipeErrorAfterEven) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "PipeErrorAfterEven";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->HasMore(ctx);
  pipe->PipeEven(ctx);
  sink->NeedsMore(ctx);
  source->HasMore(ctx);
  pipe->PipeSync(ctx);
  pipe->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

TYPED_TEST(PipelineTaskTest, PipeErrorAfterNeedsMore) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "PipeErrorAfterNeedsMore";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->HasMore(ctx);
  pipe->PipeNeedsMore(ctx);
  source->HasMore(ctx);
  pipe->PipeSync(ctx);
  pipe->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

TYPED_TEST(PipelineTaskTest, PipeErrorAfterHasMore) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "PipeErrorAfterHasMore";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->HasMore(ctx);
  pipe->PipeHasMore(ctx);
  sink->NeedsMore(ctx);
  pipe->PipeSync(ctx);
  pipe->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

// TODO: This case is probably unstable as if the error thread is fast enough then other
// threads won't emit yield.
// TYPED_TEST(PipelineTaskTest, PipeErrorAfterYield) {
//   pipelang::ImperativeContext ctx;
//   size_t dop = 4;
//   auto name = "PipeErrorAfterYield";
//   auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
//   auto source = pipeline->DeclSource("Source");
//   auto pipe = pipeline->DeclPipe("Pipe", {source});
//   auto sink = pipeline->DeclSink("Sink", {pipe});

//   source->HasMore(ctx);
//   pipe->PipeSync(ctx);
//   pipe->PipeYield(ctx);
//   pipe->Error(ctx, "42");

//   this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
// }

TYPED_TEST(PipelineTaskTest, PipeErrorAfterYieldBack) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "PipeErrorAfterYield";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->HasMore(ctx);
  pipe->PipeYield(ctx);
  pipe->PipeYieldBack(ctx);
  pipe->PipeSync(ctx);
  pipe->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

TYPED_TEST(PipelineTaskTest, PipeErrorAfterBlocked) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "PipeErrorAfterBlocked";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->HasMore(ctx);
  pipe->PipeBlocked(ctx);
  pipeline->Resume(ctx, "Pipe");
  pipe->PipeSync(ctx);
  pipe->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

TYPED_TEST(PipelineTaskTest, DrainError) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "DrainError";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->Finished(ctx);
  pipe->DrainSync(ctx);
  pipe->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

TYPED_TEST(PipelineTaskTest, DrainErrorAfterHasMore) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "DrainErrorAfterHasMore";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->Finished(ctx);
  pipe->DrainHasMore(ctx);
  sink->NeedsMore(ctx);
  pipe->DrainSync(ctx);
  pipe->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

// TODO: This case is probably unstable as if the error thread is fast enough then other
// threads won't emit yield.
// TYPED_TEST(PipelineTaskTest, DrainErrorAfterYield) {
//   pipelang::ImperativeContext ctx;
//   size_t dop = 4;
//   auto name = "DrainErrorAfterYield";
//   auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
//   auto source = pipeline->DeclSource("Source");
//   auto pipe = pipeline->DeclPipe("Pipe", {source});
//   auto sink = pipeline->DeclSink("Sink", {pipe});

//   source->Finished(ctx);
//   pipe->DrainSync(ctx);
//   pipe->DrainYield(ctx);
//   pipe->Error(ctx, "42");

//   this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
// }

TYPED_TEST(PipelineTaskTest, DrainErrorAfterYieldBack) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "DrainErrorAfterYield";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->Finished(ctx);
  pipe->DrainYield(ctx);
  pipe->DrainYieldBack(ctx);
  pipe->DrainSync(ctx);
  pipe->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

TYPED_TEST(PipelineTaskTest, DrainErrorAfterBlocked) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "DrainErrorAfterBlocked";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->Finished(ctx);
  pipe->DrainBlocked(ctx);
  pipeline->Resume(ctx, "Pipe");
  pipe->DrainSync(ctx);
  pipe->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

TYPED_TEST(PipelineTaskTest, SinkError) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "SinkError";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->HasMore(ctx);
  pipe->PipeEven(ctx);
  sink->Sync(ctx);
  sink->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

TYPED_TEST(PipelineTaskTest, SinkErrorAfterNeedsMore) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "SinkErrorAfterNeedsMore";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->HasMore(ctx);
  pipe->PipeEven(ctx);
  sink->NeedsMore(ctx);
  source->HasMore(ctx);
  pipe->PipeEven(ctx);
  sink->Sync(ctx);
  sink->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}

TYPED_TEST(PipelineTaskTest, SinkErrorAfterBlocked) {
  pipelang::ImperativeContext ctx;
  size_t dop = 4;
  auto name = "SinkErrorAfterBlocked";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, dop);
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});

  source->HasMore(ctx);
  pipe->PipeEven(ctx);
  sink->Blocked(ctx);
  pipeline->Resume(ctx, "Sink");
  sink->Sync(ctx);
  sink->Error(ctx, "42");

  this->TestTracePipelineWithUnknownError(ctx, *pipeline, "42");
}
