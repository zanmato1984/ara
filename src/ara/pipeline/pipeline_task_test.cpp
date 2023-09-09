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

struct ImperativeContext {
  size_t dop;
  std::atomic_bool finished = false;
  std::mutex mutex;
  Resumers resumers;

  void AddResumer(ResumerPtr resumer) {
    std::lock_guard<std::mutex> lock(mutex);
    resumers.push_back(std::move(resumer));
  }

  TaskResult TryResume() {
    if (finished) {
      return TaskStatus::Finished();
    }

    bool work = false;
    {
      std::unique_lock<std::mutex> lock(mutex);
      work = resumers.size() >= dop;
    }
    if (work) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      std::unique_lock<std::mutex> lock(mutex);
      for (auto& resumer : resumers) {
        resumer->Resume();
      }
      resumers.clear();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return TaskStatus::Continue();
  }

  void Finish() { finished = true; }
};

class ImperativePipeline {
 public:
  explicit ImperativePipeline(std::string name, ImperativeContext* context)
      : name_(std::move(name)), context_(context) {}

  ImperativeSource* DeclSource(std::string);

  ImperativePipe* DeclPipe(std::string, std::initializer_list<ImperativeOp*>);

  ImperativeSink* DeclSink(std::string, std::initializer_list<ImperativeOp*>);

  ImperativeSource* DeclImplicitSource(std::string, ImperativePipe*);

  void ChannelFinished(size_t task_id = 0) {
    Trace(ImperativeTrace{TaskName(name_, task_id), "Run",
                          OpOutput::Finished().ToString()});
  }

  const std::vector<ImperativeTrace>& Traces() const { return traces_; }

  LogicalPipeline ToLogicalPipeline() const;

  const std::string& Name() const { return name_; }

 private:
  void Trace(ImperativeTrace trace) { traces_.push_back(std::move(trace)); }

 private:
  std::string name_;
  ImperativeContext* context_;

  std::vector<std::shared_ptr<ImperativeSource>> sources_;
  std::vector<ImperativeTrace> traces_;

  friend class ImperativeOp;
};

class ImperativeOp : public internal::Meta {
 public:
  explicit ImperativeOp(std::string name, std::string desc, ImperativeContext* context,
                        ImperativePipeline* pipeline)
      : Meta(std::move(name), std::move(desc)),
        context_(context),
        pipeline_(pipeline),
        thread_locals_(context_->dop) {}
  virtual ~ImperativeOp() = default;

  ImperativeOp* GetChild() { return child_.get(); }
  void SetChild(std::shared_ptr<ImperativeOp> child) { child_ = std::move(child); }

 protected:
  void OpInstructAndTrace(ImperativeInstruction instruction, std::string method) {
    ImperativeTrace trace{Meta::Name(), std::move(method), instruction->ToString()};
    instructions_.emplace_back(std::move(instruction));
    pipeline_->Trace(std::move(trace));
  }

  void TaskTrace(ImperativeInstruction instruction, size_t pipeline_id = 0) {
    pipeline_->Trace(ImperativeTrace{TaskName(pipeline_->Name(), pipeline_id), "Run",
                                     instruction->ToString()});
  }

  ImperativeInstruction Fetch(const TaskContext& task_context, ThreadId thread_id) {
    auto i = instructions_[thread_locals_[thread_id].pc++];
    if (i.ok() && i->IsBlocked()) {
      ARA_CHECK(task_context.resumer_factory != nullptr);
      ARA_ASSIGN_OR_RAISE(auto resumer, task_context.resumer_factory());
      context_->AddResumer(resumer);
      return OpOutput::Blocked(std::move(resumer));
    } else {
      return std::move(i);
    }
  }

 protected:
  ImperativeContext* context_;
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
  explicit ImperativeSource(std::string name, ImperativeContext* context,
                            ImperativePipeline* pipeline)
      : ImperativeOp(std::move(name), "ImperativeSource", context, pipeline),
        SourceOp(ImperativeOp::Name(), ImperativeOp::Desc()) {}

  void HasMore() { OpInstructAndTrace(OpOutput::SourcePipeHasMore(Batch{}), "Source"); }
  void Blocked(size_t task_id = 0) {
    OpInstructAndTrace(OpOutput::Blocked(nullptr), "Source");
    TaskTrace(OpOutput::Blocked(nullptr), task_id);
  }
  void Finished(std::optional<Batch> batch = std::nullopt) {
    OpInstructAndTrace(OpOutput::Finished(std::move(batch)), "Source");
  }

  PipelineSource Source() override {
    return [&](const PipelineContext&, const TaskContext& task_context,
               ThreadId thread_id) -> OpResult { return Fetch(task_context, thread_id); };
  }

  TaskGroups Frontend(const PipelineContext&) override { return {}; }

  std::optional<TaskGroup> Backend(const PipelineContext&) override {
    return std::nullopt;
  }
};

class ImperativePipe : public ImperativeOp, public PipeOp {
 public:
  explicit ImperativePipe(std::string name, ImperativeContext* context,
                          ImperativePipeline* pipeline)
      : ImperativeOp(std::move(name), "ImperativePipe", context, pipeline),
        PipeOp(ImperativeOp::Name(), ImperativeOp::Desc()),
        implicit_source_(nullptr),
        has_drain_(false) {}

  void SetImplicitSource(std::unique_ptr<ImperativeSource> implicit_source) {
    implicit_source_ = std::move(implicit_source);
  }

  void PipeEven() { OpInstructAndTrace(OpOutput::PipeEven(Batch{}), "Pipe"); }
  void PipeNeedsMore(size_t task_id = 0) {
    OpInstructAndTrace(OpOutput::PipeSinkNeedsMore(), "Pipe");
    TaskTrace(OpOutput::PipeSinkNeedsMore(), task_id);
  }
  void PipeHasMore() { OpInstructAndTrace(OpOutput::SourcePipeHasMore(Batch{}), "Pipe"); }
  void PipeYield(size_t task_id = 0) {
    OpInstructAndTrace(OpOutput::PipeYield(), "Pipe");
    TaskTrace(OpOutput::PipeYield(), task_id);
  }
  void PipeYieldBack(size_t task_id = 0) {
    OpInstructAndTrace(OpOutput::PipeYieldBack(), "Pipe");
    TaskTrace(OpOutput::PipeYieldBack());
  }
  void PipeBlocked(size_t task_id = 0) {
    OpInstructAndTrace(OpOutput::Blocked(nullptr), "Pipe");
    TaskTrace(OpOutput::Blocked(nullptr), task_id);
  }

  void DrainHasMore() {
    has_drain_ = true;
    OpInstructAndTrace(OpOutput::SourcePipeHasMore(Batch{}), "Drain");
  }
  void DrainYield(size_t task_id = 0) {
    has_drain_ = true;
    OpInstructAndTrace(OpOutput::PipeYield(), "Drain");
    TaskTrace(OpOutput::PipeYield(), task_id);
  }
  void DrainYieldBack(size_t task_id = 0) {
    has_drain_ = true;
    OpInstructAndTrace(OpOutput::PipeYieldBack(), "Drain");
    TaskTrace(OpOutput::PipeYieldBack(), task_id);
  }
  void DrainBlocked(size_t task_id = 0) {
    has_drain_ = true;
    OpInstructAndTrace(OpOutput::Blocked(nullptr), "Drain");
    TaskTrace(OpOutput::Blocked(nullptr), task_id);
  }
  void DrainFinished(std::optional<Batch> batch = std::nullopt) {
    has_drain_ = true;
    OpInstructAndTrace(OpOutput::Finished(std::move(batch)), "Drain");
  }

  PipelinePipe Pipe() override {
    return
        [&](const PipelineContext&, const TaskContext& task_context, ThreadId thread_id,
            std::optional<Batch>) -> OpResult { return Fetch(task_context, thread_id); };
  }

  PipelineDrain Drain() override {
    if (!has_drain_) {
      return nullptr;
    }
    return [&](const PipelineContext&, const TaskContext& task_context,
               ThreadId thread_id) -> OpResult { return Fetch(task_context, thread_id); };
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
  explicit ImperativeSink(std::string name, ImperativeContext* context,
                          ImperativePipeline* pipeline)
      : ImperativeOp(std::move(name), "ImperativeSink", context, pipeline),
        SinkOp(ImperativeOp::Name(), ImperativeOp::Desc()) {}

  void NeedsMore(size_t task_id = 0) {
    OpInstructAndTrace(OpOutput::PipeSinkNeedsMore(), "Sink");
    TaskTrace(OpOutput::PipeSinkNeedsMore(), task_id);
  }
  void Blocked(size_t task_id = 0) {
    OpInstructAndTrace(OpOutput::Blocked(nullptr), "Sink");
    TaskTrace(OpOutput::Blocked(nullptr), task_id);
  }

  PipelineSink Sink() override {
    return
        [&](const PipelineContext&, const TaskContext& task_context, ThreadId thread_id,
            std::optional<Batch>) -> OpResult { return Fetch(task_context, thread_id); };
  }

  TaskGroups Frontend(const PipelineContext&) override { return {}; }

  std::optional<TaskGroup> Backend(const PipelineContext&) override {
    auto task_hint = TaskHint{TaskHint::Type::IO};
    auto task = Task(
        ImperativeOp::Name(), ImperativeOp::Desc(),
        [&](const TaskContext& task_context, TaskId task_id) -> TaskResult {
          return context_->TryResume();
        },
        std::move(task_hint));

    auto notify_finish = [&](const TaskContext&) {
      context_->Finish();
      return Status::OK();
    };

    return TaskGroup(ImperativeOp::Name(), ImperativeOp::Desc(), std::move(task), 1,
                     std::nullopt, std::move(notify_finish));
  }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }
};

ImperativeSource* ImperativePipeline::DeclSource(std::string name) {
  auto source = std::make_shared<ImperativeSource>(std::move(name), context_, this);
  sources_.emplace_back(source);
  return source.get();
}

ImperativePipe* ImperativePipeline::DeclPipe(
    std::string name, std::initializer_list<ImperativeOp*> parents) {
  auto pipe = std::make_shared<ImperativePipe>(std::move(name), context_, this);
  for (auto parent : parents) {
    parent->SetChild(pipe);
  }
  return pipe.get();
}

ImperativeSink* ImperativePipeline::DeclSink(
    std::string name, std::initializer_list<ImperativeOp*> parents) {
  auto sink = std::make_shared<ImperativeSink>(std::move(name), context_, this);
  for (auto parent : parents) {
    parent->SetChild(sink);
  }
  return sink.get();
}

ImperativeSource* ImperativePipeline::DeclImplicitSource(std::string name,
                                                         ImperativePipe* pipe) {
  auto source = std::make_unique<ImperativeSource>(std::move(name), context_, this);
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
                             const PipelineContext&, const task::TaskContext&,
                             ThreadId thread_id, const OpResult& result) override {
    auto source_name = pipeline_task.Pipeline().Channels()[channel].source_op->Name();
    traces_[thread_id].push_back(
        ImperativeTrace{std::move(source_name), "Source", result->ToString()});
    return Status::OK();
  }

  Status OnPipelinePipeEnd(const PipelineTask& pipeline_task, size_t channel, size_t pipe,
                           const PipelineContext&, const task::TaskContext&,
                           ThreadId thread_id, const OpResult& result) override {
    auto pipe_name = pipeline_task.Pipeline().Channels()[channel].pipe_ops[pipe]->Name();
    traces_[thread_id].push_back(
        ImperativeTrace{std::move(pipe_name), "Pipe", result->ToString()});
    return Status::OK();
  }

  Status OnPipelineDrainEnd(const PipelineTask& pipeline_task, size_t channel,
                            size_t pipe, const PipelineContext&, const task::TaskContext&,
                            ThreadId thread_id, const OpResult& result) override {
    auto pipe_name = pipeline_task.Pipeline().Channels()[channel].pipe_ops[pipe]->Name();
    traces_[thread_id].push_back(
        ImperativeTrace{std::move(pipe_name), "Drain", result->ToString()});
    return Status::OK();
  }

  Status OnPipelineSinkEnd(const PipelineTask& pipeline_task, size_t channel,
                           const PipelineContext&, const task::TaskContext&,
                           ThreadId thread_id, const OpResult& result) override {
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
    auto dop = context.dop;

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
    std::unique_ptr<TaskGroupHandle> sink_be_handle = nullptr;
    auto sink_be = logical_pipeline.SinkOp()->Backend(pipeline_context);
    if (sink_be.has_value()) {
      auto result = scheduler.Schedule(schedule_context, std::move(sink_be.value()));
      ASSERT_OK(result);
      sink_be_handle = std::move(*result);
    }

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

    if (sink_be_handle != nullptr) {
      auto result = sink_be_handle->Wait(schedule_context);
      ASSERT_OK(result);
      ASSERT_TRUE(result->IsFinished());
    }

    auto& traces_act = tracer->Traces();
    auto& traces_exp = pipeline.Traces();
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

void MakeEmptySourcePipeline(pipelang::ImperativeContext* context,
                             std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "EmptySource";
  result = std::make_unique<pipelang::ImperativePipeline>(name, context);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto sink = pipeline->DeclSink("Sink", {source});
  source->Finished();
  pipeline->ChannelFinished();

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline->Traces().size(), 2);
  ASSERT_EQ(
      pipeline->Traces()[0],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(pipeline->Traces()[1],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, EmptySource) {
  pipelang::ImperativeContext context{4};
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeEmptySourcePipeline(&context, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeEmptySourceNotReadyPipeline(
    pipelang::ImperativeContext* context,
    std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "EmptySourceNotReady";
  result = std::make_unique<pipelang::ImperativePipeline>(name, context);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto sink = pipeline->DeclSink("Sink", {source});
  source->Blocked();
  source->Finished();
  pipeline->ChannelFinished();

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline->Traces().size(), 4);
  ASSERT_EQ(pipeline->Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(pipeline->Traces()[1],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[2],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(pipeline->Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, EmptySourceNotReady) {
  pipelang::ImperativeContext context{4};
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeEmptySourceNotReadyPipeline(&context, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeTwoSourcesOneNotReadyPipeline(
    pipelang::ImperativeContext* context,
    std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "TwoSourceOneNotReady";
  result = std::make_unique<pipelang::ImperativePipeline>(name, context);
  auto pipeline = result.get();
  auto source1 = pipeline->DeclSource("Source1");
  auto source2 = pipeline->DeclSource("Source2");
  auto sink = pipeline->DeclSink("Sink", {source1, source2});
  source1->Blocked();
  source2->Finished();
  pipeline->ChannelFinished();
  source1->Finished();
  pipeline->ChannelFinished();

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 2);
  ASSERT_EQ(logical.Channels()[0].source_op, source1);
  ASSERT_EQ(logical.Channels()[1].source_op, source2);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline->Traces().size(), 6);
  ASSERT_EQ(pipeline->Traces()[0],
            (pipelang::ImperativeTrace{"Source1", "Source",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(pipeline->Traces()[1],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[2],
      (pipelang::ImperativeTrace{"Source2", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(pipeline->Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[4],
      (pipelang::ImperativeTrace{"Source1", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(pipeline->Traces()[5],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, TwoSourceOneNotReady) {
  pipelang::ImperativeContext context{4};
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeTwoSourcesOneNotReadyPipeline(&context, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeOnePassPipeline(pipelang::ImperativeContext* context,
                         std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "OnePass";
  result = std::make_unique<pipelang::ImperativePipeline>(name, context);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto sink = pipeline->DeclSink("Sink", {source});
  source->HasMore();
  sink->NeedsMore();
  source->Finished();
  pipeline->ChannelFinished();

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline->Traces().size(), 5);
  ASSERT_EQ(pipeline->Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[1],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[2],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[3],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(pipeline->Traces()[4],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, OnePass) {
  pipelang::ImperativeContext context{4};
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeOnePassPipeline(&context, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeOnePassDirectFinishPipeline(
    pipelang::ImperativeContext* context,
    std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "OnePassDirectFinish";
  result = std::make_unique<pipelang::ImperativePipeline>(name, context);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto sink = pipeline->DeclSink("Sink", {source});
  source->Finished(Batch{});
  sink->NeedsMore();
  pipeline->ChannelFinished();

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_TRUE(logical.Channels()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline->Traces().size(), 4);
  ASSERT_EQ(
      pipeline->Traces()[0],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[1],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[2],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, OnePassDirectFinish) {
  pipelang::ImperativeContext context{4};
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeOnePassDirectFinishPipeline(&context, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeOnePassWithPipePipeline(pipelang::ImperativeContext* context,
                                 std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "OnePassWithPipe";
  result = std::make_unique<pipelang::ImperativePipeline>(name, context);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore();
  pipe->PipeEven();
  sink->NeedsMore();
  source->Finished();
  pipeline->ChannelFinished();

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline->Traces().size(), 6);
  ASSERT_EQ(pipeline->Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[1],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[4],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(pipeline->Traces()[5],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, OnePassWithPipe) {
  pipelang::ImperativeContext context{4};
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeOnePassWithPipePipeline(&context, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakePipeNeedsMorePipeline(pipelang::ImperativeContext* context,
                               std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "PipeNeedsMore";
  result = std::make_unique<pipelang::ImperativePipeline>(name, context);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore();
  pipe->PipeNeedsMore();
  source->Finished(Batch{});
  pipe->PipeEven();
  sink->NeedsMore();
  pipeline->ChannelFinished();

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline->Traces().size(), 8);
  ASSERT_EQ(pipeline->Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[1],
            (pipelang::ImperativeTrace{"Pipe", "Pipe",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[2],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[3],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[4],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[5],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[6],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[7],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, PipeNeedsMore) {
  pipelang::ImperativeContext context{4};
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakePipeNeedsMorePipeline(&context, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakePipeHasMorePipeline(pipelang::ImperativeContext* context,
                             std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "PipeHasMore";
  result = std::make_unique<pipelang::ImperativePipeline>(name, context);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore();
  pipe->PipeHasMore();
  sink->NeedsMore();
  pipe->PipeEven();
  sink->NeedsMore();
  source->Finished();
  pipeline->ChannelFinished();

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline->Traces().size(), 9);
  ASSERT_EQ(pipeline->Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[1],
            (pipelang::ImperativeTrace{"Pipe", "Pipe",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[4],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[5],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[6],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[7],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[8],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, PipeHasMore) {
  pipelang::ImperativeContext context{4};
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakePipeHasMorePipeline(&context, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakePipeYieldPipeline(pipelang::ImperativeContext* context,
                           std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "PipeYield";
  result = std::make_unique<pipelang::ImperativePipeline>(name, context);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore();
  pipe->PipeYield();
  pipe->PipeYieldBack();
  pipe->PipeNeedsMore();
  source->Finished();
  pipeline->ChannelFinished();

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline->Traces().size(), 9);
  ASSERT_EQ(pipeline->Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[1],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(pipeline->Traces()[2],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[3],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(pipeline->Traces()[4],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(pipeline->Traces()[5],
            (pipelang::ImperativeTrace{"Pipe", "Pipe",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[6],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[7],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[8],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, PipeYield) {
  pipelang::ImperativeContext context{4};
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakePipeYieldPipeline(&context, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeDrainPipeline(pipelang::ImperativeContext* context,
                       std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "Drain";
  result = std::make_unique<pipelang::ImperativePipeline>(name, context);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore();
  pipe->PipeEven();
  sink->NeedsMore();
  source->Finished(Batch{});
  pipe->PipeEven();
  sink->NeedsMore();
  pipe->DrainHasMore();
  sink->NeedsMore();
  pipe->DrainYield();
  pipe->DrainYieldBack();
  pipe->DrainHasMore();
  sink->NeedsMore();
  pipe->DrainFinished(Batch{});
  sink->NeedsMore();
  pipeline->ChannelFinished();

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Channels()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline->Traces().size(), 22);
  ASSERT_EQ(pipeline->Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[1],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[4],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[5],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[6],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[7],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[8],
            (pipelang::ImperativeTrace{"Pipe", "Drain",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[9],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[10],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[11],
      (pipelang::ImperativeTrace{"Pipe", "Drain", OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(pipeline->Traces()[12],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[13],
      (pipelang::ImperativeTrace{"Pipe", "Drain", OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(pipeline->Traces()[14],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(pipeline->Traces()[15],
            (pipelang::ImperativeTrace{"Pipe", "Drain",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[16],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[17],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[18],
      (pipelang::ImperativeTrace{"Pipe", "Drain", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[19],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[20],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[21],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, Drain) {
  pipelang::ImperativeContext context{4};
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeDrainPipeline(&context, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeImplicitSourcePipeline(pipelang::ImperativeContext* context,
                                std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "ImplicitSource";
  result = std::make_unique<pipelang::ImperativePipeline>(name, context);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto implicit_source = pipeline->DeclImplicitSource("ImplicitSource", pipe);
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->Finished(Batch{});
  pipe->PipeEven();
  sink->NeedsMore();
  pipeline->ChannelFinished();
  implicit_source->Finished(Batch{});
  sink->NeedsMore(1);
  pipeline->ChannelFinished(1);

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline->Traces().size(), 9);
  ASSERT_EQ(
      pipeline->Traces()[0],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[1],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[4],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
  ASSERT_EQ(pipeline->Traces()[5],
            (pipelang::ImperativeTrace{"ImplicitSource", "Source",
                                       OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[6],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[7],
            (pipelang::ImperativeTrace{TaskName(name, 1), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[8],
            (pipelang::ImperativeTrace{TaskName(name, 1), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, ImplicitSource) {
  pipelang::ImperativeContext context{4};
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeImplicitSourcePipeline(&context, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

void MakeBackpressurePipeline(pipelang::ImperativeContext* context,
                              std::unique_ptr<pipelang::ImperativePipeline>& result) {
  auto name = "Backpressure";
  result = std::make_unique<pipelang::ImperativePipeline>(name, context);
  auto pipeline = result.get();
  auto source = pipeline->DeclSource("Source");
  auto pipe = pipeline->DeclPipe("Pipe", {source});
  auto sink = pipeline->DeclSink("Sink", {pipe});
  source->HasMore();
  pipe->PipeEven();
  sink->Blocked();
  sink->Blocked();
  sink->NeedsMore();
  source->Finished(Batch{});
  pipe->PipeEven();
  sink->NeedsMore();
  pipeline->ChannelFinished();

  auto logical = pipeline->ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), name);
  ASSERT_EQ(logical.Channels().size(), 1);
  ASSERT_EQ(logical.Channels()[0].source_op, source);
  ASSERT_EQ(logical.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline->Traces().size(), 13);
  ASSERT_EQ(pipeline->Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[1],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[2],
      (pipelang::ImperativeTrace{"Sink", "Sink", OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(pipeline->Traces()[3],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[4],
      (pipelang::ImperativeTrace{"Sink", "Sink", OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(pipeline->Traces()[5],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Blocked(nullptr).ToString()}));
  ASSERT_EQ(pipeline->Traces()[6],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[7],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[8],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(
      pipeline->Traces()[9],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline->Traces()[10],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[11],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline->Traces()[12],
            (pipelang::ImperativeTrace{TaskName(name), "Run",
                                       OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, Backpressure) {
  pipelang::ImperativeContext context{4};
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeBackpressurePipeline(&context, pipeline);
  this->TestTracePipeline(context, *pipeline);
}

TYPED_TEST(PipelineTaskTest, MultiPipe) {
  pipelang::ImperativeContext context{4};
  auto name = "MultiPipe";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, &context);
  auto source = pipeline->DeclSource("Source");
  auto pipe1 = pipeline->DeclPipe("Pipe1", {source});
  auto pipe2 = pipeline->DeclPipe("Pipe2", {pipe1});
  auto sink = pipeline->DeclSink("Sink", {pipe2});
  source->HasMore();
  pipe1->PipeEven();
  pipe2->PipeEven();
  sink->NeedsMore();

  source->HasMore();
  pipe1->PipeNeedsMore();

  source->HasMore();
  pipe1->PipeNeedsMore();

  source->Blocked();

  source->HasMore();
  pipe1->PipeHasMore();
  pipe2->PipeYield();
  pipe2->PipeYieldBack();
  pipe2->PipeNeedsMore();

  pipe1->PipeYield();
  pipe1->PipeYieldBack();
  pipe1->PipeHasMore();
  pipe2->PipeNeedsMore();

  pipe1->PipeEven();
  pipe2->PipeEven();
  sink->NeedsMore();

  source->HasMore();
  pipe1->PipeHasMore();
  pipe2->PipeHasMore();
  sink->NeedsMore();

  pipe2->PipeHasMore();
  sink->NeedsMore();

  pipe2->PipeEven();
  sink->NeedsMore();

  pipe1->PipeEven();
  pipe2->PipeHasMore();
  sink->NeedsMore();

  pipe2->PipeEven();
  sink->NeedsMore();

  source->Finished(Batch{});
  pipe1->PipeEven();
  pipe2->PipeEven();
  sink->NeedsMore();

  pipeline->ChannelFinished();

  this->TestTracePipeline(context, *pipeline);
}

TYPED_TEST(PipelineTaskTest, MultiDrain) {
  pipelang::ImperativeContext context{4};
  auto name = "MultiDrain";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, &context);
  auto source = pipeline->DeclSource("Source");
  auto pipe1 = pipeline->DeclPipe("Pipe1", {source});
  auto pipe2 = pipeline->DeclPipe("Pipe2", {pipe1});
  auto sink = pipeline->DeclSink("Sink", {pipe2});
  source->Finished();

  pipe1->DrainHasMore();
  pipe2->PipeYield();
  pipe2->PipeYieldBack();
  pipe2->PipeNeedsMore();

  pipe1->DrainHasMore();
  pipe2->PipeEven();
  sink->NeedsMore();

  pipe1->DrainFinished();
  pipe2->DrainHasMore();
  sink->NeedsMore();

  pipe2->DrainYield();
  pipe2->DrainYieldBack();
  pipe2->DrainHasMore();
  sink->NeedsMore();

  pipe2->DrainFinished();
  pipeline->ChannelFinished();

  this->TestTracePipeline(context, *pipeline);
}

TYPED_TEST(PipelineTaskTest, MultiChannel) {
  pipelang::ImperativeContext context{4};
  auto name = "MultiChannel";
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>(name, &context);
  auto source1 = pipeline->DeclSource("Source1");
  auto source2 = pipeline->DeclSource("Source2");
  auto pipe1 = pipeline->DeclPipe("Pipe1", {source1});
  auto pipe2 = pipeline->DeclPipe("Pipe2", {source2});
  auto sink = pipeline->DeclSink("Sink", {pipe1, pipe2});

  source1->Blocked();
  source2->Blocked();

  source1->HasMore();
  pipe1->PipeEven();
  sink->NeedsMore();

  source1->Blocked();
  source2->HasMore();
  pipe2->PipeEven();
  sink->NeedsMore();

  // TODO: More.

  source1->Finished();
  pipeline->ChannelFinished();
  source2->Finished();
  pipeline->ChannelFinished();

  this->TestTracePipeline(context, *pipeline);
}

// TODO: Pipe/Drain after implicit sources.
// TODO: Multi-channel.
// TODO: Multi-physical.
// TODO: Backpressure everywhere.
