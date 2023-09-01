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

  friend void PrintTo(const ImperativeTrace& trace, std::ostream* os) {
    *os << trace.op << "::" << trace.method << "(" << trace.payload.value_or("") << ")";
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

  ImperativePipe* DeclPipe(std::string, std::initializer_list<ImperativeOp*>);

  ImperativeSink* DeclSink(std::string, std::initializer_list<ImperativeOp*>);

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

  ImperativeInstruction Fetch(ThreadId thread_id) {
    return instructions_[thread_locals_[thread_id].pc++];
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

  void NotReady() { InstructAndTrace(OpOutput::SourceNotReady(), "Source"); }
  void HasMore() { InstructAndTrace(OpOutput::SourcePipeHasMore(Batch{}), "Source"); }
  void Finished(std::optional<Batch> batch = std::nullopt) {
    InstructAndTrace(OpOutput::Finished(std::move(batch)), "Source");
  }

  PipelineSource Source() override {
    return [&](const PipelineContext&, const TaskContext&,
               ThreadId thread_id) -> OpResult { return Fetch(thread_id); };
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

  void PipeEven() { InstructAndTrace(OpOutput::PipeEven(Batch{}), "Pipe"); }
  void PipeNeedsMore() { InstructAndTrace(OpOutput::PipeSinkNeedsMore(), "Pipe"); }
  void PipeHasMore() { InstructAndTrace(OpOutput::SourcePipeHasMore(Batch{}), "Pipe"); }
  void PipeYield() { InstructAndTrace(OpOutput::PipeYield(), "Pipe"); }
  void PipeYieldBack() { InstructAndTrace(OpOutput::PipeYieldBack(), "Pipe"); }

  void DrainHasMore() {
    has_drain_ = true;
    InstructAndTrace(OpOutput::SourcePipeHasMore(Batch{}), "Drain");
  }
  void DrainYield() {
    has_drain_ = true;
    InstructAndTrace(OpOutput::PipeYield(), "Drain");
  }
  void DrainFinished(std::optional<Batch> batch = std::nullopt) {
    has_drain_ = true;
    InstructAndTrace(OpOutput::Finished(std::move(batch)), "Drain");
  }
  void DrainYieldBack() {
    has_drain_ = true;
    InstructAndTrace(OpOutput::PipeYieldBack(), "Drain");
  }

  PipelinePipe Pipe() override {
    return [&](const PipelineContext&, const TaskContext&, ThreadId thread_id,
               std::optional<Batch>) -> OpResult { return Fetch(thread_id); };
  }

  std::optional<PipelineDrain> Drain() override {
    if (!has_drain_) {
      return std::nullopt;
    }
    return [&](const PipelineContext&, const TaskContext&,
               ThreadId thread_id) -> OpResult { return Fetch(thread_id); };
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
        SinkOp(ImperativeOp::Name(), ImperativeOp::Desc()),
        finished_(false) {}

  void NeedsMore() { InstructAndTrace(OpOutput::PipeSinkNeedsMore(), "Sink"); }
  void Backpressure() { InstructAndTrace(OpOutput::SinkBackpressure({}), "Sink"); }

  PipelineSink Sink() override {
    return [&](const PipelineContext&, const TaskContext& task_context,
               ThreadId thread_id, std::optional<Batch>) -> OpResult {
      auto result = Fetch(thread_id);
      if (result.ok() && result->IsSinkBackpressure()) {
        ARA_CHECK(task_context.backpressure_pair_factory.has_value());
        auto backpressure_pair = task_context.backpressure_pair_factory.value()(
            task_context, Task("", "", {}), thread_id);
        ARA_RETURN_NOT_OK(backpressure_pair);
        {
          std::lock_guard<std::mutex> lock(mutex_);
          backpressure_callbacks_.push_back(std::move(backpressure_pair->second));
        }
        return OpOutput::SinkBackpressure(std::move(backpressure_pair->first));
      } else {
        return std::move(result);
      }
    };
  }

  TaskGroups Frontend(const PipelineContext&) override { return {}; }

  std::optional<TaskGroup> Backend(const PipelineContext&) override {
    auto task_hint = TaskHint{TaskHint::Type::IO};
    auto task = Task(
        ImperativeOp::Name(), ImperativeOp::Desc(),
        [&](const TaskContext& task_context, TaskId task_id) -> TaskResult {
          if (finished_) {
            return TaskStatus::Finished();
          }

          bool work = false;
          {
            std::unique_lock<std::mutex> lock(mutex_);
            work = backpressure_callbacks_.size() == pipeline_->Dop();
          }
          if (work) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            std::unique_lock<std::mutex> lock(mutex_);
            for (auto& callback : backpressure_callbacks_) {
              ARA_RETURN_NOT_OK(callback());
            }
            backpressure_callbacks_.clear();
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          return TaskStatus::Continue();
        },
        std::move(task_hint));

    auto notify_finish = [&](const TaskContext&) {
      finished_ = true;
      return Status::OK();
    };

    return TaskGroup(ImperativeOp::Name(), ImperativeOp::Desc(), std::move(task), 1,
                     std::nullopt, std::move(notify_finish));
  }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }

 private:
  std::atomic_bool finished_;
  std::mutex mutex_;
  std::vector<BackpressureResetCallback> backpressure_callbacks_;
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

// TODO: Trace the task begin/end.
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

      for (const auto& plex : physical_pipeline.Plexes()) {
        ASSERT_TRUE(plex.source_op->Frontend(pipeline_context).empty());
        ASSERT_TRUE(!plex.source_op->Backend(pipeline_context).has_value());
      }

      Task task(pipeline_task.Name(), pipeline_task.Desc(),
                [&pipeline_context, &pipeline_task](const TaskContext& task_context,
                                                    TaskId task_id) -> TaskResult {
                  return pipeline_task(pipeline_context, task_context, task_id);
                });
      TaskGroup task_group(pipeline_task.Name(), pipeline_task.Desc(), std::move(task),
                           dop, std::nullopt, std::nullopt);

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

void MakeEmptySourcePipeline(size_t dop,
                             std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("EmptySource", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto sink = pipeline.DeclSink("Sink", {source});
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

void MakeEmptySourceNotReadyPipeline(
    size_t dop, std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("EmptySourceNotReady", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto sink = pipeline.DeclSink("Sink", {source});
  source->NotReady();
  source->Finished();

  auto logical = pipeline.ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), "EmptySourceNotReady");
  ASSERT_EQ(logical.Plexes().size(), 1);
  ASSERT_EQ(logical.Plexes()[0].source_op, source);
  ASSERT_TRUE(logical.Plexes()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline.Traces().size(), 2);
  ASSERT_EQ(pipeline.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourceNotReady().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[1],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, EmptySourceNotReady) {
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeEmptySourceNotReadyPipeline(4, pipeline);
  this->TestTracePipeline(*pipeline);
}

void MakeTwoSourcesOneNotReadyPipeline(
    size_t dop, std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("TwoSourceOneNotReady", dop);
  auto& pipeline = *result;
  auto source1 = pipeline.DeclSource("Source1");
  auto source2 = pipeline.DeclSource("Source2");
  auto sink = pipeline.DeclSink("Sink", {source1, source2});
  source1->NotReady();
  source2->Finished();
  source1->Finished();

  auto logical = pipeline.ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), "TwoSourceOneNotReady");
  ASSERT_EQ(logical.Plexes().size(), 2);
  ASSERT_EQ(logical.Plexes()[0].source_op, source1);
  ASSERT_EQ(logical.Plexes()[1].source_op, source2);
  ASSERT_TRUE(logical.Plexes()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline.Traces().size(), 3);
  ASSERT_EQ(pipeline.Traces()[0],
            (pipelang::ImperativeTrace{"Source1", "Source",
                                       OpOutput::SourceNotReady().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[1],
      (pipelang::ImperativeTrace{"Source2", "Source", OpOutput::Finished().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[2],
      (pipelang::ImperativeTrace{"Source1", "Source", OpOutput::Finished().ToString()}));
}

TYPED_TEST(PipelineTaskTest, TwoSourceOneNotReady) {
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeTwoSourcesOneNotReadyPipeline(4, pipeline);
  this->TestTracePipeline(*pipeline);
}

void MakeOnePassPipeline(size_t dop,
                         std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("OnePass", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto sink = pipeline.DeclSink("Sink", {source});
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

void MakeOnePassDirectFinishPipeline(
    size_t dop, std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("OnePassDirectFinish", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto sink = pipeline.DeclSink("Sink", {source});
  source->Finished(Batch{});
  sink->NeedsMore();

  auto logical = pipeline.ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), "OnePassDirectFinish");
  ASSERT_EQ(logical.Plexes().size(), 1);
  ASSERT_EQ(logical.Plexes()[0].source_op, source);
  ASSERT_TRUE(logical.Plexes()[0].pipe_ops.empty());
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline.Traces().size(), 2);
  ASSERT_EQ(
      pipeline.Traces()[0],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[1],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
}

TYPED_TEST(PipelineTaskTest, OnePassDirectFinish) {
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeOnePassDirectFinishPipeline(4, pipeline);
  this->TestTracePipeline(*pipeline);
}

void MakeOnePassWithPipePipeline(size_t dop,
                                 std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("OnePassWithPipe", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto pipe = pipeline.DeclPipe("Pipe", {source});
  auto sink = pipeline.DeclSink("Sink", {pipe});
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

void MakePipeNeedsMorePipeline(size_t dop,
                               std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("PipeNeedsMore", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto pipe = pipeline.DeclPipe("Pipe", {source});
  auto sink = pipeline.DeclSink("Sink", {pipe});
  source->HasMore();
  pipe->PipeNeedsMore();
  source->Finished(Batch{});
  pipe->PipeEven();
  sink->NeedsMore();

  auto logical = pipeline.ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), "PipeNeedsMore");
  ASSERT_EQ(logical.Plexes().size(), 1);
  ASSERT_EQ(logical.Plexes()[0].source_op, source);
  ASSERT_EQ(logical.Plexes()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Plexes()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline.Traces().size(), 5);
  ASSERT_EQ(pipeline.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[1],
            (pipelang::ImperativeTrace{"Pipe", "Pipe",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[2],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[3],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[4],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
}

TYPED_TEST(PipelineTaskTest, PipeNeedsMore) {
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakePipeNeedsMorePipeline(4, pipeline);
  this->TestTracePipeline(*pipeline);
}

void MakePipeHasMorePipeline(size_t dop,
                             std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("PipeHasMore", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto pipe = pipeline.DeclPipe("Pipe", {source});
  auto sink = pipeline.DeclSink("Sink", {pipe});
  source->HasMore();
  pipe->PipeHasMore();
  sink->NeedsMore();
  pipe->PipeEven();
  sink->NeedsMore();
  source->Finished();

  auto logical = pipeline.ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), "PipeHasMore");
  ASSERT_EQ(logical.Plexes().size(), 1);
  ASSERT_EQ(logical.Plexes()[0].source_op, source);
  ASSERT_EQ(logical.Plexes()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Plexes()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline.Traces().size(), 6);
  ASSERT_EQ(pipeline.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[1],
            (pipelang::ImperativeTrace{"Pipe", "Pipe",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[3],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[4],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[5],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
}

TYPED_TEST(PipelineTaskTest, PipeHasMore) {
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakePipeHasMorePipeline(4, pipeline);
  this->TestTracePipeline(*pipeline);
}

void MakePipeYieldPipeline(size_t dop,
                           std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("PipeYield", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto pipe = pipeline.DeclPipe("Pipe", {source});
  auto sink = pipeline.DeclSink("Sink", {pipe});
  source->HasMore();
  pipe->PipeYield();
  pipe->PipeYieldBack();
  pipe->PipeEven();
  sink->NeedsMore();
  source->HasMore();
  pipe->PipeYield();
  pipe->PipeYieldBack();
  pipe->PipeNeedsMore();
  source->Finished();

  auto logical = pipeline.ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), "PipeYield");
  ASSERT_EQ(logical.Plexes().size(), 1);
  ASSERT_EQ(logical.Plexes()[0].source_op, source);
  ASSERT_EQ(logical.Plexes()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Plexes()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline.Traces().size(), 10);
  ASSERT_EQ(pipeline.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[1], (pipelang::ImperativeTrace{
                                      "Pipe", "Pipe", OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[2],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[3],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[4],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline.Traces()[5],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[6], (pipelang::ImperativeTrace{
                                      "Pipe", "Pipe", OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[7],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(pipeline.Traces()[8],
            (pipelang::ImperativeTrace{"Pipe", "Pipe",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[9],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
}

TYPED_TEST(PipelineTaskTest, PipeYield) {
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakePipeYieldPipeline(4, pipeline);
  this->TestTracePipeline(*pipeline);
}

void MakeDrainPipeline(size_t dop,
                       std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("Drain", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto pipe = pipeline.DeclPipe("Pipe", {source});
  auto sink = pipeline.DeclSink("Sink", {pipe});
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

  auto logical = pipeline.ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), "Drain");
  ASSERT_EQ(logical.Plexes().size(), 1);
  ASSERT_EQ(logical.Plexes()[0].source_op, source);
  ASSERT_EQ(logical.Plexes()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.Plexes()[0].pipe_ops[0], pipe);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline.Traces().size(), 14);
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
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[4],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[5],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline.Traces()[6],
            (pipelang::ImperativeTrace{"Pipe", "Drain",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[7],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[8],
      (pipelang::ImperativeTrace{"Pipe", "Drain", OpOutput::PipeYield().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[9],
      (pipelang::ImperativeTrace{"Pipe", "Drain", OpOutput::PipeYieldBack().ToString()}));
  ASSERT_EQ(pipeline.Traces()[10],
            (pipelang::ImperativeTrace{"Pipe", "Drain",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[11],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[12],
      (pipelang::ImperativeTrace{"Pipe", "Drain", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[13],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
}

TYPED_TEST(PipelineTaskTest, Drain) {
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeDrainPipeline(4, pipeline);
  this->TestTracePipeline(*pipeline);
}

void MakeImplicitSourcePipeline(size_t dop,
                                std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("ImplicitSource", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto pipe = pipeline.DeclPipe("Pipe", {source});
  auto implicit_source = pipeline.DeclImplicitSource("ImplicitSource", pipe);
  auto sink = pipeline.DeclSink("Sink", {pipe});
  source->Finished(Batch{});
  pipe->PipeEven();
  sink->NeedsMore();
  implicit_source->Finished(Batch{});
  sink->NeedsMore();

  auto logical = pipeline.ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), "ImplicitSource");
  ASSERT_EQ(logical.Plexes().size(), 1);
  ASSERT_EQ(logical.Plexes()[0].source_op, source);
  ASSERT_EQ(logical.Plexes()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline.Traces().size(), 5);
  ASSERT_EQ(
      pipeline.Traces()[0],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[1],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
  ASSERT_EQ(pipeline.Traces()[3],
            (pipelang::ImperativeTrace{"ImplicitSource", "Source",
                                       OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[4],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
}

TYPED_TEST(PipelineTaskTest, ImplicitSource) {
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeImplicitSourcePipeline(4, pipeline);
  this->TestTracePipeline(*pipeline);
}

void MakeBackpressurePipeline(size_t dop,
                              std::unique_ptr<pipelang::ImperativePipeline>& result) {
  result = std::make_unique<pipelang::ImperativePipeline>("Backpressure", dop);
  auto& pipeline = *result;
  auto source = pipeline.DeclSource("Source");
  auto pipe = pipeline.DeclPipe("Pipe", {source});
  auto sink = pipeline.DeclSink("Sink", {pipe});
  source->HasMore();
  pipe->PipeEven();
  sink->Backpressure();
  source->Finished(Batch{});
  pipe->PipeEven();
  sink->NeedsMore();

  auto logical = pipeline.ToLogicalPipeline();
  ASSERT_EQ(logical.Name(), "Backpressure");
  ASSERT_EQ(logical.Plexes().size(), 1);
  ASSERT_EQ(logical.Plexes()[0].source_op, source);
  ASSERT_EQ(logical.Plexes()[0].pipe_ops.size(), 1);
  ASSERT_EQ(logical.SinkOp(), sink);

  ASSERT_EQ(pipeline.Traces().size(), 6);
  ASSERT_EQ(pipeline.Traces()[0],
            (pipelang::ImperativeTrace{"Source", "Source",
                                       OpOutput::SourcePipeHasMore({}).ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[1],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[2],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::SinkBackpressure({}).ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[3],
      (pipelang::ImperativeTrace{"Source", "Source", OpOutput::Finished({}).ToString()}));
  ASSERT_EQ(
      pipeline.Traces()[4],
      (pipelang::ImperativeTrace{"Pipe", "Pipe", OpOutput::PipeEven({}).ToString()}));
  ASSERT_EQ(pipeline.Traces()[5],
            (pipelang::ImperativeTrace{"Sink", "Sink",
                                       OpOutput::PipeSinkNeedsMore().ToString()}));
}

TYPED_TEST(PipelineTaskTest, Backpressure) {
  std::unique_ptr<pipelang::ImperativePipeline> pipeline;
  MakeBackpressurePipeline(4, pipeline);
  this->TestTracePipeline(*pipeline);
}

TYPED_TEST(PipelineTaskTest, MultiPipe) {
  size_t dop = 1;
  auto pipeline = std::make_unique<pipelang::ImperativePipeline>("MultiPipe", dop);
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

  source->NotReady();

  source->HasMore();
  pipe1->PipeHasMore();
  pipe2->PipeNeedsMore();

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

  this->TestTracePipeline(*pipeline);
}
