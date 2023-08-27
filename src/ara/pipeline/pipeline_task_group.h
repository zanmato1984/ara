#pragma once

#include <ara/common/defines.h>
#include <ara/pipeline/op/op_output.h>
#include <ara/pipeline/pipeline_context.h>
#include <ara/task/task.h>

namespace ara::pipeline {

class PipelineTask;
class PipelineContinuation;

}  // namespace ara::pipeline

namespace ara::task::internal {

template <>
struct TaskTraits<ara::pipeline::PipelineTask> {
  using ContextType = pipeline::PipelineContext;
  using Signature = std::function<pipeline::OpResult(const pipeline::PipelineContext&,
                                                     const TaskContext&, ThreadId)>;
  using ReturnType = pipeline::OpResult;
  static const auto& GetObserver(const pipeline::PipelineContext& pipeline_context) {
    return pipeline_context.pipeline_observer;
  }
};

template <>
struct TaskTraits<ara::pipeline::PipelineContinuation> {
  using ContextType = pipeline::PipelineContext;
  using Signature = std::function<pipeline::OpResult(const pipeline::PipelineContext&,
                                                     const TaskContext&)>;
  using ReturnType = pipeline::OpResult;
  static const auto& GetObserver(const pipeline::PipelineContext& pipeline_context) {
    return pipeline_context.pipeline_observer;
  }
};

}  // namespace ara::task::internal

namespace ara::pipeline {

class PipelineTask : public task::internal::InternalTask<PipelineTask> {
 public:
  using task::internal::InternalTask<PipelineTask>::InternalTask;

 private:
  Status ObserverBegin(ChainedObserver<PipelineObserver>*, const PipelineContext&,
                       const task::TaskContext&, ThreadId) const;
  Status ObserverEnd(ChainedObserver<PipelineObserver>*, const PipelineContext&,
                     const task::TaskContext&, ThreadId, const OpResult&) const;

  friend task::internal::InternalTask<PipelineTask>;
};

class PipelineContinuation : public task::internal::InternalTask<PipelineContinuation> {
 public:
  using task::internal::InternalTask<PipelineContinuation>::InternalTask;

 private:
  Status ObserverBegin(ChainedObserver<PipelineObserver>*, const PipelineContext&,
                       const task::TaskContext&) const;
  Status ObserverEnd(ChainedObserver<PipelineObserver>*, const PipelineContext&,
                     const task::TaskContext&, const OpResult&) const;

  friend task::internal::InternalTask<PipelineContinuation>;
};

class PipelineTaskGroup : public internal::Meta {
 public:
  using NotifyFinishFunc =
      std::function<Status(const PipelineContext&, const task::TaskContext&)>;

  PipelineTaskGroup(std::string name, std::string desc, PipelineTask task,
                    size_t num_tasks, std::optional<PipelineContinuation> cont,
                    std::optional<NotifyFinishFunc> notify)
      : Meta(std::move(name), std::move(desc)),
        task_(std::move(task)),
        cont_(std::move(cont)),
        num_tasks_(num_tasks),
        notify_(std::move(notify)) {}

  const PipelineTask& GetTask() const { return task_; }

  size_t NumTasks() const { return num_tasks_; }

  const std::optional<PipelineContinuation>& GetContinuation() const { return cont_; }

  Status NotifyFinish(const PipelineContext&, const task::TaskContext&) const;

 private:
  PipelineTask task_;
  size_t num_tasks_;
  std::optional<PipelineContinuation> cont_;
  std::optional<NotifyFinishFunc> notify_;
};

using PipelineTaskGroups = std::vector<PipelineTaskGroup>;

}  // namespace ara::pipeline
