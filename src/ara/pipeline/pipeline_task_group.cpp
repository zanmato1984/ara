#include "pipeline_task_group.h"
#include "pipeline_observer.h"

namespace ara::pipeline {

using task::TaskContext;

Status PipelineTask::ObserverBegin(ChainedObserver<PipelineObserver>* observer,
                                   const PipelineContext& pipeline_context,
                                   const TaskContext& task_context,
                                   ThreadId thread_id) const {
  return observer->Observe(&PipelineObserver::OnPipelineTaskBegin, *this,
                           pipeline_context, task_context, thread_id);
}

Status PipelineTask::ObserverEnd(ChainedObserver<PipelineObserver>* observer,
                                 const PipelineContext& pipeline_context,
                                 const TaskContext& task_context, ThreadId thread_id,
                                 const OpResult& result) const {
  return observer->Observe(&PipelineObserver::OnPipelineTaskEnd, *this, pipeline_context,
                           task_context, thread_id, result);
}

Status PipelineContinuation::ObserverBegin(ChainedObserver<PipelineObserver>* observer,
                                           const PipelineContext& pipeline_context,
                                           const TaskContext& task_context) const {
  return observer->Observe(&PipelineObserver::OnPipelineContinuationBegin, *this,
                           pipeline_context, task_context);
}

Status PipelineContinuation::ObserverEnd(ChainedObserver<PipelineObserver>* observer,
                                         const PipelineContext& pipeline_context,
                                         const TaskContext& task_context,
                                         const OpResult& result) const {
  return observer->Observe(&PipelineObserver::OnPipelineContinuationEnd, *this,
                           pipeline_context, task_context, result);
}

}  // namespace ara::pipeline
