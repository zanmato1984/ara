#include "task_group.h"
#include "task_context.h"

namespace ara::task {

Status TaskGroup::OnBegin(const TaskContext& context) {
  if (context.task_observer == nullptr) {
    return Status::OK();
  }

  return context.task_observer->OnTaskGroupBegin(*this, context);
}

Status TaskGroup::OnEnd(const TaskContext& context, const TaskResult& result) {
  if (context.task_observer == nullptr) {
    return Status::OK();
  }

  return context.task_observer->OnTaskGroupEnd(*this, context, result);
}

Status TaskGroup::NotifyFinish(const TaskContext& context) {
  if (!notify_.has_value()) {
    return Status::OK();
  }

  if (context.task_observer != nullptr) {
    ARA_RETURN_NOT_OK(context.task_observer->OnNotifyFinishBegin(*this, context));
  }

  auto status = notify_.value()(context);

  if (context.task_observer != nullptr) {
    ARA_RETURN_NOT_OK(context.task_observer->OnNotifyFinishEnd(*this, context, status));
  }

  return status;
}

}  // namespace ara::task
