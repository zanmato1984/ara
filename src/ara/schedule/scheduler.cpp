#include "scheduler.h"
#include "schedule_context.h"
#include "schedule_observer.h"

#include <ara/task/task_context.h>
#include <ara/task/task_group.h>
#include <ara/task/task_status.h>
#include <ara/util/util.h>

namespace ara::schedule {

using task::BackpressureAndResetPair;
using task::Task;
using task::TaskContext;
using task::TaskGroup;
using task::TaskId;
using task::TaskObserver;
using task::TaskResult;

TaskGroupHandle::TaskGroupHandle(const std::string& name, const TaskGroup& task_group,
                                 TaskContext task_context)
    : name_(name + "(" + task_group.Name() + ")"),
      desc_(name + "(" + task_group.Desc() + ")"),
      task_group_(task_group),
      task_context_(std::move(task_context)) {}

TaskResult TaskGroupHandle::Wait(const ScheduleContext& context) {
  if (context.schedule_observer != nullptr) {
    ARA_RETURN_NOT_OK(context.schedule_observer->OnWaitTaskGroupBegin(*this, context));
  }
  ARA_RETURN_NOT_OK(task_group_.NotifyFinish(task_context_));
  auto status = DoWait(context);
  if (context.schedule_observer != nullptr) {
    ARA_RETURN_NOT_OK(
        context.schedule_observer->OnWaitTaskGroupEnd(*this, context, status));
  }
  return status;
}

Result<std::unique_ptr<TaskGroupHandle>> Scheduler::Schedule(
    const ScheduleContext& context, const TaskGroup& tg) {
  if (context.schedule_observer != nullptr) {
    ARA_RETURN_NOT_OK(
        context.schedule_observer->OnScheduleTaskGroupBegin(*this, context, tg));
  }
  auto result = DoSchedule(context, tg);
  if (context.schedule_observer != nullptr) {
    ARA_RETURN_NOT_OK(
        context.schedule_observer->OnScheduleTaskGroupEnd(*this, context, tg, result));
  }
  return result;
}

TaskContext Scheduler::MakeTaskContext(const ScheduleContext& schedule_context) const {
  auto task_observer = TaskObserver::Make(*schedule_context.query_context);
  auto backpressure_factory = MakeBackpressurePairFactory(schedule_context);
  return {schedule_context.query_context, schedule_context.query_id,
          std::move(backpressure_factory), std::move(task_observer)};
}

std::unique_ptr<Scheduler> Scheduler::Make(const QueryContext&) { return nullptr; }

}  // namespace ara::schedule
