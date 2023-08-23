#include "scheduler.h"
#include "schedule_context.h"
#include "schedule_observer.h"

#include <ara/task/task_context.h>
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

TaskResult TaskGroupHandle::Wait(const ScheduleContext& context) {
  if (context.schedule_observer != nullptr) {
    ARA_RETURN_NOT_OK(context.schedule_observer->OnWaitTaskGroupBegin(*this, context));
  }
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
