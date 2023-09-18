#include "scheduler.h"
#include "schedule_context.h"
#include "schedule_observer.h"

#include <ara/task/task_context.h>
#include <ara/task/task_group.h>
#include <ara/task/task_status.h>
#include <ara/util/util.h>

namespace ara::schedule {

using task::AllAwaiterFactory;
using task::AnyAwaiterFactory;
using task::ResumerFactory;
using task::SingleAwaiterFactory;
using task::Task;
using task::TaskContext;
using task::TaskGroup;
using task::TaskId;
using task::TaskObserver;
using task::TaskResult;

#define OBSERVE(Method, ...)                                                         \
  if (context.schedule_observer != nullptr) {                                        \
    ARA_RETURN_NOT_OK(                                                               \
        context.schedule_observer->Observe(&ScheduleObserver::Method, __VA_ARGS__)); \
  }

TaskGroupHandle::TaskGroupHandle(const std::string& name, const TaskGroup& task_group,
                                 TaskContext task_context)
    : Meta(name + "(" + task_group.Name() + ")", name + "(" + task_group.Desc() + ")"),
      task_group_(task_group),
      task_context_(std::move(task_context)) {}

TaskResult TaskGroupHandle::Wait(const ScheduleContext& context) {
  OBSERVE(OnWaitTaskGroupBegin, *this, context);
  ARA_RETURN_NOT_OK(task_group_.NotifyFinish(task_context_));
  auto status = DoWait(context);
  OBSERVE(OnWaitTaskGroupEnd, *this, context, status);
  return status;
}

Result<std::unique_ptr<TaskGroupHandle>> Scheduler::Schedule(
    const ScheduleContext& context, const TaskGroup& tg) {
  OBSERVE(OnScheduleTaskGroupBegin, *this, context, tg);
  auto result = DoSchedule(context, tg);
  OBSERVE(OnScheduleTaskGroupEnd, *this, context, tg, result);
  return result;
}

TaskContext Scheduler::MakeTaskContext(const ScheduleContext& schedule_context) const {
  auto task_observer = TaskObserver::Make(*schedule_context.query_context);
  auto resumer_factory = MakeResumerFactory(schedule_context);
  auto single_awaiter_factory = MakeSingleAwaiterFactgory(schedule_context);
  auto any_awaiter_factory = MakeAnyAwaiterFactgory(schedule_context);
  auto all_awaiter_factory = MakeAllAwaiterFactgory(schedule_context);
  return {schedule_context.query_context,    std::move(resumer_factory),
          std::move(single_awaiter_factory), std::move(any_awaiter_factory),
          std::move(all_awaiter_factory),    std::move(task_observer)};
}

std::unique_ptr<Scheduler> Scheduler::Make(const QueryContext&) { return nullptr; }

}  // namespace ara::schedule
