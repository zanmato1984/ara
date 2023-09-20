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

#define OBSERVE(Method, ...)                                                     \
  if (ctx.schedule_observer != nullptr) {                                        \
    ARA_RETURN_NOT_OK(                                                           \
        ctx.schedule_observer->Observe(&ScheduleObserver::Method, __VA_ARGS__)); \
  }

TaskGroupHandle::TaskGroupHandle(const std::string& name, const TaskGroup& task_group,
                                 TaskContext task_ctx)
    : Meta(name + "(" + task_group.Name() + ")", name + "(" + task_group.Desc() + ")"),
      task_group_(task_group),
      task_ctx_(std::move(task_ctx)) {}

TaskResult TaskGroupHandle::Wait(const ScheduleContext& ctx) {
  OBSERVE(OnWaitTaskGroupBegin, *this, ctx);
  ARA_RETURN_NOT_OK(task_group_.NotifyFinish(task_ctx_));
  auto status = DoWait(ctx);
  OBSERVE(OnWaitTaskGroupEnd, *this, ctx, status);
  return status;
}

Result<std::unique_ptr<TaskGroupHandle>> Scheduler::Schedule(const ScheduleContext& ctx,
                                                             const TaskGroup& tg) {
  OBSERVE(OnScheduleTaskGroupBegin, *this, ctx, tg);
  auto result = DoSchedule(ctx, tg);
  OBSERVE(OnScheduleTaskGroupEnd, *this, ctx, tg, result);
  return result;
}

TaskContext Scheduler::MakeTaskContext(const ScheduleContext& ctx) const {
  auto task_observer = TaskObserver::Make(*ctx.query_ctx);
  auto resumer_factory = MakeResumerFactory(ctx);
  auto single_awaiter_factory = MakeSingleAwaiterFactgory(ctx);
  auto any_awaiter_factory = MakeAnyAwaiterFactgory(ctx);
  auto all_awaiter_factory = MakeAllAwaiterFactgory(ctx);
  return {ctx.query_ctx,
          std::move(resumer_factory),
          std::move(single_awaiter_factory),
          std::move(any_awaiter_factory),
          std::move(all_awaiter_factory),
          std::move(task_observer)};
}

std::unique_ptr<Scheduler> Scheduler::Make(const QueryContext&) { return nullptr; }

}  // namespace ara::schedule
