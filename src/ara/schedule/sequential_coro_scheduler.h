#pragma once

#include <ara/schedule/scheduler.h>

namespace ara::schedule {

// Runs tasks cooperatively on the calling thread using C++20 coroutines.
// No background threads are used.
class SequentialCoroScheduler : public Scheduler {
 public:
  static const std::string kName;
  static const std::string kDesc;

  SequentialCoroScheduler() : Scheduler(kName, kDesc) {}

 protected:
  Result<std::unique_ptr<TaskGroupHandle>> DoSchedule(const ScheduleContext&,
                                                      const task::TaskGroup&) override;

  task::ResumerFactory MakeResumerFactory(const ScheduleContext&) const override;
  task::SingleAwaiterFactory MakeSingleAwaiterFactgory(
      const ScheduleContext&) const override;
  task::AnyAwaiterFactory MakeAnyAwaiterFactgory(const ScheduleContext&) const override;
  task::AllAwaiterFactory MakeAllAwaiterFactgory(const ScheduleContext&) const override;
};

}  // namespace ara::schedule
