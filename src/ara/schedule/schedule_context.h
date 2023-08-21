#pragma once

#include <ara/common/defines.h>
#include <ara/schedule/schedule_observer.h>

namespace ara::schedule {

struct ScheduleContext {
  QueryId query_id;
  std::unique_ptr<ScheduleObserver> schedule_observer = nullptr;
};

}  // namespace ara::schedule
