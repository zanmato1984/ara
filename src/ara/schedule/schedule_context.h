#pragma once

#include <ara/common/defines.h>

namespace ara::schedule {

class ScheduleObserver;

struct ScheduleContext {
  QueryId query_id;
  ScheduleObserver* schedule_observer = nullptr;
};

}  // namespace ara::schedule
