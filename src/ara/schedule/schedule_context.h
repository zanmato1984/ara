#pragma once

#include <ara/common/defines.h>
#include <ara/common/query_context.h>

namespace ara::schedule {

class ScheduleObserver;

struct ScheduleContext {
  const QueryContext* query_context;
  QueryId query_id;
  ScheduleObserver* schedule_observer = nullptr;
};

}  // namespace ara::schedule
