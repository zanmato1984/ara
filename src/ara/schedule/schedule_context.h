#pragma once

#include <ara/common/defines.h>
#include <ara/common/query_context.h>
#include <ara/common/observer.h>

namespace ara::schedule {

class ScheduleObserver;

struct ScheduleContext {
  const QueryContext* query_context;
  QueryId query_id;
  std::unique_ptr<ChainedObserver<ScheduleObserver>> schedule_observer = nullptr;
};

}  // namespace ara::schedule
