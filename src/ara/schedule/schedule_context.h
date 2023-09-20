#pragma once

#include <ara/common/defines.h>
#include <ara/common/observer.h>

namespace ara {

class QueryContext;

namespace schedule {

class ScheduleObserver;

struct ScheduleContext {
  const QueryContext* query_ctx;
  std::unique_ptr<ChainedObserver<ScheduleObserver>> schedule_observer = nullptr;
};

}  // namespace schedule

}  // namespace ara
