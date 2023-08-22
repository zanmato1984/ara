#include "schedule_observer.h"

namespace ara::schedule {

std::unique_ptr<ScheduleObserver> ScheduleObserver::Make(const QueryContext&) {
  return nullptr;
}

}  // namespace ara::schedule
