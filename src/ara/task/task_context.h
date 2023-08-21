#pragma once

#include <ara/task/backpressure.h>
#include <ara/task/task_observer.h>

namespace ara::task {

struct TaskContext {
  QueryId query_id;
  BackpressurePairFactory backpressure_pair_factory;
  std::unique_ptr<TaskObserver> task_observer = nullptr;
};

};  // namespace ara::task
