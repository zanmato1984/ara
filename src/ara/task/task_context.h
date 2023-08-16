#pragma once

#include <ara/task/backpressure.h>
#include <ara/task/task_observer.h>

namespace ara::task {

struct TaskContext {
  BackpressurePairFactory backpressure_pair_factory;
  std::optional<TaskObserver> observer;
};

};  // namespace ara::task
