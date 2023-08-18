#pragma once

#include <ara/task/backpressure.h>
#include <ara/task/task_observer.h>

namespace ara::task {

class TaskObserver;

struct TaskContext {
  BackpressurePairFactory backpressure_pair_factory;
  std::unique_ptr<TaskObserver> task_observer;
};

};  // namespace ara::task
