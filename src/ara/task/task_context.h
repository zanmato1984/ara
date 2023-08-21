#pragma once

#include <ara/common/defines.h>
#include <ara/task/backpressure.h>

namespace ara::task {

class TaskObserver;

struct TaskContext {
  QueryId query_id;
  BackpressurePairFactory backpressure_pair_factory;
  TaskObserver* task_observer;
};

};  // namespace ara::task
