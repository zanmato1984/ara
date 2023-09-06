#pragma once

#include <ara/common/defines.h>
#include <ara/common/observer.h>
#include <ara/task/backpressure.h>
#include <ara/task/task_observer.h>

namespace ara {

class QueryContext;

namespace task {

class TaskObserver;

struct TaskContext {
  const QueryContext* query_context;
  BackpressurePairFactory backpressure_pair_factory;
  std::unique_ptr<ChainedObserver<TaskObserver>> task_observer;
};

};  // namespace task

};  // namespace ara
