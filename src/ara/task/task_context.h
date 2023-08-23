#pragma once

#include <ara/common/defines.h>
#include <ara/common/query_context.h>
#include <ara/task/backpressure.h>
#include <ara/task/task_observer.h>
#include <ara/common/observer.h>

namespace ara::task {

class TaskObserver;

struct TaskContext {
  const QueryContext* query_context;
  QueryId query_id;
  std::optional<BackpressurePairFactory> backpressure_pair_factory;
  std::unique_ptr<ChainedObserver<TaskObserver>> task_observer;
};

};  // namespace ara::task
