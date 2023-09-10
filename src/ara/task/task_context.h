#pragma once

#include <ara/common/defines.h>
#include <ara/common/observer.h>
#include <ara/task/awaiter.h>
#include <ara/task/resumer.h>
#include <ara/task/task_observer.h>

namespace ara {

class QueryContext;

namespace task {

class TaskObserver;

struct TaskContext {
  const QueryContext* query_context;
  ResumerFactory resumer_factory;
  SingleAwaiterFactory single_awaiter_factory;
  AnyAwaiterFactory any_awaiter_factory;
  AllAwaiterFactory all_awaiter_factory;
  std::unique_ptr<ChainedObserver<TaskObserver>> task_observer;
};

};  // namespace task

};  // namespace ara
