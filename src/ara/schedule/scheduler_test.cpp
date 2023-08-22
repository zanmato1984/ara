#include "scheduler.h"

#include <ara/task/defines.h>
#include <ara/task/task_status.h>
#include <ara/util/util.h>
#include <folly/futures/Future.h>
#include <gtest/gtest.h>

TEST(SchedulerTest, FutureOfTaskResult) {
  using Promise = folly::Promise<folly::Unit>;
  using Future = folly::Future<ara::task::TaskResult>;
  auto [p, f] = folly::makePromiseContract<folly::Unit>();
  Future f2 = folly::makeFuture<ara::task::TaskResult>(ara::task::TaskStatus::Continue());
  //   folly::Future<ara::task::TaskResult> f =
  //   folly::makeFuture(ara::task::TaskResult(ara::task::TaskStatus::Continue()));
  // folly::Future f =
  // folly::makeFuture(ara::task::TaskResult(ara::task::TaskStatus::Continue()));
}
