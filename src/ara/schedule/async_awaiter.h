#pragma once

#include <ara/task/awaiter.h>

#include <folly/futures/Future.h>

namespace ara::schedule {

class AsyncAwaiter : public task::Awaiter {
 public:
  using Future = folly::SemiFuture<folly::Unit>;

 public:
  virtual Future& GetFuture() = 0;

  static std::shared_ptr<AsyncAwaiter> MakeSingle(task::ResumerPtr);
  static std::shared_ptr<AsyncAwaiter> MakeAny(task::Resumers);
  static std::shared_ptr<AsyncAwaiter> MakeAll(task::Resumers);
};

}  // namespace ara::schedule
