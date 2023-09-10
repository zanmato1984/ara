#pragma once

#include <ara/task/resumer.h>

#include <folly/futures/Future.h>

namespace ara::schedule {

class AsyncResumer : public task::Resumer {
 private:
  using Promise = folly::Promise<folly::Unit>;
  using Future = folly::SemiFuture<folly::Unit>;

 public:
  AsyncResumer() {
    auto [p, f] = folly::makePromiseContract<folly::Unit>();
    promise_ = std::move(p);
    future_ = std::move(f);
  }

  void Resume() override { promise_.setValue(); }

  bool IsResumed() override { return promise_.isFulfilled(); }

  Future& GetFuture() { return future_; }

 private:
  Promise promise_;
  Future future_;
};

}  // namespace ara::schedule
