#pragma once

#include <ara/task/resumer.h>

#include <folly/futures/Future.h>

namespace ara::schedule {

class AsyncResumer : public task::Resumer {
 private:
  using Promise = folly::Promise<folly::Unit>;
  using Future = folly::SemiFuture<folly::Unit>;

 public:
  AsyncResumer() { ResetNoLock(); }

  void Resume() override {
    std::lock_guard<std::mutex> lock(mutex_);
    promise_.setValue();
  }

  bool IsResumed() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return promise_.isFulfilled();
  }

  Future& GetFuture() {
    std::lock_guard<std::mutex> lock(mutex_);
    return future_;
  }

  void Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    bool fulfilled = promise_.isFulfilled();
    ResetNoLock();
    if (fulfilled) {
      promise_.setValue();
    }
  }

 private:
  void ResetNoLock() {
    auto [p, f] = folly::makePromiseContract<folly::Unit>();
    promise_ = std::move(p);
    future_ = std::move(f);
  }

 private:
  std::mutex mutex_;
  Promise promise_;
  Future future_;
};

}  // namespace ara::schedule
