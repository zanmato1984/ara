#include "async_awaiter.h"
#include "async_resumer.h"

#include <ara/util/defines.h>

namespace ara::schedule {

using task::AwaiterPtr;
using task::ResumerPtr;
using task::Resumers;

class AsyncSingleAwaiter : public AsyncAwaiter {
 public:
  explicit AsyncSingleAwaiter(ResumerPtr& resumer)
      : resumer_(std::dynamic_pointer_cast<AsyncResumer>(resumer)) {
    ARA_CHECK(resumer_ != nullptr);
  }

  Future& GetFuture() override { return resumer_->GetFuture(); }

 private:
  std::shared_ptr<AsyncResumer> resumer_;
};

class AsyncAnyAwaiter : public AsyncAwaiter {
 public:
  explicit AsyncAnyAwaiter(Resumers& resumers) {
    std::vector<Future> futures;
    for (auto& resumer : resumers) {
      auto casted = std::dynamic_pointer_cast<AsyncResumer>(resumer);
      ARA_CHECK(casted != nullptr);
      futures.push_back(std::move(casted->GetFuture()));
    }
    future_ = folly::collectAny(futures).defer([](auto&&) {});
  }

  Future& GetFuture() override { return future_; }

 private:
  Future future_;
};

class AsyncAllAwaiter : public AsyncAwaiter {
 public:
  explicit AsyncAllAwaiter(Resumers& resumers) {
    std::vector<Future> futures;
    for (auto& resumer : resumers) {
      auto casted = std::dynamic_pointer_cast<AsyncResumer>(resumer);
      ARA_CHECK(casted != nullptr);
      futures.push_back(std::move(casted->GetFuture()));
    }
    future_ = folly::collectAll(futures).defer([](auto&&) {});
  }

  Future& GetFuture() override { return future_; }

 private:
  Future future_;
};

std::shared_ptr<AsyncAwaiter> AsyncAwaiter::MakeSingle(ResumerPtr& resumer) {
  return std::make_shared<AsyncSingleAwaiter>(resumer);
}

std::shared_ptr<AsyncAwaiter> AsyncAwaiter::MakeAny(Resumers& resumers) {
  return std::make_shared<AsyncAnyAwaiter>(resumers);
}

std::shared_ptr<AsyncAwaiter> AsyncAwaiter::MakeAll(Resumers& resumers) {
  return std::make_shared<AsyncAllAwaiter>(resumers);
}

}  // namespace ara::schedule
