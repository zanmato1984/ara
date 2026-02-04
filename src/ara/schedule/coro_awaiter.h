#pragma once

#include <ara/task/awaiter.h>
#include <ara/task/resumer.h>

#include <coroutine>
#include <functional>
#include <memory>
#include <mutex>

namespace ara::schedule {

class CoroAwaiter : public task::Awaiter,
                    public std::enable_shared_from_this<CoroAwaiter> {
 public:
  using ScheduleFn = std::function<void(std::coroutine_handle<>)>;

  explicit CoroAwaiter(size_t num_readies) : num_readies_(num_readies) {}

  struct Awaitable {
    std::shared_ptr<CoroAwaiter> awaiter;
    ScheduleFn schedule;

    bool await_ready() const noexcept { return awaiter->IsReady(); }

    bool await_suspend(std::coroutine_handle<> continuation) {
      return awaiter->Suspend(std::move(schedule), continuation);
    }

    void await_resume() const noexcept {}
  };

  Awaitable Await(ScheduleFn schedule) {
    return Awaitable{shared_from_this(), std::move(schedule)};
  }

  static std::shared_ptr<CoroAwaiter> MakeSingle(task::ResumerPtr);
  static std::shared_ptr<CoroAwaiter> MakeAny(task::Resumers);
  static std::shared_ptr<CoroAwaiter> MakeAll(task::Resumers);

  void NotifyReady();

 private:
  bool IsReady() const noexcept;
  bool Suspend(ScheduleFn schedule, std::coroutine_handle<> continuation);

  size_t num_readies_;
  mutable std::mutex mutex_;
  size_t readies_ = 0;
  std::coroutine_handle<> continuation_{};
  ScheduleFn schedule_;
};

}  // namespace ara::schedule
