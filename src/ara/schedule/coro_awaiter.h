#pragma once

#include <ara/task/awaiter.h>
#include <ara/task/resumer.h>

#include <atomic>
#include <coroutine>
#include <functional>
#include <memory>

namespace ara::schedule {

class CoroAwaiter : public task::Awaiter,
                    public std::enable_shared_from_this<CoroAwaiter> {
 public:
  using ScheduleFn = std::function<void(std::coroutine_handle<>)>;

  explicit CoroAwaiter(size_t num_readies) : num_readies_(num_readies) {}
  ~CoroAwaiter() override;

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
  struct ContinuationRecord {
    std::coroutine_handle<> continuation;
    ScheduleFn schedule;
  };

  static ContinuationRecord* kScheduledSentinel() {
    return reinterpret_cast<ContinuationRecord*>(1);
  }

  bool IsReady() const noexcept;
  bool Suspend(ScheduleFn schedule, std::coroutine_handle<> continuation);

  const size_t num_readies_;
  std::atomic<size_t> readies_{0};
  std::atomic<ContinuationRecord*> record_{nullptr};
};

}  // namespace ara::schedule
