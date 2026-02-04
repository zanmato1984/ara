#include "coro_awaiter.h"
#include "coro_resumer.h"

#include <ara/util/defines.h>

namespace ara::schedule {

bool CoroAwaiter::IsReady() const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  return readies_ >= num_readies_;
}

bool CoroAwaiter::Suspend(ScheduleFn schedule, std::coroutine_handle<> continuation) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (readies_ >= num_readies_) {
    return false;
  }
  continuation_ = continuation;
  schedule_ = std::move(schedule);
  return true;
}

void CoroAwaiter::NotifyReady() {
  std::coroutine_handle<> continuation;
  ScheduleFn schedule;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (readies_ >= num_readies_) {
      return;
    }
    ++readies_;
    if (readies_ < num_readies_ || !continuation_) {
      return;
    }
    continuation = continuation_;
    continuation_ = {};
    schedule = std::move(schedule_);
    schedule_ = {};
  }
  if (continuation && schedule) {
    schedule(continuation);
  }
}

static std::shared_ptr<CoroAwaiter> MakeCoroAwaiter(size_t num_readies,
                                                    task::Resumers resumers) {
  auto awaiter = std::make_shared<CoroAwaiter>(num_readies);
  for (auto& resumer : resumers) {
    auto casted = std::dynamic_pointer_cast<CoroResumer>(resumer);
    ARA_CHECK(casted != nullptr);
    casted->AddCallback([awaiter]() { awaiter->NotifyReady(); });
  }
  return awaiter;
}

std::shared_ptr<CoroAwaiter> CoroAwaiter::MakeSingle(task::ResumerPtr resumer) {
  task::Resumers resumers{std::move(resumer)};
  return MakeCoroAwaiter(1, std::move(resumers));
}

std::shared_ptr<CoroAwaiter> CoroAwaiter::MakeAny(task::Resumers resumers) {
  return MakeCoroAwaiter(1, std::move(resumers));
}

std::shared_ptr<CoroAwaiter> CoroAwaiter::MakeAll(task::Resumers resumers) {
  return MakeCoroAwaiter(resumers.size(), std::move(resumers));
}

}  // namespace ara::schedule

