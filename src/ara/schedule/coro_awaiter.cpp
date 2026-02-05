#include "coro_awaiter.h"
#include "coro_resumer.h"

#include <ara/util/defines.h>

namespace ara::schedule {

CoroAwaiter::~CoroAwaiter() {
  auto* rec = record_.load(std::memory_order_acquire);
  if (rec == nullptr || rec == kScheduledSentinel()) {
    return;
  }
  delete rec;
}

bool CoroAwaiter::IsReady() const noexcept {
  return readies_.load(std::memory_order_acquire) >= num_readies_;
}

bool CoroAwaiter::Suspend(ScheduleFn schedule, std::coroutine_handle<> continuation) {
  if (IsReady()) {
    return false;
  }

  auto* rec = new ContinuationRecord{continuation, std::move(schedule)};

  ContinuationRecord* expected = nullptr;
  while (!record_.compare_exchange_weak(expected, rec, std::memory_order_release,
                                       std::memory_order_acquire)) {
    if (expected == kScheduledSentinel()) {
      delete rec;
      return false;
    }
    ARA_CHECK(expected == nullptr);
  }
  return true;
}

void CoroAwaiter::NotifyReady() {
  const auto new_readies = readies_.fetch_add(1, std::memory_order_acq_rel) + 1;
  if (new_readies < num_readies_) {
    return;
  }

  auto* rec = record_.exchange(kScheduledSentinel(), std::memory_order_acq_rel);
  if (rec == nullptr || rec == kScheduledSentinel()) {
    return;
  }

  auto continuation = rec->continuation;
  auto schedule = std::move(rec->schedule);
  delete rec;

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
