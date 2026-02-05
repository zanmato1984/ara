#pragma once

#include <ara/util/defines.h>

#include <folly/MPMCQueue.h>

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <thread>

namespace ara::schedule::detail {

// Mutex-free ready queue for coroutine handles.
//
// Uses folly::MPMCQueue for lock-free storage and C++20 atomic wait/notify for
// blocking when the queue is empty.
class CoroReadyQueue {
 public:
  explicit CoroReadyQueue(size_t capacity)
      : queue_(capacity == 0 ? 1 : capacity), wakeups_(0) {}

  void Enqueue(std::coroutine_handle<> h) {
    ARA_CHECK(h);
    while (!queue_.write(h)) {
      std::this_thread::yield();
    }
    WakeOne();
  }

  template <typename DonePred>
  std::coroutine_handle<> DequeueOrWait(DonePred done) {
    std::coroutine_handle<> h;
    while (true) {
      if (queue_.read(h)) {
        return h;
      }
      if (done()) {
        return {};
      }
      auto ticket = wakeups_.load(std::memory_order_relaxed);
      wakeups_.wait(ticket, std::memory_order_relaxed);
    }
  }

  void WakeOne() {
    wakeups_.fetch_add(1, std::memory_order_release);
    wakeups_.notify_one();
  }

  void WakeAll() {
    wakeups_.fetch_add(1, std::memory_order_release);
    wakeups_.notify_all();
  }

  struct YieldAwaiter {
    CoroReadyQueue* queue;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept { queue->Enqueue(h); }
    void await_resume() const noexcept {}
  };

  YieldAwaiter Yield() { return YieldAwaiter{this}; }

 private:
  folly::MPMCQueue<std::coroutine_handle<>> queue_;
  std::atomic<uint64_t> wakeups_;
};

}  // namespace ara::schedule::detail

