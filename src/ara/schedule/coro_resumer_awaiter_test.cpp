#include "coro_awaiter.h"
#include "coro_resumer.h"

#include <gtest/gtest.h>

#include <condition_variable>
#include <coroutine>
#include <chrono>
#include <deque>
#include <atomic>
#include <future>
#include <mutex>
#include <thread>

using namespace ara;
using namespace ara::schedule;
using namespace ara::task;

namespace {

class ReadyQueue {
 public:
  void Enqueue(std::coroutine_handle<> h) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ready_.push_back(h);
    }
    cv_.notify_one();
  }

  template <typename DonePred>
  std::coroutine_handle<> DequeueOrWait(DonePred done) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&]() { return !ready_.empty() || done(); });
    if (ready_.empty()) {
      return {};
    }
    auto h = ready_.front();
    ready_.pop_front();
    return h;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<std::coroutine_handle<>> ready_;
};

struct TaskCoro {
  struct promise_type {
    TaskCoro get_return_object() {
      return TaskCoro{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_always initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    void return_void() noexcept {}
    void unhandled_exception() noexcept { std::terminate(); }
  };

  explicit TaskCoro(std::coroutine_handle<promise_type> handle) : handle(handle) {}

  TaskCoro(TaskCoro&& other) noexcept : handle(other.handle) { other.handle = {}; }
  TaskCoro& operator=(TaskCoro&& other) noexcept {
    if (this != &other) {
      if (handle) {
        handle.destroy();
      }
      handle = other.handle;
      other.handle = {};
    }
    return *this;
  }

  TaskCoro(const TaskCoro&) = delete;
  TaskCoro& operator=(const TaskCoro&) = delete;

  ~TaskCoro() {
    if (handle) {
      handle.destroy();
    }
  }

  std::coroutine_handle<promise_type> handle;
};

void RunUntilDone(ReadyQueue& queue, std::coroutine_handle<> root) {
  queue.Enqueue(root);
  while (!root.done()) {
    auto h = queue.DequeueOrWait([&]() { return root.done(); });
    if (!h) {
      continue;
    }
    h.resume();
  }
}

TaskCoro AwaitOnce(std::shared_ptr<CoroAwaiter> awaiter, ReadyQueue& queue,
                   bool& finished) {
  auto schedule = [&queue](std::coroutine_handle<> h) { queue.Enqueue(h); };
  co_await awaiter->Await(std::move(schedule));
  finished = true;
}

}  // namespace

TEST(CoroResumerTest, Basic) {
  auto resumer = std::make_shared<CoroResumer>();
  bool cb1_called = false, cb2_called = false, cb3_called = false;

  resumer->AddCallback([&]() { cb1_called = true; });
  resumer->AddCallback([&]() { cb2_called = cb1_called; });

  ASSERT_FALSE(resumer->IsResumed());
  ASSERT_FALSE(cb1_called);
  ASSERT_FALSE(cb2_called);

  resumer->Resume();

  ASSERT_TRUE(resumer->IsResumed());
  ASSERT_TRUE(cb1_called);
  ASSERT_TRUE(cb2_called);

  resumer->AddCallback([&]() { cb3_called = true; });
  ASSERT_TRUE(cb3_called);
}

TEST(CoroResumerTest, Interleaving) {
  auto resumer = std::make_shared<CoroResumer>();
  bool cb1_called = false, cb2_called = false, cb3_called = false;
  resumer->AddCallback([&]() { cb1_called = true; });
  resumer->AddCallback([&]() { cb2_called = cb1_called; });

  ASSERT_FALSE(resumer->IsResumed());
  ASSERT_FALSE(cb1_called);
  ASSERT_FALSE(cb2_called);

  auto resume_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    resumer->Resume();
    resumer->AddCallback([&]() { cb3_called = true; });
    ASSERT_TRUE(cb3_called);
  });

  while (!resumer->IsResumed()) {
  }
  ASSERT_TRUE(cb1_called);
  ASSERT_TRUE(cb2_called);
  resume_future.get();
}

TEST(CoroResumerTest, Interleaving2) {
  auto resumer = std::make_shared<CoroResumer>();
  bool cb1_called = false, cb2_called = false, cb3_called = false;
  resumer->AddCallback([&]() { cb1_called = true; });
  resumer->AddCallback([&]() { cb2_called = cb1_called; });

  ASSERT_FALSE(resumer->IsResumed());
  ASSERT_FALSE(cb1_called);
  ASSERT_FALSE(cb2_called);

  auto resume_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    resumer->Resume();
  });

  while (!resumer->IsResumed() || !cb1_called || !cb2_called) {
  }
  ASSERT_TRUE(cb1_called);
  ASSERT_TRUE(cb2_called);

  resumer->AddCallback([&]() { cb3_called = true; });
  ASSERT_TRUE(cb3_called);

  resume_future.get();
}

TEST(CoroAwaiterTest, SingleWaitFirst) {
  ResumerPtr resumer = std::make_shared<CoroResumer>();
  auto awaiter = CoroAwaiter::MakeSingle(resumer);

  ReadyQueue queue;
  bool finished = false;
  auto coro = AwaitOnce(awaiter, queue, finished);

  auto resume_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(finished);
    resumer->Resume();
  });

  RunUntilDone(queue, coro.handle);
  ASSERT_TRUE(finished);
  ASSERT_TRUE(resumer->IsResumed());
  resume_future.get();
}

TEST(CoroAwaiterTest, SingleResumeFirst) {
  ResumerPtr resumer = std::make_shared<CoroResumer>();
  resumer->Resume();
  auto awaiter = CoroAwaiter::MakeSingle(resumer);

  ReadyQueue queue;
  bool finished = false;
  auto coro = AwaitOnce(awaiter, queue, finished);

  RunUntilDone(queue, coro.handle);
  ASSERT_TRUE(finished);
}

TEST(CoroAwaiterTest, AnyWaitFirst) {
  size_t num_resumers = 1000;
  size_t lucky = 42;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<CoroResumer>();
  }
  auto awaiter = CoroAwaiter::MakeAny(resumers);

  ReadyQueue queue;
  bool finished = false;
  auto coro = AwaitOnce(awaiter, queue, finished);

  auto resume_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    resumers[lucky]->Resume();
  });

  RunUntilDone(queue, coro.handle);
  ASSERT_TRUE(finished);
  for (size_t i = 0; i < num_resumers; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }
  resume_future.get();
}

TEST(CoroAwaiterTest, AnyReentrantWait) {
  size_t num_resumers = 1000;
  size_t lucky = 42;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<CoroResumer>();
  }

  {
    auto awaiter1 = CoroAwaiter::MakeAny(resumers);
    resumers[lucky]->Resume();

    ReadyQueue queue;
    bool finished = false;
    auto coro = AwaitOnce(awaiter1, queue, finished);
    RunUntilDone(queue, coro.handle);
    ASSERT_TRUE(finished);
  }

  for (size_t i = 0; i < num_resumers; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }

  resumers[lucky] = std::make_shared<CoroResumer>();
  auto awaiter2 = CoroAwaiter::MakeAny(resumers);
  resumers[lucky]->Resume();

  ReadyQueue queue;
  bool finished = false;
  auto coro = AwaitOnce(awaiter2, queue, finished);
  RunUntilDone(queue, coro.handle);
  ASSERT_TRUE(finished);

  for (size_t i = 0; i < num_resumers; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }
}

TEST(CoroAwaiterTest, AnyResumeFirst) {
  size_t num_resumers = 1000;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<CoroResumer>();
    resumer->Resume();
  }
  auto awaiter = CoroAwaiter::MakeAny(resumers);

  ReadyQueue queue;
  bool finished = false;
  auto coro = AwaitOnce(awaiter, queue, finished);
  RunUntilDone(queue, coro.handle);
  ASSERT_TRUE(finished);

  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
}

TEST(CoroAwaiterTest, LifeSpan) {
  size_t num_resumers = 1000;
  size_t lucky = 42;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<CoroResumer>();
  }
  auto awaiter = CoroAwaiter::MakeAny(resumers);

  ReadyQueue queue;
  bool finished = false;
  auto coro = AwaitOnce(awaiter, queue, finished);

  auto resume_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    resumers[lucky]->Resume();
  });

  RunUntilDone(queue, coro.handle);
  ASSERT_TRUE(finished);

  awaiter.reset();

  for (size_t i = 0; i < num_resumers; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
      resumers[i]->Resume();
    }
  }

  resume_future.get();
}

TEST(CoroAwaiterTest, AllWaitFirst) {
  size_t num_resumers = 1000;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<CoroResumer>();
  }
  auto awaiter = CoroAwaiter::MakeAll(resumers);

  ReadyQueue queue;
  bool finished = false;
  auto coro = AwaitOnce(awaiter, queue, finished);

  std::atomic<size_t> counter = 0;
  auto resume_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    for (auto& resumer : resumers) {
      counter++;
      resumer->Resume();
    }
  });

  RunUntilDone(queue, coro.handle);
  ASSERT_TRUE(finished);
  ASSERT_EQ(counter.load(), num_resumers);
  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
  resume_future.get();
}

TEST(CoroAwaiterTest, AllResumeFirst) {
  size_t num_resumers = 1000;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<CoroResumer>();
    resumer->Resume();
  }
  auto awaiter = CoroAwaiter::MakeAll(resumers);

  ReadyQueue queue;
  bool finished = false;
  auto coro = AwaitOnce(awaiter, queue, finished);
  RunUntilDone(queue, coro.handle);
  ASSERT_TRUE(finished);

  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
}
