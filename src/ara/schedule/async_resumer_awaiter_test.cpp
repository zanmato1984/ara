#include "async_awaiter.h"
#include "async_resumer.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>
#include <future>

using namespace ara;
using namespace ara::schedule;
using namespace ara::task;

TEST(AsyncResumerTest, Basic) {
  auto resumer = std::make_shared<AsyncResumer>();
  ASSERT_FALSE(resumer->IsResumed());
  resumer->Resume();
  ASSERT_TRUE(resumer->IsResumed());
}

TEST(AsyncAwaiterTest, SingleWaitFirst) {
  ResumerPtr resumer = std::make_shared<AsyncResumer>();
  auto awaiter = AsyncAwaiter::MakeSingle(resumer);

  bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    finished = true;
    resumer->Resume();
  });
  folly::CPUThreadPoolExecutor executor(4);
  std::move(awaiter->GetFuture()).via(&executor).wait();
  ASSERT_TRUE(finished);
  ASSERT_TRUE(resumer->IsResumed());
  future.get();
}

TEST(AsyncAwaiterTest, SingleResumeFirst) {
  ResumerPtr resumer = std::make_shared<AsyncResumer>();
  auto awaiter = AsyncAwaiter::MakeSingle(resumer);

  resumer->Resume();
  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    folly::CPUThreadPoolExecutor executor(4);
    std::move(awaiter->GetFuture()).via(&executor).wait();
    return true;
  });
  ASSERT_TRUE(future.get());
}

TEST(AsyncAwaiterTest, Race) {
  size_t rounds = 100000;
  folly::CPUThreadPoolExecutor executor(4);
  for (size_t i = 0; i < rounds; ++i) {
    ResumerPtr resumer = std::make_shared<AsyncResumer>();
    auto awaiter = AsyncAwaiter::MakeSingle(resumer);

    std::atomic_bool resumer_ready = false, awaiter_ready = false, kickoff = false;
    auto resume_future = std::async(std::launch::async, [&]() {
      resumer_ready = true;
      while (!kickoff) {
      }
      resumer->Resume();
    });
    auto await_future = std::async(std::launch::async, [&]() {
      awaiter_ready = true;
      while (!kickoff) {
      }
      std::move(awaiter->GetFuture()).via(&executor).wait();
      return true;
    });
    while (!resumer_ready || !awaiter_ready) {
    }
    kickoff = true;
    resume_future.get();
    ASSERT_TRUE(resumer->IsResumed());
    ASSERT_TRUE(await_future.get());
  }
}

TEST(AsyncAwaiterTest, AnyWaitFirst) {
  size_t num_resumers = 1000;
  folly::CPUThreadPoolExecutor executor(4);
  size_t lucky = 42;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<AsyncResumer>();
  }
  auto awaiter = AsyncAwaiter::MakeAny(resumers);

  bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    finished = true;
    resumers[lucky]->Resume();
  });
  std::move(awaiter->GetFuture()).via(&executor).wait();
  ASSERT_TRUE(finished);
  for (size_t i = 0; i < 1000; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }
  future.get();
}

TEST(AsyncAwaiterTest, AnyReentrantWait) {
  size_t num_resumers = 1000;
  folly::CPUThreadPoolExecutor executor(4);
  size_t lucky = 42;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<AsyncResumer>();
  }
  auto awaiter1 = AsyncAwaiter::MakeAny(resumers);
  resumers[lucky]->Resume();
  std::move(awaiter1->GetFuture()).via(&executor).wait();
  for (size_t i = 0; i < 1000; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }
  resumers[lucky] = std::make_shared<AsyncResumer>();
  auto awaiter2 = AsyncAwaiter::MakeAny(resumers);
  resumers[lucky]->Resume();
  std::move(awaiter2->GetFuture()).via(&executor).wait();
  for (size_t i = 0; i < 1000; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }
}

TEST(AsyncAwaiterTest, AnyResumeFirst) {
  size_t num_resumers = 1000;
  folly::CPUThreadPoolExecutor executor(4);
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<AsyncResumer>();
    resumer->Resume();
  }
  auto awaiter = AsyncAwaiter::MakeAny(resumers);

  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::move(awaiter->GetFuture()).via(&executor).wait();
    return true;
  });
  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
  ASSERT_TRUE(future.get());
}

TEST(AsyncAwaiterTest, LifeSpan) {
  size_t num_resumers = 1000;
  folly::CPUThreadPoolExecutor executor(4);
  size_t lucky = 42;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<AsyncResumer>();
  }
  auto awaiter = AsyncAwaiter::MakeAny(resumers);

  bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    finished = true;
    resumers[lucky]->Resume();
  });
  std::move(awaiter->GetFuture()).via(&executor).wait();
  awaiter.reset();
  ASSERT_TRUE(finished);
  for (size_t i = 0; i < 1000; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
      resumers[i]->Resume();
    }
  }
  future.get();
}

TEST(AsyncAwaiterTest, AllWaitFirst) {
  size_t num_resumers = 1000;
  folly::CPUThreadPoolExecutor executor(4);
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<AsyncResumer>();
  }
  auto awaiter = AsyncAwaiter::MakeAll(resumers);

  std::atomic<size_t> counter = 0;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    for (auto& resumer : resumers) {
      counter++;
      resumer->Resume();
    }
  });
  std::move(awaiter->GetFuture()).via(&executor).wait();
  ASSERT_EQ(counter.load(), num_resumers);
  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
  future.get();
}

TEST(AsyncAwaiterTest, AllResumeFirst) {
  size_t num_resumers = 1000;
  folly::CPUThreadPoolExecutor executor(4);
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<AsyncResumer>();
    resumer->Resume();
  }
  auto awaiter = AsyncAwaiter::MakeAll(resumers);

  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::move(awaiter->GetFuture()).via(&executor).wait();
    return true;
  });
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_TRUE(resumers[i]->IsResumed());
  }
  ASSERT_TRUE(future.get());
}
