#include "sync_awaiter.h"
#include "sync_resumer.h"

#include <gtest/gtest.h>
#include <future>

using namespace ara;
using namespace ara::schedule;
using namespace ara::task;

TEST(SyncResumerTest, Basic) {
  auto resumer = std::make_shared<SyncResumer>();
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

TEST(SyncResumerTest, Interleaving) {
  auto resumer = std::make_shared<SyncResumer>();
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

TEST(SyncResumerTest, Interleaving2) {
  auto resumer = std::make_shared<SyncResumer>();
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
  while (!resumer->IsResumed()) {
  }
  ASSERT_TRUE(cb1_called);
  ASSERT_TRUE(cb2_called);
  resumer->AddCallback([&]() { cb3_called = true; });
  ASSERT_TRUE(cb3_called);
  resume_future.get();
}

TEST(SyncAwaiterTest, SingleWaitFirst) {
  ResumerPtr resumer = std::make_shared<SyncResumer>();
  auto awaiter = SyncAwaiter::MakeSingle(resumer);

  bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    finished = true;
    resumer->Resume();
  });
  awaiter->Wait();
  ASSERT_TRUE(finished);
  ASSERT_TRUE(resumer->IsResumed());
  future.get();
}

TEST(SyncAwaiterTest, SingleResumeFirst) {
  ResumerPtr resumer = std::make_shared<SyncResumer>();
  auto awaiter = SyncAwaiter::MakeSingle(resumer);

  resumer->Resume();
  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    awaiter->Wait();
    return true;
  });
  ASSERT_TRUE(future.get());
}

TEST(SyncAwaiterTest, Race) {
  size_t rounds = 100000;
  for (size_t i = 0; i < rounds; ++i) {
    ResumerPtr resumer = std::make_shared<SyncResumer>();
    auto awaiter = SyncAwaiter::MakeSingle(resumer);

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
      awaiter->Wait();
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

TEST(SyncAwaiterTest, AnyWaitFirst) {
  size_t num_resumers = 1000;
  size_t lucky = 42;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<SyncResumer>();
  }
  auto awaiter = SyncAwaiter::MakeAny(resumers);

  bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    finished = true;
    resumers[lucky]->Resume();
  });
  awaiter->Wait();
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

TEST(SyncAwaiterTest, AnyResumeFirst) {
  size_t num_resumers = 1000;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<SyncResumer>();
    resumer->Resume();
  }
  auto awaiter = SyncAwaiter::MakeAny(resumers);

  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    awaiter->Wait();
    return true;
  });
  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
  ASSERT_TRUE(future.get());
}

TEST(SyncAwaiterTest, LifeSpan) {
  size_t num_resumers = 1000;
  size_t lucky = 42;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<SyncResumer>();
  }
  auto awaiter = SyncAwaiter::MakeAny(resumers);

  bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    finished = true;
    resumers[lucky]->Resume();
  });
  awaiter->Wait();
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

TEST(SyncAwaiterTest, AllWaitFirst) {
  size_t num_resumers = 1000;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<SyncResumer>();
  }
  auto awaiter = SyncAwaiter::MakeAll(resumers);

  std::atomic<size_t> counter = 0;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    for (auto& resumer : resumers) {
      counter++;
      resumer->Resume();
    }
  });
  awaiter->Wait();
  ASSERT_EQ(counter.load(), num_resumers);
  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
  future.get();
}

TEST(SyncAwaiterTest, AllResumeFirst) {
  size_t num_resumers = 1000;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<SyncResumer>();
    resumer->Resume();
  }
  auto awaiter = SyncAwaiter::MakeAll(resumers);

  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    awaiter->Wait();
    return true;
  });
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_TRUE(resumers[i]->IsResumed());
  }
  ASSERT_TRUE(future.get());
}
