#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <folly/io/async/EventBase.h>
#include <gtest/gtest.h>
#include <numeric>

using namespace folly;

TEST(FutureTest, ContinuationOfCollection) {
  CPUThreadPoolExecutor executor(4);

  std::vector<SemiFuture<int>> futures;
  for (int i = 0; i < 10; ++i) {
    auto fut = makeSemiFutureWith([i]() {
      auto v = i * 2;
      return v;
    });
    futures.emplace_back(std::move(fut));
  }

  auto continuation =
      collect(std::move(futures)).via(&executor).thenValue([](std::vector<int> results) {
        return std::accumulate(results.begin(), results.end(), 0);
      });

  auto result = std::move(continuation).wait();
  int v = result.value();

  EXPECT_EQ(v, 90);
}

TEST(FutureTest, CollectionAsContinuation) {
  CPUThreadPoolExecutor executor(4);

  auto sf = makeSemiFutureWith([]() {
              auto i = 1;
              return i;
            })
                .deferValue([](int v) {
                  std::vector<SemiFuture<int>> sfs;
                  for (int i = 0; i < 10; ++i) {
                    auto fut = makeSemiFutureWith([v, i]() {
                      auto v2 = v * i;
                      return v2;
                    });
                    sfs.emplace_back(std::move(fut));
                  }
                  return collect(std::move(sfs));
                })
                .deferValue([](std::vector<int> results) {
                  return std::accumulate(results.begin(), results.end(), 0);
                });
  int result = std::move(sf).via(&executor).wait().value();

  EXPECT_EQ(result, 45);
}

TEST(FutureTest, ChainSpawn) {
  CPUThreadPoolExecutor e1(4);
  CPUThreadPoolExecutor e2(4);
  std::array<CPUThreadPoolExecutor*, 2> e = {&e1, &e2};

  std::unordered_set<std::thread::id> tid_set;

  auto chaining = [&](size_t start, size_t chain_length) {
    auto f = makeFuture();
    for (size_t i = 0; i < chain_length; ++i) {
      f = std::move(f).via(e[start++ % e.size()]).thenValue([&](Unit) {
        tid_set.insert(std::this_thread::get_id());
        return Unit{};
      });
    }
    return f;
  };

  chaining(0, 1000).wait();
  chaining(1, 1000).wait();

  std::cout << "thread count: " << tid_set.size() << std::endl;
}

TEST(FutureTest, FutureLoop) {
  size_t count = 10;
  size_t i = 0;
  while (makeFutureWith([&]() { return ++i; }).wait().value() < count) {
  }
  EXPECT_EQ(i, count);
}

TEST(FutureTest, ParallelSpawn) {
  CPUThreadPoolExecutor e(8);

  std::unordered_set<std::thread::id> tid_set;

  size_t count = 10;

  std::vector<Future<Unit>> futures;

  for (size_t i = 0; i < count; ++i) {
    futures.push_back(makeFuture().via(&e).thenValue([&](Unit) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      tid_set.insert(std::this_thread::get_id());
      return Unit{};
    }));
  }

  collectAll(futures).wait();

  std::cout << "thread count: " << tid_set.size() << std::endl;
}