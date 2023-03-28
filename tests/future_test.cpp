#include <arrow/api.h>
#include <arrow/util/future.h>
#include <arrow/util/logging.h>
#include <arrow/util/thread_pool.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <folly/io/async/EventBase.h>
#include <gtest/gtest.h>
#include <numeric>

TEST(FollyFutureTest, ContinuationOfCollection) {
  folly::CPUThreadPoolExecutor executor(4);

  std::vector<folly::SemiFuture<int>> futures;
  for (int i = 0; i < 10; ++i) {
    auto fut = folly::makeSemiFutureWith([i]() {
      auto v = i * 2;
      return v;
    });
    futures.emplace_back(std::move(fut));
  }

  auto continuation = folly::collect(std::move(futures))
                          .via(&executor)
                          .thenValue([](std::vector<int> results) {
                            return std::accumulate(results.begin(), results.end(), 0);
                          });

  auto result = std::move(continuation).wait();
  int v = result.value();

  EXPECT_EQ(v, 90);
}

TEST(FollyFutureTest, CollectionAsContinuation) {
  folly::CPUThreadPoolExecutor executor(4);

  auto sf = folly::makeSemiFutureWith([]() {
              auto i = 1;
              return i;
            })
                .deferValue([](int v) {
                  std::vector<folly::SemiFuture<int>> sfs;
                  for (int i = 0; i < 10; ++i) {
                    auto fut = folly::makeSemiFutureWith([v, i]() {
                      auto v2 = v * i;
                      return v2;
                    });
                    sfs.emplace_back(std::move(fut));
                  }
                  return folly::collect(std::move(sfs));
                })
                .deferValue([](std::vector<int> results) {
                  return std::accumulate(results.begin(), results.end(), 0);
                });
  int result = std::move(sf).via(&executor).wait().value();

  EXPECT_EQ(result, 45);
}

TEST(FollyFutureTest, ChainSpawn) {
  folly::CPUThreadPoolExecutor e1(4);
  folly::CPUThreadPoolExecutor e2(4);
  std::array<folly::CPUThreadPoolExecutor*, 2> e = {&e1, &e2};

  std::unordered_set<std::thread::id> tid_set;

  auto chaining = [&](size_t start, size_t chain_length) {
    auto f = folly::makeFuture();
    for (size_t i = 0; i < chain_length; ++i) {
      f = std::move(f).via(e[start++ % e.size()]).thenValue([&](folly::Unit) {
        tid_set.insert(std::this_thread::get_id());
        return folly::Unit{};
      });
    }
    return f;
  };

  chaining(0, 1000).wait();
  chaining(1, 1000).wait();

  std::cout << "thread count: " << tid_set.size() << std::endl;
}

TEST(FollyFutureTest, FutureLoop) {
  size_t count = 10;
  size_t i = 0;
  while (folly::makeFutureWith([&]() { return ++i; }).wait().value() < count) {
  }
  EXPECT_EQ(i, count);
}

TEST(FollyFutureTest, ParallelSpawn) {
  folly::CPUThreadPoolExecutor e(8);

  std::unordered_set<std::thread::id> tid_set;

  size_t count = 10;

  std::vector<folly::Future<folly::Unit>> futures;

  for (size_t i = 0; i < count; ++i) {
    futures.push_back(folly::makeFuture().via(&e).thenValue([&](folly::Unit) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      tid_set.insert(std::this_thread::get_id());
      return folly::Unit{};
    }));
  }

  folly::collectAll(futures).wait();

  std::cout << "thread count: " << tid_set.size() << std::endl;
}

TEST(ArrowFutureTest, AllCompleteThen) {
  size_t n = 100;
  size_t num_threads = 8;
  auto thread_pool = *arrow::internal::ThreadPool::Make(num_threads);
  std::unordered_set<std::thread::id> tid_set;
  std::atomic<size_t> counter{0};

  std::vector<arrow::Future<>> src_futures;
  for (size_t i = 0; i < n; ++i) {
    src_futures.emplace_back(arrow::Future<>::Make());
  }

  std::vector<arrow::Future<>> then_futures;
  {
    arrow::CallbackOptions options{arrow::ShouldSchedule::Always, thread_pool.get()};
    for (size_t i = 0; i < n; ++i) {
      then_futures.emplace_back(src_futures[i].Then(
          [&] {
            tid_set.insert(std::this_thread::get_id());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            counter++;
            return arrow::Status::OK();
          },
          {}, options));
    }
  }

  auto fut = arrow::AllComplete(then_futures).Then([&] { EXPECT_EQ(counter, n); });
  for (auto& f : src_futures) {
    f.MarkFinished();
  }
  fut.Wait();
  EXPECT_EQ(fut.is_finished(), true);
  EXPECT_EQ(tid_set.size(), num_threads);
}