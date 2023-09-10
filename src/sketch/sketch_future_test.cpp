#include <arrow/api.h>
#include <arrow/testing/gtest_util.h>
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

TEST(FollyFutureTest, WhileDoFinish) {
  enum class OperatorStatusCode { INIT, MORE, FINISHED, OTHER };

  struct OperatorOutput {
    int output = -1;
  };

  struct OperatorStatus {
    OperatorStatusCode code;
    std::variant<OperatorOutput, arrow::Status> payload;

    static OperatorStatus Init() { return OperatorStatus{OperatorStatusCode::INIT, {}}; }
    static OperatorStatus More(int output) {
      return OperatorStatus{OperatorStatusCode::MORE, OperatorOutput{output}};
    }
    static OperatorStatus Finished(int output) {
      return OperatorStatus{OperatorStatusCode::FINISHED, OperatorOutput{output}};
    }
    static OperatorStatus Other(arrow::Status status) {
      return OperatorStatus{OperatorStatusCode::OTHER, std::move(status)};
    }
  };

  struct Operator {
    Operator() = default;

    Operator(size_t id, int num_rounds, int sleep_ms)
        : id(id),
          current_output(0),
          num_rounds(num_rounds),
          sleep_ms(sleep_ms),
          round_to_error(num_rounds + 1),
          error(arrow::Status::OK()) {}

    Operator(size_t id, int num_rounds, int sleep_ms, int round_to_error,
             arrow::Status error)
        : id(id),
          current_output(0),
          num_rounds(num_rounds),
          sleep_ms(sleep_ms),
          round_to_error(round_to_error),
          error(std::move(error)) {}

    arrow::Status Run(OperatorStatus& status) {
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));

      if (current_output >= num_rounds) {
        status = OperatorStatus::Finished(current_output++);
        return arrow::Status::OK();
      }
      if (current_output == round_to_error) {
        status = OperatorStatus::Other(error);
        return error;
      }
      status = OperatorStatus::More(current_output++);
      return arrow::Status::OK();
    }

   private:
    size_t id;
    int current_output;
    int num_rounds;
    int sleep_ms;
    int round_to_error;
    arrow::Status error;
  };

  auto op_task = [](Operator& op,
                    OperatorStatus& status) -> folly::SemiFuture<folly::Unit> {
    auto pred = [&]() {
      return status.code == OperatorStatusCode::INIT ||
             status.code == OperatorStatusCode::MORE;
    };
    auto thunk = [&]() {
      // std::ignore = op.Run(status);
      // return folly::makeSemiFuture();
      return folly::makeSemiFutureWith([&]() { std::ignore = op.Run(status); });
    };
    return folly::whileDo(pred, thunk);
  };

  constexpr size_t dop = 32;
  constexpr int num_rounds = 42;
  Operator op[dop];
  OperatorStatus status[dop];
  for (size_t i = 0; i < dop; ++i) {
    op[i] = Operator(i, num_rounds, 50);
    status[i] = OperatorStatus::Init();
  }
  folly::CPUThreadPoolExecutor e(4);
  std::unordered_set<std::thread::id> tid_set;

  std::vector<folly::Future<folly::Unit>> futures;
  for (size_t i = 0; i < dop; ++i) {
    futures.push_back(op_task(op[i], status[i]).via(&e).thenValue([&](folly::Unit) {
      tid_set.insert(std::this_thread::get_id());
      return folly::Unit{};
    }));
  }
  folly::collectAll(futures).wait();
  std::cout << "thread count: " << tid_set.size() << std::endl;

  for (size_t i = 0; i < dop; ++i) {
    ASSERT_EQ(status[i].code, OperatorStatusCode::FINISHED);
    ASSERT_EQ(std::get<OperatorOutput>(status[i].payload).output, num_rounds);
  }
}

TEST(FollyFutureTest, WhileDoSwitchExecutor) {
  enum class Status { CONTINUE, YIELD, FINISHED };

  Status status = Status::CONTINUE;
  size_t counter = 0;
  folly::CPUThreadPoolExecutor e1(1), e2(1);
  std::unordered_map<std::thread::id, size_t> thread_ids;

  auto op = [&counter, &thread_ids]() -> Status {
    thread_ids[std::this_thread::get_id()]++;
    Status status;
    if (counter >= 41) {
      status = Status::FINISHED;
    } else if (counter % 2 == 1) {
      status = Status::CONTINUE;
    } else {
      status = Status::YIELD;
    }
    counter++;
    return status;
  };
  auto pred = [&]() { return status != Status::FINISHED; };
  auto thunk = [&]() {
    if (status == Status::CONTINUE) {
      return folly::via(&e1).then([&](auto&&) { status = op(); });
    } else if (status == Status::YIELD) {
      return folly::via(&e2).then([&](auto&&) { status = op(); });
    } else {
      throw std::runtime_error("unreachable");
    }
  };
  folly::whileDo(pred, thunk).wait().value();
  ASSERT_EQ(counter, 42);
  ASSERT_EQ(thread_ids.size(), 2);
  auto iter = thread_ids.begin();
  ASSERT_EQ(iter->second, 21);
  iter++;
  ASSERT_EQ(iter->second, 21);
}

TEST(FollyFutureTest, RacyCallback) {
  folly::CPUThreadPoolExecutor executor(4);
  size_t rounds = 100000;
  for (size_t i = 0; i < rounds; ++i) {
    std::atomic_bool start = false;
    auto pair = folly::makePromiseContract<int>();
    auto& promise = pair.first;
    auto& future = pair.second;
    auto fulfill_task = std::async(std::launch::async, [&]() {
      while (!start) {
      }
      promise.setValue(i);
    });
    auto get_task = std::async(std::launch::async, [&]() {
      while (!start) {
      }
      std::move(future)
          .via(&executor)
          .thenValue([&](int value) { EXPECT_EQ(value, i); })
          .wait();
    });
    start = true;
    fulfill_task.get();
    get_task.get();
  }
}

TEST(FollyFutureTest, CallbackTiming) {
  size_t pred_run = 0, thunk_run = 0;
  bool finished = false;
  folly::CPUThreadPoolExecutor executor(4);
  auto pair = folly::makePromiseContract<folly::Unit>(&executor);
  auto promise = std::move(pair.first);
  auto future = std::move(pair.second);
  promise.setValue();
  future = std::move(future).then([&](auto&&) { finished = true; });
  ASSERT_FALSE(finished);
  auto pred = [&]() {
    pred_run++;
    return !finished;
  };
  auto thunk = [&]() {
    thunk_run++;
    return std::move(future);
  };
  folly::whileDo(pred, thunk).wait().value();
  ASSERT_TRUE(finished);
  ASSERT_EQ(pred_run, 2);
  ASSERT_EQ(thunk_run, 1);
}
