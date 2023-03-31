#include <arrow/api.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/exec/accumulation_queue.h>
#include <arrow/compute/exec/hash_join.h>
#include <arrow/compute/exec/hash_join_node.h>
#include <arrow/compute/exec/options.h>
#include <arrow/compute/exec/task_util.h>
#include <arrow/compute/exec/test_util.h>
#include <arrow/compute/exec/util.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/logging.h>
#include <arrow/util/vector.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <gtest/gtest.h>

template <typename Prober>
using ProberFactory = std::function<Prober(arrow::compute::HashJoinImpl*,
                                           arrow::util::AccumulationQueue&&)>;
struct TaskRunner {
  using Task = std::function<arrow::Status(size_t, int64_t)>;
  using TaskCont = std::function<arrow::Status(size_t)>;
  using TaskGroup = std::pair<Task, TaskCont>;

  virtual ~TaskRunner() = default;

  virtual int RegisterTaskGroup(Task, TaskCont) = 0;
  virtual arrow::Status StartTaskGroup(int, int64_t) = 0;
  virtual void OutputBatch(int64_t, arrow::compute::ExecBatch) = 0;
};

struct HashJoinCase {
  using Task = TaskRunner::Task;
  using TaskCont = TaskRunner::TaskCont;

  HashJoinCase(int batch_size, int num_build_batches, int num_probe_batches,
               arrow::compute::JoinType join_type)
      : join_type(join_type) {
    std::vector<std::shared_ptr<arrow::DataType>> key_types = {arrow::int32()};
    std::vector<std::shared_ptr<arrow::DataType>> build_payload_types = {
        arrow::int64(), arrow::decimal256(15, 2)};
    std::vector<std::shared_ptr<arrow::DataType>> probe_payload_types = {arrow::int64(),
                                                                         arrow::utf8()};
    double null_percentage = 0.0;
    double cardinality = 1.0;  // Proportion of distinct keys in build side
    double selectivity = 1.0;  // Probability of a match for a given row

    arrow::SchemaBuilder l_schema_builder, r_schema_builder;
    std::vector<arrow::FieldRef> left_keys, right_keys;
    std::vector<arrow::compute::JoinKeyCmp> key_cmp;
    for (size_t i = 0; i < key_types.size(); i++) {
      std::string l_name = "lk" + std::to_string(i);
      std::string r_name = "rk" + std::to_string(i);

      // For integers, selectivity is the proportion of the build interval that overlaps
      // with the probe interval
      uint64_t num_build_rows = num_build_batches * batch_size;

      uint64_t min_build_value = 0;
      uint64_t max_build_value = static_cast<uint64_t>(num_build_rows * cardinality);

      uint64_t min_probe_value =
          static_cast<uint64_t>((1.0 - selectivity) * max_build_value);
      uint64_t max_probe_value = min_probe_value + max_build_value;

      std::unordered_map<std::string, std::string> build_metadata;
      build_metadata["null_probability"] = std::to_string(null_percentage);
      build_metadata["min"] = std::to_string(min_build_value);
      build_metadata["max"] = std::to_string(max_build_value);
      build_metadata["min_length"] = "2";
      build_metadata["max_length"] = "20";

      std::unordered_map<std::string, std::string> probe_metadata;
      probe_metadata["null_probability"] = std::to_string(null_percentage);
      probe_metadata["min"] = std::to_string(min_probe_value);
      probe_metadata["max"] = std::to_string(max_probe_value);

      auto l_field =
          field(l_name, key_types[i], arrow::key_value_metadata(probe_metadata));
      auto r_field =
          field(r_name, key_types[i], arrow::key_value_metadata(build_metadata));

      DCHECK_OK(l_schema_builder.AddField(l_field));
      DCHECK_OK(r_schema_builder.AddField(r_field));

      left_keys.push_back(arrow::FieldRef(l_name));
      right_keys.push_back(arrow::FieldRef(r_name));
      key_cmp.push_back(arrow::compute::JoinKeyCmp::EQ);
    }

    for (size_t i = 0; i < build_payload_types.size(); i++) {
      std::string name = "lp" + std::to_string(i);
      DCHECK_OK(l_schema_builder.AddField(field(name, probe_payload_types[i])));
    }

    for (size_t i = 0; i < build_payload_types.size(); i++) {
      std::string name = "rp" + std::to_string(i);
      DCHECK_OK(r_schema_builder.AddField(field(name, build_payload_types[i])));
    }

    auto l_schema = *l_schema_builder.Finish();
    auto r_schema = *r_schema_builder.Finish();

    arrow::compute::BatchesWithSchema l_batches_with_schema =
        arrow::compute::MakeRandomBatches(l_schema, num_probe_batches, batch_size);
    arrow::compute::BatchesWithSchema r_batches_with_schema =
        arrow::compute::MakeRandomBatches(r_schema, num_build_batches, batch_size);

    for (arrow::compute::ExecBatch& batch : l_batches_with_schema.batches)
      l_batches.InsertBatch(std::move(batch));
    for (arrow::compute::ExecBatch& batch : r_batches_with_schema.batches)
      r_batches.InsertBatch(std::move(batch));

    filter = arrow::compute::literal(true);
    DCHECK_OK(schema_mgr->Init(join_type, *l_batches_with_schema.schema, left_keys,
                               *r_batches_with_schema.schema, right_keys, filter, "l_",
                               "r_"));
  }

  arrow::Status Init(size_t dop, TaskRunner& task_runner) {
    auto* memory_pool = arrow::default_memory_pool();
    ctx = std::make_unique<arrow::compute::QueryContext>(
        arrow::compute::QueryOptions{},
        arrow::compute::ExecContext(memory_pool, NULLPTR, NULLPTR));
    DCHECK_OK(ctx->Init(dop, NULLPTR));

    arrow::compute::Expression filter = arrow::compute::literal(true);
    return join->Init(
        ctx.get(), join_type, dop, &(schema_mgr->proj_maps[0]),
        &(schema_mgr->proj_maps[1]), std::move(key_cmp), std::move(filter),
        [&task_runner](Task task, TaskCont task_cont) {
          return task_runner.RegisterTaskGroup(std::move(task), std::move(task_cont));
        },
        [&task_runner](int task_group_id, int64_t num_tasks) {
          return task_runner.StartTaskGroup(task_group_id, num_tasks);
        },
        [&task_runner](int64_t thread_id, arrow::compute::ExecBatch batch) {
          task_runner.OutputBatch(thread_id, std::move(batch));
        },
        [](int64_t x) {});
  }

  arrow::Status Build() {
    return join->BuildHashTable(0, std::move(r_batches),
                                [&](size_t thread_index) { return arrow::Status::OK(); });
  }

  template <typename Prober>
  Prober GetProber(ProberFactory<Prober> prober_factory) {
    return prober_factory(join.get(), std::move(l_batches));
  }

 private:
  arrow::util::AccumulationQueue l_batches;
  arrow::util::AccumulationQueue r_batches;

  arrow::compute::JoinType join_type;
  std::vector<arrow::compute::JoinKeyCmp> key_cmp;
  arrow::compute::Expression filter = arrow::compute::literal(true);
  std::unique_ptr<arrow::compute::HashJoinSchema> schema_mgr =
      std::make_unique<arrow::compute::HashJoinSchema>();
  std::unique_ptr<arrow::compute::HashJoinImpl> join =
      *arrow::compute::HashJoinImpl::MakeSwiss();
  std::unique_ptr<arrow::compute::QueryContext> ctx;
};

struct TaskGroupProber {
 private:
  arrow::compute::HashJoinImpl* join;
  arrow::util::AccumulationQueue probe_batches;

  TaskGroupProber(arrow::compute::HashJoinImpl* join,
                  arrow::util::AccumulationQueue&& probe_batches)
      : join(join), probe_batches(std::move(probe_batches)) {}

 public:
  static TaskGroupProber Make(arrow::compute::HashJoinImpl* join,
                              arrow::util::AccumulationQueue&& probe_batches) {
    return TaskGroupProber(join, std::move(probe_batches));
  }

  arrow::Status ProbeSingleBatch(size_t thread_index, int64_t task_id) {
    return join->ProbeSingleBatch(thread_index, probe_batches[task_id]);
  }

  arrow::Status ProbeFinished(size_t thread_index) {
    return join->ProbingFinished(thread_index);
  }
};

struct ArrowSchedulerTaskRunner : public TaskRunner {
  ArrowSchedulerTaskRunner(size_t num_threads)
      : TaskRunner(),
        scheduler(arrow::compute::TaskScheduler::Make()),
        thread_pool(*arrow::internal::ThreadPool::Make(num_threads)) {}

  void RegisterEnd() { scheduler->RegisterEnd(); }

  arrow::Status StartScheduling(size_t dop) {
    return scheduler->StartScheduling(
        thread_id(),
        [&](std::function<arrow::Status(size_t)> func) {
          return thread_pool->Spawn([&, func]() { ARROW_DCHECK_OK(func(thread_id())); });
        },
        dop, false);
  }

  int RegisterTaskGroup(Task task, TaskCont task_cont) override {
    return scheduler->RegisterTaskGroup(std::move(task), std::move(task_cont));
  }

  arrow::Status StartTaskGroup(int task_group_id, int64_t num_tasks) override {
    return scheduler->StartTaskGroup(thread_id(), task_group_id, num_tasks);
  }

  void OutputBatch(int64_t, arrow::compute::ExecBatch batch) override {
    std::cout << batch.ToString() << std::endl;
  }

  void WaitForIdle() { thread_pool->WaitForIdle(); }

 private:
  arrow::compute::ThreadIndexer thread_id;
  std::unique_ptr<arrow::compute::TaskScheduler> scheduler;
  std::shared_ptr<arrow::internal::ThreadPool> thread_pool;
};

TEST(HashJoinTest, ArrowScheduler) {
  int batch_size = 4096;
  int num_build_batches = 128;
  int num_probe_batches = 128 * 8;
  arrow::compute::JoinType join_type = arrow::compute::JoinType::RIGHT_OUTER;
  size_t dop = 16;
  size_t num_threads = 7;

  ArrowSchedulerTaskRunner task_runner(num_threads);
  HashJoinCase join_case(batch_size, num_build_batches, num_probe_batches, join_type);
  DCHECK_OK(join_case.Init(dop, task_runner));
  auto prober =
      join_case.GetProber(ProberFactory<TaskGroupProber>(TaskGroupProber::Make));

  auto task_group_probe = task_runner.RegisterTaskGroup(
      [&](size_t thread_index, int64_t task_id) -> arrow::Status {
        return prober.ProbeSingleBatch(thread_index, task_id);
      },
      [&](size_t thread_index) -> arrow::Status {
        return prober.ProbeFinished(thread_index);
      });

  task_runner.RegisterEnd();

  DCHECK_OK(task_runner.StartScheduling(dop));

  DCHECK_OK(join_case.Build());
  task_runner.WaitForIdle();

  DCHECK_OK(task_runner.StartTaskGroup(task_group_probe, num_probe_batches));
  task_runner.WaitForIdle();
}

struct FollyFutureTaskRunner : public TaskRunner {
  FollyFutureTaskRunner(size_t dop, size_t num_threads)
      : TaskRunner(), dop(dop), executor(num_threads) {}

  int RegisterTaskGroup(Task task, TaskCont task_cont) override {
    int task_group_id = task_groups.size();
    auto task_wrapped = [this, task = std::move(task)](size_t thread_id,
                                                       int64_t task_id) {
      thread_ids.insert(std::this_thread::get_id());
      return task(thread_id, task_id);
    };
    task_groups.emplace(task_group_id,
                        std::make_pair(std::move(task_wrapped), std::move(task_cont)));
    return task_group_id;
  }

  arrow::Status StartTaskGroup(int task_group_id, int64_t num_tasks) override {
    std::vector<folly::Future<arrow::Status>> task_futures;
    auto task = task_groups[task_group_id].first;
    for (int64_t task_id = 0; task_id < num_tasks;) {
      for (size_t thread_id = 0; thread_id < dop && task_id < num_tasks;
           thread_id++, task_id++) {
        if (thread_id == task_id) {
          task_futures.emplace_back(
              folly::via(&executor).thenValue([task, thread_id, task_id](folly::Unit) {
                return task(thread_id, task_id);
              }));
        } else {
          task_futures[thread_id] =
              std::move(task_futures[thread_id])
                  .thenValue([task, thread_id, task_id](arrow::Status status) {
                    DCHECK_OK(status);
                    return task(thread_id, task_id);
                  });
        }
      }
    }
    folly::collectAll(task_futures)
        .via(&executor)
        .thenValue([this, task_group_id](auto&& results) {
          for (auto& result : results) {
            DCHECK_OK(result.value());
          }
          auto cont = task_groups[task_group_id].second;
          DCHECK_OK(cont(0));
        })
        .wait();
    return arrow::Status::OK();
  }

  void OutputBatch(int64_t, arrow::compute::ExecBatch batch) override {
    std::cout << batch.ToString() << std::endl;
  }

  size_t NumThreadsOccupied() const { return thread_ids.size(); }

 private:
  const size_t dop;
  folly::CPUThreadPoolExecutor executor;
  std::unordered_map<int, TaskRunner::TaskGroup> task_groups;
  std::unordered_set<std::thread::id> thread_ids;
};

// TODO: This test occasionally hangs.
TEST(HashJoinTest, FollyFuture) {
  int batch_size = 4096;
  int num_build_batches = 128;
  int num_probe_batches = 128 * 8;
  arrow::compute::JoinType join_type = arrow::compute::JoinType::RIGHT_OUTER;
  size_t dop = 16;
  size_t num_threads = 7;

  FollyFutureTaskRunner task_runner(dop, num_threads);
  HashJoinCase join_case(batch_size, num_build_batches, num_probe_batches, join_type);
  DCHECK_OK(join_case.Init(dop, task_runner));
  auto prober =
      join_case.GetProber(ProberFactory<TaskGroupProber>(TaskGroupProber::Make));

  auto task_group_probe = task_runner.RegisterTaskGroup(
      [&](size_t thread_index, int64_t task_id) -> arrow::Status {
        return prober.ProbeSingleBatch(thread_index, task_id);
      },
      [&](size_t thread_index) -> arrow::Status {
        return prober.ProbeFinished(thread_index);
      });

  DCHECK_OK(join_case.Build());

  DCHECK_OK(task_runner.StartTaskGroup(task_group_probe, num_probe_batches));

  std::cout << "thread id num: " << task_runner.NumThreadsOccupied() << std::endl;
}

struct ArrowFutureTaskRunner : public TaskRunner {
  ArrowFutureTaskRunner(size_t dop, size_t num_threads)
      : TaskRunner(),
        dop(dop),
        thread_pool(*arrow::internal::ThreadPool::Make(num_threads)) {}

  int RegisterTaskGroup(Task task, TaskCont task_cont) override {
    int task_group_id = task_groups.size();
    auto task_wrapped = [this, task = std::move(task)](size_t thread_id,
                                                       int64_t task_id) {
      thread_ids.insert(std::this_thread::get_id());
      return task(thread_id, task_id);
    };
    task_groups.emplace(task_group_id,
                        std::make_pair(std::move(task_wrapped), std::move(task_cont)));
    return task_group_id;
  }

  arrow::Status StartTaskGroup(int task_group_id, int64_t num_tasks) override {
    std::vector<arrow::Future<>> src_futures;
    for (size_t thread_id = 0; thread_id < dop; thread_id++) {
      src_futures.emplace_back(arrow::Future<>::Make());
    }
    std::vector<arrow::Future<>> task_futures;
    auto task = task_groups[task_group_id].first;
    arrow::CallbackOptions always_options{arrow::ShouldSchedule::Always,
                                          thread_pool.get()};
    arrow::CallbackOptions if_different_options{
        arrow::ShouldSchedule::IfDifferentExecutor, thread_pool.get()};
    for (int64_t task_id = 0; task_id < num_tasks;) {
      for (size_t thread_id = 0; thread_id < dop && task_id < num_tasks;
           thread_id++, task_id++) {
        if (task_id == thread_id) {
          task_futures.emplace_back(src_futures[thread_id].Then(
              [task, thread_id, task_id] { DCHECK_OK(task(thread_id, task_id)); }, {},
              always_options));
        } else {
          task_futures[thread_id] =
              std::move(task_futures[thread_id])
                  .Then(
                      [task, thread_id, task_id] { DCHECK_OK(task(thread_id, task_id)); },
                      {}, if_different_options);
        }
      }
    }
    auto fut = arrow::AllFinished(task_futures).Then([this, task_group_id]() {
      auto cont = task_groups[task_group_id].second;
      DCHECK_OK(cont(0));
    });
    for (auto& f : src_futures) {
      f.MarkFinished();
    }
    return fut.status();
  }

  void OutputBatch(int64_t, arrow::compute::ExecBatch batch) override {
    std::cout << batch.ToString() << std::endl;
  }

  size_t NumThreadsOccupied() const { return thread_ids.size(); }

  auto GetThreadPool() const { return thread_pool.get(); }

 private:
  const size_t dop;
  std::shared_ptr<arrow::internal::ThreadPool> thread_pool;
  std::unordered_map<int, TaskRunner::TaskGroup> task_groups;
  std::unordered_set<std::thread::id> thread_ids;
};

// TODO: This test occasionally reports double-free.
TEST(HashJoinTest, ArrowFuture) {
  int batch_size = 4096;
  int num_build_batches = 128;
  int num_probe_batches = 128 * 8;
  arrow::compute::JoinType join_type = arrow::compute::JoinType::RIGHT_OUTER;
  size_t dop = 16;
  size_t num_threads = 7;

  ArrowFutureTaskRunner task_runner(dop, num_threads);
  HashJoinCase join_case(batch_size, num_build_batches, num_probe_batches, join_type);
  DCHECK_OK(join_case.Init(dop, task_runner));
  auto prober =
      join_case.GetProber(ProberFactory<TaskGroupProber>(TaskGroupProber::Make));

  auto task_group_probe = task_runner.RegisterTaskGroup(
      [&](size_t thread_index, int64_t task_id) -> arrow::Status {
        return prober.ProbeSingleBatch(thread_index, task_id);
      },
      [&](size_t thread_index) -> arrow::Status {
        return prober.ProbeFinished(thread_index);
      });

  DCHECK_OK(join_case.Build());

  DCHECK_OK(task_runner.StartTaskGroup(task_group_probe, num_probe_batches));

  std::cout << "thread id num: " << task_runner.NumThreadsOccupied() << std::endl;
}

using OptionalExecBatch = std::optional<arrow::compute::ExecBatch>;
using AsyncGenerator = arrow::AsyncGenerator<OptionalExecBatch>;

struct FromAsyncGeneratorProber {
 private:
  arrow::compute::HashJoinImpl* join;
  AsyncGenerator gen;
  mutable std::mutex mutex;

 public:
  FromAsyncGeneratorProber(arrow::compute::HashJoinImpl* join, AsyncGenerator gen)
      : join(join), gen(std::move(gen)) {}

  arrow::Status Probe(size_t dop, arrow::internal::Executor* exec) const {
    arrow::CallbackOptions options{arrow::ShouldSchedule::IfDifferentExecutor, exec};
    std::vector<arrow::Future<>> futures;
    for (size_t thread_id = 0; thread_id < dop; thread_id++) {
      auto loop = arrow::Loop([this, thread_id, options] {
        std::unique_lock<std::mutex> lock(mutex);
        auto future = gen();
        lock.unlock();
        return future
            .Then(
                [this, thread_id, options](const OptionalExecBatch& batch)
                    -> arrow::Future<std::optional<arrow::Status>> {
                  return batch.has_value() ? join->ProbeSingleBatch(thread_id, *batch)
                                           : std::optional<arrow::Status>{};
                },
                {}, options)
            .Then(
                [](const std::optional<arrow::Status>& status)
                    -> arrow::Result<arrow::ControlFlow<>> {
                  if (arrow::IsIterationEnd(status)) {
                    return arrow::Break();
                  }
                  if (status.has_value() && !status.value().ok()) {
                    return status.value();
                  }
                  return arrow::Continue();
                },
                {}, options);
      });
      futures.emplace_back(std::move(loop));
    }
    auto future = arrow::AllFinished(futures).Then(
        [this] { return join->ProbingFinished(0); }, {}, options);
    return future.status();
  }
};

FromAsyncGeneratorProber MakeFromVectorGeneratorProber(
    arrow::compute::HashJoinImpl* join, arrow::util::AccumulationQueue&& probe_batches) {
  std::vector<std::optional<arrow::compute::ExecBatch>> batches;
  for (size_t i = 0; i < probe_batches.batch_count(); i++) {
    batches.emplace_back(std::move(probe_batches[i]));
  }
  auto gen = arrow::MakeVectorGenerator(std::move(batches));
  return FromAsyncGeneratorProber(join, std::move(gen));
}

template <typename FromAsyncGeneratorProberFactory>
void HashJoinTestArrowFromAsyncGeneratorProber(FromAsyncGeneratorProberFactory factory) {
  int batch_size = 4096;
  int num_build_batches = 128;
  int num_probe_batches = 128 * 8;
  arrow::compute::JoinType join_type = arrow::compute::JoinType::RIGHT_OUTER;
  size_t dop = 16;
  size_t num_threads = 7;

  ArrowFutureTaskRunner task_runner(dop, num_threads);
  HashJoinCase join_case(batch_size, num_build_batches, num_probe_batches, join_type);
  DCHECK_OK(join_case.Init(dop, task_runner));
  auto prober =
      join_case.GetProber(ProberFactory<FromAsyncGeneratorProber>(std::move(factory)));

  DCHECK_OK(join_case.Build());

  DCHECK_OK(prober.Probe(dop, task_runner.GetThreadPool()));

  std::cout << "thread id num: " << task_runner.NumThreadsOccupied() << std::endl;
}

TEST(HashJoinTest, ArrowFromVectorGenerator) {
  HashJoinTestArrowFromAsyncGeneratorProber(MakeFromVectorGeneratorProber);
}

struct AsAsyncGeneratorFromAsyncGeneratorProber {
 private:
  arrow::compute::HashJoinImpl* join;
  AsyncGenerator gen;
  const size_t dop;
  arrow::internal::Executor* exec;
  bool finished = false;
  std::mutex mutex;

 public:
  AsAsyncGeneratorFromAsyncGeneratorProber(arrow::compute::HashJoinImpl* join,
                                           AsyncGenerator gen, size_t dop,
                                           arrow::internal::Executor* exec)
      : join(join), gen(std::move(gen)), dop(dop), exec(exec) {}

 private:
  auto Gen() {
    auto lock = std::unique_lock<std::mutex>(mutex);
    return gen();
  }

 public:
  arrow::Future<std::optional<arrow::Status>> operator()(size_t thread_id) {
    {
      auto lock = std::unique_lock<std::mutex>(mutex);
      if (finished) {
        return arrow::AsyncGeneratorEnd<std::optional<arrow::Status>>();
      }
    }

    auto fut = Gen();
    arrow::CallbackOptions options{arrow::ShouldSchedule::IfDifferentExecutor, exec};
    return fut.Then(
        [this,
         thread_id](const OptionalExecBatch& batch) -> std::optional<arrow::Status> {
          {
            auto lock = std::unique_lock<std::mutex>(mutex);
            if (finished) {
              return std::nullopt;
            }
            if (arrow::IsIterationEnd(batch)) {
              finished = true;
            }
          }
          if (finished) {
            return join->ProbingFinished(0);
          }
          return join->ProbeSingleBatch(thread_id, std::move(batch.value()));
        },
        {}, std::move(options));
  }
};

AsAsyncGeneratorFromAsyncGeneratorProber
MakeAsAsyncGeneratorFromVectorAsyncGeneratorProber(
    arrow::compute::HashJoinImpl* join, arrow::util::AccumulationQueue&& probe_batches,
    size_t dop, arrow::internal::Executor* exec) {
  std::vector<std::optional<arrow::compute::ExecBatch>> batches;
  for (size_t i = 0; i < probe_batches.batch_count(); i++) {
    batches.emplace_back(std::move(probe_batches[i]));
  }
  auto gen = arrow::MakeVectorGenerator(std::move(batches));
  return AsAsyncGeneratorFromAsyncGeneratorProber(join, std::move(gen), dop, exec);
}

template <typename AsAsyncGeneratorFromAsyncGeneratorProberFactory>
void HashJoinTestArrowAsAsyncGeneratorFromAsyncGeneratorProber(
    AsAsyncGeneratorFromAsyncGeneratorProberFactory factory) {
  int batch_size = 4096;
  int num_build_batches = 128;
  int num_probe_batches = 128 * 8;
  arrow::compute::JoinType join_type = arrow::compute::JoinType::RIGHT_OUTER;
  size_t dop = 16;
  size_t num_threads = 7;

  ArrowFutureTaskRunner task_runner(dop, num_threads);
  HashJoinCase join_case(batch_size, num_build_batches, num_probe_batches, join_type);
  DCHECK_OK(join_case.Init(dop, task_runner));
  auto prober =
      join_case.GetProber(ProberFactory<AsAsyncGeneratorFromAsyncGeneratorProber>(
          [factory = std::move(factory), dop, exec = task_runner.GetThreadPool()](
              arrow::compute::HashJoinImpl* join,
              arrow::util::AccumulationQueue&& probe_batches) {
            return factory(join, std::move(probe_batches), dop, exec);
          }));

  DCHECK_OK(join_case.Build());

  {
    arrow::CallbackOptions options{arrow::ShouldSchedule::IfDifferentExecutor,
                                   task_runner.GetThreadPool()};
    std::vector<arrow::Future<>> futures;
    for (size_t thread_id = 0; thread_id < dop; thread_id++) {
      auto loop = arrow::Loop([&, thread_id] {
        auto future = prober(thread_id);
        return future.Then(
            [](const std::optional<arrow::Status>& status)
                -> arrow::Result<arrow::ControlFlow<>> {
              if (arrow::IsIterationEnd(status)) {
                return arrow::Break();
              }
              if (status.has_value() && !status.value().ok()) {
                return status.value();
              }
              return arrow::Continue();
            },
            {}, options);
      });
      futures.emplace_back(std::move(loop));
    }
    EXPECT_TRUE(arrow::AllFinished(futures).status().ok());
  }

  std::cout << "thread id num: " << task_runner.NumThreadsOccupied() << std::endl;
}

TEST(HashJoinTest, ArrowAsAsyncGeneratorFromVectorGenerator) {
  HashJoinTestArrowAsAsyncGeneratorFromAsyncGeneratorProber(
      MakeAsAsyncGeneratorFromVectorAsyncGeneratorProber);
}

struct ErrorInjectionProber {
 private:
  arrow::compute::HashJoinImpl* join;
  AsyncGenerator gen;
  mutable std::mutex mutex;

 public:
  ErrorInjectionProber(arrow::compute::HashJoinImpl* join, AsyncGenerator gen)
      : join(join), gen(std::move(gen)) {}

  arrow::Status Probe(size_t dop, arrow::internal::Executor* exec) const {
    arrow::CallbackOptions options{arrow::ShouldSchedule::IfDifferentExecutor, exec};
    std::vector<arrow::Future<>> futures;
    for (size_t thread_id = 0; thread_id < dop; thread_id++) {
      auto loop = arrow::Loop([this, thread_id, dop, options] {
        std::unique_lock<std::mutex> lock(mutex);
        auto future = gen();
        lock.unlock();
        return future
            .Then(
                [this, thread_id, options](const OptionalExecBatch& batch)
                    -> arrow::Future<std::optional<arrow::Status>> {
                  if (!batch.has_value()) {
                    return std::optional{arrow::Status::UnknownError("Injected Error")};
                  }
                  return std::optional{join->ProbeSingleBatch(thread_id, *batch)};
                },
                {}, options)
            .Then(
                [thread_id, dop](const std::optional<arrow::Status>& status)
                    -> arrow::Result<arrow::ControlFlow<>> {
                  if (arrow::IsIterationEnd(status)) {
                    return arrow::Break();
                  }
                  if (status.has_value() && !status.value().ok()) {
                    return status.value();
                  }
                  return arrow::Continue();
                },
                {}, options);
      });
      futures.emplace_back(std::move(loop));
    }
    auto future = arrow::AllFinished(futures);
    return future.status();
  }
};

ErrorInjectionProber MakeErrorInjectionProber(
    arrow::compute::HashJoinImpl* join, arrow::util::AccumulationQueue&& probe_batches) {
  std::vector<std::optional<arrow::compute::ExecBatch>> batches;
  for (size_t i = 0; i < probe_batches.batch_count(); i++) {
    batches.emplace_back(std::move(probe_batches[i]));
  }
  auto gen = arrow::MakeVectorGenerator(std::move(batches));
  return ErrorInjectionProber(join, std::move(gen));
}

template <typename ErrorInjectionProberFactory>
void HashJoinTestErrorInjectionProber(ErrorInjectionProberFactory factory) {
  int batch_size = 4096;
  int num_build_batches = 128;
  int num_probe_batches = 128 * 8;
  arrow::compute::JoinType join_type = arrow::compute::JoinType::RIGHT_OUTER;
  size_t dop = 16;
  size_t num_threads = 7;

  ArrowFutureTaskRunner task_runner(dop, num_threads);
  HashJoinCase join_case(batch_size, num_build_batches, num_probe_batches, join_type);
  DCHECK_OK(join_case.Init(dop, task_runner));
  auto prober =
      join_case.GetProber(ProberFactory<ErrorInjectionProber>(std::move(factory)));

  DCHECK_OK(join_case.Build());

  auto status = prober.Probe(dop, task_runner.GetThreadPool());

  EXPECT_EQ(arrow::Status::UnknownError("Injected Error"), status);

  std::cout << "thread id num: " << task_runner.NumThreadsOccupied() << std::endl;
}

TEST(HashJoinTest, ArrowErrorInjection) {
  HashJoinTestErrorInjectionProber(MakeErrorInjectionProber);
}

struct CancelProber {
 private:
  arrow::compute::HashJoinImpl* join;
  AsyncGenerator gen;
  const size_t num_batches;
  mutable std::atomic<bool> cancelled;
  mutable std::atomic<size_t> num_cancelled;
  mutable std::atomic<size_t> num_probed;
  mutable std::mutex mutex;

 public:
  CancelProber(arrow::compute::HashJoinImpl* join, AsyncGenerator gen, size_t num_batches)
      : join(join),
        gen(std::move(gen)),
        num_batches(num_batches),
        cancelled(false),
        num_cancelled(0),
        num_probed(0) {}

  auto Probe(size_t dop, arrow::internal::Executor* exec) const {
    arrow::CallbackOptions options{arrow::ShouldSchedule::IfDifferentExecutor, exec};
    std::vector<arrow::Future<>> futures;
    for (size_t thread_id = 0; thread_id < dop; thread_id++) {
      auto loop = arrow::Loop([this, thread_id, dop, options] {
        std::unique_lock<std::mutex> lock(mutex);
        auto future = gen();
        lock.unlock();
        return future
            .Then(
                [this, thread_id, options](const OptionalExecBatch& batch)
                    -> arrow::Future<std::optional<arrow::Status>> {
                  if (!batch.has_value()) {
                    return std::optional{arrow::Status::UnknownError("Injected Error")};
                  }
                  if (cancelled) {
                    num_cancelled++;
                    return std::optional{arrow::Status::Cancelled("Canceled")};
                  }
                  num_probed++;
                  return std::optional{join->ProbeSingleBatch(thread_id, *batch)};
                },
                {}, options)
            .Then(
                [this, thread_id, dop](const std::optional<arrow::Status>& status)
                    -> arrow::Result<arrow::ControlFlow<>> {
                  if (arrow::IsIterationEnd(status)) {
                    return arrow::Break();
                  }
                  if (status.has_value() && !status.value().ok()) {
                    cancelled = true;
                    return status.value();
                  }
                  return arrow::Continue();
                },
                {}, options);
      });
      futures.emplace_back(std::move(loop));
    }
    auto status = arrow::AllFinished(futures).status();
    EXPECT_EQ(cancelled, true);
    EXPECT_TRUE(status.Equals(arrow::Status::Cancelled("Canceled")) ||
                status.Equals(arrow::Status::UnknownError("Injected Error")));
    size_t actual_num_errors = 0, actual_num_cancelled = 0;
    for (const auto& future : futures) {
      EXPECT_TRUE(future.is_finished());
      if (future.status().Equals(arrow::Status::Cancelled("Canceled"))) {
        actual_num_cancelled++;
      } else if (future.status().Equals(arrow::Status::UnknownError("Injected Error"))) {
        actual_num_errors++;
      }
    }
    std::cout << "actual error num: " << actual_num_errors << std::endl;
    std::cout << "actual cancelled num: " << actual_num_cancelled << std::endl;
    std::cout << "expected cancelled num: " << num_cancelled << std::endl;
    std::cout << "expected probed num: " << num_probed << std::endl;
    EXPECT_GE(actual_num_errors, 1);
    EXPECT_GE(actual_num_cancelled, 1);
    EXPECT_EQ(actual_num_cancelled, num_cancelled);
    EXPECT_EQ(num_cancelled + num_probed, num_batches);
  }
};

CancelProber MakeCancelProber(arrow::compute::HashJoinImpl* join,
                              arrow::util::AccumulationQueue&& probe_batches) {
  std::vector<std::optional<arrow::compute::ExecBatch>> batches;
  for (size_t i = 0; i < probe_batches.batch_count(); i++) {
    batches.emplace_back(std::move(probe_batches[i]));
  }
  auto num_batches = probe_batches.batch_count();
  auto gen = arrow::MakeVectorGenerator(std::move(batches));
  return CancelProber(join, std::move(gen), num_batches);
}

template <typename CancelProberFactory>
void HashJoinTestCancelProber(CancelProberFactory factory) {
  int batch_size = 4096;
  int num_build_batches = 128;
  int num_probe_batches = 128 * 8;
  arrow::compute::JoinType join_type = arrow::compute::JoinType::RIGHT_OUTER;
  size_t dop = 16;
  size_t num_threads = 7;

  ArrowFutureTaskRunner task_runner(dop, num_threads);
  HashJoinCase join_case(batch_size, num_build_batches, num_probe_batches, join_type);
  DCHECK_OK(join_case.Init(dop, task_runner));
  auto prober = join_case.GetProber(ProberFactory<CancelProber>(std::move(factory)));

  DCHECK_OK(join_case.Build());

  prober.Probe(dop, task_runner.GetThreadPool());

  std::cout << "thread id num: " << task_runner.NumThreadsOccupied() << std::endl;
}

TEST(HashJoinTest, ArrowCancel) { HashJoinTestCancelProber(MakeCancelProber); }

// TODO: How the hell to use arrow::Future<arrow::Status> with arrow::Loop?
// TODO: Case about pipeline task pausing.