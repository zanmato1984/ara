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

struct HashJoinCase {
  using Task = std::function<arrow::Status(size_t, int64_t)>;
  using TaskCont = std::function<arrow::Status(size_t)>;
  using RegisterTaskGroupCallBack = std::function<int(Task, TaskCont)>;
  using StartTaskGroupCallBack = std::function<arrow::Status(int, int64_t)>;
  using OutputBatchCallback =
      std::function<arrow::Status(int64_t, arrow::compute::ExecBatch batch)>;

  arrow::Status Init(int batch_size, int num_build_batches, int num_probe_batches,
                     arrow::compute::JoinType join_type, size_t dop,
                     RegisterTaskGroupCallBack register_task_group_callback,
                     StartTaskGroupCallBack start_task_group_callback,
                     OutputBatchCallback output_batch_callback) {
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

    arrow::compute::Expression filter = arrow::compute::literal(true);
    DCHECK_OK(schema_mgr->Init(join_type, *l_batches_with_schema.schema, left_keys,
                               *r_batches_with_schema.schema, right_keys, filter, "l_",
                               "r_"));

    auto* memory_pool = arrow::default_memory_pool();
    ctx = std::make_unique<arrow::compute::QueryContext>(
        arrow::compute::QueryOptions{},
        arrow::compute::ExecContext(memory_pool, NULLPTR, NULLPTR));
    DCHECK_OK(ctx->Init(dop, NULLPTR));

    return join->Init(ctx.get(), join_type, dop, &(schema_mgr->proj_maps[0]),
                      &(schema_mgr->proj_maps[1]), std::move(key_cmp), std::move(filter),
                      std::move(register_task_group_callback),
                      std::move(start_task_group_callback),
                      std::move(output_batch_callback), [](int64_t x) {});
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

TEST(HashJoinTest, ArrowAync) {
  int batch_size = 4096;
  int num_build_batches = 128;
  int num_probe_batches = 128 * 8;
  arrow::compute::JoinType join_type = arrow::compute::JoinType::INNER;
  size_t dop = 16;

  size_t num_threads = 7;
  arrow::compute::ThreadIndexer thread_id;
  auto scheduler = arrow::compute::TaskScheduler::Make();
  auto thread_pool = *arrow::internal::ThreadPool::Make(num_threads);

  auto schedule_callback =
      [&](std::function<arrow::Status(size_t)> func) -> arrow::Status {
    return thread_pool->Spawn([&, func]() { ARROW_DCHECK_OK(func(thread_id())); });
  };
  auto register_task_group_callback =
      [&](std::function<arrow::Status(size_t, int64_t)> task,
          std::function<arrow::Status(size_t)> cont) {
        return scheduler->RegisterTaskGroup(std::move(task), std::move(cont));
      };
  auto start_task_group_callback = [&](int task_group_id, int64_t num_tasks) {
    return scheduler->StartTaskGroup(thread_id(), task_group_id, num_tasks);
  };
  auto output_batch_callback = [&](int64_t, arrow::compute::ExecBatch batch) {
    std::cout << batch.ToString() << std::endl;
    return arrow::Status::OK();
  };

  HashJoinCase join_case;
  DCHECK_OK(join_case.Init(batch_size, num_build_batches, num_probe_batches, join_type,
                           dop, std::move(register_task_group_callback),
                           std::move(start_task_group_callback),
                           std::move(output_batch_callback)));
  auto prober =
      join_case.GetProber(ProberFactory<TaskGroupProber>(TaskGroupProber::Make));

  auto task_group_probe = register_task_group_callback(
      [&](size_t thread_index, int64_t task_id) -> arrow::Status {
        return prober.ProbeSingleBatch(thread_index, task_id);
      },
      [&](size_t thread_index) -> arrow::Status {
        return prober.ProbeFinished(thread_index);
      });

  scheduler->RegisterEnd();

  DCHECK_OK(scheduler->StartScheduling(0, schedule_callback, dop, false));

  DCHECK_OK(join_case.Build());
  thread_pool->WaitForIdle();

  DCHECK_OK(start_task_group_callback(task_group_probe, num_probe_batches));
  thread_pool->WaitForIdle();
}

TEST(HashJoinTest, FollyFuture) {
  int batch_size = 4096;
  int num_build_batches = 128;
  int num_probe_batches = 128 * 8;
  arrow::compute::JoinType join_type = arrow::compute::JoinType::RIGHT_OUTER;
  size_t dop = 16;

  size_t num_threads = 7;
  using TaskGroup = std::pair<HashJoinCase::Task, HashJoinCase::TaskCont>;
  std::unordered_map<int, TaskGroup> task_groups;
  std::unordered_set<std::thread::id> thread_ids;

  folly::CPUThreadPoolExecutor e(num_threads);

  auto register_task_group_callback = [&](HashJoinCase::Task task,
                                          HashJoinCase::TaskCont cont) {
    int task_group_id = task_groups.size();
    auto task_wrapped = [task = std::move(task), &thread_ids](size_t thread_id,
                                                              int64_t task_id) {
      thread_ids.insert(std::this_thread::get_id());
      return task(thread_id, task_id);
    };
    task_groups.emplace(task_group_id,
                        std::make_pair(std::move(task_wrapped), std::move(cont)));
    return task_group_id;
  };
  auto start_task_group_callback = [&](int task_group_id, int64_t num_tasks) {
    std::vector<folly::Future<arrow::Status>> task_futures;
    auto task = task_groups[task_group_id].first;
    for (int64_t task_id = 0; task_id < num_tasks;) {
      for (size_t thread_id = 0; thread_id < dop && task_id < num_tasks;
           thread_id++, task_id++) {
        if (thread_id == task_id) {
          task_futures.emplace_back(
              folly::via(&e).thenValue([task, thread_id, task_id](folly::Unit) {
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
        .via(&e)
        .thenValue([task_group_id, &task_groups](auto&& results) {
          for (auto& result : results) {
            DCHECK_OK(result.value());
          }
          auto cont = task_groups[task_group_id].second;
          DCHECK_OK(cont(0));
        })
        .wait();
    return arrow::Status::OK();
  };
  auto output_batch_callback = [&](int64_t, arrow::compute::ExecBatch batch) {
    std::cout << batch.ToString() << std::endl;
    return arrow::Status::OK();
  };

  HashJoinCase join_case;
  DCHECK_OK(join_case.Init(batch_size, num_build_batches, num_probe_batches, join_type,
                           dop, std::move(register_task_group_callback),
                           std::move(start_task_group_callback),
                           std::move(output_batch_callback)));
  auto prober =
      join_case.GetProber(ProberFactory<TaskGroupProber>(TaskGroupProber::Make));

  auto task_group_probe = register_task_group_callback(
      [&](size_t thread_index, int64_t task_id) -> arrow::Status {
        return prober.ProbeSingleBatch(thread_index, task_id);
      },
      [&](size_t thread_index) -> arrow::Status {
        return prober.ProbeFinished(thread_index);
      });

  DCHECK_OK(join_case.Build());

  DCHECK_OK(start_task_group_callback(task_group_probe, num_probe_batches));

  std::cout << "thread id num: " << thread_ids.size() << std::endl;
}

TEST(HashJoinTest, ArrowFuture) {
  int batch_size = 4096;
  int num_build_batches = 128;
  int num_probe_batches = 128 * 8;
  arrow::compute::JoinType join_type = arrow::compute::JoinType::RIGHT_OUTER;
  size_t dop = 16;

  size_t num_threads = 7;
  using TaskGroup = std::pair<HashJoinCase::Task, HashJoinCase::TaskCont>;
  std::unordered_map<int, TaskGroup> task_groups;
  std::unordered_set<std::thread::id> thread_ids;

  auto thread_pool = *arrow::internal::ThreadPool::Make(num_threads);

  auto register_task_group_callback = [&](HashJoinCase::Task task,
                                          HashJoinCase::TaskCont cont) {
    int task_group_id = task_groups.size();
    auto task_wrapped = [task = std::move(task), &thread_ids](size_t thread_id,
                                                              int64_t task_id) {
      thread_ids.insert(std::this_thread::get_id());
      return task(thread_id, task_id);
    };
    task_groups.emplace(task_group_id,
                        std::make_pair(std::move(task_wrapped), std::move(cont)));
    return task_group_id;
  };
  auto start_task_group_callback = [&](int task_group_id, int64_t num_tasks) {
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
    auto fut = arrow::AllComplete(task_futures).Then([task_group_id, &task_groups]() {
      auto cont = task_groups[task_group_id].second;
      DCHECK_OK(cont(0));
    });
    for (auto& f : src_futures) {
      f.MarkFinished();
    }
    fut.Wait();
    return arrow::Status::OK();
  };
  auto output_batch_callback = [&](int64_t, arrow::compute::ExecBatch batch) {
    std::cout << batch.ToString() << std::endl;
    return arrow::Status::OK();
  };

  HashJoinCase join_case;
  DCHECK_OK(join_case.Init(batch_size, num_build_batches, num_probe_batches, join_type,
                           dop, std::move(register_task_group_callback),
                           std::move(start_task_group_callback),
                           std::move(output_batch_callback)));
  auto prober =
      join_case.GetProber(ProberFactory<TaskGroupProber>(TaskGroupProber::Make));

  auto task_group_probe = register_task_group_callback(
      [&](size_t thread_index, int64_t task_id) -> arrow::Status {
        return prober.ProbeSingleBatch(thread_index, task_id);
      },
      [&](size_t thread_index) -> arrow::Status {
        return prober.ProbeFinished(thread_index);
      });

  DCHECK_OK(join_case.Build());

  DCHECK_OK(start_task_group_callback(task_group_probe, num_probe_batches));

  std::cout << "thread id num: " << thread_ids.size() << std::endl;
}

using OptionalExecBatch = std::optional<arrow::compute::ExecBatch>;
using AsyncGenerator = arrow::AsyncGenerator<OptionalExecBatch>;

struct AsyncGeneratorProber {
 private:
  arrow::compute::HashJoinImpl* join;
  AsyncGenerator gen;
  mutable std::mutex mutex;

 public:
  AsyncGeneratorProber(arrow::compute::HashJoinImpl* join, AsyncGenerator gen)
      : join(join), gen(std::move(gen)) {}

  arrow::Status Probe(size_t dop, arrow::internal::Executor* exec) const {
    arrow::CallbackOptions options{arrow::ShouldSchedule::IfDifferentExecutor, exec};
    struct StatusWrapper {
      arrow::Status status;
    };
    std::vector<arrow::Future<StatusWrapper>> futures;
    for (size_t thread_id = 0; thread_id < dop; thread_id++) {
      auto loop = arrow::Loop([this, thread_id, options] {
        std::unique_lock<std::mutex> lock(mutex);
        auto future = gen();
        lock.unlock();
        return future.Then(
            [this, thread_id, options](const OptionalExecBatch& batch)
                -> arrow::Future<arrow::ControlFlow<StatusWrapper>> {
              if (arrow::IsIterationEnd(batch)) {
                return arrow::Break(StatusWrapper{arrow::Status::OK()});
              }
              if (auto status = join->ProbeSingleBatch(thread_id, *batch); !status.ok()) {
                return arrow::Break(StatusWrapper{std::move(status)});
              }
              return arrow::Future<arrow::ControlFlow<StatusWrapper>>::MakeFinished(
                  arrow::Continue());
            },
            {}, options);
      });
      futures.emplace_back(std::move(loop));
    }
    auto future = arrow::All(futures)
                      .Then(
                          [](const std::vector<arrow::Result<StatusWrapper>>& results) {
                            for (const auto& result : results) {
                              RETURN_NOT_OK(result);
                              RETURN_NOT_OK(result.ValueUnsafe().status);
                            }
                            return arrow::Status::OK();
                          },
                          {}, arrow::CallbackOptions::Defaults())
                      .Then([this] { return join->ProbingFinished(0); }, {}, options);
    return future.status();
  }
};

AsyncGeneratorProber MakeVectorGeneratorProber(
    arrow::compute::HashJoinImpl* join, arrow::util::AccumulationQueue&& probe_batches) {
  std::vector<std::optional<arrow::compute::ExecBatch>> batches;
  for (size_t i = 0; i < probe_batches.batch_count(); i++) {
    batches.emplace_back(std::move(probe_batches[i]));
  }
  auto gen = arrow::MakeVectorGenerator(std::move(batches));
  return AsyncGeneratorProber(join, std::move(gen));
}

TEST(HashJoinTest, ArrowFutureAndVectorGenerator) {
  int batch_size = 4096;
  int num_build_batches = 128;
  int num_probe_batches = 128 * 8;
  arrow::compute::JoinType join_type = arrow::compute::JoinType::RIGHT_OUTER;
  size_t dop = 16;

  size_t num_threads = 7;
  using TaskGroup = std::pair<HashJoinCase::Task, HashJoinCase::TaskCont>;
  std::unordered_map<int, TaskGroup> task_groups;
  std::unordered_set<std::thread::id> thread_ids;

  auto thread_pool = *arrow::internal::ThreadPool::Make(num_threads);

  auto register_task_group_callback = [&](HashJoinCase::Task task,
                                          HashJoinCase::TaskCont cont) {
    int task_group_id = task_groups.size();
    auto task_wrapped = [task = std::move(task), &thread_ids](size_t thread_id,
                                                              int64_t task_id) {
      thread_ids.insert(std::this_thread::get_id());
      return task(thread_id, task_id);
    };
    task_groups.emplace(task_group_id,
                        std::make_pair(std::move(task_wrapped), std::move(cont)));
    return task_group_id;
  };
  auto start_task_group_callback = [&](int task_group_id, int64_t num_tasks) {
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
    auto fut = arrow::AllComplete(task_futures).Then([task_group_id, &task_groups]() {
      auto cont = task_groups[task_group_id].second;
      DCHECK_OK(cont(0));
    });
    for (auto& f : src_futures) {
      f.MarkFinished();
    }
    fut.Wait();
    return arrow::Status::OK();
  };
  auto output_batch_callback = [&](int64_t, arrow::compute::ExecBatch batch) {
    std::cout << batch.ToString() << std::endl;
    return arrow::Status::OK();
  };

  HashJoinCase join_case;
  DCHECK_OK(join_case.Init(batch_size, num_build_batches, num_probe_batches, join_type,
                           dop, std::move(register_task_group_callback),
                           std::move(start_task_group_callback),
                           std::move(output_batch_callback)));
  auto prober =
      join_case.GetProber(ProberFactory<AsyncGeneratorProber>(MakeVectorGeneratorProber));

  DCHECK_OK(join_case.Build());

  DCHECK_OK(prober.Probe(dop, thread_pool.get()));

  std::cout << "thread id num: " << thread_ids.size() << std::endl;
}