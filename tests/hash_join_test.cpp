#include <arrow/api.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/exec/task_util.h>
#include <arrow/compute/exec/options.h>
#include <arrow/compute/exec/test_util.h>
#include <arrow/compute/exec/hash_join_node.h>
#include <arrow/compute/exec/hash_join.h>
#include <arrow/compute/exec/accumulation_queue.h>
#include <arrow/util/logging.h>
#include <arrow/util/vector.h>
#include <gtest/gtest.h>

TEST(HashJoinTest, Basic) {
  std::unique_ptr<arrow::compute::TaskScheduler> scheduler_;
  arrow::util::AccumulationQueue l_batches_;
  arrow::util::AccumulationQueue r_batches_;
  std::unique_ptr<arrow::compute::HashJoinSchema> schema_mgr_;
  std::unique_ptr<arrow::compute::HashJoinImpl> join_;
  arrow::compute::QueryContext ctx_;
  int task_group_probe_;

  struct {
    uint64_t num_probe_rows;
  } stats_;

  struct BenchmarkSettings {
    int num_threads = 8;
    arrow::compute::JoinType join_type = arrow::compute::JoinType::INNER;
    // Change to 'true' to benchmark alternative, non-default and less optimized version of
    // a hash join node implementation.
    bool use_basic_implementation = false;
    int batch_size = 1024;
    int num_build_batches = 32;
    int num_probe_batches = 32 * 16;
    std::vector<std::shared_ptr<arrow::DataType>> key_types = {arrow::int32()};
    std::vector<std::shared_ptr<arrow::DataType>> build_payload_types = {arrow::int64(), arrow::decimal256(15, 2)};
    std::vector<std::shared_ptr<arrow::DataType>> probe_payload_types = {arrow::int64(), arrow::utf8()};

    double null_percentage = 0.0;
    double cardinality = 1.0;  // Proportion of distinct keys in build side
    double selectivity = 1.0;  // Probability of a match for a given row
  } settings;

  arrow::SchemaBuilder l_schema_builder, r_schema_builder;
  std::vector<arrow::FieldRef> left_keys, right_keys;
  std::vector<arrow::compute::JoinKeyCmp> key_cmp;
  for (size_t i = 0; i < settings.key_types.size(); i++) {
    std::string l_name = "lk" + std::to_string(i);
    std::string r_name = "rk" + std::to_string(i);

    // For integers, selectivity is the proportion of the build interval that overlaps
    // with the probe interval
    uint64_t num_build_rows = settings.num_build_batches * settings.batch_size;

    uint64_t min_build_value = 0;
    uint64_t max_build_value =
        static_cast<uint64_t>(num_build_rows * settings.cardinality);

    uint64_t min_probe_value =
        static_cast<uint64_t>((1.0 - settings.selectivity) * max_build_value);
    uint64_t max_probe_value = min_probe_value + max_build_value;

    std::unordered_map<std::string, std::string> build_metadata;
    build_metadata["null_probability"] = std::to_string(settings.null_percentage);
    build_metadata["min"] = std::to_string(min_build_value);
    build_metadata["max"] = std::to_string(max_build_value);
    build_metadata["min_length"] = "2";
    build_metadata["max_length"] = "20";

    std::unordered_map<std::string, std::string> probe_metadata;
    probe_metadata["null_probability"] = std::to_string(settings.null_percentage);
    probe_metadata["min"] = std::to_string(min_probe_value);
    probe_metadata["max"] = std::to_string(max_probe_value);

    auto l_field =
        field(l_name, settings.key_types[i], arrow::key_value_metadata(probe_metadata));
    auto r_field =
        field(r_name, settings.key_types[i], arrow::key_value_metadata(build_metadata));

    DCHECK_OK(l_schema_builder.AddField(l_field));
    DCHECK_OK(r_schema_builder.AddField(r_field));

    left_keys.push_back(arrow::FieldRef(l_name));
    right_keys.push_back(arrow::FieldRef(r_name));
    key_cmp.push_back(arrow::compute::JoinKeyCmp::EQ);
  }

  for (size_t i = 0; i < settings.build_payload_types.size(); i++) {
    std::string name = "lp" + std::to_string(i);
    DCHECK_OK(l_schema_builder.AddField(field(name, settings.probe_payload_types[i])));
  }

  for (size_t i = 0; i < settings.build_payload_types.size(); i++) {
    std::string name = "rp" + std::to_string(i);
    DCHECK_OK(r_schema_builder.AddField(field(name, settings.build_payload_types[i])));
  }

  auto l_schema = *l_schema_builder.Finish();
  auto r_schema = *r_schema_builder.Finish();

  arrow::compute::BatchesWithSchema l_batches_with_schema =
      arrow::compute::MakeRandomBatches(l_schema, settings.num_probe_batches, settings.batch_size);
  arrow::compute::BatchesWithSchema r_batches_with_schema =
      arrow::compute::MakeRandomBatches(r_schema, settings.num_build_batches, settings.batch_size);

  for (arrow::compute::ExecBatch& batch : l_batches_with_schema.batches)
    l_batches_.InsertBatch(std::move(batch));
  for (arrow::compute::ExecBatch& batch : r_batches_with_schema.batches)
    r_batches_.InsertBatch(std::move(batch));

  stats_.num_probe_rows = settings.num_probe_batches * settings.batch_size;

  schema_mgr_ = std::make_unique<arrow::compute::HashJoinSchema>();
  arrow::compute::Expression filter = arrow::compute::literal(true);
  DCHECK_OK(schema_mgr_->Init(settings.join_type, *l_batches_with_schema.schema,
                              left_keys, *r_batches_with_schema.schema, right_keys,
                              filter, "l_", "r_"));

  if (settings.use_basic_implementation) {
    join_ = *arrow::compute::HashJoinImpl::MakeBasic();
  } else {
    join_ = *arrow::compute::HashJoinImpl::MakeSwiss();
  }

//  omp_set_num_threads(settings.num_threads);
//  auto schedule_callback = [](std::function<Status(size_t)> func) -> Status {
//#pragma omp task
//    { DCHECK_OK(func(omp_get_thread_num())); }
//    return Status::OK();
//  };
//
//  scheduler_ = TaskScheduler::Make();
  DCHECK_OK(ctx_.Init(settings.num_threads, nullptr));

  std::unordered_map<int, std::pair<std::function<arrow::Status(size_t, int64_t)>,
                                    std::function<arrow::Status(size_t)>>>
      task_groups;

  auto register_task_group_callback =
      [&](std::function<arrow::Status(size_t, int64_t)> task,
          std::function<arrow::Status(size_t)> cont) {
        static int group_id = 0;
        task_groups.emplace(group_id, std::make_pair(std::move(task), std::move(cont)));
        return group_id++;
      };

  auto start_task_group_callback = [&](int task_group_id, int64_t num_tasks) {
    std::vector<std::thread> threads;
    for (size_t i = 0; i < settings.num_threads; ++i) {
      threads.emplace_back([task_group_id, thread_id = i, num_threads = settings.num_threads, num_tasks, &task_groups]() {
        for (size_t task_id = 0; task_id < num_tasks; task_id += num_threads) {
          DCHECK_OK(task_groups[task_group_id].first(thread_id, static_cast<int64_t>(task_id)));
        }
      });
    }
    for (auto & t : threads) {
      t.join();
    }
    threads.clear();
    for (size_t i = 0; i < settings.num_threads; ++i) {
      threads.emplace_back([task_group_id, thread_id = i, num_threads = settings.num_threads, num_tasks, &task_groups]() {
        for (size_t task_id = 0; task_id < num_tasks; task_id += num_threads) {
          DCHECK_OK(task_groups[task_group_id].first(thread_id, static_cast<int64_t>(task_id)));
        }
      });
    }
    for (auto & t : threads) {
      t.join();
    }
    return arrow::Status::OK();
  };

  auto output_batch_callback =
      [&](int64_t, arrow::compute::ExecBatch batch) {
        std::cout << batch.ToString() << std::endl;
        return arrow::Status::OK();
      };

  DCHECK_OK(join_->Init(
      &ctx_, settings.join_type, settings.num_threads, &(schema_mgr_->proj_maps[0]),
      &(schema_mgr_->proj_maps[1]), std::move(key_cmp), std::move(filter),
      std::move(register_task_group_callback), std::move(start_task_group_callback),
      std::move(output_batch_callback), {}));

  task_group_probe_ = register_task_group_callback(
      [&](size_t thread_index, int64_t task_id) -> arrow::Status {
        return join_->ProbeSingleBatch(thread_index, std::move(l_batches_[task_id]));
      },
      [&](size_t thread_index) -> arrow::Status {
        return join_->ProbingFinished(thread_index);
      });

  DCHECK_OK(
      join_->BuildHashTable(0, std::move(r_batches_), [&](size_t thread_index) {
        return start_task_group_callback(task_group_probe_,
                                          l_batches_.batch_count());
      }));
}
