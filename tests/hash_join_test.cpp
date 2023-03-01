#include <arrow/api.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/exec/accumulation_queue.h>
#include <arrow/compute/exec/hash_join.h>
#include <arrow/compute/exec/hash_join_node.h>
#include <arrow/compute/exec/options.h>
#include <arrow/compute/exec/task_util.h>
#include <arrow/compute/exec/test_util.h>
#include <arrow/compute/exec/util.h>
#include <arrow/util/logging.h>
#include <arrow/util/vector.h>
#include <gtest/gtest.h>

TEST(HashJoinTest, Basic) {
  arrow::util::AccumulationQueue l_batches_;
  arrow::util::AccumulationQueue r_batches_;
  std::unique_ptr<arrow::compute::HashJoinSchema> schema_mgr_;
  std::unique_ptr<arrow::compute::HashJoinImpl> join_;
  int task_group_probe_;

  size_t dop = 32;
  size_t num_threads = 16;
  int batch_size = 4096;
  int num_build_batches = 128;
  int num_probe_batches = 128 * 16;
  arrow::compute::JoinType join_type = arrow::compute::JoinType::INNER;
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

    auto l_field = field(l_name, key_types[i], arrow::key_value_metadata(probe_metadata));
    auto r_field = field(r_name, key_types[i], arrow::key_value_metadata(build_metadata));

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
    l_batches_.InsertBatch(std::move(batch));
  for (arrow::compute::ExecBatch& batch : r_batches_with_schema.batches)
    r_batches_.InsertBatch(std::move(batch));

  schema_mgr_ = std::make_unique<arrow::compute::HashJoinSchema>();
  arrow::compute::Expression filter = arrow::compute::literal(true);
  DCHECK_OK(schema_mgr_->Init(join_type, *l_batches_with_schema.schema, left_keys,
                              *r_batches_with_schema.schema, right_keys, filter, "l_",
                              "r_"));

  join_ = *arrow::compute::HashJoinImpl::MakeSwiss();

  auto* memory_pool = arrow::default_memory_pool();
  arrow::compute::QueryContext ctx_(
      {}, arrow::compute::ExecContext(memory_pool, NULLPTR, NULLPTR));
  DCHECK_OK(ctx_.Init(dop, NULLPTR));
  arrow::compute::ThreadIndexer thread_id;
  auto scheduler = arrow::compute::TaskScheduler::Make();
  auto thread_pool = *arrow::internal::ThreadPool::Make(num_threads);
  // *arrow::internal::ThreadPool::Make(arrow::internal::ThreadPool::DefaultCapacity());
  // arrow::compute::ExecContext exec_ctx(memory_pool, tp.get());

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

  DCHECK_OK(join_->Init(&ctx_, join_type, dop, &(schema_mgr_->proj_maps[0]),
                        &(schema_mgr_->proj_maps[1]), std::move(key_cmp),
                        std::move(filter), std::move(register_task_group_callback),
                        std::move(start_task_group_callback),
                        std::move(output_batch_callback), [](int64_t x) {}));

  task_group_probe_ = register_task_group_callback(
      [&](size_t thread_index, int64_t task_id) -> arrow::Status {
        return join_->ProbeSingleBatch(thread_index, std::move(l_batches_[task_id]));
      },
      [&](size_t thread_index) -> arrow::Status {
        return join_->ProbingFinished(thread_index);
      });

  scheduler->RegisterEnd();

  DCHECK_OK(scheduler->StartScheduling(0, schedule_callback, dop, false));
  // ARROW_ASSIGN_OR_RAISE(auto tp = arrow::util::Threa)

  DCHECK_OK(join_->BuildHashTable(0, std::move(r_batches_), [&](size_t thread_index) {
    return arrow::Status::OK();
  }));

  DCHECK_OK(start_task_group_callback(task_group_probe_, l_batches_.batch_count()));

  DCHECK_OK(thread_pool->Shutdown(true));
}
