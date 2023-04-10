/// NOTE: How to implement hash join probe operator in a parallel-agnostic and
/// future-agnostic fashion.  That is:
/// 1. How to invoke and chain the `Push()` methods of all the operators in this
/// pipeline.
/// 2. How to invoke the `Finish()` methods of all the operators in this pipeline.
/// This is critical for cases like a pipeline having two hash right outer join probe
/// operators, the `Finish()` method of each will scan the hash table and produce
/// non-joined rows.
/// 3. The operator should be sync/async-agnostic, that is, the concern of how to invoke
/// them should be hidden from the operator implementation.
/// 4. The operator should be parallel-agnostic, that is, the concern of how to parallel
/// them should be hidden from the operator implementation.
/// 5. The operator should be backpressure-agnostic, that is, the concern of how to handle
/// backpressure should be hidden from the operator implementation. Try to encapsulate the
/// backpressure handling logic only among source, sink, and optionally the pipeline
/// runner itself.
/// 6. The operator should be cancel-agnostic, that is, the concern of how to cancel
/// all the corresponding tasks when one of them meets error, should be hidden from the
/// operator implementation.

#include <arrow/api.h>
#include <arrow/compute/exec/hash_join.h>
#include <arrow/compute/exec/hash_join_node.h>
#include <arrow/compute/exec/test_util.h>
#include <gtest/gtest.h>

enum class OperatorStatusCode : char {
  RUNNING = 0,
  FINISHED = 1,
  SPILLING = 2,
  CANCELLED = 3,
  ERROR = 4,
};

struct OperatorStatus {
  OperatorStatusCode code;
  arrow::Status status;

  static OperatorStatus RUNNING() {
    return OperatorStatus{OperatorStatusCode::RUNNING, arrow::Status::OK()};
  }
  static OperatorStatus FINISHED() {
    return OperatorStatus{OperatorStatusCode::RUNNING, arrow::Status::OK()};
  }
  static OperatorStatus SPILLING() {
    return OperatorStatus{OperatorStatusCode::RUNNING, arrow::Status::OK()};
  }
  static OperatorStatus CANCELLED() {
    return OperatorStatus{OperatorStatusCode::RUNNING, arrow::Status::OK()};
  }
  static OperatorStatus ERROR(arrow::Status status) {
    return OperatorStatus{OperatorStatusCode::RUNNING, std::move(status)};
  }
};

struct Batch {
  int value = 0;
};

struct OperatorResult {
  OperatorStatus status;
  std::optional<Batch> batch;
};

class TaskGroups {
 public:
  using Task = std::function<OperatorResult(size_t /*round*/)>;
  using TaskGroup = std::vector<std::pair<Task, size_t /*num_rounds*/>>;

  virtual ~TaskGroups() = default;

  virtual bool HasNext() = 0;
  virtual arrow::Result<TaskGroup> Next() = 0;
};

class Operator {
 public:
  virtual ~Operator() = default;

  virtual OperatorResult Push(size_t thread_id, const Batch& batch) {
    return {OperatorStatus::RUNNING(), {}};
  }
  virtual arrow::Result<std::unique_ptr<TaskGroups>> Break(size_t dop) { return nullptr; }
  virtual OperatorResult Finish(size_t thread_id) {
    return {OperatorStatus::RUNNING(), {}};
  }
};

TEST(OperatorTest, HashJoinBuild) {
  struct HashJoinCase {
    arrow::compute::JoinType join_type;
    size_t dop;
    std::vector<arrow::compute::JoinKeyCmp> key_cmp;
    arrow::compute::Expression filter;
    std::unique_ptr<arrow::compute::HashJoinSchema> schema_mgr;
    std::unique_ptr<arrow::compute::QueryContext> ctx;

    arrow::util::AccumulationQueue l_batches;
    arrow::util::AccumulationQueue r_batches;

    HashJoinCase(int batch_size, int num_build_batches, int num_probe_batches,
                 arrow::compute::JoinType join_type, size_t dop)
        : join_type(join_type), dop(dop) {
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
      schema_mgr = std::make_unique<arrow::compute::HashJoinSchema>();
      DCHECK_OK(schema_mgr->Init(join_type, *l_batches_with_schema.schema, left_keys,
                                 *r_batches_with_schema.schema, right_keys, filter, "l_",
                                 "r_"));

      auto* memory_pool = arrow::default_memory_pool();
      ctx = std::make_unique<arrow::compute::QueryContext>(
          arrow::compute::QueryOptions{},
          arrow::compute::ExecContext(memory_pool, NULLPTR, NULLPTR));
      DCHECK_OK(ctx->Init(dop, NULLPTR));
    }
  };

  class BuildTaskGroups : public TaskGroups {
   public:
    explicit BuildTaskGroups(arrow::compute::HashJoinImpl* join_impl, size_t dop)
        : join_impl(join_impl),
          dop(dop),
          count(0),
          current_build_task_group(-1),
          current_num_build_tasks(0),
          next_build_task_cont(
              [this](size_t) { return this->join_impl->BuildHashTable(0, {}, {}); }) {}

    bool HasNext() override { return count < build_task_groups.size(); }

    arrow::Result<TaskGroup> Next() override {
      ARROW_RETURN_NOT_OK(next_build_task_cont(0));
      auto it = build_task_groups.find(current_build_task_group);
      ARROW_RETURN_IF(it == build_task_groups.end(),
                      arrow::Status::Invalid("Build task group not found"));
      auto& build_task = it->second.first;
      auto& build_task_cont = it->second.second;

      TaskGroup task_group;
      task_group.reserve(current_num_build_tasks);

      // Spread tasks across threads.
      auto num_tasks_per_thread = (current_num_build_tasks + dop - 1) / dop;
      for (size_t build_task_id = 0; build_task_id < current_num_build_tasks;
           build_task_id += num_tasks_per_thread) {
        auto thread_id = task_group.size();
        auto num_tasks =
            std::min(num_tasks_per_thread, current_num_build_tasks - build_task_id);
        task_group.emplace_back(
            [build_task, build_task_cont, num_tasks_per_thread, build_task_id, thread_id,
             num_tasks](size_t round) {
              auto status =
                  build_task(thread_id, thread_id * num_tasks_per_thread + round);
              if (status.ok()) {
                return OperatorResult{OperatorStatus::RUNNING(), {}};
              } else {
                return OperatorResult{OperatorStatus::ERROR(std::move(status)), {}};
              }
            },
            num_tasks);
      }

      count++;
      next_build_task_cont = build_task_cont;
      return arrow::Result(std::move(task_group));
    }

   private:
    arrow::compute::HashJoinImpl* join_impl;

   public:
    using BuildTask = std::function<arrow::Status(size_t, int64_t)>;
    using BuildTaskCont = std::function<arrow::Status(size_t)>;
    using BuildTaskGroup = std::pair<BuildTask, BuildTaskCont>;

    int RegisterTaskGroup(BuildTask build_task, BuildTaskCont build_task_cont) {
      int id = build_task_groups.size();
      build_task_groups.emplace(
          id, std::make_pair(std::move(build_task), std::move(build_task_cont)));
      return id;
    }

    arrow::Status StartTaskGroup(int build_task_group, int64_t num_build_tasks) {
      current_build_task_group = build_task_group;
      current_num_build_tasks = num_build_tasks;
      return arrow::Status::OK();
    }

   private:
    const size_t dop;
    std::unordered_map<int, BuildTaskGroup> build_task_groups;
    size_t count;
    int current_build_task_group;
    int64_t current_num_build_tasks;
    BuildTaskCont next_build_task_cont;
  };

  class HashJoinBuildOperator : public Operator {
   public:
    HashJoinBuildOperator(const HashJoinCase& join_case)
        : join_case(join_case), join_impl(nullptr) {}
    ~HashJoinBuildOperator() override = default;

    arrow::Result<std::unique_ptr<TaskGroups>> Break(size_t dop) override {
      auto join_impl_ = arrow::compute::HashJoinImpl::MakeSwiss();
      ARROW_RETURN_NOT_OK(join_impl_);
      join_impl = std::move(*join_impl_);
      auto build_task_groups = std::make_unique<BuildTaskGroups>(join_impl.get(), dop);

      using BuildTask = std::function<arrow::Status(size_t, int64_t)>;
      using BuildTaskCont = std::function<arrow::Status(size_t)>;

      ARROW_RETURN_NOT_OK(join_impl->Init(
          join_case.ctx.get(), join_case.join_type, join_case.dop,
          &(join_case.schema_mgr->proj_maps[0]), &(join_case.schema_mgr->proj_maps[1]),
          std::move(join_case.key_cmp), std::move(join_case.filter),
          [build_task_groups = build_task_groups.get()](
              BuildTaskGroups::BuildTask build_task,
              BuildTaskGroups::BuildTaskCont build_task_cont) {
            return build_task_groups->RegisterTaskGroup(std::move(build_task),
                                                        std::move(build_task_cont));
          },
          [build_task_groups = build_task_groups.get()](int build_task_group_id,
                                                        int64_t num_build_tasks) {
            return build_task_groups->StartTaskGroup(build_task_group_id,
                                                     num_build_tasks);
          },
          {}, [](int64_t x) {}));

      return build_task_groups;
    }

   private:
    const HashJoinCase& join_case;
    std::shared_ptr<arrow::compute::HashJoinImpl> join_impl;
  };

  {
    int batch_size = 4096;
    int num_build_batches = 128;
    int num_probe_batches = 128 * 8;
    arrow::compute::JoinType join_type = arrow::compute::JoinType::RIGHT_OUTER;
    size_t dop = 16;
    size_t num_threads = 7;

    HashJoinCase join_case(batch_size, num_build_batches, num_probe_batches, join_type,
                           dop);
    HashJoinBuildOperator join_build(join_case);
    auto build_task_groups = join_build.Break(dop);
  }
}

// TODO: Case about chaining Push methods in a pipeline.

// TODO: Case about operator ping-pong between RUNNING and SPILLING states.

// TODO: Case about backpressure from sink to source.