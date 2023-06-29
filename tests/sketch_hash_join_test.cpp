#include <arrow/acero/exec_plan.h>
#include <arrow/acero/query_context.h>
#include <arrow/compute/exec.h>
#include <gtest/gtest.h>

#include "arrow/acero/test_util_internal.h"
#include "sketch_hash_join.h"

using namespace arra;

arrow::acero::BatchesWithSchema GenerateBatchesFromString(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<std::string_view>& json_strings, int multiplicity = 1) {
  arrow::acero::BatchesWithSchema out_batches{{}, schema};

  std::vector<arrow::TypeHolder> types;
  for (auto&& field : schema->fields()) {
    types.emplace_back(field->type());
  }

  for (auto&& s : json_strings) {
    out_batches.batches.push_back(arrow::acero::ExecBatchFromJSON(types, s));
  }

  size_t batch_count = out_batches.batches.size();
  for (int repeat = 1; repeat < multiplicity; ++repeat) {
    for (size_t i = 0; i < batch_count; ++i) {
      out_batches.batches.push_back(out_batches.batches[i]);
    }
  }

  return out_batches;
}

class TestHashJoinSerialFineGrained : public ::testing::Test {
 public:
  void SetUp() override {
    query_ctx_ = std::make_unique<arrow::acero::QueryContext>(
        arrow::acero::QueryOptions{}, arrow::compute::ExecContext());
  }

  void Init(size_t dop, const arra::detail::HashJoinNodeOptions& options,
            const arrow::Schema& left_schema, const arrow::Schema& right_schema) {
    dop_ = dop;
    EXPECT_TRUE(query_ctx_->Init(dop, nullptr).ok());
    EXPECT_TRUE(
        hash_join_.Init(query_ctx_.get(), dop, options, left_schema, right_schema).ok());
  }

  void Build(arrow::acero::BatchesWithSchema build_batches) {
    auto build_pipe = hash_join_.BuildPipe();
    for (int i = 0; i < build_batches.batches.size(); ++i) {
      OperatorStatus status;
      EXPECT_TRUE(build_pipe(i % dop_, std::move(build_batches.batches[i]), status).ok());
      EXPECT_TRUE(status.code == OperatorStatusCode::HAS_OUTPUT);
      EXPECT_FALSE(std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
    }

    auto build_break = hash_join_.BuildBreak();
    RunTaskGroups(build_break);
  }

  OperatorStatus ProbeOne(arrow::ExecBatch batch) {
    auto probe_pipe = hash_join_.ProbePipe();
    OperatorStatus status;
    EXPECT_TRUE(probe_pipe(0, std::move(batch), status).ok());
    return status;
  }

  OperatorStatus Drain() {
    auto probe_drain = hash_join_.ProbeDrain();
    OperatorStatus status;
    EXPECT_TRUE(probe_drain(0, std::nullopt, status).ok());
    return status;
  }

 private:
  size_t dop_;
  std::unique_ptr<arrow::acero::QueryContext> query_ctx_;
  HashJoin hash_join_;

 private:
  void RunTaskGroups(const TaskGroups& groups) {
    for (auto&& group : groups) {
      RunTaskGroup(group);
    }
  }

  void RunTaskGroup(const TaskGroup& group) {
    const auto& [task, num_tasks, task_cont] = group;
    std::vector<bool> finished(num_tasks, false);
    size_t finished_count = 0;
    while (finished_count < num_tasks) {
      for (size_t i = 0; i < num_tasks; ++i) {
        if (finished[i]) {
          continue;
        }
        OperatorStatus status;
        EXPECT_TRUE(task(i, status).ok());
        if (status.code == OperatorStatusCode::HAS_OUTPUT) {
          EXPECT_FALSE(
              std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
        } else if (status.code == OperatorStatusCode::FINISHED) {
          EXPECT_FALSE(
              std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
          finished[i] = true;
          ++finished_count;
        } else {
          EXPECT_TRUE(false);
        }
      }
    }
    if (task_cont.has_value()) {
      EXPECT_TRUE(task_cont.value()(0).ok());
    }
  }
};

TEST_F(TestHashJoinSerialFineGrained, BuildOnly) {
  auto l_schema = arrow::schema(
      {arrow::field("l_i32", arrow::int32()), arrow::field("l_str", arrow::utf8())});
  auto r_schema = arrow::schema(
      {arrow::field("r_str", arrow::utf8()), arrow::field("r_i32", arrow::int32())});

  arrow::acero::HashJoinNodeOptions options(arrow::acero::JoinType::INNER, {{0}}, {{1}},
                                            {{0}, {1}}, {{0}, {1}});

  Init(4, options, *l_schema, *r_schema);

  auto r_batches = GenerateBatchesFromString(
      r_schema,
      {R"([["f", 0], ["b", 1], ["b", 2]])", R"([["c", 3], ["g", 4]])", R"([["e", 5]])"},
      1);

  Build(r_batches);
}

TEST_F(TestHashJoinSerialFineGrained, ProbeOne) {
  auto l_schema = arrow::schema(
      {arrow::field("l_i32", arrow::int32()), arrow::field("l_str", arrow::utf8())});
  auto r_schema = arrow::schema(
      {arrow::field("r_str", arrow::utf8()), arrow::field("r_i32", arrow::int32())});

  arrow::acero::HashJoinNodeOptions options(arrow::acero::JoinType::INNER, {{0}}, {{1}},
                                            {{0}, {1}}, {{0}, {1}});

  Init(4, options, *l_schema, *r_schema);

  auto l_batches = GenerateBatchesFromString(
      l_schema,
      {R"([[0,"d"], [1,"b"]])", R"([[2,"d"], [3,"a"], [4,"a"]])",
       R"([[5,"b"], [6,"c"], [7,"e"], [8,"e"]])"},
      1);

  auto r_batches = GenerateBatchesFromString(
      r_schema,
      {R"([["f", 0], ["b", 1], ["b", 2]])", R"([["c", 3], ["g", 4]])", R"([["e", 5]])"},
      1);

  Build(r_batches);

  {
    auto status = ProbeOne(std::move(l_batches.batches[0]));
    EXPECT_TRUE(status.code == OperatorStatusCode::HAS_OUTPUT);
    EXPECT_FALSE(std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
  }

  {
    auto status = Drain();
    EXPECT_TRUE(status.code == OperatorStatusCode::FINISHED);
    EXPECT_TRUE(std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
  }
}

// class TestTaskRunner {
//   using Task = std::function<arrow::Status(size_t, int64_t)>;
//   using TaskCont = std::function<arrow::Status(size_t)>;

//   TestTaskRunner(size_t num_threads)
//       : scheduler(arrow::acero::TaskScheduler::Make()),
//         thread_pool(*arrow::internal::ThreadPool::Make(num_threads)) {}

//   void RegisterEnd() { scheduler->RegisterEnd(); }

//   arrow::Status StartScheduling(size_t dop) {
//     return scheduler->StartScheduling(
//         thread_id(),
//         [&](std::function<arrow::Status(size_t)> func) {
//           return thread_pool->Spawn([&, func]() { ARROW_DCHECK_OK(func(thread_id()));
//           });
//         },
//         dop, false);
//   }

//   int RegisterTaskGroup(Task task, TaskCont task_cont) {
//     return scheduler->RegisterTaskGroup(std::move(task), std::move(task_cont));
//   }

//   arrow::Status StartTaskGroup(int task_group_id, int64_t num_tasks) {
//     return scheduler->StartTaskGroup(thread_id(), task_group_id, num_tasks);
//   }

//   void WaitForIdle() { thread_pool->WaitForIdle(); }

//  private:
//   arrow::acero::ThreadIndexer thread_id;
//   std::unique_ptr<arrow::acero::TaskScheduler> scheduler;
//   std::shared_ptr<arrow::internal::ThreadPool> thread_pool;
// };

// class TestDriver {
//  public:
//   TestDriver(HashJoin* hash_join, TestTaskRunner* runner)
//       : hash_join_(hash_join), runner_(runner) {}

//  private:
//   HashJoin* hash_join_;
//   TestTaskRunner* runner_;
// };

// class TestHashJoin : public ::testing::Test {
//  private:
//   HashJoin hash_join_;
//   TestTaskRunner runner_;
//   TestDriver driver_;
// };