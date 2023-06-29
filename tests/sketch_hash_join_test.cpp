#include <arrow/acero/exec_plan.h>
#include <arrow/acero/query_context.h>
#include <arrow/acero/util.h>
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

void AssertBatchesEqual(const arrow::acero::BatchesWithSchema& out,
                        const arrow::acero::BatchesWithSchema& exp) {
  ASSERT_OK_AND_ASSIGN(auto out_table,
                       arrow::acero::TableFromExecBatches(out.schema, out.batches));
  ASSERT_OK_AND_ASSIGN(auto exp_table,
                       arrow::acero::TableFromExecBatches(exp.schema, exp.batches));

  std::vector<arrow::compute::SortKey> sort_keys;
  for (auto&& f : exp.schema->fields()) {
    sort_keys.emplace_back(f->name());
  }
  ASSERT_OK_AND_ASSIGN(
      auto exp_table_sort_ids,
      arrow::compute::SortIndices(exp_table, arrow::compute::SortOptions(sort_keys)));
  ASSERT_OK_AND_ASSIGN(auto exp_table_sorted,
                       arrow::compute::Take(exp_table, exp_table_sort_ids));
  ASSERT_OK_AND_ASSIGN(
      auto out_table_sort_ids,
      arrow::compute::SortIndices(out_table, arrow::compute::SortOptions(sort_keys)));
  ASSERT_OK_AND_ASSIGN(auto out_table_sorted,
                       arrow::compute::Take(out_table, out_table_sort_ids));

  AssertTablesEqual(*exp_table_sorted.table(), *out_table_sorted.table(),
                    /*same_chunk_layout=*/false, /*flatten=*/true);
}

struct HashJoinCase {
  size_t dop_;
  const std::shared_ptr<arrow::Schema> left_schema_, right_schema_;
  arrow::acero::HashJoinNodeOptions options_;
  size_t multiplicity_;
};

HashJoinCase MakeHashJoinCase(size_t dop, arrow::acero::JoinType join_type,
                              size_t multiplicity) {
  auto l_schema = arrow::schema(
      {arrow::field("l_i32", arrow::int32()), arrow::field("l_str", arrow::utf8())});
  auto r_schema = arrow::schema(
      {arrow::field("r_str", arrow::utf8()), arrow::field("r_i32", arrow::int32())});

  std::vector<arrow::FieldRef> left_out, right_out;

  if (join_type != arrow::acero::JoinType::RIGHT_SEMI &&
      join_type != arrow::acero::JoinType::RIGHT_ANTI) {
    left_out = {{0}, {1}};
  }
  if (join_type != arrow::acero::JoinType::LEFT_SEMI &&
      join_type != arrow::acero::JoinType::LEFT_ANTI) {
    right_out = {{0}, {1}};
  }

  return HashJoinCase{
      dop, l_schema, r_schema,
      arrow::acero::HashJoinNodeOptions(join_type, {{0}}, {{1}}, std::move(left_out),
                                        std::move(right_out)),
      multiplicity};
}

class TestHashJoinSerial : public testing::TestWithParam<HashJoinCase> {
 protected:
  void SetUp() override {
    query_ctx_ = std::make_unique<arrow::acero::QueryContext>(
        arrow::acero::QueryOptions{}, arrow::compute::ExecContext());
  }

  void Init() {
    const auto& join_case = GetParam();
    EXPECT_TRUE(query_ctx_->Init(join_case.dop_, nullptr).ok());
    EXPECT_TRUE(hash_join_
                    .Init(query_ctx_.get(), join_case.dop_, join_case.options_,
                          *join_case.left_schema_, *join_case.right_schema_)
                    .ok());
  }

  size_t Dop() const { return GetParam().dop_; }

  std::shared_ptr<arrow::Schema> OutputSchema() const {
    return hash_join_.OutputSchema();
  }

  void Build(const arrow::acero::BatchesWithSchema& build_batches) {
    auto build_pipe = hash_join_.BuildPipe();
    for (int i = 0; i < build_batches.batches.size(); ++i) {
      OperatorStatus status;
      EXPECT_TRUE(
          build_pipe(i % Dop(), std::move(build_batches.batches[i]), status).ok());
      EXPECT_TRUE(status.code == OperatorStatusCode::HAS_OUTPUT);
      EXPECT_FALSE(std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
    }

    auto build_drain = hash_join_.BuildDrain();
    for (size_t thread_id = 0; thread_id < Dop(); thread_id++) {
      OperatorStatus status;
      EXPECT_TRUE(build_drain(thread_id, std::nullopt, status).ok());
      EXPECT_TRUE(status.code == OperatorStatusCode::FINISHED);
      EXPECT_FALSE(std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
    }

    auto build_break = hash_join_.BuildBreak();
    RunTaskGroups(build_break);
  }

  OperatorStatus Probe(ThreadId thread_id, const arrow::ExecBatch& batch) {
    auto probe_pipe = hash_join_.ProbePipe();
    OperatorStatus status;
    EXPECT_TRUE(probe_pipe(thread_id, std::move(batch), status).ok());
    return status;
  }

  OperatorStatus Drain(ThreadId thread_id) {
    auto probe_drain = hash_join_.ProbeDrain();
    OperatorStatus status;
    EXPECT_TRUE(probe_drain(thread_id, std::nullopt, status).ok());
    return status;
  }

 private:
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

TEST_P(TestHashJoinSerial, BuildOnly) {
  Init();

  auto r_batches = GenerateBatchesFromString(
      GetParam().right_schema_,
      {R"([["j", null], ["i", 0], ["h", 1]])", R"([["g", 2], ["f", 3]])",
       R"([["e", 4], ["d", 2]])", R"([["c", 5]])", R"([["b", 3], ["a", 6]])"},
      GetParam().multiplicity_);

  Build(r_batches);
}

const std::vector<HashJoinCase> build_cases = []() {
  std::vector<HashJoinCase> cases;
  for (auto dop : {1, 4, 8, 16}) {
    for (auto join_type :
         {arrow::acero::JoinType::INNER, arrow::acero::JoinType::LEFT_OUTER,
          arrow::acero::JoinType::RIGHT_OUTER, arrow::acero::JoinType::LEFT_SEMI,
          arrow::acero::JoinType::RIGHT_SEMI, arrow::acero::JoinType::LEFT_ANTI,
          arrow::acero::JoinType::RIGHT_ANTI, arrow::acero::JoinType::FULL_OUTER}) {
      for (auto mul : {1, 128, 256, 512, 1024, 2048, 4096}) {
        cases.push_back(MakeHashJoinCase(dop, join_type, mul));
      }
    }
  }
  return cases;
}();

INSTANTIATE_TEST_SUITE_P(BuildOnly, TestHashJoinSerial, testing::ValuesIn(build_cases));

// TEST_F(TestHashJoinSerial, Probe) {
//   auto l_schema = arrow::schema(
//       {arrow::field("l_i32", arrow::int32()), arrow::field("l_str", arrow::utf8())});
//   auto r_schema = arrow::schema(
//       {arrow::field("r_str", arrow::utf8()), arrow::field("r_i32", arrow::int32())});

//   arrow::acero::HashJoinNodeOptions options(arrow::acero::JoinType::LEFT_OUTER, {{0}},
//                                             {{1}}, {{0}, {1}}, {{0}, {1}});

//   size_t dop = 4;
//   Init(dop, options, *l_schema, *r_schema);

//   auto l_batches = GenerateBatchesFromString(
//       l_schema,
//       {R"([[0,"d"], [1,"b"]])", R"([[2,"d"], [3,"a"], [4,"a"]])",
//        R"([[5,"b"], [6,"c"], [7,"e"], [8,"e"]])"},
//       1);

//   auto r_batches = GenerateBatchesFromString(
//       r_schema,
//       {R"([["f", 0], ["b", 1], ["b", 2]])", R"([["c", 3], ["g", 4]])", R"([["e",
//       5]])"}, 1);

//   Build(r_batches);

//   for (size_t thread_id = 0; thread_id < dop; thread_id++) {
//     {
//       auto status = Probe(thread_id, l_batches.batches[0]);
//       EXPECT_TRUE(status.code == OperatorStatusCode::HAS_OUTPUT);
//       EXPECT_FALSE(std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
//     }

//     {
//       auto status = Drain(thread_id);
//       EXPECT_TRUE(status.code == OperatorStatusCode::FINISHED);
//       auto output = std::get<std::optional<arrow::ExecBatch>>(status.payload);
//       EXPECT_TRUE(output.has_value());
//       auto exp_batches = GenerateBatchesFromString(
//           OutputSchema(), {R"([[0, "d", "f", 0], [1, "b", "b", 1]])"}, 1);
//       AssertBatchesEqual(
//           arrow::acero::BatchesWithSchema{{output.value()}, OutputSchema()},
//           exp_batches);
//     }
//   }
// }

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