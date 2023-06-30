#include <arrow/acero/exec_plan.h>
#include <arrow/acero/query_context.h>
#include <arrow/acero/util.h>
#include <arrow/compute/exec.h>
#include <gtest/gtest.h>

#include "arrow/acero/test_util_internal.h"
#include "sketch_hash_join.h"

using namespace arra;

void AppendBatchesFromString(arrow::acero::BatchesWithSchema& out_batches,
                             const std::vector<std::string_view>& json_strings,
                             int multiplicity_intra, int multiplicity_inter) {
  std::vector<arrow::TypeHolder> types;
  for (auto&& field : out_batches.schema->fields()) {
    types.emplace_back(field->type());
  }

  for (auto&& s : json_strings) {
    std::stringstream ss;
    ss << "[" << s;
    for (int repeat = 1; repeat < multiplicity_intra; ++repeat) {
      ss << ", " << s;
    }
    ss << "]";
    out_batches.batches.push_back(arrow::acero::ExecBatchFromJSON(types, ss.str()));
  }

  size_t batch_count = out_batches.batches.size();
  for (int repeat = 1; repeat < multiplicity_inter; ++repeat) {
    for (size_t i = 0; i < batch_count; ++i) {
      out_batches.batches.push_back(out_batches.batches[i]);
    }
  }
}

arrow::acero::BatchesWithSchema GenerateBatchesFromString(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<std::string_view>& json_strings, int multiplicity_intra,
    int multiplicity_inter) {
  arrow::acero::BatchesWithSchema out_batches{{}, schema};
  AppendBatchesFromString(out_batches, json_strings, multiplicity_intra,
                          multiplicity_inter);
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

struct HashJoinConstants {
  static std::shared_ptr<arrow::Schema> LeftSchema() {
    static auto left_schema = arrow::schema(
        {arrow::field("l_i32", arrow::int32()), arrow::field("l_str", arrow::utf8())});
    return left_schema;
  }

  static std::shared_ptr<arrow::Schema> RightSchema() {
    static auto right_schema = arrow::schema(
        {arrow::field("r_str", arrow::utf8()), arrow::field("r_i32", arrow::int32())});
    return right_schema;
  }

  static arrow::acero::BatchesWithSchema LeftBatches(size_t multiplicity_intra,
                                                     size_t multiplicity_inter) {
    return GenerateBatchesFromString(
        LeftSchema(),
        {R"([null, "a"], [0, "b"], [1111, "c"], [2, "d"], [3333, "e"], [4, null], [4, "f"], [5555, "g"], [6666, "h"], [7, "i"])"},
        multiplicity_intra, multiplicity_inter);
  }

  static arrow::acero::BatchesWithSchema RightBatches(size_t multiplicity_intra,
                                                      size_t multiplicity_inter) {
    return GenerateBatchesFromString(
        RightSchema(),
        {R"(["i", null], ["h", 0], ["g", 1])", R"(["f", 2])", R"(["e", 2], ["d", 3])",
         R"(["c", 3])", R"([null, 4], ["b", 5], ["a", 6])"},
        multiplicity_intra, multiplicity_inter);
  }

  static std::shared_ptr<arrow::Schema> ExpSchema(arrow::acero::JoinType join_type) {
    switch (join_type) {
      case arrow::acero::JoinType::INNER:
      case arrow::acero::JoinType::LEFT_OUTER:
      case arrow::acero::JoinType::RIGHT_OUTER:
      case arrow::acero::JoinType::FULL_OUTER:
        return arrow::schema({arrow::field("l_i32", arrow::int32()),
                              arrow::field("l_str", arrow::utf8()),
                              arrow::field("r_str", arrow::utf8()),
                              arrow::field("r_i32", arrow::int32())});
      case arrow::acero::JoinType::LEFT_SEMI:
      case arrow::acero::JoinType::LEFT_ANTI:
        return LeftSchema();
      case arrow::acero::JoinType::RIGHT_SEMI:
      case arrow::acero::JoinType::RIGHT_ANTI:
        return RightSchema();
    }
    EXPECT_TRUE(false);
  }

  static arrow::acero::BatchesWithSchema ExpBatches(arrow::acero::JoinType join_type,
                                                    size_t multiplicity_left,
                                                    size_t multiplicity_right) {
    auto exp_schema = ExpSchema(join_type);
    constexpr size_t left_base = 10, right_base = 10, inner_base = 5, left_outer_base = 5,
                     right_outer_base = 5;
    std::string inner_seed =
        R"([0, "b", "h", 0], [2, "d", "f", 2], [2, "d", "e", 2], [4, null, "b", 4], [4, "f", "b", 4])";
    std::string left_outer_seed =
        R"([null, "a", null, null], [1111, "c", null, null], [3333, "e", null, null], [5555, "g", null, null], [6666, "h", null, null], [7, "i", null, null])";
    std::string right_outer_seed =
        R"([null, null, "i", null], [null, null, "g", 1], [null, null, "d", 3], [null, null, "c", 3], [null, null, "b", 5], [null, null, "a", 6])";
    std::string left_semi_seed = R"([0, "b"], [2, "d"], [4, null], [4, "f"])";
    std::string left_anti_seed =
        R"([null, "a"], [1111, "c"], [3333, "e"], [5555, "g"], [6666, "h"], [7, "i"])";
    std::string right_semi_seed = R"(["h", 0], ["f", 2], ["e", 2], [null, 4])";
    std::string right_anti_seed =
        R"(["i", null], ["g", 1], ["d", 3], ["c", 3], ["b", 5], ["a", 6])";
    switch (join_type) {
      case arrow::acero::JoinType::INNER: {
        return GenerateBatchesFromString(exp_schema, {inner_seed}, 1,
                                         multiplicity_left * multiplicity_right);
      }
      case arrow::acero::JoinType::LEFT_OUTER: {
        auto batches = GenerateBatchesFromString(exp_schema, {inner_seed}, 1,
                                                 multiplicity_left * multiplicity_right);
        AppendBatchesFromString(batches, {left_outer_seed}, 1, multiplicity_left);
        return batches;
      }
      case arrow::acero::JoinType::RIGHT_OUTER:
      case arrow::acero::JoinType::FULL_OUTER:
      case arrow::acero::JoinType::LEFT_SEMI:
      case arrow::acero::JoinType::LEFT_ANTI:
      case arrow::acero::JoinType::RIGHT_SEMI:
      case arrow::acero::JoinType::RIGHT_ANTI: {
        return GenerateBatchesFromString(exp_schema, {right_anti_seed}, 1,
                                         multiplicity_right);
      }
    }

    EXPECT_TRUE(false);
  }

  static size_t ExpRowCount(arrow::acero::JoinType join_type, size_t multiplicity_left,
                            size_t multiplicity_right) {
    constexpr size_t left_seed = 10, right_seed = 10, inner_seed = 5, left_outer_seed = 6,
                     right_outer_seed = 6, left_semi_seed = 4, left_anti_seed = 6,
                     right_semi_seed = 4, right_anti_seed = 6;
    size_t inner_match = inner_seed * multiplicity_left * multiplicity_right;
    size_t left_outer_match = left_outer_seed * multiplicity_left;
    size_t right_outer_match = right_outer_seed * multiplicity_right;
    size_t left_semi_match = left_semi_seed * multiplicity_left;
    size_t left_anti_match = left_anti_seed * multiplicity_left;
    size_t right_semi_match = right_semi_seed * multiplicity_right;
    size_t right_anti_match = right_anti_seed * multiplicity_right;
    switch (join_type) {
      case arrow::acero::JoinType::INNER:
        return inner_match;
      case arrow::acero::JoinType::LEFT_OUTER:
        return inner_match + left_outer_match;
      case arrow::acero::JoinType::RIGHT_OUTER:
        return inner_match + right_outer_match;
      case arrow::acero::JoinType::FULL_OUTER:
        return inner_match + left_outer_match + right_outer_match;
      case arrow::acero::JoinType::LEFT_SEMI:
        return left_semi_match;
      case arrow::acero::JoinType::LEFT_ANTI:
        return left_anti_match;
      case arrow::acero::JoinType::RIGHT_SEMI:
        return right_semi_match;
      case arrow::acero::JoinType::RIGHT_ANTI:
        return right_anti_match;
      default:
        EXPECT_TRUE(false);
    }
  }
};

struct HashJoinCase {
  HashJoinCase() = default;

  HashJoinCase(size_t dop, arrow::acero::JoinType join_type, size_t multiplicity_intra,
               size_t multiplicity_inter)
      : dop_(dop),
        multiplicity_intra_(multiplicity_intra),
        multiplicity_inter_(multiplicity_inter) {
    std::vector<arrow::FieldRef> left_out, right_out;

    if (join_type != arrow::acero::JoinType::RIGHT_SEMI &&
        join_type != arrow::acero::JoinType::RIGHT_ANTI) {
      left_out = {{0}, {1}};
    }
    if (join_type != arrow::acero::JoinType::LEFT_SEMI &&
        join_type != arrow::acero::JoinType::LEFT_ANTI) {
      right_out = {{0}, {1}};
    }

    options_ = arrow::acero::HashJoinNodeOptions(
        join_type, {{0}}, {{1}}, std::move(left_out), std::move(right_out));
  }

  size_t dop_;
  arrow::acero::HashJoinNodeOptions options_;
  size_t multiplicity_intra_;
  size_t multiplicity_inter_;

  std::vector<std::string> left_batches_seed_, right_batches_seed_;
};

auto HashJoinCaseName = [](const testing::TestParamInfo<HashJoinCase>& param_info) {
  const HashJoinCase& join_case = param_info.param;
  std::stringstream ss;
  ss << param_info.index << "_" << join_case.dop_ << "_"
     << arrow::acero::ToString(join_case.options_.join_type) << "_"
     << join_case.multiplicity_intra_ << "_" << join_case.multiplicity_inter_;
  return ss.str();
};

class TestHashJoinSerialBase : public testing::Test {
 protected:
  HashJoinCase join_case_;

  void SetUp() override {
    query_ctx_ = std::make_unique<arrow::acero::QueryContext>(
        arrow::acero::QueryOptions{}, arrow::compute::ExecContext());
  }

  void Init(HashJoinCase join_case) {
    join_case_ = std::move(join_case);

    EXPECT_TRUE(query_ctx_->Init(join_case_.dop_, nullptr).ok());
    EXPECT_TRUE(hash_join_
                    .Init(query_ctx_.get(), join_case_.dop_, join_case_.options_,
                          *HashJoinConstants::LeftSchema(),
                          *HashJoinConstants::RightSchema())
                    .ok());
  }

  std::shared_ptr<arrow::Schema> OutputSchema() const {
    return hash_join_.OutputSchema();
  }

  void Build(const arrow::acero::BatchesWithSchema& build_batches) {
    auto build_pipe = hash_join_.BuildPipe();
    for (int i = 0; i < build_batches.batches.size(); ++i) {
      OperatorStatus status;
      EXPECT_TRUE(
          build_pipe(i % join_case_.dop_, std::move(build_batches.batches[i]), status)
              .ok());
      EXPECT_TRUE(status.code == OperatorStatusCode::HAS_OUTPUT);
      EXPECT_FALSE(std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
    }

    auto build_drain = hash_join_.BuildDrain();
    for (size_t thread_id = 0; thread_id < join_case_.dop_; thread_id++) {
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

class TestHashJoinSerialBuildOnly : public TestHashJoinSerialBase,
                                    public testing::WithParamInterface<HashJoinCase> {
 protected:
  void Init() { TestHashJoinSerialBase::Init(GetParam()); }
};

TEST_P(TestHashJoinSerialBuildOnly, BuildOnly) {
  Init();

  auto r_batches = HashJoinConstants::RightBatches(join_case_.multiplicity_intra_,
                                                   join_case_.multiplicity_inter_);
  Build(r_batches);
}

const std::vector<HashJoinCase> build_cases = []() {
  std::vector<HashJoinCase> cases;
  for (auto dop : {1, 8, 64}) {
    for (auto join_type :
         {arrow::acero::JoinType::INNER, arrow::acero::JoinType::LEFT_OUTER,
          arrow::acero::JoinType::RIGHT_OUTER, arrow::acero::JoinType::LEFT_SEMI,
          arrow::acero::JoinType::RIGHT_SEMI, arrow::acero::JoinType::LEFT_ANTI,
          arrow::acero::JoinType::RIGHT_ANTI, arrow::acero::JoinType::FULL_OUTER}) {
      for (auto num_rows : {1, (1 << 6) - 1, 1 << 6, (1 << 12) - 1, 1 << 12}) {
        for (auto intra : {1, (1 << 6) - 1, 1 << 6, (1 << 12) - 1, 1 << 12}) {
          auto inter = arrow::bit_util::CeilDiv(num_rows, intra);
          cases.emplace_back(dop, join_type, intra, inter);
        }
      }
    }
  }
  return cases;
}();

INSTANTIATE_TEST_SUITE_P(BuildOnlyCases, TestHashJoinSerialBuildOnly,
                         testing::ValuesIn(build_cases), HashJoinCaseName);

class TestHashJoinSerialProbeOne : public TestHashJoinSerialBase,
                                   public testing::WithParamInterface<HashJoinCase> {
 protected:
  void Init() { TestHashJoinSerialBase::Init(GetParam()); }
};

TEST_P(TestHashJoinSerialProbeOne, ProbeOne) {
  Init();

  auto l_batches = HashJoinConstants::LeftBatches(1, 1);
  auto r_batches = HashJoinConstants::RightBatches(join_case_.multiplicity_intra_,
                                                   join_case_.multiplicity_inter_);

  auto num_matches = 5 * join_case_.multiplicity_intra_ * join_case_.multiplicity_inter_;

  auto exp_batches = HashJoinConstants::ExpBatches(
      join_case_.options_.join_type, 1,
      join_case_.multiplicity_intra_ * join_case_.multiplicity_inter_);

  Build(r_batches);

  for (size_t thread_id = 0; thread_id < join_case_.dop_; thread_id++) {
    {
      auto status = Probe(thread_id, l_batches.batches[0]);
      EXPECT_TRUE(status.code == OperatorStatusCode::HAS_OUTPUT);
      EXPECT_FALSE(std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
    }

    {
      auto status = Drain(thread_id);
      EXPECT_TRUE(status.code == OperatorStatusCode::FINISHED);
      auto output = std::get<std::optional<arrow::ExecBatch>>(status.payload);
      EXPECT_TRUE(output.has_value());
      AssertBatchesEqual(
          arrow::acero::BatchesWithSchema{{output.value()}, OutputSchema()}, exp_batches);
    }
  }
}

INSTANTIATE_TEST_SUITE_P(ProbeOneCases, TestHashJoinSerialProbeOne,
                         testing::Values(HashJoinCase(4, arrow::acero::JoinType::INNER, 1,
                                                      1)),
                         HashJoinCaseName);

// class TestTaskRunner {
//   using Task = std::function<arrow::Status(size_t, int64_t)>;
//   using TaskCont = std::function<arrow::Status(size_t)>;

//   TestTaskRunner(size_t num_threads)
//       : scheduler(arrow::acero::TaskScheduler::Make()),
//         thread_pool(*arrow::internal::ThreadPool::Make(num_threads))
//         {}

//   void RegisterEnd() { scheduler->RegisterEnd(); }

//   arrow::Status StartScheduling(size_t dop) {
//     return scheduler->StartScheduling(
//         thread_id(),
//         [&](std::function<arrow::Status(size_t)> func) {
//           return thread_pool->Spawn([&, func]() {
//           ARROW_DCHECK_OK(func(thread_id()));
//           });
//         },
//         dop, false);
//   }

//   int RegisterTaskGroup(Task task, TaskCont task_cont) {
//     return scheduler->RegisterTaskGroup(std::move(task),
//     std::move(task_cont));
//   }

//   arrow::Status StartTaskGroup(int task_group_id, int64_t
//   num_tasks) {
//     return scheduler->StartTaskGroup(thread_id(),
//     task_group_id, num_tasks);
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