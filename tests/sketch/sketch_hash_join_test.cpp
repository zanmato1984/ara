#include <arrow/acero/query_context.h>
#include <arrow/compute/exec.h>
#include <gtest/gtest.h>

#include "sketch_hash_join.h"

using namespace arra::sketch;

void AppendBatchesFromString(arrow::acero::BatchesWithSchema& out_batches,
                             const std::vector<std::string_view>& json_strings,
                             int multiplicity_intra, int multiplicity_inter) {
  std::vector<arrow::TypeHolder> types;
  for (auto&& field : out_batches.schema->fields()) {
    types.emplace_back(field->type());
  }

  size_t new_start = out_batches.batches.size();
  for (auto&& s : json_strings) {
    std::stringstream ss;
    ss << "[" << s;
    for (int repeat = 1; repeat < multiplicity_intra; ++repeat) {
      ss << ", " << s;
    }
    ss << "]";
    out_batches.batches.push_back(arrow::acero::ExecBatchFromJSON(types, ss.str()));
  }

  size_t new_batch_count = out_batches.batches.size();
  for (int repeat = 1; repeat < multiplicity_inter; ++repeat) {
    for (size_t i = new_start; i < new_batch_count; ++i) {
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

void AssertBatchesLightEqual(const arrow::acero::BatchesWithSchema& out,
                             const arrow::acero::BatchesWithSchema& exp) {
  ASSERT_EQ(*out.schema, *exp.schema);
  size_t num_cols = exp.schema->num_fields();

  std::vector<std::shared_ptr<arrow::DataType>> exp_types;
  for (const auto& f : exp.schema->fields()) {
    exp_types.push_back(f->type());
  }

  size_t exp_num_rows = 0;
  std::vector<size_t> exp_num_nulls(num_cols, 0);
  for (size_t i = 0; i < exp.batches.size(); ++i) {
    exp_num_rows += exp.batches[i].length;
    for (size_t j = 0; j < exp.batches[i].values.size(); ++j) {
      auto col = exp.batches[i].values[j].array();
      exp_num_nulls[j] += col->GetNullCount();
    }
  }

  size_t out_num_rows = 0;
  std::vector<size_t> out_num_nulls(num_cols, 0);
  for (size_t i = 0; i < out.batches.size(); ++i) {
    ASSERT_EQ(out.batches[i].num_values(), num_cols);
    out_num_rows += out.batches[i].length;
    for (size_t j = 0; j < out.batches[i].values.size(); ++j) {
      ASSERT_TRUE(out.batches[i].values[j].is_array());
      auto col = out.batches[i].values[j].array();
      ASSERT_EQ(*col->type, *exp_types[j]);
      out_num_nulls[j] += col->GetNullCount();
    }
  }

  ASSERT_EQ(out_num_rows, exp_num_rows);
  ASSERT_EQ(out_num_nulls, exp_num_nulls);
}

struct HashJoinFixture {
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
  }

  static size_t ExpRowCount(arrow::acero::JoinType join_type, size_t multiplicity_left,
                            size_t multiplicity_right) {
    constexpr size_t inner_seed = 5, left_outer_seed = 6, right_outer_seed = 6,
                     left_semi_seed = 4, left_anti_seed = 6, right_semi_seed = 4,
                     right_anti_seed = 6;
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
    }
  }

  static arrow::acero::BatchesWithSchema ExpBatches(arrow::acero::JoinType join_type,
                                                    size_t multiplicity_left,
                                                    size_t multiplicity_right) {
    auto exp_schema = ExpSchema(join_type);
    std::string inner_seed =
        R"([0, "b", "h", 0], [2, "d", "f", 2], [2, "d", "e", 2], [4, null, null, 4], [4, "f", null, 4])";
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
      case arrow::acero::JoinType::RIGHT_OUTER: {
        auto batches = GenerateBatchesFromString(exp_schema, {inner_seed}, 1,
                                                 multiplicity_left * multiplicity_right);
        AppendBatchesFromString(batches, {right_outer_seed}, 1, multiplicity_right);
        return batches;
      }
      case arrow::acero::JoinType::FULL_OUTER: {
        auto batches = GenerateBatchesFromString(exp_schema, {inner_seed}, 1,
                                                 multiplicity_left * multiplicity_right);
        AppendBatchesFromString(batches, {left_outer_seed}, 1, multiplicity_left);
        AppendBatchesFromString(batches, {right_outer_seed}, 1, multiplicity_right);
        return batches;
      }
      case arrow::acero::JoinType::LEFT_SEMI: {
        return GenerateBatchesFromString(exp_schema, {left_semi_seed}, 1,
                                         multiplicity_left);
      }
      case arrow::acero::JoinType::LEFT_ANTI: {
        return GenerateBatchesFromString(exp_schema, {left_anti_seed}, 1,
                                         multiplicity_left);
      }
      case arrow::acero::JoinType::RIGHT_SEMI: {
        return GenerateBatchesFromString(exp_schema, {right_semi_seed}, 1,
                                         multiplicity_right);
      }
      case arrow::acero::JoinType::RIGHT_ANTI: {
        return GenerateBatchesFromString(exp_schema, {right_anti_seed}, 1,
                                         multiplicity_right);
      }
    }
  }
};

struct HashJoinCase {
  HashJoinCase() = default;

  HashJoinCase(size_t dop, arrow::acero::JoinType join_type,
               size_t left_multiplicity_intra, size_t left_multiplicity_inter,
               size_t right_multiplicity_intra, size_t right_multiplicity_inter)
      : dop_(dop),
        left_multiplicity_intra_(left_multiplicity_intra),
        left_multiplicity_inter_(left_multiplicity_inter),
        right_multiplicity_intra_(right_multiplicity_intra),
        right_multiplicity_inter_(right_multiplicity_inter) {
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

  std::string Name(size_t index) const {
    std::stringstream ss;
    ss << index << "_" << dop_ << "_" << arrow::acero::ToString(options_.join_type) << "_"
       << left_multiplicity_intra_ << "_" << left_multiplicity_inter_ << "_"
       << right_multiplicity_intra_ << "_" << right_multiplicity_inter_;
    return ss.str();
  }

  size_t dop_;
  arrow::acero::HashJoinNodeOptions options_;
  size_t left_multiplicity_intra_, left_multiplicity_inter_, right_multiplicity_intra_,
      right_multiplicity_inter_;
};

class TestHashJoin : public testing::Test {
 protected:
  HashJoinCase join_case_;
  std::unique_ptr<arrow::acero::QueryContext> query_ctx_;

  void SetUp() override {
    query_ctx_ = std::make_unique<arrow::acero::QueryContext>(
        arrow::acero::QueryOptions{}, arrow::compute::ExecContext());
  }

  void Init(HashJoinCase join_case) {
    join_case_ = std::move(join_case);
    ASSERT_OK(query_ctx_->Init(join_case_.dop_, nullptr));
  }
};

class TestHashJoinSerial : public TestHashJoin {
 protected:
  void Init(HashJoinCase join_case) {
    TestHashJoin::Init(std::move(join_case));
    ASSERT_OK(hash_join_.Init(query_ctx_.get(), join_case_.dop_, join_case_.options_,
                              *HashJoinFixture::LeftSchema(),
                              *HashJoinFixture::RightSchema()));
  }

  std::shared_ptr<arrow::Schema> OutputSchema() const {
    return hash_join_.OutputSchema();
  }

  void Build(const arrow::acero::BatchesWithSchema& build_batches) {
    auto build_sink = hash_join_.BuildSink();

    auto build_pipe = build_sink->Pipe();
    for (int i = 0; i < build_batches.batches.size(); ++i) {
      OperatorStatus status;
      ASSERT_OK(
          build_pipe(i % join_case_.dop_, std::move(build_batches.batches[i]), status));
      ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
      ASSERT_FALSE(std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
    }

    auto build_break = build_sink->Frontend();
    RunTaskGroups(build_break);
  }

  arrow::Status Probe(ThreadId thread_id, std::optional<arrow::ExecBatch> batch,
                      OperatorStatus& status) {
    auto probe_pipe = hash_join_.Pipe();
    return probe_pipe(thread_id, std::move(batch), status);
  }

  arrow::Status Drain(ThreadId thread_id, OperatorStatus& status) {
    auto probe_drain = hash_join_.Drain();
    return probe_drain.value()(thread_id, std::nullopt, status);
  }

  void StartScan() {
    scan_source_ = hash_join_.Source();
    ASSERT_NE(scan_source_, nullptr);
    ASSERT_TRUE(scan_source_->Backend().empty());
    auto scan_groups = scan_source_->Frontend();
    ASSERT_EQ(scan_groups.size(), 1);
    RunTaskGroup(scan_groups[0]);
  }

  arrow::Status Scan(ThreadId thread_id, OperatorStatus& status) {
    auto scan = scan_source_->Source();
    return scan(thread_id, status);
  }

 private:
  HashJoin hash_join_;
  std::unique_ptr<SourceOp> scan_source_;

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
        ASSERT_OK(task(i, status));
        if (status.code == OperatorStatusCode::HAS_OUTPUT) {
          ASSERT_FALSE(
              std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
        } else if (status.code == OperatorStatusCode::FINISHED) {
          ASSERT_FALSE(
              std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
          finished[i] = true;
          ++finished_count;
        } else {
          ASSERT_TRUE(false);
        }
      }
    }
    if (task_cont.has_value()) {
      ASSERT_OK(task_cont.value()());
    }
  }
};

struct BuildOnlyCase : public HashJoinCase {
  BuildOnlyCase() = default;

  BuildOnlyCase(size_t dop, arrow::acero::JoinType join_type, size_t multiplicity_intra,
                size_t multiplicity_inter)
      : HashJoinCase(dop, join_type, 0, 0, multiplicity_intra, multiplicity_inter) {}

  std::string Name(size_t index) const {
    std::stringstream ss;
    ss << index << "_" << dop_ << "_" << arrow::acero::ToString(options_.join_type) << "_"
       << right_multiplicity_intra_ << "_" << right_multiplicity_inter_;
    return ss.str();
  }
};

class SerialBuildOnly : public TestHashJoinSerial,
                        public testing::WithParamInterface<BuildOnlyCase> {
 protected:
  void Init() { TestHashJoinSerial::Init(GetParam()); }
};

TEST_P(SerialBuildOnly, BuildOnly) {
  Init();

  auto r_batches = HashJoinFixture::RightBatches(join_case_.right_multiplicity_intra_,
                                                 join_case_.right_multiplicity_inter_);
  Build(r_batches);
}

std::vector<BuildOnlyCase> SerialBuildOnlyCases() {
  std::vector<BuildOnlyCase> cases;
  for (auto dop : {1, 16}) {
    for (auto join_type :
         {arrow::acero::JoinType::INNER, arrow::acero::JoinType::LEFT_OUTER,
          arrow::acero::JoinType::RIGHT_OUTER, arrow::acero::JoinType::LEFT_SEMI,
          arrow::acero::JoinType::RIGHT_SEMI, arrow::acero::JoinType::LEFT_ANTI,
          arrow::acero::JoinType::RIGHT_ANTI, arrow::acero::JoinType::FULL_OUTER}) {
      for (auto num_res : {1, (1 << 6) - 1, 1 << 6, (1 << 12) - 1, 1 << 12}) {
        for (auto inter : {1, (1 << 6) - 1, 1 << 6, (1 << 12) - 1, 1 << 12}) {
          auto intra = arrow::bit_util::CeilDiv(num_res, inter);
          cases.emplace_back(dop, join_type, intra, inter);
        }
      }
    }
  }
  return cases;
}

INSTANTIATE_TEST_SUITE_P(SerialBuildOnlyCases, SerialBuildOnly,
                         testing::ValuesIn(SerialBuildOnlyCases()),
                         [](const auto& param_info) {
                           return param_info.param.Name(param_info.index);
                         });

template <arrow::acero::JoinType join_type>
struct ProbeCase : public HashJoinCase {
  ProbeCase() = default;

  ProbeCase(size_t dop, size_t left_multiplicity_intra, size_t left_multiplicity_inter,
            size_t right_multiplicity)
      : HashJoinCase(dop, join_type, left_multiplicity_intra, left_multiplicity_inter, 1,
                     right_multiplicity) {}

  std::string Name(size_t index) const {
    std::stringstream ss;
    ss << index << "_" << dop_ << "_" << left_multiplicity_intra_ << "_"
       << left_multiplicity_inter_ << "_" << right_multiplicity_inter_;
    return ss.str();
  }
};

template <arrow::acero::JoinType join_type>
class SerialProbe : public TestHashJoinSerial,
                    public testing::WithParamInterface<ProbeCase<join_type>> {
 protected:
  void Init() { TestHashJoinSerial::Init(this->GetParam()); }
};

template <arrow::acero::JoinType join_type>
const std::vector<ProbeCase<join_type>> SerialProbeCases() {
  std::vector<ProbeCase<join_type>> cases;
  for (auto dop : {1, 4}) {
    for (auto num_res : {63, 64, 127, 128, 255, 256, 511, 512, 1023, 1024, 2047, 2048,
                         4095, 4096, 8191, 8192}) {
      for (auto right_intra : {1, 2, 511, 512, 2047, 2048}) {
        auto left_intra = arrow::bit_util::CeilDiv(num_res, right_intra);
        cases.emplace_back(dop, left_intra, 1, right_intra);
      }
    }
  }
  return cases;
}

template <arrow::acero::JoinType join_type>
class SerialFineProbeInnerLeft : public SerialProbe<join_type> {
 public:
  static const arrow::acero::JoinType JoinType = join_type;

 protected:
  void ProbeOne() {
    this->Init();

    auto l_batches =
        HashJoinFixture::LeftBatches(this->join_case_.left_multiplicity_intra_,
                                     this->join_case_.left_multiplicity_inter_);
    auto r_batches =
        HashJoinFixture::RightBatches(this->join_case_.right_multiplicity_intra_,
                                      this->join_case_.right_multiplicity_inter_);

    auto exp_rows =
        HashJoinFixture::ExpRowCount(this->join_case_.options_.join_type,
                                     this->join_case_.left_multiplicity_intra_ *
                                         this->join_case_.left_multiplicity_inter_,
                                     this->join_case_.right_multiplicity_intra_ *
                                         this->join_case_.right_multiplicity_inter_);
    auto exp_batches =
        HashJoinFixture::ExpBatches(this->join_case_.options_.join_type,
                                    this->join_case_.left_multiplicity_intra_ *
                                        this->join_case_.left_multiplicity_inter_,
                                    this->join_case_.right_multiplicity_intra_ *
                                        this->join_case_.right_multiplicity_inter_);

    this->Build(r_batches);

    for (size_t thread_id = 0; thread_id < this->join_case_.dop_; thread_id++) {
      OperatorStatus status = OperatorStatus::Other(arrow::Status::UnknownError(""));
      if (exp_rows <= kMaxPipeBatchSize) {
        ASSERT_OK(this->Probe(thread_id, l_batches.batches[0], status));
        ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
        ASSERT_FALSE(
            std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());

        ASSERT_OK(this->Drain(thread_id, status));
        ASSERT_EQ(status.code, OperatorStatusCode::FINISHED);
        auto drain_batch = std::get<std::optional<arrow::ExecBatch>>(status.payload);
        ASSERT_TRUE(drain_batch.has_value());
        ASSERT_EQ(drain_batch.value().length, exp_rows);
        AssertBatchesEqual(
            arrow::acero::BatchesWithSchema{{drain_batch.value()}, this->OutputSchema()},
            exp_batches);
      } else if (exp_rows <= 2 * kMaxPipeBatchSize) {
        ASSERT_OK(this->Probe(thread_id, l_batches.batches[0], status));
        ASSERT_EQ(status.code, OperatorStatusCode::HAS_MORE_OUTPUT);
        auto probe_batch_first =
            std::get<std::optional<arrow::ExecBatch>>(status.payload);
        ASSERT_TRUE(probe_batch_first.has_value());
        ASSERT_EQ(probe_batch_first.value().length, kMaxPipeBatchSize);

        ASSERT_OK(this->Probe(thread_id, std::nullopt, status));
        ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
        ASSERT_FALSE(
            std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());

        ASSERT_OK(this->Drain(thread_id, status));
        ASSERT_EQ(status.code, OperatorStatusCode::FINISHED);
        auto drain_batch = std::get<std::optional<arrow::ExecBatch>>(status.payload);
        ASSERT_TRUE(drain_batch.has_value());
        ASSERT_EQ(drain_batch.value().length, exp_rows - kMaxPipeBatchSize);
        AssertBatchesEqual(
            arrow::acero::BatchesWithSchema{
                {probe_batch_first.value(), drain_batch.value()}, this->OutputSchema()},
            exp_batches);
      } else {
        ASSERT_OK(this->Probe(thread_id, l_batches.batches[0], status));
        ASSERT_EQ(status.code, OperatorStatusCode::HAS_MORE_OUTPUT);
        auto probe_batch_first =
            std::move(std::get<std::optional<arrow::ExecBatch>>(status.payload));
        ASSERT_TRUE(probe_batch_first.has_value());
        ASSERT_EQ(probe_batch_first.value().length, kMaxPipeBatchSize);

        ASSERT_OK(this->Probe(thread_id, std::nullopt, status));
        ASSERT_EQ(status.code, OperatorStatusCode::HAS_MORE_OUTPUT);
        auto probe_batch_second =
            std::move(std::get<std::optional<arrow::ExecBatch>>(status.payload));
        ASSERT_TRUE(probe_batch_second.has_value());
        ASSERT_EQ(probe_batch_second.value().length, kMaxPipeBatchSize);

        ASSERT_OK(this->Drain(thread_id, status));
        ASSERT_EQ(status.code, OperatorStatusCode::FINISHED);
        auto drain_batch = std::get<std::optional<arrow::ExecBatch>>(status.payload);
        ASSERT_TRUE(drain_batch.has_value());
        ASSERT_GT(drain_batch.value().length, 0);
        ASSERT_LT(drain_batch.value().length, kMaxPipeBatchSize);
      }
    }
  }

  void ProbeTwo() {
    this->Init();

    auto l_batches =
        HashJoinFixture::LeftBatches(this->join_case_.left_multiplicity_intra_,
                                     this->join_case_.left_multiplicity_inter_);
    auto r_batches =
        HashJoinFixture::RightBatches(this->join_case_.right_multiplicity_intra_,
                                      this->join_case_.right_multiplicity_inter_);

    auto exp_rows_single =
        HashJoinFixture::ExpRowCount(this->join_case_.options_.join_type,
                                     this->join_case_.left_multiplicity_intra_ *
                                         this->join_case_.left_multiplicity_inter_,
                                     this->join_case_.right_multiplicity_intra_ *
                                         this->join_case_.right_multiplicity_inter_);
    auto exp_batches_double =
        HashJoinFixture::ExpBatches(this->join_case_.options_.join_type,
                                    2 * this->join_case_.left_multiplicity_intra_ *
                                        this->join_case_.left_multiplicity_inter_,
                                    this->join_case_.right_multiplicity_intra_ *
                                        this->join_case_.right_multiplicity_inter_);

    this->Build(r_batches);

    for (size_t thread_id = 0; thread_id < this->join_case_.dop_; thread_id++) {
      OperatorStatus status = OperatorStatus::Other(arrow::Status::UnknownError(""));

      if (exp_rows_single <= kMaxPipeBatchSize) {
        ASSERT_OK(this->Probe(thread_id, l_batches.batches[0], status));
        ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
        ASSERT_FALSE(
            std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());

        if (exp_rows_single * 2 <= kMaxPipeBatchSize) {
          ASSERT_OK(this->Probe(thread_id, l_batches.batches[0], status));
          ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
          ASSERT_FALSE(
              std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());

          ASSERT_OK(this->Drain(thread_id, status));
          ASSERT_EQ(status.code, OperatorStatusCode::FINISHED);
          auto drain_batch = std::get<std::optional<arrow::ExecBatch>>(status.payload);
          ASSERT_TRUE(drain_batch.has_value());
          ASSERT_EQ(drain_batch.value().length, exp_rows_single * 2);
          AssertBatchesEqual(arrow::acero::BatchesWithSchema{{drain_batch.value()},
                                                             this->OutputSchema()},
                             exp_batches_double);
        } else {
          ASSERT_OK(this->Probe(thread_id, l_batches.batches[0], status));
          ASSERT_EQ(status.code, OperatorStatusCode::HAS_MORE_OUTPUT);
          auto probe_batch = std::get<std::optional<arrow::ExecBatch>>(status.payload);
          ASSERT_TRUE(probe_batch.has_value());
          ASSERT_EQ(probe_batch.value().length, kMaxPipeBatchSize);

          ASSERT_OK(this->Probe(thread_id, std::nullopt, status));
          ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
          ASSERT_FALSE(
              std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());

          ASSERT_OK(this->Drain(thread_id, status));
          ASSERT_EQ(status.code, OperatorStatusCode::FINISHED);
          auto drain_batch = std::get<std::optional<arrow::ExecBatch>>(status.payload);
          ASSERT_TRUE(drain_batch.has_value());
          ASSERT_EQ(drain_batch.value().length, exp_rows_single * 2 - kMaxPipeBatchSize);
          AssertBatchesEqual(
              arrow::acero::BatchesWithSchema{{probe_batch.value(), drain_batch.value()},
                                              this->OutputSchema()},
              exp_batches_double);
        }
      } else {
        ASSERT_OK(this->Probe(thread_id, l_batches.batches[0], status));
        ASSERT_EQ(status.code, OperatorStatusCode::HAS_MORE_OUTPUT);
        auto probe_batch_first =
            std::get<std::optional<arrow::ExecBatch>>(status.payload);
        ASSERT_TRUE(probe_batch_first.has_value());
        ASSERT_EQ(probe_batch_first.value().length, kMaxPipeBatchSize);

        if (exp_rows_single <= kMaxPipeBatchSize + kMaxPipeBatchSize / 2) {
          ASSERT_OK(this->Probe(thread_id, std::nullopt, status));
          ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
          ASSERT_FALSE(
              std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());

          ASSERT_OK(this->Probe(thread_id, l_batches.batches[0], status));
          ASSERT_EQ(status.code, OperatorStatusCode::HAS_MORE_OUTPUT);
          auto probe_batch_second =
              std::get<std::optional<arrow::ExecBatch>>(status.payload);
          ASSERT_TRUE(probe_batch_second.has_value());
          ASSERT_EQ(probe_batch_second.value().length, kMaxPipeBatchSize);

          ASSERT_OK(this->Probe(thread_id, std::nullopt, status));
          ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
          ASSERT_FALSE(
              std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());

          ASSERT_OK(this->Drain(thread_id, status));
          ASSERT_EQ(status.code, OperatorStatusCode::FINISHED);
          auto drain_batch = std::get<std::optional<arrow::ExecBatch>>(status.payload);
          ASSERT_TRUE(drain_batch.has_value());
          ASSERT_EQ(drain_batch.value().length,
                    exp_rows_single * 2 - kMaxPipeBatchSize * 2);
          AssertBatchesEqual(arrow::acero::BatchesWithSchema{{probe_batch_first.value(),
                                                              probe_batch_second.value(),
                                                              drain_batch.value()},
                                                             this->OutputSchema()},
                             exp_batches_double);
        } else if (exp_rows_single <= kMaxPipeBatchSize * 2) {
          ASSERT_OK(this->Probe(thread_id, std::nullopt, status));
          ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
          ASSERT_FALSE(
              std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());

          ASSERT_OK(this->Probe(thread_id, l_batches.batches[0], status));
          ASSERT_EQ(status.code, OperatorStatusCode::HAS_MORE_OUTPUT);
          auto probe_batch_second =
              std::get<std::optional<arrow::ExecBatch>>(status.payload);
          ASSERT_TRUE(probe_batch_first.has_value());
          ASSERT_EQ(probe_batch_first.value().length, kMaxPipeBatchSize);

          ASSERT_OK(this->Probe(thread_id, std::nullopt, status));
          ASSERT_EQ(status.code, OperatorStatusCode::HAS_MORE_OUTPUT);
          auto probe_batch_third =
              std::get<std::optional<arrow::ExecBatch>>(status.payload);
          ASSERT_TRUE(probe_batch_third.has_value());
          ASSERT_EQ(probe_batch_third.value().length, kMaxPipeBatchSize);

          ASSERT_OK(this->Probe(thread_id, std::nullopt, status));
          ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
          ASSERT_FALSE(
              std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());

          ASSERT_OK(this->Drain(thread_id, status));
          ASSERT_EQ(status.code, OperatorStatusCode::FINISHED);
          auto drain_batch = std::get<std::optional<arrow::ExecBatch>>(status.payload);
          ASSERT_TRUE(drain_batch.has_value());
          ASSERT_EQ(drain_batch.value().length,
                    exp_rows_single * 2 - kMaxPipeBatchSize * 3);
          AssertBatchesEqual(
              arrow::acero::BatchesWithSchema{
                  {probe_batch_first.value(), probe_batch_second.value(),
                   probe_batch_third.value(), drain_batch.value()},
                  this->OutputSchema()},
              exp_batches_double);
        } else {
          ASSERT_OK(this->Probe(thread_id, std::nullopt, status));
          ASSERT_EQ(status.code, OperatorStatusCode::HAS_MORE_OUTPUT);
          auto probe_batch_second =
              std::get<std::optional<arrow::ExecBatch>>(status.payload);
          ASSERT_TRUE(probe_batch_second.has_value());
          ASSERT_EQ(probe_batch_second.value().length, kMaxPipeBatchSize);

          ASSERT_OK(this->Drain(thread_id, status));
          ASSERT_EQ(status.code, OperatorStatusCode::FINISHED);
          auto drain_batch = std::get<std::optional<arrow::ExecBatch>>(status.payload);
          ASSERT_TRUE(drain_batch.has_value());
          ASSERT_GT(drain_batch.value().length, 0);
          ASSERT_LT(drain_batch.value().length, kMaxPipeBatchSize);
        }
      }
    }
  }
};

using SerialFineProbeInner = SerialFineProbeInnerLeft<arrow::acero::JoinType::INNER>;
using SerialFineProbeLeftOuter =
    SerialFineProbeInnerLeft<arrow::acero::JoinType::LEFT_OUTER>;
using SerialFineProbeLeftSemi =
    SerialFineProbeInnerLeft<arrow::acero::JoinType::LEFT_SEMI>;
using SerialFineProbeLeftAnti =
    SerialFineProbeInnerLeft<arrow::acero::JoinType::LEFT_ANTI>;

TEST_P(SerialFineProbeInner, ProbeOne) { ProbeOne(); }
TEST_P(SerialFineProbeInner, ProbeTwo) { ProbeTwo(); }
INSTANTIATE_TEST_SUITE_P(
    SerialFineProbeInnerCases, SerialFineProbeInner,
    ::testing::ValuesIn(SerialProbeCases<SerialFineProbeInner::JoinType>()),
    [](const auto& param_info) { return param_info.param.Name(param_info.index); });

TEST_P(SerialFineProbeLeftOuter, ProbeOne) { ProbeOne(); }
TEST_P(SerialFineProbeLeftOuter, ProbeTwo) { ProbeTwo(); }
INSTANTIATE_TEST_SUITE_P(
    SerialFineProbeLeftOuterCases, SerialFineProbeLeftOuter,
    ::testing::ValuesIn(SerialProbeCases<SerialFineProbeLeftOuter::JoinType>()),
    [](const auto& param_info) { return param_info.param.Name(param_info.index); });

TEST_P(SerialFineProbeLeftSemi, ProbeOne) { ProbeOne(); }
TEST_P(SerialFineProbeLeftSemi, ProbeTwo) { ProbeTwo(); }
INSTANTIATE_TEST_SUITE_P(
    SerialFineProbeLeftSemiCases, SerialFineProbeLeftSemi,
    ::testing::ValuesIn(SerialProbeCases<SerialFineProbeLeftSemi::JoinType>()),
    [](const auto& param_info) { return param_info.param.Name(param_info.index); });

TEST_P(SerialFineProbeLeftAnti, ProbeOne) { ProbeOne(); }
TEST_P(SerialFineProbeLeftAnti, ProbeTwo) { ProbeTwo(); }
INSTANTIATE_TEST_SUITE_P(
    SerialFineProbeLeftAntiCases, SerialFineProbeLeftAnti,
    ::testing::ValuesIn(SerialProbeCases<SerialFineProbeLeftAnti::JoinType>()),
    [](const auto& param_info) { return param_info.param.Name(param_info.index); });

template <arrow::acero::JoinType join_type>
class SerialFineProbeRightFullOuter : public SerialProbe<join_type> {
 public:
  static const arrow::acero::JoinType JoinType = join_type;

 protected:
  void ProbeOneScan() {
    this->Init();

    auto l_batches =
        HashJoinFixture::LeftBatches(this->join_case_.left_multiplicity_intra_,
                                     this->join_case_.left_multiplicity_inter_);
    auto r_batches =
        HashJoinFixture::RightBatches(this->join_case_.right_multiplicity_intra_,
                                      this->join_case_.right_multiplicity_inter_);

    auto exp_rows_probe =
        HashJoinFixture::ExpRowCount(join_type == arrow::acero::JoinType::RIGHT_OUTER
                                         ? arrow::acero::JoinType::INNER
                                         : arrow::acero::JoinType::LEFT_OUTER,
                                     this->join_case_.left_multiplicity_intra_ *
                                         this->join_case_.left_multiplicity_inter_,
                                     this->join_case_.right_multiplicity_intra_ *
                                         this->join_case_.right_multiplicity_inter_);
    auto exp_batches =
        HashJoinFixture::ExpBatches(this->join_case_.options_.join_type,
                                    this->join_case_.left_multiplicity_intra_ *
                                        this->join_case_.left_multiplicity_inter_,
                                    this->join_case_.right_multiplicity_intra_ *
                                        this->join_case_.right_multiplicity_inter_);

    this->Build(r_batches);

    for (size_t thread_id = 0; thread_id < this->join_case_.dop_; thread_id++) {
      OperatorStatus status = OperatorStatus::Other(arrow::Status::UnknownError(""));
      std::vector<arrow::ExecBatch> output_batches;
      if (exp_rows_probe <= kMaxPipeBatchSize) {
        ASSERT_OK(this->Probe(thread_id, l_batches.batches[0], status));
        ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
        ASSERT_FALSE(
            std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());

        ASSERT_OK(this->Drain(thread_id, status));
        ASSERT_EQ(status.code, OperatorStatusCode::FINISHED);
        ASSERT_FALSE(
            std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
      } else if (exp_rows_probe <= 2 * kMaxPipeBatchSize) {
        ASSERT_OK(this->Probe(thread_id, l_batches.batches[0], status));
        ASSERT_EQ(status.code, OperatorStatusCode::HAS_MORE_OUTPUT);
        auto probe_batch = std::get<std::optional<arrow::ExecBatch>>(status.payload);
        ASSERT_TRUE(probe_batch.has_value());
        ASSERT_EQ(probe_batch.value().length, kMaxPipeBatchSize);
        output_batches.push_back(std::move(probe_batch.value()));

        ASSERT_OK(this->Probe(thread_id, std::nullopt, status));
        ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
        ASSERT_FALSE(
            std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());

        ASSERT_OK(this->Drain(thread_id, status));
        ASSERT_EQ(status.code, OperatorStatusCode::FINISHED);
        ASSERT_FALSE(
            std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
      } else {
        size_t offset = 0, total_length = l_batches.batches[0].length;
        do {
          size_t slice_length = std::min(total_length - offset, kMaxSourceBatchSize);
          arrow::ExecBatch batch = l_batches.batches[0].Slice(offset, slice_length);

          ASSERT_OK(this->Probe(thread_id, batch, status));

          if (status.code == OperatorStatusCode::HAS_OUTPUT) {
            ASSERT_FALSE(
                std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
          } else {
            ASSERT_EQ(status.code, OperatorStatusCode::HAS_MORE_OUTPUT);
            auto probe_batch_first =
                std::move(std::get<std::optional<arrow::ExecBatch>>(status.payload));
            ASSERT_TRUE(probe_batch_first.has_value());
            ASSERT_EQ(probe_batch_first.value().length, kMaxPipeBatchSize);
            output_batches.push_back(std::move(probe_batch_first.value()));
          }

          while (status.code == OperatorStatusCode::HAS_MORE_OUTPUT) {
            ASSERT_OK(this->Probe(thread_id, std::nullopt, status));
            if (status.code == OperatorStatusCode::HAS_MORE_OUTPUT) {
              auto probe_batch =
                  std::move(std::get<std::optional<arrow::ExecBatch>>(status.payload));
              ASSERT_TRUE(probe_batch.has_value());
              ASSERT_EQ(probe_batch.value().length, kMaxPipeBatchSize);
              output_batches.push_back(std::move(probe_batch.value()));
            }
          }

          offset += slice_length;
        } while (offset < total_length);

        ASSERT_OK(this->Drain(thread_id, status));
        ASSERT_EQ(status.code, OperatorStatusCode::FINISHED);
        ASSERT_FALSE(
            std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
      }

      this->StartScan();

      for (size_t thread_id = 0; thread_id < this->join_case_.dop_; thread_id++) {
        do {
          ASSERT_OK(this->Scan(thread_id, status));
          if (status.code == OperatorStatusCode::HAS_MORE_OUTPUT) {
            auto scan_batch =
                std::move(std::get<std::optional<arrow::ExecBatch>>(status.payload));
            ASSERT_TRUE(scan_batch.has_value());
            output_batches.push_back(std::move(scan_batch.value()));
          } else if (status.code == OperatorStatusCode::FINISHED) {
            auto scan_batch =
                std::move(std::get<std::optional<arrow::ExecBatch>>(status.payload));
            if (scan_batch.has_value()) {
              output_batches.push_back(std::move(scan_batch.value()));
            }
          } else {
            ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
            ASSERT_FALSE(
                std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
          }
        } while (status.code != OperatorStatusCode::FINISHED);
      }

      AssertBatchesEqual(
          arrow::acero::BatchesWithSchema{output_batches, this->OutputSchema()},
          exp_batches);
    }
  }
};

using SerialFineProbeRightOuter =
    SerialFineProbeRightFullOuter<arrow::acero::JoinType::RIGHT_OUTER>;
using SerialFineProbeFullOuter =
    SerialFineProbeRightFullOuter<arrow::acero::JoinType::FULL_OUTER>;

TEST_P(SerialFineProbeRightOuter, ProbeOneScan) { ProbeOneScan(); }
INSTANTIATE_TEST_SUITE_P(
    SerialFineProbeRightOuterCases, SerialFineProbeRightOuter,
    ::testing::ValuesIn(SerialProbeCases<SerialFineProbeRightOuter::JoinType>()),
    [](const auto& param_info) { return param_info.param.Name(param_info.index); });

TEST_P(SerialFineProbeFullOuter, ProbeOneScan) { ProbeOneScan(); }
INSTANTIATE_TEST_SUITE_P(
    SerialFineProbeFullOuterCases, SerialFineProbeFullOuter,
    ::testing::ValuesIn(SerialProbeCases<SerialFineProbeFullOuter::JoinType>()),
    [](const auto& param_info) { return param_info.param.Name(param_info.index); });

template <arrow::acero::JoinType join_type>
class SerialFineProbeRightSemiAnti : public SerialProbe<join_type> {
 public:
  static const arrow::acero::JoinType JoinType = join_type;

 protected:
  void ProbeOneScan() {
    this->Init();

    auto l_batches =
        HashJoinFixture::LeftBatches(this->join_case_.left_multiplicity_intra_,
                                     this->join_case_.left_multiplicity_inter_);
    auto r_batches =
        HashJoinFixture::RightBatches(this->join_case_.right_multiplicity_intra_,
                                      this->join_case_.right_multiplicity_inter_);
    auto exp_batches =
        HashJoinFixture::ExpBatches(this->join_case_.options_.join_type,
                                    this->join_case_.left_multiplicity_intra_ *
                                        this->join_case_.left_multiplicity_inter_,
                                    this->join_case_.right_multiplicity_intra_ *
                                        this->join_case_.right_multiplicity_inter_);

    this->Build(r_batches);

    for (size_t thread_id = 0; thread_id < this->join_case_.dop_; thread_id++) {
      OperatorStatus status = OperatorStatus::Other(arrow::Status::UnknownError(""));

      ASSERT_OK(this->Probe(thread_id, l_batches.batches[0], status));
      ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
      ASSERT_FALSE(std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());

      ASSERT_OK(this->Drain(thread_id, status));
      ASSERT_EQ(status.code, OperatorStatusCode::FINISHED);
      ASSERT_FALSE(std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());

      this->StartScan();

      std::vector<arrow::ExecBatch> output_batches;
      for (size_t thread_id = 0; thread_id < this->join_case_.dop_; thread_id++) {
        do {
          ASSERT_OK(this->Scan(thread_id, status));
          if (status.code == OperatorStatusCode::HAS_MORE_OUTPUT) {
            auto scan_batch =
                std::move(std::get<std::optional<arrow::ExecBatch>>(status.payload));
            ASSERT_TRUE(scan_batch.has_value());
            output_batches.push_back(std::move(scan_batch.value()));
          } else if (status.code == OperatorStatusCode::FINISHED) {
            auto scan_batch =
                std::move(std::get<std::optional<arrow::ExecBatch>>(status.payload));
            if (scan_batch.has_value()) {
              output_batches.push_back(std::move(scan_batch.value()));
            }
          } else {
            ASSERT_EQ(status.code, OperatorStatusCode::HAS_OUTPUT);
            ASSERT_FALSE(
                std::get<std::optional<arrow::ExecBatch>>(status.payload).has_value());
          }
        } while (status.code != OperatorStatusCode::FINISHED);
      }

      AssertBatchesEqual(
          arrow::acero::BatchesWithSchema{output_batches, this->OutputSchema()},
          exp_batches);
    }
  }
};

using SerialFineProbeRightSemi =
    SerialFineProbeRightSemiAnti<arrow::acero::JoinType::RIGHT_SEMI>;
using SerialFineProbeRightAnti =
    SerialFineProbeRightSemiAnti<arrow::acero::JoinType::RIGHT_ANTI>;

TEST_P(SerialFineProbeRightSemi, ProbeOneScan) { ProbeOneScan(); }
INSTANTIATE_TEST_SUITE_P(
    SerialFineProbeRightSemiCases, SerialFineProbeRightSemi,
    ::testing::ValuesIn(SerialProbeCases<SerialFineProbeRightSemi::JoinType>()),
    [](const auto& param_info) { return param_info.param.Name(param_info.index); });

TEST_P(SerialFineProbeRightAnti, ProbeOneScan) { ProbeOneScan(); }
INSTANTIATE_TEST_SUITE_P(
    SerialFineProbeRightAntiCases, SerialFineProbeRightAnti,
    ::testing::ValuesIn(SerialProbeCases<SerialFineProbeRightAnti::JoinType>()),
    [](const auto& param_info) { return param_info.param.Name(param_info.index); });

class ParallelDriver : public TestHashJoin,
                       public testing::WithParamInterface<HashJoinCase> {
 protected:
  void Init() { TestHashJoin::Init(std::move(GetParam())); }

  template <typename Scheduler>
  void RunOneJoin(Scheduler* scheduler) {
    auto l_batches = HashJoinFixture::LeftBatches(join_case_.left_multiplicity_intra_,
                                                  join_case_.left_multiplicity_inter_);
    auto r_batches = HashJoinFixture::RightBatches(join_case_.right_multiplicity_intra_,
                                                   join_case_.right_multiplicity_inter_);
    auto exp_rows = HashJoinFixture::ExpRowCount(
        join_case_.options_.join_type,
        join_case_.left_multiplicity_intra_ * join_case_.left_multiplicity_inter_,
        join_case_.right_multiplicity_intra_ * join_case_.right_multiplicity_inter_);
    auto exp_batches = HashJoinFixture::ExpBatches(
        join_case_.options_.join_type,
        join_case_.left_multiplicity_intra_ * join_case_.left_multiplicity_inter_,
        join_case_.right_multiplicity_intra_ * join_case_.right_multiplicity_inter_);

    MemorySource build_source(join_case_.dop_, std::move(r_batches.batches));
    MemorySource probe_source(join_case_.dop_, std::move(l_batches.batches));
    HashJoin hash_join;
    ASSERT_OK(hash_join.Init(query_ctx_.get(), join_case_.dop_, join_case_.options_,
                             *HashJoinFixture::LeftSchema(),
                             *HashJoinFixture::RightSchema()));
    arrow::acero::BatchesWithSchema out_batches{{}, hash_join.OutputSchema()};
    MemorySink probe_sink(out_batches);

    auto build_sink = hash_join.BuildSink();

    Driver<Scheduler> driver({{&build_source, {}, build_sink.get()},
                              {&probe_source, {&hash_join}, &probe_sink}},
                             scheduler);

    driver.Run(join_case_.dop_);

    if (exp_rows < 1024 * 1024) {
      AssertBatchesEqual(out_batches, exp_batches);
    } else {
      AssertBatchesLightEqual(out_batches, exp_batches);
    }
  }
};

TEST_P(ParallelDriver, OneJoinFollyFutureScheduler) {
  Init();
  FollyFutureScheduler scheduler(8);
  RunOneJoin(&scheduler);
}

std::vector<HashJoinCase> ParallelDriverCases() {
  std::vector<HashJoinCase> cases;
  const std::vector<std::tuple<size_t, size_t, size_t, size_t>> small_cases{
      {0, 0, 0, 0},    {1, 0, 0, 0},    {0, 1, 0, 0},    {1, 1, 0, 0},    {0, 0, 1, 0},
      {1, 0, 1, 0},    {0, 1, 1, 0},    {1, 1, 1, 0},    {0, 0, 0, 1},    {1, 0, 0, 1},
      {0, 1, 0, 1},    {1, 1, 0, 1},    {0, 0, 1, 1},    {1, 0, 1, 1},    {0, 1, 1, 1},
      {1, 1, 1, 1},    {31, 1, 31, 1},  {32, 1, 31, 1},  {1, 31, 31, 1},  {1, 32, 31, 1},
      {31, 1, 32, 1},  {32, 1, 32, 1},  {1, 31, 32, 1},  {1, 32, 32, 1},  {31, 1, 1, 31},
      {32, 1, 1, 31},  {1, 31, 1, 31},  {1, 32, 1, 31},  {31, 1, 1, 31},  {32, 1, 1, 31},
      {1, 31, 1, 31},  {1, 32, 1, 31},  {2047, 1, 1, 1}, {2048, 1, 1, 1}, {1, 2047, 1, 1},
      {1, 2048, 1, 1}, {2047, 1, 1, 2}, {2048, 1, 1, 2}, {1, 2047, 1, 2}, {1, 2048, 1, 2},
      {2047, 1, 2, 1}, {2048, 1, 2, 1}, {1, 2047, 2, 1}, {1, 2048, 2, 1}, {2047, 1, 2, 2},
      {2048, 1, 2, 2}, {1, 2047, 2, 2}, {1, 2048, 2, 2}, {8191, 1, 1, 1}, {8192, 1, 1, 1},
      {1, 8191, 1, 1}, {1, 8192, 1, 1}, {1, 1, 8191, 1}, {1, 1, 8192, 1}, {1, 1, 1, 8191},
      {1, 1, 1, 8192}, {8191, 1, 2, 1}, {8192, 1, 2, 1}, {1, 8191, 1, 2}, {1, 8192, 1, 2},
      {2, 1, 8191, 1}, {2, 1, 8192, 1}, {1, 2, 1, 8191}, {1, 2, 1, 8192}};
  const std::vector<std::tuple<size_t, size_t, size_t, size_t>> big_cases{
      {15, 127, 15, 127}, {16, 127, 15, 127}, {15, 128, 15, 127}, {16, 128, 15, 127},
      {15, 127, 16, 127}, {16, 127, 16, 127}, {15, 128, 16, 127}, {16, 128, 16, 127},
      {15, 127, 15, 128}, {16, 127, 15, 128}, {15, 128, 15, 128}, {16, 128, 15, 128},
      {15, 127, 16, 128}, {16, 127, 16, 128}, {15, 128, 16, 128}, {16, 128, 16, 128}};
  for (auto join_type :
       {arrow::acero::JoinType::INNER, arrow::acero::JoinType::LEFT_OUTER,
        arrow::acero::JoinType::RIGHT_OUTER, arrow::acero::JoinType::LEFT_SEMI,
        arrow::acero::JoinType::RIGHT_SEMI, arrow::acero::JoinType::LEFT_ANTI,
        arrow::acero::JoinType::RIGHT_ANTI, arrow::acero::JoinType::FULL_OUTER}) {
    for (auto dop : {1, 4}) {
      for (auto [left_intra, left_inter, right_intra, right_inter] : small_cases) {
        cases.emplace_back(1, join_type, left_intra, left_inter, right_intra,
                           right_inter);
      }
    }
    for (auto dop : {8, 16}) {
      for (auto [left_intra, left_inter, right_intra, right_inter] : small_cases) {
        cases.emplace_back(dop, join_type, left_intra, left_inter, right_intra,
                           right_inter);
      }
      for (auto [left_intra, left_inter, right_intra, right_inter] : big_cases) {
        cases.emplace_back(dop, join_type, left_intra, left_inter, right_intra,
                           right_inter);
      }
    }
  }
  return cases;
}

INSTANTIATE_TEST_SUITE_P(ParallelDriverCases, ParallelDriver,
                         ::testing::ValuesIn(ParallelDriverCases()),
                         [](const auto& param_info) {
                           return param_info.param.Name(param_info.index);
                         });