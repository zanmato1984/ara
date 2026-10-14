#include <ara/common/query_context.h>
#include <ara/pipeline/pipeline_context.h>
#include <ara/pipeline/pipeline_observer.h>
#include <arrow/acero/query_context.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/util.h>
#include <arrow/compute/initialize.h>
#include <arrow/scalar.h>
#include <gtest/gtest.h>
#include "hash_join.h"

namespace {
using ara::pipeline::detail::HashJoin;
using arrow::compute::ExecBatch;
// Frozen baseline copied from the replaced production algorithm. Never linked
// into ara; this oracle checks native behavior, not a second replacement path.
arrow::Result<ExecBatch> Baseline(const arrow::acero::HashJoinProjectionMaps* schema,
                                  arrow::MemoryPool* pool, const ExecBatch& input) {
  ExecBatch projected({}, input.length);
  for (auto kind : {arrow::acero::HashJoinProjection::KEY,
                    arrow::acero::HashJoinProjection::PAYLOAD}) {
    auto map = schema->map(kind, arrow::acero::HashJoinProjection::INPUT);
    for (int i = 0; i < schema->num_cols(kind); ++i) {
      const auto& value = input.values[map.get(i)];
      if (value.is_scalar()) {
        ARROW_ASSIGN_OR_RAISE(
            auto array, arrow::MakeArrayFromScalar(*value.scalar(), input.length, pool));
        projected.values.emplace_back(std::move(array));
      } else
        projected.values.push_back(value);
    }
  }
  return projected;
}
class FailPool : public arrow::MemoryPool {
 public:
  arrow::Status Allocate(int64_t, int64_t, uint8_t**) override {
    return arrow::Status::OutOfMemory("injected pool failure");
  }
  arrow::Status Reallocate(int64_t, int64_t, int64_t, uint8_t**) override {
    return arrow::Status::OutOfMemory("injected pool failure");
  }
  void Free(uint8_t*, int64_t, int64_t) override {}
  int64_t bytes_allocated() const override { return 0; }
  int64_t total_bytes_allocated() const override { return 0; }
  int64_t num_allocations() const override { return 0; }
  std::string backend_name() const override { return "failing-test"; }
};
TEST(BrokenExprProductionProjection, BaselineScalarNullVectorEmptyAndOwnership) {
  ASSERT_TRUE(arrow::compute::Initialize().ok());
  auto fields = arrow::schema(
      {arrow::field("a", arrow::int32()), arrow::field("b", arrow::int32()),
       arrow::field("c", arrow::utf8()), arrow::field("d", arrow::int32())});
  arrow::acero::HashJoinNodeOptions options{
      arrow::acero::JoinType::INNER, {{1}, {0}}, {{0}, {1}}, {{3}, {2}, {1}}, {{2}, {3}}};
  auto query = std::make_unique<arrow::acero::QueryContext>(
      arrow::acero::QueryOptions{}, arrow::compute::ExecContext());
  ASSERT_TRUE(query->Init(nullptr).ok());
  ara::QueryContext context{ara::Options{8, 8, 4}};
  ara::pipeline::PipelineContext pipeline{&context};
  HashJoin join;
  ASSERT_TRUE(join.Init(pipeline, 1, query.get(), options, *fields, *fields).ok());
  for (int64_t length : {0, 1, 3, 17, 4096}) {
    arrow::Int32Builder builder;
    for (int64_t row = 0; row < length; ++row) {
      ASSERT_TRUE((row % 3 == 1 ? builder.AppendNull()
                                : builder.Append(static_cast<int32_t>(row)))
                      .ok());
    }
    auto array = builder.Finish().ValueOrDie();
    ExecBatch input(
        {arrow::Datum(array), arrow::Datum(std::make_shared<arrow::Int32Scalar>()),
         arrow::Datum(std::make_shared<arrow::StringScalar>("hello")),
         arrow::Datum(std::make_shared<arrow::Int32Scalar>(7))},
        length);
    std::vector<arrow::Datum> last_output;
    for (int side : {0, 1}) {
      auto expected = Baseline(join.schema[side], join.pool, input);
      ASSERT_TRUE(expected.ok()) << expected.status();
      auto actual = join.KeyPayloadFromInput(side, &input);  // REAL production method
      ASSERT_TRUE(actual.ok()) << actual.status();
      ASSERT_EQ(actual->length, expected->length);
      ASSERT_EQ(actual->values.size(), expected->values.size());
      for (size_t c = 0; c < actual->values.size(); ++c) {
        ASSERT_TRUE(actual->values[c].Equals(expected->values[c]));
        ASSERT_TRUE(actual->values[c].is_array());
      }
      // Drop temporary wrappers; the output has independent native handles.
      auto retained = actual->values;
      actual = arrow::Status::Invalid("drop");
      expected = arrow::Status::Invalid("drop");
      ASSERT_FALSE(retained.empty());
      for (const auto& value : retained) ASSERT_EQ(value.length(), length);
      last_output = std::move(retained);
    }
    input.values.clear();
    array.reset();
    // All input and oracle handles have now gone. The array key's buffer is
    // still readable through the projection output, not a wrapper-only ticket.
    auto kept = std::static_pointer_cast<arrow::Int32Array>(last_output[0].make_array());
    ASSERT_TRUE(kept->ValidateFull().ok());
    if (length > 0) ASSERT_EQ(kept->Value(0), 0);
    if (length > 1) ASSERT_TRUE(kept->IsNull(1));
  }
  FailPool fail;
  join.pool = &fail;
  ExecBatch input({arrow::Datum(std::make_shared<arrow::Int32Scalar>(1)),
                   arrow::Datum(std::make_shared<arrow::Int32Scalar>(2)),
                   arrow::Datum(std::make_shared<arrow::StringScalar>("x")),
                   arrow::Datum(std::make_shared<arrow::Int32Scalar>(4))},
                  3);
  auto failed = join.KeyPayloadFromInput(0, &input);
  ASSERT_FALSE(failed.ok());
  ASSERT_TRUE(failed.status().IsOutOfMemory()) << failed.status();
  ASSERT_NE(failed.status().ToString().find("injected pool failure"), std::string::npos);
  join.pool = query->memory_pool();
  join.expression_scratch_limit = 0;
  auto denied = join.KeyPayloadFromInput(0, &input);
  ASSERT_FALSE(denied.ok());
  ASSERT_TRUE(denied.status().IsOutOfMemory()) << denied.status();
  ASSERT_NE(denied.status().ToString().find("scratch admission"), std::string::npos);
}
}  // namespace
