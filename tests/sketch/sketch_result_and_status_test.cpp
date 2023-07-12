#include <arrow/compute/exec.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/logging.h>
#include <gtest/gtest.h>

#define ARRA_DCHECK ARROW_DCHECK
#define ARRA_DCHECK_OK ARROW_DCHECK_OK

namespace arra::sketch {

struct OperatorResult {
 private:
  enum class OperatorStatus {
    NEEDS_MORE = 0,
    HAS_MORE = 1,
    FINISHED = 2,
    BACKPRESSURE = 3,
    YIELD = 4,
    ERROR = 5,
    CANCELLED = 6,
  } status_;
  std::optional<arrow::compute::ExecBatch> output_;

  OperatorResult(OperatorStatus status,
                 std::optional<arrow::compute::ExecBatch> output = std::nullopt)
      : status_(status), output_(std::move(output)) {}

 public:
  bool IsNeedsMore() { return status_ == OperatorStatus::NEEDS_MORE; }
  bool IsHasMore() { return status_ == OperatorStatus::HAS_MORE; }
  bool IsFinished() { return status_ == OperatorStatus::FINISHED; }
  bool IsBackpressure() { return status_ == OperatorStatus::BACKPRESSURE; }
  bool IsYield() { return status_ == OperatorStatus::YIELD; }
  bool IsError() { return status_ == OperatorStatus::ERROR; }
  bool IsCancelled() { return status_ == OperatorStatus::CANCELLED; }

  std::optional<arrow::compute::ExecBatch> GetOutput() {
    ARRA_DCHECK(IsHasMore() || IsFinished());
    return output_;
  }

 public:
  static OperatorResult NeedsMore() { return OperatorResult(OperatorStatus::NEEDS_MORE); }
  static OperatorResult HasMore(arrow::compute::ExecBatch output) {
    return OperatorResult{OperatorStatus::HAS_MORE, std::move(output)};
  }
  static OperatorResult Finished(std::optional<arrow::compute::ExecBatch> output) {
    return OperatorResult{OperatorStatus::FINISHED, std::move(output)};
  }
  static OperatorResult Backpressure() {
    return OperatorResult{OperatorStatus::BACKPRESSURE};
  }
  static OperatorResult Yield() { return OperatorResult{OperatorStatus::YIELD}; }
  static OperatorResult Error() { return OperatorResult{OperatorStatus::ERROR}; }
  static OperatorResult CANCELLED() { return OperatorResult{OperatorStatus::CANCELLED}; }
};

}  // namespace arra::sketch

namespace arrow {

::arrow::Status OK() { return ::arrow::Status::OK(); }

::arrow::Status Invalid() { return ::arrow::Status::Invalid(""); }

}  // namespace arrow

using namespace arra::sketch;

TEST(ResultAndStatusTest, EmitBatch) {
  struct Operator {
    arrow::Result<OperatorResult> Run() {
      arrow::compute::ExecBatch batch;
      return OperatorResult::HasMore(std::move(batch));
    }
  } op;
  auto result = op.Run();
  ASSERT_NO_THROW(result.ValueOrDie());
  ASSERT_OK(result);
  ASSERT_TRUE(result.ValueOrDie().IsHasMore());
  ASSERT_TRUE(result.ValueOrDie().GetOutput().has_value());
}

TEST(ResultAndStatusTest, EmitBatchAfterArrowOK) {
  struct Operator {
    arrow::Result<OperatorResult> Run() {
      ARROW_RETURN_NOT_OK(arrow::OK());
      arrow::compute::ExecBatch batch;
      return OperatorResult::HasMore(std::move(batch));
    }
  } op;
  auto result = op.Run();
  ASSERT_NO_THROW(result.ValueOrDie());
  ASSERT_OK(result);
  ASSERT_TRUE(result.ValueOrDie().IsHasMore());
  ASSERT_TRUE(result.ValueOrDie().GetOutput().has_value());
}

TEST(ResultAndStatusTest, EmitErrorBecauseArrowInvalid) {
  struct Operator {
    arrow::Result<OperatorResult> Run() {
      ARROW_RETURN_NOT_OK(arrow::Invalid());
      arrow::compute::ExecBatch batch;
      return OperatorResult::HasMore(std::move(batch));
    }
  } op;
  auto result = op.Run();
  ASSERT_NOT_OK(result);
  ASSERT_TRUE(result.status().IsInvalid());
}

TEST(ResultAndStatusTest, EmitError) {
  struct Operator {
    arrow::Result<OperatorResult> Run() { return arrow::Status::Invalid(""); }
  } op;
  auto result = op.Run();
  ASSERT_NOT_OK(result);
  ASSERT_TRUE(result.status().IsInvalid());
}