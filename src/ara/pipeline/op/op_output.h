#pragma once

#include <ara/common/batch.h>
#include <ara/task/backpressure.h>
#include <ara/util/defines.h>

namespace ara::pipeline {

namespace detail {

using ara::task::Backpressure;

template <typename BatchT>
struct OpOutput {
 private:
  enum class Code {
    SOURCE_NOT_READY,
    PIPE_SINK_NEEDS_MORE,
    PIPE_EVEN,
    SOURCE_PIPE_HAS_MORE,
    SINK_BACKPRESSURE,
    PIPE_YIELD,
    FINISHED,
    CANCELLED,
  } code_;
  std::variant<Backpressure, std::optional<BatchT>> payload_;

  explicit OpOutput(Code code, std::optional<BatchT> batch = std::nullopt)
      : code_(code), payload_(std::move(batch)) {}

  explicit OpOutput(Backpressure backpressure)
      : code_(Code::SINK_BACKPRESSURE), payload_(std::move(backpressure)) {}

 public:
  bool IsSourceNotReady() { return code_ == Code::SOURCE_NOT_READY; }
  bool IsPipeSinkNeedsMore() { return code_ == Code::PIPE_SINK_NEEDS_MORE; }
  bool IsPipeEven() { return code_ == Code::PIPE_EVEN; }
  bool IsSourcePipeHasMore() { return code_ == Code::SOURCE_PIPE_HAS_MORE; }
  bool IsSinkBackpressure() { return code_ == Code::SINK_BACKPRESSURE; }
  bool IsPipeYield() { return code_ == Code::PIPE_YIELD; }
  bool IsFinished() { return code_ == Code::FINISHED; }
  bool IsCancelled() { return code_ == Code::CANCELLED; }

  std::optional<Batch>& GetBatch() {
    ARA_DCHECK(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return std::get<std::optional<Batch>>(payload_);
  }

  const std::optional<Batch>& GetBatch() const {
    ARA_DCHECK(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return std::get<std::optional<Batch>>(payload_);
  }

  Backpressure& GetBackpressure() {
    ARA_DCHECK(IsSinkBackpressure());
    return std::get<Backpressure>(payload_);
  }

  const Backpressure& GetBackpressure() const {
    ARA_DCHECK(IsSinkBackpressure());
    return std::get<Backpressure>(payload_);
  }

  bool operator==(const OpOutput& other) const { return code_ == other.code_; }

 public:
  static OpOutput SourceNotReady() { return OpOutput(Code::SOURCE_NOT_READY); }
  static OpOutput PipeSinkNeedsMore() { return OpOutput(Code::PIPE_SINK_NEEDS_MORE); }
  static OpOutput PipeEven(Batch batch) {
    return OpOutput(Code::PIPE_EVEN, std::move(batch));
  }
  static OpOutput SourcePipeHasMore(Batch batch) {
    return OpOutput{Code::SOURCE_PIPE_HAS_MORE, std::move(batch)};
  }
  static OpOutput SinkBackpressure(Backpressure backpressure) {
    return OpOutput(std::move(backpressure));
  }
  static OpOutput PipeYield() { return OpOutput{Code::PIPE_YIELD}; }
  static OpOutput Finished(std::optional<BatchT> batch = std::nullopt) {
    return OpOutput{Code::FINISHED, std::move(batch)};
  }
  static OpOutput Cancelled() { return OpOutput{Code::CANCELLED}; }
};

}  // namespace detail

using OpOutput = detail::OpOutput<ara::Batch>;
using OpResult = arrow::Result<OpOutput>;

}  // namespace ara::pipeline
