#pragma once

#include <ara/common/batch.h>
#include <ara/task/backpressure.h>
#include <ara/util/defines.h>

namespace ara::pipeline {

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
  std::variant<task::Backpressure, std::optional<Batch>> payload_;

  explicit OpOutput(Code code, std::optional<Batch> batch = std::nullopt)
      : code_(code), payload_(std::move(batch)) {}

  explicit OpOutput(task::Backpressure backpressure)
      : code_(Code::SINK_BACKPRESSURE), payload_(std::move(backpressure)) {}

 public:
  bool IsSourceNotReady() const { return code_ == Code::SOURCE_NOT_READY; }
  bool IsPipeSinkNeedsMore() const { return code_ == Code::PIPE_SINK_NEEDS_MORE; }
  bool IsPipeEven() const { return code_ == Code::PIPE_EVEN; }
  bool IsSourcePipeHasMore() const { return code_ == Code::SOURCE_PIPE_HAS_MORE; }
  bool IsSinkBackpressure() const { return code_ == Code::SINK_BACKPRESSURE; }
  bool IsPipeYield() const { return code_ == Code::PIPE_YIELD; }
  bool IsFinished() const { return code_ == Code::FINISHED; }
  bool IsCancelled() const { return code_ == Code::CANCELLED; }

  std::optional<Batch>& GetBatch() {
    ARA_CHECK(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return std::get<std::optional<Batch>>(payload_);
  }

  const std::optional<Batch>& GetBatch() const {
    ARA_CHECK(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return std::get<std::optional<Batch>>(payload_);
  }

  task::Backpressure& GetBackpressure() {
    ARA_CHECK(IsSinkBackpressure());
    return std::get<task::Backpressure>(payload_);
  }

  const task::Backpressure& GetBackpressure() const {
    ARA_CHECK(IsSinkBackpressure());
    return std::get<task::Backpressure>(payload_);
  }

  bool operator==(const OpOutput& other) const { return code_ == other.code_; }

  std::string ToString() const {
    switch (code_) {
      case Code::SOURCE_NOT_READY:
        return "SOURCE_NOT_READY";
      case Code::PIPE_SINK_NEEDS_MORE:
        return "PIPE_SINK_NEEDS_MORE";
      case Code::PIPE_EVEN:
        return "PIPE_EVEN";
      case Code::SOURCE_PIPE_HAS_MORE:
        return "SOURCE_PIPE_HAS_MORE";
      case Code::SINK_BACKPRESSURE:
        return "SINK_BACKPRESSURE";
      case Code::PIPE_YIELD:
        return "PIPE_YIELD";
      case Code::FINISHED:
        return "FINISHED";
      case Code::CANCELLED:
        return "CANCELLED";
    }
  }

 public:
  static OpOutput SourceNotReady() { return OpOutput(Code::SOURCE_NOT_READY); }
  static OpOutput PipeSinkNeedsMore() { return OpOutput(Code::PIPE_SINK_NEEDS_MORE); }
  static OpOutput PipeEven(Batch batch) {
    return OpOutput(Code::PIPE_EVEN, std::move(batch));
  }
  static OpOutput SourcePipeHasMore(Batch batch) {
    return OpOutput{Code::SOURCE_PIPE_HAS_MORE, std::move(batch)};
  }
  static OpOutput SinkBackpressure(task::Backpressure backpressure) {
    return OpOutput(std::move(backpressure));
  }
  static OpOutput PipeYield() { return OpOutput{Code::PIPE_YIELD}; }
  static OpOutput Finished(std::optional<Batch> batch = std::nullopt) {
    return OpOutput{Code::FINISHED, std::move(batch)};
  }
  static OpOutput Cancelled() { return OpOutput{Code::CANCELLED}; }
};

}  // namespace ara::pipeline