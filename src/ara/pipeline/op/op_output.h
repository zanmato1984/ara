#pragma once

#include <ara/common/batch.h>
#include <ara/task/resumer.h>
#include <ara/util/defines.h>

namespace ara::pipeline {

struct OpOutput {
 private:
  enum class Code {
    PIPE_SINK_NEEDS_MORE,
    PIPE_EVEN,
    SOURCE_PIPE_HAS_MORE,
    BLOCKED,
    PIPE_YIELD,
    PIPE_YIELD_BACK,
    FINISHED,
    CANCELLED,
  } code_;
  std::variant<task::ResumerPtr, std::optional<Batch>> payload_;

  explicit OpOutput(Code code, std::optional<Batch> batch = std::nullopt)
      : code_(code), payload_(std::move(batch)) {}

  explicit OpOutput(task::ResumerPtr resumer)
      : code_(Code::BLOCKED), payload_(std::move(resumer)) {}

 public:
  bool IsPipeSinkNeedsMore() const { return code_ == Code::PIPE_SINK_NEEDS_MORE; }
  bool IsPipeEven() const { return code_ == Code::PIPE_EVEN; }
  bool IsSourcePipeHasMore() const { return code_ == Code::SOURCE_PIPE_HAS_MORE; }
  bool IsBlocked() const { return code_ == Code::BLOCKED; }
  bool IsPipeYield() const { return code_ == Code::PIPE_YIELD; }
  bool IsPipeYieldBack() const { return code_ == Code::PIPE_YIELD_BACK; }
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

  task::ResumerPtr& GetResumer() {
    ARA_CHECK(IsBlocked());
    return std::get<task::ResumerPtr>(payload_);
  }

  const task::ResumerPtr& GetResumer() const {
    ARA_CHECK(IsBlocked());
    return std::get<task::ResumerPtr>(payload_);
  }

  bool operator==(const OpOutput& other) const { return code_ == other.code_; }

  std::string ToString() const {
    switch (code_) {
      case Code::PIPE_SINK_NEEDS_MORE:
        return "PIPE_SINK_NEEDS_MORE";
      case Code::PIPE_EVEN:
        return "PIPE_EVEN";
      case Code::SOURCE_PIPE_HAS_MORE:
        return "SOURCE_PIPE_HAS_MORE";
      case Code::BLOCKED:
        return "BLOCKED";
      case Code::PIPE_YIELD:
        return "PIPE_YIELD";
      case Code::PIPE_YIELD_BACK:
        return "PIPE_YIELD_BACK";
      case Code::FINISHED:
        return "FINISHED";
      case Code::CANCELLED:
        return "CANCELLED";
    }
  }

 public:
  static OpOutput PipeSinkNeedsMore() { return OpOutput(Code::PIPE_SINK_NEEDS_MORE); }
  static OpOutput PipeEven(Batch batch) {
    return OpOutput(Code::PIPE_EVEN, std::move(batch));
  }
  static OpOutput SourcePipeHasMore(Batch batch) {
    return OpOutput(Code::SOURCE_PIPE_HAS_MORE, std::move(batch));
  }
  static OpOutput Blocked(task::ResumerPtr resumer) {
    return OpOutput(std::move(resumer));
  }
  static OpOutput PipeYield() { return OpOutput(Code::PIPE_YIELD); }
  static OpOutput PipeYieldBack() { return OpOutput(Code::PIPE_YIELD_BACK); }
  static OpOutput Finished(std::optional<Batch> batch = std::nullopt) {
    return OpOutput(Code::FINISHED, std::move(batch));
  }
  static OpOutput Cancelled() { return OpOutput(Code::CANCELLED); }
};

}  // namespace ara::pipeline
