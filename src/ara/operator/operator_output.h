#include <ara/common/batch.h>

namespace ara::op {

namespace detail {

template <typename BatchT>
struct OperatorOutput {
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
  std::optional<BatchT> output_;

  OperatorOutput(Code code, std::optional<Batch> output = std::nullopt)
      : code_(code), output_(std::move(output)) {}

 public:
  bool IsSourceNotReady() { return code_ == Code::SOURCE_NOT_READY; }
  bool IsPipeSinkNeedsMore() { return code_ == Code::PIPE_SINK_NEEDS_MORE; }
  bool IsPipeEven() { return code_ == Code::PIPE_EVEN; }
  bool IsSourcePipeHasMore() { return code_ == Code::SOURCE_PIPE_HAS_MORE; }
  bool IsSinkBackpressure() { return code_ == Code::SINK_BACKPRESSURE; }
  bool IsPipeYield() { return code_ == Code::PIPE_YIELD; }
  bool IsFinished() { return code_ == Code::FINISHED; }
  bool IsCancelled() { return code_ == Code::CANCELLED; }

  std::optional<Batch>& GetOutput() {
    ARA_DCHECK(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return output_;
  }

 public:
  static OperatorOutput SourceNotReady() {
    return OperatorOutput(Code::SOURCE_NOT_READY);
  }
  static OperatorOutput PipeSinkNeedsMore() {
    return OperatorOutput(Code::PIPE_SINK_NEEDS_MORE);
  }
  static OperatorOutput PipeEven(Batch output) {
    return OperatorOutput(Code::PIPE_EVEN, std::move(output));
  }
  static OperatorOutput SourcePipeHasMore(Batch output) {
    return OperatorOutput{Code::SOURCE_PIPE_HAS_MORE, std::move(output)};
  }
  static OperatorOutput SinkBackpressure() {
    return OperatorOutput{Code::SINK_BACKPRESSURE};
  }
  static OperatorOutput PipeYield() { return OperatorOutput{Code::PIPE_YIELD}; }
  static OperatorOutput Finished(std::optional<Batch> output = std::nullopt) {
    return OperatorOutput{Code::FINISHED, std::move(output)};
  }
  static OperatorOutput Cancelled() { return OperatorOutput{Code::CANCELLED}; }
};

}  // namespace detail

using OperatorOutput = detail::OperatorOutput<ara::Batch>;

}  // namespace ara::op
