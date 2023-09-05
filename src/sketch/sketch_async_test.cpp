#include <arrow/api.h>
#include <arrow/util/logging.h>
#include <folly/MPMCQueue.h>
#include <folly/futures/Future.h>
#include <gtest/gtest.h>

#include <future>

#define ARA_CHECK ARROW_CHECK
#define ARA_CHECK_OK ARROW_CHECK_OK

#define ARA_RETURN_NOT_OK ARROW_RETURN_NOT_OK
#define ARA_ASSIGN_OR_RAISE ARROW_ASSIGN_OR_RAISE

#define ARA_LOG ARROW_LOG

namespace ara::sketch {

using Status = arrow::Status;
template <typename T>
using Result = arrow::Result<T>;

using Batch = std::vector<int>;

using Block = std::any;
using Unblock = std::function<Status(std::optional<Batch>)>;
using BlockPair = std::pair<Block, Unblock>;
using BlockPairFactory = std::function<Result<BlockPair>()>;

struct TaskStatus {
 private:
  enum class Code {
    CONTINUE,
    BLOCKING,
    YIELD,
    FINISHED,
    CANCELLED,
  } code_;
  Block block_;

  explicit TaskStatus(Code code) : code_(code) {}

 public:
  bool IsContinue() const { return code_ == Code::CONTINUE; }
  bool IsBlocking() const { return code_ == Code::BLOCKING; }
  bool IsYield() const { return code_ == Code::YIELD; }
  bool IsFinished() const { return code_ == Code::FINISHED; }
  bool IsCancelled() const { return code_ == Code::CANCELLED; }

  Block& GetBlock() {
    ARA_CHECK(IsBlocking());
    return block_;
  }

  const Block& GetBlock() const {
    ARA_CHECK(IsBlocking());
    return block_;
  }

  bool operator==(const TaskStatus& other) const { return code_ == other.code_; }

  std::string ToString() const {
    switch (code_) {
      case Code::CONTINUE:
        return "CONTINUE";
      case Code::BLOCKING:
        return "BLOCKING";
      case Code::YIELD:
        return "YIELD";
      case Code::FINISHED:
        return "FINISHED";
      case Code::CANCELLED:
        return "CANCELLED";
    }
  }

 public:
  static TaskStatus Continue() { return TaskStatus(Code::CONTINUE); }
  static TaskStatus Blocking(Block block) {
    auto status = TaskStatus(Code::BLOCKING);
    status.block_ = std::move(block);
    return status;
  }
  static TaskStatus Yield() { return TaskStatus{Code::YIELD}; }
  static TaskStatus Finished() { return TaskStatus{Code::FINISHED}; }
  static TaskStatus Cancelled() { return TaskStatus{Code::CANCELLED}; }
};
using TaskResult = arrow::Result<TaskStatus>;

using TaskId = size_t;

struct TaskContext {
  BlockPairFactory block_pair_factory;
};

using ThreadId = size_t;

struct OpOutput {
 private:
  enum class Code {
    SOURCE_NOT_READY,
    PIPE_SINK_NEEDS_MORE,
    PIPE_EVEN,
    SOURCE_PIPE_HAS_MORE,
    BLOCKING,
    PIPE_YIELD,
    PIPE_YIELD_BACK,
    FINISHED,
    CANCELLED,
  } code_;
  std::variant<Block, std::optional<Batch>> payload_;

  explicit OpOutput(Code code) : code_(code) {}

 public:
  bool IsSourceNotReady() const { return code_ == Code::SOURCE_NOT_READY; }
  bool IsPipeSinkNeedsMore() const { return code_ == Code::PIPE_SINK_NEEDS_MORE; }
  bool IsPipeEven() const { return code_ == Code::PIPE_EVEN; }
  bool IsSourcePipeHasMore() const { return code_ == Code::SOURCE_PIPE_HAS_MORE; }
  bool IsBlocking() const { return code_ == Code::BLOCKING; }
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

  Block& GetBlock() {
    ARA_CHECK(IsBlocking());
    return std::get<Block>(payload_);
  }

  const Block& GetBlock() const {
    ARA_CHECK(IsBlocking());
    return std::get<Block>(payload_);
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
      case Code::BLOCKING:
        return "BLOCKING";
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
  static OpOutput SourceNotReady() { return OpOutput(Code::SOURCE_NOT_READY); }
  static OpOutput PipeSinkNeedsMore() { return OpOutput(Code::PIPE_SINK_NEEDS_MORE); }
  static OpOutput PipeEven(Batch batch) {
    auto output = OpOutput(Code::PIPE_EVEN);
    output.payload_ = std::optional{std::move(batch)};
    return output;
  }
  static OpOutput SourcePipeHasMore(Batch batch) {
    auto output = OpOutput(Code::SOURCE_PIPE_HAS_MORE);
    output.payload_ = std::optional{std::move(batch)};
    return output;
  }
  static OpOutput Blocking(Block block) {
    auto output = OpOutput(Code::BLOCKING);
    output.payload_ = std::move(block);
    return output;
  }
  static OpOutput PipeYield() { return OpOutput(Code::PIPE_YIELD); }
  static OpOutput PipeYieldBack() { return OpOutput(Code::PIPE_YIELD_BACK); }
  static OpOutput Finished(std::optional<Batch> batch = std::nullopt) {
    auto output = OpOutput(Code::FINISHED);
    output.payload_ = std::optional{std::move(batch)};
    return output;
  }
  static OpOutput Cancelled() { return OpOutput(Code::CANCELLED); }
};
using OpResult = arrow::Result<OpOutput>;

using PipelineSource = std::function<OpResult(const TaskContext&, ThreadId)>;

class SourceOp {
 public:
  virtual ~SourceOp() = default;
  virtual PipelineSource Source() = 0;
  //   virtual TaskGroups Frontend(const PipelineContext&) = 0;
};

class MockAsyncSource : public SourceOp {
 public:
  MockAsyncSource(size_t q_size, size_t dop)
      : data_queue_(q_size), unblock_queue_(dop), finished_(false) {}

  std::future<Status> AsyncEventLoop(size_t num_data, std::chrono::milliseconds delay) {
    return std::async(std::launch::async, [this, num_data, delay]() {
      for (size_t i = 0; i < num_data; ++i) {
        std::this_thread::sleep_for(delay);
        ARA_RETURN_NOT_OK(Produce());
      }
      finished_ = true;
      return Status::OK();
    });
  }

  Status Produce() {
    Batch batch;
    Unblock unblock;
    if (unblock_queue_.read(unblock)) {
      // Shortcut.
      return unblock(std::move(batch));
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!unblock_queue_.read(unblock)) {
        data_queue_.blockingWrite(std::move(batch));
        return Status::OK();
      }
    }

    return unblock(std::move(batch));
  }

  OpResult Consume(const TaskContext& context) {
    Batch batch;
    Unblock unblock;
    if (data_queue_.read(batch)) {
      // Shortcut.
      return OpOutput::SourcePipeHasMore(std::move(batch));
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!data_queue_.read(batch)) {
        ARA_ASSIGN_OR_RAISE(auto pair, context.block_pair_factory());
        ARA_CHECK(unblock_queue_.write(std::move(pair.second)));
        return OpOutput::Blocking(std::move(pair.first));
      }
    }

    return OpOutput::SourcePipeHasMore(std::move(batch));
  }

  PipelineSource Source() override {
    return [this](const TaskContext& context, ThreadId) -> OpResult {
      if (finished_ && data_queue_.size() == 0) {
        return OpOutput::Finished();
      }
      return Consume(context);
    };
  }

 private:
  std::mutex mutex_;
  folly::MPMCQueue<Batch> data_queue_;
  folly::MPMCQueue<Unblock> unblock_queue_;

  std::atomic_bool finished_;
};

}  // namespace ara::sketch
