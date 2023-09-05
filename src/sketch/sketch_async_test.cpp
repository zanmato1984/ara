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

class Resumable {
 public:
  virtual ~Resumable() = default;
  virtual void Resume() = 0;
};
using ResumablePtr = std::shared_ptr<Resumable>;
using ResumableFactory = std::function<Result<std::shared_ptr<Resumable>>()>;

struct TaskStatus {
 private:
  enum class Code {
    CONTINUE,
    BLOCKING,
    YIELD,
    FINISHED,
    CANCELLED,
  } code_;
  ResumablePtr resumable_ = nullptr;

  explicit TaskStatus(Code code) : code_(code) {}

 public:
  bool IsContinue() const { return code_ == Code::CONTINUE; }
  bool IsBlocking() const { return code_ == Code::BLOCKING; }
  bool IsYield() const { return code_ == Code::YIELD; }
  bool IsFinished() const { return code_ == Code::FINISHED; }
  bool IsCancelled() const { return code_ == Code::CANCELLED; }

  ResumablePtr& GetResumable() {
    ARA_CHECK(IsBlocking());
    return resumable_;
  }

  const ResumablePtr& GetResumable() const {
    ARA_CHECK(IsBlocking());
    return resumable_;
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
  static TaskStatus Blocking(ResumablePtr resumable) {
    auto status = TaskStatus(Code::BLOCKING);
    status.resumable_ = std::move(resumable);
    return status;
  }
  static TaskStatus Yield() { return TaskStatus{Code::YIELD}; }
  static TaskStatus Finished() { return TaskStatus{Code::FINISHED}; }
  static TaskStatus Cancelled() { return TaskStatus{Code::CANCELLED}; }
};
using TaskResult = arrow::Result<TaskStatus>;

using TaskId = size_t;

struct TaskContext {
  ResumableFactory resumable_factory;
};

using ThreadId = size_t;

struct OpOutput {
 private:
  enum class Code {
    SOURCE_NOT_READY,
    PIPE_SINK_NEEDS_MORE,
    PIPE_EVEN,
    SOURCE_PIPE_HAS_MORE,
    PIPE_YIELD,
    PIPE_YIELD_BACK,
    BLOCKING,
    FINISHED,
    CANCELLED,
  } code_;
  std::optional<Batch> batch_;
  ResumablePtr resumable_;

  explicit OpOutput(Code code) : code_(code) {}

 public:
  bool IsSourceNotReady() const { return code_ == Code::SOURCE_NOT_READY; }
  bool IsPipeSinkNeedsMore() const { return code_ == Code::PIPE_SINK_NEEDS_MORE; }
  bool IsPipeEven() const { return code_ == Code::PIPE_EVEN; }
  bool IsSourcePipeHasMore() const { return code_ == Code::SOURCE_PIPE_HAS_MORE; }
  bool IsPipeYield() const { return code_ == Code::PIPE_YIELD; }
  bool IsPipeYieldBack() const { return code_ == Code::PIPE_YIELD_BACK; }
  bool IsBlocking() const { return code_ == Code::BLOCKING; }
  bool IsFinished() const { return code_ == Code::FINISHED; }
  bool IsCancelled() const { return code_ == Code::CANCELLED; }

  std::optional<Batch>& GetBatch() {
    ARA_CHECK(IsPipeEven() || IsSourcePipeHasMore() || IsFinished() || IsBlocking());
    return batch_;
  }

  const std::optional<Batch>& GetBatch() const {
    ARA_CHECK(IsPipeEven() || IsSourcePipeHasMore() || IsFinished() || IsBlocking());
    return batch_;
  }

  ResumablePtr GetResumable() const {
    ARA_CHECK(IsBlocking());
    return resumable_;
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
    output.batch_ = std::optional{std::move(batch)};
    return output;
  }
  static OpOutput SourcePipeHasMore(Batch batch) {
    auto output = OpOutput(Code::SOURCE_PIPE_HAS_MORE);
    output.batch_ = std::optional{std::move(batch)};
    return output;
  }
  static OpOutput PipeYield() { return OpOutput(Code::PIPE_YIELD); }
  static OpOutput PipeYieldBack() { return OpOutput(Code::PIPE_YIELD_BACK); }
  static OpOutput Blocking(ResumablePtr resumable) {
    auto output = OpOutput(Code::BLOCKING);
    output.resumable_ = std::move(resumable);
    return output;
  }
  static OpOutput Finished(std::optional<Batch> batch = std::nullopt) {
    auto output = OpOutput(Code::FINISHED);
    output.batch_ = std::optional{std::move(batch)};
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
      : data_queue_(q_size), resumable_queue_(dop), finished_(false) {}

  std::future<Status> AsyncEventLoop(size_t num_data, std::chrono::milliseconds delay) {
    return std::async(std::launch::async, [this, num_data, delay]() {
      for (size_t i = 0; i < num_data; ++i) {
        std::this_thread::sleep_for(delay);
        ARA_RETURN_NOT_OK(Produce(Batch{}));
      }
      ARA_RETURN_NOT_OK(Produce(std::nullopt));
      return Status::OK();
    });
  }

  Status Produce(std::optional<Batch> batch) {
    std::vector<ResumablePtr> to_resume;
    if (!batch.has_value()) {
      std::lock_guard<std::mutex> lock(mutex_);
      finished_ = true;
      ResumablePtr resumable;
      while (resumable_queue_.read(resumable)) {
        to_resume.push_back(std::move(resumable));
      }
    } else {
      data_queue_.blockingWrite(std::move(batch.value()));
      std::lock_guard<std::mutex> lock(mutex_);
      if (data_queue_.size() > 0 && resumable_queue_.size() > 0) {
        ResumablePtr resumable;
        ARA_CHECK(resumable_queue_.read(resumable));
        to_resume.push_back(std::move(resumable));
      }
    }
    for (auto& resumable : to_resume) {
      resumable->Resume();
    }

    return Status::OK();
  }

  OpResult Consume(const TaskContext& context) {
    Batch batch;
    // Fast path.
    if (data_queue_.read(batch)) {
      return OpOutput::SourcePipeHasMore(std::move(batch));
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (finished_) {
      return OpOutput::Finished();
    } else if (data_queue_.read(batch)) {
      return OpOutput::SourcePipeHasMore(std::move(batch));
    } else {
      ARA_ASSIGN_OR_RAISE(auto resumable, context.resumable_factory());
      ARA_CHECK(resumable_queue_.write(resumable));
      return OpOutput::Blocking(std::move(resumable));
    }
  }

  PipelineSource Source() override {
    return [this](const TaskContext& context, ThreadId) -> OpResult {
      return Consume(context);
    };
  }

 private:
  std::mutex mutex_;
  folly::MPMCQueue<Batch> data_queue_;
  folly::MPMCQueue<ResumablePtr> resumable_queue_;
  bool finished_;
};

}  // namespace ara::sketch
