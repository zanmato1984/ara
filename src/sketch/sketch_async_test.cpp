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
  using Callback = std::function<void()>;
  virtual ~Resumable() = default;
  virtual void Resume() = 0;
  virtual void AddCallback(Callback) = 0;
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

using Task = std::function<TaskResult(const TaskContext&, TaskId)>;
using TaskCont = std::function<TaskResult(const TaskContext&)>;
using TaskNotifyFinish = std::function<arrow::Status()>;
using TaskGroup =
    std::tuple<Task, size_t, std::optional<TaskCont>, std::optional<TaskNotifyFinish>>;
using TaskGroups = std::vector<TaskGroup>;

using ThreadId = size_t;

struct OpOutput {
 private:
  enum class Code {
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

  ResumablePtr& GetResumable() {
    ARA_CHECK(IsBlocking());
    return resumable_;
  }

  const ResumablePtr& GetResumable() const {
    ARA_CHECK(IsBlocking());
    return resumable_;
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
using PipelinePipe =
    std::function<OpResult(const TaskContext&, ThreadId, std::optional<Batch>)>;
using PipelineDrain = std::function<OpResult(const TaskContext&, ThreadId)>;
using PipelineSink =
    std::function<OpResult(const TaskContext&, ThreadId, std::optional<Batch>)>;

class SourceOp {
 public:
  virtual ~SourceOp() = default;
  virtual PipelineSource Source() = 0;
  virtual TaskGroups Frontend() = 0;
  virtual std::optional<TaskGroup> Backend() = 0;
};

class PipeOp {
 public:
  virtual ~PipeOp() = default;
  virtual PipelinePipe Pipe() = 0;
  virtual std::optional<PipelineDrain> Drain() = 0;
};

class SinkOp {
 public:
  virtual ~SinkOp() = default;
  virtual PipelineSink Sink() = 0;
  virtual TaskGroups Frontend() = 0;
  virtual std::optional<TaskGroup> Backend() = 0;
};

struct Pipeline {
  struct Channel {
    SourceOp* source_op;
    std::vector<PipeOp*> pipe_ops;
    SinkOp* sink_op;
  };

  std::vector<Channel> channels;
};

class PipelineTask {
 public:
  class Channel {
   public:
    Channel(const Pipeline::Channel& channel, size_t dop)
        : dop_(dop),
          source_(channel.source_op->Source()),
          pipes_(channel.pipe_ops.size()),
          sink_(channel.sink_op->Sink()),
          thread_locals_(dop),
          cancelled_(false) {
      const auto& pipe_ops = channel.pipe_ops;
      std::transform(pipe_ops.begin(), pipe_ops.end(), pipes_.begin(),
                     [&](auto* pipe_op) {
                       return std::make_pair(pipe_op->Pipe(), pipe_op->Drain());
                     });
      std::vector<size_t> drains;
      for (size_t i = 0; i < pipes_.size(); ++i) {
        if (pipes_[i].second.has_value()) {
          drains.push_back(i);
        }
      }
      for (size_t i = 0; i < dop; ++i) {
        thread_locals_[i].drains = drains;
      }
    }

    Channel(Channel&& other)
        : dop_(other.dop_),
          source_(std::move(other.source_)),
          pipes_(std::move(other.pipes_)),
          sink_(std::move(other.sink_)),
          thread_locals_(std::move(other.thread_locals_)),
          cancelled_(other.cancelled_.load()) {}

    OpResult operator()(const TaskContext& task_context, ThreadId thread_id) {
      if (cancelled_) {
        return OpOutput::Cancelled();
      }

      if (thread_locals_[thread_id].sinking) {
        thread_locals_[thread_id].sinking = false;
        auto result = Sink(task_context, thread_id, std::nullopt);
      }

      if (!thread_locals_[thread_id].pipe_stack.empty()) {
        auto pipe_id = thread_locals_[thread_id].pipe_stack.top();
        thread_locals_[thread_id].pipe_stack.pop();
        auto result = Pipe(task_context, thread_id, pipe_id, std::nullopt);
        return result;
      }

      if (!thread_locals_[thread_id].source_done) {
        auto result = source_(task_context, thread_id);
        if (!result.ok()) {
          cancelled_ = true;
          return result.status();
        }
        if (result->IsBlocking()) {
          return result;
        } else if (result->IsFinished()) {
          thread_locals_[thread_id].source_done = true;
          if (result->GetBatch().has_value()) {
            auto new_result =
                Pipe(task_context, thread_id, 0, std::move(result->GetBatch()));
            return new_result;
          }
        } else {
          ARA_CHECK(result->IsSourcePipeHasMore());
          ARA_CHECK(result->GetBatch().has_value());
          auto new_result =
              Pipe(task_context, thread_id, 0, std::move(result->GetBatch()));
          return new_result;
        }
      }

      if (thread_locals_[thread_id].draining >= thread_locals_[thread_id].drains.size()) {
        return OpOutput::Finished();
      }

      for (; thread_locals_[thread_id].draining < thread_locals_[thread_id].drains.size();
           ++thread_locals_[thread_id].draining) {
        auto drain_id =
            thread_locals_[thread_id].drains[thread_locals_[thread_id].draining];
        auto result = pipes_[drain_id].second.value()(task_context, thread_id);
        if (!result.ok()) {
          cancelled_ = true;
          return result.status();
        }
        if (thread_locals_[thread_id].yield) {
          ARA_CHECK(result->IsPipeYieldBack());
          thread_locals_[thread_id].yield = false;
          return OpOutput::PipeYieldBack();
        }
        if (result->IsPipeYield()) {
          ARA_CHECK(!thread_locals_[thread_id].yield);
          thread_locals_[thread_id].yield = true;
          return OpOutput::PipeYield();
        }
        if (result->IsBlocking()) {
          return result;
        }
        ARA_CHECK(result->IsSourcePipeHasMore() || result->IsFinished());
        if (result->GetBatch().has_value()) {
          if (result->IsFinished()) {
            ++thread_locals_[thread_id].draining;
          }
          auto new_result =
              Pipe(task_context, thread_id, drain_id + 1, std::move(result->GetBatch()));
          return new_result;
        }
      }

      return OpOutput::Finished();
    }

    OpResult Pipe(const TaskContext& task_context, ThreadId thread_id, size_t pipe_id,
                  std::optional<Batch> input) {
      for (size_t i = pipe_id; i < pipes_.size(); ++i) {
        auto result = pipes_[i].first(task_context, thread_id, std::move(input));
        if (!result.ok()) {
          cancelled_ = true;
          return result.status();
        }
        if (thread_locals_[thread_id].yield) {
          ARA_CHECK(result->IsPipeYieldBack());
          thread_locals_[thread_id].pipe_stack.push(i);
          thread_locals_[thread_id].yield = false;
          return OpOutput::PipeYieldBack();
        }
        if (result->IsPipeYield()) {
          ARA_CHECK(!thread_locals_[thread_id].yield);
          thread_locals_[thread_id].pipe_stack.push(i);
          thread_locals_[thread_id].yield = true;
          return OpOutput::PipeYield();
        }
        if (result->IsBlocking()) {
          thread_locals_[thread_id].pipe_stack.push(i);
          return result;
        }
        ARA_CHECK(result->IsPipeSinkNeedsMore() || result->IsPipeEven() ||
                  result->IsSourcePipeHasMore());
        if (result->IsPipeEven() || result->IsSourcePipeHasMore()) {
          if (result->IsSourcePipeHasMore()) {
            thread_locals_[thread_id].pipe_stack.push(i);
          }
          ARA_CHECK(result->GetBatch().has_value());
          input = std::move(result->GetBatch());
        } else {
          return OpOutput::PipeSinkNeedsMore();
        }
      }

      return Sink(task_context, thread_id, std::move(input));
    }

    OpResult Sink(const TaskContext& task_context, ThreadId thread_id,
                  std::optional<Batch> input) {
      auto result = sink_(task_context, thread_id, std::move(input));
      if (!result.ok()) {
        cancelled_ = true;
        return result.status();
      }
      ARA_CHECK(result->IsPipeSinkNeedsMore() || result->IsBlocking());
      if (result->IsBlocking()) {
        thread_locals_[thread_id].sinking = true;
      }
      return result;
    }

   private:
    size_t dop_;
    PipelineSource source_;
    std::vector<std::pair<PipelinePipe, std::optional<PipelineDrain>>> pipes_;
    PipelineSink sink_;

    struct ThreadLocal {
      bool sinking = false;
      std::stack<size_t> pipe_stack;
      bool source_done = false;
      std::vector<size_t> drains;
      size_t draining = 0;
      bool yield = false;
    };
    std::vector<ThreadLocal> thread_locals_;
    std::atomic_bool cancelled_;
  };

 public:
  PipelineTask(const Pipeline& pipeline, size_t dop) : dop_(dop) {
    for (const auto& channel : pipeline.channels) {
      channels_.emplace_back(channel, dop);
    }
    for (size_t i = 0; i < dop; ++i) {
      thread_locals_.emplace_back(channels_.size());
    }
  }

  TaskResult operator()(const TaskContext& task_context, ThreadId thread_id) {
    bool all_finished = true;
    bool all_blocking = false;
    std::vector<ResumablePtr> resumables;
    for (size_t i = 0; i < channels_.size(); ++i) {
      if (thread_locals_[thread_id].finished[i]) {
        continue;
      } else if (thread_locals_[thread_id].blocking[i]) {
        return TaskStatus::Blocking(thread_locals_[thread_id].resumables[i]);
      }
      auto& channel = channels_[i];
      ARA_ASSIGN_OR_RAISE(auto op_result, channel(task_context, thread_id));
      if (op_result.IsFinished()) {
        ARA_CHECK(!op_result.GetBatch().has_value());
        thread_locals_[thread_id].finished[i] = true;
      } else {
        all_finished = false;
      }
      // if (op_result->IsBlocking()) {

      // }
      // if (!op_result.IsFinished() && !op_result.IsSourceNotReady()) {
      //   return OpResultToTaskResult(std::move(op_result));
      // }
    }
    if (all_finished) {
      return TaskStatus::Finished();
    } else {
      return TaskStatus::Continue();
    }
  }

 private:
  static TaskResult OpResultToTaskResult(OpResult op_result) {
    if (!op_result.ok()) {
      return op_result.status();
    }
    if (op_result->IsBlocking()) {
      return TaskStatus::Blocking(std::move(op_result->GetResumable()));
    }
    if (op_result->IsPipeYield()) {
      return TaskStatus::Yield();
    }
    if (op_result->IsFinished()) {
      return TaskStatus::Finished();
    }
    if (op_result->IsCancelled()) {
      return TaskStatus::Cancelled();
    }
    return TaskStatus::Continue();
  }

 private:
  size_t dop_;
  std::vector<Channel> channels_;

  struct ThreadLocal {
    ThreadLocal(size_t size) : finished(size, false), blocking(size, false) {}

    std::vector<bool> finished;
    std::vector<bool> blocking;
  };
  std::vector<ThreadLocal> thread_locals_;
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

  TaskGroups Frontend() override { return {}; }

  std::optional<TaskGroup> Backend() override { return {}; }

 private:
  std::mutex mutex_;
  folly::MPMCQueue<Batch> data_queue_;
  folly::MPMCQueue<ResumablePtr> resumable_queue_;
  bool finished_;
};

class SyncResumable : public Resumable {
 public:
  virtual void Wait() = 0;
};

class ConcreteSyncResumable : public SyncResumable {
 public:
  void Resume() override {
    std::unique_lock<std::mutex> lock(mutex);
    ready = true;
    cv.notify_one();
    for (const auto& cb : callbacks) {
      cb();
    }
  }

  void AddCallback(std::function<void()> cb) override {
    std::unique_lock<std::mutex> lock(mutex);
    if (ready) {
      cb();
    } else {
      callbacks.push_back(cb);
    }
  }

  void Wait() override {
    std::unique_lock<std::mutex> lock(mutex);
    while (!ready) {
      cv.wait(lock);
    }
  }

 private:
  std::mutex mutex;
  std::condition_variable cv;
  bool ready = false;
  std::vector<std::function<void()>> callbacks;
};

class AnySyncResumable : public SyncResumable {
 public:
  AnySyncResumable(const std::vector<ResumablePtr>& resumables)
      : resumables_(resumables), any_ready_(false) {
    for (auto& resumable : resumables) {
      resumable->AddCallback([this]() {
        std::unique_lock<std::mutex> lock(mutex_);
        any_ready_ = true;
        cv_.notify_one();
      });
    }
  }

  void Resume() override { ARA_CHECK(false); }

  void AddCallback(std::function<void()>) override { ARA_CHECK(false); }

  void Wait() override {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!any_ready_) {
      cv_.wait(lock);
    }
  }

 private:
  std::vector<ResumablePtr> resumables_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool any_ready_;
};

}  // namespace ara::sketch
