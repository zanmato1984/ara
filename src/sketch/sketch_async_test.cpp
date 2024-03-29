#include <arrow/api.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/logging.h>
#include <folly/MPMCQueue.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <gtest/gtest.h>

#include <atomic>
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

using Batch = size_t;

class Resumer {
 public:
  virtual ~Resumer() = default;
  virtual void Resume() = 0;
  virtual bool IsResumed() = 0;
};
using ResumerPtr = std::shared_ptr<Resumer>;
using Resumers = std::vector<ResumerPtr>;
using ResumerFactory = std::function<Result<ResumerPtr>()>;

class Awaiter {
 public:
  virtual ~Awaiter() = default;
};
using AwaiterPtr = std::shared_ptr<Awaiter>;
using SingleAwaiterFactory = std::function<Result<AwaiterPtr>(ResumerPtr&)>;
using AnyAwaiterFactory = std::function<Result<AwaiterPtr>(Resumers&)>;
using AllAwaiterFactory = std::function<Result<AwaiterPtr>(Resumers&)>;

struct TaskStatus {
 private:
  enum class Code {
    CONTINUE,
    BLOCKING,
    YIELD,
    FINISHED,
    CANCELLED,
  } code_;
  AwaiterPtr awaiter_ = nullptr;

  explicit TaskStatus(Code code) : code_(code) {}

 public:
  bool IsContinue() const { return code_ == Code::CONTINUE; }
  bool IsBlocking() const { return code_ == Code::BLOCKING; }
  bool IsYield() const { return code_ == Code::YIELD; }
  bool IsFinished() const { return code_ == Code::FINISHED; }
  bool IsCancelled() const { return code_ == Code::CANCELLED; }

  AwaiterPtr& GetAwaiter() {
    ARA_CHECK(IsBlocking());
    return awaiter_;
  }

  const AwaiterPtr& GetAwaiter() const {
    ARA_CHECK(IsBlocking());
    return awaiter_;
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
  static TaskStatus Blocking(AwaiterPtr awaiter) {
    auto status = TaskStatus(Code::BLOCKING);
    status.awaiter_ = std::move(awaiter);
    return status;
  }
  static TaskStatus Yield() { return TaskStatus{Code::YIELD}; }
  static TaskStatus Finished() { return TaskStatus{Code::FINISHED}; }
  static TaskStatus Cancelled() { return TaskStatus{Code::CANCELLED}; }
};
using TaskResult = arrow::Result<TaskStatus>;

using TaskId = size_t;

struct TaskContext {
  ResumerFactory resumer_factory;
  SingleAwaiterFactory single_awaiter_factory;
  AnyAwaiterFactory any_awaiter_factory;
  AllAwaiterFactory all_awaiter_factory;
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
  std::optional<Batch> batch_ = std::nullopt;
  ResumerPtr resumer_ = nullptr;

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

  ResumerPtr& GetResumer() {
    ARA_CHECK(IsBlocking());
    return resumer_;
  }

  const ResumerPtr& GetResumer() const {
    ARA_CHECK(IsBlocking());
    return resumer_;
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
  static OpOutput Blocking(ResumerPtr resumer) {
    auto output = OpOutput(Code::BLOCKING);
    output.resumer_ = std::move(resumer);
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
    Resumers resumers;
    OpResult op_result;
    for (size_t i = 0; i < channels_.size(); ++i) {
      if (thread_locals_[thread_id].finished[i]) {
        continue;
      }
      if (auto& resumer = thread_locals_[thread_id].resumers[i]; resumer != nullptr) {
        if (resumer->IsResumed()) {
          resumer = nullptr;
        } else {
          resumers.push_back(resumer);
          continue;
        }
      }
      auto& channel = channels_[i];
      ARA_ASSIGN_OR_RAISE(op_result, channel(task_context, thread_id));
      if (op_result->IsFinished()) {
        ARA_CHECK(!op_result->GetBatch().has_value());
        thread_locals_[thread_id].finished[i] = true;
      } else {
        all_finished = false;
      }
      if (op_result->IsBlocking()) {
        thread_locals_[thread_id].resumers[i] = std::move(op_result->GetResumer());
        resumers.push_back(op_result->GetResumer());
      }
      if (!op_result->IsFinished() && !op_result->IsBlocking()) {
        break;
      }
    }
    if (all_finished) {
      return TaskStatus::Finished();
    } else if (resumers.size() == channels_.size()) {
      ARA_ASSIGN_OR_RAISE(auto awaiter, task_context.any_awaiter_factory(resumers));
      return TaskStatus::Blocking(std::move(awaiter));
    } else if (!op_result.ok()) {
      return op_result.status();
    } else if (op_result->IsPipeYield()) {
      return TaskStatus::Yield();
    } else if (op_result->IsFinished()) {
      return TaskStatus::Finished();
    } else if (op_result->IsCancelled()) {
      return TaskStatus::Cancelled();
    }
    return TaskStatus::Continue();
  }

 private:
  size_t dop_;
  std::vector<Channel> channels_;

  struct ThreadLocal {
    ThreadLocal(size_t size) : finished(size, false), resumers(size, nullptr) {}

    std::vector<bool> finished;
    Resumers resumers;
  };
  std::vector<ThreadLocal> thread_locals_;
};

class SyncResumer : public Resumer {
 public:
  using Callback = std::function<void()>;

  void Resume() override {
    std::unique_lock<std::mutex> lock(mutex_);
    ready_ = true;
    for (const auto& cb : callbacks_) {
      cb();
    }
  }

  bool IsResumed() override {
    std::unique_lock<std::mutex> lock(mutex_);
    return ready_;
  }

  void AddCallback(Callback cb) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (ready_) {
      cb();
    } else {
      callbacks_.push_back(std::move(cb));
    }
  }

 private:
  std::mutex mutex_;
  bool ready_ = false;
  std::vector<Callback> callbacks_;
};

TEST(SyncResumerTest, Basic) {
  auto resumer = std::make_shared<SyncResumer>();
  bool cb1_called = false, cb2_called = false, cb3_called = false;
  resumer->AddCallback([&]() { cb1_called = true; });
  resumer->AddCallback([&]() { cb2_called = cb1_called; });
  ASSERT_FALSE(resumer->IsResumed());
  ASSERT_FALSE(cb1_called);
  ASSERT_FALSE(cb2_called);
  resumer->Resume();
  ASSERT_TRUE(resumer->IsResumed());
  ASSERT_TRUE(cb1_called);
  ASSERT_TRUE(cb2_called);
  resumer->AddCallback([&]() { cb3_called = true; });
  ASSERT_TRUE(cb3_called);
}

TEST(SyncResumerTest, Interleaving) {
  auto resumer = std::make_shared<SyncResumer>();
  bool cb1_called = false, cb2_called = false, cb3_called = false;
  resumer->AddCallback([&]() { cb1_called = true; });
  resumer->AddCallback([&]() { cb2_called = cb1_called; });
  ASSERT_FALSE(resumer->IsResumed());
  ASSERT_FALSE(cb1_called);
  ASSERT_FALSE(cb2_called);
  auto resume_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    resumer->Resume();
    resumer->AddCallback([&]() { cb3_called = true; });
    ASSERT_TRUE(cb3_called);
  });
  while (!resumer->IsResumed()) {
  }
  ASSERT_TRUE(cb1_called);
  ASSERT_TRUE(cb2_called);
  resume_future.get();
}

TEST(SyncResumerTest, Interleaving2) {
  auto resumer = std::make_shared<SyncResumer>();
  bool cb1_called = false, cb2_called = false, cb3_called = false;
  resumer->AddCallback([&]() { cb1_called = true; });
  resumer->AddCallback([&]() { cb2_called = cb1_called; });
  ASSERT_FALSE(resumer->IsResumed());
  ASSERT_FALSE(cb1_called);
  ASSERT_FALSE(cb2_called);
  auto resume_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    resumer->Resume();
  });
  while (!resumer->IsResumed()) {
  }
  ASSERT_TRUE(cb1_called);
  ASSERT_TRUE(cb2_called);
  resumer->AddCallback([&]() { cb3_called = true; });
  ASSERT_TRUE(cb3_called);
  resume_future.get();
}

class SyncAwaiter : public Awaiter, public std::enable_shared_from_this<SyncAwaiter> {
 public:
  explicit SyncAwaiter(size_t num_readies) : num_readies_(num_readies), readies_(0) {}

  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (readies_ < num_readies_) {
      cv_.wait(lock);
    }
  }

 private:
  static std::shared_ptr<SyncAwaiter> MakeSyncAwaiter(size_t num_readies,
                                                      Resumers& resumers) {
    auto awaiter = std::make_shared<SyncAwaiter>(num_readies);
    for (auto& resumer : resumers) {
      auto casted = std::dynamic_pointer_cast<SyncResumer>(resumer);
      ARA_CHECK(casted != nullptr);
      casted->AddCallback([awaiter = awaiter->shared_from_this()]() {
        std::unique_lock<std::mutex> lock(awaiter->mutex_);
        awaiter->readies_++;
        awaiter->cv_.notify_one();
      });
    }
    return awaiter;
  }

 public:
  static std::shared_ptr<SyncAwaiter> MakeSyncSingleAwaiter(ResumerPtr& resumer) {
    Resumers resumers{resumer};
    return MakeSyncAwaiter(1, resumers);
  }

  static std::shared_ptr<SyncAwaiter> MakeSyncAnyAwaiter(Resumers& resumers) {
    return MakeSyncAwaiter(1, resumers);
  }

  static std::shared_ptr<SyncAwaiter> MakeSyncAllAwaiter(Resumers& resumers) {
    return MakeSyncAwaiter(resumers.size(), resumers);
  }

 private:
  size_t num_readies_;
  std::mutex mutex_;
  std::condition_variable cv_;
  size_t readies_;
};

TEST(SyncAwaiterTest, SingleWaitFirst) {
  ResumerPtr resumer = std::make_shared<SyncResumer>();
  auto awaiter = SyncAwaiter::MakeSyncSingleAwaiter(resumer);

  bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    finished = true;
    resumer->Resume();
  });
  awaiter->Wait();
  ASSERT_TRUE(finished);
  ASSERT_TRUE(resumer->IsResumed());
  future.get();
}

TEST(SyncAwaiterTest, SingleResumeFirst) {
  ResumerPtr resumer = std::make_shared<SyncResumer>();
  auto awaiter = SyncAwaiter::MakeSyncSingleAwaiter(resumer);

  resumer->Resume();
  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    awaiter->Wait();
    return true;
  });
  ASSERT_TRUE(future.get());
}

TEST(SyncAwaiterTest, Race) {
  size_t rounds = 100000;
  for (size_t i = 0; i < rounds; ++i) {
    ResumerPtr resumer = std::make_shared<SyncResumer>();
    auto awaiter = SyncAwaiter::MakeSyncSingleAwaiter(resumer);

    std::atomic_bool resumer_ready = false, awaiter_ready = false, kickoff = false;
    auto resume_future = std::async(std::launch::async, [&]() {
      resumer_ready = true;
      while (!kickoff) {
      }
      resumer->Resume();
    });
    auto await_future = std::async(std::launch::async, [&]() {
      awaiter_ready = true;
      while (!kickoff) {
      }
      awaiter->Wait();
      return true;
    });
    while (!resumer_ready || !awaiter_ready) {
    }
    kickoff = true;
    resume_future.get();
    ASSERT_TRUE(resumer->IsResumed());
    ASSERT_TRUE(await_future.get());
  }
}

TEST(SyncAwaiterTest, AnyWaitFirst) {
  size_t num_resumers = 1000;
  size_t lucky = 42;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<SyncResumer>();
  }
  auto awaiter = SyncAwaiter::MakeSyncAnyAwaiter(resumers);

  bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    finished = true;
    resumers[lucky]->Resume();
  });
  awaiter->Wait();
  ASSERT_TRUE(finished);
  for (size_t i = 0; i < 1000; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }
  future.get();
}

TEST(SyncAwaiterTest, AnyResumeFirst) {
  size_t num_resumers = 1000;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<SyncResumer>();
    resumer->Resume();
  }
  auto awaiter = SyncAwaiter::MakeSyncAnyAwaiter(resumers);

  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    awaiter->Wait();
    return true;
  });
  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
  ASSERT_TRUE(future.get());
}

TEST(SyncAwaiterTest, LifeSpan) {
  size_t num_resumers = 1000;
  size_t lucky = 42;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<SyncResumer>();
  }
  auto awaiter = SyncAwaiter::MakeSyncAnyAwaiter(resumers);

  bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    finished = true;
    resumers[lucky]->Resume();
  });
  awaiter->Wait();
  awaiter.reset();
  ASSERT_TRUE(finished);
  for (size_t i = 0; i < 1000; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
      resumers[i]->Resume();
    }
  }
  future.get();
}

TEST(SyncAwaiterTest, AllWaitFirst) {
  size_t num_resumers = 1000;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<SyncResumer>();
  }
  auto awaiter = SyncAwaiter::MakeSyncAllAwaiter(resumers);

  std::atomic<size_t> counter = 0;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    for (auto& resumer : resumers) {
      counter++;
      resumer->Resume();
    }
  });
  awaiter->Wait();
  ASSERT_EQ(counter.load(), num_resumers);
  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
  future.get();
}

TEST(SyncAwaiterTest, AllResumeFirst) {
  size_t num_resumers = 1000;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<SyncResumer>();
    resumer->Resume();
  }
  auto awaiter = SyncAwaiter::MakeSyncAllAwaiter(resumers);

  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    awaiter->Wait();
    return true;
  });
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_TRUE(resumers[i]->IsResumed());
  }
  ASSERT_TRUE(future.get());
}

class AsyncResumer : public Resumer {
 private:
  using Promise = folly::Promise<folly::Unit>;
  using Future = folly::SemiFuture<folly::Unit>;

 public:
  AsyncResumer() {
    auto [p, f] = folly::makePromiseContract<folly::Unit>();
    promise_ = std::move(p);
    future_ = std::move(f);
  }

  void Resume() override { promise_.setValue(); }

  bool IsResumed() override { return promise_.isFulfilled(); }

  Future& GetFuture() { return future_; }

 private:
  Promise promise_;
  Future future_;
};

TEST(AsyncResumerTest, Basic) {
  auto resumer = std::make_shared<AsyncResumer>();
  ASSERT_FALSE(resumer->IsResumed());
  resumer->Resume();
  ASSERT_TRUE(resumer->IsResumed());
}

class AsyncAwaiter : public Awaiter {
 protected:
  using Future = folly::SemiFuture<folly::Unit>;

 public:
  virtual Future& GetFuture() = 0;
};

class AsyncSingleAwaiter : public AsyncAwaiter {
 public:
  explicit AsyncSingleAwaiter(ResumerPtr& resumer)
      : resumer_(std::dynamic_pointer_cast<AsyncResumer>(resumer)) {
    ARA_CHECK(resumer_ != nullptr);
  }

  Future& GetFuture() override { return resumer_->GetFuture(); }

 private:
  std::shared_ptr<AsyncResumer> resumer_;
};

class AsyncAnyAwaiter : public AsyncAwaiter {
 public:
  explicit AsyncAnyAwaiter(Resumers& resumers) {
    std::vector<Future> futures;
    for (auto& resumer : resumers) {
      auto casted = std::dynamic_pointer_cast<AsyncResumer>(resumer);
      ARA_CHECK(casted != nullptr);
      futures.push_back(std::move(casted->GetFuture()));
    }
    future_ = folly::collectAny(futures).defer([](auto&&) {});
  }

  Future& GetFuture() override { return future_; }

 private:
  Future future_;
};

class AsyncAllAwaiter : public AsyncAwaiter {
 public:
  explicit AsyncAllAwaiter(Resumers& resumers) {
    std::vector<Future> futures;
    for (auto& resumer : resumers) {
      auto casted = std::dynamic_pointer_cast<AsyncResumer>(resumer);
      ARA_CHECK(casted != nullptr);
      futures.push_back(std::move(casted->GetFuture()));
    }
    future_ = folly::collectAll(futures).defer([](auto&&) {});
  }

  Future& GetFuture() override { return future_; }

 private:
  Future future_;
};

TEST(AsyncAwaiterTest, SingleWaitFirst) {
  ResumerPtr resumer = std::make_shared<AsyncResumer>();
  auto awaiter = std::make_shared<AsyncSingleAwaiter>(resumer);

  bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    finished = true;
    resumer->Resume();
  });
  folly::CPUThreadPoolExecutor executor(4);
  std::move(awaiter->GetFuture()).via(&executor).wait();
  ASSERT_TRUE(finished);
  ASSERT_TRUE(resumer->IsResumed());
  future.get();
}

TEST(AsyncAwaiterTest, SingleResumeFirst) {
  ResumerPtr resumer = std::make_shared<AsyncResumer>();
  auto awaiter = std::make_shared<AsyncSingleAwaiter>(resumer);

  resumer->Resume();
  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    folly::CPUThreadPoolExecutor executor(4);
    std::move(awaiter->GetFuture()).via(&executor).wait();
    return true;
  });
  ASSERT_TRUE(future.get());
}

TEST(AsyncAwaiterTest, Race) {
  size_t rounds = 100000;
  folly::CPUThreadPoolExecutor executor(4);
  for (size_t i = 0; i < rounds; ++i) {
    ResumerPtr resumer = std::make_shared<AsyncResumer>();
    auto awaiter = std::make_shared<AsyncSingleAwaiter>(resumer);

    std::atomic_bool resumer_ready = false, awaiter_ready = false, kickoff = false;
    auto resume_future = std::async(std::launch::async, [&]() {
      resumer_ready = true;
      while (!kickoff) {
      }
      resumer->Resume();
    });
    auto await_future = std::async(std::launch::async, [&]() {
      awaiter_ready = true;
      while (!kickoff) {
      }
      std::move(awaiter->GetFuture()).via(&executor).wait();
      return true;
    });
    while (!resumer_ready || !awaiter_ready) {
    }
    kickoff = true;
    resume_future.get();
    ASSERT_TRUE(resumer->IsResumed());
    ASSERT_TRUE(await_future.get());
  }
}

TEST(AsyncAwaiterTest, AnyWaitFirst) {
  size_t num_resumers = 1000;
  folly::CPUThreadPoolExecutor executor(4);
  size_t lucky = 42;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<AsyncResumer>();
  }
  auto awaiter = std::make_shared<AsyncAnyAwaiter>(resumers);

  bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    finished = true;
    resumers[lucky]->Resume();
  });
  std::move(awaiter->GetFuture()).via(&executor).wait();
  ASSERT_TRUE(finished);
  for (size_t i = 0; i < 1000; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }
  future.get();
}

TEST(AsyncAwaiterTest, AnyResumeFirst) {
  size_t num_resumers = 1000;
  folly::CPUThreadPoolExecutor executor(4);
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<AsyncResumer>();
    resumer->Resume();
  }
  auto awaiter = std::make_shared<AsyncAnyAwaiter>(resumers);

  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::move(awaiter->GetFuture()).via(&executor).wait();
    return true;
  });
  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
  ASSERT_TRUE(future.get());
}

TEST(AsyncAwaiterTest, LifeSpan) {
  size_t num_resumers = 1000;
  folly::CPUThreadPoolExecutor executor(4);
  size_t lucky = 42;
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<AsyncResumer>();
  }
  auto awaiter = std::make_shared<AsyncAnyAwaiter>(resumers);

  bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    finished = true;
    resumers[lucky]->Resume();
  });
  std::move(awaiter->GetFuture()).via(&executor).wait();
  awaiter.reset();
  ASSERT_TRUE(finished);
  for (size_t i = 0; i < 1000; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
      resumers[i]->Resume();
    }
  }
  future.get();
}

TEST(AsyncAwaiterTest, AllWaitFirst) {
  size_t num_resumers = 1000;
  folly::CPUThreadPoolExecutor executor(4);
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<AsyncResumer>();
  }
  auto awaiter = std::make_shared<AsyncAllAwaiter>(resumers);

  std::atomic<size_t> counter = 0;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    for (auto& resumer : resumers) {
      counter++;
      resumer->Resume();
    }
  });
  std::move(awaiter->GetFuture()).via(&executor).wait();
  ASSERT_EQ(counter.load(), num_resumers);
  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
  future.get();
}

TEST(AsyncAwaiterTest, AllResumeFirst) {
  size_t num_resumers = 1000;
  folly::CPUThreadPoolExecutor executor(4);
  Resumers resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<AsyncResumer>();
    resumer->Resume();
  }
  auto awaiter = std::make_shared<AsyncAllAwaiter>(resumers);

  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::move(awaiter->GetFuture()).via(&executor).wait();
    return true;
  });
  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_TRUE(resumers[i]->IsResumed());
  }
  ASSERT_TRUE(future.get());
}

class MockAsyncSource : public SourceOp {
 public:
  MockAsyncSource(size_t q_size, size_t dop)
      : data_queue_(q_size),
        resumer_queue_(dop),
        finished_(false),
        error_(Status::OK()) {}

  std::future<Status> AsyncEventLoop(size_t num_data, std::chrono::milliseconds delay) {
    return std::async(std::launch::async, [this, num_data, delay]() {
      for (size_t i = 0; i < num_data; ++i) {
        std::this_thread::sleep_for(delay);
        ARA_RETURN_NOT_OK(Produce(Batch{}, Status::OK()));
      }
      ARA_RETURN_NOT_OK(Produce(std::nullopt, Status::OK()));
      return Status::OK();
    });
  }

  PipelineSource Source() override {
    return [this](const TaskContext& context, ThreadId) -> OpResult {
      return Consume(context);
    };
  }

  TaskGroups Frontend() override { return {}; }

  std::optional<TaskGroup> Backend() override { return {}; }

 private:
  Status Produce(std::optional<Batch> batch, Status status) {
    // In case that the source subscribes some slot in a background async service, e.g., a
    // process-wide async grpc connection, this method should first unsubscribe this slot.
    // Also make sure if the async service will issue multiple failed Produce() call, in
    // such case, use some lock and flag to avoid re-entrancing the release work.
    std::vector<ResumerPtr> to_resume;
    if (!status.ok()) {
      std::lock_guard<std::mutex> lock(mutex_);
      error_ = std::move(status);
      ResumerPtr resumer;
      while (resumer_queue_.read(resumer)) {
        to_resume.push_back(std::move(resumer));
      }
    } else if (!batch.has_value()) {
      std::lock_guard<std::mutex> lock(mutex_);
      finished_ = true;
      ResumerPtr resumer;
      while (resumer_queue_.read(resumer)) {
        to_resume.push_back(std::move(resumer));
      }
    } else {
      data_queue_.blockingWrite(std::move(batch.value()));
      std::lock_guard<std::mutex> lock(mutex_);
      if (data_queue_.size() > 0 && resumer_queue_.size() > 0) {
        ResumerPtr resumer;
        ARA_CHECK(resumer_queue_.read(resumer));
        to_resume.push_back(std::move(resumer));
      }
    }
    for (auto& resumer : to_resume) {
      resumer->Resume();
    }

    return Status::OK();
  }

  OpResult Consume(const TaskContext& context) {
    Batch batch;
    std::lock_guard<std::mutex> lock(mutex_);
    if (!error_.ok()) {
      return error_;
    } else if (finished_) {
      return OpOutput::Finished();
    } else if (data_queue_.read(batch)) {
      return OpOutput::SourcePipeHasMore(std::move(batch));
    } else {
      ARA_ASSIGN_OR_RAISE(auto resumer, context.resumer_factory());
      ARA_CHECK(resumer_queue_.write(resumer));
      return OpOutput::Blocking(std::move(resumer));
    }
  }

 private:
  std::mutex mutex_;
  folly::MPMCQueue<Batch> data_queue_;
  folly::MPMCQueue<ResumerPtr> resumer_queue_;
  Status error_;
  bool finished_;

  template <typename RunnerType>
  friend class MockAsyncSourceTest;
};

// TODO: Use folly future to implement the async source.

class SyncRunner {
 public:
  TaskContext MakeContext() {
    TaskContext context;
    context.resumer_factory = [&]() -> Result<ResumerPtr> {
      return std::make_shared<SyncResumer>();
    };
    context.single_awaiter_factory = [&](ResumerPtr& resumer) -> Result<AwaiterPtr> {
      return SyncAwaiter::MakeSyncSingleAwaiter(resumer);
    };
    context.any_awaiter_factory = [&](Resumers& resumers) -> Result<AwaiterPtr> {
      return SyncAwaiter::MakeSyncAnyAwaiter(resumers);
    };
    context.all_awaiter_factory = [&](Resumers& resumers) -> Result<AwaiterPtr> {
      return SyncAwaiter::MakeSyncAllAwaiter(resumers);
    };
    return context;
  }

  OpResult WaitThenDo(AwaiterPtr& awaiter, std::function<OpResult()> f) {
    auto sync_awaiter = std::dynamic_pointer_cast<SyncAwaiter>(awaiter);
    ARA_CHECK(sync_awaiter != nullptr);
    sync_awaiter->Wait();
    return f();
  }
};

class AsyncRunner {
 public:
  TaskContext MakeContext() {
    TaskContext context;
    context.resumer_factory = [&]() -> Result<ResumerPtr> {
      return std::make_shared<AsyncResumer>();
    };
    context.single_awaiter_factory = [&](ResumerPtr& resumer) -> Result<AwaiterPtr> {
      return std::make_shared<AsyncSingleAwaiter>(resumer);
    };
    context.any_awaiter_factory = [&](Resumers& resumers) -> Result<AwaiterPtr> {
      return std::make_shared<AsyncAnyAwaiter>(resumers);
    };
    context.all_awaiter_factory = [&](Resumers& resumers) -> Result<AwaiterPtr> {
      return std::make_shared<AsyncAllAwaiter>(resumers);
    };
    return context;
  }

  OpResult WaitThenDo(AwaiterPtr& awaiter, std::function<OpResult()> f) {
    auto async_awaiter = std::dynamic_pointer_cast<AsyncAwaiter>(awaiter);
    ARA_CHECK(async_awaiter != nullptr);
    return std::move(async_awaiter->GetFuture())
        .via(&executor)
        .thenValue([&](auto&&) { return f(); })
        .wait()
        .value();
  }

 private:
  folly::CPUThreadPoolExecutor executor{4};
};

template <typename RunnerType>
class MockAsyncSourceTest : public ::testing::Test {
 protected:
  void Init(size_t q_size, size_t dop) {
    context = runner.MakeContext();
    source = std::make_unique<MockAsyncSource>(q_size, dop);
  }

  void Produce(std::optional<Batch> batch, Status status = Status::OK()) {
    ASSERT_OK(source->Produce(std::move(batch), std::move(status)));
  }

  OpResult Consume(const TaskContext& context, size_t thread_id) {
    return source->Source()(context, thread_id);
  }

 protected:
  RunnerType runner;
  TaskContext context;
  std::unique_ptr<MockAsyncSource> source;
};

using RunnerTypes = ::testing::Types<SyncRunner, AsyncRunner>;
TYPED_TEST_SUITE(MockAsyncSourceTest, RunnerTypes);

TYPED_TEST(MockAsyncSourceTest, ProduceAndConsume) {
  size_t q_size = 10, dop = 4;
  this->Init(q_size, dop);

  {
    auto result = this->Consume(this->context, 0);
    ASSERT_TRUE(result->IsBlocking());
    ASSERT_FALSE(result->GetResumer()->IsResumed());
    this->Produce(Batch{0});
    ASSERT_TRUE(result->GetResumer()->IsResumed());
  }

  {
    for (size_t i = 0; i < q_size - 1; ++i) {
      this->Produce(Batch{i + 1});
    }
    std::atomic_bool produced = false;
    auto future = std::async(std::launch::async, [&]() {
      this->Produce(Batch{q_size});
      produced = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(produced);
    auto result = this->Consume(this->context, 0);
    ASSERT_TRUE(result->IsSourcePipeHasMore());
    ASSERT_EQ(result->GetBatch(), 0);
    future.get();
    ASSERT_TRUE(produced);
    for (size_t i = 0; i < q_size; ++i) {
      auto result = this->Consume(this->context, i % dop);
      ASSERT_TRUE(result->IsSourcePipeHasMore());
      ASSERT_EQ(result->GetBatch(), i + 1);
    }
  }

  {
    std::vector<AwaiterPtr> awaiters;
    for (size_t i = 0; i < dop; ++i) {
      auto result = this->Consume(this->context, i);
      ASSERT_TRUE(result->IsBlocking());
      ASSERT_FALSE(result->GetResumer()->IsResumed());
      auto awaiter = this->context.single_awaiter_factory(result->GetResumer());
      awaiters.push_back(std::move(*awaiter));
    }
    std::vector<std::future<OpResult>> futures;
    for (size_t i = 0; i < dop; ++i) {
      futures.emplace_back(std::async(std::launch::async, [&, i]() {
        return this->runner.WaitThenDo(awaiters[i],
                                       [&]() { return this->Consume(this->context, i); });
      }));
    }
    for (size_t i = 0; i < dop; ++i) {
      this->Produce(Batch{i});
      auto result = futures[i].get();
      ASSERT_TRUE(result->IsSourcePipeHasMore());
      ASSERT_EQ(result->GetBatch(), i);
    }
  }
}

TYPED_TEST(MockAsyncSourceTest, FinishNoOutstanding) {
  size_t q_size = 10, dop = 4;
  this->Init(q_size, dop);

  this->Produce(std::nullopt);

  for (size_t i = 0; i < dop; ++i) {
    auto result = this->Consume(this->context, i);
    ASSERT_TRUE(result->IsFinished());
    ASSERT_FALSE(result->GetBatch().has_value());
  }
}

TYPED_TEST(MockAsyncSourceTest, FinishOneOutstanding) {
  size_t q_size = 10, dop = 4;
  this->Init(q_size, dop);

  AwaiterPtr awaiter;
  {
    auto result = this->Consume(this->context, 0);
    ASSERT_TRUE(result->IsBlocking());
    auto resumer = std::move(result->GetResumer());
    ASSERT_FALSE(resumer->IsResumed());
    awaiter = *(this->context.single_awaiter_factory(resumer));
  }

  auto future = std::async(std::launch::async, [&]() {
    return this->runner.WaitThenDo(awaiter,
                                   [&]() { return this->Consume(this->context, 0); });
  });
  this->Produce(std::nullopt);

  for (size_t i = 1; i < dop; ++i) {
    auto result = this->Consume(this->context, 0);
    ASSERT_TRUE(result->IsFinished());
    ASSERT_FALSE(result->GetBatch().has_value());
  }

  {
    auto result = future.get();
    ASSERT_TRUE(result->IsFinished());
    ASSERT_FALSE(result->GetBatch().has_value());
  }
}

TYPED_TEST(MockAsyncSourceTest, FinishAllOutstanding) {
  size_t q_size = 10, num_batches = 2, dop = 4;
  this->Init(q_size, dop);

  std::vector<std::future<OpResult>> futures;
  std::vector<AwaiterPtr> awaiters;
  for (size_t i = dop; i > 0; --i) {
    auto result = this->Consume(this->context, i - 1);
    ASSERT_TRUE(result->IsBlocking());
    ASSERT_FALSE(result->GetResumer()->IsResumed());
    auto awaiter = this->context.single_awaiter_factory(result->GetResumer());
    awaiters.push_back(std::move(*awaiter));
  }
  for (size_t i = 0; i < dop; ++i) {
    futures.emplace_back(std::async(std::launch::async, [&, i]() {
      return this->runner.WaitThenDo(awaiters[i],
                                     [&]() { return this->Consume(this->context, i); });
    }));
  }
  for (size_t i = 0; i < num_batches; ++i) {
    this->Produce(Batch{i});
    auto result = futures[i].get();
    ASSERT_TRUE(result->IsSourcePipeHasMore());
    ASSERT_EQ(result->GetBatch(), i);
  }

  this->Produce(std::nullopt);

  for (size_t i = num_batches; i < dop; ++i) {
    auto result = futures[i].get();
    ASSERT_TRUE(result->IsFinished());
    ASSERT_FALSE(result->GetBatch().has_value());
  }
}

TYPED_TEST(MockAsyncSourceTest, ErrorNoOutstanding) {
  size_t q_size = 10, dop = 4;
  this->Init(q_size, dop);

  this->Produce(std::nullopt, Status::UnknownError("42"));

  for (size_t i = 0; i < dop; ++i) {
    auto result = this->Consume(this->context, i);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsUnknownError());
    ASSERT_EQ(result.status().message(), "42");
  }
}

TYPED_TEST(MockAsyncSourceTest, ErrorOneOutstanding) {
  size_t q_size = 10, dop = 4;
  this->Init(q_size, dop);

  AwaiterPtr awaiter;
  {
    auto result = this->Consume(this->context, 0);
    ASSERT_TRUE(result->IsBlocking());
    auto resumer = std::move(result->GetResumer());
    ASSERT_FALSE(resumer->IsResumed());
    awaiter = *(this->context.single_awaiter_factory(resumer));
  }

  auto future = std::async(std::launch::async, [&]() {
    return this->runner.WaitThenDo(awaiter,
                                   [&]() { return this->Consume(this->context, 0); });
  });
  this->Produce(std::nullopt, Status::UnknownError("42"));

  for (size_t i = 1; i < dop; ++i) {
    auto result = this->Consume(this->context, 0);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsUnknownError());
    ASSERT_EQ(result.status().message(), "42");
  }

  {
    auto result = future.get();
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsUnknownError());
    ASSERT_EQ(result.status().message(), "42");
  }
}

TYPED_TEST(MockAsyncSourceTest, ErrorAllOutstanding) {
  size_t q_size = 10, num_batches = 2, dop = 4;
  this->Init(q_size, dop);

  std::vector<std::future<OpResult>> futures;
  std::vector<AwaiterPtr> awaiters;
  for (size_t i = dop; i > 0; --i) {
    auto result = this->Consume(this->context, i - 1);
    ASSERT_TRUE(result->IsBlocking());
    ASSERT_FALSE(result->GetResumer()->IsResumed());
    auto awaiter = this->context.single_awaiter_factory(result->GetResumer());
    awaiters.push_back(std::move(*awaiter));
  }
  for (size_t i = 0; i < dop; ++i) {
    futures.emplace_back(std::async(std::launch::async, [&, i]() {
      return this->runner.WaitThenDo(awaiters[i],
                                     [&]() { return this->Consume(this->context, i); });
    }));
  }
  for (size_t i = 0; i < num_batches; ++i) {
    this->Produce(Batch{i});
    auto result = futures[i].get();
    ASSERT_TRUE(result->IsSourcePipeHasMore());
    ASSERT_EQ(result->GetBatch(), i);
  }

  this->Produce(std::nullopt, Status::UnknownError("42"));

  for (size_t i = num_batches; i < dop; ++i) {
    auto result = futures[i].get();
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().IsUnknownError());
    ASSERT_EQ(result.status().message(), "42");
  }
}

}  // namespace ara::sketch
