#pragma once

#include <ara/task/awaiter.h>
#include <ara/util/defines.h>

namespace ara::task {

struct TaskStatus {
 private:
  enum class Code {
    CONTINUE,
    BLOCKED,
    YIELD,
    FINISHED,
    CANCELLED,
  } code_;
  AwaiterPtr awaiter_ = nullptr;

  explicit TaskStatus(Code code) : code_(code) {}

  explicit TaskStatus(AwaiterPtr awaiter)
      : code_(Code::BLOCKED), awaiter_(std::move(awaiter)) {}

 public:
  bool IsContinue() const { return code_ == Code::CONTINUE; }
  bool IsBlocked() const { return code_ == Code::BLOCKED; }
  bool IsYield() const { return code_ == Code::YIELD; }
  bool IsFinished() const { return code_ == Code::FINISHED; }
  bool IsCancelled() const { return code_ == Code::CANCELLED; }

  AwaiterPtr& GetAwaiter() {
    ARA_CHECK(IsBlocked());
    return awaiter_;
  }

  const AwaiterPtr& GetAwaiter() const {
    ARA_CHECK(IsBlocked());
    return awaiter_;
  }

  bool operator==(const TaskStatus& other) const { return code_ == other.code_; }

  std::string ToString() const {
    switch (code_) {
      case Code::CONTINUE:
        return "CONTINUE";
      case Code::BLOCKED:
        return "BLOCKED";
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
  static TaskStatus Blocked(AwaiterPtr awaiter) { return TaskStatus(std::move(awaiter)); }
  static TaskStatus Yield() { return TaskStatus(Code::YIELD); }
  static TaskStatus Finished() { return TaskStatus(Code::FINISHED); }
  static TaskStatus Cancelled() { return TaskStatus(Code::CANCELLED); }
};

}  // namespace ara::task
