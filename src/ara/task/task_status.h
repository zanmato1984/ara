#pragma once

#include <ara/task/backpressure.h>
#include <ara/util/util.h>

namespace ara::task {

struct TaskStatus {
 private:
  enum class Code {
    CONTINUE,
    BACKPRESSURE,
    YIELD,
    FINISHED,
    CANCELLED,
  } code_;
  Backpressure backpressure_;

  explicit TaskStatus(Code code) : code_(code) {}
  explicit TaskStatus(Backpressure backpressure)
      : code_(Code::BACKPRESSURE), backpressure_(std::move(backpressure)) {}

 public:
  bool IsContinue() const { return code_ == Code::CONTINUE; }
  bool IsBackpressure() const { return code_ == Code::BACKPRESSURE; }
  bool IsYield() const { return code_ == Code::YIELD; }
  bool IsFinished() const { return code_ == Code::FINISHED; }
  bool IsCancelled() const { return code_ == Code::CANCELLED; }

  Backpressure& GetBackpressure() {
    ARA_DCHECK(IsBackpressure());
    return backpressure_;
  }

  const Backpressure& GetBackpressure() const {
    ARA_DCHECK(IsBackpressure());
    return backpressure_;
  }

  bool operator==(const TaskStatus& other) const { return code_ == other.code_; }

 public:
  static TaskStatus Continue() { return TaskStatus(Code::CONTINUE); }
  static TaskStatus Backpressure(Backpressure backpressure) {
    return TaskStatus(std::move(backpressure));
  }
  static TaskStatus Yield() { return TaskStatus{Code::YIELD}; }
  static TaskStatus Finished() { return TaskStatus{Code::FINISHED}; }
  static TaskStatus Cancelled() { return TaskStatus{Code::CANCELLED}; }
};

}  // namespace ara::task
