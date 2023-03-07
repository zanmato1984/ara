#include <arrow/api.h>
#include <gtest/gtest.h>
#include <queue>

class TaskStatus {
 public:
  static const TaskStatus runnable;
  static const TaskStatus waiting;
  static const TaskStatus finished;
  static const TaskStatus error;
  static const TaskStatus canceled;

  bool operator==(const TaskStatus& other) const { return internal_ == other.internal_; }

 private:
  enum class Internal : int8_t {
    RUNNABLE,
    WAITING,
    FINISHED,
    ERROR,
    CANCELED,
  } internal_;

  explicit TaskStatus(Internal internal) : internal_(internal) {}
};

class Task {
  public:
  virtual ~Task() = default;
  virtual TaskStatus status() = 0;
};

class Operator;

struct PipelineState {
  explicit PipelineState(size_t dop) : task_local_states(dop) {}

  /// Queue of data read by source and ready to process.
  std::queue<arrow::Datum> source_data;

  struct TaskLocalState {
    std::shared_ptr<Operator> pending_op;
    arrow::Datum pending_data;
  };

  /// Per-task states.
  std::vector<TaskLocalState> task_local_states;
};

class Operator {
 public:
  virtual arrow::Result<TaskStatus> Stream() { return downstream->Stream(); }

 protected:
 protected:
  std::shared_ptr<Operator> downstream;
};

class Source : public Operator {};

class PipelineBreaker : public Operator {};

class Sink : public PipelineBreaker {};
