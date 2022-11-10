#include <arrow/api.h>
#include <gtest/gtest.h>
#include <queue>

class Status {
 public:
  static Status runnable, blocking, finished, error, canceled;

 private:
  enum class Internal : int8_t {
    RUNNABLE,
    BLOCKING,
    FINISHED,
    ERROR,
    CANCELED,
  };

  template <Status::Internal internal>
  Status() : internal_(internal) {}

  Internal internal_;
};

Status Status::runnable template <Status::Internal>(Status::RUNNABLE);

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
  virtual arrow::Result<Status> Stream() { return downstream->Stream(); }

 protected:
 protected:
  std::shared_ptr<Operator> downstream;
};

class Source : public Operator {};

class PipelineBreaker : public Operator {};

class Sink : public PipelineBreaker {};
