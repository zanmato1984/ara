#include <arrow/api.h>

enum class OperatorStatusCode : char {
  RUNNING = 0,
  FINISHED = 1,
  SPILLING = 2,
  CANCELLED = 3,
  ERROR = 4,
};

struct OperatorStatus {
  OperatorStatusCode code;
  arrow::Status status;

  static OperatorStatus RUNNING() {
    return OperatorStatus{OperatorStatusCode::RUNNING, arrow::Status::OK()};
  }
  static OperatorStatus FINISHED() {
    return OperatorStatus{OperatorStatusCode::RUNNING, arrow::Status::OK()};
  }
  static OperatorStatus SPILLING() {
    return OperatorStatus{OperatorStatusCode::RUNNING, arrow::Status::OK()};
  }
  static OperatorStatus CANCELLED() {
    return OperatorStatus{OperatorStatusCode::RUNNING, arrow::Status::OK()};
  }
  static OperatorStatus ERROR(arrow::Status status) {
    return OperatorStatus{OperatorStatusCode::RUNNING, std::move(status)};
  }
};

struct Batch {
  int value = 0;
};

struct Result {
  OperatorStatus status;
  std::optional<Batch> batch;
};

class Operator {
 public:
  virtual ~Operator() = default;

  virtual Result Push(size_t thread_id, const Batch & batch) {
    return {OperatorStatus::RUNNING(), {}};
  }
  // TODO: Breakdown the `Break()` method into TaskGroup-like primitives?
  virtual OperatorStatus Break(size_t thread_id) { return OperatorStatus::RUNNING(); }
  virtual Result Finish(size_t thread_id) { return {OperatorStatus::RUNNING(), {}}; }
};

// TODO: Case about operator chaining in a pipeline.
// NOTE: How to implement hash join probe operator in a parallel-agnostic and
// future-agnostic fashion.  That is:
// 1. How to invoke and chain the `Stream()` methods of all the operators in this
// pipeline.
// 2. How to invoke the `Finish()` methods of all the operators in this pipeline.
// This is critical for cases like a pipeline having two hash right outer join probe
// operators, the `Finish()` method of each will scan the hash table and produce
// non-joined rows.
// 3. The operator should be sync/async-agnostic, that is, the concern of how to invoke
// them should be hidden from the operator implementation.
// 4. The operator should be parallel-agnostic, that is, the concern of how to parallel
// them should be hidden from the operator implementation.
// 5. The operator should be backpressure-agnostic, that is, the concern of how to handle
// backpressure should be hidden from the operator implementation. Try to encapsulate the
// backpressure handling logic only among source, sink, and optionally the pipeline runner
// itself.

// TODO: Case about operator ping-pong between RUNNING and SPILLING states.