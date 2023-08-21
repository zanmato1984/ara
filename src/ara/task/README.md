# Ara Task Module

Ara employs task-based parallelism for query execution. A query is eventually assembled into `Task`s, which are then scheduled by the `Scheduler` for execution on the underlying `Executor`.

## Task

A `Task` is the basic unit of **execution** with some descriptive information. The code to execute is essentially a function that accepts a `TaskContext` and a `TaskId` as parameters and returns a `TaskStatus`.

The `Task` is designed to be able to execute repeatedly. This design mirrors the "one batch at a time" execution approach inherent in query processing. Additionally, it permits fine-grained scheduling of a specific task, particularly at the granularity of a batch. The flow control of the task hinges on the `TaskStatus` returned by the function.

Moreover, the `Task` is designed to be able to execute in parallel, to enable parallelism in query processing. The `TaskId` parameter aids in distinguishing an individual task instance amidst all the parallel instances of the same task. It's essential to note that the decision to parallelize the task is determined by the scheduler and the underlying executor. The task itself remains oblivious to this choice.

## Task Context

A `TaskContext` is used to provide all the common and **consistent** information throughout a `Task`'s lifespan. For instance, it identifies the query associated with the task and provides an optional observer to oversee the task's execution.

## Task Status

The crux of task execution flow control is the `TaskStatus`. It is used to indicate the current status of the task and to determine the next step of the task execution. There are five explicit `TaskStatus` values, with an implicit error status that leverages the Arrow `Result` (note that the actual return type of the task function is `TaskResult`, synonymous with `arrow::Result<TaskStatus>`). These five statuses are:

### Continue

This status indicates the task is still in progress and ready to continue its execution.

### Backpressure

This status indicates the task is still in progress but meets some kind of backpressure, e.g., a `SinkOp` not able to accept more data. The scheduler is free to make decisions such as stopping scheduling the task for execution until the backpressure is reset (resolved).

### Yield

This status indicates the task is still in progress but is going to do some time-consuming, blocking operator thus is willing to yield itself, e.g., an operator going to spill some data onto disk to reduce the memory consumption. The scheduler is free to make decisions such as moving the task to a different, possibly a low-priority, thread pool to avoid blocking the current thread.

### Finished

This status indicates the task has finished its execution.

### Cancelled

This status indicates the task has been cancelled, possibly because some other instance of the same task has encountered an error.

## Task Group

A `TaskGroup` is a conceptual group of all the parallel instances of a particular `Task`, with a designated quantity of such instances. Additionally, it contains optionally a `Continuation` and a `NotifyFinish`.

A `Continuation` is a function that accepts a `TaskContext` as its only parameter and returns an Arrow `Status`, i.e., OK or error. It is guaranteed to be called exactly once after all the parallel instances of the task have successfully finished their execution, thus can be used to do some post-processing work, e.g., to assemble the results from all the parallel task instances.

A `NotifyFinish` is a function that accepts a `TaskContext` as its only parameter and returns an Arrow `Status`, i.e., OK or error. It is used to notify the possibly infinite tasks to finish, e.g., to stop the `SinkOp` from waiting for more data.

Note that one shall not assume either the `Continuation` or the `NotifyFinish` is called in any particular thread, i.e., the actual thread, depending on the **scheduler**, can be the `Driver` thread, the `Scheduler` thread, or any thread within the `Executor` thread pool.
