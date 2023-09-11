# Ara Task Module

Ara employs task-based parallelism for query execution.
A query is eventually assembled into `Task`s, which are then scheduled by the `Scheduler` for execution on the underlying `Executor`.

## Task

A `Task` is the basic unit of execution with some descriptive information.
The code to execute is essentially a function that accepts a `TaskContext` and a `TaskId` as parameters and returns a `TaskStatus`.

The `Task` is designed to be able to execute repeatedly.
This design mirrors the "one batch at a time" execution approach inherent in query processing.
Additionally, it permits fine-grained scheduling of a specific task, particularly at the granularity of a batch.
The flow control of the task hinges on the `TaskStatus` returned by the function.

Moreover, the `Task` is designed to be able to execute in parallel, to enable parallelism in query processing.
The `TaskId` parameter aids in distinguishing an individual task instance amidst all the parallel instances of the same task.
It's essential to note that the decision to parallelize the task is determined by the scheduler and the underlying executor.
The task itself remains oblivious to this choice.

## Task Context

A `TaskContext` is used to provide all the common and consistent information throughout a `Task`'s lifespan.
For instance, it identifies the query associated with the task and provides an optional observer to oversee the task's execution.
It also contains factory methods for blocking handling.

## Task Status

The crux of task execution flow control is the `TaskStatus`.
It is used to indicate the current status of the task and to determine the next step of the task execution.
There are five explicit `TaskStatus` values, with an implicit error status that leverages the Arrow `Result` (note that the actual return type of the task function is `TaskResult`, synonymous with `arrow::Result<TaskStatus>`). 
These five statuses are:

### Continue

This status indicates the task is still in progress and ready to continue its execution.

### Blocked

This status indicates that the task is still in progress but is currently blocked.
It will not proceed until a certain condition is met and a "resumer" resumes it.

Blocking typically occurs when a `SourceOp` or `SinkOp`, which uses async I/O, is unable to produce or consume more data.
The task will remain blocked until the resumer, in this case, async I/O, notifies it via a callback.
Specifically, a blocked `SinkOp` will halt the entire task execution, including the `SourceOp` that reads in more data.
This can be thought of as applying "backpressure".

When in this status, a specific `Awaiter` instance can be obtained using the `GetAwaiter()` method.
The handling of blocking using this `Awaiter` and its associated `Resumer` is detailed below.

The scheduler is free to make scheduling decisions, such as stopping scheduling the task for execution until being notified by the resumer. 

### Yield

This status indicates that the task is still in progress but is about to undertake a time-consuming, synchronous operation. As a result, it is willing to yield itself. For example, an operator might need to spill some data onto the disk to reduce memory consumption.

The scheduler is free to make scheduling decisions, such as relocating the task to a different, potentially lower-priority, thread pool to prevent blocking the current thread.

It's important to note the distinction between `Blocked` and `Yield`. The former requires explicit resumption to continue, facilitating interoperation with async I/O in a purely async manner. In contrast, the latter will continue execution regardless, albeit possibly on a different thread pool and at a reduced priority.

### Finished

This status indicates the task has finished its execution.

### Cancelled

This status indicates the task has been cancelled, possibly because some other instance of the same task has encountered an error.

# Handle Blocking

The handling of blocking is designed to interoperate with async I/O in a purely asynchronous manner.
The principle is that the task shouldn't engage in any busy-wait during its execution.
Instead, it should be awakened by the async I/O to resume its execution via the callback provided by the async I/O framework.

This is achieved using a `Resumer`/`Awaiter` mechanism, i.e.:

* The `TaskContext` contains a `ResumerFactory` to create a `Resumer` instance.

* When blocked, for instance, when an aync `SourceOp` is unable to produce more data, the task first creates one or more `Resumer` instances using the `ResumerFactory`.
It then retains them for use in the async I/O callback.

* The `TaskContext` contains a set of `AwaiterFactory`s, namely:
  * `SingleAwaiterFactory`: Creates an `Awaiter` instance that waits for a single resumer to resume.
  * `AnyAwaiterFactory`: Creates an `Awaiter` instance that waits for any resumer in a group to resume.
  * `AllAwaiterFactory`: Creates an `Awaiter` instance that waits for all resumers in a group to resume.

* The task creates an `Awaiter` instance using the outstanding `Resumer` instance(s) it just instantiated, via one of the `AwaiterFactory`s, based on its desired awakening conditions.
A `BLOCKED` task status is then associated with this `Awaiter` instance and is returned to the scheduler by the task.

* Upon seeing the `BLOCKED` task status returned by the task, the scheduler pauses the task execution.
It then retrieves the `Awaiter` instance from the task status and performs all necessary actions to ensure the task can proceed once the resumer(s) resume it.

* Once the async I/O makes progress, it invokes the callback provided by the task. This callback, in turn, invokes the `Resume()` method(s) of the `Resumer` instance(s)' retained by the task.

* The scheduler then notices the task's resumption and the task proceeds.

Note that the implantation of `Resumer`/`Awaiter` is entirely scheduler-specific.
The task remains oblivious to this.
This means that the "async" aspect of blocking handling exists only at a semantic level.
For instance, a scheduler implementation that genuinely employs async programming primitives, such as `Future` or `Coroutine`, can make the system fully async.
However, a naive scheduler implementation, although not recommended, can still resort to busy-waiting for the resumer(s) to resume the task.

## Task Group

A `TaskGroup` is a conceptual group of all the parallel instances of a particular `Task`, with a designated quantity of such instances. Additionally, it contains optionally a `Continuation` and a `NotifyFinish`.

A `Continuation` is a function that accepts a `TaskContext` as its only parameter and returns an Arrow `Status`, i.e., OK or error.
It is guaranteed to be called exactly once after all the parallel instances of the task have successfully finished their execution, thus can be used to do some post-processing work, e.g., to assemble the results from all the parallel task instances.

A `NotifyFinish` is a function that accepts a `TaskContext` as its only parameter and returns an Arrow `Status`, i.e., OK or error.
It is used to notify the possibly infinite tasks to finish, e.g., to stop the `SinkOp` from waiting for more data.

Note that one shall not assume either the `Continuation` or the `NotifyFinish` is called in any particular thread, i.e., the actual thread, depending on the scheduler, can be the driver thread, the scheduler thread, or any thread within the executor thread pool.
