# Ara Design Notes: Operator / Pipeline / Task / Execution

This document summarizes the essential design of Ara’s execution model, with an emphasis on the “deep meaning” behind the separation of concerns: **how the Operator/Pipeline interfaces isolate parallel, streaming computation from the parts that must be synchronized or serialized**, and how that separation is realized via `Task`, `TaskGroup`, and the scheduler.

Chinese version: `DESIGN.md`

Key references:
- Prototype / original ideas: `src/sketch/sketch_aggregation_test.cpp`
- “Same shape” real implementation: `src/ara/pipeline/op/scalar_aggregate.cpp`, `src/ara/pipeline/op/hash_aggregate.cpp`
- Pipeline runtime: `src/ara/pipeline/pipeline_task.h`, `src/ara/pipeline/pipeline_task.cpp`
- Pipeline segmentation (“stage splitting”): `src/ara/pipeline/physical_pipeline.cpp`, `src/ara/pipeline/pipeline_compile_test.cpp`
- Task abstraction + blocking semantics: `src/ara/task/README.md`, `src/ara/task/task_status.h`, `src/ara/task/task_group.h`
- Scheduler examples: `src/ara/schedule/naive_parallel_scheduler.cpp`, `src/ara/schedule/async_dual_pool_scheduler.cpp`

---

## 1. The “Tower” of Layers: vertically separating concerns

Ara’s execution design can be read as a tower (layer cake) from top to bottom:

1) **Operator layer** (operator semantics)
- Focus: how a batch becomes another batch (or how state is accumulated).
- Not responsible for: thread pools, futures, IO waiting, preemption/yield decisions.

2) **Pipeline layer** (composition + flow control)
- Focus: driving `Source -> Pipe... -> Sink` in the correct order, and making “needs more input / has more output / needs drain / blocked / yield” explicit.
- Not responsible for: which scheduler runs it or on which threads.

3) **Task layer** (the schedulable unit)
- Turns a pipeline into a *repeatable, small step* (batch-at-a-time / step-at-a-time).
- Exposes only `TaskStatus` (Continue/Blocked/Yield/Finished/Cancelled) so schedulers only need one unified protocol.

4) **Scheduler/Executor layer** (execution strategy)
- Decides: how parallelism maps to threads, how Blocked waits, how Yield migrates across pools, and how fairness/priorities work.
- Is swappable: the same pipeline/task code can run under a synchronous blocking scheduler or an async future-driven scheduler.

The “deep meaning” of this tower: **upper layers describe semantics and a local state machine; lower layers own concurrency/async/resource policy**. Concurrency is not baked into operators; it is a pluggable strategy implemented by the scheduler.

---

## 2. Data model basics: `Batch` and `dop`

Ara uses Arrow’s `arrow::compute::ExecBatch` as the batch unit:
- `src/ara/common/batch.h`: `using Batch = arrow::compute::ExecBatch;`

Parallelism (degree of parallelism, `dop`) shows up as:
- The number of parallel instances in a `TaskGroup`: `TaskGroup::NumTasks()`
- The number of “lanes” in `PipelineTask`, where each `ThreadId` indexes thread-local state.

Key constraint: **operators do not decide their own parallelism**. They are given a `ThreadId` and use it to index per-thread state, staying “parallel-capable but scheduler-agnostic”.

---

## 3. Operator interfaces: streaming vs staged work

### 3.1 Three operator kinds: Source / Pipe / Sink

Interfaces are defined in `src/ara/pipeline/op/op.h`:

- `SourceOp`
  - `PipelineSource Source(...)`
  - `task::TaskGroups Frontend(...)`
  - `std::optional<task::TaskGroup> Backend(...)`
- `PipeOp`
  - `PipelinePipe Pipe(...)`
  - `PipelineDrain Drain(...)`
  - `std::unique_ptr<SourceOp> ImplicitSource(...)`
- `SinkOp`
  - `PipelineSink Sink(...)`
  - `task::TaskGroups Frontend(...)`
  - `std::optional<task::TaskGroup> Backend(...)`
  - `std::unique_ptr<SourceOp> ImplicitSource(...)`

Meaning:
- `Source/Pipe/Sink/Drain` are the **streaming execution interface**: repeatedly invoked by the pipeline task.
- `Frontend/Backend` are **stage (lifecycle) hooks**: they express work that must happen at a stage boundary (before/after streaming).
- `ImplicitSource` is the key hook for **splitting pipelines / introducing a new stage**: when an operator needs to emit an additional data stream (not just a flush), it can be represented as an implicit source, which triggers pipeline compilation to create separate physical pipelines.

### 3.2 Why `Pipe/Sink` take `optional<Batch>`

`Pipe`/`Sink` signatures include `std::optional<Batch>`:
- New input: pass a `Batch`
- “Continue without new input”: pass `std::nullopt`

This is not about optionality for convenience; it encodes a **reentrant generator/coroutine-like contract**:
- An operator can produce multiple outputs from a single input (e.g., join probe iterators, materialize flush).
- An operator can be Blocked or Yielded mid-way and later resume processing **the same internal suspended state**, without requiring the caller to re-supply the input batch.

This is one of the foundations that allows Ara to run pipelines as explicit state machines without actual coroutine language features.

---

## 4. `OpOutput`: encoding flow control into a finite protocol

`src/ara/pipeline/op/op_output.h` defines `OpOutput` (returned as `OpResult = Result<OpOutput>`).

It is the “wire protocol” between operators and `PipelineTask`. Key codes:

| Code | Typical producer | Semantics (what the pipeline should do next) |
|---|---|---|
| `PIPE_SINK_NEEDS_MORE` | Pipe / Sink | No output can be pushed downstream now; upstream needs to provide more input (or schedule another step) |
| `PIPE_EVEN(batch)` | Pipe | Consumed current input and produced one output batch; continue to next operator |
| `SOURCE_PIPE_HAS_MORE(batch)` | Source / Pipe / Drain | Produced one batch but **has more internal output pending**; next step should resume at this operator (usually with `nullopt`) |
| `BLOCKED(resumer)` | Source / Pipe / Sink / Drain | Cannot make progress (IO/backpressure/resource); wait for `resumer->Resume()` |
| `PIPE_YIELD` | Pipe / Drain | Operator wants to yield before a long synchronous operation; scheduler may migrate it |
| `PIPE_YIELD_BACK` | Pipe / Drain | “Yield-back handshake” from resumption (see `PipelineTask` two-phase yield handling) |
| `FINISHED(optional<batch>)` | Source / Drain | Stream ended; may carry a final batch |
| `CANCELLED` | PipelineTask/Operator | Cancelled (often because a sibling parallel instance errored) |

Deep meaning: `OpOutput` turns implicit thread behaviors (block/yield/continue) into an **explicit composable protocol**. Pipelines can be driven as pure state machines; the scheduler decides how and where to run that state machine.

---

## 5. `PipelineTask`: turning a `PhysicalPipeline` into schedulable “small steps”

### 5.1 Logical vs physical pipelines

`LogicalPipeline` (`src/ara/pipeline/logical_pipeline.h`) describes:
- Multiple **channels**: each channel is `source + pipe ops...`
- A shared `sink`

`PhysicalPipeline` (`src/ara/pipeline/physical_pipeline.h`) is the compilation output:
- Each channel becomes `source + pipe ops... + sink`
- A logical pipeline may be split into multiple physical pipelines (see Stage discussion in §9).

### 5.2 What `PipelineTask` does

`src/ara/pipeline/pipeline_task.cpp` implements `PipelineTask` with two roles:

1) Drive **one channel** as a state machine over Source/Pipe/Drain/Sink (`PipelineTask::Channel`)
2) **Multiplex** multiple channels in one task instance: when one channel blocks, it can still run another channel and keep the CPU busy.

### 5.3 Channel-local state: explicit stacks + drain cursor

Each channel maintains per-`ThreadId` local state (`PipelineTask::Channel::ThreadLocal`):
- `sinking`: last time sink was Blocked; next step should call `Sink(nullopt)` first
- `pipe_stack`: where to resume a pipe (HasMore/Blocked/Yield push an index)
- `source_done`: whether source has returned `FINISHED`
- `drains`: which pipes have a drain function (precomputed list of indices)
- `draining`: current drain cursor
- `yield`: marks a “yield already sent to scheduler” handshake; next expects `PIPE_YIELD_BACK`

This is literally the pipeline’s program counter.

### 5.4 One `Channel::operator()` call: prioritized progression

Each invocation does bounded work and progresses in this order:

1) If `sinking`: resume `Sink(nullopt)`
2) Else if `pipe_stack` non-empty: resume `Pipe(pipe_id, nullopt)` from the stack top
3) Else if `!source_done`: call `Source()`; if it returns a batch, push it through from pipe0
4) Else: enter drain phase; call `Drain()` in `drains[draining]` order
5) After all drains finish: channel returns `FINISHED`

Key point: **normal streaming and “post-source flush/drain” are strictly separated**. Many operators (join/materialize/aggregate-like pipes) have tail output that can only be produced once the upstream is exhausted; this design makes that boundary explicit.

### 5.5 `PIPE_SINK_NEEDS_MORE` vs `SOURCE_PIPE_HAS_MORE`: separating “need new input” from “have internal output”

In `Channel::Pipe(...)` the main cases (conceptually):

- `PIPE_SINK_NEEDS_MORE`:
  - The pipe cannot produce output without more upstream input.
  - The pipeline returns upward and, on the next scheduling step, will pull from source again.

- `PIPE_EVEN(batch)`:
  - The pipe produced one output batch and does not require immediate continuation at this pipe.
  - The batch is forwarded to the next operator.

- `SOURCE_PIPE_HAS_MORE(batch)`:
  - The pipe produced a batch but still has more output pending internally.
  - `PipelineTask` pushes the current pipe index onto `pipe_stack` so the next step resumes here with `nullopt`.

Deep meaning: with one small resumption stack, Ara can represent **pipeline-parallel streaming** and **serial “drain this operator’s internal output first”** without blocking threads or using language-level coroutines.

### 5.6 Blocked: turning backpressure/IO into composable events

If a pipe or sink returns `BLOCKED(resumer)`:
- Pipe: push the pipe index, resume later via `Pipe(pipe_id, nullopt)`
- Sink: set `sinking=true`, resume later via `Sink(nullopt)`

`PipelineTask` never waits; it bubbles blocking upward as `TaskStatus::Blocked(awaiter)` (see §6/§7).

### 5.7 Yield: a two-phase handshake (`PIPE_YIELD` / `PIPE_YIELD_BACK`)

Yield exists to let the scheduler migrate execution to another pool/priority before long synchronous work.

`PipelineTask` treats yield as a two-step handshake:

1) On the first `PIPE_YIELD`:
   - Set `yield=true`, push current pipe index
   - Return `PIPE_YIELD` (mapped to `TaskStatus::Yield()`)
2) On resumption:
   - Expect the operator to return `PIPE_YIELD_BACK`
   - Clear `yield=false` and return `PIPE_YIELD_BACK` upward (typically mapped to Continue)

This design forces an explicit resume point after yielding, preventing “yield” from being treated as a normal continue and preserving scheduler policy.

---

## 6. Task / TaskStatus / TaskGroup: a scheduler-friendly protocol

### 6.1 Task: a repeatable small step

Task is defined in `src/ara/task/task.h` and explained in `src/ara/task/README.md`:
- Signature: `TaskResult (TaskContext&, TaskId)`
- `TaskResult` is `Result<TaskStatus>`
- A scheduler repeatedly invokes a task until it returns Finished/Cancelled or an error

This matches “one batch at a time” query execution: each run does a bit of work, enabling fairness and preemption.

### 6.2 TaskStatus: Continue / Blocked / Yield / Finished / Cancelled

`src/ara/task/task_status.h`:
- `Continue`: just schedule again
- `Blocked(awaiter)`: must wait for some external condition
- `Yield`: willing to yield; scheduler may migrate it
- `Finished`: this task instance is done
- `Cancelled`: cancelled (often due to sibling failures)

### 6.3 How PipelineTask maps OpOutput to TaskStatus

In `src/ara/pipeline/pipeline_task.cpp` (`PipelineTask::operator()`):

- If all channels finished: `TaskStatus::Finished()`
- If all unfinished channels are blocked:
  - Gather each channel’s `resumer`
  - Build an awaiter via `task_ctx.any_awaiter_factory(resumers)`
  - Return `TaskStatus::Blocked(awaiter)`
- If the last op result is `PIPE_YIELD`: return `TaskStatus::Yield()`
- If cancelled: return `TaskStatus::Cancelled()`
- Otherwise: `TaskStatus::Continue()`

Deep meaning: **PipelineTask compresses complex internal flow control into 5 states** that a scheduler can handle without knowing anything about Source/Pipe/Sink/Drain.

### 6.4 TaskGroup: parallel instances + barrier + “finish notification”

`src/ara/task/task_group.h` and `src/ara/task/README.md`:

- A `TaskGroup` is N parallel instances of one `Task` (`num_tasks`) plus an optional `Continuation`.
- `Continuation` runs **exactly once** after *all* parallel task instances finish successfully.
  - This is a primary tool for stage synchronization: global merge/finalize work belongs here (or in a follow-up task group), not inside streaming operators.
- `NotifyFinish` is a callback used to tell “possibly infinite waiting” tasks to finish (e.g., a sink waiting for more data).
  - `TaskGroupHandle::Wait` calls `TaskGroup::NotifyFinish` before waiting (`src/ara/schedule/scheduler.cpp`).

In other words, `TaskGroup` is both a parallelism container and a **barrier**.

---

## 7. Resumer/Awaiter: unifying async and backpressure as “being woken up”

The Task module abstracts “waiting” into two objects:

- `Resumer` (`src/ara/task/resumer.h`): a waker that external callbacks can trigger
- `Awaiter` (`src/ara/task/awaiter.h`): an object the scheduler uses to wait for wake-up conditions

`TaskContext` (`src/ara/task/task_context.h`) provides factories:
- `resumer_factory`
- `single_awaiter_factory` / `any_awaiter_factory` / `all_awaiter_factory`

Schedulers choose the concrete mechanism:
- Synchronous blocking: `src/ara/schedule/sync_awaiter.*` + `src/ara/schedule/sync_resumer.h`
- folly async: `src/ara/schedule/async_awaiter.*` + `src/ara/schedule/async_resumer.h`

Deep meaning: the same pipeline/operator code can run in a “true async” world or a “sync waiting” world, because *blocked* is purely semantic; the waiting mechanism is scheduler-specific.

---

## 8. Execution: pluggable schedulers (Naive vs Async Dual Pool)

Ara includes at least two schedulers to demonstrate that the `TaskStatus` protocol is abstract enough:

### 8.1 NaiveParallelScheduler: straightforward parallelism + blocking waits

`src/ara/schedule/naive_parallel_scheduler.cpp`:
- Uses `std::async` for each task instance
- Loops until Finished/Cancelled/error
- Blocked: `SyncAwaiter::Wait()` blocks
- Yield: observed/logged; does not migrate pools

### 8.2 AsyncDualPoolScheduler: CPU/IO dual pools + future-driven loops

`src/ara/schedule/async_dual_pool_scheduler.cpp`:
- Uses folly futures to represent task lifecycles
- Blocked: wait on `AsyncAwaiter::GetFuture()`, then continue
- Yield: run the next step via `io_executor_` (yielding away from the CPU pool)
- Normal execution: choose executor based on `TaskHint` (CPU/IO)

Deep meaning: Yield/Blocked are not implemented via sleeping/awaiting inside operators; they are decisions and mechanisms owned by the scheduler.

---

## 9. How “Stages” are expressed: three kinds of boundaries

Ara does not have a single “Stage” class today. Instead, stages are expressed via three mechanisms:

### 9.1 Stage boundary #1: `Frontend/Backend` task groups (operator lifecycle phases)

When an operator needs stage-specific work (setup, global merge, teardown), it exposes it as task groups:
- For sources: “start scan / prepare handles / open resources” (e.g., `HashJoinScanSource`)
- For sinks: “merge thread-local / finalize / materialize outputs” (aggregates are the canonical example)

Critically, this work is **not** done in the streaming `Source/Pipe/Sink` path, keeping the streaming path parallel-friendly and schedulable.

### 9.2 Stage boundary #2: `Drain` (tail output after source exhaustion)

`PipeOp::Drain` expresses “the upstream is done, but the operator still has tail output / needs flush”.

`PipelineTask` triggers drain only after `source_done`:
- Drain can return `SOURCE_PIPE_HAS_MORE` multiple times to emit tail batches
- Drain ends with `FINISHED(last_batch?)`

This fits cases like:
- join/materialize: flush buffered outputs
- operators with internal buffering whose tail output depends on end-of-stream

### 9.3 Stage boundary #3: `ImplicitSource` + physical pipeline splitting (cross-pipeline stages)

`PipeOp::ImplicitSource` and `SinkOp::ImplicitSource` express an even stronger boundary:
- After the “main input stream” ends, an operator may produce a *new stream* that becomes the source for downstream processing.
- This can lead to splitting a logical pipeline into multiple physical pipelines.

Compilation logic: `src/ara/pipeline/physical_pipeline.cpp`
- Today, `CompilePipeline` splits based on **pipe implicit sources**: it scans the pipe chain; if a pipe provides an implicit source, the remaining operators become a new channel rooted at that implicit source, creating additional `PhysicalPipeline(id)` groups.
- `SinkOp::ImplicitSource` (e.g., aggregates producing a materialized result batch as the next pipeline’s source) is typically used by a higher-level plan/driver when chaining adjacent pipelines/stages, rather than inside `CompilePipeline`’s intra-logical-pipeline splitting.

`src/ara/pipeline/pipeline_compile_test.cpp` contains many tests demonstrating these split results.

---

## 10. Reading the prototype: aggregation as the canonical “parallel + barrier” stage split

`src/sketch/sketch_aggregation_test.cpp` matters because it expresses Ara’s core split with minimal code.

### 10.1 What the prototype already contains (and what became “real”)

The prototype defines:
- `TaskStatus`: `CONTINUE/BACKPRESSURE/YIELD/FINISHED/CANCELLED`
- `OperatorResult`: `PIPE_SINK_NEEDS_MORE/PIPE_EVEN/SOURCE_PIPE_HAS_MORE/...`
- `PipelineTaskSource/Pipe/Drain/Sink` function types
- `SinkOp`: `Sink()` + `Frontend()` + `Backend()`

In the real implementation:
- `TaskStatus` → `src/ara/task/task_status.h`
- `OperatorResult` → `src/ara/pipeline/op/op_output.h` (`OpOutput`)
- pipeline function types → `src/ara/pipeline/op/op.h`

Key evolution:
- The prototype distinguishes `SOURCE_NOT_READY` and `SINK_BACKPRESSURE`.
- The real system unifies them as `BLOCKED(resumer)` and delegates waiting semantics to the scheduler via `Awaiter`.

This shows a crucial design choice: **all “cannot progress right now” reasons become Blocked + wake-up**, avoiding coupling operators to a specific async framework.

### 10.2 Scalar aggregate sink: parallel consume path + single-point merge path

In the prototype, `ScalarAggregateSink`:
- `Sink()`:
  - Called concurrently (test uses `std::async`)
  - Each thread updates its own local aggregate state via `states_[i][thread_id]`
  - No locks needed (thread-local state avoids synchronization)
- `Frontend()`:
  - Returns a task group with one instance
  - Does `MergeAll + finalize` to produce the final output batch

This is the clearest example of “pipeline interfaces isolate parallel vs serial”:
- **Parallel part**: per-thread state updates, streaming, scalable
- **Serial/synchronized part**: merge/finalize must happen after all input is consumed, naturally behind a barrier

### 10.3 Hash aggregate sink: parallel accumulation + global transposition merge

The prototype `HashAggregateSink` illustrates why barriers are unavoidable:
- Each thread has its own `Grouper` (local group ids) + aggregate kernel states
- Merge must align group-id spaces across groupers (transposition), then merge kernel states

This is inherently stage work:
- Only after all input is consumed can the global group set be finalized
- Merge crosses thread-local state boundaries

Therefore, placing it in `Frontend()` keeps the streaming `Sink()` clean and parallel-friendly.

---

## 11. From prototype to production code: Aggregates and Hash Join

### 11.1 Aggregates: thread-local consume in Sink; merge/finalize in Frontend; `ImplicitSource` to feed the next stage

Implementation:
- `src/ara/pipeline/op/scalar_aggregate.cpp`
- `src/ara/pipeline/op/hash_aggregate.cpp`

Shared structure:
- `Sink(...)`: parallel accumulation (one state per thread)
- `Frontend(...)`: task group with `num_tasks=1` to merge/finalize into `output_batch_`
- `ImplicitSource(...)`: wrap `output_batch_` with `SingleBatchParallelSource` to become the source for the downstream pipeline
  - `src/ara/pipeline/op/single_batch_parallel_source.cpp` splits the output batch by `dop`, so downstream consumption can be parallel again.

This forms a canonical two-stage pattern:
1) **Streaming stage (parallel)**: consume input, accumulate per-thread
2) **Barrier stage (sync/serial)**: merge/finalize, materialize results, then continue as a new pipeline

### 11.2 HashJoinBuild: buffer build side, then multiple task groups for build/merge/finalize

`src/ara/pipeline/op/hash_join_build.cpp` shows a multi-substage pattern:

- `Sink(...)`:
  - Inserts batches into an `AccumulationQueue` with a mutex
  - Acts as “build-side materialization/buffering”
- `Frontend(...)` returns three task groups:
  1) Build: `num_tasks=dop`, parallel build; continuation triggers the next stage
  2) Merge: `num_tasks=num_prtns`, parallel partition merge
  3) FinishMerge: `num_tasks=1`, finalization and wiring build-side into materialize structures

Here `TaskGroup`’s role is explicit:
- `num_tasks` encodes parallelism per substage
- continuation encodes stage ordering / happens-before barriers

### 11.3 HashJoinProbe: Pipe/Drain/ImplicitSource expresses “scan tail” as a new stage for RIGHT/FULL joins

`src/ara/pipeline/op/hash_join_probe.cpp`:

- `Pipe(...)`: probe input batches, often returning `SOURCE_PIPE_HAS_MORE` repeatedly (mini-batches / match iterators)
- `Drain(...)`:
  - For join types that do not require scanning the build side, drain flushes materialize after source is done
- `ImplicitSource(...)`:
  - For RIGHT_* / FULL_OUTER joins that must scan the build side, returns `HashJoinScanSource`
  - The scan source has its own `Frontend()` (StartScan task group), reflecting a source preparation phase

Deep meaning:
- For right/full joins, “unmatched outputs” are not a simple transform of the main input stream; they are a *new stream* that depends on state after probing completes.
- Using `ImplicitSource` to represent this as a **new pipeline stage** is clearer than forcing scan logic into drain:
  - Drain is for flushing existing buffers
  - Implicit sources are for producing a new source-side stream

### 11.4 Pipeline compiler: stage splitting and ordering

`src/ara/pipeline/physical_pipeline.cpp` can be summarized as:
- Logically, the user sees one pipeline; but when a pipe has an implicit source, the topology is cut there.
- The downstream segment becomes a new channel rooted at the implicit source.
- Channels and implicit sources are grouped into `PhysicalPipeline(id)` units, forming a stage sequence/grouping.

`src/ara/pipeline/pipeline_compile_test.cpp` cases like `DoublePhysicalPipeline/TripplePhysicalPipeline/...` demonstrate the resulting physical descriptions.

> Tests often execute physical pipelines sequentially (see `RunPipeline` in `src/ara/pipeline/pipeline_task_test.cpp`). The comments in `src/sketch/sketch_pipeline_test.cpp` suggest a future direction: scheduling a stage DAG with resource/performance trade-offs.

---

## 12. Practical rules when writing new operators

1) **Keep the streaming path lock-free when possible**: use `ThreadId` for thread-local state; do cross-thread merges in `Frontend/Continuation`.
2) **Separate “need new input” vs “have internal output”**:
   - Need new input: `PIPE_SINK_NEEDS_MORE`
   - Have internal output: `SOURCE_PIPE_HAS_MORE` (and guarantee you can continue with `nullopt`)
3) **Use Drain for tail output**: only when you need “flush after source done”.
4) **Use ImplicitSource for a new stream**: when output is not flush but a new source logic (e.g., right/full outer scan).
5) **Waiting for external events**: return `BLOCKED(resumer)`; invoke `resumer->Resume()` in the external callback.
6) **Long synchronous work**: return `PIPE_YIELD` and let the scheduler decide migration (AsyncDualPoolScheduler routes it to the IO pool).

