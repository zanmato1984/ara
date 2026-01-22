# Ara 设计要点：Operator / Pipeline / Task / Execution

本文聚焦 Ara 当前代码中最核心、最“有意味”的执行模型设计：**Operator 接口如何把可并发的流式计算与必须同步/串行的阶段切开**，以及这种切分如何落到 `Task` / `TaskGroup` / `Scheduler` 的组合里。

English version: `DESIGN.en.md`

重点参考：
- 原型/思想来源：`src/sketch/sketch_aggregation_test.cpp`
- 真实实现（同构落地）：`src/ara/pipeline/op/scalar_aggregate.cpp`、`src/ara/pipeline/op/hash_aggregate.cpp`
- Pipeline 运行时：`src/ara/pipeline/pipeline_task.h`、`src/ara/pipeline/pipeline_task.cpp`
- Pipeline 分段（stage 拆分）：`src/ara/pipeline/physical_pipeline.cpp`、`src/ara/pipeline/pipeline_compile_test.cpp`
- Task 抽象与阻塞语义：`src/ara/task/README.md`、`src/ara/task/task_status.h`、`src/ara/task/task_group.h`
- 调度器示例：`src/ara/schedule/naive_parallel_scheduler.cpp`、`src/ara/schedule/async_dual_pool_scheduler.cpp`

---

## 1. “塔”的分层：把关注点垂直拆开

可以把 Ara 的执行模型理解为一座自顶向下的分层塔（layer cake / tower）：

1) **Operator 层**（算子语义层）
- 关心：如何把一个 batch 变换成另一个 batch（或累积状态）。
- 不关心：线程池、future、如何等待 IO、如何抢占/让出。

2) **Pipeline 层**（算子组合与流控层）
- 关心：按正确顺序驱动 `Source -> Pipe... -> Sink`，把“需要更多输入/还有更多输出/需要 drain/阻塞/让出”显式化。
- 不关心：用什么调度器跑、跑在哪个线程。

3) **Task 层**（可调度最小单元）
- 把 pipeline 变成“可重复执行的一小步”（batch-at-a-time / step-at-a-time）。
- 用 `TaskStatus` 表达 Continue/Blocked/Yield/Finished/Cancelled，使调度器只需认识一个统一协议。

4) **Scheduler/Executor 层**（执行策略层）
- 决定：并行度如何映射到线程、Blocked 如何等待、Yield 如何迁移到不同线程池、任务如何公平调度。
- 可替换：同一套 pipeline/task 代码，可在同步阻塞调度或异步 future 调度中运行。

这座塔的“深层意味”是：**上层只表达语义与局部状态机，下层自由选择并发/异步/资源策略**。它不是把并发“揉进” operator，而是把并发作为可插拔策略留给 scheduler。

---

## 2. 基本数据模型：Batch 与并行度 dop

Ara 以 Arrow 的 `arrow::compute::ExecBatch` 作为批处理数据单元：
- `src/ara/common/batch.h`: `using Batch = arrow::compute::ExecBatch;`

并行度（degree of parallelism, dop）通常体现在：
- 一个 `TaskGroup` 中并行实例数：`TaskGroup::NumTasks()`
- PipelineTask 中每个 `ThreadId` 对应一条并行执行“lane”

核心约束：**Operator 的并行执行由外部决定**。Operator 通过 `ThreadId` 索引自己的 thread-local 状态，从而做到“可并发但不自带调度”。

---

## 3. Operator 接口：流式部分 vs. 阶段性部分

### 3.1 三类 Operator：Source / Pipe / Sink

接口定义在 `src/ara/pipeline/op/op.h`：

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

其中：
- `Source/Pipe/Sink/Drain` 是 **流式执行接口**：在 pipeline 任务里被反复调用。
- `Frontend/Backend` 是 **阶段性（stage）接口**：用于表达必须在某个边界前后运行的一组 task。
- `ImplicitSource` 是 **“拆 pipeline / 引入新 stage”** 的关键钩子：当某个 pipe/sink 需要在后续再产出一条“额外数据流”，就用隐式 source 表达，从而触发 pipeline 编译分段。

### 3.2 为什么 `Pipe/Sink` 输入是 `optional<Batch>`

`Pipe`/`Sink` 的签名里都有 `std::optional<Batch>`：
- 新输入：传 `Batch`
- “不带新输入的继续执行”：传 `std::nullopt`

这不是为了“可选”，而是为了表达一种 **可重入的生成器/协程式语义**：
- 一个 operator 可能在一次输入上产出多个输出（典型：join probe 迭代、materialize flush）。
- 一个 operator 可能在中途被 Blocked 或 Yield，需要在恢复后继续处理“同一份内部挂起状态”（并不一定需要再次提供输入 batch）。

这就是 Ara 把 pipeline 驱动写成显式状态机（而不是隐式线程阻塞）的关键铺垫。

---

## 4. OpOutput：把流控结果编码成有限集合

`src/ara/pipeline/op/op_output.h` 定义了 `OpOutput`（返回类型是 `OpResult = Result<OpOutput>`）。

它是 Operator 与 PipelineTask 之间的“协议”，核心 code 含义如下：

| Code | 典型产生者 | 语义（最关键的下一步） |
|---|---|---|
| `PIPE_SINK_NEEDS_MORE` | Pipe / Sink | 当前没有可向下游推进的输出；需要上游提供更多输入（或下一轮继续调度） |
| `PIPE_EVEN(batch)` | Pipe | 消耗了当前输入并产出一个输出 batch；pipeline 继续推进到下一算子 |
| `SOURCE_PIPE_HAS_MORE(batch)` | Source / Pipe / Drain | 产出一个 batch，但 **内部还“有货”**，下次应在同一算子处继续（通常用 `nullopt` 继续拉取） |
| `BLOCKED(resumer)` | Source / Pipe / Sink / Drain | 因 IO/背压/资源不足无法前进；等待 `resumer->Resume()` 唤醒 |
| `PIPE_YIELD` | Pipe / Drain | 算子主动让出（将做耗时同步工作）；scheduler 可迁移到低优先级/IO 池 |
| `PIPE_YIELD_BACK` | Pipe / Drain | 从 yield 恢复后的“回跳信号”（见下文 PipelineTask 的 yield 两阶段处理） |
| `FINISHED(optional<batch>)` | Source / Drain | 数据流结束；可能还附带最后一个 batch |
| `CANCELLED` | PipelineTask/Operator | 被取消（通常因为其他并行实例出错） |

> 深层意味：`OpOutput` 把“并发/阻塞/让出/继续”从隐式线程行为，变成了**显式的、可组合的协议**。从而 pipeline 可以被驱动成一个纯状态机，scheduler 决定怎么跑这个状态机。

---

## 5. PipelineTask：把一条 PhysicalPipeline 变成“可重复调度的一小步”

### 5.1 逻辑/物理 pipeline

`LogicalPipeline`（`src/ara/pipeline/logical_pipeline.h`）描述：
- 多个 **Channel**：每个 channel 是 `source + pipe ops...`
- 一个共享的 `sink`

`PhysicalPipeline`（`src/ara/pipeline/physical_pipeline.h`）是编译产物：
- 每个 channel 变成 `source + pipe ops... + sink`
- 可能被拆成多个 physical pipelines（见第 7 节 stage）

### 5.2 PipelineTask 的职责

`src/ara/pipeline/pipeline_task.cpp` 的 `PipelineTask` 做两件事：

1) **驱动单个 channel 的 Source/Pipe/Drain/Sink 状态机**（`PipelineTask::Channel`）
2) **在同一个 Task 实例里复用/复调多个 channel**（multiplexing），以便当某个 channel Blocked 时还能做别的工作

### 5.3 Channel 的局部状态：显式栈 + drain 游标

每个 channel 对每个 `ThreadId` 维护一份 `ThreadLocal`（见 `PipelineTask::Channel::ThreadLocal`）：
- `sinking`: 上次 sink Blocked，需要下一次先 `Sink(nullopt)` 续跑
- `pipe_stack`: 需要从某个 pipe 位置继续（HasMore/Blocked/Yield 都会压栈）
- `source_done`: source 是否已 `FINISHED`
- `drains`: 哪些 pipe 有 drain（提前预处理成索引列表）
- `draining`: 当前 drain 游标
- `yield`: 标记“上一轮已经对 scheduler yield 过”，下一次期待 `PIPE_YIELD_BACK`

这组状态就是 pipeline 的“程序计数器”。

### 5.4 单次调用到底做什么：优先级顺序

`Channel::operator()` 一次只做“有限工作”，按以下优先级推进：

1) 若 `sinking`：先续跑 `Sink(nullopt)`
2) 若 `pipe_stack` 非空：从栈顶 pipe 续跑 `Pipe(pipe_id, nullopt)`
3) 若 `!source_done`：拉取 `Source()` 得到 batch，再从 pipe0 开始推进
4) 若 `source_done`：进入 drain 阶段，按 `drains[draining]` 顺序调用 `Drain()`
5) 所有 drain 都 finished：channel `FINISHED`

关键点：**“正常流式推进”与“源耗尽后的 flush/drain”被严格区分**。这正是 stage/同步语义的一个体现：很多算子（join/materialize/aggregate）必须等到输入耗尽才知道还有没有尾部输出。

### 5.5 `PipeSinkNeedsMore` vs `SourcePipeHasMore`：切开“要新输入”与“内部还有货”

`Channel::Pipe(...)` 的核心分支（简化解释）：

- `PIPE_SINK_NEEDS_MORE`：
  - 表示当前 pipe 没有可向下游推进的输出，想要更多上游输入
  - PipelineTask 直接返回该状态给上层（转成 `TaskStatus::Continue()`）
  - 下一次调度会回到 source 拉下一批

- `PIPE_EVEN(batch)`：
  - 产出一个 batch 且不需要在本 pipe 继续“吐更多”
  - PipelineTask 把 batch 交给下一 pipe（或最终 sink）

- `SOURCE_PIPE_HAS_MORE(batch)`：
  - 产出一个 batch，但本 pipe 内部还有后续输出
  - PipelineTask 会把当前 pipe id 压入 `pipe_stack`
  - 继续把 batch 往下推进；下次调度时会先从这个 pipe 继续（传 `nullopt`）

深层意味：这个区分使 pipeline 能把**并发可流水化的部分**（批处理推进）与**必须串行 drain 的部分**（同一个 pipe 内部“吐完”再回到 source）用一个小栈表达出来，而不需要真实的协程/线程阻塞。

### 5.6 Blocked：把 backpressure/IO 等待变成可组合事件

当 pipe 或 sink 返回 `BLOCKED(resumer)`：
- pipe：压栈当前 pipe id，等待恢复后 `Pipe(pipe_id, nullopt)`
- sink：设置 `sinking=true`，等待恢复后 `Sink(nullopt)`

PipelineTask 本身并不等待，它只是把 resumer 往上交给 `TaskStatus::Blocked(awaiter)`（见第 6/7 节）。

### 5.7 Yield 的“两阶段”处理（PIPE_YIELD / PIPE_YIELD_BACK）

`PIPE_YIELD` 的目标是让 scheduler 有机会把任务迁移到别的线程池/优先级，避免长时间占用当前 CPU 执行上下文。

PipelineTask 的处理不是简单“原地继续”，而是显式两段：

1) 第一次遇到 `PIPE_YIELD`：
   - 记录 `yield=true`，把当前 pipe 压栈
   - 立即向上返回 `PIPE_YIELD`（从而 `TaskStatus::Yield()`）
2) 恢复后再次进入该 pipe：
   - 期待 pipe 返回 `PIPE_YIELD_BACK`
   - 清掉 `yield=false`，再返回 `PIPE_YIELD_BACK` 给上层（上层通常转成 Continue）

这种设计强迫算子在“已经向 scheduler 让出一次”后，显式确认恢复点，避免 yield 被当成普通 continue 导致调度层策略失效。

---

## 6. Task / TaskStatus / TaskGroup：把 pipeline 变成调度器可理解的协议

### 6.1 Task：可重复执行的一小步

Task 定义见 `src/ara/task/task.h` 与 `src/ara/task/README.md`：
- 签名：`TaskResult (TaskContext&, TaskId)`
- `TaskResult` 是 `Result<TaskStatus>`
- 一个 Task 会被 scheduler 反复调用，直到返回 Finished/Cancelled 或 error

这与 query processing 的“one batch at a time”思想一致：每次做一点点，方便抢占与公平调度。

### 6.2 TaskStatus：Continue / Blocked / Yield / Finished / Cancelled

`src/ara/task/task_status.h`：
- `Continue`：继续调度即可
- `Blocked(awaiter)`：必须等待某个条件被唤醒
- `Yield`：愿意让出，scheduler 可迁移执行位置
- `Finished`：本 Task 实例结束
- `Cancelled`：被取消（通常因为并行同伴失败）

### 6.3 PipelineTask 如何把 OpOutput 映射到 TaskStatus

见 `src/ara/pipeline/pipeline_task.cpp` 的 `PipelineTask::operator()`：

- 若所有 channel 都 finished：`TaskStatus::Finished()`
- 若所有未完成的 channel 都 blocked：
  - 收集各 channel 的 `resumer`
  - 用 `task_ctx.any_awaiter_factory(resumers)` 构造 awaiter
  - 返回 `TaskStatus::Blocked(awaiter)`
- 若遇到 `PIPE_YIELD`：返回 `TaskStatus::Yield()`
- 若 cancelled：返回 `TaskStatus::Cancelled()`
- 否则：`TaskStatus::Continue()`

深层意味：**PipelineTask 把 pipeline 内部复杂流控“压缩”成调度层只需理解的 5 个状态**。调度器无需理解 source/pipe/sink，也无需知道 drain/stack。

### 6.4 TaskGroup：并行实例 + barrier（Continuation）+ 终止通知（NotifyFinish）

`src/ara/task/task_group.h` 与 `src/ara/task/README.md`：

- `TaskGroup` = 一个 `Task` 的 N 个并行实例（`num_tasks`） + 可选 `Continuation`
- `Continuation`：保证 **所有并行实例成功 Finished 后恰好执行一次**
  - 这是 stage 同步/串行化的核心工具：把“并行阶段结束后要做的全局合并/收尾”放进 continuation 或后续 task group
- `NotifyFinish`：用于通知可能“无限等待”的任务收敛结束（典型：sink 等待更多输入）
  - `TaskGroupHandle::Wait` 在等待前调用 `TaskGroup::NotifyFinish`（见 `src/ara/schedule/scheduler.cpp`）

换句话说：`TaskGroup` 既是并行度的载体，也是一个**同步栅栏（barrier）**。

---

## 7. Resumer/Awaiter：把异步与背压统一为“被唤醒”

Task 模块把异步等待抽象成两件事：

- `Resumer`（`src/ara/task/resumer.h`）：一个可被外部回调触发的“唤醒器”
- `Awaiter`（`src/ara/task/awaiter.h`）：scheduler 用来等待唤醒条件的对象

`TaskContext`（`src/ara/task/task_context.h`）提供工厂：
- `resumer_factory`
- `single_awaiter_factory` / `any_awaiter_factory` / `all_awaiter_factory`

调度器实现决定 await 的具体机制：
- 同步阻塞：`src/ara/schedule/sync_awaiter.*` + `sync_resumer.h`
- folly 异步：`src/ara/schedule/async_awaiter.*` + `async_resumer.h`

深层意味：**同一个 pipeline/operator 代码可以在“真异步”或“同步等待”两种世界里运行**，因为“阻塞”只在语义层存在，具体怎么等由 scheduler 决定。

---

## 8. Execution：调度策略可替换（Naive vs Async Dual Pool）

Ara 至少给出两种 scheduler，用来证明 “TaskStatus 协议足够抽象”：

### 8.1 NaiveParallelScheduler：最直观的并发与同步等待

`src/ara/schedule/naive_parallel_scheduler.cpp`：
- 每个 task 实例用 `std::async` 起一个线程/任务
- 循环调用 task，直到 finished/cancelled/error
- Blocked：用 `SyncAwaiter::Wait()` 阻塞等待
- Yield：仅做 observer 记录，不迁移线程池

### 8.2 AsyncDualPoolScheduler：CPU/IO 双线程池 + future 驱动

`src/ara/schedule/async_dual_pool_scheduler.cpp`：
- 用 folly future 表达任务生命周期
- Blocked：等待 `AsyncAwaiter::GetFuture()` 完成后继续
- Yield：把后续执行切到 `io_executor_`（即“让出 CPU 池”）
- 普通执行：根据 `TaskHint`（CPU/IO）选择 executor

深层意味：**Yield/Blocked 不是 operator 自己去 sleep/await，而是把决策权交给 scheduler**。这正是分层塔的意义。

---

## 9. Stage 的体现：三种“必须同步/串行”的边界如何表达

Ara 里“stage”并不是一个单独的 class，而是由三类机制共同表达：

### 9.1 Stage 边界一：`Frontend/Backend` TaskGroups（算子生命周期阶段）

当算子需要在流式执行前/后做阶段性工作（准备、全局合并、收尾），就把这部分表达成 task group：
- 对 Source：常见是“开始扫描/准备句柄”等（见 `HashJoinScanSource`）
- 对 Sink：常见是“合并 thread-local / finalize / 产出物化结果”等（聚合就是典型）

关键：这部分工作 **不在 pipeline 的 Source/Pipe/Sink 里做**，从而不会污染流式路径的并发性与可抢占性。

### 9.2 Stage 边界二：`Drain`（源耗尽后的尾部输出阶段）

`PipeOp::Drain` 表达“输入已经结束，但算子内部还有尾部输出/需要 flush”：
- 只在 `source_done` 之后由 PipelineTask 触发
- 可以 `SOURCE_PIPE_HAS_MORE` 多次产出尾部 batch
- 可以 `FINISHED(last_batch?)` 结束

这类阶段常见于：
- join/materialize：flush 缓冲区
- 聚合：如果实现为 pipe（或某些有缓存的算子）也会用到

### 9.3 Stage 边界三：`ImplicitSource` + PhysicalPipeline 拆分（跨 pipeline stage）

`PipeOp::ImplicitSource` 与 `SinkOp::ImplicitSource` 用来表达一种更强的 stage 边界：
- 某个 operator 在“主输入流”结束后，还会产生一条新的数据流，作为后续算子的输入源
- 这会导致 pipeline 编译时把一条逻辑 pipeline 拆成多个 physical pipelines

编译逻辑见 `src/ara/pipeline/physical_pipeline.cpp`：
- 当前 `CompilePipeline` 的拆分点来自 **pipe 的 implicit source**：扫描 pipe ops，一旦某个 pipe 提供了 implicit source，就把后续算子链挂到这个 source 上形成新 channel，由此形成多个 physical pipelines（可理解为 stage DAG 的线性化/分组）
- `SinkOp::ImplicitSource`（例如聚合 sink 产出物化结果再作为下一 pipeline 的 source）通常由更高层的 plan/driver 在“连接相邻 pipeline/stage”时使用，而不是在单条 `LogicalPipeline` 的内部拆分里完成

`src/ara/pipeline/pipeline_compile_test.cpp` 用大量用例验证了这种“按 implicit source 拆段”的行为。

---

## 10. 从 `sketch_aggregation_test.cpp` 看最经典的 stage：并行累积 + 串行合并

`src/sketch/sketch_aggregation_test.cpp` 的意义在于：它把 Ara 执行模型中最本质的切分，用最小代码说明白了。

### 10.1 原型里有哪些“后来的核心”

原型文件中定义了：
- `TaskStatus`：`CONTINUE/BACKPRESSURE/YIELD/FINISHED/CANCELLED`
- `OperatorResult`：`PIPE_SINK_NEEDS_MORE/PIPE_EVEN/SOURCE_PIPE_HAS_MORE/...`
- `PipelineTaskSource/Pipe/Drain/Sink` 的函数类型
- `SinkOp`：提供 `Sink()` + `Frontend()` + `Backend()`

对应到真实实现：
- `TaskStatus` → `src/ara/task/task_status.h`
- `OperatorResult` → `src/ara/pipeline/op/op_output.h` 的 `OpOutput`
- `PipelineTask*` → `src/ara/pipeline/op/op.h` 的 `PipelineSource/Pipe/Drain/Sink`

更关键的演进点：
- 原型区分 `SOURCE_NOT_READY` / `SINK_BACKPRESSURE`
- 真实实现统一为 `BLOCKED(resumer)`，并通过 `Awaiter` 机制交给 scheduler

这体现了 Ara 的一个“深层设计选择”：**把所有“暂时跑不了”的原因统一抽象为 Blocked + 可唤醒**，避免 operator 与具体异步框架耦合。

### 10.2 ScalarAggregateSink：并发安全的累积路径 + 单点合并路径

原型里的 `ScalarAggregateSink`：
- `Sink()`：
  - 多线程并发调用（测试里 `std::async`）
  - 每个 thread 用 `states_[i][thread_id]` 更新自己的局部聚合状态
  - 不需要锁（thread-local 状态避免了同步）
- `Frontend()`：
  - 返回一个只有 1 个实例的 task group
  - 做 `MergeAll + finalize`，把所有 thread-local state 合并成最终输出 batch

这就是“pipeline 接口隔离并发与串行”的最直观例子：
- **并发部分**：每个线程只写自己的 state，流式且可扩展
- **串行/同步部分**：merge 只能在“全体输入结束”后做，并且天然带 barrier

### 10.3 HashAggregateSink：并发累积 + 需要全局 transposition 的 merge

原型里的 `HashAggregateSink` 更能体现“为什么必须有 stage”：
- 每个线程维护自己的 `Grouper`（局部分组 id）+ `agg_states`
- 合并时不仅要 merge kernel state，还要把不同 grouper 的 group id 空间对齐（transposition）

这件事本质上就是一个同步阶段：
- 只有在所有输入都 consume 完之后，才能确定全局 group 集合
- merge 过程要跨线程访问/移动 state，是典型的 barrier 工作

因此把它放进 `Frontend()` 的单点 task，不污染 `Sink()` 的并发路径，是非常“正确的切分”。

---

## 11. 原型如何落到真实代码：ScalarAggregate / HashAggregate / HashJoin

### 11.1 聚合：Sink 里只做 thread-local 累积；Frontend 做 merge+finalize；ImplicitSource 产出下一 stage 的 source

对应实现：
- `src/ara/pipeline/op/scalar_aggregate.cpp`
- `src/ara/pipeline/op/hash_aggregate.cpp`

共同结构：
- `Sink(...)`：并行累积（每线程一份 state）
- `Frontend(...)`：返回 `num_tasks=1` 的 task group，做 merge/finalize，生成 `output_batch_`
- `ImplicitSource(...)`：把 `output_batch_` 包装成 `SingleBatchParallelSource`，作为下游 pipeline 的 source
  - `src/ara/pipeline/op/single_batch_parallel_source.cpp` 会按 dop 切分输出 batch，让下游也能并行消费

这实际上构成一个经典两阶段：
1) **Streaming stage（可并发）**：源数据流进来，局部聚合并行累积
2) **Barrier stage（同步/可能串行）**：merge+finalize，产出物化结果，再进入下一 pipeline

### 11.2 HashJoinBuild：先收集 build side，再用多个 task group 做 build/merge/finalize

`src/ara/pipeline/op/hash_join_build.cpp` 展示了更复杂的“多子阶段”：

- `Sink(...)`：
  - 用互斥锁把输入 batch 插入 `AccumulationQueue`
  - 本质上是“build side 物化/缓存”阶段（这里是同步点之一）
- `Frontend(...)` 返回 3 个 task groups：
  1) Build：`num_tasks=dop`，并行构建分区/局部结构，带 continuation 触发下一阶段
  2) Merge：`num_tasks=num_prtns`，并行做分区合并
  3) FinishMerge：`num_tasks=1`，做最终收尾，并把 build side 设置到 materialize 结构

这里 `TaskGroup` 的价值非常明确：
- 用 `num_tasks` 明确每个子阶段的并行度
- 用 continuation 明确子阶段之间的 happens-before（barrier）

### 11.3 HashJoinProbe：Pipe/Drain/ImplicitSource 三件套把 right/full outer 的“扫描尾部”表达为新 stage

`src/ara/pipeline/op/hash_join_probe.cpp`：

- `Pipe(...)`：对输入 batch 做 probe，可能通过 `SOURCE_PIPE_HAS_MORE` 多次吐出输出（mini-batch/match iterator）
- `Drain(...)`：
  - 对不需要扫描 build side 的 join type，在 source done 后 flush materialize（尾部输出）
- `ImplicitSource(...)`：
  - 对 RIGHT_* / FULL_OUTER 等需要扫描 build side 的 join type，返回 `HashJoinScanSource`
  - scan source 自己还有 `Frontend()`（StartScan task group），体现“source 的准备阶段”

深层意味：
- right/full outer 的“未匹配输出”不是对主输入流的简单变换，而是一条依赖于“probe 完成后状态”的新数据流
- 用 `ImplicitSource` 把它表达成 **新的 pipeline stage**，比把扫描逻辑硬塞进 drain 更清晰：
  - drain 适合 flush “已有缓冲”
  - implicit source 适合产生“新的源端流”

### 11.4 PipelineCompiler：stage 拆分与顺序

`src/ara/pipeline/physical_pipeline.cpp` 的编译策略可以概括为：
- 逻辑上仍然是一条 pipeline，但遇到 “pipe 有 implicit source” 就在拓扑上切开
- 被切开的后半段以 implicit source 作为新 channel 的 source
- 多个 channel/implicit source 被归入若干 `PhysicalPipeline(id)`，形成 stage 的序列/分组

`src/ara/pipeline/pipeline_compile_test.cpp` 的 `DoublePhysicalPipeline/TripplePhysicalPipeline/...` 用例很好地展示了拆分后的 physical pipeline 描述字符串差异。

> 目前测试里通常按 physical pipeline 顺序串行执行（见 `src/ara/pipeline/pipeline_task_test.cpp` 的 `RunPipeline`），但从 `src/sketch/sketch_pipeline_test.cpp` 的注释可以看出：未来要解决的是 stage DAG 的并发调度与资源权衡。

---

## 12. 写新算子时如何使用这些接口（经验法则）

1) **优先让流式路径无锁**：用 `ThreadId` 切 thread-local state；跨线程合并放到 `Frontend/Continuation`。
2) **区分“要新输入”与“内部还有输出”**：
   - 要新输入：`PIPE_SINK_NEEDS_MORE`
   - 内部还有输出：`SOURCE_PIPE_HAS_MORE`（并保证 `nullopt` 可继续拉取）
3) **尾部输出用 Drain**：当且仅当需要 “source done 之后 flush”。
4) **“新数据流”用 ImplicitSource**：当输出不是 flush，而是一个新的 source 逻辑（如 right/full outer scan）。
5) **需要等待外部事件**：返回 `BLOCKED(resumer)`，把唤醒回调里调用 `resumer->Resume()`。
6) **会做耗时同步工作**：返回 `PIPE_YIELD`，让 scheduler 决定迁移策略（AsyncDualPoolScheduler 会把它切到 IO 池）。
