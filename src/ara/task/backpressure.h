#pragma once

#include <ara/common/defines.h>
#include <ara/task/defines.h>

#include <any>

namespace ara::task {

class TaskContext;
class Task;

using Backpressure = std::any;
using BackpressureResetCallback = std::function<Status()>;
using BackpressureAndResetPair = std::pair<Backpressure, BackpressureResetCallback>;
using BackpressurePairFactory = std::function<Result<BackpressureAndResetPair>(
    const TaskContext&, const Task&, TaskId)>;

};  // namespace ara::task
