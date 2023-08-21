#pragma once

#include <ara/common/defines.h>

#include <any>

namespace ara::task {

using Backpressure = std::any;
using BackpressureResetCallback = std::function<Status()>;
using BackpressureAndResetPair = std::pair<Backpressure, BackpressureResetCallback>;
using BackpressurePairFactory = std::function<BackpressureAndResetPair()>;

};  // namespace ara::task
