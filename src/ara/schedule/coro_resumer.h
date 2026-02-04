#pragma once

#include <ara/task/resumer.h>

#include <functional>
#include <mutex>
#include <vector>

namespace ara::schedule {

class CoroResumer : public task::Resumer {
 public:
  using Callback = std::function<void()>;

  void Resume() override {
    std::vector<Callback> callbacks;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (resumed_) {
        return;
      }
      resumed_ = true;
      callbacks = std::move(callbacks_);
    }
    for (const auto& cb : callbacks) {
      cb();
    }
  }

  bool IsResumed() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return resumed_;
  }

  void AddCallback(Callback cb) {
    bool call_now = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      call_now = resumed_;
      if (!call_now) {
        callbacks_.push_back(std::move(cb));
      }
    }
    if (call_now) {
      cb();
    }
  }

 private:
  std::mutex mutex_;
  bool resumed_ = false;
  std::vector<Callback> callbacks_;
};

}  // namespace ara::schedule

