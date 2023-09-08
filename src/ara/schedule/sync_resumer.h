#pragma once

#include <ara/task/resumer.h>

namespace ara::schedule {

class SyncResumer : public task::Resumer {
 public:
  using Callback = std::function<void()>;

  void Resume() override {
    std::unique_lock<std::mutex> lock(mutex_);
    ready_ = true;
    for (const auto& cb : callbacks_) {
      cb();
    }
  }

  bool IsResumed() override {
    std::unique_lock<std::mutex> lock(mutex_);
    return ready_;
  }

  void AddCallback(Callback cb) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (ready_) {
      cb();
    } else {
      callbacks_.push_back(std::move(cb));
    }
  }

 private:
  std::mutex mutex_;
  bool ready_ = false;
  std::vector<Callback> callbacks_;
};

}  // namespace ara::schedule
