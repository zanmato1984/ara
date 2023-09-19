#pragma once

#include <ara/task/awaiter.h>

namespace ara::schedule {

class SyncAwaiter : public task::Awaiter,
                    public std::enable_shared_from_this<SyncAwaiter> {
 public:
  explicit SyncAwaiter(size_t num_readies) : num_readies_(num_readies), readies_(0) {}

  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (readies_ < num_readies_) {
      cv_.wait(lock);
    }
  }

 private:
  static std::shared_ptr<SyncAwaiter> MakeSyncAwaiter(size_t num_readies,
                                                      task::Resumers resumers);

 public:
  static std::shared_ptr<SyncAwaiter> MakeSingle(task::ResumerPtr);

  static std::shared_ptr<SyncAwaiter> MakeAny(task::Resumers);

  static std::shared_ptr<SyncAwaiter> MakeAll(task::Resumers);

 private:
  size_t num_readies_;
  std::mutex mutex_;
  std::condition_variable cv_;
  size_t readies_;
};

}  // namespace ara::schedule
