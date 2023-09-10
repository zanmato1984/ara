#include "sync_awaiter.h"
#include "sync_resumer.h"

#include <ara/util/defines.h>

namespace ara::schedule {

using task::AwaiterPtr;
using task::ResumerPtr;
using task::Resumers;

std::shared_ptr<SyncAwaiter> SyncAwaiter::MakeSyncAwaiter(size_t num_readies,
                                                          Resumers& resumers) {
  auto awaiter = std::make_shared<SyncAwaiter>(num_readies);
  for (auto& resumer : resumers) {
    auto casted = std::dynamic_pointer_cast<SyncResumer>(resumer);
    ARA_CHECK(casted != nullptr);
    casted->AddCallback([awaiter = awaiter->shared_from_this()]() {
      std::unique_lock<std::mutex> lock(awaiter->mutex_);
      awaiter->readies_++;
      awaiter->cv_.notify_one();
    });
  }
  return awaiter;
}

std::shared_ptr<SyncAwaiter> SyncAwaiter::MakeSingle(ResumerPtr& resumer) {
  Resumers resumers{resumer};
  return MakeSyncAwaiter(1, resumers);
}

std::shared_ptr<SyncAwaiter> SyncAwaiter::MakeAny(Resumers& resumers) {
  return MakeSyncAwaiter(1, resumers);
}

std::shared_ptr<SyncAwaiter> SyncAwaiter::MakeAll(Resumers& resumers) {
  return MakeSyncAwaiter(resumers.size(), resumers);
}

}  // namespace ara::schedule
