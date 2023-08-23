#pragma once

#include <ara/common/defines.h>
#include <ara/util/defines.h>

namespace ara {

class Observer {
 public:
  virtual ~Observer() = default;
};

template <typename ObserverType>
class ChainedObserver : public ObserverType {
 public:
  explicit ChainedObserver(std::vector<std::unique_ptr<ObserverType>>&& observers)
      : observers_(std::move(observers)) {}

  void AddObserver(std::unique_ptr<ObserverType> observer) {
    observers_.push_back(std::move(observer));
  }

  template <typename Func, typename... Args>
  Status Observe(Func&& func, Args&&... args) {
    for (auto& observer : observers_) {
      auto typed_observer = dynamic_cast<ObserverType*>(observer.get());
      ARA_CHECK(typed_observer != nullptr);
      ARA_RETURN_NOT_OK((typed_observer->*func)(std::forward<Args>(args)...));
    }
    return Status::OK();
  }

 private:
  std::vector<std::unique_ptr<ObserverType>> observers_;
};

}  // namespace ara
