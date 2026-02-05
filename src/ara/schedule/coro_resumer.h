#pragma once

#include <ara/task/resumer.h>

#include <atomic>
#include <functional>

namespace ara::schedule {

class CoroResumer : public task::Resumer {
 public:
  using Callback = std::function<void()>;

  ~CoroResumer() override {
    auto* head = callbacks_.load(std::memory_order_acquire);
    if (head == kResumedSentinel()) {
      return;
    }
    while (head != nullptr) {
      auto* next = head->next;
      delete head;
      head = next;
    }
  }

  void Resume() override {
    auto* head =
        callbacks_.exchange(kResumedSentinel(), std::memory_order_acq_rel);
    if (head == kResumedSentinel()) {
      return;
    }

    // Call callbacks in the order they were registered.
    CallbackNode* reversed = nullptr;
    while (head != nullptr) {
      auto* next = head->next;
      head->next = reversed;
      reversed = head;
      head = next;
    }
    head = reversed;

    while (head != nullptr) {
      auto* next = head->next;
      auto cb = std::move(head->cb);
      delete head;
      head = next;
      if (cb) {
        cb();
      }
    }
  }

  bool IsResumed() override {
    return callbacks_.load(std::memory_order_acquire) == kResumedSentinel();
  }

  void AddCallback(Callback cb) {
    auto* head = callbacks_.load(std::memory_order_acquire);
    if (head == kResumedSentinel()) {
      cb();
      return;
    }

    auto* node = new CallbackNode{std::move(cb), nullptr};
    while (head != kResumedSentinel()) {
      node->next = head;
      if (callbacks_.compare_exchange_weak(head, node, std::memory_order_release,
                                          std::memory_order_acquire)) {
        return;
      }
    }

    auto call_now = std::move(node->cb);
    delete node;
    if (call_now) {
      call_now();
    }
  }

 private:
  struct CallbackNode {
    Callback cb;
    CallbackNode* next;
  };

  static CallbackNode* kResumedSentinel() {
    return reinterpret_cast<CallbackNode*>(1);
  }

  std::atomic<CallbackNode*> callbacks_{nullptr};
};

}  // namespace ara::schedule
