#pragma once

#include <string>

namespace ara::task::detail {

class TaskMeta {
 public:
  TaskMeta(std::string name, std::string desc)
      : name_(std::move(name)), desc_(std::move(desc)) {}

  std::string GetName() const { return name_; }

  std::string GetDesc() const { return desc_; }

 private:
  std::string name_;
  std::string desc_;
};

}  // namespace ara::task::detail
