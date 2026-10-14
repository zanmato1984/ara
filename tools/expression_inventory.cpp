#include <arrow/compute/function.h>
#include <arrow/compute/initialize.h>
#include <arrow/compute/registry.h>
#include <arrow/util/config.h>
#include <algorithm>
#include <iomanip>
#include <iostream>

int main() {
  auto status = arrow::compute::Initialize();
  if (!status.ok()) {
    std::cerr << status;
    return 1;
  }
  auto* registry = arrow::compute::GetFunctionRegistry();
  auto names = registry->GetFunctionNames();
  std::sort(names.begin(), names.end());
  std::cout << "{\"arrow_version\":" << std::quoted(ARROW_VERSION_STRING)
            << ",\"functions\":[";
  bool first = true;
  for (const auto& name : names) {
    auto function = registry->GetFunction(name);
    if (!function.ok()) return 2;
    if (!first) std::cout << ',';
    first = false;
    std::cout << "{\"name\":" << std::quoted(name)
              << ",\"kind\":" << static_cast<int>((*function)->kind()) << '}';
  }
  std::cout << "]}\n";
}
