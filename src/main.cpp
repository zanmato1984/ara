#include <arrow/compute/api.h>

namespace {
auto foo() {
  return arrow::compute::ExecContext();
}
}
