#include "op/op_output.h"

#include <gtest/gtest.h>
#include <iostream>

using namespace ara::pipeline;

void foo() {
  OpOutput o1 = OpOutput::Cancelled(), o2 = OpOutput::Cancelled();
  if (o1 == o2) {
    std::cout << "a1 == a2" << std::endl;
  }
}
