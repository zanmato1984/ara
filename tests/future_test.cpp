#include <folly/futures/Future.h>
#include <gtest/gtest.h>

using namespace folly;

TEST(FutureTest, Basic) {
  {
    std::vector<Future<Unit>> fs;
    for (int i = 0; i < 10; i++) {
      fs.push_back(makeFuture());
    }

    collectAllUnsafe(fs).thenValue(
        [&](std::vector<Try<Unit>> ts) { EXPECT_EQ(fs.size(), ts.size()); });
  }
}