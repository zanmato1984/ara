#include <arrow/api.h>
#include <gtest/gtest.h>
#include <stdint.h>

class TaskStatus {
 public:
  static const TaskStatus runnable;
  static const TaskStatus waiting;
  static const TaskStatus finished;
  static const TaskStatus error;
  static const TaskStatus canceled;

  bool operator==(const TaskStatus& other) const { return internal_ == other.internal_; }

 private:
  enum class Internal : int8_t {
    RUNNABLE,
    WAITING,
    FINISHED,
    ERROR,
    CANCELED,
  } internal_;

  explicit TaskStatus(Internal internal) : internal_(internal) {}
};

const TaskStatus TaskStatus::runnable(TaskStatus::Internal::RUNNABLE);
const TaskStatus TaskStatus::waiting(TaskStatus::Internal::WAITING);
const TaskStatus TaskStatus::finished(TaskStatus::Internal::FINISHED);
const TaskStatus TaskStatus::error(TaskStatus::Internal::ERROR);
const TaskStatus TaskStatus::canceled(TaskStatus::Internal::CANCELED);

TEST(TaskTest, TaskStatusBasic) {
    TaskStatus s1(TaskStatus::runnable);
    ASSERT_EQ(s1, TaskStatus::runnable);
    ASSERT_NE(s1, TaskStatus::waiting);
    ASSERT_NE(&s1, &TaskStatus::runnable);

    TaskStatus s2 = TaskStatus::waiting;
    ASSERT_EQ(s2, TaskStatus::waiting);
    ASSERT_NE(s2, TaskStatus::runnable);
    ASSERT_NE(&s2, &TaskStatus::waiting);

    ASSERT_NE(s1, s2);
}