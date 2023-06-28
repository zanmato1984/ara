#include <arrow/acero/exec_plan.h>
#include <arrow/acero/query_context.h>
#include <arrow/compute/exec.h>
#include <gtest/gtest.h>

#include "sketch_hash_join.h"

using namespace arra;

class TestHashJoinFineGrained : public ::testing::Test {
 public:
  void SetUp() override {
    query_ctx_ = std::make_unique<arrow::acero::QueryContext>(
        arrow::acero::QueryOptions{}, arrow::compute::ExecContext());
  }

  void InitHashJoin(size_t dop, const arra::detail::HashJoinNodeOptions& options,
                    const arrow::Schema& left_schema, const arrow::Schema& right_schema) {
    dop_ = dop;
    ASSERT_TRUE(query_ctx_->Init(dop, nullptr).ok());

    // auto l_schema = arrow::schema(
    //     {arrow::field("l_i32", arrow::int32()), arrow::field("l_str", arrow::utf8())});
    // auto r_schema = arrow::schema(
    //     {arrow::field("r_str", arrow::utf8()), arrow::field("r_i32", arrow::int32())});
    ASSERT_TRUE(
        hash_join_.Init(query_ctx_.get(), dop, options, left_schema, right_schema).ok());
  }

 private:
  size_t dop_;
  std::unique_ptr<arrow::acero::QueryContext> query_ctx_;
  HashJoin hash_join_;
};

// class TestTaskRunner {
//   using Task = std::function<arrow::Status(size_t, int64_t)>;
//   using TaskCont = std::function<arrow::Status(size_t)>;

//   TestTaskRunner(size_t num_threads)
//       : scheduler(arrow::acero::TaskScheduler::Make()),
//         thread_pool(*arrow::internal::ThreadPool::Make(num_threads)) {}

//   void RegisterEnd() { scheduler->RegisterEnd(); }

//   arrow::Status StartScheduling(size_t dop) {
//     return scheduler->StartScheduling(
//         thread_id(),
//         [&](std::function<arrow::Status(size_t)> func) {
//           return thread_pool->Spawn([&, func]() { ARROW_DCHECK_OK(func(thread_id()));
//           });
//         },
//         dop, false);
//   }

//   int RegisterTaskGroup(Task task, TaskCont task_cont) {
//     return scheduler->RegisterTaskGroup(std::move(task), std::move(task_cont));
//   }

//   arrow::Status StartTaskGroup(int task_group_id, int64_t num_tasks) {
//     return scheduler->StartTaskGroup(thread_id(), task_group_id, num_tasks);
//   }

//   void WaitForIdle() { thread_pool->WaitForIdle(); }

//  private:
//   arrow::acero::ThreadIndexer thread_id;
//   std::unique_ptr<arrow::acero::TaskScheduler> scheduler;
//   std::shared_ptr<arrow::internal::ThreadPool> thread_pool;
// };

// class TestDriver {
//  public:
//   TestDriver(HashJoin* hash_join, TestTaskRunner* runner)
//       : hash_join_(hash_join), runner_(runner) {}

//  private:
//   HashJoin* hash_join_;
//   TestTaskRunner* runner_;
// };

// class TestHashJoin : public ::testing::Test {
//  private:
//   HashJoin hash_join_;
//   TestTaskRunner runner_;
//   TestDriver driver_;
// };