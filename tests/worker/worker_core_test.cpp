#include <gtest/gtest.h>
#include "task_registry.h"
#include "thread_pool.h"
#include <future>
#include <vector>
#include <atomic>

using namespace dts::worker;
using dts::common::ThreadPool;

// ----------------------------------------------------------------
// 测试 1: TaskRegistry (注册表)
// ----------------------------------------------------------------
TEST(WorkerCoreTest, TaskRegistry_RegisterAndGet) {
  auto& registry = TaskRegistry::GetInstance();

  // 1. 注册一个测试函数
  std::string expected_output = "hello world";
  registry.Register("test_func",
                    [&](const std::string& params) { return expected_output; });

  // 2. 获取函数
  auto handler = registry.Get("test_func");
  ASSERT_TRUE(handler != nullptr);

  // 3. 执行验证
  EXPECT_EQ(handler("{}"), expected_output);

  // 4. 获取不存在的函数
  auto null_handler = registry.Get("non_existent_func");
  EXPECT_TRUE(null_handler == nullptr);
}

// ----------------------------------------------------------------
// 测试 2: ThreadPool (基本执行)
// ----------------------------------------------------------------
TEST(WorkerCoreTest, ThreadPool_ExecuteSimple) {
  ThreadPool pool(2);  // 2个线程

  // 使用 promise/future 获取异步结果
  std::promise<int> p;
  auto f = p.get_future();

  pool.enqueue([&p] { p.set_value(42); });

  // 等待结果 (带超时)
  EXPECT_EQ(f.wait_for(std::chrono::seconds(1)), std::future_status::ready);
  EXPECT_EQ(f.get(), 42);
}

// ----------------------------------------------------------------
// 测试 3: ThreadPool (并发性验证)
// ----------------------------------------------------------------
TEST(WorkerCoreTest, ThreadPool_ConcurrentExecution) {
  // 开启 4 个线程
  ThreadPool pool(4);
  std::atomic<int> counter{0};
  int task_count = 100;

  std::vector<std::future<void>> futures;

  for (int i = 0; i < task_count; ++i) {
    // enqueue 返回 future
    futures.emplace_back(pool.enqueue([&counter] {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));  // 模拟耗时
      counter++;
    }));
  }

  // 等待所有任务完成
  for (auto& fut : futures) {
    fut.wait();
  }

  EXPECT_EQ(counter.load(), task_count);
}

// ----------------------------------------------------------------
// 测试 4: ThreadPool (异常安全性 - 这一步极其重要！)
// ----------------------------------------------------------------
TEST(WorkerCoreTest, ThreadPool_ExceptionSafety) {
  ThreadPool pool(1);

  // 提交一个会抛出异常的任务
  auto future = pool.enqueue([] { throw std::runtime_error("Task Failed!"); });

  // 验证 future 能捕获到异常
  EXPECT_THROW(
      {
        try {
          future.get();
        } catch (const std::exception& e) {
          EXPECT_STREQ(e.what(), "Task Failed!");
          throw;  // 重新抛出以满足 EXPECT_THROW
        }
      },
      std::runtime_error);

  // 验证线程池是否还活着 (还能执行下一个任务)
  auto next_future = pool.enqueue([] { return 1; });
  EXPECT_EQ(next_future.get(), 1);
}