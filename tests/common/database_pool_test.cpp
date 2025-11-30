#include <gtest/gtest.h>
#include "database_pool.h"
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>

using dts::common::DatabasePool;

TEST(DatabasePoolTest, ConcurrencyAndBlocking) {
  const char* env_url = std::getenv("DATABASE_URL");
  std::string db_url =
      env_url ? env_url : "postgresql://sakura:password@localhost/dts";

  // 1. 创建一个小池子 (容量 2)
  DatabasePool pool(db_url, 2);

  std::atomic<int> active_connections{0};
  std::vector<std::thread> threads;

  // 2. 启动 4 个线程，每个都需要占用连接 500ms
  auto start_time = std::chrono::steady_clock::now();

  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([&]() {
      pool.ExecuteTx([&](pqxx::work& tx) {
        // 占用连接
        active_connections++;
        // 模拟 DB 操作耗时
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        // 简单查询保活
        tx.exec("SELECT 1");
        active_connections--;
      });
    });
  }

  for (auto& t : threads) t.join();
  auto end_time = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                      end_time - start_time)
                      .count();

  // 3. 验证
  // 因为池子大小是 2，线程有 4 个，耗时 500ms
  // 理论上必须分两批执行，总耗时应 >= 1000ms
  // 如果没有阻塞(并发了4个)，耗时会接近 500ms，那就错了
  EXPECT_GE(duration, 1000);
  EXPECT_EQ(active_connections.load(), 0);
}