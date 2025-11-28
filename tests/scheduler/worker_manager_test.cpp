#include <gtest/gtest.h>
#include "worker_manager.h"
#include "dts/internal/internal_service.pb.h"
#include <thread>
#include <vector>

using namespace dts::scheduler;

class WorkerManagerTest : public ::testing::Test {
 protected:
  // 在测试开始前，我们创建一个超时时间很短的 Manager (比如 1秒)
  // 这样测试 PruneDeadWorkers 时不用等太久
  // 如果你没改构造函数，这里就只能用默认构造: WorkerManager manager;
  WorkerManager manager{std::chrono::seconds(1)};
};

// 测试 1: 注册与心跳的基本流程
TEST_F(WorkerManagerTest, RegisterAndHeartbeat) {
  std::string worker_id = "worker-01";
  std::string address = "127.0.0.1:5000";

  // 1. 注册
  manager.HandleRegister(worker_id, address);

  // 验证：注册后应该能查到 (通过 GetAvailableWorkersSorted 间接验证)
  auto workers = manager.GetAvailableWorkersSorted();
  ASSERT_EQ(workers.size(), 1);
  EXPECT_EQ(workers[0].worker_id, worker_id);
  EXPECT_EQ(workers[0].address, address);

  // 2. 心跳 (成功情况)
  dts::internal::HeartbeatRequest req;
  req.set_worker_id(worker_id);
  req.set_running_task_count(5);  // 假设正在跑5个任务

  bool success = manager.HandleHeartbeat(&req);
  EXPECT_TRUE(success) << "Registered worker heartbeat should return true";

  // 验证状态更新
  workers = manager.GetAvailableWorkersSorted();
  EXPECT_EQ(workers[0].running_task_count, 5)
      << "Running task count should update";

  // 3. 心跳 (失败情况 - 未知的 Worker)
  dts::internal::HeartbeatRequest unknown_req;
  unknown_req.set_worker_id("ghost-worker");

  bool fail = manager.HandleHeartbeat(&unknown_req);
  EXPECT_FALSE(fail) << "Unknown worker heartbeat should return false";
}

// 测试 2: 负载均衡排序 (GetAvailableWorkersSorted)
// 这是调度器最核心的逻辑之一，必须测准
TEST_F(WorkerManagerTest, LoadBalancingSort) {
  // 注册 3 个 Worker
  manager.HandleRegister("w1", "addr1");
  manager.HandleRegister("w2", "addr2");
  manager.HandleRegister("w3", "addr3");

  // 模拟心跳，上报不同的负载
  // w1: 很忙 (10个任务)
  dts::internal::HeartbeatRequest r1;
  r1.set_worker_id("w1");
  r1.set_running_task_count(10);
  manager.HandleHeartbeat(&r1);

  // w2: 很闲 (0个任务)
  dts::internal::HeartbeatRequest r2;
  r2.set_worker_id("w2");
  r2.set_running_task_count(0);
  manager.HandleHeartbeat(&r2);

  // w3: 一般 (5个任务)
  dts::internal::HeartbeatRequest r3;
  r3.set_worker_id("w3");
  r3.set_running_task_count(5);
  manager.HandleHeartbeat(&r3);

  // 获取排序后的列表
  auto sorted_workers = manager.GetAvailableWorkersSorted();

  ASSERT_EQ(sorted_workers.size(), 3);

  // 期望顺序：w2 (0) -> w3 (5) -> w1 (10) (升序)
  EXPECT_EQ(sorted_workers[0].worker_id, "w2");
  EXPECT_EQ(sorted_workers[0].running_task_count, 0);

  EXPECT_EQ(sorted_workers[1].worker_id, "w3");
  EXPECT_EQ(sorted_workers[1].running_task_count, 5);

  EXPECT_EQ(sorted_workers[2].worker_id, "w1");
  EXPECT_EQ(sorted_workers[2].running_task_count, 10);
}

// 测试 3: 死节点剔除 (PruneDeadWorkers)
TEST_F(WorkerManagerTest, PruneDeadWorkers) {
  // 注册一个 worker
  manager.HandleRegister("dead-worker", "addr");

  // 立即检查：不应该被剔除 (刚注册，认为是活的)
  auto dead_list = manager.PruneDeadWorkers();
  EXPECT_TRUE(dead_list.empty());

  // 等待 1.1 秒 (因为我们在 Setup 里设置超时为 1 秒)
  // 如果你没改构造函数，这里可能要等很久，或者无法测试超时
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));

  // 再次检查：应该超时被剔除
  dead_list = manager.PruneDeadWorkers();
  ASSERT_EQ(dead_list.size(), 1);
  EXPECT_EQ(dead_list[0], "dead-worker");

  // 再次获取列表，应该是空的
  auto workers = manager.GetAvailableWorkersSorted();
  EXPECT_TRUE(workers.empty());
}