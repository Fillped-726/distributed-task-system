#pragma once

// === Standard Library Includes ===
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// === Third-Party Includes ===
#include <grpcpp/grpcpp.h>

// === Project Includes ===
#include "dts/internal/internal_service.grpc.pb.h"
#include "redis/RedisManager.hpp"
#include "task.hpp"
#include "task_repository.h"
#include "thread_pool.h"
#include "utils/TaskSerializer.hpp"
#include "worker_manager.h"

namespace dts::scheduler {

using RedisManager = dts::common::RedisManager;
using dts::common::StreamEntry;

/**
 * @class SchedulerLoop
 * @brief 调度器核心循环。
 *
 * 职责：
 * 1. 消费 Redis Stream 中的 Pending 任务。
 * 2. 维护 Worker 的负载均衡。
 * 3. 将任务通过 gRPC 分发给 Worker。
 * 4. 监控任务超时并执行救援 (Rescue)。
 */
class SchedulerLoop {
 public:
  SchedulerLoop(std::shared_ptr<TaskRepository> task_repo,
                std::shared_ptr<WorkerManager> worker_manager);

  ~SchedulerLoop();

  void Start();
  void Stop();

 private:
  // 主循环函数
  void RunLoop();

  // 处理救援回来的单条消息 (Rescue 流程复用)
  void ProcessStreamEntry(const StreamEntry& entry);

  // 救援逻辑：处理长时间未 ACK 的消息 (Zombie Tasks)
  void DoRescue();

  // 真正的派发逻辑 (发送 RPC)
  void DoDispatch(const dts::Task& task, const WorkerInfo& worker);

  // 获取或创建 gRPC Stub (带缓存)
  std::shared_ptr<dts::internal::WorkerService::Stub> GetWorkerStub(
      const std::string& address);

 private:
  // =========================================================
  // 依赖组件
  // =========================================================
  std::shared_ptr<TaskRepository> task_repo_;
  std::shared_ptr<WorkerManager> worker_manager_;
  std::unique_ptr<dts::common::ThreadPool> dispatch_pool_;

  // =========================================================
  // 运行状态与并发控制
  // =========================================================
  std::thread loop_thread_;
  std::atomic<bool> stop_flag_{false};
  std::string consumer_name_;

  // gRPC 连接缓存
  std::mutex stub_cache_mtx_;
  std::map<std::string, std::shared_ptr<grpc::Channel>> channel_cache_;

  // 背压控制 (Backpressure)
  // 用于限制同时处理的任务数，防止调度器内存爆炸
  std::atomic<int> inflight_tasks_{0};
  const int MAX_INFLIGHT = 2000;
};

}  // namespace dts::scheduler