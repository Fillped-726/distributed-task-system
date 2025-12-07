#pragma once

#include <thread>
#include <atomic>
#include <memory>
#include <map>
#include <mutex>
#include <vector>

#include "task_repository.h"
#include "worker_manager.h"

// Redis & Utils
#include "redis/RedisManager.hpp"
#include "utils/TaskSerializer.hpp"
#include "task.hpp"

// gRPC 依赖
#include <grpcpp/grpcpp.h>
#include "dts/internal/internal_service.grpc.pb.h"

#include "thread_pool.h"

namespace dts {
namespace scheduler {
using RedisManager = dts::common::redis::RedisManager;

class SchedulerLoop {
 public:
  SchedulerLoop(std::shared_ptr<TaskRepository> task_repo,
                std::shared_ptr<WorkerManager> worker_manager);

  ~SchedulerLoop();

  void Start();
  void Stop();

 private:
  void RunLoop();

  // 处理单条 Stream 消息的通用逻辑
  void ProcessStreamEntry(const RedisManager::StreamEntry& entry);

  // 救援逻辑：处理长时间未ACK的消息
  void DoRescue();

  // 真正的派发逻辑
  void DoDispatch(const dts::Task& task, const WorkerInfo& worker);

  std::shared_ptr<dts::internal::WorkerService::Stub> GetWorkerStub(
      const std::string& address);

  // -----------------------------------------------------
  // 成员变量
  // -----------------------------------------------------
  std::shared_ptr<TaskRepository> task_repo_;
  std::shared_ptr<WorkerManager> worker_manager_;

  std::mutex stub_cache_mtx_;
  std::map<std::string, std::shared_ptr<grpc::Channel>> channel_cache_;

  std::thread loop_thread_;
  std::atomic<bool> stop_flag_;

  std::unique_ptr<dts::common::ThreadPool> dispatch_pool_;

  std::string consumer_name_;
};

}  // namespace scheduler
}  // namespace dts