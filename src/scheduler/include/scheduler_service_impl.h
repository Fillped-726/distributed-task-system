#pragma once

#include <memory>
#include <string>

// gRPC 生成文件
#include "dts/internal/internal_service.grpc.pb.h"

// 领域对象前置声明
namespace dts {
namespace scheduler {
class WorkerManager;
class TaskRepository;
}  // namespace scheduler
}  // namespace dts

namespace dts {
namespace scheduler {

class SchedulerServiceImpl final
    : public dts::internal::SchedulerService::Service {
 public:
  // 构造函数：依赖注入
  SchedulerServiceImpl(std::shared_ptr<WorkerManager> worker_manager,
                       std::shared_ptr<TaskRepository> task_repository);

  virtual ~SchedulerServiceImpl();

  // -----------------------------------------------------
  // gRPC 接口实现
  // -----------------------------------------------------

  // 1. Worker 启动注册
  grpc::Status Register(grpc::ServerContext* context,
                        const dts::internal::RegisterRequest* request,
                        dts::internal::RegisterResponse* response) override;

  // 2. Worker 心跳保活
  grpc::Status Heartbeat(grpc::ServerContext* context,
                         const dts::internal::HeartbeatRequest* request,
                         dts::internal::HeartbeatResponse* response) override;

  // 3. 任务完成回调 (核心 DAG 触发器)
  grpc::Status UpdateTaskStatus(
      grpc::ServerContext* context,
      const dts::internal::UpdateTaskStatusRequest* request,
      dts::internal::UpdateTaskStatusResponse* response) override;

 private:
  std::shared_ptr<WorkerManager> worker_manager_;
  std::shared_ptr<TaskRepository> task_repository_;
};

}  // namespace scheduler
}  // namespace dts