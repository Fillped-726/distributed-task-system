#pragma once

#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "dts/internal/internal_service.grpc.pb.h"
#include "dts/task/task_state.pb.h"
#include "task.hpp"

namespace dts {
namespace worker {

class SchedulerClient {
 public:
  // 构造函数：传入 channel 方便单元测试 mock 或复用
  explicit SchedulerClient(std::shared_ptr<grpc::Channel> channel);
  ~SchedulerClient() = default;

  // 1. 注册 Worker
  // 返回值: true 成功, false 失败
  bool RegisterWorker(const std::string& worker_id,
                      const std::string& ip_address);

  // 2. 发送心跳
  bool SendHeartbeat(const std::string& worker_id, int running_task_count,
                     float cpu_usage);

  // 3. 汇报任务状态
  bool UpdateTaskStatus(const std::string& task_id, dts::TaskState state,
                        const std::string& job_id,
                        const std::string& error_msg = "",
                        const std::string& result_json = "",
                        const std::string& worker_id = "");

 private:
  std::unique_ptr<dts::internal::SchedulerService::Stub> stub_;
};

}  // namespace worker
}  // namespace dts