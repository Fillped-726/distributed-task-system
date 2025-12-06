#include "scheduler_client.h"
#include "utils/rpc_utils.h"
#include "logger.hpp"
#include "converters.hpp"

namespace dts {
namespace worker {

SchedulerClient::SchedulerClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(dts::internal::SchedulerService::NewStub(channel)) {}

bool SchedulerClient::RegisterWorker(const std::string& worker_id,
                                     const std::string& ip_address) {
  dts::internal::RegisterRequest request;
  request.set_worker_id(worker_id);
  request.set_address(ip_address);

  dts::internal::RegisterResponse response;
  grpc::ClientContext context;

  // 设置超时，防止注册卡死
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));

  LOG_INFO << "Registering worker: " << worker_id << " at " << ip_address;

  grpc::Status status = stub_->Register(&context, request, &response);

  // 使用我们封装的工具
  if (!dts::common::CheckRpcStatus(status, response, "RegisterWorker")) {
    return false;
  }

  LOG_INFO << "Worker registered successfully.";
  return true;
}

bool SchedulerClient::SendHeartbeat(const std::string& worker_id,
                                    int running_task_count, float cpu_usage) {
  dts::internal::HeartbeatRequest request;
  request.set_worker_id(worker_id);
  request.set_running_task_count(running_task_count);
  request.set_cpu_usage_percent(cpu_usage);

  dts::internal::HeartbeatResponse response;
  grpc::ClientContext context;

  // 心跳通常超时时间短一些
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(2));

  grpc::Status status = stub_->Heartbeat(&context, request, &response);

  // 心跳失败通常不需要 Fatal，记个 Warn 即可，所以这里 CheckRpcStatus 内部打
  // Error 也没问题
  return dts::common::CheckRpcStatus(status, response, "SendHeartbeat");
}

bool SchedulerClient::UpdateTaskStatus(const std::string& task_id,
                                       dts::TaskState state,
                                       const std::string& error_msg,
                                       const std::string& result_json) {
  dts::internal::UpdateTaskStatusRequest request;
  request.set_task_id(task_id);
  request.set_final_state(static_cast<dts::task::TaskState>(state));
  request.set_error_msg(error_msg);
  request.set_result_json(result_json);

  dts::internal::UpdateTaskStatusResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));

  // 重要：确保 RPC 调用链追踪
  // 这里的 req_id 应该是当前线程正在处理的任务 ID (通过 thread_local t_req_id
  // 获取)
  if (!dts::t_req_id.empty()) {
    context.AddMetadata("x-request-id", dts::t_req_id);
  }

  LOG_INFO << "Updating task " << task_id << " status to "
           << dts::task::TaskState_Name(
                  static_cast<dts::task::TaskState>(state));

  grpc::Status status = stub_->UpdateTaskStatus(&context, request, &response);

  return dts::common::CheckRpcStatus(status, response, "UpdateTaskStatus");
}

}  // namespace worker
}  // namespace dts