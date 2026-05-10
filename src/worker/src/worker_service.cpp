#include "worker_service.h"
#include "logger.hpp"
#include "converters.hpp"
#include "dts/error/sys_error.pb.h"

namespace dts {
namespace worker {

WorkerServiceImpl::WorkerServiceImpl(TaskSubmitter submitter)
    : submitter_(std::move(submitter)) {}

grpc::Status WorkerServiceImpl::RunTask(
    grpc::ServerContext* context, const dts::internal::RunTaskRequest* request,
    dts::internal::RunTaskResponse* response) {
  auto client_metadata = context->client_metadata();
  auto it = client_metadata.find("x-request-id");  // Key 必须全小写

  std::string current_trace_id;
  if (it != client_metadata.end()) {
    // 拿到 Scheduler 传过来的 ID
    current_trace_id = std::string(it->second.data(), it->second.length());
  } else {
    // 如果没有（比如手动调用），生成一个新的，确保链路不断
    current_trace_id = "Worker-Gen-" + std::to_string(std::rand());
  }
  dts::SetRequestId(current_trace_id);

  // 1. 基本校验
  if (!request->has_task()) {
    auto err = response->mutable_header()->mutable_error();
    err->set_sys(dts::error::SysErr::SYS_INVALID_PARAM);
    err->set_msg("RunTaskRequest missing task field");
    LOG_ERROR << "Received RunTaskRequest without task payload.";
    return grpc::Status::OK;  // RPC 本身是成功的，只是业务失败
  }

  const auto& pb_task = request->task();
  LOG_INFO << "Received RunTask Request: task_id=" << pb_task.task_id()
           << " name=" << pb_task.natural_id();

  // 2. 将任务递交给上层 (WorkerNode) 处理
  // 注意：这里是异步的，WorkerNode 会把它扔进线程池，不会阻塞这里
  try {
    dts::Task cpp_task = dts::TaskFromProto(pb_task);

    // 3. 提交给 WorkerNode (此时传递的是纯净的 C++ 对象)
    if (submitter_) {
      submitter_(cpp_task);
    } else {
      LOG_ERROR << "No submitter registered!";
    }

    // 4. 响应
    response->mutable_header()->set_request_id(pb_task.task_id());

  } catch (const std::exception& e) {
    LOG_ERROR << "Failed to convert task from proto: " << e.what();
    // 这里可以返回错误给 Scheduler
  }

  // 3. 立即返回成功响应
  // 这里的 header error 为空，代表“接收成功”
  response->mutable_header()->set_request_id(
      pb_task.task_id());  // 回填 ID 方便追踪

  return grpc::Status::OK;
}

}  // namespace worker
}  // namespace dts