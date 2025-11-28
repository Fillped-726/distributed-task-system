#include "scheduler_service_impl.h"

#include "worker_manager.h"
#include "task_repository.h"
#include "logger.hpp"
#include "dts/error/error.pb.h"
#include "dts/error/sys_error.pb.h"

namespace dts {
namespace scheduler {

// -----------------------------------------------------
// 构造函数
// -----------------------------------------------------
SchedulerServiceImpl::SchedulerServiceImpl(
    std::shared_ptr<WorkerManager> worker_manager,
    std::shared_ptr<TaskRepository> task_repository)
    : worker_manager_(worker_manager), task_repository_(task_repository) {
  if (!worker_manager_) {
    LOG_FATAL << "SchedulerServiceImpl initialized with null WorkerManager!";
  }
  // 暂时允许 task_repository 为空，方便你分步调试，但最终上线必须不为空
  if (!task_repository_) {
    LOG_WARN << "SchedulerServiceImpl initialized with null TaskRepository "
                "(DAG features disabled)";
  }

  LOG_INFO << "SchedulerServiceImpl started.";
}

SchedulerServiceImpl::~SchedulerServiceImpl() {
  LOG_INFO << "SchedulerServiceImpl stopped.";
}

// -----------------------------------------------------
// 1. 实现 Register
// -----------------------------------------------------
grpc::Status SchedulerServiceImpl::Register(
    grpc::ServerContext* context, const dts::internal::RegisterRequest* request,
    dts::internal::RegisterResponse* response) {
  // 设置 RequestID (如果有拦截器更好，没有的话这里手动设也没问题)
  dts::SetRequestId("REG-" + request->worker_id());

  LOG_INFO << "[Register] Worker registering from: " << request->address();

  // 1. 参数校验
  if (request->worker_id().empty() || request->address().empty()) {
    auto* err = response->mutable_header()->mutable_error();
    err->set_sys(dts::error::SYS_INVALID_PARAM);
    err->set_msg("worker_id or address is empty");
    LOG_WARN << "[Register] Failed: Empty params";
    return grpc::Status::OK;
  }

  // 2. 转交 WorkerManager
  worker_manager_->HandleRegister(request->worker_id(), request->address());

  // 3. 成功响应
  response->mutable_header();  // 默认无 error
  return grpc::Status::OK;
}

// -----------------------------------------------------
// 2. 实现 Heartbeat
// -----------------------------------------------------
grpc::Status SchedulerServiceImpl::Heartbeat(
    grpc::ServerContext* context,
    const dts::internal::HeartbeatRequest* request,
    dts::internal::HeartbeatResponse* response) {
  // 心跳太频繁，不要每次都打印。
  // 使用 glog 的 LOG_EVERY_N，每收到 100 次心跳才打印一条日志
  LOG_EVERY_N(INFO, 100) << "[Heartbeat] Sampled log from "
                         << request->worker_id();

  // 1. 转交 WorkerManager
  bool success = worker_manager_->HandleHeartbeat(request);

  // 2. 处理“游离 Worker”
  if (!success) {
    // 这是一个严重问题，Worker 以为自己活着，但 Master 早就把它踢了
    // 我们需要告诉 Worker："你已经死了，请重新注册"
    LOG_WARN << "[Heartbeat] Rejected unknown worker: " << request->worker_id();

    auto* err = response->mutable_header()->mutable_error();
    // 这里用 SYS_Internal 或者自定义一个 RE_REGISTER 错误码
    err->set_sys(dts::error::SYS_INVALID_PARAM);
    err->set_msg("Worker not found. Please re-register.");

    return grpc::Status::OK;
  }

  response->mutable_header();
  return grpc::Status::OK;
}

// -----------------------------------------------------
// 3. 实现 UpdateTaskStatus (核心 DAG 驱动)
// -----------------------------------------------------
grpc::Status SchedulerServiceImpl::UpdateTaskStatus(
    grpc::ServerContext* context,
    const dts::internal::UpdateTaskStatusRequest* request,
    dts::internal::UpdateTaskStatusResponse* response) {
  dts::SetRequestId("UPD-" + request->task_id());

  // 这里非常关键：Worker 汇报任务结束，可能是 SUCCESS，也可能是 FAILED
  LOG_INFO << "[UpdateTaskStatus] Task: " << request->task_id()
           << ", State: " << dts::task::TaskState_Name(request->final_state());

  if (request->task_id().empty()) {
    return grpc::Status::OK;
  }

  // 防御性编程
  if (!task_repository_) {
    LOG_ERROR << "TaskRepository is null, cannot process task completion";
    return grpc::Status(grpc::INTERNAL, "Internal Server Error");
  }

  // 2. 委托给 TaskRepository 处理 DAG 状态流转
  // 如果任务成功，Repository 需要找到后续任务，将 pending_deps - 1
  // 如果减到 0，Repository 需要把任务状态改为 PENDING 并推入队列
  bool success = task_repository_->HandleTaskCompletion(request);

  if (!success) {
    LOG_ERROR << "[UpdateTaskStatus] Failed to update DAG state for task "
              << request->task_id();
    // 这里的失败通常意味着数据库挂了
  }

  response->mutable_header();
  return grpc::Status::OK;
}

}  // namespace scheduler
}  // namespace dts