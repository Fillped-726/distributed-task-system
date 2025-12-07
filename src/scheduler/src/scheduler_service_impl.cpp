#include "scheduler_service_impl.h"

#include "worker_manager.h"
#include "task_repository.h"
#include "logger.hpp"
#include "dts/error/error.pb.h"
#include "dts/error/sys_error.pb.h"
#include "dts/task/task_state.grpc.pb.h"

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
  std::string task_id = request->task_id();
  dts::task::TaskState state = request->final_state();

  dts::SetRequestId("UPD-" + task_id);

  LOG_INFO << "[UpdateTaskStatus] Task: " << task_id
           << ", State: " << dts::task::TaskState_Name(state);

  if (task_id.empty()) return grpc::Status::OK;
  if (!task_repository_) {
    LOG_ERROR << "TaskRepository is null";
    return grpc::Status(grpc::INTERNAL, "Internal Server Error");
  }

  // ---------------------------------------------------------
  // 分支处理：成功 vs 失败
  // ---------------------------------------------------------
  if (state == dts::task::TaskState::SUCCESS) {
    // [关键] 成功分支：可能会触发后续任务
    // 我们假设 TaskRepository 有一个新方法: FinishTaskAndGetReadyChildren
    // 它负责原子性地更新 DB，并返回那些 "刚刚变成 PENDING" 的子任务
    std::vector<dts::Task> next_tasks =
        task_repository_->FinishTaskAndGetReadyChildren(
            task_id,
            request->result()  // 结果 JSON
        );

    // 如果有新任务就绪，推送到 Redis
    if (!next_tasks.empty()) {
      LOG_INFO << "Task " << task_id << " triggered " << next_tasks.size()
               << " next tasks.";

      auto& redis = RedisManager::GetInstance();
      int pushed_count = 0;

      for (const auto& task : next_tasks) {
        // 1. 序列化
        auto args_opt = TaskSerializer::ToXAddArgs(task);
        if (!args_opt) {
          LOG_ERROR << "Failed to serialize child task " << task.task_id;
          continue;
        }

        // 2. 推送 Redis Stream
        auto msg_id_opt = redis.XAdd(keys::stream::kTasks, *args_opt);
        if (msg_id_opt) {
          pushed_count++;
        } else {
          LOG_WARN << "Failed to push child task " << task.task_id
                   << " to Redis!";
        }
      }
      LOG_DEBUG << "Pushed " << pushed_count << " child tasks to Redis.";
    } else {
      LOG_DEBUG << "Task " << task_id
                << " finished, but no new tasks triggered.";
    }

  } else {
    // [关键] 失败分支：处理重试或标记失败
    // 调用旧的 UpdateStatus 方法即可，不需要触发子任务
    // 如果需要重试，TaskRepository 内部逻辑会将状态改回 PENDING 吗？
    // 如果是重试，你也需要在这里重新推 Redis！
    // 假设 Repository 的 HandleTaskFailure 返回 true 表示 "需要重试"

    bool need_retry = task_repository_->HandleTaskFailure(
        task_id, request->error_msg(),
        request->retry_count()  // Worker 汇报时可能带上当前的重试次数
    );

    if (need_retry) {
      // 如果决定重试，我们需要重新获取该任务的完整信息并推 Redis
      auto retry_task_opt = task_repository_->GetTaskById(task_id);
      if (retry_task_opt) {
        LOG_WARN << "Task " << task_id
                 << " failed but will retry. Pushing back to Redis.";
        auto args_opt = TaskSerializer::ToXAddArgs(*retry_task_opt);
        if (args_opt) {
          RedisManager::GetInstance().XAdd(keys::stream::kTasks, *args_opt);
        }
      }
    }
  }

  response->mutable_header();
  return grpc::Status::OK;
}

}  // namespace scheduler
}  // namespace dts