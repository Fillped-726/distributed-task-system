#include "scheduler_service_impl.h"

#include "worker_manager.h"
#include "task_repository.h"
#include "db_batcher.hpp"
#include "logger.hpp"
#include "dts/error/error.pb.h"
#include "dts/error/sys_error.pb.h"
#include "dts/task/task_state.grpc.pb.h"

#include "redis/RedisManager.hpp"
#include "redis/RedisKeys.hpp"
#include "utils/TaskSerializer.hpp"
#include "redis/scripts/complete_task.hpp"

using dts::common::RedisManager;
using dts::common::utils::TaskSerializer;

namespace keys = dts::common;

namespace {

std::string g_complete_task_sha;

void EnsureScriptLoaded() {
  // 双重检查锁定 (Double-Checked Locking) 的简化版，或者在单线程初始化时调用
  if (g_complete_task_sha.empty()) {
    try {
      LOG_INFO << "Loading embedded Lua script to Redis...";

      // 直接传入内存中的字符串 kCompleteTaskScript
      g_complete_task_sha =
          RedisManager::GetInstance().LoadScript(keys::kCompleteTaskScript);

      LOG_INFO << "Lua script loaded successfully. SHA: "
               << g_complete_task_sha;

    } catch (const std::exception& e) {
      // 这是一个致命错误：如果脚本加载不进 Redis，任务调度无法进行
      LOG_FATAL << "Critical Error: Failed to load embedded Lua script: "
                << e.what();

      // 强烈建议这里直接终止进程，而不是让程序带着残缺的状态跑下去
      throw;
    }
  }
}

}  // namespace

namespace dts {
namespace scheduler {

// -----------------------------------------------------
// 构造函数
// -----------------------------------------------------
SchedulerServiceImpl::SchedulerServiceImpl(
    std::shared_ptr<WorkerManager> worker_manager,
    std::shared_ptr<TaskRepository> task_repository,
    std::shared_ptr<DbBatcher> db_batcher)
    : worker_manager_(worker_manager),
      task_repository_(task_repository),
      db_batcher_(db_batcher) {
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

  std::string job_id = request->job_id();

  dts::SetRequestId("UPD-" + task_id);

  LOG_INFO << "[UpdateTaskStatus] Task: " << task_id << ", Job: " << job_id
           << ", State: " << dts::task::TaskState_Name(state);

  if (task_id.empty()) return grpc::Status::OK;

  // ---------------------------------------------------------
  // 分支 1：任务成功 (走 Redis Lua + 异步落盘)
  // ---------------------------------------------------------
  if (state == dts::task::TaskState::SUCCESS) {
    std::vector<std::string> keys = {
        keys::dag::Children(task_id),       // KEYS[1]: 子任务集合
        keys::dag::DependencyHash(job_id),  // KEYS[2]: 依赖计数表
        std::string(keys::stream::kTasks),  // KEYS[3]: 任务队列
        std::string(keys::stream::kErrors)  // KEYS[4]: 错误队列
    };
    std::vector<std::string> args = {job_id};  // ARGV[1]

    // 1. [极速] 执行 Lua 脚本，触发 DAG
    EnsureScriptLoaded();

    // 这一步耗时通常 < 1ms
    auto triggered_opt =
        RedisManager::GetInstance().EvalSha(g_complete_task_sha, keys, args);

    if (triggered_opt) {
      LOG_INFO << "Lua execution success. Triggered " << *triggered_opt
               << " next tasks.";
    } else {
      // 如果 Lua 失败（极罕见），为了安全，可以 fallback 到 DB 逻辑，
      // 或者直接报错等待重试。这里简单处理：只记录日志。
      LOG_ERROR << "Lua execution failed for task " << task_id;
    }

    // 2. [异步] 放入内存队列，等待批量写入 DB
    // 替代了原来的 task_repository_->FinishTaskAndGetReadyChildren
    if (db_batcher_) {
      db_batcher_->AddStatusUpdate(task_id, state, request->result_json(),
                                   request->error_msg(), request->worker_id());
    } else {
      LOG_ERROR << "DbBatcher is null! Status update might be lost.";
    }

  }
  // ---------------------------------------------------------
  // 分支 2：任务失败 (走 DB 同步处理 + 重试逻辑)
  // ---------------------------------------------------------
  else {
    if (!task_repository_) {
      return grpc::Status(grpc::INTERNAL, "TaskRepo missing");
    }

    // 1. DB 层面处理失败逻辑 (增加 retry_count, 判断是否需要重试)
    bool need_retry = task_repository_->HandleTaskFailure(
        task_id, request->error_msg(), request->retry_count());

    if (need_retry) {
      LOG_WARN << "Task " << task_id
               << " failed, retrying... Pushing back to Redis.";

      // 2. 获取 Job ID
      auto retry_task_opt = task_repository_->GetTaskById(task_id);

      if (retry_task_opt) {
        const auto& task = *retry_task_opt;

        // 3. [核心修改] 使用新的轻量级接口
        // 不需要序列化 Payload，只需要 ID 和 JobID
        auto args = TaskSerializer::ToXAddArgs(task.task_id, task.job_id);

        // 4. 推入 Redis Stream (作为指针)
        // Worker 抢到后，会拿着 ID 去 Redis Meta 里查静态参数，
        // 拿着 retry_count (从 DB 获取) 决定是否继续。
        RedisManager::GetInstance().XAdd(keys::stream::kTasks, args);

      } else {
        LOG_ERROR << "Critical: Task needed retry but not found in DB: "
                  << task_id;
      }
    } else {
      // 彻底失败 (Exceeded max_retry)
      // DB 状态已经是 FAILED，不需要额外操作
      LOG_ERROR << "Task " << task_id << " failed permanently after retries.";
    }
  }

  response->mutable_header();
  return grpc::Status::OK;
}

}  // namespace scheduler
}  // namespace dts