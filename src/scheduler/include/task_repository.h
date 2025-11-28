#pragma once

#include <string>
#include <vector>
#include <memory>
#include <pqxx/pqxx>

#include "database_pool.h"
#include "dts/internal/internal_service.pb.h"  // UpdateTaskStatusRequest
#include "dts/task/task.pb.h"                  // Task, TaskState

namespace dts {
namespace scheduler {

class TaskRepository {
 public:
  // 构造函数
  explicit TaskRepository(std::shared_ptr<dts::common::DatabasePool> db_pool);
  virtual ~TaskRepository();

  // -----------------------------------------------------
  // 核心 API
  // -----------------------------------------------------

  // 1. 处理任务完成 (核心 DAG 驱动)
  // 返回值: true 表示事务提交成功
  bool HandleTaskCompletion(
      const dts::internal::UpdateTaskStatusRequest* request);

  // 2. 拉取待调度的任务 (Polling)
  std::vector<dts::task::Task> GetPendingTasks(int limit);

  // 3. 抢占任务 (Optimistic Locking)
  // 尝试将任务从 PENDING -> RUNNING，并绑定 worker_id
  bool UpdateTaskToRunning(const std::string& task_id,
                           const std::string& worker_id);

  // 4. 回滚任务 (当 RPC 调用失败时)
  bool RevertTaskToPending(const std::string& task_id);

  // 5. 清理僵尸任务 (当 Worker 宕机时)
  // 返回被恢复的任务数量
  int RequeueOrphanedTasks(const std::string& dead_worker_id);

 private:
  std::shared_ptr<dts::common::DatabasePool> db_pool_;

  // -----------------------------------------------------
  // 内部事务逻辑 Helper
  // -----------------------------------------------------

  // DAG 后继节点触发
  bool PropagateSuccess(pqxx::work& tx, const std::string& parent_task_id);

  // 失败重试逻辑
  bool HandleRetry(pqxx::work& tx, const std::string& task_id);
};

}  // namespace scheduler
}  // namespace dts