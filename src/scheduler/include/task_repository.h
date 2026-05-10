#pragma once

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <pqxx/pqxx>

#include "database_pool.h"
#include "task.hpp"  // 引入业务对象 dts::Task (struct)

namespace dts {
namespace scheduler {

class TaskRepository {
 public:
  explicit TaskRepository(std::shared_ptr<dts::common::DatabasePool> db_pool);
  virtual ~TaskRepository();

  // -----------------------------------------------------
  // 核心 API
  // -----------------------------------------------------

  // 1. 处理任务失败 (包含重试逻辑)
  // 返回 true: 表示触发了重试 (调用者需要将任务重新推入 Redis)
  // 返回 false: 表示任务彻底失败 (无需操作)
  bool HandleTaskFailure(const std::string& task_id,
                         const std::string& error_msg,
                         int worker_reported_retry);

  // 2. 根据 ID 获取任务详情
  // 用于重试时获取任务 Payload 推送 Redis
  std::optional<dts::Task> GetTaskById(const std::string& task_id);

  // 3. 拉取待调度的任务 (Polling)
  // 此时主要供 Patrol 线程兜底使用
  std::vector<dts::Task> GetPendingTasks(int limit);

  // 4. 抢占任务 (Optimistic Locking)
  // 调度器分发前的最后一道防线
  bool UpdateTaskToRunning(const std::string& task_id,
                           const std::string& worker_id);

  // 5. 回滚任务
  bool RevertTaskToPending(const std::string& task_id);

  // 6. 清理僵尸任务
  int RequeueOrphanedTasks(const std::string& dead_worker_id);

 private:
  std::shared_ptr<dts::common::DatabasePool> db_pool_;
};

}  // namespace scheduler
}  // namespace dts