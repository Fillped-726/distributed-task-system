#include "task_repository.h"

#include <iostream>
#include <exception>
#include <nlohmann/json.hpp>

#include "logger.hpp"
// #include "converters.hpp" // 如果不再依赖 Proto 转换，可以移除

namespace dts {
namespace scheduler {

TaskRepository::TaskRepository(
    std::shared_ptr<dts::common::DatabasePool> db_pool)
    : db_pool_(db_pool) {
  if (db_pool_ == nullptr) {
    LOG_FATAL << "TaskRepository initialized with null DatabasePool";
  }
  LOG_INFO << "TaskRepository initialized.";
}

TaskRepository::~TaskRepository() {}

// -----------------------------------------------------
// 1. HandleTaskFailure (失败处理 & 重试判定)
// -----------------------------------------------------
bool TaskRepository::HandleTaskFailure(const std::string& task_id,
                                       const std::string& error_msg,
                                       int worker_reported_retry) {
  bool retrying = false;
  try {
    db_pool_->ExecuteTx([&](pqxx::work& tx) {
      // 1. 悲观锁锁定任务 (防止并发)
      std::string sql_check =
          "SELECT retry_count, max_retry FROM task WHERE task_id = $1 FOR "
          "UPDATE";
      pqxx::result r = tx.exec_params(sql_check, task_id);

      if (r.empty()) return;  // 任务不存在？

      int current_retry = r[0]["retry_count"].as<int>();
      int max_retry = r[0]["max_retry"].as<int>();

      // 2. 判断是否重试
      if (current_retry < max_retry) {
        LOG_WARN << "Retrying task " << task_id << " (" << current_retry + 1
                 << "/" << max_retry << "). Error: " << error_msg;

        // 重置状态为 PENDING，清空执行信息
        std::string sql_retry = R"(
            UPDATE task 
            SET state = 0, -- PENDING
                retry_count = retry_count + 1,
                worker_id = NULL, start_ts = NULL, finish_ts = NULL, 
                error_msg = $2
            WHERE task_id = $1
        )";
        tx.exec_params(sql_retry, task_id, error_msg);
        retrying = true;  // 告知上层推 Redis
      } else {
        LOG_ERROR << "Task " << task_id
                  << " failed permanently. Error: " << error_msg;

        // 标记为 FAILED
        std::string sql_fail = R"(
            UPDATE task 
            SET state = 3, -- FAILED
                finish_ts = EXTRACT(EPOCH FROM NOW())::BIGINT,
                error_msg = $2
            WHERE task_id = $1
        )";
        tx.exec_params(sql_fail, task_id, error_msg);
        retrying = false;
      }
    });
  } catch (const std::exception& e) {
    LOG_ERROR << "[TaskRepo] HandleTaskFailure exception: " << e.what();
    return false;
  }
  return retrying;
}

// -----------------------------------------------------
// 2. GetTaskById (用于重试时捞取数据)
// -----------------------------------------------------
std::optional<dts::Task> TaskRepository::GetTaskById(
    const std::string& task_id) {
  try {
    std::optional<dts::Task> result_task;

    db_pool_->ExecuteTx([&](pqxx::work& tx) {
      std::string sql = "SELECT * FROM task WHERE task_id = $1";
      pqxx::result r = tx.exec_params(sql, task_id);

      if (!r.empty()) {
        const auto& row = r[0];
        dts::Task t;
        t.task_id = row["task_id"].as<std::string>();
        t.job_id = row["job_id"].as<std::string>();
        t.natural_id = row["natural_id"].as<std::string>();  // 之前可能漏了
        t.func_name = row["func_name"].as<std::string>();
        t.priority = row["priority"].as<uint32_t>();
        t.max_retry = row["max_retry"].as<uint32_t>();
        t.retry_count =
            row["retry_count"].as<uint32_t>();  // 注意：这是 DB 里的最新值
        t.timeout_ms = row["timeout_ms"].as<uint32_t>();

        // 解析 Params
        if (!row["func_params"].is_null()) {
          auto params_str = row["func_params"].as<std::string>();
          if (!params_str.empty()) {
            t.func_params = nlohmann::json::parse(params_str);
          }
        }

        // 其他字段按需填充...
        result_task = std::move(t);
      }
    });

    return result_task;
  } catch (const std::exception& e) {
    LOG_ERROR << "[TaskRepo] GetTaskById failed: " << e.what();
    return std::nullopt;
  }
}

// -----------------------------------------------------
// 3. GetPendingTasks (保留用于 Patrol/兜底)
// -----------------------------------------------------
std::vector<dts::Task> TaskRepository::GetPendingTasks(int limit) {
  std::vector<dts::Task> tasks;
  try {
    db_pool_->ExecuteTx([&](pqxx::work& tx) {
      std::string sql = R"(
            SELECT * FROM task 
            WHERE state = 0 -- PENDING
            ORDER BY priority DESC, submit_ts ASC 
            LIMIT $1
      )";
      pqxx::result r = tx.exec_params(sql, limit);

      for (auto row : r) {
        dts::Task t;
        t.task_id = row["task_id"].as<std::string>();
        t.job_id = row["job_id"].as<std::string>();
        t.func_name = row["func_name"].as<std::string>();
        t.priority = row["priority"].as<uint32_t>();
        t.timeout_ms = row["timeout_ms"].as<uint32_t>();

        if (!row["func_params"].is_null()) {
          auto s = row["func_params"].as<std::string>();
          if (!s.empty()) t.func_params = nlohmann::json::parse(s);
        }

        tasks.push_back(std::move(t));
      }
    });
  } catch (const std::exception& e) {
    LOG_ERROR << "[TaskRepo] GetPendingTasks failed: " << e.what();
  }
  return tasks;
}

// -----------------------------------------------------
// 4. UpdateTaskToRunning (保留 - 乐观锁)
// -----------------------------------------------------
bool TaskRepository::UpdateTaskToRunning(const std::string& task_id,
                                         const std::string& worker_id) {
  bool updated = false;
  try {
    db_pool_->ExecuteTx([&](pqxx::work& tx) {
      std::string sql = R"(
            UPDATE task 
            SET state = 1, -- RUNNING 
                worker_id = $1, 
                start_ts = EXTRACT(EPOCH FROM (NOW()))::BIGINT 
            WHERE task_id = $2 AND state = 0 -- PENDING
      )";
      pqxx::result r = tx.exec_params(sql, worker_id, task_id);
      updated = (r.affected_rows() == 1);
    });
  } catch (const std::exception& e) {
    LOG_ERROR << "[TaskRepo] UpdateTaskToRunning failed: " << e.what();
  }
  return updated;
}

// -----------------------------------------------------
// 5. RevertTaskToPending (保留)
// -----------------------------------------------------
bool TaskRepository::RevertTaskToPending(const std::string& task_id) {
  try {
    db_pool_->ExecuteTx([&](pqxx::work& tx) {
      tx.exec_params(
          "UPDATE task SET state = 0, worker_id = NULL, start_ts = NULL WHERE "
          "task_id = $1",
          task_id);
    });
    return true;
  } catch (...) {
    return false;
  }
}

// -----------------------------------------------------
// 6. RequeueOrphanedTasks (保留)
// -----------------------------------------------------
int TaskRepository::RequeueOrphanedTasks(const std::string& dead_worker_id) {
  int count = 0;
  try {
    db_pool_->ExecuteTx([&](pqxx::work& tx) {
      pqxx::result r = tx.exec_params(
          "UPDATE task SET state=0, worker_id=NULL, start_ts=NULL WHERE "
          "worker_id=$1 AND state=1",
          dead_worker_id);
      count = r.affected_rows();
    });
  } catch (...) {
  }
  return count;
}

}  // namespace scheduler
}  // namespace dts