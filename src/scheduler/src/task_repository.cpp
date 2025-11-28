#include "task_repository.h"

#include <iostream>
#include <exception>
#include <nlohmann/json.hpp>

#include "logger.hpp"
#include "converters.hpp"

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
// 1. GetPendingTasks (拉取)
// -----------------------------------------------------
std::vector<dts::task::Task> TaskRepository::GetPendingTasks(int limit) {
  std::vector<dts::task::Task> tasks;

  try {
    db_pool_->ExecuteTx([&](pqxx::work& tx) {
      // 按优先级降序，提交时间升序 (先提交的高优先级任务先跑)
      std::string sql = R"(
                SELECT task_id, job_id, natural_id, func_name, func_params, 
                       priority, max_retry, retry_count, timeout_ms 
                FROM task 
                WHERE state = $1 
                ORDER BY priority DESC, submit_ts ASC 
                LIMIT $2;
            )";

      // 直接转 int，简单粗暴且安全
      pqxx::result r =
          tx.exec_params(sql, static_cast<int>(dts::task::PENDING), limit);

      for (auto row : r) {
        dts::task::Task t;
        t.set_task_id(row["task_id"].as<std::string>());
        t.set_job_id(row["job_id"].as<std::string>());
        t.set_func_name(row["func_name"].as<std::string>());
        t.set_priority(row["priority"].as<uint32_t>());
        t.set_timeout_ms(row["timeout_ms"].as<uint32_t>());
        t.set_max_retry(row["max_retry"].as<uint32_t>());
        t.set_retry_count(row["retry_count"].as<uint32_t>());
        t.set_state(dts::task::PENDING);

        // JSON -> Proto Struct 转换
        if (!row["func_params"].is_null()) {
          try {
            std::string params_str = row["func_params"].as<std::string>();
            if (!params_str.empty()) {
              auto j = nlohmann::json::parse(params_str);
              // 调用你的工具函数
              dts::JsonToStruct(j, t.mutable_func_params());
            }
          } catch (const std::exception& e) {
            LOG_ERROR << "JSON parse error for task " << t.task_id() << ": "
                      << e.what();
          }
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
// 2. UpdateTaskToRunning (抢占 - 乐观锁)
// -----------------------------------------------------
bool TaskRepository::UpdateTaskToRunning(const std::string& task_id,
                                         const std::string& worker_id) {
  bool updated = false;
  try {
    db_pool_->ExecuteTx([&](pqxx::work& tx) {
      std::string sql = R"(
                UPDATE task 
                SET state = $1, 
                    worker_id = $2, 
                    start_ts = EXTRACT(EPOCH FROM (NOW()))::BIGINT 
                WHERE task_id = $3
                AND state = $4; -- 关键：CAS (Compare And Swap)
            )";

      pqxx::result r =
          tx.exec_params(sql, static_cast<int>(dts::task::RUNNING), worker_id,
                         task_id, static_cast<int>(dts::task::PENDING));

      updated = (r.affected_rows() == 1);
    });
    return updated;
  } catch (const std::exception& e) {
    LOG_ERROR << "[TaskRepo] UpdateTaskToRunning failed: " << e.what();
    return false;
  }
}

// -----------------------------------------------------
// 3. HandleTaskCompletion (DAG 驱动)
// -----------------------------------------------------
bool TaskRepository::HandleTaskCompletion(
    const dts::internal::UpdateTaskStatusRequest* request) {
  try {
    db_pool_->ExecuteTx([&](pqxx::work& tx) {
      auto final_state = request->final_state();
      auto task_id = request->task_id();

      // 1. 更新当前任务
      // 注意：这里我们把 result_json (string) 存入 result (jsonb)
      // PostgreSQL 会自动处理 string 到 jsonb 的转换，只要 string 格式合法
      std::string sql_update = R"(
                UPDATE task 
                SET state = $1, result = $2::jsonb, error_msg = $3, 
                    finish_ts = EXTRACT(EPOCH FROM (NOW()))::BIGINT
                WHERE task_id = $4;
            )";

      // 处理 result 可能为空的情况
      std::string result_json =
          request->result_json().empty() ? "{}" : request->result_json();

      tx.exec_params(sql_update, static_cast<int>(final_state), result_json,
                     request->error_msg(), task_id);

      // 2. 状态流转
      if (final_state == dts::task::SUCCESS) {
        PropagateSuccess(tx, task_id);
      } else if (final_state == dts::task::FAILED ||
                 final_state == dts::task::TIMEOUT) {
        HandleRetry(tx, task_id);
      }
    });
    return true;
  } catch (const std::exception& e) {
    LOG_ERROR << "[TaskRepo] HandleTaskCompletion failed for task "
              << request->task_id() << ": " << e.what();
    return false;
  }
}

// -----------------------------------------------------
// 4. PropagateSuccess (DAG 核心 - 原子递减)
// -----------------------------------------------------
bool TaskRepository::PropagateSuccess(pqxx::work& tx,
                                      const std::string& parent_task_id) {
  // 查出所有子任务
  std::string sql_find_children =
      "SELECT child_task_id FROM task_edge WHERE parent_task_id = $1";
  pqxx::result children = tx.exec_params(sql_find_children, parent_task_id);

  if (children.empty()) return true;

  LOG_INFO << "Propagating success from " << parent_task_id << " to "
           << children.size() << " children.";

  for (auto row : children) {
    std::string child_id = row[0].as<std::string>();

    // 原子递减，并返回新的值 (RETURNING 是 PG 的杀手级特性)
    std::string sql_decrement = R"(
            UPDATE task 
            SET pending_dependencies = pending_dependencies - 1 
            WHERE task_id = $1 AND state = $2
            RETURNING pending_dependencies;
        )";

    pqxx::result res = tx.exec_params(
        sql_decrement, child_id, static_cast<int>(dts::task::WAITING_DEPS));

    if (res.empty()) {
      // 这种情况可能发生：比如 child 已经被取消了，或者逻辑删除了
      continue;
    }

    int remaining_deps = res[0][0].as<int>();
    if (remaining_deps == 0) {
      // 依赖全部满足，变为 PENDING，等待调度
      LOG_INFO << "Task " << child_id << " is now READY (dependencies met).";
      std::string sql_ready = "UPDATE task SET state = $1 WHERE task_id = $2";
      tx.exec_params(sql_ready, static_cast<int>(dts::task::PENDING), child_id);
    }
  }
  return true;
}

// -----------------------------------------------------
// 5. HandleRetry (悲观锁 FOR UPDATE)
// -----------------------------------------------------
bool TaskRepository::HandleRetry(pqxx::work& tx, const std::string& task_id) {
  // 锁定该行，防止并发修改
  std::string sql_check =
      "SELECT retry_count, max_retry FROM task WHERE task_id = $1 FOR UPDATE";
  pqxx::result res = tx.exec_params(sql_check, task_id);

  if (res.empty()) return false;

  int retry_count = res[0]["retry_count"].as<int>();
  int max_retry = res[0]["max_retry"].as<int>();

  if (retry_count < max_retry) {
    LOG_WARN << "Retrying task " << task_id << " (" << retry_count + 1 << "/"
             << max_retry << ")";

    std::string sql_retry = R"(
            UPDATE task 
            SET state = $1, retry_count = retry_count + 1, 
                worker_id = NULL, start_ts = NULL, finish_ts = NULL, 
                error_msg = NULL, result = NULL
            WHERE task_id = $2;
        )";
    tx.exec_params(sql_retry, static_cast<int>(dts::task::PENDING), task_id);
  } else {
    LOG_ERROR << "Task " << task_id << " failed permanently after " << max_retry
              << " retries.";
    // 状态已经在 HandleTaskCompletion 里被设为 FAILED 了，这里不需要再改
  }
  return true;
}

// -----------------------------------------------------
// 6. 辅助功能
// -----------------------------------------------------
bool TaskRepository::RevertTaskToPending(const std::string& task_id) {
  try {
    db_pool_->ExecuteTx([&](pqxx::work& tx) {
      tx.exec_params(
          "UPDATE task SET state = $1, worker_id = NULL, start_ts = NULL WHERE "
          "task_id = $2",
          static_cast<int>(dts::task::PENDING), task_id);
    });
    return true;
  } catch (...) {
    return false;
  }
}

int TaskRepository::RequeueOrphanedTasks(const std::string& dead_worker_id) {
  int count = 0;
  try {
    db_pool_->ExecuteTx([&](pqxx::work& tx) {
      pqxx::result r = tx.exec_params(
          "UPDATE task SET state=$1, worker_id=NULL, start_ts=NULL WHERE "
          "worker_id=$2 AND state=$3",
          static_cast<int>(dts::task::PENDING), dead_worker_id,
          static_cast<int>(dts::task::RUNNING));
      count = r.affected_rows();
    });
    if (count > 0)
      LOG_WARN << "Requeued " << count << " orphaned tasks from "
               << dead_worker_id;
  } catch (...) {
  }
  return count;
}

}  // namespace scheduler
}  // namespace dts