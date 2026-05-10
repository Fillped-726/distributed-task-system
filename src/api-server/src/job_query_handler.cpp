#include <sstream>
#include <algorithm>
#include <charconv>

#include "job_query_handler.hpp"
#include "redis/RedisManager.hpp"
#include "redis/RedisKeys.hpp"
#include "logger.hpp"
#include "error/head_error.h"

namespace dts::api_server {

using dts::common::RedisManager;
using dts::error::JobErr;
using dts::task::JobState;
using dts::task::TaskState;
namespace keys = dts::common;

int SafeStringToInt(std::string_view sv, int default_val = 0) {
  if (sv.empty()) return default_val;
  int result = 0;
  auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), result);
  if (ec == std::errc()) {
    return result;
  }
  return default_val;
}

void JobQueryHandler::Handle(std::shared_ptr<dts::common::DatabasePool> db_conn,
                             PbGetJobStatusRequest* req,
                             PbGetJobStatusResponse* resp) {
  std::string job_id = req->job_id();
  resp->set_job_id(job_id);

  // ==================================================
  // Step 1: 获取 Job 包含的所有 Task ID
  // ==================================================
  std::vector<std::string> task_ids;
  bool hit_in_redis = GetTaskIdsFromRedis(job_id, task_ids);

  if (!hit_in_redis || task_ids.empty()) {
    // Redis 未命中或为空，尝试查 DB 兜底
    LOG(INFO) << "[JobQuery] Cache miss for job map: " << job_id
              << ", checking DB...";
    GetTaskIdsFromDB(db_conn, job_id, task_ids);
  }

  if (task_ids.empty()) {
    // DB 也没有，说明 Job 不存在
    common::SetJobError(resp, JobErr::JOB_NOT_FOUND,
                        "Job not found: " + job_id);
    return;
  }

  resp->set_total_tasks(task_ids.size());

  // ==================================================
  // Step 2: 获取 Task 详情 (Redis Pipeline -> DB Fallback)
  // ==================================================
  if (req->include_tasks()) {
    FetchTaskDetails(db_conn, task_ids, true, resp);
  } else {
    // 如果不需要详情，我们至少需要状态来计算 JobState
    // 为了简化，这里复用 FetchTaskDetails，但可以选择不回填 payload 以节省带宽
    FetchTaskDetails(db_conn, task_ids, false, resp);
  }

  // ==================================================
  // Step 3: 聚合计算 Job 状态
  // ==================================================
  int success_cnt = 0;
  int failed_cnt = 0;
  int running_cnt = 0;
  int pending_cnt = 0;

  for (const auto& task : resp->task_details()) {
    switch (task.state()) {
      case TaskState::SUCCESS:
        success_cnt++;
        break;
      case TaskState::FAILED:
      case TaskState::TIMEOUT:
      case TaskState::CANCELLED:
        failed_cnt++;
        break;  // 广义失败
      case TaskState::RUNNING:
        running_cnt++;
        break;
      default:
        pending_cnt++;
        break;  // PENDING, WAITING_DEPS
    }
  }

  resp->set_finished_tasks(success_cnt + failed_cnt);

  if (failed_cnt > 0) {
    // 1. 只要有一个失败，整个 Job 视为失败 (Fail-Fast 策略，或者根据业务改为
    // Partial)
    resp->set_state(JobState::JOB_FAILED);
  } else if (running_cnt > 0) {
    // 2. 只要有一个在跑，整体就是在跑
    resp->set_state(JobState::JOB_RUNNING);
  } else if (success_cnt == task_ids.size()) {
    // 3. 全部成功
    resp->set_state(JobState::JOB_SUCCESS);
  } else if (pending_cnt == task_ids.size()) {
    // 4. [Point 4] 全部 Pending，整体才算 Pending
    resp->set_state(JobState::JOB_PENDING);
  } else {
    // 5. 剩下的情况：部分 Success + 部分 Pending (无 Failed/Running)
    // 这种情况通常也被视为 RUNNING (进行中)
    resp->set_state(JobState::JOB_RUNNING);
  }
}

bool JobQueryHandler::GetTaskIdsFromRedis(
    const std::string& job_id, std::vector<std::string>& out_task_ids) {
  // 假设你在 SubmitDag 时写入了 Set: dts:job:tasks:{job_id}
  // 这里使用简单的 GetConnection，因为只是一个 SMEMBERS
  // 更好的做法是封装一个 RedisManager::SMembers
  try {
    auto& redis = dts::common::RedisManager::GetInstance().GetConnection();
    std::string key = keys::state::JobTasks(job_id);
    redis.smembers(key, std::back_inserter(out_task_ids));
    return !out_task_ids.empty();
  } catch (const std::exception& e) {
    LOG(WARNING) << "[JobQuery] Redis SMEMBERS failed: " << e.what();
    return false;
  }
}

void JobQueryHandler::GetTaskIdsFromDB(
    std::shared_ptr<dts::common::DatabasePool> db, const std::string& job_id,
    std::vector<std::string>& out_task_ids) {
  db->ExecuteTx([&](pqxx::work& tx) {
    // 假设表名为 dts_tasks
    std::string sql = "SELECT task_id FROM task WHERE job_id = $1";
    pqxx::result r = tx.exec_params(sql, job_id);
    for (const auto& row : r) {
      out_task_ids.push_back(row["task_id"].as<std::string>());
    }
  });
}

void JobQueryHandler::FetchTaskDetails(
    std::shared_ptr<dts::common::DatabasePool> db,
    const std::vector<std::string>& task_ids, bool need_payload,
    PbGetJobStatusResponse* resp) {
  auto& redis_mgr = RedisManager::GetInstance();

  // 1. Pipeline 批量查询
  // ------------------------------------------------
  std::vector<std::string> missed_task_ids;

  // 我们需要按顺序对应结果，所以先预分配 response 数组
  // 但因为 Pipeline 结果也是有序的，我们直接遍历处理即可

  auto pipe_results = redis_mgr.ExecPipeline([&](sw::redis::Pipeline& pipe) {
    for (const auto& tid : task_ids) {
      // HGETALL dts:task:runtime:{tid}
      pipe.hgetall(keys::state::TaskRuntime(tid));
    }
  });

  if (!pipe_results) {
    // Redis 挂了，全量回源 DB
    LOG(ERROR) << "[JobQuery] Pipeline failed, fallback to DB for all tasks";
    missed_task_ids = task_ids;  // 全部标记为丢失
  } else {
    // 2. 解析 Pipeline 结果
    // ------------------------------------------------
    auto& replies = pipe_results.value();
    std::unordered_map<std::string, std::string> hash_val;

    for (size_t i = 0; i < task_ids.size(); ++i) {
      hash_val.clear();  // 清空内容，但保留预留空间

      // 2. 使用 std::inserter 修复编译错误
      replies.get(i, std::inserter(hash_val, hash_val.end()));

      if (hash_val.empty()) {
        missed_task_ids.push_back(task_ids[i]);
        continue;  // 提前跳过，减少缩进
      }

      auto* detail = resp->add_task_details();
      detail->set_task_id(task_ids[i]);

      // 3. 使用 find() 优化查询性能，避免两次查找
      auto it = hash_val.find("state");
      if (it != hash_val.end()) {
        detail->set_state(static_cast<TaskState>(SafeStringToInt(it->second)));
      }

      if ((it = hash_val.find("worker_id")) != hash_val.end()) {
        detail->set_worker_id(it->second);
      }

      if ((it = hash_val.find("error")) != hash_val.end()) {
        detail->set_error_message(it->second);
      }

      if ((it = hash_val.find("retries")) != hash_val.end()) {
        detail->set_retry_count(SafeStringToInt(it->second));
      }

      if (need_payload && (it = hash_val.find("result")) != hash_val.end()) {
        detail->set_result_payload(it->second);
      }
    }
  }

  // 3. DB 补漏查询 (Bulk Query)
  // ------------------------------------------------
  if (!missed_task_ids.empty()) {
    LOG(INFO) << "[JobQuery] Fetching " << missed_task_ids.size()
              << " tasks from DB";

    try {
      db->ExecuteTx([&](pqxx::work& tx) {
        std::string sql =
            "SELECT task_id, state, worker_id, error_msg, result_payload, "
            "retry_count "
            "FROM task WHERE task_id = ANY($1)";

        pqxx::result r = tx.exec_params(sql, missed_task_ids);
        for (const auto& row : r) {
          auto* detail = resp->add_task_details();
          detail->set_task_id(row["task_id"].as<std::string>());

          // [Point 6] 使用 .as<type>(default) 优雅处理默认值
          int state_int = row["state"].as<int>(0);
          detail->set_state(static_cast<TaskState>(state_int));

          // [Point 6] 使用 .as<std::optional<std::string>>() 优雅处理 NULL
          auto worker_id_opt =
              row["worker_id"].as<std::optional<std::string>>();
          if (worker_id_opt) detail->set_worker_id(*worker_id_opt);

          auto error_opt = row["error_msg"].as<std::optional<std::string>>();
          if (error_opt) detail->set_error_message(*error_opt);

          detail->set_retry_count(row["retry_count"].as<int>(0));

          // [Point 3] 二进制安全：.as<std::string>() 会正确构造带长度的
          // string，不会被 \0 截断
          if (need_payload) {
            auto payload_opt =
                row["result_payload"].as<std::optional<std::string>>();
            if (payload_opt) detail->set_result_payload(*payload_opt);
          }
        }
      });
    } catch (const std::exception& e) {
      LOG(ERROR) << "[JobQuery] DB Fallback failed: " << e.what();
      // DB 也挂了，这些任务只能标记为 UNKNOWN 或者不返回
    }
  }
}

}  // namespace dts::api_server