#include <pqxx/pqxx>
#include <iostream>
#include "task_submitter.hpp"
#include "task.hpp"
#include <stdexcept>
#include <sstream>
#include <map>
#include <chrono>
#include <memory_resource>
#include "uuid_generator.hpp"
#include <array>
#include <vector>

#include "logger.hpp"
#include "redis/RedisManager.hpp"
#include "redis/RedisKeys.hpp"
#include "utils/TaskSerializer.hpp"
#include "uuid_generator.hpp"
#include "utils/utils.hpp"

namespace dts::api_server {

using RedisManager = dts::common::redis::RedisManager;
using TaskSerializer = dts::common::utils::TaskSerializer;
namespace keys = dts::common::redis::keys;

bool TaskSubmitter::handleSubmitDag(dts::SubmitDagRequest& request,
                                    pqxx::work& tx) {
  LOG_INFO << "Starting DAG submission. ClientID: " << request.client_id;

  // -----------------------------------------------------------------
  // 步骤 1: 计算依赖计数
  // -----------------------------------------------------------------
  std::map<std::string, int> dependency_count;
  for (const auto& edge : request.edges) {
    dependency_count[edge.child_natural_id]++;
  }

  auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();

  // -----------------------------------------------------------------
  // 步骤 2: 插入 Job (幂等性检查)
  // -----------------------------------------------------------------
  try {
    request.job_id = dts::common::generate();

    std::string job_insert_sql =
        "INSERT INTO public.job (job_id, idempotency_key, state) VALUES (" +
        tx.quote(request.job_id) + ", " + tx.quote(request.idempotency_key) +
        ", " + "'0'" +
        ") ON CONFLICT (idempotency_key) DO NOTHING RETURNING job_id;";

    pqxx::result job_res = tx.exec(job_insert_sql);

    if (job_res.empty()) {
      LOG_ERROR << "[LOG] 幂等性冲突 (重复请求): " << request.idempotency_key;
      return true;
    }

    // -----------------------------------------------------------------
    // 步骤 4: "两遍循环 + Map 映射" (逻辑不变)
    // -----------------------------------------------------------------

    std::array<std::byte, 32 * 1024> buffer;
    std::pmr::monotonic_buffer_resource pool{buffer.data(), buffer.size()};

    std::pmr::map<std::pmr::string, std::pmr::string, std::less<>>
        natural_to_uuid_map{&pool};

    std::vector<dts::Task*> tasks_to_push;

    // -----------------------------------------------------------------
    // 步骤 4a: 第一次循环 (插入 Task)
    // -----------------------------------------------------------------
    if (request.tasks.empty()) {
      throw std::runtime_error("提交的 tasks 列表不能为空");
    }

    std::stringstream task_insert_sql;
    task_insert_sql << "INSERT INTO public.task "
                    << "(task_id, job_id, natural_id, func_name, func_params, "
                    << "priority, state, pending_dependencies, "
                    << "max_retry, retry_count, timeout_ms, submit_ts) "
                    << "VALUES ";

    for (size_t i = 0; i < request.tasks.size(); ++i) {
      auto& task = request.tasks[i];

      // 1. 生成并回填 UUID
      task.task_id = dts::common::generate();
      task.job_id = request.job_id;  // 确保 task 里有 job_id

      // 2. 建立映射
      natural_to_uuid_map.emplace(task.natural_id, task.task_id);

      // 3. 计算状态
      int pending_count = dependency_count[task.natural_id];

      // 确定初始状态 (0=PENDING, 6=WAITING_DEPS)
      // 假设你的枚举里 PENDING 是 0, WAITING_DEPS 是 6
      int initial_state = (pending_count == 0) ? 0 : 6;

      // 4.如果是入口任务，加入待推送列表
      if (pending_count == 0) {
        tasks_to_push.push_back(&task);
      }

      // 5. 构建 SQL
      task_insert_sql << "(" << tx.quote(task.task_id) << ", "
                      << tx.quote(request.job_id) << ", "
                      << tx.quote(task.natural_id) << ", "
                      << tx.quote(task.func_name)
                      << ", "

                      // [修复] 使用 .dump() 将 JSON 对象序列化为字符串
                      << tx.quote(task.func_params.dump()) << ", "

                      << task.priority << ", " << initial_state << ", "
                      << pending_count << ", " << task.max_retry << ", "
                      << task.retry_count << ", " << task.timeout_ms << ", "
                      << "EXTRACT(EPOCH FROM NOW())::BIGINT"
                      << ")";

      if (i < request.tasks.size() - 1) task_insert_sql << ", ";
    }
    task_insert_sql << ";";

    LOG_INFO << "[DB] " << task_insert_sql.str();
    tx.exec(task_insert_sql.str());

    // -----------------------------------------------------------------
    // 步骤 4b: 第二次循环 (插入 Edge)
    // -----------------------------------------------------------------
    if (!request.edges.empty()) {
      std::stringstream edge_insert_sql;
      edge_insert_sql << "INSERT INTO public.task_edge (parent_task_id, "
                         "child_task_id) VALUES ";

      for (size_t i = 0; i < request.edges.size(); ++i) {
        const auto& edge = request.edges[i];

        if (natural_to_uuid_map.find(edge.parent_natural_id.c_str()) ==
                natural_to_uuid_map.end() ||
            natural_to_uuid_map.find(edge.child_natural_id.c_str()) ==
                natural_to_uuid_map.end()) {
          throw std::runtime_error(
              "Edge definition references unknown natural_id");
        }

        const auto& parent_uuid =
            natural_to_uuid_map.at(edge.parent_natural_id.c_str());
        const auto& child_uuid =
            natural_to_uuid_map.at(edge.child_natural_id.c_str());

        edge_insert_sql << "(" << tx.quote(std::string_view(parent_uuid))
                        << ", " << tx.quote(std::string_view(child_uuid))
                        << ")";
        if (i < request.edges.size() - 1) edge_insert_sql << ", ";
      }
      edge_insert_sql << ";";

      LOG_INFO << "[DB] " << edge_insert_sql.str();
      tx.exec(edge_insert_sql.str());
    }

    // -----------------------------------------------------------------
    // 步骤 5: Redis 双写 (Push to Stream)
    // -----------------------------------------------------------------
    // 此时 DB 语句已执行但未 Commit。
    // 我们乐观地推送到 Redis。
    // *注意*: Worker 可能会在 main commit 之前从 Redis 拿到任务。
    // 解决办法: Worker 查 DB 如果查不到，应重试而不是报错。

    auto& redis = RedisManager::GetInstance();
    int pushed_count = 0;

    for (dts::Task* task_ptr : tasks_to_push) {
      if (!task_ptr) continue;

      // A. 序列化 (Task -> Proto -> Redis Args)
      auto args_opt = TaskSerializer::ToXAddArgs(*task_ptr);
      if (!args_opt) {
        LOG_ERROR << "Failed to serialize task " << task_ptr->task_id
                  << ", skipping Redis push.";
        continue;  // 软故障，依靠 Rescue 线程兜底
      }

      // B. 推送 XADD
      auto msg_id_opt = redis.XAdd(keys::stream::kTasks, *args_opt);

      if (msg_id_opt) {
        pushed_count++;
        LOG_DEBUG << "Pushed task to Redis. ID: " << task_ptr->task_id
                  << ", StreamID: " << *msg_id_opt;
      } else {
        LOG_WARN << "Failed to push task to Redis (Infrastructure error). ID: "
                 << task_ptr->task_id;
      }
    }

    LOG_INFO << "DAG Submitted successfully. JobID: " << request.job_id
             << ", Tasks: " << request.tasks.size()
             << ", Pushed to Queue: " << pushed_count;

    return true;

  } catch (const std::exception& e) {
    LOG_ERROR << "Exception handling DAG submission: " << e.what();
    // tx 会在 main 中析构时自动 rollback
    return false;
  }
}

}  // namespace dts::api_server