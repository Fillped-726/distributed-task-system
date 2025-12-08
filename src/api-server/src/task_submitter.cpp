#include <pqxx/pqxx>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <map>
#include <chrono>
#include <memory_resource>
#include <array>
#include <vector>
#include <sw/redis++/redis++.h>

#include "task_submitter.hpp"
#include "uuid_generator.hpp"
#include "task.hpp"
#include "logger.hpp"
#include "redis/RedisManager.hpp"
#include "redis/RedisKeys.hpp"
#include "utils/TaskSerializer.hpp"
#include "uuid_generator.hpp"
#include "converters.hpp"
#include "dts/task/task.pb.h"

namespace dts::api_server {

// 简化命名空间引用
using RedisManager = dts::common::redis::RedisManager;
using TaskSerializer = dts::common::utils::TaskSerializer;
namespace keys = dts::common::redis::keys;

bool TaskSubmitter::handleSubmitDag(dts::SubmitDagRequest& request,
                                    pqxx::work& tx) {
  LOG_INFO << "Starting DAG submission. ClientID: " << request.client_id;

  // 1. 计算依赖计数
  std::map<std::string, int> dependency_count;
  for (const auto& edge : request.edges) {
    dependency_count[edge.child_natural_id]++;
  }

  // 2. 插入 Job (幂等性检查)
  try {
    request.job_id = dts::common::generate();

    std::string job_insert_sql =
        "INSERT INTO public.job (job_id, idempotency_key, state) VALUES (" +
        tx.quote(request.job_id) + ", " + tx.quote(request.idempotency_key) +
        ", " +
        "'0') ON CONFLICT (idempotency_key) DO NOTHING RETURNING job_id;";

    pqxx::result job_res = tx.exec(job_insert_sql);

    if (job_res.empty()) {
      LOG_WARN << "Idempotency conflict (Duplicate Job): "
               << request.idempotency_key;
      return true;
    }

    // 3. 准备数据 & 插入 DB Task 表
    if (request.tasks.empty()) return false;

    std::array<std::byte, 64 * 1024> buffer;  // 稍微加大 buffer
    std::pmr::monotonic_buffer_resource pool{buffer.data(), buffer.size()};
    std::pmr::map<std::pmr::string, std::pmr::string, std::less<>>
        natural_to_uuid_map{&pool};

    // 用于收集入口任务 (可以直接执行的任务)
    std::vector<dts::Task*> entry_tasks;

    std::stringstream task_insert_sql;
    task_insert_sql << "INSERT INTO public.task "
                    << "(task_id, job_id, natural_id, func_name, func_params, "
                    << "priority, state, pending_dependencies, "
                    << "max_retry, retry_count, timeout_ms, submit_ts) VALUES ";

    for (size_t i = 0; i < request.tasks.size(); ++i) {
      auto& task = request.tasks[i];

      // 生成并回填 UUID
      task.task_id = dts::common::generate();
      task.job_id = request.job_id;
      natural_to_uuid_map.emplace(task.natural_id, task.task_id);

      int pending_count = dependency_count[task.natural_id];
      int initial_state =
          (pending_count == 0) ? 0 : 6;  // 0=PENDING, 6=WAITING_DEPS

      task.state = static_cast<dts::TaskState>(initial_state);
      task.pending_dependencies = pending_count;

      if (pending_count == 0) {
        entry_tasks.push_back(&task);
      }

      // 构建 SQL (略去具体字段拼接，保持你原有的逻辑即可，确保 params dump
      // 正确)
      task_insert_sql << "(" << tx.quote(task.task_id) << ", "
                      << tx.quote(request.job_id) << ", "
                      << tx.quote(task.natural_id) << ", "
                      << tx.quote(task.func_name) << ", "
                      << tx.quote(task.func_params.dump()) << ", "
                      << task.priority << ", " << initial_state << ", "
                      << pending_count << ", " << task.max_retry << ", "
                      << task.retry_count << ", " << task.timeout_ms << ", "
                      << "EXTRACT(EPOCH FROM NOW())::BIGINT)";

      if (i < request.tasks.size() - 1) task_insert_sql << ", ";
    }
    task_insert_sql << ";";
    tx.exec(task_insert_sql.str());  // Execute Task Insert

    // 4. 插入 DB Edge 表
    if (!request.edges.empty()) {
      std::stringstream edge_insert_sql;
      edge_insert_sql << "INSERT INTO public.task_edge (parent_task_id, "
                         "child_task_id) VALUES ";

      for (size_t i = 0; i < request.edges.size(); ++i) {
        const auto& edge = request.edges[i];
        // 安全检查
        if (natural_to_uuid_map.find(edge.parent_natural_id.c_str()) ==
                natural_to_uuid_map.end() ||
            natural_to_uuid_map.find(edge.child_natural_id.c_str()) ==
                natural_to_uuid_map.end()) {
          throw std::runtime_error("Edge references unknown task");
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
      tx.exec(edge_insert_sql.str());  // Execute Edge Insert
    }

    // =========================================================================
    // 5. Redis DAG 初始化 (Pipeline 批量写入) [核心改动]
    // =========================================================================
    // 此时 DB 写入无异常，开始构建 Redis 运行时状态

    auto& redis_manager = RedisManager::GetInstance();
    auto& redis_client =
        redis_manager
            .GetConnection();  // 获取原始 redis++ client 以使用 pipeline
    auto pipe = redis_client.pipeline();

    // A. 铺设 Meta 数据和依赖计数
    for (const auto& task : request.tasks) {
      // 1. 序列化 Task -> Protobuf Binary (供 Lua 搬运)
      dts::task::Task proto_task;
      dts::TaskToProto(task, &proto_task);  // 转换
      std::string binary_payload;
      if (proto_task.SerializeToString(&binary_payload)) {
        // SET dts:task:meta:{id} <binary> EX 86400
        // 设置 24h 过期，防止内存泄漏 (Job 结束后理论上应该手动清理，这里兜底)
        pipe.set(keys::dag::TaskMeta(task.task_id), binary_payload,
                 std::chrono::hours(24));
      }

      // 2. 如果有依赖，写入 Hash 计数器
      // HSET dts:dag:deps:{job_id} {task_id} {count}
      if (task.pending_dependencies > 0) {
        pipe.hset(keys::dag::DependencyHash(task.job_id), task.task_id,
                  std::to_string(task.pending_dependencies));

        // 给 Hash 也设置个过期时间 (注意：Redis Hash 设置过期是针对 Key
        // 的，不是 Field) 我们只需要对整个 job 的 Hash 设置一次即可，这里多次
        // expire 也没关系，开销很小
        pipe.expire(keys::dag::DependencyHash(task.job_id),
                    std::chrono::hours(24));
      }
    }

    // B. 铺设父子关系 (Edges)
    for (const auto& edge : request.edges) {
      const auto& parent_id =
          natural_to_uuid_map.at(edge.parent_natural_id.c_str());
      const auto& child_id =
          natural_to_uuid_map.at(edge.child_natural_id.c_str());

      // SADD dts:dag:children:{parent_id} {child_id}
      std::string children_key = keys::dag::Children(parent_id);
      pipe.sadd(children_key, child_id);
      pipe.expire(children_key, std::chrono::hours(24));
    }

    // C. 执行 Pipeline
    try {
      pipe.exec();
      // LOG_INFO << "Redis DAG structures initialized.";
    } catch (const std::exception& e) {
      LOG_ERROR << "Failed to write DAG to Redis: " << e.what();
      // 这里是否要回滚？
      // 策略：返回 false，让 main 回滚 DB。保持强一致性。
      // 否则 DB 有任务，Redis 没 DAG，Lua 脚本跑不通，任务链会断。
      return false;
    }

    // =========================================================================
    // 6. 触发第一波任务 (Entry Tasks -> Stream)
    // =========================================================================
    int pushed_count = 0;
    for (dts::Task* task_ptr : entry_tasks) {
      // 生成 XADD 参数
      auto args_opt = TaskSerializer::ToXAddArgs(*task_ptr);
      if (args_opt) {
        // 注意：这里不用 Pipeline，因为 XADD 需要返回 ID
        // (虽然这里不强制需要，但为了逻辑清晰分开) 也可以放到 Pipeline 里，但
        // XADD 是独立逻辑
        redis_manager.XAdd(keys::stream::kTasks, *args_opt);
        pushed_count++;
      }
    }

    LOG_INFO << "DAG Submitted. JobID: " << request.job_id
             << ", Tasks: " << request.tasks.size()
             << ", Edges: " << request.edges.size()
             << ", Entry Pushed: " << pushed_count;

    return true;  // 告知 main 提交事务

  } catch (const std::exception& e) {
    LOG_ERROR << "Submit DAG Exception: " << e.what();
    return false;
  }
}

}  // namespace dts::api_server