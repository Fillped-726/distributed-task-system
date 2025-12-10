#include <pqxx/pqxx>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <map>
#include <chrono>
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
#include "dts/task/task.pb.h"

namespace dts::api_server {

// 简化命名空间引用
using RedisManager = dts::common::redis::RedisManager;
using TaskSerializer = dts::common::utils::TaskSerializer;
namespace keys = dts::common::redis::keys;

std::vector<std::pair<std::string, std::string>> BuildXAddFieldsFromProto(
    const dts::task::Task& /*client_task*/
    ,                      // 不需要读它了，参数保留是为了接口兼容或以后扩展
    const std::string& task_uuid, const std::string& job_id) {
  std::vector<std::pair<std::string, std::string>> fields;
  // 只需要 2 个字段，极度轻量
  fields.reserve(2);

  // 1. 核心指针：Worker 拿到这个 ID 后，去 KV (Meta) 里查详情
  fields.emplace_back("id", task_uuid);

  // 2. 上下文：用于日志、隔离或快速判断
  fields.emplace_back("job", job_id);

  return fields;
}

std::optional<DagCommitContext> TaskSubmitter::PersistDagToDB(
    const dts::service::SubmitDagRequest& proto_req, pqxx::work& tx) {
  DagCommitContext ctx;

  // 1. 生成 Job UUID
  ctx.job_id = dts::common::generate();

  LOG_INFO << "Persisting DAG to DB. ClientID: " << proto_req.client_id()
           << ", JobID: " << ctx.job_id;

  // =========================================================
  // 2. 插入 Job 表 (单条 Insert)
  // =========================================================
  // 假设 state 0 = PENDING
  std::string job_sql =
      "INSERT INTO public.job (job_id, idempotency_key, state) VALUES (" +
      tx.quote(ctx.job_id) + ", " + tx.quote(proto_req.idempotency_key()) +
      ", '0') "
      "ON CONFLICT (idempotency_key) DO NOTHING RETURNING job_id";

  pqxx::result res = tx.exec(job_sql);

  if (res.empty()) {
    LOG_WARN << "Duplicate Job (Idempotency check failed): "
             << proto_req.idempotency_key();
    return std::nullopt;
  }

  if (proto_req.tasks_size() == 0) return ctx;

  // =========================================================
  // 3. 计算依赖关系 (直接遍历 Proto edges)
  // =========================================================
  std::unordered_map<std::string, int> dependency_count;
  dependency_count.reserve(proto_req.edges_size());

  for (const auto& edge : proto_req.edges()) {
    dependency_count[edge.child_natural_id()]++;
  }

  // =========================================================
  // 4. 批量插入 Tasks (Stream To)
  // =========================================================
  ctx.natural_to_uuid.reserve(proto_req.tasks_size());

  auto task_stream = pqxx::stream_to::table(
      tx, {"task"},
      {"task_id", "job_id", "natural_id", "func_name", "func_params",
       "priority", "state", "pending_dependencies", "max_retry", "retry_count",
       "timeout_ms", "submit_ts"});

  long long now_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

  for (const auto& task : proto_req.tasks()) {
    // A. 生成 UUID 并注册到 Context
    std::string uuid = dts::common::generate();
    ctx.natural_to_uuid[task.natural_id()] = uuid;  // 建立映射

    // B. 计算初始状态
    int pending = dependency_count[task.natural_id()];
    int initial_state = (pending == 0) ? 0 : 6;  // 0=PENDING, 6=WAITING

    if (pending == 0) {
      ctx.entry_task_ids.push_back(uuid);
    }

    // C. 写入 DB (核心优化点)
    task_stream.write_values(
        uuid,                // task_id
        ctx.job_id,          // job_id
        task.natural_id(),   // natural_id
        task.func_name(),    // func_name
        task.func_params(),  // 直接透传 Proto 里的 string，不做JSON 解析

        task.priority(),    // priority
        initial_state,      // state
        pending,            // pending_deps
        task.max_retry(),   // max_retry
        0,                  // retry_count (init 0)
        task.timeout_ms(),  // timeout
        now_ts              // submit_ts
    );
  }
  task_stream.complete();

  // =========================================================
  // 5. 批量插入 Edges
  // =========================================================
  if (proto_req.edges_size() > 0) {
    auto edge_stream = pqxx::stream_to::table(
        tx, {"task_edge"}, {"parent_task_id", "child_task_id"});

    for (const auto& edge : proto_req.edges()) {
      // 必须确保所有 natural_id 都能找到对应的 UUID
      // 生产环境建议 catch out_of_range 异常并 log error
      try {
        const std::string& p_uuid =
            ctx.natural_to_uuid.at(edge.parent_natural_id());
        const std::string& c_uuid =
            ctx.natural_to_uuid.at(edge.child_natural_id());

        edge_stream.write_values(p_uuid, c_uuid);
      } catch (const std::out_of_range&) {
        // 这通常意味着客户端传的 Edge 引用了不存在的 Task
        // 在这里抛出异常会让事务回滚，符合一致性要求
        throw std::runtime_error("Edge references unknown task natural_id");
      }
    }
    edge_stream.complete();
  }

  return ctx;
}

bool TaskSubmitter::DispatchDagToRedis(
    const dts::service::SubmitDagRequest& proto_req,
    const DagCommitContext& ctx) {
  if (ctx.job_id.empty()) return false;

  try {
    auto& redis_manager = RedisManager::GetInstance();
    auto& redis = redis_manager.GetConnection();
    auto pipe = redis.pipeline();
    const auto TTL = std::chrono::hours(24);

    // =========================================================
    // Phase 1: 处理边 (Edges) - 构建拓扑 & 计数
    // =========================================================
    std::unordered_map<std::string, int> dependency_map;
    // 预分配优化
    dependency_map.reserve(proto_req.tasks_size());

    for (const auto& edge : proto_req.edges()) {
      // 1. 计数 (内存操作)
      dependency_map[edge.child_natural_id()]++;

      // 2. Redis SADD (父子关系)
      const std::string& p_uuid =
          ctx.natural_to_uuid.at(edge.parent_natural_id());
      const std::string& c_uuid =
          ctx.natural_to_uuid.at(edge.child_natural_id());

      std::string children_key = keys::dag::Children(p_uuid);
      pipe.sadd(children_key, c_uuid);
      pipe.expire(children_key, TTL);
    }

    // =========================================================
    // Phase 2: 处理任务 (Tasks) - Meta, HSET, XADD 一气呵成
    // =========================================================

    // 用于延长 XADD 参数的生命周期
    std::vector<std::vector<std::pair<std::string, std::string>>>
        xadd_args_holder;
    xadd_args_holder.reserve(ctx.entry_task_ids.size());  // 可选优化

    for (const auto& client_task : proto_req.tasks()) {
      const std::string& uuid =
          ctx.natural_to_uuid.at(client_task.natural_id());

      // --- A. Meta 数据 ---
      dts::task::Task internal_task;
      internal_task.set_task_id(uuid);
      internal_task.set_job_id(ctx.job_id);
      internal_task.set_natural_id(client_task.natural_id());
      internal_task.set_func_name(client_task.func_name());
      internal_task.set_func_params(client_task.func_params());  // string copy
      internal_task.set_priority(client_task.priority());
      internal_task.set_max_retry(client_task.max_retry());
      internal_task.set_timeout_ms(client_task.timeout_ms());

      std::string binary_payload;
      internal_task.SerializeToString(&binary_payload);
      pipe.set(keys::dag::TaskMeta(uuid), binary_payload, TTL);

      // --- B. 依赖处理 (HSET vs XADD) ---
      // 直接查刚才算好的 map，不存在即为 0
      int pending = dependency_map[client_task.natural_id()];

      if (pending > 0) {
        // 有依赖 -> HSET 等待
        pipe.hset(keys::dag::DependencyHash(ctx.job_id), uuid,
                  std::to_string(pending));
      } else {
        // 无依赖 -> XADD 触发
        auto fields = BuildXAddFieldsFromProto(client_task, uuid, ctx.job_id);
        xadd_args_holder.push_back(std::move(fields));
        auto& current_fields = xadd_args_holder.back();

        pipe.xadd(keys::stream::kTasks, "*", current_fields.begin(),
                  current_fields.end());
      }
    }

    // 给整个 Hash 设置一次过期即可 (优化：不需要在循环里设)
    if (proto_req.edges_size() > 0) {  // 只有有边的时候才有 Hash
      pipe.expire(keys::dag::DependencyHash(ctx.job_id), TTL);
    }

    // =========================================================
    // Phase 3: 发射
    // =========================================================
    auto replies = pipe.exec();

    return true;

  } catch (const std::exception& e) {
    LOG_ERROR << "Redis Dispatch Failed: " << e.what();
    return false;
  }
}

}  // namespace dts::api_server