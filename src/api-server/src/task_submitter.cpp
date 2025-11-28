#include <pqxx/pqxx>
#include <iostream>
#include "task_submitter.hpp"  // (假设这是 .hpp 文件)
#include "dag.hpp"             // (需要 SubmitDagRequest 和 TaskEdge)
#include "task.hpp"            // (需要 Task)
#include <stdexcept>
#include <sstream>
#include <map>
#include <chrono>
#include "uuid_generator.hpp"  // (假设你有一个这样的头文件)

// *** 1. 关键修改：函数签名 ***
// (它现在接收一个 *事务引用*, 而不是连接)
bool TaskSubmitter::handleSubmitDag(dts::SubmitDagRequest& request,
                                    pqxx::work& tx) {
  // -----------------------------------------------------------------
  // 步骤 1: 计算依赖计数 (不变)
  // -----------------------------------------------------------------
  std::map<std::string, int> dependency_count;
  for (const auto& edge : request.edges) {
    dependency_count[edge.child_natural_id]++;
  }
  std::cout << "[LOG] 依赖计数计算完毕。" << std::endl;

  auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();

  // -----------------------------------------------------------------
  // 步骤 2: (*** 关键修改：移除 ***)
  // (事务现在由 *调用者* (main.cpp) 创建)
  // pqxx::work txn(conn); // <--- 已移除
  // -----------------------------------------------------------------

  try {
    // -----------------------------------------------------------------
    // 步骤 3: 插入 Job (处理幂等性)
    // -----------------------------------------------------------------
    std::string new_job_uuid = dts::uuid::generate();
    request.job_id = new_job_uuid;

    std::string job_insert_sql =
        "INSERT INTO public.job (job_id, idempotency_key, state) VALUES (" +
        tx.quote(request.job_id) + ", " +           // (*** 已修改：使用 tx ***)
        tx.quote(request.idempotency_key) + ", " +  // (*** 已修改：使用 tx ***)
        "'0'" + ") ON CONFLICT (idempotency_key) DO NOTHING RETURNING job_id;";

    // (*** 已修改：使用 tx ***)
    pqxx::result job_res = tx.exec(job_insert_sql);

    if (job_res.empty()) {
      // 冲突发生 (ON CONFLICT DO NOTHING)，说明是重复提交
      std::cout << "[LOG] 幂等性冲突 (重复请求): " << request.idempotency_key
                << std::endl;

      // (*** 关键修改：移除 ***)
      // (调用者 main.cpp 会 commit 这个空事务, 这是无害的)
      // tx.abort(); // <--- 已移除

      // (返回 true, 因为“提交”这个动作在逻辑上是成功的)
      return true;
    }

    // -----------------------------------------------------------------
    // 步骤 4: "两遍循环 + Map 映射" (逻辑不变)
    // -----------------------------------------------------------------

    std::map<std::string, std::string> natural_to_uuid_map;

    // -----------------------------------------------------------------
    // 步骤 4a: 第一次循环 (插入 Task)
    // -----------------------------------------------------------------
    if (request.tasks.empty()) {
      throw std::runtime_error("提交的 tasks 列表不能为空");
    }

    std::stringstream task_insert_sql;
    // ... (SQL 字符串不变) ...
    task_insert_sql << "INSERT INTO public.task "
                    << "(task_id, job_id, natural_id, func_name, func_params, "
                    << "priority, state, pending_dependencies, "
                    << "max_retry, retry_count, timeout_ms, submit_ts) "
                    << "VALUES ";

    for (size_t i = 0; i < request.tasks.size(); ++i) {
      auto& task = request.tasks[i];

      // [修复 2] 必须在这里生成 UUID，否则 task_id 是空的
      task.task_id = dts::uuid::generate();
      // 记录映射关系，供后面处理 Edge 使用
      natural_to_uuid_map[task.natural_id] = task.task_id;

      // 计算初始依赖数 (这部分逻辑你应该已经有了)
      int pending_count = dependency_count[task.natural_id];

      // 确定初始状态 (0=PENDING, 6=WAITING_DEPS)
      // 假设你的枚举里 PENDING 是 0, WAITING_DEPS 是 6
      int initial_state = (pending_count == 0) ? 0 : 6;

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

    std::cout << "[DB] " << task_insert_sql.str() << std::endl;
    tx.exec(task_insert_sql.str());  // (*** 已修改：使用 tx ***)

    // -----------------------------------------------------------------
    // 步骤 4b: 第二次循环 (插入 Edge)
    // -----------------------------------------------------------------
    if (!request.edges.empty()) {
      std::stringstream edge_insert_sql;
      edge_insert_sql << "INSERT INTO public.task_edge (parent_task_id, "
                         "child_task_id) VALUES ";

      for (size_t i = 0; i < request.edges.size(); ++i) {
        const auto& edge = request.edges[i];

        // ... (UUID 查找逻辑不变) ...
        std::string parent_uuid =
            natural_to_uuid_map.at(edge.parent_natural_id);
        std::string child_uuid = natural_to_uuid_map.at(edge.child_natural_id);

        edge_insert_sql << "(" << tx.quote(parent_uuid)
                        << ", "                  // (*** 已修改：使用 tx ***)
                        << tx.quote(child_uuid)  // (*** 已修改：使用 tx ***)
                        << ")";
        if (i < request.edges.size() - 1) edge_insert_sql << ", ";
      }
      edge_insert_sql << ";";

      std::cout << "[DB] " << edge_insert_sql.str() << std::endl;
      tx.exec(edge_insert_sql.str());  // (*** 已修改：使用 tx ***)
    }

    // -----------------------------------------------------------------
    // 步骤 5: (*** 关键修改：移除 ***)
    // (调用者 main.cpp 将会负责提交)
    // -----------------------------------------------------------------
    // tx.commit(); // <--- 已移除

    std::cout << "[LOG] 任务提交成功 (Job ID: " << request.job_id << ")"
              << std::endl;
    return true;  // (返回 true, 告知 main.cpp 可以 commit)

  } catch (const std::exception& e) {
    // 捕获所有异常
    std::cerr << "[ERROR] 发生数据库异常或逻辑错误: " << e.what() << std::endl;

    // (*** 关键修改：不需要回滚 ***)
    // (调用者 main.cpp 的 try...catch 会捕获这个异常,
    //  并且 *不会* commit, tx 析构时会自动回滚)
    return false;  // (返回 false, 告知 main.cpp *不要* commit)
  }
}