#include "task_submitter.hpp"

// 包含所有实现所需的头文件
#include <pqxx/pqxx> // **重要: 包含真实的 libpqxx**
#include <map>
#include <sstream>
#include <stdexcept>
#include <chrono>
#include <iostream>

bool TaskSubmitter::handleSubmitDag(dts::SubmitDagRequest& request, pqxx::connection& conn) {
    
    // --- 步骤 1: 构建“父依赖计数 Map” (业务逻辑, 不变) ---
    std::map<std::string, int> dependency_count;
    for (const auto& edge : request.edges) {
        dependency_count[edge.child_id]++;
    }
    std::cout << "[LOG] 依赖计数计算完毕。" << std::endl;

    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    // --- 步骤 2: 启动数据库事务 (已更新: 使用 pqxx::work) ---
    // 'txn' 对象在构造时自动开始 (BEGIN;)
    // 析构时自动提交 (COMMIT;) 或回滚 (ROLLBACK;)
    pqxx::work txn(conn);

    try {
        // --- 步骤 3: (幂等性) (已更新: 使用 txn.exec 和 txn.quote) ---
        // **安全**: 使用 txn.quote() 转义所有字符串输入
        std::string idempotency_sql = "INSERT INTO task_idempotency (business_id, job_def_id, created_at) VALUES (" + 
                                  txn.quote(request.business_id) + ", " + 
                                  txn.quote(request.job_def_id) + ", " + 
                                  "NOW()" +  // 直接使用 SQL 的 NOW() 函数，不需要 quote
                                  ");";
        
        try {
            txn.exec(idempotency_sql);
        } catch (const pqxx::unique_violation& e) {
            // **重要**: 专门捕获唯一的键冲突
            std::cerr << "[ERROR] 幂等性检查失败 (重复请求): " << e.what() << std::endl;
            txn.abort(); // 明确回滚
            return false;
        }

        // --- 步骤 4: (批量创建节点) (已更新: 安全的 SQL) ---
        if (!request.tasks.empty()) {
            std::stringstream task_insert_sql;
            task_insert_sql << "INSERT INTO task_run (job_def_id, task_id, client_id, priority, "
                            << "func_name, func_params, required, shard, timeout_ms, max_retry, retry_count, submit_ts, "
                            << "pending_dependencies, state) VALUES ";

            for (size_t i = 0; i < request.tasks.size(); ++i) {
                auto& task = request.tasks[i];
                int deps = dependency_count.count(task.task_id) ? dependency_count[task.task_id] : 0;
                dts::TaskState initial_state = (deps == 0) ? dts::TaskState::PENDING : dts::TaskState::WAITING_DEPS;
                task.submit_ts = now_ms;

                // **安全**: 序列化后的 JSON 字符串也必须被转义
                std::string func_params_json = task.func_params.is_null() ? "null" : task.func_params.dump();
                std::string required_json = nlohmann::json(task.required).dump();
                std::string shard_json = nlohmann::json(task.shard).dump();

                task_insert_sql << "("
                                << txn.quote(request.job_def_id) << ", "  // 安全
                                << txn.quote(task.task_id) << ", "    // 安全
                                << txn.quote(task.client_id) << ", " // 安全
                                << task.priority << ", "             // 数字, 安全
                                << txn.quote(task.func_name) << ", " // 安全
                                << txn.quote(func_params_json) << ", " // 安全
                                << txn.quote(required_json) << ", "    // 安全
                                << txn.quote(shard_json) << ", "       // 安全
                                << task.timeout_ms << ", "
                                << task.max_retry << ", "
                                << task.retry_count << ", "
                                << task.submit_ts << ", "
                                << deps << ", "
                                << static_cast<std::int8_t>(initial_state)
                                << ")";
                if (i < request.tasks.size() - 1) task_insert_sql << ", ";
            }
            task_insert_sql << ";";
            
            std::cout << "[DB] " << task_insert_sql.str() << std::endl; // 打印生成的 SQL
            txn.exec(task_insert_sql.str()); // 执行批量插入
        }

        // --- 步骤 5: (批量创建边) (已更新: 安全的 SQL) ---
        if (!request.edges.empty()) {
            std::stringstream edge_insert_sql;
            edge_insert_sql << "INSERT INTO task_edge (job_def_id, parent_run_id, child_run_id) VALUES ";
            for (size_t i = 0; i < request.edges.size(); ++i) {
                const auto& edge = request.edges[i];
                edge_insert_sql << "("
                                << txn.quote(request.job_def_id) << ", "
                                << txn.quote(edge.parent_id) << ", "
                                << txn.quote(edge.child_id)
                                << ")";
                if (i < request.edges.size() - 1) edge_insert_sql << ", ";
            }
            edge_insert_sql << ";";
            
            std::cout << "[DB] " << edge_insert_sql.str() << std::endl;
            txn.exec(edge_insert_sql.str());
        }

        // --- 步骤 6: (审计) (已更新: 安全的 SQL) ---
        std::string audit_sql = "INSERT INTO audit_log (user_id, job_def_id, action_type, details) VALUES ('api_user', " +
                                txn.quote(request.job_def_id) + ", 'SUBMIT_DAG', 'DAG submitted with " +
                                txn.quote(std::to_string(request.tasks.size())) + " tasks.');"; // 数字转字符串后也转义
        
        std::cout << "[DB] " << audit_sql << std::endl;
        txn.exec(audit_sql);

        // --- 步骤 7: COMMIT (已更新: 使用 txn.commit) ---
        txn.commit(); // 显式提交
        std::cout << "[LOG] 任务提交成功 (Job ID: " << request.job_def_id << ")" << std::endl;
        return true;

    } catch (const std::exception& e) {
        // 捕获所有其他异常 (包括 pqxx::sql_error)
        std::cerr << "[ERROR] 发生数据库异常: " << e.what() << std::endl;
        // txn 在析构时会自动调用 abort(), 因为 commit() 未被调用
        return false;
    }
}