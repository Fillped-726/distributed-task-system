#include "task_repository.h"
#include <iostream>
#include <exception> // for std::exception
#include <string>
#include <nlohmann/json.hpp>
#include "converters.hpp"

namespace pqxx {
// 为我们的 TaskState 枚举特化 string_traits
template<>
struct string_traits<dts::task::TaskState> {
    // 告诉 libpqxx, 我们将把它转换为一个整数
    static constexpr bool converts_to_string = false;
    static constexpr bool converts_from_string = false;

    // "size_buffer" - 转换需要多少空间?
    static std::size_t size_buffer(dts::task::TaskState const &obj) noexcept {
        // 转换为一个 C++ string, 然后返回其大小
        return string_traits<std::string>::size_buffer(std::to_string(obj));
    }

    // "into_buf" - 如何执行转换?
    static char *into_buf(
        char *begin, 
        char *end, 
        dts::task::TaskState const &value)
    {
        // 转换为一个 C++ string, 然后调用 string 的转换器
        return string_traits<std::string>::into_buf(
            begin, end, std::to_string(value)
        );
    }
};
} // namespace pqxx

// -----------------------------------------------------
// 构造函数 (不变)
// -----------------------------------------------------
TaskRepository::TaskRepository(std::shared_ptr<DatabasePool> db_pool)
    : db_pool_(db_pool)
{
    if (db_pool_ == nullptr) {
        throw std::runtime_error("DatabasePool is null");
    }
}

// -----------------------------------------------------
// 2a. GetPendingTasks (已修复)
// -----------------------------------------------------
std::vector<dts::task::Task> TaskRepository::GetPendingTasks(int limit) {
    std::vector<dts::task::Task> tasks;
    
    try {
        // *** 关键修复 1: 使用 ExecuteTx ***
        db_pool_->ExecuteTx([&](pqxx::work& tx) { // <-- tx 是一个引用 (pqxx::work&)

            // 2. 准备 SQL
            std::string sql = R"(
                SELECT task_id, job_id, natural_id, func_name, func_params, 
                       priority, max_retry, retry_count, timeout_ms 
                FROM task 
                WHERE state = $1 -- 0 (PENDING)
                ORDER BY priority DESC, submit_ts ASC 
                LIMIT $2;
            )";
            
            // *** 关键修复 2: 使用 tx. (点操作符), 而不是 tx-> ***
            pqxx::result r = tx.exec_params(sql, dts::task::PENDING, limit);

            // 4. 将 SQL 结果映射到 Protobuf 对象
            for (auto row : r) {
               dts::task::Task t;
               t.set_task_id(row["task_id"].as<std::string>());
               t.set_job_id(row["job_id"].as<std::string>());
               t.set_func_name(row["func_name"].as<std::string>());
               
               // [!!! 核心修复开始 !!!]
               // 处理 func_params 字段
               if (!row["func_params"].is_null()) {
                   try {
                       // 1. 从数据库读取 JSON 字符串
                       std::string params_str = row["func_params"].as<std::string>();
                       
                       // 2. 只有非空字符串才解析
                       if (!params_str.empty()) {
                           nlohmann::json j_params = nlohmann::json::parse(params_str);
                           
                           // 3. 调用你写好的 JsonToStruct，填入 t.mutable_func_params()
                           // 注意：这里我们假设 JsonToStruct 在 dts 命名空间下或全局可用
                           dts::JsonToStruct(j_params, t.mutable_func_params());
                       }
                   } catch (const std::exception& e) {
                       std::cerr << "[TaskRepo] JSON parse error for task " 
                                 << t.task_id() << ": " << e.what() << std::endl;
                       // 解析失败时不填参数，或者填一个空的 Struct，不要崩
                   }
               }
               // [!!! 核心修复结束 !!!]

               t.set_timeout_ms(row["timeout_ms"].as<int>());
               t.set_max_retry(row["max_retry"].as<int>());
               t.set_retry_count(row["retry_count"].as<int>());
               
               // 处理 state 等其他字段...
               //t.set_state(static_cast<dts::task::TaskState>(row["state"].as<int>()));

               tasks.push_back(t);
            }
            // (ExecuteTx 在这里自动 commit)
        });

    } catch (const std::exception& e) {
        std::cerr << "[TaskRepo] GetPendingTasks failed: " << e.what() << std::endl;
        // (ExecuteTx 在这里自动回滚)
    }
    return tasks;
}

// -----------------------------------------------------
// 2b. UpdateTaskToRunning (已修复)
// -----------------------------------------------------
bool TaskRepository::UpdateTaskToRunning(const std::string& task_id, const std::string& worker_id) {
    // *** 关键修复 3: 在 lambda 外部声明 'updated' ***
    bool updated = false; 
    try {
        db_pool_->ExecuteTx([&](pqxx::work& tx) { // <-- tx 是引用
            std::string sql = R"(
                UPDATE task 
                SET state = $1,      -- 1 (RUNNING)
                    worker_id = $2, 
                    start_ts = EXTRACT(EPOCH FROM (NOW()))::BIGINT 
                WHERE task_id = $3
                AND state = $4;      -- 0 (PENDING)
            )";
            
            // *** 关键修复 2: 使用 tx. (点操作符) ***
            pqxx::result r = tx.exec_params(sql, 
               dts::task::RUNNING, 
               worker_id, 
               task_id, 
               dts::task::PENDING
            );

            // *** 关键修复 3: 在 lambda 内部 *赋值* ***
            updated = (r.affected_rows() == 1);
            // (自动 commit)
        });
        
        // *** 关键修复 3: 在 lambda 外部 *返回* ***
        return updated;

    } catch (const std::exception& e) {
        std::cerr << "[TaskRepo] UpdateTaskToRunning failed: " << e.what() << std::endl;
        return false;
    }
}

// -----------------------------------------------------
// 2c. RevertTaskToPending (已修复)
// -----------------------------------------------------
bool TaskRepository::RevertTaskToPending(const std::string& task_id) {
    try {
        db_pool_->ExecuteTx([&](pqxx::work& tx) { // <-- tx 是引用
            std::string sql = R"(
                UPDATE task 
                SET state = $1, worker_id = NULL, start_ts = NULL
                WHERE task_id = $2 AND state = $3;
            )";
            
            // *** 关键修复 2: 使用 tx. (点操作符) ***
            tx.exec_params(sql, dts::task::PENDING, task_id, dts::task::RUNNING);
            // (自动 commit)
        });
        return true;

    } catch (const std::exception& e) {
        std::cerr << "[TaskRepo] RevertTaskToPending failed: " << e.what() << std::endl;
        return false;
    }
}

// -----------------------------------------------------
// 3a. RequeueOrphanedTasks (已修复)
// -----------------------------------------------------
int TaskRepository::RequeueOrphanedTasks(const std::string& dead_worker_id) {
    // *** 关键修复 3: 在 lambda 外部声明 'requeued_count' ***
    int requeued_count = 0;
    try {
        db_pool_->ExecuteTx([&](pqxx::work& tx) { // <-- tx 是引用
            std::string sql = R"(
                UPDATE task 
                SET state = $1, worker_id = NULL, start_ts = NULL
                WHERE worker_id = $2 AND state = $3;
            )";
            
            // *** 关键修复 2: 使用 tx. (点操作符) ***
            pqxx::result r = tx.exec_params(sql, 
                dts::task::PENDING, 
                dead_worker_id, 
                dts::task::RUNNING
            );
            
            // *** 关键修复 3: 在 lambda 内部 *赋值* ***
            requeued_count = r.affected_rows();
            // (自动 commit)
        });
        
        if (requeued_count > 0) {
            std::cout << "[TaskRepo] Requeued " << requeued_count 
                      << " tasks from dead worker: " << dead_worker_id << std::endl;
        }
        // *** 关键修复 3: 在 lambda 外部 *返回* ***
        return requeued_count;

    } catch (const std::exception& e) {
        std::cerr << "[TaskRepo] RequeueOrphanedTasks failed: " << e.what() << std::endl;
        return 0;
    }
}


// -----------------------------------------------------
// 1. HandleTaskCompletion (已修复)
// -----------------------------------------------------
bool TaskRepository::HandleTaskCompletion(
    const dts::internal::UpdateTaskStatusRequest* request) 
{
    try {
        db_pool_->ExecuteTx([&](pqxx::work& tx) { // <-- tx 是引用
            auto final_state = request->final_state();
            auto task_id = request->task_id();

            // 步骤 1：更新当前任务的最终状态
            std::string sql_update = R"(
                UPDATE task 
                SET state = $1, result = $2, error_msg = $3, 
                    finish_ts = EXTRACT(EPOCH FROM (NOW()))::BIGINT
                WHERE task_id = $4;
            )";
            
            // *** 关键修复 2: 使用 tx. (点操作符) ***
            tx.exec_params(sql_update, 
               final_state, 
               request->result_json(),
               request->error_msg(), 
               task_id
            );

            // 步骤 2：根据状态进行下一步
            if (final_state == dts::task::SUCCESS) {
                // *** 关键修复 4: 传递 tx (引用), 而不是 *tx ***
                PropagateSuccess(tx, task_id);
                
            } else if (final_state == dts::task::FAILED || 
                       final_state == dts::task::TIMEOUT) {
                // *** 关键修复 4: 传递 tx (引用), 而不是 *tx ***
                HandleRetry(tx, task_id);
            }
            // (自动 commit)
        });
        return true;

    } catch (const std::exception& e) {
        std::cerr << "[TaskRepo] HandleTaskCompletion transaction failed: " << e.what() << std::endl;
        // (自动回滚)
        return false;
    }
}


// -----------------------------------------------------
// 6. 私有助手：PropagateSuccess (不变, 你的代码已正确)
// -----------------------------------------------------
bool TaskRepository::PropagateSuccess(pqxx::work& tx, const std::string& parent_task_id) {
    // (你的代码已经使用了 tx. (点操作符), 非常好!)
    std::string sql_find_children = R"(
        SELECT child_task_id FROM task_edge WHERE parent_task_id = $1;
    )";
    pqxx::result children = tx.exec_params(sql_find_children, parent_task_id);

    for (auto row : children) {
       std::string child_id = row[0].as<std::string>();
        std::string sql_decrement = R"(
            UPDATE task SET pending_dependencies = pending_dependencies - 1 
            WHERE task_id = $1 AND state = $2
            RETURNING pending_dependencies;
        )";
        pqxx::result res_deps = tx.exec_params(sql_decrement, child_id, dts::task::WAITING_DEPS);
        
        if (res_deps.empty()) continue; 
        int remaining_deps = res_deps[0][0].as<int>();

        if (remaining_deps == 0) {
            std::string sql_set_pending = R"(
                UPDATE task SET state = $1 WHERE task_id = $2;
            )";
            tx.exec_params(sql_set_pending, dts::task::PENDING, child_id);
        }
    }
    return true;
}

// -----------------------------------------------------
// 7. 私有助手：HandleRetry (不变, 你的代码已正确)
// -----------------------------------------------------
bool TaskRepository::HandleRetry(pqxx::work& tx, const std::string& task_id) {
    // (你的代码已经使用了 tx. (点操作符), 非常好!)
    std::string sql_get_retries = R"(
        SELECT retry_count, max_retry FROM task 
        WHERE task_id = $1 FOR UPDATE;
    )";
    pqxx::result res = tx.exec_params(sql_get_retries, task_id);
    if (res.empty()) return false;
    
    int retry_count = res[0]["retry_count"].as<int>();
    int max_retry = res[0]["max_retry"].as<int>();

    if (retry_count < max_retry) {
        std::string sql_requeue = R"(
            UPDATE task 
            SET state = $1, retry_count = $2, 
                worker_id = NULL, start_ts = NULL, finish_ts = NULL, 
                error_msg = NULL, result = NULL
            WHERE task_id = $3;
        )";
        tx.exec_params(sql_requeue, 
           dts::task::PENDING, 
           retry_count + 1, 
           task_id
        );
    }
    return true;
}