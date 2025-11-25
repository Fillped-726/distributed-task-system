#pragma once

#include <string>
#include <vector>
#include <memory>
#include <pqxx/pqxx> // *** 包含 libpqxx ***

// 包含我们的 DatabasePool
#include "database_pool.h" 

// 包含 gRPC proto 生成的头文件
#include "dts/internal/internal_service.grpc.pb.h"
#include "dts/task/task.pb.h" // 需要 dts::task::Task 和 dts::task::TaskState

class TaskRepository {
public:
    // -----------------------------------------------------
    // 构造函数 (依赖注入)
    // -----------------------------------------------------
    
    // *** 关键改动：注入你新的 DatabasePool ***
    TaskRepository(std::shared_ptr<DatabasePool> db_pool);

    virtual ~TaskRepository() {}

    // -----------------------------------------------------
    // 5 个核心功能 (API 保持不变)
    // -----------------------------------------------------

    // 1. (被 gRPC 调用) 核心 DAG 推进逻辑
    bool HandleTaskCompletion(
        const dts::internal::UpdateTaskStatusRequest* request
    );

    // 2a. (被调度循环调用) 拉取待调度的任务
    std::vector<dts::task::Task> GetPendingTasks(int limit);

    // 2b. (被调度循环调用) 将任务状态更新为 RUNNING (乐观锁)
    bool UpdateTaskToRunning(const std::string& task_id, const std::string& worker_id);

    // 2c. (被调度循环调用) 回滚任务状态 (gRPC 派发失败时)
    bool RevertTaskToPending(const std::string& task_id);

    // 3a. (被巡检线程调用) 重置“僵尸”任务
    int RequeueOrphanedTasks(const std::string& dead_worker_id);


private:
    // -----------------------------------------------------
    // 私有成员 (依赖)
    // -----------------------------------------------------
    
    std::shared_ptr<DatabasePool> db_pool_;
    
    // -----------------------------------------------------
    // 私有助手 (Helper) 函数 (用于事务)
    // -----------------------------------------------------
    
    // *** 关键改动：我们明确使用 pqxx::work 作为事务类型 ***
    
    // 1. 当任务成功时, 推进 DAG (在事务中调用)
    bool PropagateSuccess(pqxx::work& tx, const std::string& parent_task_id);

    // 2. 当任务失败时, 处理重试 (在事务中调用)
    bool HandleRetry(pqxx::work& tx, const std::string& task_id);
};