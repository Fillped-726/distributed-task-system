#pragma once

// 1. 包含 gRPC 生成的头文件
#include "dts/internal/internal_service.grpc.pb.h"

// 2. 包含我们的依赖

// #include "task_repository.h" // (TODO: 下一步创建)

// 3. 包含我们统一的错误头文件 (用于创建 error)
#include "dts/error/error.pb.h"
#include "dts/error/sys_error.pb.h"

#include <memory> // for std::shared_ptr

// (前置声明, 告诉编译器这个类存在, 避免循环引用)
class WorkerManager;
class TaskRepository; 


// 4. 定义服务实现类
//    它继承自 gRPC 自动生成的服务基类
class SchedulerServiceImpl final : public dts::internal::SchedulerService::Service {
public:
    // -----------------------------------------------------
    // 构造函数 (依赖注入)
    // -----------------------------------------------------
    
    // 我们通过构造函数“注入”它所依赖的两个核心服务
    SchedulerServiceImpl(
        std::shared_ptr<WorkerManager> worker_manager,
        std::shared_ptr<TaskRepository> task_repository // (TODO: 下一步创建)
    );
    
    // 析构函数
    virtual ~SchedulerServiceImpl() {}

    // -----------------------------------------------------
    // gRPC 接口实现
    // -----------------------------------------------------

    // 1. 实现 Worker 注册
    grpc::Status Register(
        grpc::ServerContext* context, 
        const dts::internal::RegisterRequest* request, 
        dts::internal::RegisterResponse* response
    ) override;

    // 2. 实现 Worker 心跳
    grpc::Status Heartbeat(
        grpc::ServerContext* context, 
        const dts::internal::HeartbeatRequest* request, 
        dts::internal::HeartbeatResponse* response
    ) override;

    // 3. 实现 任务状态更新 (核心 DAG 逻辑)
    grpc::Status UpdateTaskStatus(
        grpc::ServerContext* context, 
        const dts::internal::UpdateTaskStatusRequest* request, 
        dts::internal::UpdateTaskStatusResponse* response
    ) override;


private:
    // -----------------------------------------------------
    // 私有成员 (依赖)
    // -----------------------------------------------------

    // 指向 Worker 管理器的指针 (已实现)
    std::shared_ptr<WorkerManager> worker_manager_;
    
    // 指向数据库操作的指针 (待实现)
    std::shared_ptr<TaskRepository> task_repository_;
};