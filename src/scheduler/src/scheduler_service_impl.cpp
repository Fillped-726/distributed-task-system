#include "scheduler_service_impl.h"
#include "worker_manager.h" 
#include "task_repository.h" // (TODO: 下一步创建)

// -----------------------------------------------------
// 构造函数
// -----------------------------------------------------
SchedulerServiceImpl::SchedulerServiceImpl(
    std::shared_ptr<WorkerManager> worker_manager,
    std::shared_ptr<TaskRepository> task_repository
) : // C++ 成员初始化列表
    worker_manager_(worker_manager),
    task_repository_(task_repository) 
{
    // 确保依赖不为空
    if (worker_manager_ == nullptr) {
        throw std::runtime_error("WorkerManager is null");
    }
    if (task_repository_ == nullptr) {
        // (暂时注释, 等我们创建了 TaskRepository)
        // throw std::runtime_error("TaskRepository is null");
    }
    std::cout << "SchedulerServiceImpl created." << std::endl;
}

// -----------------------------------------------------
// 1. 实现 Register
// -----------------------------------------------------
grpc::Status SchedulerServiceImpl::Register(
    grpc::ServerContext* context, 
    const dts::internal::RegisterRequest* request, 
    dts::internal::RegisterResponse* response) 
{
    std::cout << "gRPC [Register] from worker: " << request->worker_id() << std::endl;

    // 1. 参数校验
    if (request->worker_id().empty() || request->address().empty()) {
        // (这是我们统一的错误处理)
        auto* err = response->mutable_header()->mutable_error();
        err->set_sys(dts::error::SYS_INVALID_PARAM);
        err->set_msg("worker_id or address is empty");
        return grpc::Status::OK; // 业务错误, gRPC 状态仍是 OK
    }

    // 2. 将请求转交给 WorkerManager
    worker_manager_->HandleRegister(request->worker_id(), request->address());

    // 3. 构造成功响应 (header 为空, 表示没有 error)
    response->mutable_header(); // (创建一个空的 header)
    return grpc::Status::OK;
}


// -----------------------------------------------------
// 2. 实现 Heartbeat
// -----------------------------------------------------
grpc::Status SchedulerServiceImpl::Heartbeat(
    grpc::ServerContext* context, 
    const dts::internal::HeartbeatRequest* request, 
    dts::internal::HeartbeatResponse* response) 
{
    // (心跳日志太频繁, 默认注释掉)
    // std::cout << "gRPC [Heartbeat] from worker: " << request->worker_id() << std::endl;

    // 1. 将请求转交给 WorkerManager
    bool success = worker_manager_->HandleHeartbeat(request);

    // 2. 处理 Worker 未注册的边缘情况
    if (!success) {
        std::cerr << "Heartbeat from unknown worker: " << request->worker_id() << std::endl;
        auto* err = response->mutable_header()->mutable_error();
        err->set_sys(dts::error::SYS_INVALID_PARAM); // (或者一个更具体的 "WORKER_NOT_REGISTERED" 错误)
        err->set_msg("Worker is not registered. Please register first.");
        return grpc::Status::OK;
    }

    // 3. 构造成功响应
    response->mutable_header();
    return grpc::Status::OK;
}


// -----------------------------------------------------
// 3. 实现 UpdateTaskStatus
// -----------------------------------------------------
grpc::Status SchedulerServiceImpl::UpdateTaskStatus(
    grpc::ServerContext* context, 
    const dts::internal::UpdateTaskStatusRequest* request, 
    dts::internal::UpdateTaskStatusResponse* response) 
{
    std::cout << "gRPC [UpdateTaskStatus] for task: " << request->task_id() << std::endl;
    
    // 1. 参数校验
    if (request->task_id().empty()) {
        // 简单处理：如果 ID 为空，直接返回
        return grpc::Status::OK;
    }
    
    // 2. 将 *所有* 复杂的数据库和 DAG 逻辑委托给 TaskRepository 去处理
    // [修改] 取消注释，启用核心逻辑
    bool dag_success = task_repository_->HandleTaskCompletion(request);

    if (!dag_success) {
        std::cerr << "[Scheduler] HandleTaskCompletion failed for task " << request->task_id() << std::endl;
        // 这里我们可以选择返回 gRPC 错误，或者只记录日志
        // 如果返回错误，Worker 可能会重试（取决于 Worker 逻辑）
        // return grpc::Status(grpc::INTERNAL, "Database error");
    }
    
    // 3. 构造响应
    response->mutable_header();
    return grpc::Status::OK;
}