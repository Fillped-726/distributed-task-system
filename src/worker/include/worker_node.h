#pragma once

#include <memory>
#include <string>
#include <atomic>
#include <grpcpp/grpcpp.h>

#include "scheduler_client.h"
#include "worker_service.h"
#include "thread_pool.h"
#include "task_registry.h"
#include "task.hpp"

namespace dts {
namespace worker {

class WorkerNode {
public:
    // 构造函数
    // server_address: 本地监听地址 (e.g., "0.0.0.0:50051")
    // scheduler_address: 调度器地址 (e.g., "localhost:8080")
    WorkerNode(const std::string& worker_id, 
               const std::string& server_address,
               const std::string& scheduler_address);
    
    ~WorkerNode();

    // 启动 Worker
    void Start();

    // 停止 Worker
    void Stop();

    // 阻塞等待服务结束
    void Await(); 

private:
    // 处理收到任务的回调逻辑 (The "Executor" Logic)
    void OnTaskReceived(const dts::Task& task);

    // 后台心跳线程逻辑
    void HeartbeatLoop();

private:
    std::string worker_id_;
    std::string server_address_;
    
    // 组件
    std::unique_ptr<ThreadPool> thread_pool_;
    std::unique_ptr<SchedulerClient> scheduler_client_;
    std::unique_ptr<WorkerServiceImpl> service_impl_; // gRPC Service 实现
    std::unique_ptr<grpc::Server> grpc_server_;       // gRPC Server 实例

    // 心跳控制
    std::thread heartbeat_thread_;
    std::atomic<bool> running_;
};

} // namespace worker
} // namespace dts