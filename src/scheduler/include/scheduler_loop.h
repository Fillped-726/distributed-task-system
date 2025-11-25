#pragma once

#include <thread>
#include <atomic> // 用于 std::atomic<bool>
#include <memory>
#include <map>
#include <mutex>

// 依赖
#include "task_repository.h"
#include "worker_manager.h"

// gRPC 客户端依赖
#include <grpcpp/grpcpp.h>
#include "dts/internal/internal_service.grpc.pb.h"

class SchedulerLoop {
public:
    // 构造函数 (依赖注入)
    SchedulerLoop(
        std::shared_ptr<TaskRepository> task_repo,
        std::shared_ptr<WorkerManager> worker_manager
    );

    // 析构函数 (确保线程停止)
    ~SchedulerLoop();

    // 启动调度循环 (在后台线程中)
    void Start();

    // 停止调度循环
    void Stop();

private:
    // -----------------------------------------------------
    // 内部实现
    // -----------------------------------------------------

    // 线程将要执行的函数 (包含 "while(true)" 循环)
    void RunLoop();

    // 派发单个任务的
    // (在单独的线程中执行，以避免阻塞 RunLoop)
    void DoDispatch(dts::task::Task task, WorkerInfo worker);

    // -----------------------------------------------------
    // gRPC 客户端 (Stub) 缓存
    // -----------------------------------------------------
    
    // (校招亮点：避免为每个任务都重新创建 gRPC 连接)
    // 根据 Worker 地址获取或创建一个 gRPC Stub
    std::shared_ptr<dts::internal::WorkerService::Stub> GetWorkerStub(
        const std::string& address
    );

    // 保护 Stub 缓存的互斥锁
    std::mutex stub_cache_mtx_;
    
    // key: "ip:port"
    // value: 对应的 gRPC Channel (Stub 依赖 Channel)
    std::map<std::string, std::shared_ptr<grpc::Channel>> channel_cache_;

    // -----------------------------------------------------
    // 依赖 (的拷贝)
    // -----------------------------------------------------
    std::shared_ptr<TaskRepository> task_repo_;
    std::shared_ptr<WorkerManager> worker_manager_;

    // -----------------------------------------------------
    // 线程管理
    // -----------------------------------------------------
    std::thread loop_thread_;
    std::atomic<bool> stop_flag_; 
};