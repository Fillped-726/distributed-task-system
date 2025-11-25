#pragma once // 防止头文件被重复包含

#include <iostream>
#include <string>
#include <map>
#include <mutex> // 必不可少，用于线程安全
#include <vector>
#include <chrono> // 用于处理时间戳
#include <memory> // 用于 std::shared_ptr

// 内部 gRPC 服务的 proto 头文件 (由 protoc 生成)
// 假设它位于 "build/proto/internal_service.grpc.pb.h"
// 我们需要它，因为它定义了 dts::internal::HeartbeatRequest
#include "dts/internal/internal_service.grpc.pb.h" 

// 存储在内存中的 Worker 信息
struct WorkerInfo {
    std::string worker_id;
    std::string address; // Worker 的 gRPC 地址 "ip:port"
    
    // 最近一次心跳的时间点
    std::chrono::system_clock::time_point last_heartbeat_time;
    
    // 该 Worker 上报的正在运行的任务数 (用于负载均衡)
    int32_t running_task_count;

    // 默认构造
    WorkerInfo() : running_task_count(0) {}
};


// WorkerManager 类
// 这是一个线程安全的类，用于管理所有 Worker 的注册、心跳和状态
class WorkerManager {
public:
    // 构造函数
    WorkerManager();

    // 析构函数（用于启动/停止巡检线程）
    ~WorkerManager();

    // -----------------------------------------------------
    // 核心 gRPC 服务调用 (由 SchedulerServiceImpl 调用)
    // -----------------------------------------------------

    // 1. 处理 Worker 注册
    void HandleRegister(const std::string& worker_id, const std::string& address);

    // 2. 处理 Worker 心跳
    // @return: 返回 true 表示心跳成功, false 表示 Worker 未注册
    bool HandleHeartbeat(const dts::internal::HeartbeatRequest* request);

    // -----------------------------------------------------
    // 核心调度循环调用 (由 SchedulerLoop 调用)
    // -----------------------------------------------------
    
    // 3. 获取一个按“空闲度”排序的可用 Worker 列表
    //    (这是我们负载均衡的关键)
    // @return: 一个 WorkerInfo 的 *拷贝* 列表，按 running_task_count 升序排序
    std::vector<WorkerInfo> GetAvailableWorkersSorted();

    std::vector<std::string> PruneDeadWorkers();

private:
    // -----------------------------------------------------
    // 内部实现
    // -----------------------------------------------------


    // 互斥锁，保护下面的 workers_ 映射
    std::mutex mtx_;
    
    // 存储所有已注册的 Worker
    // key: worker_id
    // value: WorkerInfo
    std::map<std::string, std::shared_ptr<WorkerInfo>> workers_;

    
    std::chrono::seconds worker_timeout_;  // Worker 超时阈值
};