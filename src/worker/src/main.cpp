#include "worker_node.h"
#include "task_registry.h"
#include "logger.hpp"
#include <iostream>
#include <thread>
#include <csignal>

// 全局指针用于信号处理
std::unique_ptr<dts::worker::WorkerNode> g_worker_node;
std::atomic<bool> g_quit{false};

// 信号处理：捕获 Ctrl+C (SIGINT) 实现优雅退出
void SignalHandler(int signum) {
    LOG_INFO << "Interrupt signal (" << signum << ") received. Stopping worker...";
    if (g_worker_node) {
        g_worker_node->Stop();
    }
    g_quit = true;
}

// --- 定义一些简单的测试任务 ---
// 实际项目中，这些可能会放在单独的业务 .cpp 文件中，通过链接器自动注册
std::string FailTask(const std::string& params) {
    throw std::runtime_error("Simulated intentional failure!");
}

int main(int argc, char** argv) {
    // 1. 初始化日志
    dts::InitGlog(argv[0]);

    // 2. 注册业务任务
    // 使用宏或直接调用 Register
    dts::worker::TaskRegistry::GetInstance().Register("fail_test", FailTask);

    // 3. 配置启动参数 (可以通过命令行解析库 gflags 传入)
    std::string worker_id = "worker-01";
    std::string server_addr = "0.0.0.0:50051";
    std::string scheduler_addr = "localhost:9090";

    if (argc > 1) worker_id = argv[1];
    if (argc > 2) server_addr = argv[2];

    LOG_INFO << "Starting DTS Worker Node...";
    LOG_INFO << "ID: " << worker_id;
    LOG_INFO << "Listen: " << server_addr;
    LOG_INFO << "Scheduler: " << scheduler_addr;

    // 4. 创建并启动节点
    g_worker_node = std::make_unique<dts::worker::WorkerNode>(worker_id, server_addr, scheduler_addr);
    
    // 注册信号处理
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    // 启动 (这里会连接 Scheduler，如果 Scheduler 没起可能会失败退出，视 WorkerNode 逻辑而定)
    g_worker_node->Start();

    // 5. 阻塞主线程，直到收到信号
    // 由于 g_worker_node->Start() 中的 grpc server 是异步的 (Wait 逻辑被封装了或者我们没调用 Wait)，
    // 我们需要在这里保持主线程存活。
    // *修正*：grpc::Server::Wait() 是阻塞的，但我们在 WorkerNode::Start 里用的是 BuildAndStart()，它是非阻塞的。
    // 所以我们需要一个 Wait 机制。
    
    // 最简单的方法：使用 while 循环检查 g_worker_node 状态，或者使用 condition_variable
    LOG_INFO << "Worker is running. Press Ctrl+C to stop.";
    g_worker_node->Await();
    LOG_INFO << "Main loop exited. Bye.";
    return 0;
}