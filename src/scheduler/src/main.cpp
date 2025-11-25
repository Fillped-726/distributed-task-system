#include <iostream>
#include <memory>      // std::shared_ptr
#include <thread>      // std::thread
#include <chrono>      // std::chrono
#include <atomic>      // std::atomic<bool>
#include <csignal>     // signal()

// gRPC
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

// 自定义组件
#include "logger.hpp"
#include "database_pool.h"
#include "worker_manager.h"
#include "task_repository.h"
#include "scheduler_service_impl.h"
#include "scheduler_loop.h"

// 全局 gRPC 服务器指针，供信号处理函数使用
std::unique_ptr<grpc::Server> g_grpc_server = nullptr;

// 信号处理
void HandleSignal(int signum) {
    std::cout << "\n[Main] 捕获到信号 " << signum << "。开始关闭服务器..." << std::endl;
    if (g_grpc_server) {
        g_grpc_server->Shutdown();
    }
}

// 巡检线程函数
void RunPatrolLoop(std::shared_ptr<WorkerManager> worker_manager,
                   std::shared_ptr<TaskRepository> task_repo,
                   std::atomic<bool>* stop_flag) {
    std::cout << "[PatrolLoop] 巡检循环已启动。" << std::endl;
    while (!(*stop_flag)) {
        for (int i = 0; i < 10 && !(*stop_flag); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        if (*stop_flag) break;

        std::cout << "[PatrolLoop] 正在执行巡检..." << std::endl;
        try {
            auto dead_workers = worker_manager->PruneDeadWorkers();
            for (const auto& worker_id : dead_workers) {
                task_repo->RequeueOrphanedTasks(worker_id);
            }
        } catch (const std::exception& e) {
            std::cerr << "[PatrolLoop] 巡检时发生异常: " << e.what() << std::endl;
        }
    }
    std::cout << "[PatrolLoop] 巡检循环已终止。" << std::endl;
}

// ---------------- main ----------------
int main(int argc, char** argv) {
    std::cout << "[Main] Scheduler 进程启动中..." << std::endl;

    // 1. 注册信号
    signal(SIGINT, HandleSignal);
    signal(SIGTERM, HandleSignal);

    // 2. 组件初始化
    std::shared_ptr<DatabasePool>   db_pool;
    std::shared_ptr<WorkerManager>  worker_manager;
    std::shared_ptr<TaskRepository> task_repo;
    std::shared_ptr<SchedulerLoop>  scheduler_loop;
    std::unique_ptr<SchedulerServiceImpl> service_impl;

    std::thread patrol_thread;
    std::atomic<bool> patrol_stop_flag{false};

    try {
        const char* env_conn_string = std::getenv("DATABASE_URL"); // 替换为您的环境变量名

        std::string conn_string;

        if (env_conn_string != nullptr) {
            conn_string = env_conn_string;
        } else {
            // 如果环境变量未设置，这通常是一个致命错误，程序无法启动
            LOG(FATAL) << "关键环境变量 'DATABASE_URL' 未设置。程序无法启动。";
            return 1; // 或者抛出一个运行时错误
        }
        
        // 2. *** 关键：创建池, 大小为 10 ***
        db_pool = std::make_shared<DatabasePool>(conn_string, 10);
        worker_manager = std::make_shared<WorkerManager>();
        task_repo      = std::make_shared<TaskRepository>(db_pool);
        scheduler_loop = std::make_shared<SchedulerLoop>(task_repo, worker_manager);

        // 3. gRPC 服务器
        std::string server_address = "0.0.0.0:9090";
        service_impl = std::make_unique<SchedulerServiceImpl>(worker_manager, task_repo);

        grpc::EnableDefaultHealthCheckService(true);
        grpc::ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(service_impl.get());

        g_grpc_server = builder.BuildAndStart();
        if (!g_grpc_server) {
            throw std::runtime_error("gRPC 服务器启动失败");
        }
        std::cout << "[gRPC] 服务器已启动，监听地址: " << server_address << std::endl;

        // 4. 启动后台服务
        scheduler_loop->Start();
        patrol_thread = std::thread(RunPatrolLoop, worker_manager, task_repo, &patrol_stop_flag);

    } catch (const std::exception& e) {
        std::cerr << "[Main] 初始化失败: " << e.what() << std::endl;
        return 1;
    }

    // 5. 阻塞等待信号
    std::cout << "[Main] Scheduler 启动完成。按 Ctrl+C 停止。" << std::endl;
    g_grpc_server->Wait();

    // 6. 优雅停机
    std::cout << "[Main] 开始执行优雅停机..." << std::endl;
    if (scheduler_loop) scheduler_loop->Stop();

    patrol_stop_flag = true;
    if (patrol_thread.joinable()) patrol_thread.join();

    std::cout << "[Main] 停机完成。再见。" << std::endl;
    return 0;
}