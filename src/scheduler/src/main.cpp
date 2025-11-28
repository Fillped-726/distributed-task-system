#include <iostream>
#include <memory>   // std::shared_ptr
#include <thread>   // std::thread
#include <chrono>   // std::chrono
#include <atomic>   // std::atomic<bool>
#include <csignal>  // signal()
#include <cstdlib>  // std::getenv

// gRPC
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "logger.hpp"
#include "database_pool.h"
#include "worker_manager.h"
#include "task_repository.h"
#include "scheduler_loop.h"
#include "scheduler_service_impl.h"

// 使用命名空间简化代码
using dts::common::DatabasePool;
using dts::scheduler::SchedulerLoop;
using dts::scheduler::SchedulerServiceImpl;
using dts::scheduler::TaskRepository;
using dts::scheduler::WorkerManager;

// 全局 gRPC 服务器指针，供信号处理函数使用
std::unique_ptr<grpc::Server> g_grpc_server = nullptr;

// 信号处理
void HandleSignal(int signum) {
  // 这里不能用 LOG，因为信号处理函数中应尽量避免复杂操作
  // 但 Glog 是异步安全的，通常没问题。保险起见用 write 或者 cout
  std::cout << "\n[System] Captured signal " << signum << ". Shutting down..."
            << std::endl;
  if (g_grpc_server) {
    g_grpc_server->Shutdown();
  }
}

// 巡检线程函数 (Fail-over 逻辑)
void RunPatrolLoop(std::shared_ptr<WorkerManager> worker_manager,
                   std::shared_ptr<TaskRepository> task_repo,
                   std::atomic<bool>* stop_flag) {
  // 给该线程设置日志上下文
  dts::SetRequestId("PATROL");
  LOG_INFO << "PatrolLoop started.";

  while (!(*stop_flag)) {
    // 1. 休眠 (每 10 秒巡检一次)
    // 使用小步睡眠，以便能快速响应停止信号
    for (int i = 0; i < 100; ++i) {
      if (*stop_flag) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (*stop_flag) break;

    // 2. 执行巡检
    try {
      // 找出并移除死掉的 Worker
      auto dead_workers = worker_manager->PruneDeadWorkers();

      if (!dead_workers.empty()) {
        LOG_WARN << "Found " << dead_workers.size()
                 << " dead workers. Recovering tasks...";

        for (const auto& worker_id : dead_workers) {
          // 恢复该 Worker 上的任务
          int recovered = task_repo->RequeueOrphanedTasks(worker_id);
          LOG_WARN << "Recovered " << recovered
                   << " tasks from dead worker: " << worker_id;
        }
      } else {
        // 如果想看心跳日志，可以用 LOG_EVERY_N
        // LOG_EVERY_N(INFO, 10) << "Patrol check passed. Cluster is healthy.";
      }

    } catch (const std::exception& e) {
      LOG_ERROR << "PatrolLoop exception: " << e.what();
    }
  }
  LOG_INFO << "PatrolLoop terminated.";
}

// ---------------- main ----------------
int main(int argc, char** argv) {
  // 1. 初始化日志系统 (必须在最前面)
  dts::InitGlog(argv[0], "./logs");

  LOG_INFO << "Scheduler process starting...";

  // 2. 注册信号
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  // 3. 准备组件容器
  std::shared_ptr<DatabasePool> db_pool;
  std::shared_ptr<WorkerManager> worker_manager;
  std::shared_ptr<TaskRepository> task_repo;
  std::shared_ptr<SchedulerLoop> scheduler_loop;
  std::unique_ptr<SchedulerServiceImpl> service_impl;

  std::thread patrol_thread;
  std::atomic<bool> patrol_stop_flag{false};

  try {
    // 4. 读取环境变量
    const char* env_conn_string = std::getenv("DATABASE_URL");
    std::string conn_string;

    if (env_conn_string != nullptr) {
      conn_string = env_conn_string;
    } else {
      // 默认值 (方便本地调试)
      conn_string = "postgresql://postgres:password@localhost:5432/dts_db";
      LOG_WARN << "DATABASE_URL not set. Using default: " << conn_string;
    }

    // 5. 依赖注入与组件初始化
    LOG_INFO << "Initializing components...";

    // DatabasePool (10 连接)
    db_pool = std::make_shared<DatabasePool>(conn_string, 10);

    // WorkerManager (Worker 超时时间 30s)
    worker_manager = std::make_shared<WorkerManager>(std::chrono::seconds(30));

    // TaskRepository
    task_repo = std::make_shared<TaskRepository>(db_pool);

    // SchedulerLoop
    scheduler_loop = std::make_shared<SchedulerLoop>(task_repo, worker_manager);

    // Service Implementation
    service_impl =
        std::make_unique<SchedulerServiceImpl>(worker_manager, task_repo);

    // 6. 启动 gRPC 服务器
    std::string server_address = "0.0.0.0:9090";

    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service_impl.get());

    g_grpc_server = builder.BuildAndStart();
    if (!g_grpc_server) {
      throw std::runtime_error("Failed to start gRPC server");
    }
    LOG_INFO << "gRPC Server listening on " << server_address;

    // 7. 启动后台线程 (Scheduler Loop & Patrol)
    scheduler_loop->Start();

    patrol_thread = std::thread(RunPatrolLoop, worker_manager, task_repo,
                                &patrol_stop_flag);

    // 8. 阻塞等待
    LOG_INFO << "Scheduler is running. Press Ctrl+C to stop.";
    g_grpc_server->Wait();  // <--- 阻塞在这里，直到 Shutdown 被调用

  } catch (const std::exception& e) {
    LOG_FATAL << "Main initialization failed: " << e.what();
    return 1;
  }

  // 9. 优雅停机 (Graceful Shutdown)
  LOG_INFO << "Starting graceful shutdown...";

  // 先停止调度，不再分发新任务
  if (scheduler_loop) {
    scheduler_loop->Stop();
  }

  // 停止巡检
  patrol_stop_flag = true;
  if (patrol_thread.joinable()) {
    patrol_thread.join();
  }

  LOG_INFO << "Shutdown complete. Bye.";
  return 0;
}