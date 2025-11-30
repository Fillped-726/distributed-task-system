#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include <atomic>
#include <csignal>
#include <cstdlib>

// gRPC
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "logger.hpp"
#include "database_pool.h"
#include "worker_manager.h"
#include "task_repository.h"
#include "scheduler_loop.h"
#include "scheduler_service_impl.h"

using dts::common::DatabasePool;
using dts::scheduler::SchedulerLoop;
using dts::scheduler::SchedulerServiceImpl;
using dts::scheduler::TaskRepository;
using dts::scheduler::WorkerManager;

std::unique_ptr<grpc::Server> g_grpc_server = nullptr;

void HandleSignal(int signum) {
  // 信号处理中尽量只做简单操作
  if (g_grpc_server) {
    g_grpc_server->Shutdown();
  }
}

// 巡检线程 (保持不变)
void RunPatrolLoop(std::shared_ptr<WorkerManager> worker_manager,
                   std::shared_ptr<TaskRepository> task_repo,
                   std::atomic<bool>* stop_flag) {
  dts::SetRequestId("PATROL");
  LOG_INFO << "PatrolLoop started.";

  while (!(*stop_flag)) {
    for (int i = 0; i < 100; ++i) {
      if (*stop_flag) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(70));
    }
    if (*stop_flag) break;

    try {
      auto dead_workers = worker_manager->PruneDeadWorkers();
      if (!dead_workers.empty()) {
        LOG_WARN << "Found " << dead_workers.size()
                 << " dead workers. Recovering tasks...";
        for (const auto& worker_id : dead_workers) {
          int recovered = task_repo->RequeueOrphanedTasks(worker_id);
          LOG_WARN << "Recovered " << recovered
                   << " tasks from dead worker: " << worker_id;
        }
      }
    } catch (const std::exception& e) {
      LOG_ERROR << "PatrolLoop exception: " << e.what();
    }
  }
  LOG_INFO << "PatrolLoop terminated.";
}

int main(int argc, char** argv) {
  // 1. 日志初始化 (适配新版 logger.hpp，不传路径参数)
  dts::InitGlog(argv[0]);

  LOG_INFO << "Scheduler process starting...";

  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  std::shared_ptr<DatabasePool> db_pool;
  std::shared_ptr<WorkerManager> worker_manager;
  std::shared_ptr<TaskRepository> task_repo;
  std::shared_ptr<SchedulerLoop> scheduler_loop;
  std::unique_ptr<SchedulerServiceImpl> service_impl;

  std::thread patrol_thread;
  std::atomic<bool> patrol_stop_flag{false};

  try {
    // 2. 读取环境变量
    const char* env_conn_string = std::getenv("DATABASE_URL");
    std::string conn_string =
        (env_conn_string)
            ? env_conn_string
            : "postgresql://postgres:password@localhost:5432/dts_db";

    // 3. [关键优化] 数据库连接重试循环
    // 在 Docker 中，DB 启动可能比 APP 慢，需要等待
    int max_retries = 30;  // 尝试 30 次，每次 1 秒
    for (int i = 0; i < max_retries; ++i) {
      try {
        db_pool = std::make_shared<DatabasePool>(conn_string, 120);
        LOG_INFO << "Database connection established.";
        break;
      } catch (const std::exception& e) {
        if (i == max_retries - 1) {
          LOG_FATAL << "Failed to connect to DB after " << max_retries
                    << " attempts. Exiting.";
          return 1;
        }
        LOG_WARN << "Waiting for Database... (" << (i + 1) << "/" << max_retries
                 << "): " << e.what();
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    }

    // 4. 组件初始化
    LOG_INFO << "Initializing components...";
    worker_manager = std::make_shared<WorkerManager>(std::chrono::seconds(30));
    task_repo = std::make_shared<TaskRepository>(db_pool);
    scheduler_loop = std::make_shared<SchedulerLoop>(task_repo, worker_manager);
    service_impl =
        std::make_unique<SchedulerServiceImpl>(worker_manager, task_repo);

    // 5. 启动 gRPC (支持环境变量端口)
    std::string server_address = "0.0.0.0:9090";
    if (const char* p = std::getenv("SCHEDULER_PORT")) {
      server_address = "0.0.0.0:" + std::string(p);
    }

    grpc::EnableDefaultHealthCheckService(true);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(service_impl.get());

    g_grpc_server = builder.BuildAndStart();
    if (!g_grpc_server) {
      throw std::runtime_error("Failed to start gRPC server");
    }
    LOG_INFO << "gRPC Server listening on " << server_address;

    scheduler_loop->Start();
    patrol_thread = std::thread(RunPatrolLoop, worker_manager, task_repo,
                                &patrol_stop_flag);

    LOG_INFO << "Scheduler is running. Press Ctrl+C to stop.";
    g_grpc_server->Wait();

  } catch (const std::exception& e) {
    LOG_FATAL << "Main initialization failed: " << e.what();
    return 1;
  }

  // 优雅退出
  LOG_INFO << "Starting graceful shutdown...";
  if (scheduler_loop) scheduler_loop->Stop();

  patrol_stop_flag = true;
  if (patrol_thread.joinable()) patrol_thread.join();

  LOG_INFO << "Shutdown complete. Bye.";
  return 0;
}