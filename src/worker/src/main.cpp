#include "worker_node.h"
#include "task_registry.h"
#include "logger.hpp"
#include "uuid_generator.hpp"
#include "utils/dts_metrics.h"

#include <iostream>
#include <thread>
#include <csignal>
#include <cstdlib>

std::unique_ptr<dts::worker::WorkerNode> g_worker_node;

void SignalHandler(int signum) {
  LOG(WARNING) << "Caught signal " << signum << ", shutting down worker...";
  if (g_worker_node) {
    g_worker_node->Stop();
  }
}

int main(int argc, char** argv) {
  // 1. 日志初始化 (适配新版 logger.hpp)
  dts::InitGlog(argv[0]);

  // 2. Worker ID 生成
  std::string base_id = (argc > 1) ? argv[1] : "worker-node";
  std::string worker_id =
      base_id + "-" + dts::common::generate();  // 使用 uuid_generator

  dts::SetRequestId("startup-" + worker_id);

  dts::Metrics::Instance().Start("9101");

  // 3. 配置加载
  // 监听地址: 默认为 0.0.0.0 以便容器外可访问 (虽然 Worker 主要是 Client
  // 角色，但如果有回调或状态检查需要监听)
  std::string bind_addr = "0.0.0.0:50051";
  if (const char* env_p = std::getenv("WORKER_BIND_ADDR")) {
    bind_addr = env_p;
  } else if (argc > 2) {
    bind_addr = argv[2];
  }

  std::string advertise_addr = bind_addr;
  if (const char* env_p = std::getenv("WORKER_ADVERTISE_ADDR")) {
    advertise_addr = env_p;
  }

  // Scheduler 地址: 容器中通常通过服务名访问，例如 "scheduler:9090"
  std::string scheduler_addr = "localhost:9090";  // 本地默认值
  if (const char* env_p = std::getenv("SCHEDULER_ADDR")) {
    scheduler_addr = env_p;
  }

  LOG(INFO) << "========================================";
  LOG(INFO) << "   Worker ID:      " << worker_id;
  LOG(INFO) << "   Bind Addr:      " << bind_addr;       // 实际监听
  LOG(INFO) << "   Advertise Addr: " << advertise_addr;  // 告诉 Scheduler 的
  LOG(INFO) << "   Scheduler:      " << scheduler_addr;
  LOG(INFO) << "========================================";

  try {
    g_worker_node = std::make_unique<dts::worker::WorkerNode>(
        worker_id, bind_addr, scheduler_addr, advertise_addr);

    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);

    g_worker_node->Start();
    g_worker_node->Await();

  } catch (const std::exception& e) {
    LOG(FATAL) << "Worker Exception: " << e.what();
    return 1;
  }

  LOG(INFO) << "Worker exited cleanly.";
  return 0;
}