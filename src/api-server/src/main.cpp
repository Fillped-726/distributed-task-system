#include <csignal>
#include <iostream>
#include <memory>
#include <thread>
#include <chrono>

#include "api_server.hpp"
#include "database_pool.h"
#include "converters.hpp"
#include "dts/error/error.pb.h"
#include "dts/error/job_error.pb.h"
#include "dts/error/sys_error.pb.h"
#include "logger.hpp"
#include "task_submitter.hpp"
#include "job_query_handler.hpp"
#include "utils/utils.hpp"
#include "utils/dts_metrics.h"
#include "redis/RedisManager.hpp"
#include "redis/RedisConfig.hpp"

using dts::api_server::JobQueryHandler;
using dts::api_server::TaskSubmitter;
using dts::common::DatabasePool;
using dts::common::RedisConfig;
using dts::common::RedisManager;

std::unique_ptr<AsyncServer> g_server;
static std::atomic<bool> g_shutdown{false};

static void signal_handler(int sig) {
  LOG(WARNING) << "Caught signal " << sig << ", shutting down...";
  g_shutdown = true;
  if (g_server) g_server->Shutdown();
}

prometheus::Counter* global_rpc_counter = nullptr;

int main(int argc, char* argv[]) {
  // 1. 日志初始化
  dts::InitGlog(argv[0]);
  dts::SetRequestId("server_startup");

  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  std::shared_ptr<DatabasePool> db_pool;

  // ---------------------------------------------------------------------------
  // 2. 数据库连接 (带重试)
  // ---------------------------------------------------------------------------
  const char* env_conn_string = std::getenv("DATABASE_URL");
  if (!env_conn_string) {
    LOG(FATAL) << "ENV 'DATABASE_URL' not set.";
    return 1;
  }
  dts::common::utils::InitWithRetry("Database", [&]() {
    db_pool = std::make_shared<DatabasePool>(env_conn_string, 20);
  });

  // ---------------------------------------------------------------------------
  // 3. Redis 初始化 (带重试)
  // ---------------------------------------------------------------------------
  // 必须在 submitter 创建之前完成，因为 handleSubmitDag 会用到 RedisManager
  dts::common::utils::InitWithRetry("Redis", [&]() {
    auto conf = RedisConfig::LoadFromEnv();
    RedisManager::GetInstance().Initialize(conf);
  });

  // ---------------------------------------------------------------------------
  // 4. 启动监控指标 HTTP Server
  // ---------------------------------------------------------------------------

  dts::Metrics::Instance().Start("9102");

  // ---------------------------------------------------------------------------
  // 5. 服务组件初始化
  // ---------------------------------------------------------------------------
  auto submitter = std::make_shared<TaskSubmitter>(db_pool);
  std::cout << "🔧 Registering metrics explicitly..." << std::endl;
  auto registry = dts::Metrics::Instance().GetRegistry();
  auto& family = prometheus::BuildCounter()
                     .Name("dts_api_server_requests_total")
                     .Help("Total RPC requests")
                     .Register(*registry);

  // 初始化计数器
  global_rpc_counter = &family.Add({{"method", "SubmitTask"}});

  uint16_t port = 45403;
  if (const char* p = std::getenv("DTS_PORT"))
    port = static_cast<uint16_t>(std::stoi(p));

  g_server = std::make_unique<AsyncServer>(db_pool);

  // 5. 注册 Handler
  g_server->SetSubmitTaskHandler([submitter](std::shared_ptr<DatabasePool>,
                                             PbSubmitDagRequest* req_pb,
                                             PbSubmitDagResponse* resp_pb) {
    // A. 设置 RequestID
    dts::SetRequestId(
        "req-" +
        std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count()));

    if (global_rpc_counter) {
      global_rpc_counter->Increment();
    }

    try {
      // =========================================================
      // B. 异步提交逻辑 (Async Submit)
      // =========================================================

      // 1. 提交任务到内存队列
      std::string job_id = submitter->SubmitDagAsync(*req_pb);

      // 记录日志：只记录“接收成功”，不再记录“完成”
      LOG(INFO) << "Task Accepted. JobID: " << job_id;
      resp_pb->set_job_id(job_id);

    } catch (const std::exception& e) {
      // D. 异常处理 (DB 连接断开、Redis 挂了等)
      LOG(ERROR) << "Handler Exception: " << e.what();
      auto* err = resp_pb->mutable_header()->mutable_error();
      err->set_sys(dts::error::SYS_INTERNAL);
      err->set_msg(e.what());
    }
  });

  JobQueryHandler query_handler;

  g_server->SetGetJobStatusHandler([&](auto db, auto req, auto resp) {
    query_handler.Handle(db, req, resp);
  });

  g_server->Run(port);
  LOG(INFO) << "AsyncServer running on port " << g_server->ListenPort();

  while (!g_shutdown) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  g_server->Shutdown();
  LOG(INFO) << "AsyncServer exited cleanly";
  return 0;
}