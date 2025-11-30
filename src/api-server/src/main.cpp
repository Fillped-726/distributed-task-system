#include <csignal>
#include <iostream>
#include <memory>
#include <thread>  // for sleep
#include <chrono>

#include "api_server.hpp"
#include "database_pool.h"
#include "converters.hpp"
#include "dts/error/error.pb.h"
#include "dts/error/job_error.pb.h"
#include "dts/error/sys_error.pb.h"
#include "logger.hpp"
#include "task_submitter.hpp"

using dts::common::DatabasePool;

std::unique_ptr<AsyncServer> g_server;
static std::atomic<bool> g_shutdown{false};

static void signal_handler(int sig) {
  LOG(WARNING) << "Caught signal " << sig << ", shutting down...";
  g_shutdown = true;
  if (g_server) g_server->Shutdown();
}

int main(int argc, char* argv[]) {
  // 1. 日志初始化 (适配新版 logger.hpp)
  dts::InitGlog(argv[0]);
  dts::SetRequestId("server_startup");

  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  std::shared_ptr<DatabasePool> db_pool;

  try {
    const char* env_conn_string = std::getenv("DATABASE_URL");
    if (!env_conn_string) {
      LOG(FATAL) << "ENV 'DATABASE_URL' not set.";
      return 1;
    }
    std::string conn_string = env_conn_string;

    // 2. [关键优化] 数据库重试连接
    int retries = 0;
    const int max_retries = 30;
    while (true) {
      try {
        db_pool = std::make_shared<DatabasePool>(conn_string, 30);
        LOG(INFO) << "Database connection pool initialized.";
        break;
      } catch (const std::exception& e) {
        if (retries++ >= max_retries) {
          LOG(FATAL) << "Failed to connect to DB: " << e.what();
          return 1;
        }
        LOG(WARNING) << "DB connection failed, retrying in 1s... (" << retries
                     << "/" << max_retries << ")";
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    }

  } catch (const std::exception& e) {
    LOG(FATAL) << "Unexpected error during DB init: " << e.what();
    return 1;
  }

  auto submitter = std::make_shared<TaskSubmitter>();

  // 3. 端口配置
  uint16_t port = 45403;
  if (const char* p = std::getenv("DTS_PORT"))
    port = static_cast<uint16_t>(std::stoi(p));

  g_server = std::make_unique<AsyncServer>(db_pool);

  // 4. 注册 Handler (逻辑保持不变)
  g_server->SetSubmitTaskHandler([submitter](std::shared_ptr<DatabasePool> pool,
                                             PbSubmitDagRequest* req_pb,
                                             PbSubmitDagResponse* resp_pb) {
    dts::SetRequestId("req-" + std::to_string(std::chrono::system_clock::now()
                                                  .time_since_epoch()
                                                  .count()));  // 简单的请求ID
    LOG(INFO) << "Handle SubmitTask";

    try {
      CppSubmitDagRequest cpp_req = dts::ConvertPbFromDagRequest(req_pb);
      bool success = false;
      pool->ExecuteTx([&](pqxx::work& tx) {
        success = submitter->handleSubmitDag(cpp_req, tx);
      });

      if (success) {
        resp_pb->mutable_header();
      } else {
        auto* err = resp_pb->mutable_header()->mutable_error();
        err->set_sys(dts::error::SYS_IDEMPOTENT);
        err->set_msg("Idempotency conflict or DB error");
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << "Handler Exception: " << e.what();
      auto* err = resp_pb->mutable_header()->mutable_error();
      err->set_sys(dts::error::SYS_INTERNAL);
      err->set_msg(e.what());
    }
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