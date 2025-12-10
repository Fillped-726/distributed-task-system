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
#include "utils/utils.hpp"

#include "redis/RedisManager.hpp"
#include "redis/RedisConfig.hpp"

using dts::api_server::TaskSubmitter;
using dts::common::DatabasePool;
using dts::common::redis::RedisConfig;
using dts::common::redis::RedisManager;

std::unique_ptr<AsyncServer> g_server;
static std::atomic<bool> g_shutdown{false};

static void signal_handler(int sig) {
  LOG(WARNING) << "Caught signal " << sig << ", shutting down...";
  g_shutdown = true;
  if (g_server) g_server->Shutdown();
}

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
  // 3. [新增] Redis 初始化 (带重试)
  // ---------------------------------------------------------------------------
  // 必须在 submitter 创建之前完成，因为 handleSubmitDag 会用到 RedisManager
  dts::common::utils::InitWithRetry("Redis", [&]() {
    auto conf = RedisConfig::LoadFromEnv();
    RedisManager::GetInstance().Initialize(conf);
  });

  // ---------------------------------------------------------------------------
  // 4. 服务组件初始化
  // ---------------------------------------------------------------------------
  auto submitter = std::make_shared<TaskSubmitter>();

  uint16_t port = 45403;
  if (const char* p = std::getenv("DTS_PORT"))
    port = static_cast<uint16_t>(std::stoi(p));

  g_server = std::make_unique<AsyncServer>(db_pool);

  // 5. 注册 Handler
  g_server->SetSubmitTaskHandler([submitter](std::shared_ptr<DatabasePool> pool,
                                             PbSubmitDagRequest* req_pb,
                                             PbSubmitDagResponse* resp_pb) {
    // 设置 RequestID
    dts::SetRequestId(
        "req-" +
        std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count()));

    // 如果是 Debug 模式，可以打印具体的 JSON
    // LOG(INFO) << "Handle SubmitTask. JobID: " << req_pb->job_id();
    std::optional<dts::api_server::DagCommitContext> ctx_opt;

    try {
      // 执行事务
      pool->ExecuteTx([&](pqxx::work& tx) {
        ctx_opt = submitter->PersistDagToDB(*req_pb, tx);
      });

      if (ctx_opt) {
        resp_pb->mutable_header();
        submitter->DispatchDagToRedis(*req_pb, *ctx_opt);
        LOG(INFO) << "SubmitTask finished successfully.";
      } else {
        auto* err = resp_pb->mutable_header()->mutable_error();
        err->set_sys(dts::error::SYS_IDEMPOTENT);
        err->set_msg("Idempotency conflict or DB logic error");
        LOG(WARNING) << "SubmitTask returned false (Idempotency conflict?).";
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