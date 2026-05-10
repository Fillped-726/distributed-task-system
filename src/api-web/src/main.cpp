#include "api_web.hpp"
#include "grpc_client.hpp"
#include "logger.hpp"
#include "utils/dts_metrics.h"

#include <signal.h>
#include <cstdlib>

// 全局指针，以便信号处理程序可以访问它
std::unique_ptr<dts::api::ApiServer> g_server = nullptr;

// 信号处理
void SignalHandler(int signum) {
  // 使用 Glog 输出警告
  LOG(WARNING) << "Caught signal (" << signum << "). Stopping Web Server...";
  if (g_server) {
    g_server->stop();  // 调用线程安全的 stop()
  }
}

int main(int argc, char** argv) {
  // 1. 日志初始化 (自动适配 Docker/本地 模式)
  dts::InitGlog(argv[0]);
  dts::SetRequestId("web-startup");  // 设置初始上下文

  // 注册信号
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  dts::Metrics::Instance().Start("9102");

  try {
    // 2. 初始化 gRPC 客户端 (连接到 DTS Scheduler)
    const char* env_sched_addr = std::getenv("SERVER_ADDR");
    std::string server_addr =
        env_sched_addr ? env_sched_addr : "localhost:9090";

    auto grpc_client = std::make_shared<dts::GrpcClient>(server_addr);
    LOG_INFO << "gRPC Client initialized. Target Scheduler: " << server_addr;

    // 3. 初始化 Web API 服务器
    // Docker 容器内默认监听 0.0.0.0
    const std::string api_host = "0.0.0.0";
    int api_port = 8080;

    // 支持通过环境变量修改对外端口
    if (const char* env_port = std::getenv("WEB_PORT")) {
      api_port = std::stoi(env_port);
    }

    LOG_INFO << "Starting Web API on " << api_host << ":" << api_port;
    g_server =
        std::make_unique<dts::api::ApiServer>(grpc_client, api_host, api_port);

    // 4. 运行服务器 (阻塞直到 stop 被调用)
    g_server->run();

    // 当 run() 返回时
    LOG_INFO << "API Server stopped gracefully.";

  } catch (const std::exception& e) {
    // 捕获严重错误并退出
    LOG_FATAL << "Web API Startup Error: " << e.what();
    return 1;
  }

  return 0;
}