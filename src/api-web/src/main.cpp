#include "api_web.hpp"       
#include "grpc_client.hpp"    
#include <iostream>
#include <signal.h>           // 用于优雅地关闭 (Ctrl+C)

// 全局指针，以便信号处理程序可以访问它
std::unique_ptr<dts::api::ApiServer> g_server = nullptr;

// Ctrl+C (SIGINT) 信号处理程序
void SignalHandler(int signum) {
    std::cout << "\n捕获到中断信号 (" << signum << ")。" << std::endl;
    if (g_server) {
        g_server->stop(); // 调用线程安全的 stop()
    }
}

int main() {
    // 注册信号处理程序
    signal(SIGINT, SignalHandler);

    try {
        // 1. 初始化 gRPC 客户端 (连接到 DTS 后端服务)
        const std::string dts_service_addr = "localhost:50051"; // <-- 您的 gRPC 服务地址
        auto grpc_client = std::make_shared<dts::GrpcClient>(dts_service_addr);
        std::cout << "[Main] gRPC 客户端已连接到 " << dts_service_addr << std::endl;

        // 2. 初始化 API 服务器 (注入 gRPC 客户端, 监听前端)
        const std::string api_host = "0.0.0.0";
        const int api_port = 8080; // <-- 您的 API 服务地址
        g_server = std::make_unique<dts::api::ApiServer>(grpc_client, api_host, api_port);

        // 3. 运行服务器 (此调用将阻塞，直到调用 g_server->stop())
        g_server->run();

        // 当 run() 返回时 (因为 stop() 被调用了)
        std::cout << "[Main] API 服务器已优雅关闭。" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "[Main] 启动时发生致命错误: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}