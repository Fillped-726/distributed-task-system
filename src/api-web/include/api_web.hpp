#pragma once

#include <string>
#include <memory> // For std::unique_ptr and std::shared_ptr

// --- 前向声明 (Forward Declarations) ---
// 我们不需要在这里包含 grpc_client.hpp，
// 只需要告诉编译器这个类存在即可。
namespace dts {
    class GrpcClient;
}

namespace dts {
namespace api {

/**
 * @brief 运行一个 HTTP/REST API 服务器。
 * * 它接收来自 Web 前端的 JSON 请求，使用 DagBuilder 构建它们，
 * 并通过注入的 GrpcClient 将它们转发到后端的 DTS gRPC 服务。
 *
 * (此类使用 PIMPL 模式来隐藏实现细节)
 */
class ApiServer {
public:
    /**
     * @brief 构造 API 服务器。
     * @param grpc_client 一个已初始化的、线程安全的 GrpcClient 共享指针。
     * @param host 要监听的主机/IP (例如 "0.0.0.0")。
     * @param port 要监听的端口 (例如 8080)。
     */
    ApiServer(std::shared_ptr<dts::GrpcClient> grpc_client,
              const std::string& host,
              int port);

    /**
     * @brief 析构函数。
     * 会自动停止服务器。
     */
    ~ApiServer();

    // --- 生命周期 ---

    /**
     * @brief 启动服务器并开始监听 (阻塞调用)。
     * 在当前线程上运行。
     * @throws std::runtime_error 如果服务器启动失败。
     */
    void run();

    /**
     * @brief 停止服务器 (线程安全)。
     * 可以从另一个线程或信号处理程序调用。
     */
    void stop();


    // --- 规则：使其不可拷贝和不可移动 ---
    // 因为它管理着一个活跃的服务器实例和可能的线程。
    ApiServer(const ApiServer&) = delete;
    ApiServer& operator=(const ApiServer&) = delete;
    ApiServer(ApiServer&&) = delete;
    ApiServer& operator=(ApiServer&&) = delete;

private:
    // PIMPL 模式：指向私有实现的指针。
    // ApiServerImpl 的定义将在 .cpp 文件中。
    class ApiServerImpl;
    std::unique_ptr<ApiServerImpl> pimpl_;
};

} // namespace api
} // namespace dts