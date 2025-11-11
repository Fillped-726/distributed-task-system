#include "api_web.hpp"

// --- 1. 包含所有第三方和内部实现 ---
#include "httplib.h"           // HTTP 服务器库
#include "nlohmann/json.hpp"   // JSON 解析库

#include "grpc_client.hpp"     // 完整的 GrpcClient 定义
#include "dag_builder.hpp"       // 完整的 DagBuilder 定义
#include "task.hpp"              // 包含 C++ dts::Task 定义
#include "exceptions.hpp" // 包含 GrpcError

#include <iostream>
#include <stdexcept>
#include <thread>

namespace dts {
namespace api {

// 使用 nlohmann::json 的别名
using json = nlohmann::json;

// --- 2. 辅助函数 (在 .cpp 文件中保持 static) ---

/**
 * @brief 解析 JSON 对象中的字段并填充到 C++ dts::Task 引用中。
 * (这个 C++ dts::Task 是由 DagBuilder::AddTask 返回的引用)
 */
static void ParseJsonToTask(const json& j_task, dts::Task& task_ref) {
    
    // 我们在这里解析并填充 DagBuilder 不知道的额外字段
    task_ref.priority = j_task.value("priority", 0);

    if (j_task.contains("required")) {
        const auto& j_req = j_task.at("required");
        task_ref.required.cpu_core = j_req.value("cpu_core", 0.0);
        task_ref.required.mem_mb = j_req.value("mem_mb", 0);
    }
    
    // ... 在此添加对 shard, timeout_ms, max_retry 等的解析 ...
    task_ref.timeout_ms = j_task.value("timeout_ms", 0);
    task_ref.max_retry = j_task.value("max_retry", 0);
}

/**
 * @brief 格式化一个标准的 JSON 错误响应
 */
static void SetJsonError(httplib::Response& res, const std::string& error_msg, int status_code) {
    json j_err;
    j_err["error"] = error_msg;
    res.status = status_code;
    res.set_content(j_err.dump(), "application/json");
}

class ApiServer::ApiServerImpl {
public:
    // --- 成员变量 ---
    std::shared_ptr<dts::GrpcClient> grpc_client_;
    httplib::Server svr_;
    std::string host_;
    int port_;

    // --- 构造函数 (初始化) ---
    ApiServerImpl(std::shared_ptr<dts::GrpcClient> grpc_client,
                  const std::string& host,
                  int port)
        : grpc_client_(std::move(grpc_client)), 
          host_(host), 
          port_(port) 
    {
        if (!grpc_client_) {
            throw std::invalid_argument("ApiServer: GrpcClient 不能为空。");
        }
        
        // 注册 HTTP 路由和处理程序
        svr_.Post("/api/v1/dag", [this](const httplib::Request& req, httplib::Response& res) {
            this->handle_dag_submit(req, res);
        });
        
        // (您可以在此添加其他路由, 例如 /api/v1/dag/{job_id}/status)
    }

    // --- 析构函数 ---
    ~ApiServerImpl() {
        stop();
    }
    
    // --- 核心 HTTP 处理逻辑 ---
    void handle_dag_submit(const httplib::Request& req, httplib::Response& res) {
        try {
            // 1. 解析 JSON
            json j_body = json::parse(req.body);

            // 2. 验证和提取 (使用 .at()，如果缺少会抛出 json::exception)
            const std::string key = j_body.at("idempotency_key").get<std::string>();
            const std::string job_id = j_body.value("job_id", ""); // .value() 允许可选
            const auto& j_tasks = j_body.at("tasks");

            // 3. 使用 DagBuilder
            // (在栈上创建，天然线程安全)
            dts::client::DagBuilder builder(job_id, key);

            //  3a. 添加任务
            for (const auto& j_task : j_tasks) {
                // DagBuilder 创建任务并返回引用
                dts::Task& task_ref = builder.AddTask(
                    j_task.at("task_id").get<std::string>(),
                    j_task.at("func_name").get<std::string>(),
                    j_task.value("func_params", json()) // 可选
                );
                
                // 辅助函数填充剩余字段 (priority, required, ...)
                ParseJsonToTask(j_task, task_ref);
            }

            //  3b. 添加依赖 (可选)
            if (j_body.contains("edges")) {
                for (const auto& j_edge : j_body.at("edges")) {
                    builder.AddDependency(
                        j_edge.at("parent").get<std::string>(),
                        j_edge.at("child").get<std::string>()
                    );
                }
            }

            // 4. 构建 Protobuf 请求
            dts::service::SubmitDagRequest pb_req = builder.BuildProto();

            // 5. 调用 gRPC
            dts::service::SubmitDagResponse pb_resp = grpc_client_->submit_dag_sync(pb_req);

            // 6. 格式化并返回成功响应
            json j_resp;
            j_resp["header"]["code"] = pb_resp.header().code();
            j_resp["header"]["msg"] = pb_resp.header().msg();
            j_resp["header"]["request_id"] = pb_resp.header().request_id();
            j_resp["job_id"] = pb_resp.job_def_id();
            
            res.status = 200;
            res.set_content(j_resp.dump(), "application/json");

        // --- 错误处理 ---
        } catch (const json::exception& e) {
            // JSON 解析或 .at() 失败
            SetJsonError(res, "JSON 格式无效或缺少必填字段: " + std::string(e.what()), 400); // 400 Bad Request
        } catch (const dts::GrpcError& e) {
            // DagBuilder 验证失败 (例如: "任务 ID 重复")
            // GrpcClient 业务失败 (例如: "Server rejected DAG")
            SetJsonError(res, "DAG 构建或提交失败: " + std::string(e.what()), 400); // 400 Bad Request
        } catch (const std::runtime_error& e) {
            // gRPC 传输失败 (例如: "连接被拒绝")
            SetJsonError(res, "gRPC 传输错误: " + std::string(e.what()), 503); // 503 Service Unavailable
        } catch (const std::exception& e) {
            // 捕获所有其他异常
            SetJsonError(res, "未知的内部服务器错误: " + std::string(e.what()), 500); // 500 Internal Server Error
        }
    }

    // --- 生命周期实现 ---
    void run() {
        std::cout << "[ApiServer] API 服务启动于 http://" << host_ << ":" << port_ << std::endl;
        // svr_.listen 是阻塞调用
        if (!svr_.listen(host_.c_str(), port_)) {
            throw std::runtime_error("无法在 " + host_ + ":" + std::to_string(port_) + " 启动 HTTP 服务器");
        }
    }

    void stop() {
        if (svr_.is_running()) {
            std::cout << "[ApiServer] 正在停止 API 服务..." << std::endl;
            svr_.stop();
        }
    }
};


// --- 4. ApiServer 公共方法的实现 ---
//    (这些是 .h 文件中声明的，它们只是调用 pimpl_)

ApiServer::ApiServer(std::shared_ptr<dts::GrpcClient> grpc_client,
                   const std::string& host,
                   int port)
    // 创建 pimpl_ 实例
    : pimpl_(std::make_unique<ApiServerImpl>(std::move(grpc_client), host, port))
{
    // 构造函数体
}

ApiServer::~ApiServer() {
    // pimpl_ 的析构函数会自动被调用，它会调用 svr_.stop()
    // std::unique_ptr 会自动 delete pimpl_
}

void ApiServer::run() {
    pimpl_->run();
}

void ApiServer::stop() {
    pimpl_->stop();
}

} // namespace api
} // namespace dts