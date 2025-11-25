#include "api_web.hpp"

// --- 1. 包含所有第三方和内部实现 ---
#include "httplib.h"          
#include "nlohmann/json.hpp"   

#include "grpc_client.hpp"    
#include "dag_builder.hpp"       
#include "task.hpp"              
#include "exceptions.hpp" 

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
    
    // 1. 填充顶层字段
    task_ref.priority = j_task.value("priority", 0);
    task_ref.timeout_ms = j_task.value("timeout_ms", 30000);
    task_ref.max_retry = j_task.value("max_retry", 3);
    
    // 2. 自动转换嵌套的 JSON 对象 (得益于 task.hpp 中的 NLOHMANN_DEFINE_TYPE_INTRUSIVE)
    if (j_task.contains("required")) {
        // 将 JSON "required" 对象 自动映射到 task_ref.required 结构体
        j_task.at("required").get_to(task_ref.required);
    }
    
    if (j_task.contains("shard")) {
        // 将 JSON "shard" 对象 自动映射到 task_ref.shard 结构体
        j_task.at("shard").get_to(task_ref.shard);
    }
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

class ApiServerImpl {
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
        svr_.Post("/api/v1/job/submit", [this](const httplib::Request& req, httplib::Response& res) {
            this->handle_dag_submit(req, res);
        });
        
        // (您可以在此添加其他路由, 例如 /api/v1/dag/{job_id}/status)
    }

    // --- 析构函数 ---
    ~ApiServerImpl() {
        stop();
    }
    
    // --- 核心 HTTP 处理逻辑 ---
    // (你的 dts::api 命名空间内)

    void handle_dag_submit(const httplib::Request& req, httplib::Response& res) {
        try {
            // 1. 解析 JSON
            json j_body = json::parse(req.body);

            // 2. (已修正) 提取幂等键
            const std::string idempotency_key = j_body.at("idempotency_key").get<std::string>();
            
            // (已移除) 不再需要解析 job_id 或使用 std::stoll

            const auto& j_tasks = j_body.at("tasks");
            const auto& j_edges = j_body.value("edges", json::array()); // "edges" 是可选的

            // 3. (已修正) 使用 DagBuilder (现在是 C++ 结构体构建器)
            
            // (!!! 假设 DagBuilder 构造函数已修改为只接收 idempotency_key)
            dts::client::DagBuilder builder(idempotency_key);

            //  3a. 添加任务 (填充 C++ 结构体)
            for (const auto& j_task : j_tasks) {
                // (!!! 假设 AddTask 已修改为返回 dts::Task& 引用)
                dts::Task& task_ref = builder.AddTask(
                    // "task_id" 来自 JSON (e.g., "task_A")
                    j_task.at("natural_id").get<std::string>(), 
                    
                    // "func_name" 来自 JSON
                    j_task.at("func_name").get<std::string>(),
                    
                    // "func_params" 来自 JSON
                    j_task.value("func_params", json()) 
                );
                
                // 自动填充 required, shard, priority 等...
                ParseJsonToTask(j_task, task_ref);
            }

            //  3b. 添加依赖 (填充 C++ 结构体)
            for (const auto& j_edge : j_edges) {
                builder.AddDependency(
                    j_edge.at("parent").get<std::string>(),
                    j_edge.at("child").get<std::string>()
                );
            }

            // 4. (已修正) 构建 Protobuf 请求
            // (!!! 假设 BuildProto() 现在会正确处理 UUID 和 natural_id)
            dts::service::SubmitDagRequest pb_req = builder.BuildProto();

            // 5. 调用 gRPC (TaskSubmitter 服务)
            // (!!! 假设 pb_resp.job_id() 现在返回 std::string)
            dts::service::SubmitDagResponse pb_resp = grpc_client_->submit_dag_sync(pb_req);

            // 6. (已修正) 格式化并返回成功响应
            json j_resp;
            const auto& header = pb_resp.header(); // 获取 header

            // -----------------------------------------------------
            // *** 关键修改在这里 ***
            // -----------------------------------------------------

            if (header.has_error()) {
                // 失败: 'error' 字段存在
                const auto& err = header.error();
                
                // (你需要一个逻辑把 oneof 错误码 转换为一个 int code)
                // 这是一个示例逻辑:
                int32_t error_code = -1; // 默认为未知
                
                switch (err.code_case()) {
                    case dts::error::Error::kSys:
                        // 假设 SysErr 1, 2, 3...
                        error_code = static_cast<int32_t>(err.sys());
                        break;
                    case dts::error::Error::kJob:
                        // 假设 JobErr 1, 2...
                        // (你可以给它们一个偏移量, 比如 10000)
                        error_code = 10000 + static_cast<int32_t>(err.job());
                        break;
                    case dts::error::Error::CODE_NOT_SET:
                        // 'error' 存在, 但 'code' 没设置
                        error_code = dts::error::SYS_INTERNAL; // 视为内部错误
                        break;
                }
                
                j_resp["header"]["code"] = error_code;
                j_resp["header"]["msg"] = err.msg(); // 从 'error' 对象获取 msg

            } else {
                // 成功: 'error' 字段不存在
                j_resp["header"]["code"] = 0; // 0 代表成功
                j_resp["header"]["msg"] = "Success";
            }
            
            // (已移除) request_id, 应该由 TaskSubmitter 在 header.msg 中提供
            
            // (已修正) 直接赋值 std::string (UUID)，不再使用 std::to_string
            j_resp["job_id"] = pb_resp.job_id(); 
            
            res.status = 200;
            res.set_content(j_resp.dump(), "application/json");

        // --- 错误处理 (已修正) ---
        } catch (const json::exception& e) {
            SetJsonError(res, "JSON 格式无效或缺少必填字段: " + std::string(e.what()), 400); 
        
        } catch (const std::invalid_argument& e) {
            // (现在用于 DagBuilder 内部的验证，例如 "parent 任务不存在")
            SetJsonError(res, "DAG 逻辑无效: " + std::string(e.what()), 400); 
            
        } catch (const dts::GrpcError& e) {
            SetJsonError(res, "DAG 构建或提交失败: " + std::string(e.what()), 400); 
        } catch (const std::runtime_error& e) {
            SetJsonError(res, "gRPC 传输错误: " + std::string(e.what()), 503); 
        } catch (const std::exception& e) {
            SetJsonError(res, "未知的内部服务器错误: " + std::string(e.what()), 500);
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