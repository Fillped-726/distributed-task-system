// 包含了 "database_pool.h"
#include "database_pool.h"
#include "logger.hpp"
#include "task_submitter.hpp"
#include "converters.hpp"
#include "api_server.hpp"
#include "dts/error/error.pb.h"
#include "dts/error/sys_error.pb.h"
#include "dts/error/job_error.pb.h"

#include <csignal>
#include <iostream>
#include <memory>

std::unique_ptr<AsyncServer> g_server;
static std::atomic<bool> g_shutdown{false};

static void signal_handler(int sig) {
    LOG(WARNING) << "Caught signal " << sig << ", shutting down...";
    g_shutdown = true;
    if (g_server) g_server->Shutdown();
}

int main(int argc, char* argv[]) {
    // 1. 日志初始化
    dts::InitGlog(argv[0], false);
    dts::SetRequestId("server_startup");

    // 2. 信号注册
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // 3. (*** 关键修改 ***) 初始化核心共享资源：数据库连接池
    std::shared_ptr<DatabasePool> db_pool;

    try {
        // 1. (从 env 获取 conn_string)
        const char* env_conn_string = std::getenv("DATABASE_URL"); // 替换为您的环境变量名

        std::string conn_string;

        if (env_conn_string != nullptr) {
            conn_string = env_conn_string;
        } else {
            // 如果环境变量未设置，这通常是一个致命错误，程序无法启动
            LOG(FATAL) << "关键环境变量 'DATABASE_URL' 未设置。程序无法启动。";
            return 1; // 或者抛出一个运行时错误
        }
        
        // 2. *** 关键：创建池, 大小为 10 ***
        db_pool = std::make_shared<DatabasePool>(conn_string, 10);
        LOG(INFO) << "数据库连接池初始化成功。";

    } catch (const std::exception& e) {
        // 捕获所有标准异常（包括可能的数据库连接失败）
        LOG(FATAL) << "数据库连接池创建失败: " << e.what();
        return 1; 
    }

    //提交实例
    auto submitter = std::make_shared<TaskSubmitter>();

    // 4. 读端口
    uint16_t port = 45403;
    if (const char* p = std::getenv("DTS_PORT")) port = static_cast<uint16_t>(std::stoi(p));

    // 5. (*** 关键修改 ***) 启动服务器, 注入连接池
    g_server = std::make_unique<AsyncServer>(db_pool); // <-- 依赖注入已更新

    // (*** 关键修改 ***)
    // SetSubmitTaskHandler 的 lambda 签名现在接收 DatabasePool
    g_server->SetSubmitTaskHandler(
        [submitter](
            // <--- 类型已更改
            std::shared_ptr<DatabasePool> pool,
            PbSubmitDagRequest* req_pb,
            PbSubmitDagResponse* resp_pb)
        {
            LOG(INFO) << "SubmitTaskHandler (智能路由) 被调用...";
            bool success = false;

            // *** 核心事务逻辑 ***
            try {
                // *** 1. 转换 ***
                CppSubmitDagRequest cpp_req = dts::ConvertPbFromDagRequest(req_pb);
                bool success = false;

                // *** 2. 执行事务 ***
                pool->ExecuteTx([&](pqxx::work& tx) {
                    // (我们现在在事务内部了)
                    success = submitter->handleSubmitDag(cpp_req, tx);
                    // (如果 handleSubmitDag 抛出异常, ExecuteTx 会自动回滚)
                });

                // *** 3. 设置响应 (基于 ExecuteTx 是否抛出异常) ***
                if (success) {
                    resp_pb->mutable_header(); // 成功
                } else {
                    auto* err = resp_pb->mutable_header()->mutable_error();
                    err->set_sys(dts::error::SYS_IDEMPOTENT); 
                    err->set_msg("Idempotency conflict or DB error");
                }
                
            } catch (const std::exception& e) {
                // (ExecuteTx 抛出了异常, 事务已回滚)
                LOG(ERROR) << "SubmitTaskHandler 发生严重异常: " << e.what();
                auto* err = resp_pb->mutable_header()->mutable_error();
                err->set_sys(dts::error::SYS_INTERNAL);
                err->set_msg(e.what());
            }
        }
    );

    g_server->Run(port);
    LOG(INFO) << "AsyncServer running on port " << g_server->ListenPort();

    // 6. 阻塞主线程 (不变)
    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // 7. 优雅退出 (不变)
    g_server->Shutdown();
    LOG(INFO) << "AsyncServer exited cleanly";
    return 0;
}