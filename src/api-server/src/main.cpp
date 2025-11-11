
#include "postgres_connection.h" 
#include "logger.hpp"
#include "task_submitter.hpp"    
#include "converters.hpp"        
#include "api_server.hpp"        

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
    dts::SetRequestId("server_startup"); // 设置一个启动日志ID

    // 2. 信号注册
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // 3. (新!) 初始化核心共享资源：数据库连接
    // 我们使用 shared_ptr 来管理数据库连接的生命周期
    std::shared_ptr<PostgresConnection> db_conn;
    try {
        db_conn = std::make_shared<PostgresConnection>();
        LOG(INFO) << "数据库连接池初始化成功。";

    } catch (const std::exception& e) {
        LOG(FATAL) << "数据库连接失败，服务启动终止: " << e.what();
        return 1; // 启动失败
    }

    //提交实例
    auto submitter = std::make_shared<TaskSubmitter>();

    // 4. 读端口
    uint16_t port = 0;
    if (const char* p = std::getenv("DTS_PORT")) port = static_cast<uint16_t>(std::stoi(p));

    // 5. 启动服务器, 注入共享资源
    g_server = std::make_unique<AsyncServer>(db_conn); // <-- 依赖注入

    g_server->SetSubmitTaskHandler(
        [submitter](
            std::shared_ptr<PostgresConnection> conn, 
            PbSubmitDagRequest* req_pb,                
            PbSubmitDagResponse* resp_pb)              
        {
            LOG(INFO) << "SubmitTaskHandler (智能路由) 被调用...";
            bool success = false;
            
            try {
                // 3.1 转换 gRPC -> C++ 结构体
                CppSubmitDagRequest cpp_req = dts::ConvertPbFromDagRequest(req_pb);        
                    
                LOG(INFO) << "检测到完整 DAG, job_def_id: " << cpp_req.job_def_id 
                            << ", " << cpp_req.tasks.size() << " 任务, " 
                            << cpp_req.edges.size() << " 边。";

                // (这是我们最开始写的完整 DAG 提交逻辑)
                success = submitter->handleSubmitDag(
                    cpp_req, 
                    conn->get_connection()
                );
                
                // (设置 gRPC 响应 - 仅为示例)
                // resp_pb->set_job_def_id(cpp_req.job_def_id); 
                

                // 3.3 设置通用响应头
                if (success) {
                    resp_pb->mutable_header()->set_code(0);
                    resp_pb->mutable_header()->set_msg("DAG Submitted");
                } else {
                    resp_pb->mutable_header()->set_code(1); 
                    resp_pb->mutable_header()->set_msg("Idempotency conflict or DB error");
                }

            } catch (const std::exception& e) {
                LOG(ERROR) << "SubmitTaskHandler 发生严重异常: " << e.what();
                resp_pb->mutable_header()->set_code(-1);
                resp_pb->mutable_header()->set_msg(e.what());
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