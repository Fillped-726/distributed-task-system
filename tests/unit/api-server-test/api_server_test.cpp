#include "api_server.hpp"
#include "logger.hpp"
#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>

using namespace dts;
using namespace ::testing;

/* ---------- 测试套件 ---------- */
class AsyncServerTest : public Test {
protected:
    static void SetUpTestSuite() {
        LOG(INFO) << "[TestSuite] Starting AsyncServer ...";
        server_ = std::make_unique<AsyncServer>();
        
        // [FIX 1] 修正 Handler 的签名和实现
        server_->SetSubmitTaskHandler(
            [](dts::service::SubmitTaskRequest* req, SubmitTaskResponse* resp) {
                resp->mutable_task()->CopyFrom(req->task());
                resp->mutable_task()->set_state(dts::task::SUCCESS);
                VLOG(1) << "[Handler] task_id=" << req->task().task_id(); // 只有 -v=1 才可见
            });
            
        server_->Run(0);
        channel_ = grpc::CreateChannel(
            "127.0.0.1:" + std::to_string(server_->ListenPort()),
            grpc::InsecureChannelCredentials());
        stub_ = TaskService::NewStub(channel_);
        LOG(INFO) << "[TestSuite] AsyncServer ready on port "
                  << server_->ListenPort();
    }

    static void TearDownTestSuite() {
        LOG(INFO) << "[TestSuite] Shutting down AsyncServer ...";
        server_->Shutdown();
    }

    /* 方便后续每个 TEST 快速拿 request_id */
    static std::string NextReqId() {
        static std::atomic<uint64_t> id{1};
        return "gtest-" + std::to_string(id++);
    }

    /* 静态成员不变 */
    static std::unique_ptr<AsyncServer> server_;
    static std::shared_ptr<grpc::Channel> channel_;
    static std::unique_ptr<TaskService::Stub> stub_;
};

/* ---------- 静态定义 ---------- */
std::unique_ptr<AsyncServer> AsyncServerTest::server_;
std::shared_ptr<grpc::Channel> AsyncServerTest::channel_;
std::unique_ptr<TaskService::Stub> AsyncServerTest::stub_;

/* ---------- 最小端到端通过性 ---------- */
TEST_F(AsyncServerTest, SubmitTaskEchoAndSuccess) {
    std::string req_id = NextReqId();
    LOG(INFO) << "[TEST] EchoAndSuccess req_id=" << req_id;

    // [FIX 2] 创建 Task 并将其放入 Request 包装器
    Task task_data;
    task_data.set_task_id(req_id);
    
    SubmitTaskRequest req;
    req.mutable_task()->CopyFrom(task_data);

    SubmitTaskResponse resp;
    grpc::ClientContext ctx;
    grpc::Status st = stub_->SubmitTask(&ctx, req, &resp); // 传递包装器 req
    
    ASSERT_TRUE(st.ok()) << st.error_message();
    EXPECT_EQ(resp.task().task_id(), req_id);
    
    // [FIX 3] 修正枚举命名空间
    EXPECT_EQ(resp.task().state(), dts::task::SUCCESS);
}

/* ---------- 预留扩展：异常、并发、流式 ---------- */
TEST_F(AsyncServerTest, SubmitTaskTimeout) {
    // [FIX 2] 创建 Task 并将其放入 Request 包装器
    Task task_data;
    task_data.set_task_id("timeout");

    SubmitTaskRequest req;
    req.mutable_task()->CopyFrom(task_data);
    
    SubmitTaskResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::milliseconds(1)); // 极短超时
                     
    auto st = stub_->SubmitTask(&ctx, req, &resp); // 传递包装器 req
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
}

TEST_F(AsyncServerTest, ConcurrentSubmit) {
    constexpr int kThreads = 4;
    constexpr int kReqPerThread = 400;
    std::atomic<int> ok_count{0};
    auto worker = [&](int tid) {
        for (int i = 0; i < kReqPerThread; ++i) {
            std::string req_id = NextReqId();
            
            // [FIX 2] 创建 Task 并将其放入 Request 包装器
            Task task_data;
            task_data.set_task_id(req_id);

            SubmitTaskRequest req;
            req.mutable_task()->CopyFrom(task_data);
            
            SubmitTaskResponse resp;
            grpc::ClientContext ctx;
            
            // [FIX 3] 修正枚举命名空间
            if (stub_->SubmitTask(&ctx, req, &resp).ok() &&
                resp.task().state() == dts::task::SUCCESS) {
                ok_count++;
            } else {
                LOG(ERROR) << "[Concurrent] failed req_id=" << req_id;
            }
        }
    };

    std::vector<std::thread> ths;
    for (int t = 0; t < kThreads; ++t) ths.emplace_back(worker, t);
    for (auto& th : ths) th.join();

    LOG(INFO) << "[Concurrent] total=" << kThreads * kReqPerThread
              << " ok=" << ok_count.load();
    EXPECT_EQ(ok_count.load(), kThreads * kReqPerThread);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    dts::InitGlog(argv[0], true /* =unit-test */); // 只打 ERROR 到 stderr
    return RUN_ALL_TESTS();
}