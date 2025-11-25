#include "gtest/gtest.h"
#include "httplib.h" // 包含 httplib

// Pimpl 测试技巧：包含 .cpp 文件以获取 Impl 类的完整定义
#include "../../../../src/api-web/src/api_web.cpp" 

#include "mock_grpc_client.hpp" // 包含我们的 Mock 类

// GTest/GMock 命名空间
using ::testing::_;
using ::testing::Return;
using ::testing::Throw;

// 测试桩 (Test Fixture)
class ApiServerTest : public ::testing::Test {
protected:
    // FIX: 重新添加这行被我误删的声明
    std::shared_ptr<MockGrpcClient> mock_client;
    
    std::unique_ptr<dts::api::ApiServerImpl> api_impl;

    void SetUp() override {
        // 1. 创建 Mock 实例
        mock_client = std::make_shared<MockGrpcClient>(); // <-- 这行现在可以工作了

        // 2. 注入 Mock 实例
        api_impl = std::make_unique<dts::api::ApiServerImpl>(
            mock_client, "test.host", 1234);
    }
};

// --- 测试用例 ---

// 测试用例 1: 成功的流程
TEST_F(ApiServerTest, HandleDagSubmit_Success) {
    httplib::Request req;
    httplib::Response res;

    // 1. 准备输入 (使用一个有效的 JSON)
    req.body = R"({
        "idempotency_key": "key1",
        "tasks": [
            {"task_id": "A", "func_name": "f1"}
        ]
    })";

    // 2. 准备 Mock 的返回值
    dts::service::SubmitDagResponse fake_response;
    fake_response.mutable_header()->set_code(0);
    fake_response.mutable_header()->set_msg("Success");

    // 3. 设置期望：
    EXPECT_CALL(*mock_client, submit_dag_sync(_)) // <-- 这行现在可以工作了
        .Times(1)
        .WillOnce(Return(fake_response));

    // 4. 执行被测函数
    api_impl->handle_dag_submit(req, res);

    // 5. 断言结果
    ASSERT_EQ(res.status, 200);
    ASSERT_TRUE(res.body.find("Success") != std::string::npos);
}

// 测试用例 2: JSON 解析失败
TEST_F(ApiServerTest, HandleDagSubmit_BadJson) {
    httplib::Request req;
    httplib::Response res;
    req.body = "{ invalid json: }"; // 错误的 JSON

    // 期望：gRPC 客户端根本不应该被调用
    EXPECT_CALL(*mock_client, submit_dag_sync(_)).Times(0); // <-- 这行现在可以工作了

    // 执行
    api_impl->handle_dag_submit(req, res);

    // 断言：返回 400 错误
    ASSERT_EQ(res.status, 400);
    ASSERT_TRUE(res.body.find("JSON 格式无效") != std::string::npos);
}

// 测试用例 3: gRPC 传输失败
TEST_F(ApiServerTest, HandleDagSubmit_GrpcTransportError) {
    httplib::Request req;
    httplib::Response res;
    
    // 1. 准备有效的输入
    req.body = R"({
        "idempotency_key": "key2",
        "tasks": [
            {"task_id": "B", "func_name": "f2"}
        ]
    })";

    // 2. 期望：当 gRPC 客户端被调用时，它抛出一个 runtime_error
    EXPECT_CALL(*mock_client, submit_dag_sync(_)) // <-- 这行现在可以工作了
        .Times(1)
        .WillOnce(Throw(std::runtime_error("Connection refused")));

    // 3. 执行
    api_impl->handle_dag_submit(req, res);

    // 4. 断言：返回 503 错误
    ASSERT_EQ(res.status, 503);
    ASSERT_TRUE(res.body.find("gRPC 传输错误") != std::string::npos);
}