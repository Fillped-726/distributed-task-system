#include "gtest/gtest.h"
#include "api_web.hpp" // 技巧：包含 .cpp 以测试 static 函数和 Impl 类
#include "mock_grpc_client.hpp" // 上一步的 Mock

using ::testing::_;
using ::testing::Return;
using ::testing::Throw;

// 测试套件
class ApiServerTest : public ::testing::Test {
protected:
    std::shared_ptr<MockGrpcClient> mock_client;
    std::unique_ptr<dts::api::ApiServer::ApiServerImpl> api_impl;

    void SetUp() override {
        // 1. 创建 Mock 实例
        mock_client = std::make_shared<MockGrpcClient>();

        // 2. 注入 Mock 实例
        // (注意：这里我们直接实例化 Impl 类，而不是 ApiServer)
        api_impl = std::make_unique<dts::api::ApiServer::ApiServerImpl>(
            mock_client, "test.host", 1234);
    }
};

// 测试用例 1: 成功的流程
TEST_F(ApiServerTest, HandleDagSubmit_Success) {
    httplib::Request req;
    httplib::Response res;

    // 1. 准备输入
    req.body = R"({ "idempotency_key": "key1", "tasks": [...] })"; // (省略完整的 JSON)

    // 2. 准备 Mock 的返回值
    dts::service::SubmitDagResponse fake_response;
    fake_response.mutable_header()->set_code(0);
    fake_response.mutable_header()->set_msg("Success");

    // 3. 设置期望：
    // 期望 mock_client 的 submit_dag_sync 会被调用 1 次
    // 当它被调用时，返回我们准备好的 fake_response
    EXPECT_CALL(*mock_client, submit_dag_sync(_))
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
    EXPECT_CALL(*mock_client, submit_dag_sync(_)).Times(0);

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
    req.body = R"({ "idempotency_key": "key2", "tasks": [...] })"; // (省略)

    // 期望：当 gRPC 客户端被调用时，它抛出一个 runtime_error
    EXPECT_CALL(*mock_client, submit_dag_sync(_))
        .Times(1)
        .WillOnce(Throw(std::runtime_error("Connection refused")));

    // 执行
    api_impl->handle_dag_submit(req, res);

    // 断言：返回 503 错误
    ASSERT_EQ(res.status, 503);
    ASSERT_TRUE(res.body.find("gRPC 传输错误") != std::string::npos);
}