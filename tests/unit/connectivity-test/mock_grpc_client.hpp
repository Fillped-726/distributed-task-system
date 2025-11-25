#pragma once
#include "gmock/gmock.h"
#include "grpc_client.hpp" // 确保这个路径是正确的

class MockGrpcClient : public dts::GrpcClient {
public:
    // 构造函数，用于调用基类 dts::GrpcClient 的构造函数
    MockGrpcClient() : dts::GrpcClient("dummy-mock-address") {}

    // 模拟我们之前设为 virtual 的函数
    MOCK_METHOD(dts::service::SubmitDagResponse, submit_dag_sync,
                (const dts::service::SubmitDagRequest& request), (override));
};