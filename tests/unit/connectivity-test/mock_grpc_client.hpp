#include "gmock/gmock.h"
#include "grpc_client.hpp" // 你的 GrpcClient 头文件

class MockGrpcClient : public dts::GrpcClient {
public:
    // 使用 MOCK_METHOD 宏来模拟 submit_dag_sync 方法
    MOCK_METHOD(dts::service::SubmitDagResponse, submit_dag_sync,
                (const dts::service::SubmitDagRequest& request), (override));
};