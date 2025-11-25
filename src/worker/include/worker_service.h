#pragma once

#include <functional>
#include <string>
#include <grpcpp/grpcpp.h>
#include "dts/internal/internal_service.grpc.pb.h"
#include "dts/task/task.pb.h"
#include "task.hpp"

namespace dts {
namespace worker {

// 定义一个回调函数类型：当收到 RunTask 请求时调用
// 参数是 Task 对象的引用
using TaskSubmitter = std::function<void(const dts::Task&)>;

class WorkerServiceImpl final : public dts::internal::WorkerService::Service {
public:
    // 构造函数注入“提交者”逻辑，实现完全解耦
    explicit WorkerServiceImpl(TaskSubmitter submitter);
    
    // 实现 gRPC 接口
    grpc::Status RunTask(grpc::ServerContext* context, 
                         const dts::internal::RunTaskRequest* request, 
                         dts::internal::RunTaskResponse* response) override;

private:
    TaskSubmitter submitter_;
};

} // namespace worker
} // namespace dts