#pragma once
#include <grpcpp/grpcpp.h>
#include <functional>
#include "dts/task/task.pb.h"
#include "dts/service/task_service.pb.h"

namespace dts {

// 类型别名
using PbTask = ::dts::task::Task;
using PbSubmitDagRequest = ::dts::service::SubmitDagRequest;
using SubmitDagResponse = ::dts::service::SubmitDagResponse;
using CancelTaskRequest = ::dts::service::CancelTaskRequest;
using CancelTaskResponse = ::dts::service::CancelTaskResponse;
using QueryTaskResponse = ::dts::service::QueryTaskResponse;
using QueryTaskRequest = ::dts::service::QueryTaskRequest;
using SubscribeRequest = ::dts::service::SubscribeRequest;
using TaskResult = ::dts::service::TaskResult;
using TaskService = ::dts::service::TaskService;

// 前置声明
class Task;
class GrpcClient;
using DagCallback = std::function<void(const SubmitDagResponse&, const grpc::Status&)>;
} // namespace dts