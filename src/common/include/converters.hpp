#pragma once
#include "dts/task/task.pb.h"
#include "dag.hpp"
#include <nlohmann/json.hpp>
#include <glog/logging.h>
#include "dts/service/task_service.pb.h" 
#include "dts/task/task.pb.h"          

// 1. C++ 运行时结构体 (我们的领域对象)
using CppSubmitDagRequest = dts::SubmitDagRequest;
using CppTaskEdge = dts::TaskEdge;
using CppTask = dts::Task;

// 2. Protobuf 数据传输对象 (gRPC DTOs)
using PbSubmitDagRequest = dts::service::SubmitDagRequest;
using PbTaskEdge = dts::task::TaskEdge;
using PbTask = dts::task::Task;
using PbTaskState = ::dts::task::TaskState;

namespace dts {

using json = nlohmann::json;

// 辅助：nlohmann::json ↔ google::protobuf::Struct
void JsonToStruct(const json& j, google::protobuf::Struct* proto);
json StructToJson(const google::protobuf::Struct& proto);

// Task 类型转换
void TaskToProto(const dts::Task& task, PbTask* proto);
CppTask TaskFromProto(const PbTask& proto);

CppSubmitDagRequest ConvertPbFromDagRequest(const PbSubmitDagRequest* req_pb);

} // namespace dts