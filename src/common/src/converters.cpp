#include "converters.hpp"

namespace dts {

void JsonToStruct(const json& j, google::protobuf::Struct* proto) {
    proto->Clear();
    for (auto& [k, v] : j.items()) {
        google::protobuf::Value pv;
        if (v.is_boolean())       pv.set_bool_value(v);
        else if (v.is_number())   pv.set_number_value(v);
        else if (v.is_string())   pv.set_string_value(v);
        else if (v.is_null()) pv.set_null_value(google::protobuf::NULL_VALUE);
        else if (v.is_array() || v.is_object()) {
            JsonToStruct(v, pv.mutable_struct_value());
        }
        (*proto->mutable_fields())[k] = std::move(pv);
    }
}

json StructToJson(const google::protobuf::Struct& proto) {
    json j;
    for (auto& [k, v] : proto.fields()) {
        switch (v.kind_case()) {
            case google::protobuf::Value::kBoolValue:   j[k] = v.bool_value(); break;
            case google::protobuf::Value::kNumberValue: j[k] = v.number_value(); break;
            case google::protobuf::Value::kStringValue: j[k] = v.string_value(); break;
            case google::protobuf::Value::kStructValue: j[k] = StructToJson(v.struct_value()); break;
            default: break;
        }
    }
    return j;
}

void TaskToProto(const dts::Task& task, PbTask* proto) {

    proto->set_task_id(task.task_id);
    proto->set_client_id(task.client_id);
    proto->set_priority(task.priority);
    proto->set_state(static_cast<PbTaskState>(task.state)); 
    proto->set_func_name(task.func_name);

    JsonToStruct(task.func_params, proto->mutable_func_params());
    proto->mutable_required()->set_cpu_core(task.required.cpu_core);
    proto->mutable_required()->set_mem_mb(task.required.mem_mb);
    proto->mutable_shard()->set_shard_id(task.shard.shard_id);
    proto->mutable_shard()->set_total_shards(task.shard.total_shards);

    proto->set_timeout_ms(task.timeout_ms);
    proto->set_max_retry(task.max_retry);
    proto->set_retry_count(task.retry_count);
    proto->set_submit_ts(task.submit_ts);
    proto->set_start_ts(task.start_ts);
    proto->set_finish_ts(task.finish_ts);

    JsonToStruct(task.result, proto->mutable_result());
    proto->set_error_msg(task.error_msg);

}

Task TaskFromProto(const PbTask& proto) {
    Task task;
    task.task_id     = proto.task_id();
    task.client_id   = proto.client_id();
    task.priority    = proto.priority();
    task.state       = static_cast<TaskState>(proto.state());
    task.func_name   = proto.func_name();

    task.func_params = StructToJson(proto.func_params());
    task.required.cpu_core = proto.required().cpu_core();
    task.required.mem_mb   = proto.required().mem_mb();
    task.shard.shard_id     = proto.shard().shard_id();
    task.shard.total_shards = proto.shard().total_shards();

    task.timeout_ms  = proto.timeout_ms();
    task.max_retry   = proto.max_retry();
    task.retry_count = proto.retry_count();
    task.submit_ts   = proto.submit_ts();
    task.start_ts    = proto.start_ts();
    task.finish_ts   = proto.finish_ts();

    task.result      = StructToJson(proto.result());
    task.error_msg   = proto.error_msg();
    return task;
}

CppSubmitDagRequest ConvertPbFromDagRequest(const PbSubmitDagRequest* req_pb) {
    
    // 1. 创建 C++ 运行时对象 (输出)
    CppSubmitDagRequest cpp_req;

    if (!req_pb) {
        LOG(WARNING) << "ConvertPbfromDagRequest 接收到一个空指针。返回空请求。";
        return cpp_req; // 返回一个默认构造的空 C++ 请求
    }

    // 2. 复制顶层简单字段
    cpp_req.job_def_id = req_pb->job_def_id();
    cpp_req.business_id = req_pb->business_id();

    // 3. 转换 'repeated Task' (节点列表)
    //    (从 PbTask 列表 转换为 CppTask 列表)
    cpp_req.tasks.reserve(req_pb->tasks_size());
    for (const PbTask& task_pb : req_pb->tasks()) { // <--- 更新了类型
        cpp_req.tasks.push_back(TaskFromProto(task_pb)); 
    }

    // 4. 转换 'repeated TaskEdge' (边列表)
    //    (从 PbTaskEdge 列表 转换为 CppTaskEdge 列表)
    cpp_req.edges.reserve(req_pb->edges_size());
    for (const PbTaskEdge& edge_pb : req_pb->edges()) { 
        
        cpp_req.edges.push_back({
            edge_pb.parent_id(), 
            edge_pb.child_id()
        });
    }

    // 5. 返回完整的 C++ 运行时对象
    return cpp_req;
}

} // namespace dts