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
    json j = json::object();
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

PbTaskState StateToProto(dts::TaskState state) {
    // 如果你的 Enum 定义数值是一一对应的，直接强转：
    return static_cast<PbTaskState>(static_cast<int>(state));
    
    // 如果不一致，请用 switch-case 映射
}

void TaskToProto(const dts::Task& task, PbTask* proto) {

    // (!! 关键 !!)
    // 此时 task.task_id (UUID) 是空的，这是正确的。
    // 我们必须发送 natural_id，否则服务端无法构建 map。
    proto->set_task_id(task.task_id);         // 发送空字符串
    proto->set_natural_id(task.natural_id);   // (!! 关键 !!) 发送 "task_A"
    
    proto->set_client_id(task.client_id);
    proto->set_priority(task.priority);
    proto->set_state(static_cast<PbTaskState>(task.state)); 
    proto->set_func_name(task.func_name);

    JsonToStruct(task.func_params, proto->mutable_func_params());
    
    // (假设 .proto 中 Resource 和 Shard 是嵌套 message)
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
    
    // (!! 关键 !!)
    // 客户端在提交时不需要设置这个，但在 TaskFromProto 中必须有
    proto->set_pending_dependencies(task.pending_dependencies);
}

//
// (已修正) Proto Task -> C++ Task
// (由 TaskSubmitter, Scheduler, Worker 在 服务端 调用)
//
Task TaskFromProto(const PbTask& proto) {
    Task task;
    
    // (!! 关键 !!)
    task.task_id     = proto.task_id();     // (UUID)
    task.natural_id  = proto.natural_id();  // ("task_A")
    // ---
    
    task.client_id   = proto.client_id();
    task.priority    = proto.priority();
    task.state       = static_cast<TaskState>(proto.state());
    task.func_name   = proto.func_name();

    task.func_params = StructToJson(proto.func_params());
    task.required.cpu_core = proto.required().cpu_core();
    task.required.mem_mb   = proto.required().mem_mb();
    task.shard.shard_id    = proto.shard().shard_id();
    task.shard.total_shards = proto.shard().total_shards();

    task.timeout_ms  = proto.timeout_ms();
    task.max_retry   = proto.max_retry();
    task.retry_count = proto.retry_count();
    task.submit_ts   = proto.submit_ts();
    task.start_ts    = proto.start_ts();
    task.finish_ts   = proto.finish_ts();

    task.result      = StructToJson(proto.result());
    task.error_msg   = proto.error_msg();
    
    // (!! 关键 !!)
    task.pending_dependencies = proto.pending_dependencies();

    return task;
}

//
// (已修正) Proto Request -> C++ Request (上下文对象)
// (由 TaskSubmitter 在 服务端 调用)
//
CppSubmitDagRequest ConvertPbFromDagRequest(const PbSubmitDagRequest* req_pb) {
    
    CppSubmitDagRequest cpp_req;

    if (!req_pb) {
        // (LOG 警告是好的)
        return cpp_req;
    }

    // (!! 关键修正 !!)
    // 1. 转换幂等键 (不再有 job_def_id 或 business_id)
    cpp_req.idempotency_key = req_pb->idempotency_key();

    // 2. 转换 'repeated Task' (节点列表)
    cpp_req.tasks.reserve(req_pb->tasks_size());
    for (const PbTask& task_pb : req_pb->tasks()) {
        // TaskFromProto 现在会正确填充 C++ Task (包含 natural_id)
        cpp_req.tasks.push_back(TaskFromProto(task_pb)); 
    }

    // 3. 转换 'repeated TaskEdge' (边列表)
    cpp_req.edges.reserve(req_pb->edges_size());
    for (const PbTaskEdge& edge_pb : req_pb->edges()) { 
        // (这部分是正确的, "parent_id" 是 "task_A")
        cpp_req.edges.push_back({
            edge_pb.parent_natural_id(), 
            edge_pb.child_natural_id()
        });
    }

    return cpp_req;
}

} // namespace dts