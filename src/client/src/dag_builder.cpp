#include "dag_builder.hpp"
#include "converters.hpp" // (!! 假设 TaskToProto 转换器也已更新 !!)
#include <stdexcept>
#include <utility> 

namespace dts {
namespace client {

// --- 构造函数 (已修正) ---
DagBuilder::DagBuilder(std::string idempotency_key)
    : idempotency_key_(std::move(idempotency_key)) {
    
    if (idempotency_key_.empty()) {
        throw std::invalid_argument("idempotency_key 不能为空");
    }
}

// --- AddTask (已修正) ---
dts::Task& DagBuilder::AddTask(
    const std::string& natural_id, // <-- (约定) 命名已修正
    const std::string& func_name,
    nlohmann::json func_params
) {
    if (tasks_map_.count(natural_id)) {
        throw std::runtime_error("任务 natural_id 重复: " + natural_id);
    }

    dts::Task task;
    
    // --- (!! 关键修正 !!) ---
    // (假设 task.hpp 中有 natural_id 和 task_id_uuid 两个字段)
    
    // 1. 将 "task_A" 存入 C++ 结构体的 natural_id 字段
    task.natural_id = natural_id; 
    
    // 2. (重要) task.task_id_uuid 字段保持为空！
    //    它将由后端的 TaskSubmitter (gRPC 服务) 生成。
    // task.task_id_uuid = ""; // (默认即为空)
    
    task.func_name = func_name;
    task.func_params = std::move(func_params);
    
    // 插入并获取新插入元素的引用
    auto [it, success] = tasks_map_.emplace(natural_id, std::move(task));
    return it->second;
}

// --- AddDependency (已修正) ---
void DagBuilder::AddDependency(const std::string& parent_natural_id, const std::string& child_natural_id) {
    if (parent_natural_id == child_natural_id) {
           throw std::runtime_error("不能添加自依赖: " + parent_natural_id);
    }
    if (!tasks_map_.count(parent_natural_id)) {
        throw std::runtime_error("父节点不存在: " + parent_natural_id);
    }
    if (!tasks_map_.count(child_natural_id)) {
        throw std::runtime_error("子节点不存在: " + child_natural_id);
    }
    
    task_edges_.push_back({parent_natural_id, child_natural_id});
}

// --- BuildProto (已修正) ---
dts::service::SubmitDagRequest DagBuilder::BuildProto() const {
    
    // (!! 关键假设: 你的 .proto 文件也必须被修改 !!)
    dts::service::SubmitDagRequest pb_req;
    
    // (已修正) 1. 设置幂等键 (不再设置 job_id)
    pb_req.set_idempotency_key(idempotency_key_);

    // 2. 转换 C++ dts::Task -> Protobuf dts::task::Task
    for (const auto& [id, task] : tasks_map_) {
        // (!! 假设 TaskToProto 会正确转换 natural_id 和 func_name 等字段)
        TaskToProto(task, pb_req.add_tasks());
    }

    // 3. 转换 C++ Edge 结构 -> Protobuf dts::service::TaskEdge
    for (const auto& edge : task_edges_) {
        auto* edge_pb = pb_req.add_edges();
        
        // (正确) 将 "task_A", "task_B" 这样的 natural_id 发送给后端
        edge_pb->set_parent_natural_id(edge.parent_natural_id);
        edge_pb->set_child_natural_id(edge.child_natural_id);
    }

    return pb_req;
}

} // namespace client
} // namespace dts