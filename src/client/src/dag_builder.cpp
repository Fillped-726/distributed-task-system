#include "dag_builder.hpp"
#include "converters.hpp" // 假设 TaskToProto 在这里定义
#include <stdexcept>    // 用于 std::runtime_error
#include <utility>      // 用于 std::move

namespace dts {
namespace client {

// --- 构造函数 ---
DagBuilder::DagBuilder(std::string job_def_id, std::string business_id)
    : job_def_id_(std::move(job_def_id)),
      business_id_(std::move(business_id)) {
    // 幂等键是必需的
    if (business_id_.empty()) {
        throw std::invalid_argument("business_id 不能为空");
    }
}

// --- AddTask ---
dts::Task& DagBuilder::AddTask(
    const std::string& task_id,
    const std::string& func_name,
    nlohmann::json func_params
) {
    if (tasks_map_.count(task_id)) {
        throw std::runtime_error("任务 ID 重复: " + task_id);
    }

    dts::Task task;
    task.task_id = task_id;
    task.func_name = func_name;
    task.func_params = std::move(func_params);
    // 用户可以在返回的引用上设置 .required, .priority 等

    // 插入并获取新插入元素的引用
    auto [it, success] = tasks_map_.emplace(task_id, std::move(task));
    return it->second;
}

// --- AddDependency ---
void DagBuilder::AddDependency(const std::string& parent_id, const std::string& child_id) {
    if (parent_id == child_id) {
         throw std::runtime_error("不能添加自依赖: " + parent_id);
    }
    if (!tasks_map_.count(parent_id)) {
        throw std::runtime_error("父节点不存在: " + parent_id);
    }
    if (!tasks_map_.count(child_id)) {
        throw std::runtime_error("子节点不存在: " + child_id);
    }
    
    // (高级功能：可以在此处添加循环依赖检测)

    task_edges_.push_back({parent_id, child_id});
}

// --- BuildProto ---
dts::service::SubmitDagRequest DagBuilder::BuildProto() const {
    dts::service::SubmitDagRequest pb_req;
    pb_req.set_job_def_id(job_def_id_);
    pb_req.set_business_id(business_id_);

    // 1. 转换 C++ dts::Task -> Protobuf dts::task::Task
    // (假设您有 converters::TaskToProto)
    for (const auto& [id, task] : tasks_map_) {
        // add_tasks() 返回一个 dts::task::Task* 指针
        TaskToProto(task, pb_req.add_tasks());
    }

    // 2. 转换内部 Edge 结构 -> Protobuf dts::service::TaskEdge
    for (const auto& edge : task_edges_) {
        auto* edge_pb = pb_req.add_edges();
        edge_pb->set_parent_id(edge.parent_id);
        edge_pb->set_child_id(edge.child_id);
    }

    return pb_req;
}

} // namespace client
} // namespace dts