#pragma once

#include "dag.hpp" 
#include "dts/service/task_service.pb.h" 
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <map>

namespace dts {
namespace client {

/**
 * @brief (已修正) 帮助 ApiServer 客户端构建 C++ 结构体，
 * 最终生成一个可用于 gRPC 调用的 SubmitDagRequest Protobuf 对象。
 */
class DagBuilder {
public:
    /**
     * @brief (已修正) 构造一个新的 DAG Builder。
     * @param idempotency_key 必填，用于保证提交的幂等性。
     */
    DagBuilder(std::string idempotency_key);

    /**
     * @brief 默认析构函数。
     */
    ~DagBuilder() = default;

    // 禁止拷贝和赋值
    DagBuilder(const DagBuilder&) = delete;
    DagBuilder& operator=(const DagBuilder&) = delete;

    /**
     * @brief (已修正) 向 DAG 中添加一个新任务。
     *
     * @param natural_id 任务的业务 ID (e.g., "task_A")。
     * @param func_name 要执行的函数名称。
     * @param func_params 传递给函数的参数 (JSON 格式)。
     * @return dts::Task 的引用，允许链式修改 (例如: .priority = 10)。
     * @throws std::runtime_error 如果 natural_id 重复。
     */
    dts::Task& AddTask(
        const std::string& natural_id, // <-- (约定) 命名已修正
        const std::string& func_name,
        nlohmann::json func_params = nlohmann::json()
    );

    /**
     * @brief 在两个任务之间添加依赖关系（父任务 -> 子任务）。
     *
     * @param parent_natural_id 父任务的 natural_id (e.g., "task_A")。
     * @param child_natural_id 子任务的 natural_id (e.g., "task_B")。
     * @throws std::runtime_error 如果任一 natural_id 不存在。
     */
    void AddDependency(const std::string& parent_natural_id, const std::string& child_natural_id);

    /**
     * @brief 验证当前状态并构建最终的 Protobuf 请求对象。
     *
     * @return 可用于 gRPC 调用的 dts::service::SubmitDagRequest 对象。
     */
    dts::service::SubmitDagRequest BuildProto() const;

private:
    // (已修正) 不再需要 job_def_id_
    std::string idempotency_key_;
    
    // (正确) Map 的 key 是 "task_A" (natural_id)
    std::map<std::string, dts::Task> tasks_map_; 
    
    // (正确) 存储 "task_A" -> "task_B" 的关系
    std::vector<TaskEdge> task_edges_; 
};

} // namespace client
} // namespace dts