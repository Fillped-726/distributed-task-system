#pragma once

#include "dag.hpp" 
#include "dts/service/task_service.pb.h" // 包含 gRPC/Protobuf 生成的头文件
#include <nlohmann/json.hpp> // 包含 nlohmann::json
#include <string>
#include <vector>
#include <map>

namespace dts {
namespace client {

/**
 * @brief 帮助客户端构建和验证 DAG 提交流。
 *
 * 提供了链式 API 来添加任务和定义它们之间的依赖关系,
 * 最后生成一个可用于 gRPC 调用的 SubmitDagRequest Protobuf 对象。
 */
class DagBuilder {
public:
    /**
     * @brief 构造一个新的 DAG Builder。
     * @param job_id 可选的作业 ID，如果为空，服务端将生成。
     * @param idempotency_key 必填，用于保证提交的幂等性。
     */
    DagBuilder(std::string job_def_id, std::string business_id);

    /**
     * @brief 默认析构函数。
     */
    ~DagBuilder() = default;

    // 禁止拷贝和赋值，因为 Builder 包含状态
    DagBuilder(const DagBuilder&) = delete;
    DagBuilder& operator=(const DagBuilder&) = delete;

    /**
     * @brief 向 DAG 中添加一个新任务。
     *
     * @param task_id 任务的唯一 ID (在 DAG 内)。
     * @param func_name 要执行的函数名称。
     * @param func_params 传递给函数的参数 (JSON 格式)。
     * @return dts::Task 的引用，允许链式修改 (例如: .priority = 10)。
     * @throws std::runtime_error 如果 task_id 重复。
     */
    dts::Task& AddTask(
        const std::string& task_id,
        const std::string& func_name,
        nlohmann::json func_params = nlohmann::json() // 修正：使用 nlohmann::json() 作为默认
    );

    /**
     * @brief 在两个任务之间添加依赖关系（父任务 -> 子任务）。
     *
     * @param parent_id 父任务的 task_id。
     * @param child_id 子任务的 task_id。
     * @throws std::runtime_error 如果任一 task_id 不存在。
     */
    void AddDependency(const std::string& parent_id, const std::string& child_id);

    /**
     * @brief 验证当前状态并构建最终的 Protobuf 请求对象。
     *
     * @return 可用于 gRPC 调用的 dts::service::SubmitDagRequest 对象。
     */
    dts::service::SubmitDagRequest BuildProto() const;

private:
    std::string job_def_id_;
    std::string business_id_;
    std::map<std::string, dts::Task> tasks_map_; // 确保 task_id 唯一
    std::vector<TaskEdge> task_edges_; 
};

} // namespace client
} // namespace dts