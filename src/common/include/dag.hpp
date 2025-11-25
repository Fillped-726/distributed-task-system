// dag.hpp

#pragma once

#include "task.hpp" 
#include <string>
#include <vector>

namespace dts {

/**
 * @brief C++ 运行时的任务依赖关系（边）
 */
struct TaskEdge {
    std::string parent_natural_id;
    std::string child_natural_id;
};

/**
 * @brief C++ 运行时的 DAG 提交请求对象（图）
 * * 这是从 gRPC 的 Protobuf 对象转换而来，
 * 用于 C++ 内部的业务逻辑（如 TaskSubmitter）。
 */
struct SubmitDagRequest {
    std::string job_id;
    std::string idempotency_key;

    std::vector<dts::Task> tasks;

    std::vector<dts::TaskEdge> edges;
};

} // namespace dts