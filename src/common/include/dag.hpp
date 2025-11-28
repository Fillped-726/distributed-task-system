#pragma once

#include <string>
#include <vector>
#include <sstream>

#include "task.hpp"
#include <nlohmann/json.hpp>

namespace dts {

/**
 * @brief 任务依赖关系（边）
 * 使用 natural_id (业务ID) 而不是 task_id (系统UUID) 来定义边。
 * 原因：在提交 DAG 时，用户还不知道系统生成的 task_id
 * 是什么，只能用自己定义的名称来指代。
 */
struct TaskEdge {
  std::string parent_natural_id;  // 前置任务的业务ID
  std::string child_natural_id;   // 后置任务的业务ID

  // 序列化宏
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(TaskEdge, parent_natural_id, child_natural_id)
};

/**
 * @brief DAG 提交请求对象
 * 包含一组任务和它们的依赖关系，用于原子性地提交整个工作流。
 */
struct SubmitDagRequest {
  std::string job_id;           // 属于哪个大作业
  std::string idempotency_key;  // 幂等键 (防止网络抖动导致重复提交整个图)

  std::vector<dts::Task> tasks;      // 所有的节点
  std::vector<dts::TaskEdge> edges;  // 所有的边

  // 辅助方法：生成日志摘要 (供 glog 使用)
  // 避免直接打印巨大的 JSON 导致日志刷屏
  std::string ShortDebugString() const {
    std::stringstream ss;
    ss << "[SubmitDagRequest] JobID: " << job_id
       << ", IdempotencyKey: " << idempotency_key << ", Tasks: " << tasks.size()
       << ", Edges: " << edges.size();
    return ss.str();
  }

  // 序列化宏
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(SubmitDagRequest, job_id, idempotency_key,
                                 tasks, edges)
};

}  // namespace dts