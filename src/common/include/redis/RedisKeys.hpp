#pragma once

#include <string>
#include <string_view>
#include <format>  // C++20 标准库，若编译器暂不支持可换成 <fmt/core.h>

namespace dts::common::redis::keys {

// =============================================================================
// Part 1: Field Names (Redis Hash/Stream 内部字段名)
// 这里的字符串是静态的，使用 constexpr string_view 零开销
// =============================================================================
namespace fields {
constexpr std::string_view kPayload = "payload";     // 任务二进制载荷
constexpr std::string_view kTaskId = "task_id";      // 任务ID
constexpr std::string_view kJobId = "job_id";        // 作业ID
constexpr std::string_view kPriority = "priority";   // 优先级
constexpr std::string_view kSubmitTs = "submit_ts";  // 提交时间戳
constexpr std::string_view kWorkerId = "worker_id";  // 执行节点ID
}  // namespace fields

// =============================================================================
// Part 2: Task Dispatching (Stream 相关)
// 核心调度队列
// =============================================================================
namespace stream {
// Stream 主键
constexpr std::string_view kTasks = "dts:stream:tasks";

// 默认消费者组
constexpr std::string_view kGroupMain = "dts:group:main";
}  // namespace stream

// =============================================================================
// Part 3: Runtime State (Hash/String 相关)
// 运行时状态、心跳、临时存储
// =============================================================================
namespace state {

// 前缀定义 (内部使用)
constexpr std::string_view kPrefixWorker = "dts:worker:";
constexpr std::string_view kPrefixTaskMeta = "dts:task:meta:";

/**
 * @brief 生成 Worker 心跳 Key (String / Hash)
 * 用于服务发现和健康检查
 * Example: "dts:worker:scheduler-01"
 */
inline std::string WorkerKey(std::string_view worker_name) {
  return std::format("{}{}", kPrefixWorker, worker_name);
}

/**
 * @brief 生成任务元数据 Key (Hash)
 * 用于存储任务的实时状态、进度等
 * Example: "dts:task:meta:1001-abc-uuid"
 */
inline std::string TaskMetaKey(std::string_view task_id) {
  return std::format("{}{}", kPrefixTaskMeta, task_id);
}
}  // namespace state

// =============================================================================
// Part 4: Distributed Locks (String NX)
// 分布式锁相关
// =============================================================================
namespace lock {
constexpr std::string_view kPrefixLock = "dts:lock:";

/**
 * @brief 生成资源锁 Key
 * Example: "dts:lock:resource:db_sync"
 */
inline std::string ResourceLock(std::string_view resource_name) {
  return std::format("{}resource:{}", kPrefixLock, resource_name);
}

/**
 * @brief 生成调度器选主锁 Key
 * 谁抢到这个 Key 谁就是 Leader
 */
inline std::string SchedulerLeader() {
  return std::format("{}scheduler:leader", kPrefixLock);
}
}  // namespace lock

}  // namespace dts::common::redis::keys