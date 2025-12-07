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

// 错误/死信队列
constexpr std::string_view kErrors = "dts:stream:errors";

// 默认消费者组
constexpr std::string_view kGroupMain = "dts:group:main";
}  // namespace stream

// =============================================================================
// Part 3: DAG Runtime State (DAG 运行时状态 - 新增核心部分)
// =============================================================================
namespace dag {
// 内部前缀定义
constexpr std::string_view kPrefixChildren = "dts:dag:children:";
constexpr std::string_view kPrefixDeps = "dts:dag:deps:";
constexpr std::string_view kPrefixMeta = "dts:task:meta:";

/**
 * @brief 生成父子关系 Key (Set)
 * 存储 parent_id 的所有子节点 ID
 * Type: Set<string>
 * Example: "dts:dag:children:task-uuid-A" -> { "task-uuid-B", "task-uuid-C" }
 */
inline std::string Children(std::string_view task_id) {
  return std::format("{}{}", kPrefixChildren, task_id);
}

/**
 * @brief 生成依赖计数表 Key (Hash)
 * 存储整个 Job 中所有任务的剩余依赖数
 * Type: Hash<task_id, int>
 * Example: "dts:dag:deps:job-uuid-1" -> { "task-uuid-B": 1, "task-uuid-C": 2 }
 * 理由：用 Hash 比用 String 单独存储每个任务的计数更节省内存，且方便 Lua
 * 脚本操作。
 */
inline std::string DependencyHash(std::string_view job_id) {
  return std::format("{}{}", kPrefixDeps, job_id);
}

/**
 * @brief 生成任务元数据 Key (String)
 * 存储任务序列化后的二进制数据，供 Lua 脚本直接搬运到 Stream
 * Type: String (Protobuf Binary)
 * Example: "dts:task:meta:task-uuid-B" -> <binary_data>
 */
inline std::string TaskMeta(std::string_view task_id) {
  return std::format("{}{}", kPrefixMeta, task_id);
}
}  // namespace dag

// =============================================================================
// Part 4: Runtime State (Hash/String 相关)
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
// Part 5: Distributed Locks (String NX)
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