#pragma once

#include <cstdint>
#include <sstream>
#include <string>

// 第三方库
#include <nlohmann/json.hpp>

namespace dts {

// 任务状态
// 面试亮点：使用 enum class 强类型枚举，避免隐式转换带来的 Bug
enum class TaskState : std::int8_t {
  PENDING = 0,       // 等待调度
  RUNNING = 1,       // 正在执行
  SUCCESS = 2,       // 执行成功
  FAILED = 3,        // 执行失败
  TIMEOUT = 4,       // 超时
  CANCELLED = 5,     // 被取消
  WAITING_DEPS = 6,  // 等待依赖任务完成 (DAG特性)
  UNKNOWN = 99
};

// 状态转字符串辅助函数，方便日志打印
inline std::string TaskStateToString(TaskState state) {
  switch (state) {
    case TaskState::PENDING:
      return "PENDING";
    case TaskState::RUNNING:
      return "RUNNING";
    case TaskState::SUCCESS:
      return "SUCCESS";
    case TaskState::FAILED:
      return "FAILED";
    case TaskState::TIMEOUT:
      return "TIMEOUT";
    case TaskState::CANCELLED:
      return "CANCELLED";
    case TaskState::WAITING_DEPS:
      return "WAITING_DEPS";
    default:
      return "UNKNOWN";
  }
}

// 资源需求
struct Resource {
  double cpu_core = 0.0;
  std::uint64_t mem_mb = 0;

  // JSON 序列化宏
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(Resource, cpu_core, mem_mb)
};

// 分片信息 (用于 MapReduce 类任务)
struct Shard {
  std::uint32_t shard_id = 0;
  std::uint32_t total_shards = 1;

  NLOHMANN_DEFINE_TYPE_INTRUSIVE(Shard, shard_id, total_shards)
};

// 核心任务结构体
struct Task {
  // 1. 身份标识
  std::string job_id;      // 所属的大作业ID
  std::string task_id;     // 全局唯一ID (UUID)
  std::string natural_id;  // 业务定义的ID (用于幂等性去重)
  std::string client_id;   // 提交者的ID

  // 2. 调度属性
  std::uint32_t priority = 0;            // 优先级
  TaskState state = TaskState::PENDING;  // 当前状态
  int pending_dependencies = 0;          // 剩余依赖数 (DAG)

  // 3. 执行内容
  std::string func_name;       // 要执行的函数名/处理器名
  nlohmann::json func_params;  // 函数参数
  Resource required;           // 资源需求
  Shard shard;                 // 分片信息

  // 4. 容错与控制
  std::uint32_t timeout_ms = 30000;  // 超时时间
  std::uint32_t max_retry = 3;       // 最大重试次数
  std::uint32_t retry_count = 0;     // 已重试次数

  // 5. 统计与结果
  std::int64_t submit_ts = 0;  // 提交时间戳
  std::int64_t start_ts = 0;   // 开始执行时间戳
  std::int64_t finish_ts = 0;  // 结束时间戳
  nlohmann::json result;       // 执行结果
  std::string error_msg;       // 错误信息

  // 辅助方法：生成简短的调试信息 (供 glog 使用)
  std::string ShortDebugString() const {
    std::stringstream ss;
    ss << "[Task " << task_id << "] "
       << "Func: " << func_name << ", State: " << TaskStateToString(state)
       << ", Retry: " << retry_count << "/" << max_retry;
    return ss.str();
  }

  // 关键修复：添加 Task 的序列化宏
  // 只有加上这行，Worker 才能解析 Master 发来的 JSON
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(Task, job_id, task_id, natural_id, client_id,
                                 priority, state, pending_dependencies,
                                 func_name, func_params, required, shard,
                                 timeout_ms, max_retry, retry_count, submit_ts,
                                 start_ts, finish_ts, result, error_msg)
};

}  // namespace dts