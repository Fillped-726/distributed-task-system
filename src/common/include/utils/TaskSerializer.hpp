#pragma once

#include "task.hpp"
#include "dts/task/task.pb.h"
#include "converters.hpp"
#include "redis/RedisManager.hpp"
#include "logger.hpp"

#include <vector>
#include <string>
#include <optional>
#include <utility>

namespace dts::common::utils {

struct StreamTaskPointer {
  std::string task_id;
  std::string job_id;
  std::string stream_id;  // Redis Stream 自动生成的 ID (123456-0)
};

class TaskSerializer {
 public:
  using StreamEntry = dts::common::redis::RedisManager::StreamEntry;

  // ----------------------------------------------------------------
  // [写入时] 仅生成轻量级指针 (ID + JobID)
  // ----------------------------------------------------------------
  [[nodiscard("Redis Stream 参数")]]
  static std::vector<std::pair<std::string, std::string>> ToXAddArgs(
      const std::string& task_id, const std::string& job_id) {
    std::vector<std::pair<std::string, std::string>> args;
    args.reserve(2);
    args.emplace_back("id", task_id);
    args.emplace_back("job", job_id);
    return args;
  }

  // ----------------------------------------------------------------
  // [读取步骤 1] 解析 Stream 条目 -> 得到 TaskID
  // ----------------------------------------------------------------
  [[nodiscard("Stream 指针信息")]]
  static std::optional<StreamTaskPointer> ParseStreamEntry(
      const StreamEntry& entry) {
    StreamTaskPointer ptr;
    ptr.stream_id = entry.first;

    bool found_id = false;
    for (const auto& [field, value] : entry.second) {
      if (field == "id" || field == "task_id") {  // 兼容两种写法
        ptr.task_id = value;
        found_id = true;
      } else if (field == "job" || field == "job_id") {
        ptr.job_id = value;
      }
    }

    if (!found_id) {
      LOG_ERROR << "Bad task data: Missing 'id' in stream entry. MsgID: "
                << entry.first;
      return std::nullopt;
    }
    return ptr;
  }

  // ----------------------------------------------------------------
  // [读取步骤 2] 二进制数据 (来自 Redis GET) -> 业务对象
  // ----------------------------------------------------------------
  [[nodiscard("业务 Task 对象")]]
  static std::optional<dts::Task> FromMetaBinary(
      const std::string& binary_data) {
    if (binary_data.empty()) return std::nullopt;

    dts::task::Task proto_task;
    // 1. 反序列化 Proto
    if (!proto_task.ParseFromString(binary_data)) {
      LOG_ERROR << "Failed to parse Task Protobuf binary.";
      return std::nullopt;
    }

    // 2. 转换为业务对象 (Domain Object)
    // 假设你有这个转换函数
    return dts::TaskFromProto(proto_task);
  }
};

}  // namespace dts::common::utils