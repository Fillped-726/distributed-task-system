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

class TaskSerializer {
 public:
  using StreamEntry = dts::common::redis::RedisManager::StreamEntry;

  // ----------------------------------------------------------------
  // [序列化] 业务对象 -> Proto -> 二进制 -> Redis Stream 参数
  // ----------------------------------------------------------------
  [[nodiscard("Redis Stream 参数")]]
  static std::optional<std::vector<std::pair<std::string, std::string>>>
  ToXAddArgs(const dts::Task& task) {
    std::vector<std::pair<std::string, std::string>> args;

    // 1. 转换：CppTask -> PbTask
    dts::task::Task proto_task;
    dts::TaskToProto(task, &proto_task);  // 调用你的转换函数

    // 2. 序列化：PbTask -> Binary String
    std::string binary_payload;
    if (!proto_task.SerializeToString(&binary_payload)) {
      LOG_ERROR << "Protobuf serialization failed for task: " << task.task_id;
      return {};
    }

    // 3. 构造 Redis 参数
    // 核心 Payload (二进制)
    args.emplace_back("payload", std::move(binary_payload));

    // 冗余 Metadata (方便调试和索引)
    args.emplace_back("task_id", task.task_id);
    args.emplace_back("job_id", task.job_id);
    args.emplace_back("priority", std::to_string(task.priority));

    return args;
  }

  // ----------------------------------------------------------------
  // [反序列化] Redis Stream Entry -> 二进制 -> PbTask -> 业务对象
  // ----------------------------------------------------------------
  [[nodiscard("业务对象")]]
  static std::optional<dts::Task> FromStreamEntry(const StreamEntry& entry) {
    try {
      for (const auto& [field, value] : entry.second) {
        if (field == "payload") {
          dts::task::Task proto_task;

          // 1. 反序列化：Binary -> PbTask
          if (!proto_task.ParseFromString(value)) {
            LOG_ERROR << "Protobuf parse failed. MsgID: " << entry.first;
            return std::nullopt;
          }

          // 1. 转换业务数据
          dts::Task task = dts::TaskFromProto(proto_task);

          // 2. 注入 Redis Stream ID
          task.stream_id = entry.first;
          return task;
        }
      }
      // 如果只有 Metadata 没有 Payload，视为坏数据
      LOG_ERROR << "Missing payload in stream entry. MsgID: " << entry.first;
      return std::nullopt;
    } catch (const std::exception& e) {
      LOG_ERROR << "Deserialization error: " << e.what();
      return std::nullopt;
    }
  }
};

}  // namespace dts::common::utils