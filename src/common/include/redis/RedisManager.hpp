#pragma once

#include "RedisConfig.hpp"
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <optional>
#include <vector>

// 前置声明，隐藏第三方库细节
namespace sw::redis {
class Redis;
}

using StreamEntry =
    std::pair<std::string, std::vector<std::pair<std::string, std::string>>>;

namespace dts::common::redis {

class RedisManager {
 public:
  // 删除拷贝和移动
  RedisManager(const RedisManager&) = delete;
  RedisManager& operator=(const RedisManager&) = delete;

  // 1. 获取单例 (noexcept)
  static RedisManager& GetInstance() noexcept;

  // 2. 初始化 (非线程安全)
  void Initialize(const RedisConfig& config);

  // ---------- Stream 核心接口封装 (Facade Pattern) ----------

  /**
   * @brief 发布任务到 Stream (XADD)
   * @param key Stream 的键名 (e.g., "dts:tasks")
   * @param id 消息ID，通常传 "*" 让 Redis 自动生成
   * @param field_values 键值对列表 (e.g., {{"task_id", "1001"}, {"type",
   * "email"}})
   * @return 生成的消息 ID (e.g., "16789000-0")
   */
  std::optional<std::string> XAdd(
      std::string_view key,
      const std::vector<std::pair<std::string, std::string>>& field_values,
      std::string_view id = "*");

  /**
   * @brief 创建消费者组 (XGROUP CREATE)
   * @note 如果组已存在，可能会抛错或返回 false，我们会内部处理这种情况
   */
  bool XGroupCreate(std::string_view key, std::string_view group_name,
                    std::string_view mkstream_id = "$", bool mkstream = true);

  /**
   * @brief 消费者组读取任务 (XREADGROUP)
   * @param group 消费者组名
   * @param consumer 消费者名 (Worker 的唯一标识)
   * @param key Stream 键名
   * @param count 每次拉取多少条
   * @param block_ms 阻塞毫秒数 (0表示无限阻塞)
   * @return 返回消息列表: vector<pair<MsgID, vector<pair<Field, Value>>>>
   */
  using StreamEntry =
      std::pair<std::string, std::vector<std::pair<std::string, std::string>>>;
  std::optional<std::vector<StreamEntry>> XReadGroup(std::string_view group,
                                                     std::string_view consumer,
                                                     std::string_view key,
                                                     int count, int block_ms);

  /**
   * @brief 确认消息已处理 (XACK)
   */
  long long XAck(std::string_view key, std::string_view group,
                 std::string_view id);

  // ---------- 故障转移核心 ----------

  /**
   * @brief [抢占] XCLAIM
   * 转移消息的所有权给当前消费者。
   * @param min_idle_ms 消息在 PEL
   * 中滞留的最小时间（只有闲置这么久的消息才能被抢）
   * @param ids 要抢占的消息 ID 列表
   * @return 成功抢到的消息列表
   */
  std::optional<std::vector<StreamEntry>> XClaim(
      std::string_view key, std::string_view group, std::string_view consumer,
      long long min_idle_ms, const std::vector<std::string>& ids);

  /**
   * @brief [查询滞留] XPENDING (Summary)
   * 用于监控或者判断是否有积压/挂掉的任务
   * @return {pending_count, min_id, max_id, consumer_count}
   * 这里为了简单返回 total count，复杂结构可按需扩展
   */
  // 暂时先不封装复杂的 Range 版本，先提供一个简单的检查接口
  // std::optional<PendingInfo> XPending(...);

 private:
  RedisManager() = default;
  ~RedisManager();  // 析构函数不能默认，因为 unique_ptr 指向不完整类型

  std::unique_ptr<sw::redis::Redis> _client;  // 真正持有客户端
  std::once_flag _init_flag;
};

}  // namespace dts::common::redis