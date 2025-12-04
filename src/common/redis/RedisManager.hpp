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

  // ---------- 封装业务常用命令 (Facade Pattern) ----------

  /**
   * @brief 推送任务到队列左侧 (LPUSH)
   * @return 队列当前长度, 若失败返回 nullopt
   */
  std::optional<long long> LPush(std::string_view key, std::string_view value);

  /**
   * @brief 阻塞弹出任务 (BRPOP)
   * @param timeout_seconds 0 表示无限阻塞
   * @return 成功返回 {queue_name, value}，超时或失败返回 nullopt
   */
  std::optional<std::pair<std::string, std::string>> BRPop(
      std::string_view key, int timeout_seconds = 0);

  // 如果未来需要更多命令（如 Set, Get），在这里继续扩展封装...
  // bool Set(std::string_view key, std::string_view value,
  // std::chrono::milliseconds ttl); std::optional<std::string>
  // Get(std::string_view key);

 private:
  RedisManager() = default;
  ~RedisManager();  // 析构函数不能默认，因为 unique_ptr 指向不完整类型

  std::unique_ptr<sw::redis::Redis> _client;  // 真正持有客户端
  std::once_flag _init_flag;
};

}  // namespace dts::common::redis