#include "RedisManager.hpp"
#include "logger.hpp"

#include <sw/redis++/redis++.h>

namespace dts::common::redis {

RedisManager& RedisManager::GetInstance() noexcept {
  static RedisManager instance;
  return instance;
}

// 析构函数必须在 .cpp 里实现，因为这里 sw::redis::Redis 才是完整类型
RedisManager::~RedisManager() = default;

void RedisManager::Initialize(const RedisConfig& config) {
  std::call_once(_init_flag, [&]() {
    try {
      sw::redis::ConnectionOptions connection_opts;
      connection_opts.host = config.host;
      connection_opts.port = config.port;
      connection_opts.password = config.password;
      connection_opts.socket_timeout =
          std::chrono::milliseconds(config.socket_timeout_ms);
      connection_opts.connect_timeout =
          std::chrono::milliseconds(config.connect_timeout_ms);

      sw::redis::ConnectionPoolOptions pool_opts;
      pool_opts.size = config.pool_size;
      pool_opts.wait_timeout = std::chrono::milliseconds(100);

      // 创建客户端实例
      _client = std::make_unique<sw::redis::Redis>(connection_opts, pool_opts);

      // 验证连接
      auto response = _client->ping();

      LOG_INFO << "RedisManager initialized successfully. Host: " << config.host
               << ", Port: " << config.port << ", Ping: " << response;

    } catch (const std::exception& e) {
      LOG_ERROR << "RedisManager initialization failed: " << e.what();
      // 这里可以选择抛出异常终止程序，或者设置状态位
      throw;
    }
  });
}

std::optional<long long> RedisManager::LPush(std::string_view key,
                                             std::string_view value) {
  if (!_client) {
    LOG_ERROR << "RedisManager not initialized. Cannot execute LPush.";
    return std::nullopt;
  }

  try {
    // redis++ 的 lpush 支持 string_view
    long long len = _client->lpush(key, value);
    return len;
  } catch (const std::exception& e) {
    LOG_ERROR << "Redis LPush error. Key: " << key << ", Error: " << e.what();
    return std::nullopt;
  }
}

std::optional<std::pair<std::string, std::string>> RedisManager::BRPop(
    std::string_view key, int timeout_seconds) {
  if (!_client) {
    LOG_ERROR << "RedisManager not initialized. Cannot execute BRPop.";
    return std::nullopt;
  }

  try {
    // redis++ 的 brpop 返回 Optional<std::pair<string, string>>
    auto result = _client->brpop(key, timeout_seconds);
    if (result) {
      return *result;  // 隐式转换为 std::pair
    }
    return std::nullopt;  // 超时
  } catch (const std::exception& e) {
    // 注意：连接断开等网络错误会在这里捕获
    LOG_ERROR << "Redis BRPop error. Key: " << key << ", Error: " << e.what();
    return std::nullopt;
  }
}

}  // namespace dts::common::redis