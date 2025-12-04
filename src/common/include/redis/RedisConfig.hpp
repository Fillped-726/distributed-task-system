#pragma once

#include <string>
#include <cstdlib>

namespace dts::common::redis {

struct RedisConfig {
  std::string host = "127.0.0.1";
  int port = 6379;
  std::string password = "";
  int pool_size = 5;
  int socket_timeout_ms = 1000;
  int connect_timeout_ms = 1000;

  // 安全转换
  static int SafeStoi(const char* str, int default_val) {
    try {
      return std::stoi(str);
    } catch (...) {
      return default_val;
    }
  }

  // 从环境变量加载配置，提供默认值兜底
  static RedisConfig LoadFromEnv() {
    RedisConfig config;
    if (auto p = std::getenv("REDIS_HOST")) config.host = p;
    if (auto p = std::getenv("REDIS_PORT")) config.port = SafeStoi(p, 6379);
    if (auto p = std::getenv("REDIS_PASSWORD")) config.password = p;
    if (auto p = std::getenv("REDIS_POOL_SIZE"))
      config.pool_size = SafeStoi(p, 5);
    // ... 其他超时配置可按需扩展
    return config;
  }
};

}  // namespace dts::common::redis