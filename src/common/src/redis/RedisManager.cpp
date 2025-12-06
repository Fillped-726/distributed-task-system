#include "redis/RedisManager.hpp"
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

std::optional<std::string> RedisManager::XAdd(
    std::string_view key,
    const std::vector<std::pair<std::string, std::string>>& field_values,
    std::string_view id) {
  if (!_client) return std::nullopt;
  try {
    return _client->xadd(key, id, field_values.begin(), field_values.end());
  } catch (const std::exception& e) {
    LOG_ERROR << "Redis XADD error: " << e.what();
    return std::nullopt;
  }
}

bool RedisManager::XGroupCreate(std::string_view key,
                                std::string_view group_name,
                                std::string_view id, bool mkstream) {
  if (!_client) return false;
  try {
    if (mkstream) {
      _client->xgroup_create(key, group_name, id, true);  // mkstream=true
    } else {
      _client->xgroup_create(key, group_name, id);
    }
    return true;
  } catch (const sw::redis::Error& e) {
    // 如果错误是 "BUSYGROUP Consumer Group name already exists"，则忽略
    std::string msg = e.what();
    if (msg.find("BUSYGROUP") != std::string::npos) {
      return true;  // 已经存在算成功
    }
    LOG_ERROR << "Redis XGROUP CREATE error: " << e.what();
    return false;
  } catch (const std::exception& e) {
    LOG_ERROR << "Redis XGROUP CREATE unexpected error: " << e.what();
    return false;
  }
}

std::optional<std::vector<RedisManager::StreamEntry>> RedisManager::XReadGroup(
    std::string_view group, std::string_view consumer, std::string_view key,
    int count, int block_ms) {
  if (!_client) return std::nullopt;
  try {
    std::vector<StreamEntry> reply;

    auto streams = {std::make_pair(std::string(key), std::string(">"))};

    std::unordered_map<std::string, std::vector<StreamEntry>> result;

    // redis++ 返回的是 std::unordered_map<std::string,
    // std::vector<StreamEntry>> 因为 XREADGROUP 可以同时监听多个
    // Stream，所以外层是 Map。 我们这里简化，只监听一个 key。
    _client->xreadgroup(group, consumer, streams.begin(), streams.end(),
                        std::chrono::milliseconds(block_ms), count,
                        std::inserter(result, result.end()));

    if (result.empty()) {
      return std::vector<StreamEntry>{};  // 没有任何消息
    }

    return result.begin()->second;

  } catch (const std::exception& e) {
    // 超时在 redis++ 中可能会表现为 reply 为空，网络错误才抛异常
    LOG_ERROR << "Redis XREADGROUP error: " << e.what();
    return std::nullopt;
  }
}

long long RedisManager::XAck(std::string_view key, std::string_view group,
                             std::string_view id) {
  if (!_client) return -1;
  try {
    return _client->xack(key, group, {std::string(id)});
  } catch (const std::exception& e) {
    LOG_ERROR << "Redis XACK error: " << e.what();
    return -1;
  }
}

std::optional<std::vector<RedisManager::StreamEntry>> RedisManager::XClaim(
    std::string_view key, std::string_view group, std::string_view consumer,
    long long min_idle_ms, const std::vector<std::string>& ids) {
  if (!_client || ids.empty()) return std::nullopt;

  try {
    std::vector<StreamEntry> claimed_msgs;

    // XCLAIM key group consumer min-idle-time id [id ...]
    // redis-plus-plus 同样使用输出迭代器来接收结果
    _client->xclaim(key, group, consumer,
                    std::chrono::milliseconds(min_idle_ms), ids.begin(),
                    ids.end(), std::back_inserter(claimed_msgs));

    return claimed_msgs;
  } catch (const std::exception& e) {
    LOG_ERROR << "Redis XCLAIM error. Key: " << key << ", Error: " << e.what();
    return std::nullopt;
  }
}

std::optional<std::vector<PendingEntry>> RedisManager::XPending(
    std::string_view key, std::string_view group, int count,
    std::string_view start_id, std::string_view end_id,
    std::string_view consumer) {
  if (!_client) return std::nullopt;

  try {
    std::vector<PendingEntry> result;

    // redis-plus-plus 的 output 类型是 sw::redis::XPendingDetail
    std::vector<XPendingDetail> details;

    // 根据是否指定 consumer 调用不同的重载
    if (consumer.empty()) {
      _client->xpending(key, group, start_id, end_id, count,
                        std::back_inserter(details));
    } else {
      _client->xpending(key, group, start_id, end_id, count, consumer,
                        std::back_inserter(details));
    }

    // 转换为我们的业务结构体 (解耦)
    result.reserve(details.size());
    for (const auto& d : details) {
      result.push_back(PendingEntry{std::get<0>(d), std::get<1>(d),
                                    std::get<2>(d), std::get<3>(d)});
    }

    return result;

  } catch (const std::exception& e) {
    LOG_ERROR << "Redis XPENDING error. Key: " << key
              << ", Error: " << e.what();
    return std::nullopt;
  }
}

}  // namespace dts::common::redis