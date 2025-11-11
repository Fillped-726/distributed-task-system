#pragma once
#include <nlohmann/json.hpp>
#include <atomic>
#include <memory>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace dts {
enum class TaskState : std::int8_t {
    WAITING_DEPS = -1, PENDING = 0, RUNNING = 1, SUCCESS = 2, FAILED = 3, TIMEOUT = 4, CANCELLED = 5
};

struct Resource {
    double cpu_core = 0;
    std::uint64_t mem_mb = 0;
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(Resource, cpu_core, mem_mb)
};

struct Shard {
    std::uint32_t shard_id = 0;
    std::uint32_t total_shards = 1;
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(Shard, shard_id, total_shards)
};

struct Task {
    std::string task_id;
    std::string client_id;
    std::uint32_t priority = 0;
    TaskState state = TaskState::PENDING;

    std::shared_ptr<std::atomic<bool>> cancelled =
        std::make_shared<std::atomic<bool>>(false);

    std::string func_name;
    nlohmann::json func_params;
    Resource required;
    Shard shard;
    std::uint32_t timeout_ms = 30'000;
    std::uint32_t max_retry = 3;
    std::uint32_t retry_count = 0;
    std::int64_t submit_ts = 0;
    std::int64_t start_ts = 0;
    std::int64_t finish_ts = 0;
    nlohmann::json result;
    std::string error_msg;
};

}  // namespace dts