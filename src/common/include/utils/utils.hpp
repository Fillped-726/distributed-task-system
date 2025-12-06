#pragma once
#include <chrono>

using json = nlohmann::json;

namespace dts {

// 返回当前时间戳（毫秒）
inline uint64_t get_current_timestamp_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

} // namespace dts