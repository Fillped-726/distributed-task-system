#pragma once
#include "utils.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

namespace dts {

class Generator {
public:
    explicit Generator(uint16_t worker_id)
        : worker_id_(worker_id & 0x3FF), last_ms_(0), seq_(0) {}

    uint64_t Next() {
        uint64_t ms = 0;
        uint64_t seq = 0;
        while (true) {
            ms = get_current_timestamp_ms();
            if (ms < last_ms_) {               // 小回拨
                ms = last_ms_;
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            if (ms == last_ms_) {
                seq = seq_.fetch_add(1) & 0xFFF;
                if (seq == 0) {               // 序列号溢出
                    while (now_ms() == ms) {}
                    continue;
                }
            } else {
                seq_.store(0);
                seq = 0;
            }
            last_ms_ = ms;
            break;
        }
        return (ms - 1288834974657) << 22 | uint64_t(worker_id_) << 12 | seq;
    }

private:
    const uint16_t worker_id_;
    uint64_t       last_ms_;
    std::atomic<uint64_t> seq_;
};

} // namespace dts

//TODO worker_id 唯一，持久化：防止重启重复 ID，NTP大回拨导致阻塞