#pragma once
#include <glog/logging.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include <string>

namespace dts {

// ---------- 线程级 request_id ----------
inline thread_local std::string t_req_id;
inline void SetRequestId(const std::string& id) { t_req_id = id; }

// ---------- 统一 JSON 前缀 ----------
inline std::string LogPrefix(const char* level) {
    nlohmann::json j;
    j["level"]  = level;
    j["ts"]     = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();
    j["req_id"] = t_req_id.empty() ? "-" : t_req_id;
    return j.dump() + " | ";
}


// ---------- 初始化函数（保持原签名） ----------
inline void InitGlog(const char* argv0, bool unit_test = false) {
    google::InitGoogleLogging(argv0);
    FLAGS_logtostderr = 1;
    // if (unit_test) {
    //     FLAGS_logtostderr = 1;
    //     FLAGS_minloglevel = 1;   // 1=WARNING
    // } else {
    //     FLAGS_max_log_size = 100;  // 100 MB
    //     FLAGS_stop_logging_if_full_disk = true;
    //     FLAGS_colorlogtostderr = true;
    //     google::InstallFailureSignalHandler();
    // }
}

} // namespace dts

#define LOG_INFO  LOG(INFO)   << dts::LogPrefix("INFO")
#define LOG_WARN  LOG(WARNING)<< dts::LogPrefix("WARN")
#define LOG_ERROR LOG(ERROR)  << dts::LogPrefix("ERROR")
#define LOG_FATAL LOG(FATAL)  << dts::LogPrefix("FATAL")