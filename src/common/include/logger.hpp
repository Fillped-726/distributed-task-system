#pragma once

#include <glog/logging.h>
#include <chrono>
#include <string>
#include <filesystem>
#include <iostream>

namespace dts {

// ---------- 线程级 request_id ----------
// thread_local 意味着每个线程都有一份独立的副本，互不干扰，且无锁，性能极高
inline thread_local std::string t_req_id;

inline void SetRequestId(const std::string& id) { t_req_id = id; }

inline const std::string& GetRequestId() { return t_req_id; }

// ---------- Log 前缀生成 ----------
inline std::string LogPrefix() {
  // 获取毫秒级时间戳
  auto now = std::chrono::system_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch())
                .count();

  // 格式: [ReqID: xxx] [TS: 123456789]
  // 注意：Glog 本身会自动打印 时间、线程ID、文件名、行号，
  // 所以我们只需要补充 ReqID 即可，不要重复打印时间，避免日志冗余。
  std::string prefix =
      t_req_id.empty() ? "[ReqID:-] " : "[ReqID:" + t_req_id + "] ";
  return prefix;
}

// ---------- 初始化函数 ----------
inline void InitGlog(const char* argv0, const std::string& log_dir = "./logs") {
  // 1. 创建日志目录
  if (!std::filesystem::exists(log_dir)) {
    std::filesystem::create_directories(log_dir);
  }

  // 2. 基础配置
  FLAGS_log_dir = log_dir;        // 日志输出目录
  FLAGS_alsologtostderr = true;   // 既输出到文件，也输出到控制台 (调试方便)
  FLAGS_colorlogtostderr = true;  // 控制台输出带颜色
  FLAGS_max_log_size = 100;       // 单个日志文件最大 100MB
  FLAGS_stop_logging_if_full_disk = true;  // 磁盘满停止写入

  // 3. 初始化
  google::InitGoogleLogging(argv0);

  // 4. 安装信号处理 (比如段错误时打印堆栈) -> 这个很重要！
  google::InstallFailureSignalHandler();

  LOG(INFO) << "Glog initialized. Log dir: " << log_dir;
}

}  // namespace dts

// ---------- 宏定义重写 ----------
// 技巧：利用 Glog 的流式操作，把 Prefix 插入到最前面
// 使用方法：LOG_INFO << "Worker started";
// 输出结果：I1128 ... user_code.cpp:50] [ReqID:123abc] Worker started

#define LOG_INFO LOG(INFO) << dts::LogPrefix()
#define LOG_WARN LOG(WARNING) << dts::LogPrefix()
#define LOG_ERROR LOG(ERROR) << dts::LogPrefix()
#define LOG_FATAL LOG(FATAL) << dts::LogPrefix()

// 增加一个调试用的宏 (仅在 Debug 模式下输出)
#ifdef NDEBUG
#define LOG_DEBUG \
  if (false) LOG(INFO)
#else
#define LOG_DEBUG LOG(INFO) << "[DEBUG] " << dts::LogPrefix()
#endif