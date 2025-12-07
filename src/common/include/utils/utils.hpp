#pragma once
#include <chrono>
#include <nlohmann/json.hpp>
#include <thread>

using json = nlohmann::json;

namespace dts::common::utils {

// 返回当前时间戳（毫秒）
inline uint64_t get_current_timestamp_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

/**
 * @brief 带重试机制的初始化函数
 * @param name 组件名称 (用于日志)
 * @param init_func 初始化逻辑 (如果失败请抛出异常)
 * @param max_retries 最大重试次数
 * @param retry_interval_sec 重试间隔(秒)
 * @throw std::runtime_error 如果重试耗尽仍失败
 */
template <typename Func>
void InitWithRetry(const std::string& name, Func init_func,
                   int max_retries = 30, int retry_interval_sec = 1) {
  int retries = 0;
  while (true) {
    try {
      init_func();  // 执行传入的 lambda
      LOG(INFO) << "[" << name << "] initialized successfully.";
      return;
    } catch (const std::exception& e) {
      if (retries++ >= max_retries) {
        std::string err_msg = "[" + name + "] init failed after " +
                              std::to_string(max_retries) +
                              " retries: " + e.what();
        LOG(FATAL) << err_msg;
        throw std::runtime_error(err_msg);  // 抛出异常中断 main
      }

      LOG(WARNING) << "[" << name << "] init failed (" << e.what()
                   << "), retrying... (" << retries << "/" << max_retries
                   << ")";
      std::this_thread::sleep_for(std::chrono::seconds(retry_interval_sec));
    }
  }
}

}  // namespace dts::common::utils