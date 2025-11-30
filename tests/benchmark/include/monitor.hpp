#pragma once
#include "config.hpp"
#include "metrics.hpp"
#include <atomic>
#include <thread>

class Monitor {
 public:
  Monitor(const BenchmarkConfig& config, MetricsCollector& metrics,
          std::atomic<bool>& stop_flag);
  void Start();       // 启动后台线程
  void Stop();        // 等待结束
  void AnalyzeE2E();  // 最终全链路分析

 private:
  void Loop();

  BenchmarkConfig config_;
  MetricsCollector& metrics_;
  std::atomic<bool>& stop_submit_flag_;
  std::thread thread_;
};