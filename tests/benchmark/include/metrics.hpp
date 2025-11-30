#pragma once
#include <atomic>
#include <vector>
#include <mutex>
#include <string>

class MetricsCollector {
 public:
  // 计数器
  std::atomic<int> success_count{0};
  std::atomic<int> fail_count{0};

  // 记录耗时 (毫秒)
  void RecordLatency(double ms);

  // 生成报告
  void PrintReport(const std::string& title);

  // 清空数据
  void Reset();

 private:
  std::vector<double> latencies_;
  std::mutex mtx_;
};