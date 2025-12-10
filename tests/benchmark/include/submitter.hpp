#pragma once
#include "config.hpp"
#include "metrics.hpp"
#include <vector>
#include <thread>
#include <memory>

// 确保使用正确的命名空间
namespace dts::test {

class Submitter {
 public:
  // 构造函数
  Submitter(const BenchmarkConfig& config, MetricsCollector& metrics);

  void Run();   // 启动所有线程
  void Stop();  // 强制停止 (可选)

 private:
  void WorkerThread(int thread_id);  // 单个线程逻辑

  BenchmarkConfig config_;
  MetricsCollector& metrics_;
};

}  // namespace dts::test