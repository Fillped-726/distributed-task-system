#pragma once
#include "config.hpp"
#include "metrics.hpp"
#include <vector>
#include <thread>

class Submitter {
 public:
  Submitter(const BenchmarkConfig& config, MetricsCollector& metrics);
  void Run();  // 启动所有线程

 private:
  void WorkerThread(int thread_id);  // 单个线程逻辑

  BenchmarkConfig config_;
  MetricsCollector& metrics_;
};