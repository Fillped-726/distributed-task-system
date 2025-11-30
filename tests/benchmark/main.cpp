#include "config.hpp"
#include "metrics.hpp"
#include "submitter.hpp"
#include "monitor.hpp"
#include <iostream>

int main() {
  BenchmarkConfig config;

  MetricsCollector submit_metrics;
  std::atomic<bool> submission_done{false};

  std::cout << "=== DTS Benchmark System ===" << std::endl;

  // 1. 启动监控
  Monitor monitor(config, submit_metrics, submission_done);
  monitor.Start();

  // 2. 启动压测
  Submitter submitter(config, submit_metrics);
  auto start = std::chrono::high_resolution_clock::now();
  submitter.Run();
  auto end = std::chrono::high_resolution_clock::now();

  submission_done = true;
  monitor.Stop();

  // 3. 输出报告
  std::chrono::duration<double> diff = end - start;
  std::cout << "\n[Submission Summary]" << std::endl;
  std::cout << "  QPS: " << submit_metrics.success_count / diff.count()
            << std::endl;

  submit_metrics.PrintReport("API Submission Latency");

  monitor.AnalyzeE2E();

  return 0;
}