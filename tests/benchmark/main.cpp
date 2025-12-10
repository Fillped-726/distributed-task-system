#include "config.hpp"
#include "metrics.hpp"
#include "submitter.hpp"
#include "monitor.hpp"
#include <iostream>
#include <cstdlib>

int main(int argc, char** argv) {
  BenchmarkConfig config;

  // [优化] 简单的命令行参数支持
  // 用法: ./dts_stress [threads] [total_requests]
  if (argc > 1) config.num_threads = std::atoi(argv[1]);
  if (argc > 2) config.total_requests = std::atoi(argv[2]);

  dts::test::MetricsCollector submit_metrics;
  std::atomic<bool> submission_done{false};

  std::cout << "=== DTS Benchmark System (Redis + Write-Behind) ==="
            << std::endl;
  std::cout << "Threads: " << config.num_threads
            << ", Total Requests: " << config.total_requests << std::endl;

  // 1. 启动监控
  // Monitor 内部包含了 logic: 即使 submission_done=true，也要等到 DB
  // 不再增长才退出
  dts::test::Monitor monitor(config, submit_metrics, submission_done);
  monitor.Start();

  // 2. 启动压测 (Client 发送阶段)
  dts::test::Submitter submitter(config, submit_metrics);

  std::cout << "\n>>> Phase 1: Ingestion (Client Sending) <<<" << std::endl;
  auto start = std::chrono::high_resolution_clock::now();

  submitter.Run();  // 阻塞直到发送完毕

  auto end = std::chrono::high_resolution_clock::now();

  // 3. 进入落盘阶段
  std::cout << "\n>>> Phase 2: Draining (Waiting for DB Write-Behind) <<<"
            << std::endl;
  std::cout
      << "Client sending finished. Waiting for Scheduler & DB to catch up..."
      << std::endl;

  submission_done = true;  // 告诉 Monitor 发送结束了
  monitor.Stop();          // Monitor 会阻塞直到 DB 数据量不再变化 (即处理完成)

  // 4. 输出最终报告
  std::chrono::duration<double> diff = end - start;  // 发送阶段耗时
  double qps = submit_metrics.success_count / diff.count();
  double tps = submit_metrics.tasks_submitted / diff.count();

  std::cout << "\n=============================================" << std::endl;
  std::cout << "              BENCHMARK SUMMARY              " << std::endl;
  std::cout << "=============================================" << std::endl;
  std::cout << "  Duration (Send): " << diff.count() << " s" << std::endl;
  std::cout << "  DAG QPS:         " << "\033[1;32m" << qps << " /s\033[0m"
            << std::endl;
  std::cout << "  Task TPS:        " << "\033[1;36m" << tps << " /s\033[0m"
            << std::endl;
  std::cout << "=============================================" << std::endl;

  // Client 侧接口延迟统计
  submit_metrics.PrintReport("Client Interface Latency");

  // 端到端全链路延迟统计 (DB 数据)
  monitor.AnalyzeE2E();

  return 0;
}