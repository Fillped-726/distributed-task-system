#include "metrics.hpp"
#include <iostream>
#include <algorithm>
#include <numeric>
#include <iomanip>

namespace dts::test {

void MetricsCollector::RecordLatency(double ms) {
  // 优化建议：在高并发下，如果觉得客户端 CPU 飙高，可以改为采样记录
  // if (requests_sent % 10 != 0) return;

  std::lock_guard<std::mutex> lock(mtx_);
  latencies_.push_back(ms);
}

void MetricsCollector::PrintReport(const std::string& title) {
  std::lock_guard<std::mutex> lock(mtx_);

  if (latencies_.empty()) {
    std::cout << "\n[" << title << "] No data recorded." << std::endl;
    return;
  }

  // 排序以计算百分位
  std::sort(latencies_.begin(), latencies_.end());

  double sum = std::accumulate(latencies_.begin(), latencies_.end(), 0.0);
  double avg = sum / latencies_.size();
  double p50 = latencies_[latencies_.size() * 0.50];
  double p95 = latencies_[latencies_.size() * 0.95];
  double p99 = latencies_[latencies_.size() * 0.99];

  std::cout << "\n[" << title << "]" << std::endl;
  std::cout << "  Total Req:   " << requests_sent << std::endl;
  std::cout << "  Success:     " << success_count << std::endl;
  std::cout << "  Failed:      " << fail_count << std::endl;
  std::cout << "  Tasks:       " << tasks_submitted << std::endl;
  std::cout << "  Latency Count: " << latencies_.size() << std::endl;
  std::cout << "  AVG:   " << std::fixed << std::setprecision(2) << avg << " ms"
            << std::endl;
  std::cout << "  P50:   " << p50 << " ms" << std::endl;
  std::cout << "  P95:   " << p95 << " ms" << std::endl;
  std::cout << "  P99:   " << "\033[1;31m" << p99 << " ms\033[0m" << std::endl;
}

MetricsCollector::Snapshot MetricsCollector::GetSnapshot() const {
  return {requests_sent.load(std::memory_order_relaxed),
          success_count.load(std::memory_order_relaxed),
          tasks_submitted.load(std::memory_order_relaxed)};
}

void MetricsCollector::Reset() {
  requests_sent = 0;
  success_count = 0;
  fail_count = 0;
  tasks_submitted = 0;

  std::lock_guard<std::mutex> lock(mtx_);
  latencies_.clear();
}

}  // namespace dts::test