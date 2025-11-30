#include "metrics.hpp"
#include <iostream>
#include <algorithm>
#include <numeric>
#include <iomanip>

void MetricsCollector::RecordLatency(double ms) {
  std::lock_guard<std::mutex> lock(mtx_);
  latencies_.push_back(ms);
}

void MetricsCollector::PrintReport(const std::string& title) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (latencies_.empty()) return;

  std::sort(latencies_.begin(), latencies_.end());

  double sum = std::accumulate(latencies_.begin(), latencies_.end(), 0.0);
  double avg = sum / latencies_.size();
  double p50 = latencies_[latencies_.size() * 0.50];
  double p99 = latencies_[latencies_.size() * 0.99];

  std::cout << "\n[" << title << "]" << std::endl;
  std::cout << "  Count: " << latencies_.size() << std::endl;
  std::cout << "  AVG:   " << std::fixed << std::setprecision(2) << avg << " ms"
            << std::endl;
  std::cout << "  P50:   " << p50 << " ms" << std::endl;
  std::cout << "  P99:   " << "\033[1;31m" << p99 << " ms\033[0m" << std::endl;
}

void MetricsCollector::Reset() {
  success_count = 0;
  fail_count = 0;
  std::lock_guard<std::mutex> lock(mtx_);
  latencies_.clear();
}