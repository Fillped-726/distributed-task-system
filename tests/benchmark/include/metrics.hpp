#pragma once
#include <atomic>
#include <vector>
#include <mutex>
#include <string>

namespace dts::test {

class MetricsCollector {
 public:
  // --- 计数器 (原子操作，无锁高性能) ---
  std::atomic<long long> requests_sent{0};  // 发出的请求总数
  std::atomic<long long> success_count{0};  // 成功的请求数
  std::atomic<long long> fail_count{0};     // 失败的请求数
  std::atomic<long long> tasks_submitted{
      0};  // 提交的任务总数 (Request * TasksPerDAG)

  // --- 延迟统计 ---
  // 记录耗时 (毫秒)
  void RecordLatency(double ms);

  // 生成报告
  void PrintReport(const std::string& title);

  // 获取当前的快照 (用于 Monitor 每秒打印)
  struct Snapshot {
    long long sent;
    long long success;
    long long tasks;
  };
  Snapshot GetSnapshot() const;

  // 清空数据
  void Reset();

 private:
  std::vector<double> latencies_;  // 存储耗时数据
  std::mutex mtx_;                 // 保护 latencies_
};

}  // namespace dts::test