#include "monitor.hpp"
#include <pqxx/pqxx>
#include <iostream>
#include <iomanip>
#include <chrono>

namespace dts::test {
Monitor::Monitor(const BenchmarkConfig& config, MetricsCollector& metrics,
                 std::atomic<bool>& stop_flag)
    : config_(config), metrics_(metrics), stop_submit_flag_(stop_flag) {}

void Monitor::Start() { thread_ = std::thread(&Monitor::Loop, this); }

void Monitor::Stop() {
  if (thread_.joinable()) thread_.join();
}

void Monitor::Loop() {
  try {
    pqxx::connection C(config_.db_conn_string);
    if (!C.is_open()) {
      std::cerr << "[Monitor] Failed to connect to DB!" << std::endl;
      return;
    }

    // 1. 获取初始状态
    long long initial_db_tasks = 0;
    {
      pqxx::work txn(C);
      // 统计所有非 PENDING/WAITING 的任务 (即 Running/Success/Failed/Timeout)
      pqxx::row r = txn.exec1(
          "SELECT count(*) FROM public.task WHERE state NOT IN (0, 6)");
      initial_db_tasks = r[0].as<long long>();
    }

    long long last_db_tasks = initial_db_tasks;
    long long last_sent_reqs = 0;

    int seconds = 0;

    std::cout << "\n"
              << std::setw(4) << "Sec" << " | " << std::setw(10) << "Sent(DAG)"
              << " | " << std::setw(10) << "Done(Task)" << " | " << std::setw(8)
              << "Send/s" << " | " << std::setw(8) << "DB/s" << std::endl;
    std::cout << std::string(55, '-') << std::endl;

    while (true) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      seconds++;

      // A. 获取内存指标 (实时发送量)
      long long current_sent_reqs = metrics_.success_count.load();

      // B. 获取 DB 指标 (落盘完成量)
      long long current_db_tasks = 0;
      try {
        pqxx::work txn(C);
        pqxx::row r = txn.exec1(
            "SELECT count(*) FROM public.task WHERE state NOT IN (0, 6)");
        current_db_tasks = r[0].as<long long>();
      } catch (const std::exception& e) {
        // DB 可能会在高负载下超时，忽略单次错误
        current_db_tasks = last_db_tasks;
      }

      // C. 计算速率
      long long sent_delta = current_sent_reqs - last_sent_reqs;
      long long db_delta = current_db_tasks - last_db_tasks;

      // D. 打印
      std::cout << std::setw(4) << seconds << " | " << std::setw(10)
                << current_sent_reqs << " | " << std::setw(10)
                << (current_db_tasks - initial_db_tasks) << " | "
                << "\033[1;36m" << std::setw(8) << sent_delta
                << "\033[0m | "  // 青色显示发送速率
                << "\033[1;32m" << std::setw(8) << db_delta
                << "\033[0m"  // 绿色显示落盘速率
                << std::endl;

      // 更新游标
      last_sent_reqs = current_sent_reqs;
      last_db_tasks = current_db_tasks;

      // E. 退出条件
      // 停止发送 且 DB 落盘数量 >= 发送数量 (注意：如果 1 DAG > 1
      // Task，这里逻辑需要调整) 简单起见，只要停止发送且 DB
      // 不再增长（delta=0），大概率就是跑完了
      if (stop_submit_flag_ && sent_delta == 0 && db_delta == 0 &&
          seconds > 5) {
        // 再多等一轮确认
        std::this_thread::sleep_for(std::chrono::seconds(1));
        break;
      }
    }

  } catch (const std::exception& e) {
    std::cerr << "Monitor Error: " << e.what() << std::endl;
  }
}

void Monitor::AnalyzeE2E() {
  // std::cout << "\n>>> Calculating Scheduler Latency from DB... <<<"

  //           << std::endl;

  // try {
  //   pqxx::connection C(config_.db_conn_string);

  //   if (!C.is_open()) {
  //     std::cerr << "[Monitor] Failed to connect to DB for E2E analysis."

  //               << std::endl;

  //     return;
  //   }

  //   pqxx::work txn(C);

  //   // ---------------------------------------------------------

  //   // SQL 查询逻辑：

  //   // 1. state = 3 (SUCCESS): 只统计成功的任务

  //   // 2. submit_ts > 0 AND finish_ts > 0: 确保时间戳有效

  //   // 3. LIMIT: 限制条数，避免数据量过大撑爆内存 (取配置的总请求数即可)

  //   // ---------------------------------------------------------

  //   std::string sql =

  //       "SELECT submit_ts, finish_ts "

  //       "FROM public.task "

  //       "WHERE state = 2 AND finish_ts > 0 AND submit_ts > 0 "

  //       "LIMIT " +

  //       std::to_string(config_.total_requests);

  //   pqxx::result r = txn.exec(sql);

  //   // 存储延迟数据 (ms)

  //   std::vector<double> latencies;

  //   latencies.reserve(r.size());

  //   for (const auto& row : r) {
  //     long long submit = row[0].as<long long>();

  //     long long finish = row[1].as<long long>();

  //     // 计算差值

  //     double diff = static_cast<double>(finish - submit);

  //     // 过滤脏数据 (比如时钟回拨导致的负数)

  //     if (diff >= 0) {
  //       latencies.push_back(diff);
  //     }
  //   }

  //   if (latencies.empty()) {
  //     std::cout << "[Warning] No valid task data found (state=2). "

  //               << "Maybe workers haven't finished writing back yet?"

  //               << std::endl;

  //     return;
  //   }

  //   // ---------------------------------------------------------

  //   // 统计计算逻辑

  //   // ---------------------------------------------------------

  //   // 1. 排序 (Pxx 计算必须排序)

  //   std::sort(latencies.begin(), latencies.end());

  //   // 2. 计算平均值

  //   double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);

  //   double avg = sum / latencies.size();

  //   // 3. 计算分位值

  //   double p50 = latencies[static_cast<size_t>(latencies.size() * 0.50)];

  //   double p95 = latencies[static_cast<size_t>(latencies.size() * 0.95)];

  //   // 确保索引不越界

  //   size_t p99_idx = static_cast<size_t>(latencies.size() * 0.99);

  //   if (p99_idx >= latencies.size()) p99_idx = latencies.size() - 1;

  //   double p99 = latencies[p99_idx];

  //   // 4. 打印报告

  //   std::cout << "\n[End-to-End (E2E) Latency Report]" << std::endl;

  //   std::cout << "  Count: " << latencies.size() << std::endl;

  //   std::cout << "  AVG:   " << std::fixed << std::setprecision(2) << avg

  //             << " ms" << std::endl;

  //   std::cout << "  P50:   " << p50 << " ms" << std::endl;

  //   std::cout << "  P95:   " << p95 << " ms" << std::endl;

  //   std::cout << "  P99:   " << "\033[1;31m" << p99 << " ms\033[0m"

  //             << std::endl;

  // } catch (const std::exception& e) {
  //   std::cerr << "[Monitor E2E Fatal] " << e.what() << std::endl;
  // }
}
}  // namespace dts::test