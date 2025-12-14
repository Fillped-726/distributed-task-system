// http_benchmark.cpp

// 1. 引入和你项目一样的库
#include "httplib.h"
#include "nlohmann/json.hpp"

#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <string>
#include <random>

using json = nlohmann::json;

// --- 配置参数 ---
const std::string SERVER_HOST = "localhost";
const int SERVER_PORT = 8080;  // 假设你的 API Server 跑在 8080
const int THREAD_NUM = 32;     // 并发线程数 (建议设置为 CPU 核心数 x 2)
const int DURATION_SEC = 20;   // 压测持续时间

// --- 全局统计 ---
std::atomic<long> g_success{0};
std::atomic<long> g_fail{0};
std::atomic<bool> g_running{true};

// --- 辅助：生成随机字符串作为幂等键 (防止被服务器去重拦截) ---
std::string generate_uuid() {
  static const char alphanum[] =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  std::string tmp_s;
  tmp_s.reserve(10);
  for (int i = 0; i < 10; ++i) {
    tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
  }
  return tmp_s;
}

// --- 工作线程函数 ---
void worker(int id) {
  // 每个线程创建一个独立的 client，保持 Keep-Alive 长连接
  httplib::Client cli(SERVER_HOST, SERVER_PORT);

  // 设置超时，防止请求卡死
  cli.set_connection_timeout(0, 300000);  // 300ms
  cli.set_read_timeout(5, 0);             // 5s

  // 预先构造好 JSON 模板，避免循环里重复构造带来的性能损耗
  // 我们只需要在循环里修改 idempotency_key
  json j_req;
  j_req["tasks"] = json::array({{{"natural_id", "task_A"},
                                 {"func_name", "DoWork"},
                                 {"func_params", {{"arg1", 100}}},
                                 {"priority", 1}}});
  // 你的代码里不需要 edges 也可以提交，这里为了简单只发一个任务

  while (g_running) {
    // 1. 动态修改幂等键 (关键！否则你的逻辑可能会认为是重复请求直接丢弃)
    j_req["idempotency_key"] =
        "bench_" + std::to_string(id) + "_" + generate_uuid();

    std::string body = j_req.dump();

    // 2. 发起 POST 请求
    auto res = cli.Post("/api/v1/job/submit", body, "application/json");

    // 3. 统计结果
    if (res && res->status == 200) {
      // 还可以进一步检查 body 里的 code 是否为 0
      g_success++;
    } else {
      g_fail++;
      // 调试用：如果失败太多，可以打印一下原因
      // if (res) std::cout << res->status << std::endl;
    }
  }
}

int main() {
  std::cout << "=== DTS API Server 压测工具 ===" << std::endl;
  std::cout << "目标: http://" << SERVER_HOST << ":" << SERVER_PORT
            << "/api/v1/job/submit" << std::endl;
  std::cout << "线程数: " << THREAD_NUM << std::endl;
  std::cout << "--------------------------------" << std::endl;

  // 1. 启动线程
  std::vector<std::thread> threads;
  for (int i = 0; i < THREAD_NUM; ++i) {
    threads.emplace_back(worker, i);
  }

  // 2. 倒计时监控
  for (int i = 0; i < DURATION_SEC; ++i) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    long s = g_success.load();
    long f = g_fail.load();
    std::cout << "[Running] 成功: " << s << " | 失败: " << f
              << " | 当前 QPS: " << (s / (i + 1)) << std::endl;
  }

  // 3. 停止
  g_running = false;
  for (auto& t : threads) {
    if (t.joinable()) t.join();
  }

  // 4. 最终报告
  long total = g_success + g_fail;
  double qps = (double)g_success / DURATION_SEC;

  std::cout << "\n=== 最终报告 ===" << std::endl;
  std::cout << "平均 QPS: " << qps << std::endl;
  std::cout << "总请求: " << total << std::endl;
  std::cout << "成功率: " << (double)g_success / total * 100.0 << "%"
            << std::endl;

  return 0;
}