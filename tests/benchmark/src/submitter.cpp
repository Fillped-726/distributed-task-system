#include "submitter.hpp"
#include "dag_builder.hpp"  // 假设这是你用来构建 Proto 的工具
// 如果没有 dag_builder，直接用 proto 头文件也可以
#include "dts/service/task_service.grpc.pb.h"
#include "converters.hpp"

#include <grpcpp/grpcpp.h>
#include <chrono>
#include <string>
#include <iostream>

namespace dts::test {

// 简化引用
using dts::service::SubmitDagRequest;
using dts::service::SubmitDagResponse;
using dts::service::TaskService;

Submitter::Submitter(const BenchmarkConfig& config, MetricsCollector& metrics)
    : config_(config), metrics_(metrics) {}

void Submitter::Run() {
  std::cout << "Submitter started with " << config_.num_threads << " threads."
            << std::endl;
  std::vector<std::thread> threads;
  for (int i = 0; i < config_.num_threads; ++i) {
    threads.emplace_back(&Submitter::WorkerThread, this, i);
  }
  for (auto& t : threads) t.join();
  std::cout << "All submitter threads finished." << std::endl;
}

void Submitter::Stop() {
  // 如果需要实现中途停止，可以加个 atomic<bool> running_
}

void Submitter::WorkerThread(int thread_id) {
  // 1. 建立 gRPC 连接
  // 使用 Insecure 凭证，生产环境可能需要 SSL
  auto channel = grpc::CreateChannel(config_.api_server_addr,
                                     grpc::InsecureChannelCredentials());
  auto stub = TaskService::NewStub(channel);

  int reqs_per_thread = config_.total_requests / config_.num_threads;

  // -----------------------------------------------------------------------
  // 2. [优化] 预构造 "模板 DAG" (A -> B)
  // 避免在循环里频繁 new 对象，节省客户端 CPU
  // -----------------------------------------------------------------------
  SubmitDagRequest template_req;
  template_req.set_client_id("bench_worker_" + std::to_string(thread_id));

  // Task 1: 入口任务 (Root)
  auto* t1 = template_req.add_tasks();
  t1->set_natural_id("root_task");
  t1->set_func_name("echo");  // 确保 Worker 有这个处理器，或者用 "echo"
  t1->set_func_params(R"({
    "cost": 5,
    "priority": "high",
    "region": "cn-north"
})");

  // Task 2: 子任务 (Child) - 用于测试 Redis Lua 触发逻辑
  auto* t2 = template_req.add_tasks();
  t2->set_natural_id("child_task");
  t2->set_func_name("echo");
  t2->set_func_params(R"({
    "cost": 5,
    "priority": "high",
    "region": "cn-north"
})");
  t2->set_priority(5);

  // Edge: Root -> Child
  auto* edge = template_req.add_edges();
  edge->set_parent_natural_id("root_task");
  edge->set_child_natural_id("child_task");

  int tasks_per_dag = template_req.tasks_size();  // 这里是 2

  // -----------------------------------------------------------------------
  // 3. 发压循环
  // -----------------------------------------------------------------------
  for (int i = 0; i < reqs_per_thread; ++i) {
    // 复制模板
    SubmitDagRequest req = template_req;

    // 修改唯一标识 (幂等键)
    // 格式: bench-{ts}-{thread}-{seq}
    std::string key =
        "bench-" +
        std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count()) +
        "-" + std::to_string(thread_id) + "-" + std::to_string(i);
    req.set_idempotency_key(key);

    grpc::ClientContext context;
    // 设置超时，防止客户端一直卡住
    // 在高并发压测时，服务器响应慢，超时要设宽一点，比如 3秒
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(3000));

    SubmitDagResponse response;

    // 记录开始时间
    auto start = std::chrono::high_resolution_clock::now();

    // 统计：请求已发出
    metrics_.requests_sent++;

    // 发送 RPC
    grpc::Status status = stub->SubmitDag(&context, req, &response);

    // 记录结束时间
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> diff = end - start;

    if (status.ok()) {
      if (!response.header().has_error()) {
        // 成功
        metrics_.success_count++;
        metrics_.tasks_submitted += tasks_per_dag;  // 统计具体的 Task 吞吐
        metrics_.RecordLatency(diff.count());
      } else {
        // 业务错误 (如幂等性冲突)
        // 这种通常不算系统挂了，但也算压测失败
        metrics_.fail_count++;
      }
    } else {
      // 网络/系统错误
      metrics_.fail_count++;
      // 只有连续失败时才打印，防止刷屏
      if (i % 100 == 0) {
        std::cerr << "[Error] gRPC failed: " << status.error_message()
                  << std::endl;
      }
    }
  }
}

}  // namespace dts::test