#include "submitter.hpp"
#include "dag_builder.hpp"
#include "grpc_client.hpp"
#include <grpcpp/grpcpp.h>
#include <chrono>

Submitter::Submitter(const BenchmarkConfig& config, MetricsCollector& metrics)
    : config_(config), metrics_(metrics) {}

void Submitter::Run() {
  std::vector<std::thread> threads;
  for (int i = 0; i < config_.num_threads; ++i) {
    threads.emplace_back(&Submitter::WorkerThread, this, i);
  }
  for (auto& t : threads) t.join();
}

void Submitter::WorkerThread(int thread_id) {
  auto channel = grpc::CreateChannel(config_.api_server_addr,
                                     grpc::InsecureChannelCredentials());
  dts::GrpcClient client(channel);

  int reqs_per_thread = config_.total_requests / config_.num_threads;

  for (int i = 0; i < reqs_per_thread; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    try {
      std::string key =
          "bench_" +
          std::to_string(
              std::chrono::system_clock::now().time_since_epoch().count()) +
          "_" + std::to_string(thread_id) + "_" + std::to_string(i);

      dts::client::DagBuilder builder(key);
      builder.AddTask("t1", "echo", "{}");
      auto req = builder.BuildProto();
      req.set_client_id("bench_modular");

      client.submit_dag_sync(req);  // 记得确保 client 里设置了 deadline

      auto end = std::chrono::high_resolution_clock::now();
      std::chrono::duration<double, std::milli> diff = end - start;

      metrics_.success_count++;
      metrics_.RecordLatency(diff.count());

    } catch (...) {
      metrics_.fail_count++;
    }
  }
}