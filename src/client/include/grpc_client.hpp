#pragma once
#include "dts/task/task.pb.h"
#include "dts/task/task.grpc.pb.h"
#include "dts/service/task_service.pb.h"
#include "dts/service/task_service.grpc.pb.h"
#include "coroutines/dts_coroutine.h"
#include <grpcpp/grpcpp.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/support/async_stream.h>
#include <future>
#include <memory>
#include <atomic>
#include <functional>
#include "thread_pool.h"
#include "task.hpp"
#include "utils/utils.hpp"
#include "converters.hpp"
#include "async_tags.hpp"
#include <mutex>

using dts::common::GrpcAwaiter;
using dts::common::TagProcessor;
using dts::common::ThreadPool;

namespace dts {

// ---------- 客户端 ----------
class GrpcClient {
 public:
  explicit GrpcClient(const std::string& target);
  explicit GrpcClient(std::shared_ptr<grpc::Channel> channel);
  virtual ~GrpcClient();

  void CompleteRpc();
  virtual SubmitDagResponse submit_dag_sync(const PbSubmitDagRequest& req);
  bool cancel_task(const std::string& task_id);
  Task query_status(const std::string& task_id);
  void listen_results(const std::string& client_id, DagCallback callback);
  std::future<SubmitDagResponse> submit_dag_async(
      const PbSubmitDagRequest& req, DagCallback callback = nullptr);
  std::future<bool> cancel_task_async(const std::string& task_id);
  std::future<Task> query_status_async(const std::string& task_id);
  GrpcAwaiter<SubmitDagResponse> submit_dag_co(const PbSubmitDagRequest& req);

 private:
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<TaskService::Stub> stub_;
  grpc::CompletionQueue cq_;
  std::shared_ptr<ThreadPool> thread_pool_;
  std::atomic<bool> shutdown_{false};
  std::thread cq_thread_;
};

}  // namespace dts