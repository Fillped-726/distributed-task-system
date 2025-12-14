#pragma once
#include <grpcpp/alarm.h>
#include <grpcpp/grpcpp.h>
#include <grpc/support/time.h>
#include <vector>
#include <atomic>
#include <memory>
#include <functional>
#include <thread>
#include "logger.hpp"
#include "dts/task/task.grpc.pb.h"
#include "dts/task/task.pb.h"
#include "dts/service/task_service.grpc.pb.h"
#include "dts/service/task_service.pb.h"
#include "grpc_base.h"

using PbSubmitDagResponse = dts::service::SubmitDagResponse;
using dts::service::TaskService;
using PbSubmitDagRequest = dts::service::SubmitDagRequest;
using AsyncTaskService = dts::service::TaskService::AsyncService;
using dts::common::TagProcessor;

// 前向声明
class AsyncServer;

// 通用异步上下文（一次写成，终身复用）
template <class Service, class Request, class Response>
class AsyncCallContext : public TagProcessor {
 public:
  using ProceedFunc = std::function<void(AsyncCallContext*)>;

  AsyncCallContext(Service* svc, grpc::ServerCompletionQueue* cq,
                   ProceedFunc pf)
      : service_(svc),
        cq_(cq),
        responder_(&ctx_),
        proceed_(std::move(pf)),
        status_(CallStatus::CREATE) {
    RequestNext();  // 第一次注册
  }

  void Proceed(bool ok = true) override {
    if (!ok) {
      delete this;
      return;
    }

    switch (status_) {
      case CallStatus::CREATE:
        if (proceed_) proceed_(this);
        status_ = CallStatus::FINISH;
        responder_.Finish(response_, grpc::Status::OK, this);
        break;

      case CallStatus::FINISH: {
        // 1. 保存现场
        auto* svc = service_;
        auto* cq = cq_;
        auto pf = proceed_;
        // 2. 自杀
        delete this;
        // 3. 同线程立即注册新对象（防止 CQ 饿死）
        new AsyncCallContext<Service, Request, Response>(svc, cq, pf);
      }
        return;  // 必须 return，不再访问已销毁内存
    }
  }

  void RequestNext();

  grpc::ServerContext ctx_;
  Request request_;
  Response response_;

 private:
  enum class CallStatus { CREATE, FINISH, REARM };
  Service* service_;
  grpc::ServerCompletionQueue* cq_;
  grpc::ServerAsyncResponseWriter<Response> responder_;
  ProceedFunc proceed_;
  CallStatus status_;
};
// 高性能异步服务器（零手写状态机）
namespace dts {
namespace common {
class DatabasePool;
}
}  // namespace dts

using dts::common::DatabasePool;

class AsyncServer final {
 public:
  explicit AsyncServer(std::shared_ptr<DatabasePool> db_conn);
  ~AsyncServer();
  void Run(uint16_t port = 0);  // 0 = 随机端口
  void Shutdown();              // 优雅停机
  uint16_t ListenPort() const { return listen_port_; }

  // 业务注册点：只写函数，不写类
  using SubmitTaskFunc =
      std::function<void(std::shared_ptr<DatabasePool> db_conn,  // <-- 新增!
                         PbSubmitDagRequest* request,            // gRPC 请求
                         PbSubmitDagResponse* response           // gRPC 响应
                         )>;
  void SetSubmitTaskHandler(SubmitTaskFunc f) { submit_task_ = std::move(f); }

 private:
  void DriveCompletionQueue(grpc::ServerCompletionQueue* cq);
  void OnSubmitDag(AsyncCallContext<AsyncTaskService, PbSubmitDagRequest,
                                    PbSubmitDagResponse>* ctx);

  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
  AsyncTaskService service_;
  std::unique_ptr<grpc::Server> server_;
  std::atomic<bool> shutdown_{false};
  std::vector<std::thread> cq_threads_;
  int listen_port_ = 0;

  SubmitTaskFunc submit_task_;  // 业务回调
  std::shared_ptr<DatabasePool> db_conn_;
};
