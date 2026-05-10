#pragma once
#include <grpcpp/alarm.h>
#include <grpcpp/grpcpp.h>
#include <grpc/support/time.h>
#include <vector>
#include <atomic>
#include <memory>
#include <functional>
#include <thread>
#include "logger.hpp"  // 假设你有这个日志库
#include "dts/task/task.grpc.pb.h"
#include "dts/task/task.pb.h"
#include "dts/service/task_service.grpc.pb.h"
#include "dts/service/task_service.pb.h"
#include "grpc_base.h"

using PbSubmitDagResponse = dts::service::SubmitDagResponse;
using PbSubmitDagRequest = dts::service::SubmitDagRequest;
using PbGetJobStatusRequest = dts::service::GetJobStatusRequest;
using PbGetJobStatusResponse = dts::service::GetJobStatusResponse;
using dts::error::JobErr;
using dts::error::SysErr;
using dts::service::TaskService;
using AsyncTaskService = dts::service::TaskService::AsyncService;
using dts::common::TagProcessor;

namespace dts {
namespace common {
class DatabasePool;
}
}  // namespace dts

using dts::common::DatabasePool;

// 通用异步上下文
template <class Service, class Request, class Response>
class AsyncCallContext : public TagProcessor {
 public:
  // 定义业务处理回调类型
  using ProceedFunc = std::function<void(AsyncCallContext*)>;

  // 【修改1】定义 gRPC 注册函数的类型（解决伪泛型）
  // 这是一个通用的函数签名，匹配 service_->RequestXXX 的参数
  using RequestSetupFunc = std::function<void(
      grpc::ServerContext*, Request*,
      grpc::ServerAsyncResponseWriter<Response>*, grpc::ServerCompletionQueue*,
      grpc::ServerCompletionQueue*, void*)>;

  AsyncCallContext(Service* svc, grpc::ServerCompletionQueue* cq,
                   RequestSetupFunc setup_func,  // <-- 注入注册逻辑
                   ProceedFunc pf)
      : service_(svc),
        cq_(cq),
        setup_func_(std::move(setup_func)),  // 保存注册逻辑
        responder_(&ctx_),
        proceed_(std::move(pf)),
        status_(CallStatus::CREATE) {
    RequestNext();
  }

  void Proceed(bool ok = true) override {
    // 1. 如果 gRPC 通知失败（通常是 Server Shutdown），直接销毁
    if (!ok) {
      delete this;
      return;
    }

    switch (status_) {
      case CallStatus::CREATE:
        // --- 阶段 1：请求已收到，开始处理 ---

        // 执行业务逻辑 (OnSubmitDag)
        if (proceed_) proceed_(this);

        // 状态流转：准备发送响应
        status_ = CallStatus::FINISH;

        // 告诉 gRPC 开始发送响应。
        // 注意：Finish 是异步的！发送完成后，CQ 会再次吐出 this (进入下一个
        // case)
        responder_.Finish(response_, grpc::Status::OK, this);

        // 【优化】在这里立即注册一个新的 Context 来接客，保持高并发
        // 不用等当前响应发完，端口空闲了就立马监听
        new AsyncCallContext<Service, Request, Response>(service_, cq_,
                                                         setup_func_, proceed_);
        break;

      case CallStatus::FINISH:
        // --- 阶段 2：响应发送完毕 (IO Complete) ---
        // 【修复】只有 gRPC 通知我们发送完成了，才能安全销毁
        delete this;
        break;
    }
  }

  void RequestNext() {
    status_ = CallStatus::CREATE;
    // 【修改1】调用注入的注册函数，不再硬编码 RequestSubmitDag
    setup_func_(&ctx_, &request_, &responder_, cq_, cq_, this);
  }

  grpc::ServerContext ctx_;
  Request request_;
  Response response_;

 private:
  enum class CallStatus { CREATE, FINISH };
  Service* service_;
  grpc::ServerCompletionQueue* cq_;
  grpc::ServerAsyncResponseWriter<Response> responder_;
  ProceedFunc proceed_;
  RequestSetupFunc setup_func_;  // 保存注册函数
  CallStatus status_;
};

class AsyncServer final {
 public:
  explicit AsyncServer(std::shared_ptr<DatabasePool> db_conn);
  ~AsyncServer();
  void Run(uint16_t port = 0);
  void Shutdown();
  uint16_t ListenPort() const { return listen_port_; }

  // 业务逻辑回调
  using SubmitTaskFunc = std::function<void(
      std::shared_ptr<DatabasePool> db_conn, PbSubmitDagRequest* request,
      PbSubmitDagResponse* response)>;

  void SetSubmitTaskHandler(SubmitTaskFunc f) { submit_task_ = std::move(f); }

  // ==========================================
  // 2. 【新增】GetJobStatus 接口
  // ==========================================
  using GetJobStatusFunc = std::function<void(
      std::shared_ptr<DatabasePool> db_conn, PbGetJobStatusRequest* request,
      PbGetJobStatusResponse* response)>;

  void SetGetJobStatusHandler(GetJobStatusFunc f) {
    get_job_status_ = std::move(f);
  }

 private:
  void DriveCompletionQueue(grpc::ServerCompletionQueue* cq);

  // 具体的业务入口
  void OnSubmitDag(AsyncCallContext<AsyncTaskService, PbSubmitDagRequest,
                                    PbSubmitDagResponse>* ctx);

  void OnGetJobStatus(AsyncCallContext<AsyncTaskService, PbGetJobStatusRequest,
                                       PbGetJobStatusResponse>* ctx);

  // =========================================================
  // ⚡️ 核心抽象：统一业务执行与错误处理模板
  // =========================================================

  /**
   * @brief 安全执行业务逻辑，统一处理异常和系统级错误
   * * @tparam ContextType AsyncCallContext 的具体类型
   * @tparam HandlerFunc 业务回调函数的类型 (std::function)
   * @param ctx 上下文指针
   * @param handler 注册的业务回调
   * @param api_name API 名称 (用于日志)
   */
  template <typename ContextType, typename HandlerFunc>
  void SafeExecute(ContextType* ctx, const HandlerFunc& handler,
                   const char* api_name) {
    try {
      if (handler) {
        // 1. 正常执行业务逻辑
        handler(db_conn_, &ctx->request_, &ctx->response_);
      } else {
        // 2. Handler 未注册 -> 501 Not Implemented
        LOG(ERROR) << "[AsyncServer] API not implemented: " << api_name;
        FillSysError(&ctx->response_, SysErr::SYS_INTERNAL,
                     "API handler not registered on server");
      }
    } catch (const std::exception& e) {
      // 3. 捕获标准异常 -> 500 Internal Error
      LOG(ERROR) << "[AsyncServer] Exception in " << api_name << ": "
                 << e.what();
      FillSysError(&ctx->response_, SysErr::SYS_INTERNAL, e.what());
    } catch (...) {
      // 4. 捕获未知异常 -> 500 Unknown
      LOG(ERROR) << "[AsyncServer] Unknown exception in " << api_name;
      FillSysError(&ctx->response_, SysErr::SYS_INTERNAL,
                   "Unknown server error");
    }
    // 注意：gRPC 的 Finish 由 AsyncCallContext 自动处理，这里只需填充 response
  }

  /**
   * @brief 辅助函数：填充系统级错误 (SysErr)
   * 适配 Proto 中的 oneof 结构
   */
  template <typename ResponseType>
  void FillSysError(ResponseType* resp, SysErr code, const std::string& msg) {
    auto* header = resp->mutable_header();
    auto* error = header->mutable_error();

    // 对应 Proto: oneof code { SysErr sys = 1; }
    error->set_sys(code);
    error->set_msg(msg);

    // 可以在这里统一生成 request_id，如果 request 中没带的话
    if (header->request_id().empty()) {
      header->set_request_id("gen-" + std::to_string(std::time(nullptr)));
    }
  }

  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
  AsyncTaskService service_;
  std::unique_ptr<grpc::Server> server_;
  std::atomic<bool> shutdown_{false};
  std::vector<std::thread> cq_threads_;
  int listen_port_ = 0;

  SubmitTaskFunc submit_task_;
  GetJobStatusFunc get_job_status_;
  std::shared_ptr<DatabasePool> db_conn_;
};