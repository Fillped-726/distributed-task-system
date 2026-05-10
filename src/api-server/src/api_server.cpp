#include "api_server.hpp"
#include <iostream>

AsyncServer::AsyncServer(std::shared_ptr<DatabasePool> db_conn)
    : db_conn_(db_conn) {}

AsyncServer::~AsyncServer() { Shutdown(); }

void AsyncServer::Run(uint16_t port) {
  if (!submit_task_) {
    LOG(FATAL)
        << "[AsyncServer] Config Error: 'submit_task_' handler is missing!";
    return;
  }

  if (!get_job_status_) {
    LOG(WARNING) << "[AsyncServer] Warning: 'get_job_status_' is missing!";
  }

  std::string addr = port ? "0.0.0.0:" + std::to_string(port) : "0.0.0.0:0";
  grpc::ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials(),
                           &listen_port_);
  builder.RegisterService(&service_);

  // CQ 数量 = CPU 核心数
  size_t cq_count = std::thread::hardware_concurrency();
  for (size_t i = 0; i < cq_count; ++i) {
    cqs_.emplace_back(builder.AddCompletionQueue());
  }

  server_ = builder.BuildAndStart();
  LOG(INFO) << "[AsyncServer] Listening on " << addr
            << ", CQ Count: " << cq_count;

  // 预分配 Context 数量
  int concurrency_factor =
      std::thread::hardware_concurrency() * 20;  // 适当调大并发度
  const char* env = std::getenv("DTS_INITIAL_CONTEXT");
  if (env) concurrency_factor = std::stoi(env);

  for (int i = 0; i < concurrency_factor; ++i) {
    auto* cq = cqs_[i % cq_count].get();

    // 【关键修改】定义具体的注册逻辑 Lambda
    // 这样 AsyncCallContext 就不用知道 RequestSubmitDag 的存在
    auto setup_logic =
        [this](grpc::ServerContext* ctx, PbSubmitDagRequest* req,
               grpc::ServerAsyncResponseWriter<PbSubmitDagResponse>* resp,
               grpc::ServerCompletionQueue* new_cq,
               grpc::ServerCompletionQueue* not_cq, void* tag) {
          this->service_.RequestSubmitDag(ctx, req, resp, new_cq, not_cq, tag);
        };

    // 创建第一个 Context
    new AsyncCallContext<AsyncTaskService, PbSubmitDagRequest,
                         PbSubmitDagResponse>(
        &service_, cq,
        setup_logic,                             // 传入 lambda
        [this](auto* ctx) { OnSubmitDag(ctx); }  // 业务回调
    );

    // -------------------------------------------------------
    // 2. 【新增】注册 GetJobStatus (复用你的泛型设计)
    // -------------------------------------------------------
    auto setup_query =
        [this](grpc::ServerContext* ctx, PbGetJobStatusRequest* req,
               grpc::ServerAsyncResponseWriter<PbGetJobStatusResponse>* resp,
               grpc::ServerCompletionQueue* new_cq,
               grpc::ServerCompletionQueue* not_cq, void* tag) {
          // 这里的关键是调用 RequestGetJobStatus
          this->service_.RequestGetJobStatus(ctx, req, resp, new_cq, not_cq,
                                             tag);
        };

    new AsyncCallContext<AsyncTaskService, PbGetJobStatusRequest,
                         PbGetJobStatusResponse>(
        &service_, cq,
        setup_query,                                // 注入查询接口的注册逻辑
        [this](auto* ctx) { OnGetJobStatus(ctx); }  // 注入查询接口的回调
    );
  }

  cq_threads_.reserve(cq_count);
  for (auto& cq : cqs_) {
    cq_threads_.emplace_back(
        [this, cq_ptr = cq.get()] { DriveCompletionQueue(cq_ptr); });
  }
}

void AsyncServer::Shutdown() {
  if (shutdown_.exchange(true)) return;  // 防止重复 Shutdown

  LOG(INFO) << "[AsyncServer] Shutting down...";
  server_->Shutdown();
  for (auto& cq : cqs_) cq->Shutdown();
  for (auto& t : cq_threads_) {
    if (t.joinable()) t.join();
  }
  LOG(INFO) << "[AsyncServer] Shutdown complete.";
}

void AsyncServer::DriveCompletionQueue(grpc::ServerCompletionQueue* cq) {
  void* tag;
  bool ok;
  // 主循环
  while (cq->Next(&tag, &ok)) {
    if (tag) {
      static_cast<TagProcessor*>(tag)->Proceed(ok);
    }
  }
}

// 业务处理入口
void AsyncServer::OnSubmitDag(
    AsyncCallContext<AsyncTaskService, PbSubmitDagRequest, PbSubmitDagResponse>*
        ctx) {
  SafeExecute(ctx, submit_task_, "SubmitDag");
}

void AsyncServer::OnGetJobStatus(
    AsyncCallContext<AsyncTaskService, PbGetJobStatusRequest,
                     PbGetJobStatusResponse>* ctx) {
  SafeExecute(ctx, get_job_status_, "GetJobStatus");
}