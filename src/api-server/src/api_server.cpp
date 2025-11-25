#include "api_server.hpp"
#include <iostream>


/* ---------- AsyncCallContext 实现 ---------- */
template <typename S, typename Rq, typename Rp>
void AsyncCallContext<S, Rq, Rp>::RequestNext() {
    status_ = CallStatus::CREATE;
    service_->RequestSubmitDag(&ctx_, &request_, &responder_, cq_, cq_, this);
}

/* 显式实例化（防止模板多次定义） */
template class AsyncCallContext<AsyncTaskService, PbSubmitDagRequest, PbSubmitDagResponse>;

/* ---------- AsyncServer 实现 ---------- */
AsyncServer::AsyncServer(std::shared_ptr<DatabasePool> db_conn)
    : db_conn_(std::move(db_conn)) // <-- 保存传入的连接
{
}
AsyncServer::~AsyncServer() { Shutdown(); }

void AsyncServer::Run(uint16_t port) {

    if (!submit_task_) {
        LOG(FATAL) << "Server config error: 'submit_task_' handler is missing. Server cannot start.";
    }
    
    std::string addr = port ? "0.0.0.0:" + std::to_string(port) : "0.0.0.0:0";
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials(), &listen_port_);
    builder.RegisterService(&service_);

    size_t cq_count = std::thread::hardware_concurrency();
    for (size_t i = 0; i < cq_count; ++i) {
        cqs_.emplace_back(builder.AddCompletionQueue());
    }

    server_ = builder.BuildAndStart();
    LOG(INFO) << "AsyncServer listening on " << listen_port_;

    int c = std::thread::hardware_concurrency() * 2;
    const char* env = std::getenv("DTS_INITIAL_CONTEXT");
    if (env) c = std::stoi(env);

    for (int i = 0; i < c; ++i) {
        auto* cq = cqs_[i % cq_count].get();
        new AsyncCallContext<AsyncTaskService, PbSubmitDagRequest, PbSubmitDagResponse>(
            &service_, cq,
            [this](auto* ctx) { OnSubmitDag(ctx); });
    }

    cq_threads_.reserve(cq_count);
    for (auto& cq : cqs_) {
        cq_threads_.emplace_back([this, cq_ptr = cq.get()] {
            DriveCompletionQueue(cq_ptr);
        });
    }
}

void AsyncServer::Shutdown() {
    shutdown_ = true;
    server_->Shutdown();
    for (auto& cq : cqs_) cq->Shutdown();
    for (auto& t : cq_threads_)
        if (t.joinable()) t.join();
}

void AsyncServer::DriveCompletionQueue(grpc::ServerCompletionQueue* cq) {
    void* tag;
    bool ok;
    while (true) {
        // 被 Shutdown() 唤醒后返回 false
        if (!cq->Next(&tag, &ok)) break;
        if (tag) static_cast<AsyncCallContext<AsyncTaskService,PbSubmitDagRequest,PbSubmitDagResponse>*>(tag)->Proceed(ok);
    }
    // 排空剩余事件
    while (cq->Next(&tag, &ok)) {
        if (tag) static_cast<AsyncCallContext<AsyncTaskService,PbSubmitDagRequest,PbSubmitDagResponse>*>(tag)->Proceed(ok);
    }
}

/* ---------- 业务逻辑 = 普通函数 ---------- */
void AsyncServer::OnSubmitDag(AsyncCallContext<AsyncTaskService, PbSubmitDagRequest, PbSubmitDagResponse>* ctx) {
      submit_task_(db_conn_, &ctx->request_, &ctx->response_);
}
