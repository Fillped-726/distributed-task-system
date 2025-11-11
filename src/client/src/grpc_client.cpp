#include "grpc_client.hpp"

namespace dts {

// ---------- 构造/析构 ----------
GrpcClient::GrpcClient(const std::string& target)
    : stub_(TaskService::NewStub(
          grpc::CreateChannel(target, grpc::InsecureChannelCredentials()))),
      thread_pool_(std::make_shared<ThreadPool>(4)) {
    cq_thread_ = std::thread([this] { CompleteRpc(); });
}

GrpcClient::~GrpcClient() {
    shutdown_ = true;
    cq_.Shutdown();
    if (cq_thread_.joinable()) cq_thread_.join();
}

// ---------- 异步完成分发 ----------
void GrpcClient::CompleteRpc() {
    void* tag = nullptr;
    bool ok = false;

    while (!shutdown_.load(std::memory_order_acquire)) {
        auto status = cq_.AsyncNext(&tag, &ok,
                                    gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                                                 gpr_time_from_seconds(1, GPR_TIMESPAN)));

        switch (status) {
        case grpc::CompletionQueue::NextStatus::GOT_EVENT:
            static_cast<IAsyncTag*>(tag)->Proceed(ok);
            break;
        case grpc::CompletionQueue::NextStatus::SHUTDOWN:
            return;
        case grpc::CompletionQueue::NextStatus::TIMEOUT:
            break;
        }
    }
}

// ---------- 各 RPC 实现 ----------
//To do 提交和结果分离
std::future<SubmitDagResponse> GrpcClient::submit_dag_async(
    const PbSubmitDagRequest& req,
    DagCallback cb
) {
    if (!stub_) {
        auto prom = std::make_shared<std::promise<SubmitDagResponse>>();
        prom->set_exception(std::make_exception_ptr(
            GrpcError(grpc::Status(grpc::StatusCode::UNAVAILABLE, "stub_ is null"))));
        if (cb) cb(SubmitDagResponse{}, grpc::Status(grpc::StatusCode::UNAVAILABLE, "stub_ null"));
        return prom->get_future();
    }

    // 1. 创建 Promise 和 Tag
    auto promise = std::make_shared<std::promise<SubmitDagResponse>>();
    auto* tag = new AsyncDagSubmitTag(promise, std::move(cb));

    // 2. 准备异步 RPC
    tag->reader = stub_->PrepareAsyncSubmitDag(&tag->context, // <-- 调用 PrepareAsyncSubmitDag
                                             req, 
                                             &cq_);

    // 3. (关键) 启动调用，并将 tag 传入
    // StartCall 会立即返回，gRPC 会将 tag(this) 放入完成队列 (触发 kLaunch)
    tag->reader->StartCall();

    tag->reader->Finish(&tag->response, &tag->status, (void*)tag);

    // 4. 返回 future
    return promise->get_future();
}

SubmitDagResponse GrpcClient::submit_dag_sync(const PbSubmitDagRequest& req) {
    if (!stub_) {
        throw GrpcError(grpc::Status(grpc::StatusCode::UNAVAILABLE,
                                     "channel not created / connection refused"));
    }

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::seconds(10));  // 给予 DAG 提交 10 秒超时

    
    SubmitDagResponse resp;
    grpc::Status st = stub_->SubmitDag(&ctx, req, &resp); // 注意: 调用 SubmitDag

    /* 3. (已修改) 判失败 */
    if (!st.ok()) {
        std::cerr << "[ERROR] SubmitDag failed: " // RPC 命名已更改
                  << st.error_code() << " - " << st.error_message() << std::endl;
        throw GrpcError(st);
    }

    /* (可选) 检查服务端的业务 Header */
    if (resp.header().code() != 0) {
        std::cerr << "[ERROR] SubmitDag rejected by server: code="
                  << resp.header().code() << ", msg=" << resp.header().msg() << std::endl;
        // 抛出一个业务异常
        throw std::runtime_error("Server rejected DAG: " + resp.header().msg());
    }

    /* 4. (已修改) 返回完整的 Protobuf 响应 */
    // (删除原有的: return TaskFromProto(resp.task());)
    return resp;
}

std::future<bool> GrpcClient::cancel_task_async(const std::string& task_id) {
    auto tag = std::make_unique<AsyncCancelTag>();
    tag->request.set_task_id(task_id);
    tag->reader = stub_->PrepareAsyncCancelTask(&tag->context,
                                                tag->request, &cq_);
    tag->reader->StartCall();
    tag->reader->Finish(&tag->response, &tag->status, tag.release());
    return tag->promise.get_future();
}

std::future<Task> GrpcClient::query_status_async(const std::string& task_id) {
    auto tag = std::make_unique<AsyncQueryTag>();
    tag->request.set_task_id(task_id);
    tag->reader = stub_->PrepareAsyncQueryTask(&tag->context,
                                                 tag->request, &cq_);
    tag->reader->StartCall();
    tag->reader->Finish(&tag->response, &tag->status, tag.release());
    return tag->promise.get_future();
}

bool GrpcClient::cancel_task(const std::string& task_id) {
    return cancel_task_async(task_id).get();
}
Task GrpcClient::query_status(const std::string& task_id) {
    return query_status_async(task_id).get();
}

// 流式监听--待完善
// void GrpcClient::listen_results(const std::string& client_id, Callback cb) {
//     auto tag = std::make_unique<AsyncListenTag>(std::move(cb));
//     tag->request.set_client_id(client_id);
//     tag->reader = stub_->PrepareAsyncListenResults(&tag->context,
//                                                    tag->request, &cq_);
//     tag->reader->StartCall(tag.release()); 
// }

}   // namespace dts