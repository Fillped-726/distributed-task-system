#pragma once
#include <grpcpp/grpcpp.h>
#include <future>
#include <memory>
#include <functional>
#include <mutex>
#include "types.hpp"
#include "exceptions.hpp"

namespace dts {

// ---------- 异步上下文 ----------

struct IAsyncTag { 
    virtual ~IAsyncTag() = default;
    grpc::Status status;
    virtual void Proceed(bool ok) = 0;
    virtual void ProceedImpl(bool ok) = 0;
 };

template<typename Tag>
struct AsyncTagBase : IAsyncTag {
    void Proceed(bool ok) override {           
        static_cast<Tag*>(this)->ProceedImpl(ok);
    }
protected:
    ~AsyncTagBase() = default; 
};

struct AsyncDagSubmitTag : AsyncTagBase<AsyncDagSubmitTag> {
    
    explicit AsyncDagSubmitTag(std::shared_ptr<std::promise<SubmitDagResponse>> p,
                             DagCallback cb = {})
        : promise(std::move(p)), callback(std::move(cb)) {}

    // --- (2) ProceedImpl 逻辑不变，类型已更新 ---
    // (这个状态机逻辑对于任何一元 RPC 都是通用的，所以内部逻辑不需要大改)
    void ProceedImpl(bool ok) {
        switch (step_) {
        case kLaunch:
            if (!ok) {
                status = grpc::Status(grpc::StatusCode::INTERNAL, "StartCall failed");
                step_ = kFinish; // 直接跳到结束
            } else {
                step_ = kFinish;
            }
            // 无论 ok 与否都调 Finish，让 gRPC 再回包一次
            reader->Finish(&response, &status, this); // 'this' 再次作为 tag
            break;

        case kFinish:
            if (!ok) {
                // Finish 本身失败，覆盖 status
                status = grpc::Status(grpc::StatusCode::INTERNAL, "Finish cq !ok");
            }
            SetResult(); // (3) 调用已修改的 SetResult
            delete this; // 完成，释放内存
            break;
        }
    }

    enum Step { kLaunch, kFinish } step_{kLaunch};
    
    // --- (4) 成员类型已修改 ---
    SubmitDagResponse response; // 响应类型
    std::unique_ptr<grpc::ClientAsyncResponseReader<SubmitDagResponse>> reader; // Reader 类型
    grpc::ClientContext context;
    std::shared_ptr<std::promise<SubmitDagResponse>> promise; // Promise 类型
    DagCallback callback; // Callback 类型

    // --- (5) SetResult 逻辑已修改 ---
    void SetResult() {
        std::call_once(once_, [&] {
            if (status.ok()) {
                // 业务 Header 检查 (可选但推荐)
                if (response.header().code() != 0) {
                     status = grpc::Status(grpc::StatusCode::UNKNOWN, 
                                          "Server rejected: " + response.header().msg());
                     promise->set_exception(std::make_exception_ptr(GrpcError(status)));
                     if (callback) callback(response, status); // 即使失败也返回 response
                } else {
                     // 成功：不再转换 Task，直接设置 SubmitDagResponse
                     promise->set_value(response);
                     if (callback) callback(response, status);
                }
            } else {
                // gRPC 失败
                promise->set_exception(std::make_exception_ptr(GrpcError(status)));
                if (callback) callback(SubmitDagResponse{}, status); // 返回空 response
            }
        });
    }

private:
    std::once_flag once_;
};

struct AsyncCancelTag  : AsyncTagBase<AsyncCancelTag> {
    AsyncCancelTag() = default;
    void ProceedImpl(bool ok) {
        if (step_ == kLaunch && ok) {
            step_ = kFinish;
            reader->Finish(&response, &status, this);
            return;
        }
        if (!ok && step_ == kLaunch) status = grpc::Status(grpc::StatusCode::INTERNAL, "cq !ok");
        promise.set_value(status.ok() && response.success());
        delete this;
    }

    enum Step { kLaunch, kFinish } step_{kLaunch};
    CancelTaskRequest request;
    CancelTaskResponse response;
    std::promise<bool> promise;
    std::unique_ptr<grpc::ClientAsyncResponseReader<CancelTaskResponse>> reader;
    grpc::ClientContext context;
};

struct AsyncQueryTag   : AsyncTagBase<AsyncQueryTag> {
    AsyncQueryTag() = default;
    void ProceedImpl(bool ok) {
        if (step_ == kLaunch && ok) {
            step_ = kFinish;
            reader->Finish(&response, &status, this);
            return;
        }
        if (!ok && step_ == kLaunch) status = grpc::Status(grpc::StatusCode::INTERNAL, "cq !ok");
        SetResult();
        delete this;
    }

    enum Step { kLaunch, kFinish } step_{kLaunch};
    QueryTaskRequest request;
    QueryTaskResponse response;
    std::promise<Task> promise;
    std::unique_ptr<grpc::ClientAsyncResponseReader<QueryTaskResponse>> reader;
    grpc::ClientContext context;

    void SetResult() {
        std::call_once(once_, [&] {
            if (!status.ok()) {
                // gRPC 级别的错误 (例如网络不通)
                promise.set_exception(std::make_exception_ptr(GrpcError(status)));
            } else if (response.header().code() != 0) {
                // gRPC 通信成功，但业务逻辑返回错误 (例如 task_id 不存在)
                promise.set_exception(std::make_exception_ptr(
                    std::runtime_error("Server Error: " + response.header().msg())
                ));
            } else {
                // 完全成功
                Task task = TaskFromProto(response.task()); 
                promise.set_value(std::move(task));          
            }
        });
    }

private:
    std::once_flag once_;
};

//todo，待完善
// struct AsyncListenTag : AsyncTagBase<AsyncListenTag> {
//     enum Step { kStart = 0, kRead, kFinish, kDone };

//     explicit AsyncListenTag(Callback cb)
//         : callback(std::move(cb)), step_(kStart) {}

//     void ProceedImpl(bool ok) override {
//         switch (step_) {
//         /* ----------------------------------
//          * 0. 启动：StartCall() 完成后第一次 Proceed
//          * ---------------------------------- */
//         case kStart:
//             if (!ok) {                    // 连建立就失败
//                 TransitionToFinish(grpc::StatusCode::CANCELLED);
//                 return;
//             }
//             step_ = kRead;                // 正常建立，开始读
//             reader->Read(&response, this);
//             return;

//         /* ----------------------------------
//          * 1. 读消息：ok==true  读到一条
//          *            ok==false 对端半关 / 网络出错
//          * ---------------------------------- */
//         case kRead:
//             if (!ok) {                    // 早退
//                 TransitionToFinish(grpc::StatusCode::CANCELLED);
//                 return;
//             }
//             if (response.has_task()) {    // 正常业务数据
//                 Task task = TaskFromProto(response.task());
//                 if (callback) callback(task, grpc::Status::OK);
//                 response.Clear();
//                 reader->Read(&response, this);   // 继续读下一条
//                 return;
//             }
//             // response 没有 task → 服务端发 EOF
//             TransitionToFinish(grpc::StatusCode::OK);
//             return;

//         /* ----------------------------------
//          * 2. Finish 回调：拿到最终状态
//          * ---------------------------------- */
//         case kFinish:
//             step_ = kDone;
//             if (callback && !status.ok())   // 把最终错误带给用户
//                 callback(Task{}, status);
//             delete this;                    // 唯一 suicide 点
//             return;

//         /* ----------------------------------
//          * 3. 终态：永远不会走到
//          * ---------------------------------- */
//         case kDone:
//             GPR_ASSERT(false);
//         }
//     }

//     /* 统一入口：任何路径想结束流，都调到这儿 */
//     void TransitionToFinish(grpc::StatusCode code) {
//         step_ = kFinish;
//         if (code != grpc::StatusCode::OK)
//             status = grpc::Status(code, "stream aborted");
//         reader->Finish(&status, this);   // 恰好一次 Finish
//     }

//     Callback callback;
//     SubscribeRequest request;
//     TaskResult response;
//     std::unique_ptr<grpc::ClientAsyncReader<TaskResult>> reader;
//     grpc::ClientContext context;
//     Step step_;
// };

} // namespace dts