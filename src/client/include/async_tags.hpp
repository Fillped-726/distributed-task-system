#pragma once
#include <grpcpp/grpcpp.h>
#include <future>
#include <memory>
#include <functional>
#include <mutex>
#include "types.hpp"
#include "exceptions.hpp"
#include "grpc_base.h"

namespace dts {
using dts::common::TagProcessor;

// ---------- 异步上下文 ----------

template <typename Tag>
struct AsyncTagBase : public TagProcessor {
  void Proceed(bool ok) override { static_cast<Tag*>(this)->ProceedImpl(ok); }

 protected:
  ~AsyncTagBase() = default;
};

struct AsyncDagSubmitTag : AsyncTagBase<AsyncDagSubmitTag> {
  // 构造函数：初始化 Promise 和 Callback
  explicit AsyncDagSubmitTag(std::shared_ptr<std::promise<SubmitDagResponse>> p,
                             DagCallback cb = {})
      : promise(std::move(p)), callback(std::move(cb)) {}

  // --- 核心状态机逻辑 ---
  void ProceedImpl(bool ok) {
    switch (step_) {
      case kLaunch:
        // 阶段 1：RPC 调用刚启动 (如果使用了 PrepareAsync + StartCall)
        if (!ok) {
          status = grpc::Status(grpc::StatusCode::INTERNAL,
                                "Failed to start RPC call");
          // 如果启动都失败了，直接去处理结果，不需要调 Finish
          SetResult();
          delete this;
          return;
        }

        // 正常启动，进入下一阶段：告诉 gRPC 我们准备好接收结果了
        // 'this' 再次作为 tag 传入，当 RPC 完成时，CQ 会再次吐出这个 tag
        step_ = kFinish;
        reader->Finish(&response, &status, this);
        break;

      case kFinish:
        // 阶段 2：RPC 网络传输完成 (无论成功还是超时)
        if (!ok) {
          // 这里的 !ok 通常意味着 CQ 正在关闭或者严重的传输层错误
          status = grpc::Status(grpc::StatusCode::INTERNAL,
                                "RPC finished with !ok (CQ shutdown?)");
        }

        // 处理业务逻辑 + 设置 Promise
        SetResult();

        // --- 自杀 (Self-delete) ---
        // 这是 gRPC Async 模式的标准做法：Tag 的生命周期随 RPC 结束而结束
        delete this;
        break;
    }
  }

  // --- 核心业务逻辑：解析结果 ---
  void SetResult() {
    std::call_once(once_, [&] {
      // 1. 先判断 gRPC 传输层是否成功 (网络通不通，有没有超时)
      if (status.ok()) {
        // 2. 传输成功，检查业务层 Header 是否有错误
        // (根据你的 proto 定义：response.header().error())
        if (response.has_header() && response.header().has_error()) {
          const auto& err_obj = response.header().error();

          // --- 面试考点：错误码映射 (Error Code Mapping) ---
          // 将业务错误 (Business Error) 转换为标准的 gRPC 状态码
          // 这样上层调用者可以统一通过 catch(GrpcError) 来处理
          grpc::StatusCode code = grpc::StatusCode::FAILED_PRECONDITION;

          // 示例：可以根据 err_obj.sys() 的值做更细粒度的映射
          // if (err_obj.sys() == SYS_INVALID_PARAM) code =
          // grpc::StatusCode::INVALID_ARGUMENT;

          // 覆盖原本是 OK 的 status，变为错误状态
          status = grpc::Status(code, "BizError: " + err_obj.msg());

          // 失败路径：设置异常
          promise->set_exception(std::make_exception_ptr(GrpcError(status)));

          // 回调通知 (即便失败，如果调用方需要 response
          // 里的某些元数据，也可以传回去)
          if (callback) callback(response, status);

        } else {
          // --- 成功路径 ---
          promise->set_value(response);
          if (callback) callback(response, status);
        }

      } else {
        // 3. gRPC 传输层失败 (Timeout, Unavailable 等)
        promise->set_exception(std::make_exception_ptr(GrpcError(status)));
        if (callback) callback(SubmitDagResponse{}, status);
      }
    });
  }

  // --- 成员变量 ---
  enum Step { kLaunch, kFinish } step_{kLaunch};

  // 上下文与数据
  grpc::ClientContext context;
  SubmitDagResponse response;
  grpc::Status status;
  std::unique_ptr<grpc::ClientAsyncResponseReader<SubmitDagResponse>> reader;

  // 异步通知机制
  std::shared_ptr<std::promise<SubmitDagResponse>> promise;
  DagCallback callback;
  std::once_flag once_;
};

// struct AsyncCancelTag  : AsyncTagBase<AsyncCancelTag> {
//     AsyncCancelTag() = default;
//     void ProceedImpl(bool ok) {
//         if (step_ == kLaunch && ok) {
//             step_ = kFinish;
//             reader->Finish(&response, &status, this);
//             return;
//         }
//         if (!ok && step_ == kLaunch) status =
//         grpc::Status(grpc::StatusCode::INTERNAL, "cq !ok");
//         promise.set_value(status.ok() && response.success());
//         delete this;
//     }

//     enum Step { kLaunch, kFinish } step_{kLaunch};
//     CancelTaskRequest request;
//     CancelTaskResponse response;
//     std::promise<bool> promise;
//     std::unique_ptr<grpc::ClientAsyncResponseReader<CancelTaskResponse>>
//     reader; grpc::ClientContext context;
// };

// struct AsyncQueryTag   : AsyncTagBase<AsyncQueryTag> {
//     AsyncQueryTag() = default;
//     void ProceedImpl(bool ok) {
//         if (step_ == kLaunch && ok) {
//             step_ = kFinish;
//             reader->Finish(&response, &status, this);
//             return;
//         }
//         if (!ok && step_ == kLaunch) status =
//         grpc::Status(grpc::StatusCode::INTERNAL, "cq !ok"); SetResult();
//         delete this;
//     }

//     enum Step { kLaunch, kFinish } step_{kLaunch};
//     QueryTaskRequest request;
//     QueryTaskResponse response;
//     std::promise<Task> promise;
//     std::unique_ptr<grpc::ClientAsyncResponseReader<QueryTaskResponse>>
//     reader; grpc::ClientContext context;

//     void SetResult() {
//         std::call_once(once_, [&] {
//             if (!status.ok()) {
//                 // gRPC 级别的错误 (例如网络不通)
//                 promise.set_exception(std::make_exception_ptr(GrpcError(status)));
//             } else if (response.header().code() != 0) {
//                 // gRPC 通信成功，但业务逻辑返回错误 (例如 task_id 不存在)
//                 promise.set_exception(std::make_exception_ptr(
//                     std::runtime_error("Server Error: " +
//                     response.header().msg())
//                 ));
//             } else {
//                 // 完全成功
//                 Task task = TaskFromProto(response.task());
//                 promise.set_value(std::move(task));
//             }
//         });
//     }

// private:
//     std::once_flag once_;
// };

// todo，待完善
//  struct AsyncListenTag : AsyncTagBase<AsyncListenTag> {
//      enum Step { kStart = 0, kRead, kFinish, kDone };

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

}  // namespace dts