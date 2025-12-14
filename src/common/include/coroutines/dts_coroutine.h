#pragma once

#include <coroutine>
#include <functional>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "grpc_base.h"
#include "error/grpc_error.h"

namespace dts::common {
using dts::common::TagProcessor;

// 1. 定义协程任务类型 (C++20 标准样板代码)
// 作用：告诉编译器这是一个协程函数，返回值是 CoTask
struct CoTask {
  struct promise_type {
    CoTask get_return_object() { return {}; }
    std::suspend_never initial_suspend() { return {}; }  // 立即执行，不挂起
    std::suspend_never final_suspend() noexcept {
      return {};
    }  // 结束后不保留状态
    void return_void() {}
    void unhandled_exception() {
      std::terminate();
    }  // 生产环境建议做更细致的异常处理
  };
};

// 2. 通用 gRPC 协程等待器 (核心魔法)
// ResponseType: 比如 SubmitDagResponse
template <typename ResponseType>
class GrpcAwaiter : public TagProcessor {
 public:
  // 定义 Lambda 类型：负责调用 stub->PrepareAsyncXXX
  using ReaderPtr =
      std::unique_ptr<grpc::ClientAsyncResponseReader<ResponseType>>;
  using InitFunc =
      std::function<ReaderPtr(grpc::ClientContext*, ResponseType*, void*)>;

  // 构造函数
  GrpcAwaiter(InitFunc init_func) : init_func_(std::move(init_func)) {}

  // --- Awaitable 接口 ---

  // 1. 是否准备好：永远返回 false，强制挂起协程
  bool await_ready() const noexcept { return false; }

  // 2. 挂起时执行：在此处发起 gRPC 请求
  void await_suspend(std::coroutine_handle<> h) {
    handle_ = h;  // 保存句柄，以便稍后恢复

    // 设置默认超时 (10秒)
    context_.set_deadline(std::chrono::system_clock::now() +
                          std::chrono::seconds(10));

    // 调用 Lambda 创建 Reader
    // 【关键】将 'this' (GrpcAwaiter*) 作为 tag 传入
    reader_ = init_func_(&context_, &response_, this);

    if (!reader_) {
      // 防御：Stub 为空或创建失败
      status_ =
          grpc::Status(grpc::StatusCode::UNAVAILABLE, "Failed to init RPC");
      // 必须手动 resume，否则协程永远卡死
      h.resume();
      return;
    }

    // 启动调用流程 (PrepareAsync -> StartCall -> Finish)
    reader_->StartCall();
    reader_->Finish(&response_, &status_, (void*)this);
  }

  // 3. 恢复时执行：返回结果给 co_await 调用者
  ResponseType await_resume() {
    // 检查 gRPC 传输层状态
    if (!status_.ok()) {
      // 这里假设你有 GrpcError 类，如果没有，改用 std::runtime_error
      throw GrpcError(status_);
    }

    // 检查业务层 Header (泛型检查)
    // 使用 if constexpr 确保编译通过 (前提是 ResponseType 必须有 header 方法)
    if constexpr (requires { response_.header(); }) {
      if (response_.header().has_error()) {
        const auto& err = response_.header().error();
        throw std::runtime_error("Server Rejected: " + err.msg());
      }
    }

    return std::move(response_);
  }

  // 被 CompleteRpc 线程调用
  void Proceed(bool ok) override {
    // 如果 gRPC 队列关闭，设置一个错误状态
    if (!ok && status_.ok()) {
      status_ =
          grpc::Status(grpc::StatusCode::INTERNAL, "CompletionQueue Shutdown");
    }

    // 【核心】恢复协程执行
    if (handle_) handle_.resume();
  }

 private:
  InitFunc init_func_;
  grpc::ClientContext context_;
  ResponseType response_;
  grpc::Status status_;
  ReaderPtr reader_;  // 必须持有 reader，否则请求会被销毁
  std::coroutine_handle<> handle_;
};

}  // namespace dts