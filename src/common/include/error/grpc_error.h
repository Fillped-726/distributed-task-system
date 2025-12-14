#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/status.h>

#include <string>
#include <stdexcept>
#include <sstream>

#include "logger.hpp"

namespace dts::common {

/**
 * @brief 统一封装 gRPC 异常
 * 继承 runtime_error，使得上层可以用 standard exception 捕获
 */
class GrpcError : public std::runtime_error {
 public:
  // 使用 ::grpc::Status 确保使用全局命名空间，防止命名空间污染
  explicit GrpcError(const ::grpc::Status& status)
      : std::runtime_error(FormatMessage(status)), status_(status) {}

  [[nodiscard]] ::grpc::Status status() const { return status_; }
  [[nodiscard]] ::grpc::StatusCode code() const { return status_.error_code(); }

 private:
  // 辅助函数：构造详细的错误字符串，包含 Code 和 Details
  static std::string FormatMessage(const ::grpc::Status& status) {
    std::ostringstream oss;
    oss << "gRPC Error: "
        << "Code=" << status.error_code() << " (" << status.error_code() << ") "
        << "| Msg=\"" << status.error_message() << "\"";

    // 如果有 binary details
    if (!status.error_details().empty()) {
      oss << " | Details=\"" << status.error_details() << "\"";
    }
    return oss.str();
  }

  ::grpc::Status status_;
};

}  // namespace dts::common

// -----------------------------------------------------------
// 核心宏定义：集成 dts::LogPrefix 和 GrpcError
// -----------------------------------------------------------

/**
 * @brief 检查 gRPC 状态，如果失败则打印日志并抛出异常
 */
#define DTS_CHECK_GRPC(status)                                    \
  do {                                                            \
    const ::grpc::Status& _macro_status = (status);               \
    if (!_macro_status.ok()) {                                    \
      LOG_ERROR << "[GrpcCheck] Call failed! "                    \
                << "Code: " << _macro_status.error_code() << ", " \
                << "Msg: " << _macro_status.error_message();      \
      throw dts::common::error::GrpcError(_macro_status);         \
    }                                                             \
  } while (0)

/**
 * @brief 仅检查并记录日志，不抛出异常 (用于非关键路径)
 * 【修复 2】移除了文件末尾多余的反斜杠，并整理了格式
 */
#define DTS_LOG_GRPC_FAIL(status)                                \
  do {                                                           \
    const ::grpc::Status& _macro_status = (status);              \
    if (!_macro_status.ok()) {                                   \
      LOG_WARN << "[GrpcCheck] Call failed (ignored). "          \
               << "Code: " << _macro_status.error_code() << ", " \
               << "Msg: " << _macro_status.error_message();      \
    }                                                            \
  } while (0)
