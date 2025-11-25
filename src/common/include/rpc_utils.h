#pragma once

#include <grpcpp/grpcpp.h>
#include "dts/internal/internal_service.pb.h" 
#include "logger.hpp"

namespace dts {
namespace common {

// 统一检查 gRPC Status 和 业务 Header Error
template <typename ResponseType>
bool CheckRpcStatus(const grpc::Status& status, const ResponseType& response, const std::string& rpc_name) {
    // 1. 检查 gRPC 传输层错误
    if (!status.ok()) {
        LOG_ERROR << rpc_name << " RpcFailed: " 
                  << status.error_code() << ": " << status.error_message();
        return false;
    }

    // 2. 检查业务层错误 (header.error)
    if (response.header().has_error()) {
        const auto& err = response.header().error();
        
        int err_code_val = 0;
        std::string err_type = "Unknown";

        if (err.has_sys()) {
            err_code_val = err.sys();
            err_type = "SysErr";
        } else if (err.has_job()) {
            err_code_val = err.job();
            err_type = "JobErr";
        }

        // [修正] 使用 .msg() 而不是 .message()
        LOG_ERROR << rpc_name << " BizError [" << err_type << ":" << err_code_val << "]: " 
                  << err.msg(); 
                  
        return false;
    }

    return true;
}

} // namespace common
} // namespace dts