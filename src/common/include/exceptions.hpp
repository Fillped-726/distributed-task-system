#pragma once
#include <stdexcept>
#include <grpcpp/support/status.h>

namespace dts {

class GrpcError : public std::runtime_error {
public:
    explicit GrpcError(const grpc::Status& s);
    grpc::StatusCode code() const noexcept;
private:
    grpc::StatusCode code_;
};

} // namespace dts