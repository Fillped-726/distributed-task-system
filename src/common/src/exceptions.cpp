#include "exceptions.hpp"

namespace dts {

GrpcError::GrpcError(const grpc::Status& s)
    : std::runtime_error(s.error_message()), code_(s.error_code()) {}

grpc::StatusCode GrpcError::code() const noexcept { return code_; }

} // namespace dts