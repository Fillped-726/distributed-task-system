#include <string_view>
#include <concepts>
#include "dts/error/error.grpc.pb.h"

// 假设你的 proto 生成的代码在这些命名空间下
using dts::error::JobErr;
using dts::error::SysErr;

namespace dts::common {

/**
 * @brief 内部辅助：设置 Error 对象的具体内容
 */
inline void FillErrorInfo(dts::error::Error* pb_error, JobErr code,
                          std::string_view msg) {
  pb_error->set_job(code);  // 设置 oneof 中的 job 字段
  pb_error->set_msg(std::string(msg));
}

inline void FillErrorInfo(dts::error::Error* pb_error, SysErr code,
                          std::string_view msg) {
  pb_error->set_sys(code);  // 设置 oneof 中的 sys 字段
  pb_error->set_msg(std::string(msg));
}

/**
 * @brief 核心模板：为任何带有标准 Header 的 Response 设置 Job 错误
 * @tparam T Response 类型，需具备 mutable_header() 方法
 */
template <typename T>
void SetJobError(T* resp, JobErr code, std::string_view msg) {
  if (!resp) return;
  // mutable_header() 如果不存在会自动创建 RpcRespHeader 对象
  // mutable_error() 如果不存在会自动创建 Error 对象
  auto* pb_error = resp->mutable_header()->mutable_error();
  FillErrorInfo(pb_error, code, msg);
}

/**
 * @brief 为 Response 设置系统错误
 */
template <typename T>
void SetSysError(T* resp, SysErr code, std::string_view msg) {
  if (!resp) return;
  auto* pb_error = resp->mutable_header()->mutable_error();
  FillErrorInfo(pb_error, code, msg);
}

}  // namespace dts::common