#pragma once

namespace dts::common {

// 所有放入 gRPC CompletionQueue 的对象必须继承此接口
struct TagProcessor {
  virtual ~TagProcessor() = default;

  // 核心回调接口
  // ok: true 表示操作成功 (gRPC 层的成功)，false 通常表示队列关闭或取消
  virtual void Proceed(bool ok) = 0;
};

}  // namespace dts::common