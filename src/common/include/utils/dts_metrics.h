#pragma once

#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include <memory>
#include <string>
#include <cstdlib>
#include <iostream>
#include "logger.hpp"

namespace dts {

class Metrics {
 public:
  // 1. 获取单例实例
  inline static Metrics& Instance() {
    static Metrics instance;
    return instance;
  }

  // 禁止拷贝和赋值
  Metrics(const Metrics&) = delete;
  Metrics& operator=(const Metrics&) = delete;

  // 2. 启动 HTTP Server
  // default_port: 如果环境变量没配 METRICS_PORT，就用这个默认值
  void Start(const std::string& default_port) {
    if (exposer_) {
      LOG(ERROR) << "⚠️ Metrics server already started!" << std::endl;
      return;
    }

    // 优先读取环境变量
    const char* env_port = std::getenv("METRICS_PORT");
    std::string port = (env_port ? env_port : default_port);
    std::string bind_addr = "0.0.0.0:" + port;

    try {
      // 启动 HTTP Server (在后台线程运行)
      exposer_ = std::make_unique<prometheus::Exposer>(bind_addr);
      exposer_->RegisterCollectable(registry_);
      LOG(INFO) << "✅ [Metrics] Server listening on " << bind_addr
                << std::endl;
    } catch (const std::exception& e) {
      LOG(ERROR) << "❌ [Metrics] Failed to start server: " << e.what()
                 << std::endl;
    }
  }

  // 3. 获取注册表 (用于创建 Counter/Gauge)
  std::shared_ptr<prometheus::Registry> GetRegistry() { return registry_; }

 private:
  // 私有构造
  Metrics() : registry_(std::make_shared<prometheus::Registry>()) {}

  std::shared_ptr<prometheus::Registry> registry_;
  std::unique_ptr<prometheus::Exposer> exposer_;
};

}  // namespace dts