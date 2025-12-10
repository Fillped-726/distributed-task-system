#pragma once

#include <string>
#include <map>
#include <mutex>
#include <vector>
#include <chrono>
#include <memory>
#include <algorithm>

// 引入生成的 gRPC定义
#include "dts/internal/internal_service.pb.h"  // 只需要 message 定义

namespace dts {
namespace scheduler {

// 存储在内存中的 Worker 信息
struct WorkerInfo {
  std::string worker_id;
  std::string address;  // ip:port

  // 最近一次心跳时间
  std::chrono::system_clock::time_point last_heartbeat_time;

  // 负载信息
  int32_t running_task_count = 0;
  float cpu_usage = 0.0f;

  // 构造函数
  WorkerInfo(const std::string& id, const std::string& addr)
      : worker_id(id),
        address(addr),
        last_heartbeat_time(std::chrono::system_clock::now()) {}
};

class WorkerManager {
 public:
  // 构造函数
  explicit WorkerManager(
      std::chrono::seconds timeout = std::chrono::seconds(30));
  ~WorkerManager();

  // 禁止拷贝 (管理锁资源的类通常不可拷贝)
  WorkerManager(const WorkerManager&) = delete;
  WorkerManager& operator=(const WorkerManager&) = delete;

  // -----------------------------------------------------
  // 核心 gRPC 服务调用
  // -----------------------------------------------------

  // 处理注册
  void HandleRegister(const std::string& worker_id, const std::string& address);

  // 处理心跳 (返回 false 代表该 Worker 没注册过，需要重新注册)
  bool HandleHeartbeat(const dts::internal::HeartbeatRequest* request);

  void PrebookTask(const std::string& worker_id, int count = 1);

  // -----------------------------------------------------
  // 核心调度调用
  // -----------------------------------------------------

  // 获取可用 Worker 列表 (按负载从低到高排序 - 最小连接数策略)
  // 返回值是对象的拷贝，虽然有拷贝开销，但在多线程下最安全
  std::vector<WorkerInfo> GetAvailableWorkersSorted();

  // 清理僵死节点 (返回被清理的 worker_id 列表，用于触发任务重新调度)
  std::vector<std::string> PruneDeadWorkers();

  // 获取当前存活 Worker 数量 (监控用)
  size_t GetWorkerCount();

 private:
  std::mutex mtx_;  // 保护 workers_ map
  std::map<std::string, std::shared_ptr<WorkerInfo>> workers_;
  std::chrono::seconds worker_timeout_;
};

}  // namespace scheduler
}  // namespace dts