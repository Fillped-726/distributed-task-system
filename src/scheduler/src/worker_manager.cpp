#include "worker_manager.h"
#include "logger.hpp"  // 引入我们优化过的日志库
#include <algorithm>

namespace dts {
namespace scheduler {

// 构造函数
WorkerManager::WorkerManager(std::chrono::seconds timeout)
    : worker_timeout_(timeout) {
  LOG_INFO << "WorkerManager initialized. Timeout: " << timeout.count() << "s";
}

// 析构函数
WorkerManager::~WorkerManager() { LOG_INFO << "WorkerManager destroyed."; }

// 1. 处理 Worker 注册
void WorkerManager::HandleRegister(const std::string& worker_id,
                                   const std::string& address) {
  std::lock_guard<std::mutex> lock(mtx_);

  auto it = workers_.find(worker_id);
  if (it != workers_.end()) {
    // Worker 重启/重连
    LOG_WARN << "Worker re-registered. ID: " << worker_id
             << ", OldAddr: " << it->second->address
             << ", NewAddr: " << address;

    it->second->address = address;
    it->second->last_heartbeat_time = std::chrono::system_clock::now();
    it->second->running_task_count = 0;  // 重置任务计数
  } else {
    // 新 Worker 加入
    LOG_INFO << "New Worker registered. ID: " << worker_id
             << ", Addr: " << address;

    auto new_worker = std::make_shared<WorkerInfo>(worker_id, address);
    workers_[worker_id] = new_worker;
  }
}

// 2. 处理 Worker 心跳
bool WorkerManager::HandleHeartbeat(
    const dts::internal::HeartbeatRequest* request) {
  std::lock_guard<std::mutex> lock(mtx_);

  auto it = workers_.find(request->worker_id());
  if (it == workers_.end()) {
    // 未知 Worker，可能是 Master 重启后丢失了元数据，或者 Worker 是非法的
    // 返回 false 让 Worker 触发重新注册流程
    LOG_WARN << "Heartbeat failed: Unknown worker " << request->worker_id();
    return false;
  }

  // 更新状态
  it->second->last_heartbeat_time = std::chrono::system_clock::now();
  it->second->running_task_count = request->running_task_count();

  // 心跳日志属于高频日志，建议使用 LOG_DEBUG 或者 Glog 的 LOG_EVERY_N
  // 这里为了演示，我们假设有 LOG_DEBUG 宏，或者仅在负载高时打印
  // if (request->running_task_count() > 10) { ... }

  return true;
}

// 3. 获取排序后的可用 Worker 列表
std::vector<WorkerInfo> WorkerManager::GetAvailableWorkersSorted() {
  std::vector<WorkerInfo> available_workers;

  // --- 临界区开始 ---
  {
    std::lock_guard<std::mutex> lock(mtx_);

    auto now = std::chrono::system_clock::now();

    // 预分配内存，减少 vector 扩容开销
    available_workers.reserve(workers_.size());

    for (const auto& pair : workers_) {
      // 防御性检查：即使巡检线程还没跑，这里也先过滤掉超时的
      if ((now - pair.second->last_heartbeat_time) > worker_timeout_) {
        continue;
      }

      // 拷贝对象到临时列表
      available_workers.push_back(*(pair.second));
    }
  }
  // --- 临界区结束 ---
  // 我们把它放在锁外面，避免阻塞 HandleHeartbeat 等其他操作

  // 按 任务数 升序排序 (Least Connections 策略)
  std::sort(available_workers.begin(), available_workers.end(),
            [](const WorkerInfo& a, const WorkerInfo& b) {
              return a.running_task_count < b.running_task_count;
            });

  return available_workers;
}

std::vector<std::string> WorkerManager::PruneDeadWorkers() {
  std::vector<std::string> dead_worker_ids;
  auto now = std::chrono::system_clock::now();

  std::lock_guard<std::mutex> lock(mtx_);

  // 优化后的遍历删除写法 (C++ Idiom)
  for (auto it = workers_.begin(); it != workers_.end();) {
    auto elapsed = now - it->second->last_heartbeat_time;

    if (elapsed > worker_timeout_) {
      LOG_ERROR
          << "Worker timed out. Removing: " << it->first << " (Last seen "
          << std::chrono::duration_cast<std::chrono::seconds>(elapsed).count()
          << "s ago)";

      dead_worker_ids.push_back(it->first);

      // erase 返回指向下一个元素的迭代器
      it = workers_.erase(it);
    } else {
      ++it;
    }
  }

  return dead_worker_ids;
}

}  // namespace scheduler
}  // namespace dts