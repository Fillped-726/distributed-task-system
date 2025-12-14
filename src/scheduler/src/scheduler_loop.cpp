#include "scheduler_loop.h"

// === Standard Library Includes ===
#include <algorithm>
#include <chrono>
#include <cstdlib>  // for std::getenv
#include <stdexcept>

// === Third-Party Includes ===
#include <sw/redis++/redis++.h>

// === Project Includes ===
#include "logger.hpp"
#include "redis/RedisKeys.hpp"

namespace dts::scheduler {

namespace keys = dts::common::redis::keys;
using dts::common::redis::RedisManager;
using TaskSerializer = dts::common::utils::TaskSerializer;

// =========================================================
// 匿名命名空间：内部辅助函数
// =========================================================
namespace {

void AckAndSkip(const std::string& stream_id, const std::string& reason = "") {
  // 1. 记录日志
  if (!reason.empty()) {
    LOG_WARN << "Skipping invalid task (" << reason
             << "). XACK performed. MsgID: " << stream_id;
  } else {
    LOG_WARN << "Skipping invalid task. XACK performed. MsgID: " << stream_id;
  }

  // 2. 调用 Redis XACK 移除消息，避免反复消费
  dts::common::redis::RedisManager::GetInstance().XAck(
      dts::common::redis::keys::stream::kTasks,      // Key
      dts::common::redis::keys::stream::kGroupMain,  // Group
      {stream_id}                                    // ID List
  );
}

}  // namespace

// =========================================================
// 构造与析构
// =========================================================

SchedulerLoop::SchedulerLoop(std::shared_ptr<TaskRepository> task_repo,
                             std::shared_ptr<WorkerManager> worker_manager)
    : task_repo_(task_repo),
      worker_manager_(worker_manager),
      stop_flag_(false) {
  if (!task_repo_ || !worker_manager_) {
    LOG_FATAL << "SchedulerLoop initialized with null dependencies";
    throw std::runtime_error("Invalid dependencies");
  }

  // 初始化线程池
  // 建议：线程数应略大于 Worker 总连接数，或者根据 IO 密集度设定
  dispatch_pool_ = std::make_unique<dts::common::ThreadPool>(20);

  // 生成唯一的消费者名称 (Hostname 优先)
  const char* hostname = std::getenv("HOSTNAME");
  if (hostname) {
    consumer_name_ = std::string(hostname);
  } else {
    // Fallback: scheduler-{timestamp}
    consumer_name_ =
        "scheduler-" +
        std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count());
  }

  LOG_INFO << "SchedulerLoop initialized. Consumer Name: " << consumer_name_
           << ", Pool Size: 20";
}

SchedulerLoop::~SchedulerLoop() { Stop(); }

// =========================================================
// 生命周期控制
// =========================================================

void SchedulerLoop::Start() {
  if (stop_flag_) return;  // 避免重复启动
  LOG_INFO << "SchedulerLoop starting...";

  // 确保 Consumer Group 存在 (MKSTREAM 选项)
  // 参数: Key, GroupName, StartID("$"表示只消费新消息), MkStream(true)
  RedisManager::GetInstance().XGroupCreate(keys::stream::kTasks,
                                           keys::stream::kGroupMain, "$", true);

  loop_thread_ = std::thread(&SchedulerLoop::RunLoop, this);
}

void SchedulerLoop::Stop() {
  if (stop_flag_) return;
  LOG_INFO << "SchedulerLoop stopping...";

  stop_flag_ = true;  // 设置标志位，让 RunLoop 退出

  if (loop_thread_.joinable()) {
    loop_thread_.join();
  }
  LOG_INFO << "SchedulerLoop stopped.";
}

// =========================================================
// 核心逻辑：主循环 (RunLoop)
// =========================================================

void SchedulerLoop::RunLoop() {
  const int BATCH_SIZE = 20;
  const int BLOCK_MS = 2000;

  auto last_rescue_time = std::chrono::steady_clock::now();

  while (!stop_flag_) {
    try {
      // 1. 背压控制 (Backpressure)
      if (inflight_tasks_ > MAX_INFLIGHT) {
        // 如果在途任务太多，说明 Worker 处理不过来，或者 RPC 堵住了。
        // 暂停拉取，给系统一点喘息时间。
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;
      }

      // 2. 救援逻辑 (DoRescue)
      // 定期检查是否有其他 Scheduler 挂掉留下的“僵尸任务”
      auto now = std::chrono::steady_clock::now();
      if (now - last_rescue_time > std::chrono::seconds(10)) {
        DoRescue();
        last_rescue_time = now;
      }

      // 3. 检查是否有可用 Worker
      auto workers = worker_manager_->GetAvailableWorkersSorted();
      if (workers.empty()) {
        LOG_WARN << "No workers registered, waiting...";
        std::this_thread::sleep_for(std::chrono::seconds(1));
        continue;
      }

      // 4. Pull: 阻塞拉取 Redis Stream
      auto entries_opt = RedisManager::GetInstance().XReadGroup(
          keys::stream::kGroupMain, consumer_name_, keys::stream::kTasks,
          BATCH_SIZE, BLOCK_MS);

      if (!entries_opt || entries_opt->empty()) {
        continue;
      }

      // 只增加实际拉取到的数量，而不是 BATCH_SIZE
      inflight_tasks_.fetch_add(entries_opt->size(), std::memory_order_relaxed);

      // 5. Match & Dispatch
      size_t worker_idx = 0;  // 用于简单的 Round-Robin 负载均衡

      for (const auto& entry : *entries_opt) {
        if (stop_flag_) break;

        // --- A. 解析 Stream 信封 ---
        auto ptr_opt = TaskSerializer::ParseStreamEntry(entry);
        if (!ptr_opt) {
          AckAndSkip(entry.first, "Invalid Stream Format");
          inflight_tasks_.fetch_sub(1);  // 解析失败，不算 Inflight
          continue;
        }

        // --- B. 捞取元数据 (Claim Check 模式) ---
        std::string meta_key = keys::dag::TaskMeta(ptr_opt->task_id);
        auto& redis = RedisManager::GetInstance().GetConnection();
        auto meta_binary = redis.get(meta_key);

        if (!meta_binary) {
          // 元数据丢失（可能已过期），跳过
          AckAndSkip(entry.first, "Meta Missing: " + ptr_opt->task_id);
          inflight_tasks_.fetch_sub(1);
          continue;
        }

        // --- C. 反序列化 Task ---
        auto task_opt = TaskSerializer::FromMetaBinary(*meta_binary);
        if (!task_opt) {
          AckAndSkip(entry.first, "Protobuf Parse Failed");
          inflight_tasks_.fetch_sub(1);
          continue;
        }

        dts::Task task = std::move(*task_opt);
        task.stream_id = entry.first;  // 必须回填，用于后续 ACK

        // --- D. 调度策略 (Worker Selection) ---
        // workers 列表已按负载排序 (Least Loaded First)
        // 使用取模运算将这批任务均匀撒给最闲的几个 Worker
        WorkerInfo& selected_worker = workers[worker_idx % workers.size()];
        worker_idx++;

        // 预定资源 (乐观锁概念，防止下一次 Sort 数据滞后)
        worker_manager_->PrebookTask(selected_worker.worker_id, 1);

        // --- E. 异步分发 ---
        // 放入线程池执行耗时的 RPC 操作
        dispatch_pool_->enqueue(
            [this, t = std::move(task), w = selected_worker]() {
              this->DoDispatch(t, w);
            });
      }

    } catch (const std::exception& e) {
      LOG_ERROR << "SchedulerLoop exception: " << e.what();
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

// =========================================================
// 故障恢复逻辑 (Rescue)
// =========================================================

void SchedulerLoop::DoRescue() {
  // 定义僵尸任务判定阈值：60秒未 ACK
  const long long MIN_IDLE_MS = 60000;
  const int BATCH_SIZE = 50;

  auto& redis = RedisManager::GetInstance();

  try {
    // 1. XPending: 查询未 ACK 的消息列表
    auto pending_opt = redis.XPending(
        keys::stream::kTasks, keys::stream::kGroupMain, BATCH_SIZE, "-", "+");

    if (!pending_opt || pending_opt->empty()) return;

    std::vector<std::string> ids_to_claim;

    // 2. 筛选: 找出空闲时间超标的任务
    for (const auto& item : *pending_opt) {
      if (item.idle_time_ms >= MIN_IDLE_MS) {
        ids_to_claim.push_back(item.id);
      }
    }

    if (ids_to_claim.empty()) return;

    // 3. XClaim: 抢占任务所有权
    // 成功后，Owner 变为当前 consumer_name_，且 idle time 重置
    auto claimed_msgs_opt = redis.XClaim(
        keys::stream::kTasks, keys::stream::kGroupMain, consumer_name_,
        MIN_IDLE_MS,  // 再次校验 idle 时间，防止并发抢占
        ids_to_claim);

    if (!claimed_msgs_opt || claimed_msgs_opt->empty()) return;

    LOG_WARN << "🚑 Rescued " << claimed_msgs_opt->size() << " stuck tasks!";

    // 增加 Inflight 计数 (因为接下来要 Process 了)
    inflight_tasks_.fetch_add(claimed_msgs_opt->size());

    // 4. 重生: 立即处理
    for (const auto& entry : *claimed_msgs_opt) {
      ProcessStreamEntry(entry);
    }
  } catch (const std::exception& e) {
    LOG_ERROR << "Exception in DoRescue: " << e.what();
  }
}

// 处理 Rescue 抢回来的单个任务
void SchedulerLoop::ProcessStreamEntry(const RedisManager::StreamEntry& entry) {
  // 1. 解析与校验
  auto ptr_opt = TaskSerializer::ParseStreamEntry(entry);
  if (!ptr_opt) {
    AckAndSkip(entry.first, "Rescue: Invalid Stream Format");
    inflight_tasks_.fetch_sub(1);  // 失败回退计数
    return;
  }

  std::string meta_key =
      dts::common::redis::keys::dag::TaskMeta(ptr_opt->task_id);
  auto& redis = dts::common::redis::RedisManager::GetInstance().GetConnection();
  auto meta_binary = redis.get(meta_key);

  if (!meta_binary) {
    AckAndSkip(entry.first, "Rescue: Meta Missing");
    inflight_tasks_.fetch_sub(1);
    return;
  }

  auto task_opt = TaskSerializer::FromMetaBinary(*meta_binary);
  if (!task_opt) {
    AckAndSkip(entry.first, "Rescue: Parse Failed");
    inflight_tasks_.fetch_sub(1);
    return;
  }

  dts::Task task = std::move(*task_opt);
  task.stream_id = entry.first;

  // 2. 选择 Worker (Rescue 比较紧急，直接选负载最低的)
  auto workers = worker_manager_->GetAvailableWorkersSorted();
  if (workers.empty()) {
    LOG_WARN << "Rescued task " << task.task_id << " no workers available.";
    inflight_tasks_.fetch_sub(1);
    return;
  }

  WorkerInfo& best_worker = workers[0];
  worker_manager_->PrebookTask(best_worker.worker_id, 1);

  // 3. 异步分发

  LOG_INFO << "Rescuing task: " << task.task_id << " -> "
           << best_worker.worker_id;

  dispatch_pool_->enqueue([this, t = std::move(task), w = best_worker]() {
    this->DoDispatch(t, w);
  });
}

// =========================================================
// RPC 分发逻辑
// =========================================================

void SchedulerLoop::DoDispatch(const dts::Task& task,
                               const WorkerInfo& worker) {
  // 1. 获取连接
  auto stub = GetWorkerStub(worker.address);
  if (!stub) {
    LOG_ERROR << "Failed to connect to worker " << worker.address;
    task_repo_->RevertTaskToPending(task.task_id);
    inflight_tasks_.fetch_sub(1);
    return;
  }

  // 2. 构造 RPC 请求
  dts::internal::RunTaskRequest request;
  dts::TaskToProto(task, request.mutable_task());
  dts::internal::RunTaskResponse response;

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));

  // 3. 执行 RPC
  auto rpc_start = std::chrono::steady_clock::now();
  grpc::Status status = stub->RunTask(&context, request, &response);
  auto rpc_end = std::chrono::steady_clock::now();
  auto rpc_duration = rpc_end - rpc_start;

  // 4. 性能监控
  if (rpc_duration > std::chrono::milliseconds(50)) {
    LOG_WARN << "Slow RPC to " << worker.worker_id << ": "
             << std::chrono::duration_cast<std::chrono::milliseconds>(
                    rpc_duration)
                    .count()
             << "ms.";
  }

  // 5. 结果处理
  // Case A: 网络或系统错误
  if (!status.ok()) {
    LOG_ERROR << "RPC failed sending task " << task.task_id << " to "
              << worker.address << ": " << status.error_message();
    task_repo_->RevertTaskToPending(task.task_id);
    inflight_tasks_.fetch_sub(1);
    return;
  }

  // Case B: Worker 业务拒绝
  if (response.header().has_error()) {
    LOG_WARN << "Worker " << worker.worker_id << " rejected task "
             << task.task_id << ": " << response.header().error().msg();
    task_repo_->RevertTaskToPending(task.task_id);
    inflight_tasks_.fetch_sub(1);
    return;
  }

  // Case C: 成功
  LOG_INFO << "Dispatch Success: " << task.task_id << " -> "
           << worker.worker_id;

  // 这里的 ACK 意味着"调度成功"，而不是"执行成功"。
  // 执行结果由 Worker 稍后异步上报。
  RedisManager::GetInstance().XAck(keys::stream::kTasks,
                                   keys::stream::kGroupMain, task.stream_id);

  inflight_tasks_.fetch_sub(1);  // 正常完成，计数减少
}

std::shared_ptr<dts::internal::WorkerService::Stub>
SchedulerLoop::GetWorkerStub(const std::string& address) {
  // 1. 读锁检查缓存
  {
    std::lock_guard<std::mutex> lock(stub_cache_mtx_);
    auto it = channel_cache_.find(address);
    if (it != channel_cache_.end()) {
      return dts::internal::WorkerService::NewStub(it->second);
    }
  }

  // 2. 创建 Channel (耗时操作，无锁进行)
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);

  auto new_channel = grpc::CreateCustomChannel(
      address, grpc::InsecureChannelCredentials(), args);

  // 3. 写锁更新缓存
  {
    std::lock_guard<std::mutex> lock(stub_cache_mtx_);
    // 双重检查 (Double-Checked Locking)
    auto it = channel_cache_.find(address);
    if (it != channel_cache_.end()) {
      return dts::internal::WorkerService::NewStub(it->second);
    }
    channel_cache_[address] = new_channel;
  }

  return dts::internal::WorkerService::NewStub(new_channel);
}

}  // namespace dts::scheduler