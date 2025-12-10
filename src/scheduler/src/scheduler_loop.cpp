#include "scheduler_loop.h"
#include "logger.hpp"
#include "redis/RedisKeys.hpp"
#include "redis/RedisKeys.hpp"
#include <algorithm>
#include <chrono>
#include <sw/redis++/redis++.h>

namespace dts {
namespace scheduler {

namespace keys = dts::common::redis::keys;
using dts::common::redis::RedisManager;
using TaskSerializer = dts::common::utils::TaskSerializer;

SchedulerLoop::SchedulerLoop(std::shared_ptr<TaskRepository> task_repo,
                             std::shared_ptr<WorkerManager> worker_manager)
    : task_repo_(task_repo),
      worker_manager_(worker_manager),
      stop_flag_(false) {
  if (!task_repo_ || !worker_manager_) {
    LOG_FATAL << "SchedulerLoop initialized with null dependencies";
  }
  dispatch_pool_ = std::make_unique<dts::common::ThreadPool>(20);

  LOG_INFO << "SchedulerLoop initialized with dispatch thread pool size: 112";
  LOG_INFO << "SchedulerLoop initialized.";

  // 生成唯一的消费者名称
  // 优先取环境变量 HOSTNAME (Docker), 否则随机生成
  const char* hostname = std::getenv("HOSTNAME");
  if (hostname) {
    consumer_name_ = std::string(hostname);
  } else {
    // 简单 fallback
    consumer_name_ =
        "scheduler-" +
        std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count());
  }

  LOG_INFO << "SchedulerLoop initialized. Consumer Name: " << consumer_name_;
}

SchedulerLoop::~SchedulerLoop() { Stop(); }

void SchedulerLoop::Start() {
  if (stop_flag_) return;  // 避免重复启动
  LOG_INFO << "SchedulerLoop starting...";

  RedisManager::GetInstance().XGroupCreate(keys::stream::kTasks,
                                           keys::stream::kGroupMain, "$", true);

  loop_thread_ = std::thread(&SchedulerLoop::RunLoop, this);
}

void SchedulerLoop::Stop() {
  if (stop_flag_) return;
  LOG_INFO << "SchedulerLoop stopping...";
  stop_flag_ = true;
  if (loop_thread_.joinable()) {
    loop_thread_.join();
  }
  LOG_INFO << "SchedulerLoop stopped.";
}

void AckAndSkip(const std::string& stream_id, const std::string& reason = "") {
  // 1. 记录日志，说明丢弃原因
  if (!reason.empty()) {
    LOG_WARN << "Skipping invalid task (" << reason
             << "). XACK performed. MsgID: " << stream_id;
  } else {
    LOG_WARN << "Skipping invalid task. XACK performed. MsgID: " << stream_id;
  }

  // 2. 调用 Redis XACK
  // 注意：XACK 需要传入 Stream Key 和 Group Name
  // 这里使用你项目中的常量定义
  dts::common::redis::RedisManager::GetInstance().XAck(
      dts::common::redis::keys::stream::kTasks,      // Key: dts:stream:tasks
      dts::common::redis::keys::stream::kGroupMain,  // Group: dts:group:main
      {stream_id}                                    // ID 列表
  );
}

void SchedulerLoop::RunLoop() {
  const int BATCH_SIZE = 20;
  const int BLOCK_MS = 2000;

  auto last_rescue_time = std::chrono::steady_clock::now();

  while (!stop_flag_) {
    try {
      // 0. 救援逻辑 (保持不变)
      auto now = std::chrono::steady_clock::now();
      if (now - last_rescue_time > std::chrono::seconds(10)) {
        DoRescue();
        last_rescue_time = now;
      }

      // 1. 获取 Worker (快照)
      auto workers = worker_manager_->GetAvailableWorkersSorted();
      if (workers.empty()) {
        // 优化：没 Worker 时睡久一点，避免疯狂刷日志
        LOG_WARN << "No workers registered, waiting...";
        std::this_thread::sleep_for(std::chrono::seconds(1));
        continue;
      }

      // 2. Pull: 阻塞拉取 (保持不变)
      auto entries_opt = RedisManager::GetInstance().XReadGroup(
          keys::stream::kGroupMain, consumer_name_, keys::stream::kTasks,
          BATCH_SIZE, BLOCK_MS);

      if (!entries_opt || entries_opt->empty()) continue;

      // 3. Match & Dispatch
      // 【优化点】：用于批次内的负载均衡索引
      size_t worker_idx = 0;

      for (const auto& entry : *entries_opt) {
        if (stop_flag_) break;

        // =========================================================
        // A. 第一步：解析 Stream 信封 (获取 ID)
        // =========================================================
        auto ptr_opt = TaskSerializer::ParseStreamEntry(entry);
        if (!ptr_opt) {
          LOG_ERROR << "Invalid Stream Entry. ACK & Skip. ID: " << entry.first;
          AckAndSkip(entry.first);
          continue;
        }

        // =========================================================
        // B. 第二步：去 KV 捞取元数据 (Claim Check)
        // =========================================================
        std::string meta_key = keys::dag::TaskMeta(ptr_opt->task_id);
        auto& redis = RedisManager::GetInstance().GetConnection();
        auto meta_binary = redis.get(meta_key);

        if (!meta_binary) {
          LOG_WARN << "Meta missing for Task: " << ptr_opt->task_id
                   << " (Expired?). ACK & Skip.";
          AckAndSkip(entry.first);
          continue;
        }

        // =========================================================
        // C. 第三步：反序列化 Task
        // =========================================================
        auto task_opt = TaskSerializer::FromMetaBinary(*meta_binary);
        if (!task_opt) {
          LOG_ERROR << "Meta corrupted for Task: " << ptr_opt->task_id;
          AckAndSkip(entry.first);
          continue;
        }

        dts::Task task = std::move(*task_opt);
        // 把 Stream ID 塞进去，方便后续 ACK
        task.stream_id = entry.first;

        // =========================================================
        // D. 调度策略：带负载感知的 Round-Robin
        // =========================================================
        // 解释：workers 已经是按负载排序的(从闲到忙)。
        // 我们在这一批任务中，依次发给 workers[0], workers[1]...
        // 这样就把 batch 里的任务均匀撒给了最闲的一批机器。

        // 如果机器不够分(任务多于机器)，取模回到 workers[0]
        WorkerInfo& selected_worker = workers[worker_idx % workers.size()];
        worker_idx++;

        // 预定资源 (内存中计数，防止下一次 Sort 还没更新)
        worker_manager_->PrebookTask(selected_worker.worker_id, 1);

        // 设置 Trace
        std::string trace_id = "DISP-" + task.task_id;
        dts::SetRequestId(trace_id);

        // 异步分发
        // 注意：capture 里的 task 和 worker 建议 move 或 copy
        // (WorkerInfo 一般很小，Copy 没问题)
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

void SchedulerLoop::DoRescue() {
  // 配置：如果消息超过 60秒 没动静，视为前任死掉了
  const long long MIN_IDLE_MS = 60000;
  const int BATCH_SIZE = 50;  // 每次检查 50 条 Pending 消息

  auto& redis = RedisManager::GetInstance();

  try {
    // [Step 1] 侦查: 查询 PEL (Pending Entries List)
    // "-" 到 "+" 表示查询所有 ID 范围
    auto pending_opt = redis.XPending(
        keys::stream::kTasks, keys::stream::kGroupMain, BATCH_SIZE, "-", "+");

    if (!pending_opt) {
      LOG_WARN
          << "XPending returned nullopt (Redis error or connection failed)";
      return;
    }

    // 3. 【解包】获取内部的 vector 引用
    // 使用 *pending_opt 解引用，或者 pending_opt->
    const auto& pending_list = *pending_opt;

    if (pending_list.empty()) return;

    std::vector<std::string> ids_to_claim;

    // [Step 2] 筛选: 找出“僵尸”任务
    for (const auto& item : pending_list) {
      // 条件 A: 闲置时间超过阈值
      // 条件 B: (可选) 即使是自己的任务，如果超时太久也抢回来重试
      if (item.idle_time_ms >= MIN_IDLE_MS) {
        // LOG_DEBUG << "Found stuck task: " << item.id << ", idle: " <<
        // item.idle_time_ms;
        ids_to_claim.push_back(item.id);
      }
    }

    if (ids_to_claim.empty()) return;

    // [Step 3] 抢占: 原子性转移所有权
    // XClaim 成功后会返回这些消息的最新内容
    auto claimed_msgs_opt =
        redis.XClaim(keys::stream::kTasks, keys::stream::kGroupMain,
                     consumer_name_,  // 把 Owner 改为我自己
                     MIN_IDLE_MS,     // 再次确认空闲时间 (防止并发抢占)
                     ids_to_claim);

    if (!claimed_msgs_opt || claimed_msgs_opt->empty()) return;

    LOG_WARN << "🚑 Rescued " << claimed_msgs_opt->size() << " stuck tasks!";

    // [Step 4] 重生: 立即处理抢回来的任务
    for (const auto& entry : *claimed_msgs_opt) {
      ProcessStreamEntry(entry);  // 复用逻辑
    }
  } catch (const std::exception& e) {
    LOG_ERROR << "Exception in DoRescue: " << e.what();
  }
}

void SchedulerLoop::ProcessStreamEntry(const RedisManager::StreamEntry& entry) {
  // =========================================================
  // 1. 解析 ID -> 查 Meta -> 反序列化
  // =========================================================

  // A. 解析 Stream 信封 (获取 ID)
  auto ptr_opt = TaskSerializer::ParseStreamEntry(entry);
  if (!ptr_opt) {
    // 格式错误，直接丢弃
    AckAndSkip(entry.first, "Rescue: Invalid Stream Format");
    return;
  }

  // B. 去 KV 捞取元数据
  std::string meta_key =
      dts::common::redis::keys::dag::TaskMeta(ptr_opt->task_id);

  // 注意：这里需要获取连接来调用 get
  auto& redis = dts::common::redis::RedisManager::GetInstance().GetConnection();
  auto meta_binary = redis.get(meta_key);

  if (!meta_binary) {
    AckAndSkip(entry.first,
               "Rescue: Meta Missing (Key: " + ptr_opt->task_id + ")");
    return;
  }

  // C. 反序列化 Payload
  auto task_opt = TaskSerializer::FromMetaBinary(*meta_binary);
  if (!task_opt) {
    AckAndSkip(entry.first, "Rescue: Protobuf Parse Failed");
    return;
  }

  dts::Task task = std::move(*task_opt);
  task.stream_id = entry.first;  // 记得回填 Stream ID

  // =========================================================
  // 2. 获取可用 Worker
  // =========================================================
  auto workers = worker_manager_->GetAvailableWorkersSorted();
  if (workers.empty()) {
    LOG_WARN << "Rescued task " << task.task_id
             << " but no workers available. Leaving it in Pending List.";
    // 下一次 Rescue 周期检查时，如果它的 idle time 超过阈值，又会被捞起来重试。
    return;
  }

  WorkerInfo& best_worker = workers[0];

  worker_manager_->PrebookTask(best_worker.worker_id, 1);

  // =========================================================
  // 4. 异步分发
  // =========================================================
  std::string trace_id = "RESCUE-" + task.task_id;
  dts::SetRequestId(trace_id);

  LOG_INFO << "Rescuing task: " << task.task_id
           << " -> Worker: " << best_worker.worker_id;

  dispatch_pool_->enqueue([this, t = std::move(task), w = best_worker]() {
    this->DoDispatch(t, w);
  });
}

void SchedulerLoop::DoDispatch(const dts::Task& task,
                               const WorkerInfo& worker) {
  // 2. 获取 RPC Stub
  auto stub = GetWorkerStub(worker.address);
  if (!stub) {
    LOG_ERROR << "Failed to connect to worker " << worker.address;
    task_repo_->RevertTaskToPending(task.task_id);
    return;
  }

  // 3. 构造请求
  dts::internal::RunTaskRequest request;
  dts::TaskToProto(task, request.mutable_task());
  dts::internal::RunTaskResponse response;

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));

  // 4. RPC 调用
  auto rpc_start = std::chrono::steady_clock::now();
  grpc::Status status = stub->RunTask(&context, request, &response);
  auto rpc_end = std::chrono::steady_clock::now();
  auto rpc_duration = rpc_end - rpc_start;

  // 5. 结果处理
  if (rpc_duration >
      std::chrono::milliseconds(50)) {  // 如果 RPC 超过 50ms，立即警告！
    LOG_WARN << "RPC to worker " << worker.worker_id << " took too long: "
             << std::chrono::duration_cast<std::chrono::milliseconds>(
                    rpc_duration)
                    .count()
             << "ms. Network/Worker overloaded!";
  }
  if (!status.ok()) {
    LOG_ERROR << "RPC failed sending task " << task.task_id << " to "
              << worker.address << ": " << status.error_message();

    // 回滚状态，让任务能被重新调度
    task_repo_->RevertTaskToPending(task.task_id);
    return;
  }

  if (response.header().has_error()) {
    LOG_WARN << "Worker " << worker.worker_id << " rejected task "
             << task.task_id << ": " << response.header().error().msg();
    task_repo_->RevertTaskToPending(task.task_id);
    return;
  }

  LOG_INFO << "Successfully dispatched task " << task.task_id << " to "
           << worker.worker_id;
  RedisManager::GetInstance().XAck(keys::stream::kTasks,
                                   keys::stream::kGroupMain, task.stream_id);
}

std::shared_ptr<dts::internal::WorkerService::Stub>
SchedulerLoop::GetWorkerStub(const std::string& address) {
  // 1. 第一次检查（读锁或无锁，视 Map 实现而定，这里假设用 mutex 保护）
  {
    std::lock_guard<std::mutex> lock(stub_cache_mtx_);
    auto it = channel_cache_.find(address);
    if (it != channel_cache_.end()) {
      return dts::internal::WorkerService::NewStub(it->second);
    }
  }

  // 2. 未找到，准备创建 (不要持有锁！)
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
  // 这个耗时操作现在在锁外进行
  auto new_channel = grpc::CreateCustomChannel(
      address, grpc::InsecureChannelCredentials(), args);

  // 3. 再次加锁插入 (防止并发重复创建)
  {
    std::lock_guard<std::mutex> lock(stub_cache_mtx_);
    // 双重检查，也许别的线程刚才已经插进去了
    auto it = channel_cache_.find(address);
    if (it != channel_cache_.end()) {
      return dts::internal::WorkerService::NewStub(it->second);
    }
    channel_cache_[address] = new_channel;
  }

  return dts::internal::WorkerService::NewStub(new_channel);
}

}  // namespace scheduler
}  // namespace dts