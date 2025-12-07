#include "scheduler_loop.h"
#include "logger.hpp"
#include "redis/RedisKeys.hpp"
#include <algorithm>
#include <chrono>

namespace dts {
namespace scheduler {

namespace keys = dts::common::redis::keys;
using TaskSerializer = dts::common::utils::TaskSerializer;

SchedulerLoop::SchedulerLoop(std::shared_ptr<TaskRepository> task_repo,
                             std::shared_ptr<WorkerManager> worker_manager)
    : task_repo_(task_repo),
      worker_manager_(worker_manager),
      stop_flag_(false) {
  if (!task_repo_ || !worker_manager_) {
    LOG_FATAL << "SchedulerLoop initialized with null dependencies";
  }
  dispatch_pool_ = std::make_unique<dts::common::ThreadPool>(112);

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

void SchedulerLoop::RunLoop() {
  const int BATCH_SIZE = 20;
  const int BLOCK_MS = 2000;  // 阻塞 2秒，方便响应 stop_flag

  // 救援计时器 (每 10 秒执行一次救援)
  auto last_rescue_time = std::chrono::steady_clock::now();

  while (!stop_flag_) {
    try {
      // 0. 定期执行救援逻辑
      auto now = std::chrono::steady_clock::now();
      if (now - last_rescue_time > std::chrono::seconds(10)) {
        DoRescue();
        last_rescue_time = now;
      }

      // 1. Filter: 获取可用 Worker
      auto workers = worker_manager_->GetAvailableWorkersSorted();
      if (workers.empty()) {
        LOG_WARN << "no workers register!";
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        continue;
      }

      // 2. Pull: 阻塞拉取 Redis Stream
      auto entries_opt = RedisManager::GetInstance().XReadGroup(
          keys::stream::kGroupMain, consumer_name_, keys::stream::kTasks,
          BATCH_SIZE, BLOCK_MS);

      if (!entries_opt || entries_opt->empty()) {
        // 超时没任务，继续循环
        continue;
      }

      // 3. Match & Dispatch
      for (const auto& entry : *entries_opt) {
        if (stop_flag_) break;

        // A. 反序列化
        auto task_opt = TaskSerializer::FromStreamEntry(entry);
        if (!task_opt) {
          LOG_ERROR << "Bad task data in Redis. ACK and skip. MsgID: "
                    << entry.first;
          RedisManager::GetInstance().XAck(
              keys::stream::kTasks, keys::stream::kGroupMain, entry.first);
          continue;
        }

        dts::Task task = *task_opt;

        // 贪心策略：总是取最闲的一个
        // 由于我们每次循环都会重新 sort，所以 workers[0] 总是当前最闲的
        WorkerInfo& best_worker = workers[0];

        std::string trace_id = "DISP-" + task.task_id;
        dts::SetRequestId(trace_id);

        dispatch_pool_->enqueue([this, task, best_worker]() {
          this->DoDispatch(task, best_worker);
        });

        // 更新内存状态并重排
        best_worker.running_task_count++;

        // 局部重排 (小优化：其实只需要把 workers[0] 冒泡下去，不必全排)
        // 但 std::sort 对部分有序数组也很快
        std::sort(workers.begin(), workers.end(),
                  [](const WorkerInfo& a, const WorkerInfo& b) {
                    return a.running_task_count < b.running_task_count;
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

// 处理单条 Stream 消息的通用逻辑
void SchedulerLoop::ProcessStreamEntry(const RedisManager::StreamEntry& entry) {
  // 1. 反序列化
  auto task_opt = TaskSerializer::FromStreamEntry(entry);
  if (!task_opt) {
    LOG_ERROR << "Bad task data in Redis (Parse Failed). MsgID: "
              << entry.first;
    // 坏数据必须 ACK 掉，否则会永远卡在 Pending List 里被反复 Rescue
    RedisManager::GetInstance().XAck(keys::stream::kTasks,
                                     keys::stream::kGroupMain, entry.first);
    return;
  }

  dts::Task task = *task_opt;

  // 2. 获取可用 Worker
  auto workers = worker_manager_->GetAvailableWorkersSorted();
  if (workers.empty()) {
    LOG_WARN << "Rescued task " << task.task_id
             << " but no workers available. It will pend again.";
    // 这里不 ACK，让它留在 PEL 里，下次如果还超时再被 Rescue 一次
    // 或者你可以选择把它放回 Redis (XADD) 并 ACK 旧的，但这会打乱顺序
    return;
  }

  WorkerInfo& best_worker = workers[0];

  // 3. 异步分发
  // 增加计数，防止瞬间过载
  best_worker.running_task_count++;

  std::string trace_id = "RESCUE-" + task.task_id;
  dts::SetRequestId(trace_id);

  dispatch_pool_->enqueue(
      [this, task, best_worker]() { this->DoDispatch(task, best_worker); });
}

void SchedulerLoop::DoDispatch(const dts::Task& task,
                               const WorkerInfo& worker) {
  // 1. 乐观锁抢占 (DB)
  bool updated =
      task_repo_->UpdateTaskToRunning(task.task_id, worker.worker_id);
  if (!updated) {
    LOG_DEBUG << "Failed to update DB status for task " << task.task_id
              << ". Maybe cancelled.";
    // 既然 DB 不需要跑了，那 Redis 里的这任务也该结了
    RedisManager::GetInstance().XAck(keys::stream::kTasks,
                                     keys::stream::kGroupMain, task.stream_id);
    return;
  }

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
  std::lock_guard<std::mutex> lock(stub_cache_mtx_);

  auto it = channel_cache_.find(address);
  if (it == channel_cache_.end()) {
    // 创建新 Channel
    // 可以在这里加一些 Channel 参数，比如 KeepAlive
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
    auto channel = grpc::CreateCustomChannel(
        address, grpc::InsecureChannelCredentials(), args);

    channel_cache_[address] = channel;
    return dts::internal::WorkerService::NewStub(channel);
  }

  return dts::internal::WorkerService::NewStub(it->second);
}

}  // namespace scheduler
}  // namespace dts