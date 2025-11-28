#include "scheduler_loop.h"
#include "logger.hpp"
#include <algorithm>
#include <chrono>

namespace dts {
namespace scheduler {

SchedulerLoop::SchedulerLoop(std::shared_ptr<TaskRepository> task_repo,
                             std::shared_ptr<WorkerManager> worker_manager)
    : task_repo_(task_repo),
      worker_manager_(worker_manager),
      stop_flag_(false) {
  if (!task_repo_ || !worker_manager_) {
    LOG_FATAL << "SchedulerLoop initialized with null dependencies";
  }
  dispatch_pool_ = std::make_unique<dts::common::ThreadPool>(8);

  LOG_INFO << "SchedulerLoop initialized with dispatch thread pool size: 8";
  LOG_INFO << "SchedulerLoop initialized.";
}

SchedulerLoop::~SchedulerLoop() { Stop(); }

void SchedulerLoop::Start() {
  if (stop_flag_) return;  // 避免重复启动
  LOG_INFO << "SchedulerLoop starting...";
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
  const int WORKER_CAPACITY = 10;

  while (!stop_flag_) {
    try {
      // 1. Pull: 拉取一批 PENDING 任务
      auto tasks = task_repo_->GetPendingTasks(BATCH_SIZE);
      if (tasks.empty()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        continue;
      }

      // 2. Filter: 获取可用 Worker
      // 注意：GetAvailableWorkersSorted 返回的是副本，所以是线程安全的
      auto workers = worker_manager_->GetAvailableWorkersSorted();
      if (workers.empty()) {
        LOG_WARN << "Pending tasks found (" << tasks.size()
                 << ") but no workers available!";
        std::this_thread::sleep_for(std::chrono::seconds(5));
        continue;
      }

      // 3. Match & Dispatch
      int dispatched_count = 0;

      for (const auto& task : tasks) {
        if (stop_flag_) break;

        // 贪心策略：总是取最闲的一个
        // 由于我们每次循环都会重新 sort，所以 workers[0] 总是当前最闲的
        WorkerInfo& best_worker = workers[0];

        if (best_worker.running_task_count >= WORKER_CAPACITY) {
          // 集群满了
          break;
        }

        std::string trace_id = "DISP-" + task.task_id();
        dts::SetRequestId(trace_id);

        dispatch_pool_->enqueue([this, task, best_worker]() {
          this->DoDispatch(task, best_worker);
        });

        dispatched_count++;

        // 更新内存状态并重排
        best_worker.running_task_count++;

        // 局部重排 (小优化：其实只需要把 workers[0] 冒泡下去，不必全排)
        // 但 std::sort 对部分有序数组也很快
        std::sort(workers.begin(), workers.end(),
                  [](const WorkerInfo& a, const WorkerInfo& b) {
                    return a.running_task_count < b.running_task_count;
                  });
      }

      // 4. Sleep Strategy
      if (dispatched_count == 0 && !tasks.empty()) {
        // 有任务但派发不出去（集群满载），快速重试
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      } else {
        // 正常间隔
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }

    } catch (const std::exception& e) {
      LOG_ERROR << "SchedulerLoop exception: " << e.what();
      std::this_thread::sleep_for(std::chrono::seconds(3));
    }
  }
}

void SchedulerLoop::DoDispatch(const dts::task::Task& task,
                               const WorkerInfo& worker) {
  // 1. 乐观锁抢占 (DB)
  bool updated =
      task_repo_->UpdateTaskToRunning(task.task_id(), worker.worker_id);
  if (!updated) {
    // 抢占失败，可能是被别的 Scheduler 抢了，或者任务刚被取消
    LOG_DEBUG << "Failed to lock task " << task.task_id() << " (CAS failed)";
    return;
  }

  // 2. 获取 RPC Stub
  auto stub = GetWorkerStub(worker.address);
  if (!stub) {
    LOG_ERROR << "Failed to connect to worker " << worker.address;
    task_repo_->RevertTaskToPending(task.task_id());
    return;
  }

  // 3. 构造请求
  dts::internal::RunTaskRequest request;
  *request.mutable_task() = task;
  dts::internal::RunTaskResponse response;

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::seconds(5));

  // 4. RPC 调用
  grpc::Status status = stub->RunTask(&context, request, &response);

  // 5. 结果处理
  if (!status.ok()) {
    LOG_ERROR << "RPC failed sending task " << task.task_id() << " to "
              << worker.address << ": " << status.error_message();

    // 回滚状态，让任务能被重新调度
    task_repo_->RevertTaskToPending(task.task_id());
    return;
  }

  if (response.header().has_error()) {
    LOG_WARN << "Worker " << worker.worker_id << " rejected task "
             << task.task_id() << ": " << response.header().error().msg();
    task_repo_->RevertTaskToPending(task.task_id());
    return;
  }

  LOG_INFO << "Successfully dispatched task " << task.task_id() << " to "
           << worker.worker_id;
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