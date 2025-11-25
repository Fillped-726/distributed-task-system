#include "scheduler_loop.h"
#include <algorithm> // for std::sort
#include <chrono>    // for std::this_thread::sleep_for

// -----------------------------------------------------
// 构造与析构
// -----------------------------------------------------
SchedulerLoop::SchedulerLoop(
    std::shared_ptr<TaskRepository> task_repo,
    std::shared_ptr<WorkerManager> worker_manager
) : task_repo_(task_repo), 
    worker_manager_(worker_manager), 
    stop_flag_(false) 
{
    if (task_repo_ == nullptr) {
        throw std::runtime_error("TaskRepository is null");
    }
    if (worker_manager_ == nullptr) {
        throw std::runtime_error("WorkerManager is null");
    }
}

SchedulerLoop::~SchedulerLoop() {
    Stop();
}

// -----------------------------------------------------
// 线程启停
// -----------------------------------------------------
void SchedulerLoop::Start() {
    std::cout << "[SchedulerLoop] Starting..." << std::endl;
    // 启动一个新线程, 执行 RunLoop()
    loop_thread_ = std::thread(&SchedulerLoop::RunLoop, this);
}

void SchedulerLoop::Stop() {
    std::cout << "[SchedulerLoop] Stopping..." << std::endl;
    stop_flag_ = true; // 设置停止标记

    // 等待线程安全退出
    if (loop_thread_.joinable()) {
        loop_thread_.join();
    }
}

// -----------------------------------------------------
// 核心：调度循环 (5 步法)
// -----------------------------------------------------
void SchedulerLoop::RunLoop() {
    while (!stop_flag_) {
        try {
            // 步骤 1: 拉取 (Pull)
            // (我们之前讨论过, 批处理大小为 20)
            std::vector<dts::task::Task> tasks = task_repo_->GetPendingTasks(20);

            if (tasks.empty()) {
                // 没有任务, 休息一下, 避免 CPU 空转
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }

            // 步骤 2: 筛选 (Filter)
            // (WorkerManager 返回已按 "running_task_count" 升序排序的列表)
            std::vector<WorkerInfo> workers = worker_manager_->GetAvailableWorkersSorted();
            
            if (workers.empty()) {
                // 有任务, 但没 Worker, 严重问题, 休息 5 秒
                std::cerr << "[SchedulerLoop] " << tasks.size() 
                          << " tasks pending, but no workers available!" << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(5));
                continue;
            }

            // 步骤 3 & 4: 匹配 (Match) 与派发 (Dispatch)
            // (这是我们讨论过的“贪心 + 内存排序”负载均衡策略)
            
            int dispatched_count = 0;
            const int MAX_TASKS_PER_WORKER_CAPACITY = 10; // 假设设置一个 Worker 容量

            for (const auto& task : tasks) {
                // 3. 匹配: 永远选择列表中的第一个 (最空闲的)
                WorkerInfo& best_worker = workers[0];

                // 负载均衡: 检查最空闲的 Worker 是否也满了
                if (best_worker.running_task_count >= MAX_TASKS_PER_WORKER_CAPACITY) {
                    // 所有 Worker 都满了, 停止本轮派发
                    break; 
                }

                // 4. 派发: 启动一个*新*线程来执行派发 (避免阻塞循环)
                // (注意：这里必须拷贝 task 和 best_worker, 否则有线程安全问题)
                std::thread(&SchedulerLoop::DoDispatch, this, task, best_worker).detach();
                
                dispatched_count++;

                // *** 核心负载均衡：更新内存状态 ***
                // 1. 在内存中立刻增加该 Worker 的计数
                best_worker.running_task_count++;
                // 2. 重新排序, 让它"沉"下去, 保证下一个任务分配给"次空闲"的 Worker
                std::sort(workers.begin(), workers.end(), 
                    [](const WorkerInfo& a, const WorkerInfo& b) {
                        return a.running_task_count < b.running_task_count;
                    });
            } // end for (task : tasks)

            // 步骤 5: 休眠 (Sleep)
            if (dispatched_count == 0 && !tasks.empty()) {
                // "任务很多, 但 Worker 全满" -> 快速重试
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            } else {
                // 正常休眠
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }

        } catch (const std::exception& e) {
            std::cerr << "[SchedulerLoop] EXCEPTION: " << e.what() << std::endl;
            // 发生异常, 防止 CPU 占满, 休息一下
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
    } // end while(!stop_flag_)
    
    std::cout << "[SchedulerLoop] Loop terminated." << std::endl;
}

// -----------------------------------------------------
// 核心：派发逻辑 (在自己的线程中运行)
// -----------------------------------------------------
void SchedulerLoop::DoDispatch(dts::task::Task task, WorkerInfo worker) {
    
    // 步骤 4a: 乐观锁 - 尝试将任务状态从 PENDING 更新为 RUNNING
    bool updated = task_repo_->UpdateTaskToRunning(task.task_id(), worker.worker_id);

    if (!updated) {
        // 任务被其他 scheduler 实例抢走了 (或已被取消)
        // (这是正常现象, 不用记录错误)
        return; 
    }

    // 步骤 4b: 通过 gRPC 调用 Worker
    std::shared_ptr<dts::internal::WorkerService::Stub> stub = GetWorkerStub(worker.address);
    if (stub == nullptr) {
        std::cerr << "[SchedulerLoop] Failed to create gRPC stub for " << worker.address << std::endl;
        // 回滚
        task_repo_->RevertTaskToPending(task.task_id());
        return;
    }

    dts::internal::RunTaskRequest request;
    *request.mutable_task() = task; // 拷贝 Task 对象
    dts::internal::RunTaskResponse response;
    grpc::ClientContext context;
    // (可以设置一个超时时间)
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

    grpc::Status status = stub->RunTask(&context, request, &response);

    // 步骤 4c: 处理 gRPC 调用失败 (容错)
    if (!status.ok()) {
        // *** 核心亮点：容错与回滚 ***
        // Worker 刚巧挂了, gRPC 连接失败
        std::cerr << "[SchedulerLoop] Failed to dispatch task " << task.task_id() 
                  << " to worker " << worker.worker_id << ": " 
                  << status.error_message() << std::endl;
        
        // 我们必须把任务状态“回滚” (Rollback)
        task_repo_->RevertTaskToPending(task.task_id());
        
        // (可选: 立即将 Worker 标记为死亡, 但我们的巡检线程最终会处理的)
        return;
    }

    // (可选) 检查 Worker 是否在 *业务上* 拒绝了任务
    if (response.header().has_error()) {
        std::cerr << "[SchedulerLoop] Worker " << worker.worker_id 
                  << " REJECTED task " << task.task_id() << ": " 
                  << response.header().error().msg() << std::endl;
        
        // Worker 拒绝了, 同样回滚
        task_repo_->RevertTaskToPending(task.task_id());
        return;
    }
    
    // 派发成功！
    // 球现在踢给了 worker, 我们等待它稍后回调 UpdateTaskStatus
    std::cout << "[SchedulerLoop] Dispatched task " << task.task_id() 
              << " to worker " << worker.worker_id << std::endl;
}


// -----------------------------------------------------
// 辅助：gRPC Stub 缓存
// -----------------------------------------------------
std::shared_ptr<dts::internal::WorkerService::Stub> SchedulerLoop::GetWorkerStub(
    const std::string& address
) {
    std::shared_ptr<grpc::Channel> channel;

    // 1. (上锁) 检查缓存
    {
        std::lock_guard<std::mutex> lock(stub_cache_mtx_);
        auto it = channel_cache_.find(address);
        if (it != channel_cache_.end()) {
            channel = it->second;
        }
    } // (解锁)

    // 2. 如果没找到, 创建新的 Channel
    if (channel == nullptr) {
        channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        
        // 3. (上锁) 存入缓存
        {
            std::lock_guard<std::mutex> lock(stub_cache_mtx_);
            channel_cache_[address] = channel;
        }
    }

    // 4. Stubs 是轻量级的, 可以每次都创建
    return dts::internal::WorkerService::NewStub(channel);
}