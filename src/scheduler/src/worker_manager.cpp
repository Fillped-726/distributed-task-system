#include "worker_manager.h" // 包含我们刚定义的头文件
#include <algorithm> // 用于 std::sort

// 构造函数：初始化配置
WorkerManager::WorkerManager() 
    : worker_timeout_(30)   // 超过 30 秒没心跳就认为死亡
{
}

// 析构函数：确保线程停止
WorkerManager::~WorkerManager() {
}

// 1. 处理 Worker 注册
void WorkerManager::HandleRegister(const std::string& worker_id, const std::string& address) {
    // (C++) C++17 的写法
    std::lock_guard<std::mutex> lock(mtx_); // *** 关键：上锁 ***

    auto it = workers_.find(worker_id);
    if (it != workers_.end()) {
        // Worker 已存在, 更新信息
        it->second->address = address;
        it->second->last_heartbeat_time = std::chrono::system_clock::now();
        it->second->running_task_count = 0; // 重新注册，重置任务数
    } else {
        // 新 Worker
        auto new_worker = std::make_shared<WorkerInfo>();
        new_worker->worker_id = worker_id;
        new_worker->address = address;
        new_worker->last_heartbeat_time = std::chrono::system_clock::now();
        workers_[worker_id] = new_worker;
    }
    
    std::cout << "Worker registered: " << worker_id << " at " << address << std::endl;
} // *** 关键：lock_guard 在这里自动释放锁 ***


// 2. 处理 Worker 心跳
bool WorkerManager::HandleHeartbeat(const dts::internal::HeartbeatRequest* request) {
    std::lock_guard<std::mutex> lock(mtx_); 

    auto it = workers_.find(request->worker_id());
    if (it == workers_.end()) {
        // Worker 不存在，心跳失败
        return false; 
    }

    // Worker 存在，更新心跳时间和任务数
    it->second->last_heartbeat_time = std::chrono::system_clock::now();
    it->second->running_task_count = request->running_task_count();
    
    // std::cout << "Heartbeat from: " << request->worker_id() << std::endl;
    return true;
}


// 3. 获取排序后的可用 Worker 列表
std::vector<WorkerInfo> WorkerManager::GetAvailableWorkersSorted() {
    std::vector<WorkerInfo> available_workers;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        
        
        // (巡检线程会移除超时的 Worker, 所以这里的 Worker 理论上都是“健康”的)
        // (为了更保险，我们也可以在这里做一次检查，但巡检是更好的做法)

        for (const auto& pair : workers_) {
            // TODO: (步骤 2) 在 PatrolLoop 实现之前，我们先在这里手动过滤超时的
            auto now = std::chrono::system_clock::now();
            if ((now - pair.second->last_heartbeat_time) > worker_timeout_) {
                // 这个 Worker 已经超时了, 但我们先不移除
                continue; 
            }
            
            // 这是一个健康的 Worker，我们把它加到列表里
            // ！！注意：这里是创建了一个拷贝！！
            // 这是故意的，我们不希望调度循环持有对 map 内部元素的引用
            available_workers.push_back(*(pair.second));
        }

    }
    
    // 核心：按运行中任务数升序排序 (最空闲的在前)
    std::sort(available_workers.begin(), available_workers.end(), 
        [](const WorkerInfo& a, const WorkerInfo& b) {
            return a.running_task_count < b.running_task_count;
        });

    return available_workers;
}

std::vector<std::string> WorkerManager::PruneDeadWorkers() {
    
    std::vector<std::string> dead_worker_ids;
    auto now = std::chrono::system_clock::now();

    // *** 关键：上锁 ***
    // 我们要修改 map, 所以需要一个完整的锁
    std::lock_guard<std::mutex> lock(mtx_); 

    // 步骤 1：找出所有超时的 Worker
    // (C++ 不允许在遍历 map 时删除元素, 
    //  所以我们先用一个迭代器循环来收集要删除的 ID)
    for (auto it = workers_.begin(); it != workers_.end(); ++it) {
        auto worker_info = it->second;
        auto elapsed = now - worker_info->last_heartbeat_time;

        if (elapsed > worker_timeout_) {
            // 这个 Worker 已经超时了
            dead_worker_ids.push_back(worker_info->worker_id);
        }
    }

    // 步骤 2：真正执行删除
    for (const auto& worker_id : dead_worker_ids) {
        workers_.erase(worker_id);
        std::cout << "[WorkerManager] Worker timed out, removed: " 
                  << worker_id << std::endl;
    }

    // 步骤 3：返回被删除的列表
    // 外部调用者将用这个列表去数据库中回滚任务
    return dead_worker_ids;
    
}