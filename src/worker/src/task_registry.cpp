#include "task_registry.h"
#include "logger.hpp" 

namespace dts {
namespace worker {

TaskRegistry& TaskRegistry::GetInstance() {
    static TaskRegistry instance;
    return instance;
}

void TaskRegistry::Register(const std::string& name, TaskHandler handler) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    if (registry_.find(name) != registry_.end()) {
        // 使用 LOG_WARN 替代 std::cerr
        // 这里的 req_id 可能是空的，或者是主线程的 ID，视调用上下文而定
        LOG_WARN << "Overwriting existing task registration: " << name;
    } else {
        // 使用 LOG_INFO 替代 std::cout
        LOG_INFO << "Task registered successfully: " << name;
    }
    
    registry_[name] = std::move(handler);
}

TaskHandler TaskRegistry::Get(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = registry_.find(name);
    if (it != registry_.end()) {
        return it->second;
    }
    
    // 找不到任务通常是一个值得注意的错误，尤其是在 RunTask 阶段
    // 但 Get 只是查询，我们可以在调用层记录 Error，这里暂时不打日志或者打 Debug 日志
    return nullptr;
}

} // namespace worker
} // namespace dts