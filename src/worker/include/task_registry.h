#pragma once

#include <string>
#include <functional>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <shared_mutex> // C++17, 如果是 C++14 请改用 std::mutex

namespace dts {
namespace worker {

// 定义通用的任务函数签名
// Input: 任务参数 (通常是 JSON string)
// Output: 任务结果 (通常是 JSON string)
// 异常: 如果执行失败，业务函数应抛出 std::exception
using TaskHandler = std::function<std::string(const std::string&)>;

class TaskRegistry {
public:
    // 获取单例实例
    static TaskRegistry& GetInstance();

    // 禁止拷贝和赋值
    TaskRegistry(const TaskRegistry&) = delete;
    TaskRegistry& operator=(const TaskRegistry&) = delete;

    // 注册任务
    // name: 任务名称 (与 Scheduler 下发的 task_name 一致)
    // handler: 具体的函数实现
    void Register(const std::string& name, TaskHandler handler);

    // 获取任务
    // 返回具体的 handler，如果未找到返回空 function 对象
    TaskHandler Get(const std::string& name);

private:
    TaskRegistry() = default;
    ~TaskRegistry() = default;

    std::unordered_map<std::string, TaskHandler> registry_;
    
    // 使用读写锁：Register 是写操作，Get 是读操作
    // 如果你的编译器不支持 C++17，请改用 std::mutex
    mutable std::shared_mutex mutex_; 
};

class TaskRegisterHelper {
public:
    TaskRegisterHelper(const std::string& name, TaskHandler handler) {
        TaskRegistry::GetInstance().Register(name, handler);
    }
};

} // namespace worker
} // namespace dts

#define DTS_REGISTER_TASK(name, func) \
    static dts::worker::TaskRegisterHelper _dts_register_##func(name, func)
    