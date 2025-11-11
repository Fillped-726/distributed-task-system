#include "task_executor.hpp"
#include <iostream>
#include <chrono>
#include <stdexcept>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include "resource_tracker.hpp"
#include "utils.hpp"  
#include "thread_pool.hpp"  

namespace dts {

TaskExecutor::TaskExecutor(boost::asio::io_context& io_context)
    : io_context_(io_context), thread_pool_(std::thread::hardware_concurrency()) {  // 将线程池作为成员初始化

    // 示例：注册内置函数
    register_function("fib", [](const nlohmann::json& params, std::shared_ptr<Task> t) -> nlohmann::json {
        int n = params.value("n", 0);
        if (n < 0) throw std::runtime_error("Negative input for fib");
        if (n <= 1) return {{"result", n}};
        int a = 0, b = 1;
        for (int i = 2; i <= n; ++i) {
            if (t->cancelled->load()) return nlohmann::json{{"result", "cancelled"}};
            int c = a + b;
            a = b;
            b = c;
        }
        return {{"result", b}};
    });
}

void TaskExecutor::register_function(const std::string& func_name, TaskFunction func) {
    functions_[func_name] = std::move(func);
}

void TaskExecutor::execute_task(std::shared_ptr<Task> task) {
    // 使用线程池异步提交任务（结合io_context，如果需要Asio操作可在run_task内post）
    thread_pool_.enqueue([this, task]() {
        run_task(task);
    });
}

void TaskExecutor::run_task(std::shared_ptr<Task> task) {
    // 1. 检查资源
    if (!check_resources(task->required)) {
        update_task_state(task, TaskState::FAILED, {}, "Insufficient resources");
        return;
    }

    //动态超时
    auto now_ms = get_current_timestamp_ms();
    std::int64_t remaining_timeout = task->timeout_ms - (now_ms - task->submit_ts); 
    std::cerr<<remaining_timeout<<'\n';
    if (remaining_timeout < 0) {
        update_task_state(task, TaskState::TIMEOUT, {}, "Execution timeout");
        return;
    }
    

    // 4. 创建异步超时定时器（使用io_context_）
    auto exec_timer = std::make_shared<boost::asio::steady_timer>(io_context_);
    exec_timer->expires_after(std::chrono::milliseconds(remaining_timeout));
    exec_timer->async_wait([this, task, exec_timer](const boost::system::error_code& ec) {
            if(!ec)
            {std::cerr << "[TIMER] cb ec=" << ec.message() << '\n';
            task->cancelled->store(true, std::memory_order_release);
            update_task_state(task, TaskState::TIMEOUT, {}, "Execution timeout");}
    });

    try {
        // 设置开始时间和状态
        task->start_ts = now_ms;
        update_task_state(task, TaskState::RUNNING);  // 更新状态为RUNNING，但不带result/error

        // 查找函数
        auto it = functions_.find(task->func_name);
        if (it == functions_.end()) {
            throw std::runtime_error("Unknown function: " + task->func_name);
        }

        // 执行函数
        nlohmann::json result = it->second(task->func_params, task);

        // 成功：更新状态并取消定时器
        update_task_state(task, TaskState::SUCCESS, result);
        exec_timer->cancel();
    } catch (const std::exception& e) {  // 捕获更广泛的异常（包括std::runtime_error等）

        // 检查是否可重试
        boost::system::error_code ec;  // 默认无error_code，需根据异常类型判断
        // 假设异常可能携带error_code；否则，扩展is_retryable_error以处理字符串或自定义逻辑
        if (auto sys_err = dynamic_cast<const boost::system::system_error*>(&e)) {
            ec = sys_err->code();
        }

        if (is_retryable_error(ec) && task->retry_count < task->max_retry) {
            // 检查并发重试限额
            if (retrying_cnt.fetch_add(1, std::memory_order_acq_rel) >= MAX_CONCURRENT_RETRY) {
                retrying_cnt.fetch_sub(1, std::memory_order_acq_rel);
                update_task_state(task, TaskState::FAILED, {}, "Retry quota full");
                return;
            }

            // 指数退避延迟重试
            uint32_t retry_level = std::min(task->retry_count, 4u);
            auto delay = std::chrono::seconds(1 << retry_level);
            auto retry_timer = std::make_shared<boost::asio::steady_timer>(io_context_, delay);
            retry_timer->async_wait([this, task, retry_timer](const boost::system::error_code& ec_retry) {
                if (!ec_retry) {
                    retrying_cnt.fetch_sub(1, std::memory_order_acq_rel);
                    task->retry_count++;
                    execute_task(task);  // 重试
                }
            });
        } else {
            update_task_state(task, TaskState::FAILED, {}, "Execution failed: " + std::string(e.what()));
            exec_timer->cancel();
        }
    }
}

bool TaskExecutor::check_resources(const Resource& required) {
    return dts::common::ResourceTracker::instance().check_available(required);
}

void TaskExecutor::update_task_state(std::shared_ptr<Task> task, TaskState state,
                                     const nlohmann::json& result, const std::string& error_msg) {
    task->state = state;
    task->result = result;
    task->error_msg = error_msg;
    task->finish_ts = get_current_timestamp_ms();
    // 可添加通知或回调，如果需要
}

bool TaskExecutor::is_retryable_error(const boost::system::error_code& ec) {
    // 明确列出可重试错误
    return ec == boost::asio::error::connection_refused ||
           ec == boost::asio::error::host_unreachable ||
           ec == boost::asio::error::operation_aborted;  // 添加aborted作为可重试
    // 如果有自定义异常，可扩展此函数
}

} // namespace dts