#pragma once

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>
#include <atomic>
#include "logger.hpp"

namespace dts {
namespace common {

class ThreadPool {
 public:
  // 构造函数：启动固定数量的 Worker 线程
  explicit ThreadPool(size_t threads) : stop_(false) {
    for (size_t i = 0; i < threads; ++i) {
      workers_.emplace_back([this] {
        // 线程启动时的初始化（可选）
        // SetRequestId("worker-idle");

        while (true) {
          std::function<void()> task;

          // 临界区
          {
            std::unique_lock<std::mutex> lock(this->queue_mutex_);

            // 等待直到有任务 或 收到停止信号
            this->condition_.wait(
                lock, [this] { return this->stop_ || !this->tasks_.empty(); });

            // 如果停止且队列为空，则退出线程
            if (this->stop_ && this->tasks_.empty()) return;

            // 取出任务
            task = std::move(this->tasks_.front());
            this->tasks_.pop();
          }

          // 执行任务 (在锁外执行！)
          // 注意：这里的 task 已经是我们在 enqueue 中包装过的 lambda
          // 它内部已经包含了 SetRequestId 的逻辑
          task();

          // 任务结束后，清理上下文，防止污染日志
          dts::SetRequestId("");
        }
      });
    }
  }

  // 析构函数：优雅停止
  ~ThreadPool() {
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      stop_ = true;
    }
    condition_.notify_all();  // 唤醒所有线程让它们去自杀

    for (std::thread& worker : workers_) {
      if (worker.joinable()) worker.join();
    }
  }

  // 核心方法：提交任务
  template <class F, class... Args>
  auto enqueue(F&& f, Args&&... args)
      -> std::future<typename std::invoke_result<F, Args...>::type> {
    using return_type = typename std::invoke_result<F, Args...>::type;

    // 1. 将任务打包
    auto task = std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));

    std::future<return_type> res = task->get_future();

    // [关键改造]：捕获当前线程的 Context (req_id)
    // 当这个 lambda 被构造时，它是在 RPC 线程中，所以 t_req_id 是正确的
    std::string captured_ctx_id = dts::t_req_id;

    {
      std::unique_lock<std::mutex> lock(queue_mutex_);

      if (stop_) throw std::runtime_error("enqueue on stopped ThreadPool");

      // 2. 将任务放入队列
      // 我们包装一层 lambda，负责在执行前恢复 Context
      tasks_.emplace([task, captured_ctx_id]() {
        // A. 恢复上下文
        dts::SetRequestId(captured_ctx_id);

        // B. 执行真正的任务
        (*task)();

        // C. 上下文清理交给 Worker 循环或这里均可，
        // 为了保险，Worker 循环里也清理了一次。
      });
    }

    condition_.notify_one();
    return res;
  }

 private:
  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;

  std::mutex queue_mutex_;
  std::condition_variable condition_;
  bool stop_;
};

}  // namespace common
}  // namespace dts