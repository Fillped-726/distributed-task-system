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

// [METRICS]: 引入 Prometheus 核心组件
#include <prometheus/gauge.h>
#include "utils/dts_metrics.h"

namespace dts {
namespace common {

/**
 * @brief 通用线程池实现 (Fixed Thread Pool)
 * * 特性:
 * 1. 固定数量的工作线程。
 * 2. 支持获取任务执行结果 (std::future)。
 * 3. [关键特性] 自动捕获和传递线程上下文 (Context Propagation)，
 * 确保 Log Trace ID (req_id) 能够从提交线程传递到工作线程。
 * 4. [监控] 集成 Prometheus 监控队列堆积和活跃线程。
 */
class ThreadPool {
 public:
  /**
   * @brief 构造函数：初始化并启动线程池
   * * @param threads 线程池中工作线程的数量
   */
  explicit ThreadPool(size_t threads) : stop_(false) {
    // [METRICS INIT]: 初始化监控指标
    // 获取全局注册表
    auto registry = dts::Metrics::Instance().GetRegistry();

    // 构建 Gauge Family (指标族)
    // 注意：如果程序中有多个 ThreadPool 实例，建议构造函数传入 name 参数作为
    // Label 区分
    auto& gauge_family =
        prometheus::BuildGauge()
            .Name("dts_thread_pool_stats")
            .Help(
                "Internal thread pool statistics (queue size & active threads)")
            .Register(*registry);

    // 添加具体的 Gauge 指标，使用 Label 区分类型
    // 指针由 Family 内部管理，生命周期长于 ThreadPool (直到 Registry 销毁)
    queue_size_gauge_ = &gauge_family.Add({{"type", "queue_size"}});
    active_threads_gauge_ = &gauge_family.Add({{"type", "active_threads"}});

    // 预先分配空间，避免 vector 扩容带来的额外开销
    workers_.reserve(threads);

    for (size_t i = 0; i < threads; ++i) {
      workers_.emplace_back([this] {
        // [Thread Init]: 可以在这里设置线程名称，方便 top/gdb 调试
        // pthread_setname_np(pthread_self(), "dts_worker");

        while (true) {
          std::function<void()> task;

          // --- 临界区开始 ---
          {
            std::unique_lock<std::mutex> lock(this->queue_mutex_);

            // 等待条件：线程池停止 或 队列中有任务
            this->condition_.wait(
                lock, [this] { return this->stop_ || !this->tasks_.empty(); });

            // 退出条件：如果线程池停止且队列为空，则线程退出
            if (this->stop_ && this->tasks_.empty()) return;

            // 取出任务
            task = std::move(this->tasks_.front());
            this->tasks_.pop();

            // [METRICS]: 任务出队，队列长度 -1
            if (this->queue_size_gauge_) this->queue_size_gauge_->Decrement();
          }
          // --- 临界区结束 ---

          // [METRICS]: 开始执行，活跃线程 +1
          if (this->active_threads_gauge_)
            this->active_threads_gauge_->Increment();

          // 执行任务 (必须在锁外执行，防止死锁并提高并发度)
          // 注意：task 内部已包含了 SetRequestId 的恢复逻辑
          task();

          // [METRICS]: 执行结束，活跃线程 -1
          if (this->active_threads_gauge_)
            this->active_threads_gauge_->Decrement();

          // 任务结束后，清理当前线程的 ThreadLocal 上下文，防止污染下一条日志
          dts::SetRequestId("");
        }
      });
    }
  }

  /**
   * @brief 析构函数：优雅停止线程池
   * * 设置停止标志位，唤醒所有阻塞线程，并等待它们执行完剩余任务后退出。
   */
  ~ThreadPool() {
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      stop_ = true;
    }

    // 唤醒所有等待中的线程，让它们检查 stop_ 标志并退出
    condition_.notify_all();

    // 等待所有线程结束 (Join)
    for (std::thread& worker : workers_) {
      if (worker.joinable()) worker.join();
    }
  }

  /**
   * @brief 向线程池提交任务
   * * @tparam F 任务函数类型
   * @tparam Args 任务参数类型
   * @param f 可调用对象
   * @param args 参数列表
   * @return std::future<ReturnType> 任务的异步执行结果
   * * @note 此函数会自动捕获当前线程的 trace id (req_id)，并在执行时恢复。
   */
  template <class F, class... Args>
  auto enqueue(F&& f, Args&&... args)
      -> std::future<typename std::invoke_result<F, Args...>::type> {
    using return_type = typename std::invoke_result<F, Args...>::type;

    // 1. 将任务及其参数打包进 packaged_task
    // 使用 make_shared 是为了让任务的所有权可以被 lambda 复制并在 std::function
    // 中传递
    auto task = std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));

    std::future<return_type> res = task->get_future();

    // [Context Capture]: 捕获当前 RPC/Http 线程的 Context (req_id)
    // 必须在 enqueue 调用发生时立即捕获
    std::string captured_ctx_id = dts::t_req_id;

    {
      std::unique_lock<std::mutex> lock(queue_mutex_);

      if (stop_) throw std::runtime_error("enqueue on stopped ThreadPool");

      // 2. 将任务放入队列
      // 包装一层 lambda 用于 Context 的恢复与清理
      tasks_.emplace([task, captured_ctx_id]() {
        // A. 恢复上下文：将 Worker 线程的 ID 设置为提交者的 ID
        dts::SetRequestId(captured_ctx_id);

        // B. 执行真正的用户逻辑
        (*task)();

        // C. 清理工作交由 Worker 循环的末尾统一处理
      });

      // [METRICS]: 任务入队，队列长度 +1
      if (queue_size_gauge_) queue_size_gauge_->Increment();
    }

    // 唤醒一个闲置线程处理任务
    condition_.notify_one();
    return res;
  }

 private:
  std::vector<std::thread> workers_;         ///< 工作线程列表
  std::queue<std::function<void()>> tasks_;  ///< 任务队列

  std::mutex queue_mutex_;             ///< 保护任务队列的互斥锁
  std::condition_variable condition_;  ///< 用于线程同步的条件变量
  bool stop_;                          ///< 线程池停止标志

  // [METRICS]: Prometheus Gauges
  // 使用指针是因为 Family.Add() 返回引用，且生命周期由 Registry 托管
  prometheus::Gauge* queue_size_gauge_ = nullptr;
  prometheus::Gauge* active_threads_gauge_ = nullptr;
};

}  // namespace common
}  // namespace dts