// // thread_pool.hpp
// #pragma once

// #include <atomic>
// #include <functional>
// #include <thread>
// #include <vector>
// #include <boost/lockfree/queue.hpp>

// namespace dts {

// class ThreadPool {
//  public:
//   // 构造函数：指定线程数和队列容量
//   explicit ThreadPool(size_t num_threads, size_t queue_capacity = 1024);
//   // 析构函数：停止并join所有线程
//   ~ThreadPool();

//   // 入队任务：将std::function<void()>类型的任务添加到队列
//   void enqueue(std::function<void()> task);

//   // 动态添加线程
//   void add_threads(size_t num_threads);

//   // 尝试移除线程：优雅退出空闲线程
//   void remove_threads(size_t num_threads);

//   // 获取当前活跃线程数
//   size_t get_thread_count() const;

//   // 获取残留任务数
//   size_t get_tasks_left() const;

//   // 主动结束线程池
//   void shutdown();

//  private:
//   // 工作线程函数：不断从队列取任务执行
//   void worker();

//   // 任务队列：使用Boost无锁队列，存储任务指针
//   boost::lockfree::queue<std::function<void()>*> queue_;
//   // 线程容器：使用std::jthread（C++20）
//   std::vector<std::jthread> threads_;
//   // 停止标志：原子变量
//   std::atomic<bool> stop_{false};
//   // 活跃线程数：原子计数
//   std::atomic<size_t> active_threads_{0};
//   // 目标线程数：用于动态调整
//   std::atomic<size_t> target_thread_count_{0};

//   // 信号量
//   std::binary_semaphore task_sem_{0};
//   // 残留任务数
//   std::atomic<std::size_t> leftover_{0};
// };

// }  // namespace dts