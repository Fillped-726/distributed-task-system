// #include "thread_pool.hpp"
// #include <iostream>

// namespace dts {

// // 内联实现（为了头文件完整性，通常可移到.cpp文件）

// ThreadPool::ThreadPool(size_t num_threads, size_t queue_capacity)
//     : queue_(queue_capacity), target_thread_count_(num_threads) {
//     threads_.reserve(num_threads);
//     for (size_t i = 0; i < num_threads; ++i) {
//         threads_.emplace_back(&ThreadPool::worker, this);
//     }
// }

// ThreadPool::~ThreadPool() {
//     stop_.store(true, std::memory_order_release);
//     task_sem_.release(threads_.size()); // 唤醒所有线程
//     // 把残留任务清掉
//     std::function<void()>* ptr;
//     while (queue_.pop(ptr)) {
//         delete ptr;
//         leftover_.fetch_sub(1, std::memory_order_relaxed);
//     }
// }

// void ThreadPool::enqueue(std::function<void()> task) {
//     if (stop_.load(std::memory_order_acquire))
//         throw std::runtime_error("pool stopped");
//     auto* p = new std::function<void()>(std::move(task));
//     while (!queue_.push(p)) {                // 失败即队列满，自旋
//         if (stop_.load(std::memory_order_acquire)) {  // 再次检查
//             delete p;
//             throw std::runtime_error("pool stopped");
//         }
//         std::this_thread::yield();
//     }
//     leftover_.fetch_add(1, std::memory_order_relaxed);
//     task_sem_.release();                     // 通知一条线程
// }

// void ThreadPool::worker() {
//     active_threads_.fetch_add(1, std::memory_order_relaxed);
//     std::function<void()>* ptr = nullptr;
//     while (!stop_.load(std::memory_order_acquire)) {
//         // 最多等 200ms，兼顾快速停止
//         if (task_sem_.try_acquire_for(std::chrono::milliseconds(200))) {
//             if (queue_.pop(ptr)) {
//                 (*ptr)();
//                 delete ptr;
//                 leftover_.fetch_sub(1, std::memory_order_relaxed);
//             }
//         }
//         // 线程大于目标值进行自杀
//         if (active_threads_.load(std::memory_order_relaxed) >
//             target_thread_count_.load(std::memory_order_relaxed)) {
//             // CAS 保证只退出一条
//             size_t old = active_threads_.load(std::memory_order_relaxed);
//             if (old > target_thread_count_.load(std::memory_order_relaxed) &&
//                 active_threads_.compare_exchange_strong(old, old - 1)) {
//                 return;   //退出线程
//             }

//             if (!stop_.load(std::memory_order_acquire) && queue_.empty())
//                 std::this_thread::sleep_for(std::chrono::milliseconds(50));

//         }
//     }
//     active_threads_.fetch_sub(1, std::memory_order_relaxed);
// }

// void ThreadPool::add_threads(size_t num_threads) {
//     // 原子增加目标线程数
//     target_thread_count_.fetch_add(num_threads, std::memory_order_relaxed);
//     threads_.reserve(threads_.size() + num_threads);
//     // 创建新线程
//     for (size_t i = 0; i < num_threads; ++i) {
//         threads_.emplace_back(&ThreadPool::worker, this);
//     }
// }

// void ThreadPool::remove_threads(size_t num_threads) {
//     // 计算新目标线程数
//     size_t current_target =
//     target_thread_count_.load(std::memory_order_relaxed); size_t new_target =
//     (current_target > num_threads) ? current_target - num_threads : 0;
//     // 设置新目标（线程会在worker循环中自退出）
//     target_thread_count_.store(new_target, std::memory_order_relaxed);
// }

// size_t ThreadPool::get_thread_count() const {
//     // 返回活跃线程数
//     return active_threads_.load(std::memory_order_relaxed);
// }

// size_t ThreadPool::get_tasks_left() const { return
// leftover_.load(std::memory_order_relaxed); }

// void ThreadPool::shutdown() { stop_.store(true, std::memory_order_release); }

// }  // namespace dts