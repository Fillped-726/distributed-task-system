#include "task_scheduler.hpp"
Scheduler::Scheduler(ThreadPool* pool) : pool_(pool) {}
void Scheduler::schedule(TaskPtr task) {
    // 先无脑塞进线程池，让它立刻跑
    pool_->enqueue([task](){
        task->state = 1;
        std::this_thread::sleep_for(std::chrono::milliseconds(200)); // 模拟
        task->state = 2;
    });
}