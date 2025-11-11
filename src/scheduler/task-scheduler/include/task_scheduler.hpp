#pragma once
#include "task.h"          // 你已有的 Task 结构
#include "thread_pool.h"   // 你已有的线程池

class Scheduler {
public:
    explicit Scheduler(ThreadPool* pool);   // 依赖注入线程池
    // 外部唯一入口：把任务交给调度器
    void schedule(TaskPtr task);
private:
    ThreadPool* pool_;                      // 已写好的线程池
};