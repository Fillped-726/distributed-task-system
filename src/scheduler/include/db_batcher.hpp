#pragma once

#include <string>
#include <vector>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <deque>

#include "database_pool.h"
#include "dts/task/task.pb.h"  // 为了 TaskState 枚举

namespace dts::scheduler {

// 定义一个更新操作的数据结构
struct TaskUpdateOp {
  std::string task_id;
  dts::task::TaskState state;
  std::string result_json;
  std::string error_msg;
  std::string worker_id;
};

class DbBatcher {
 public:
  // 构造函数：需要数据库连接池
  explicit DbBatcher(std::shared_ptr<dts::common::DatabasePool> db_pool);
  ~DbBatcher();

  // 启动后台刷盘线程
  void Start();

  // 停止线程 (会等待剩余数据刷完)
  void Stop();

  // [生产端] 添加一条更新记录 (非阻塞，极快)
  void AddStatusUpdate(const std::string& task_id, dts::task::TaskState state,
                       const std::string& result_json = "",
                       const std::string& error_msg = "",
                       const std::string& worker_id = "");

 private:
  // [消费端] 后台线程主循环
  void RunLoop();

  // 执行批量 SQL
  void FlushBatch(std::vector<TaskUpdateOp>& batch);

 private:
  std::shared_ptr<dts::common::DatabasePool> db_pool_;

  // 线程安全队列相关
  std::deque<TaskUpdateOp> queue_;
  std::mutex mutex_;
  std::condition_variable cv_;

  std::thread worker_thread_;
  std::atomic<bool> stop_flag_{false};

  // 配置参数
  const size_t kBatchSize = 1000;        // 每次最多刷 100 条
  const size_t kFlushIntervalMs = 1000;  // 最长等待 1000ms
};

}  // namespace dts::scheduler