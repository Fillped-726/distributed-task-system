#pragma once

// === Standard Library Includes ===
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// === Third-Party Includes ===
#include <pqxx/transaction.hxx>
#include <sw/redis++/redis++.h>
#include "utils/dts_metrics.h"
#include <prometheus/counter.h>

// === Project Includes ===
#include "batch_item.hpp"
#include "dag.hpp"
#include "database_pool.h"
#include "dts/service/task_service.pb.h"

namespace dts::api_server {

/**
 * @class TaskSubmitter
 * @brief 负责接收客户端的 DAG 提交请求，并进行批量化处理。
 *
 * 核心设计模式：Producer-Consumer
 * 1. SubmitDagAsync (Producer): 极速生成 UUID，封装 Context，入队即返回。
 * 2. FlusherLoop (Consumer): 后台线程定期或定量从队列获取任务，进行批量落库
 * (PG) 和 推送 (Redis)。
 */
class TaskSubmitter {
 public:
  explicit TaskSubmitter(std::shared_ptr<dts::common::DatabasePool> pool);
  ~TaskSubmitter();

  /**
   * @brief 异步提交 DAG 任务。
   * 该函数是非阻塞的，它只负责 ID 生成和入队。
   *
   * @param req gRPC 请求体
   * @return std::string 本次生成的 Job ID
   * @throw std::runtime_error 如果队列已满 (背压保护)
   */
  std::string SubmitDagAsync(const dts::service::SubmitDagRequest& req);

 private:
  // =========================================================
  // 1. 线程模型与生命周期
  // =========================================================
  std::atomic<bool> running_{true};
  std::vector<std::thread> flusher_threads_;

  // 消费者线程数：建议根据 CPU 核数或 IO 密集度调整
  const int CONSUMER_THREAD_NUM = 8;

  // =========================================================
  // 2. 任务队列 (生产者-消费者缓冲区)
  // =========================================================
  // 使用 vector 作为底层容器，在 swap 时利用 capacity 避免内存重新分配，
  // 相比 std::deque 或 std::queue 有更好的内存连续性。
  std::vector<BatchItem> pending_queue_;

  std::mutex queue_mutex_;
  std::condition_variable cv_;

  // =========================================================
  // 3. 批处理配置参数
  // =========================================================
  const size_t BATCH_SIZE_THRESHOLD = 2000;  // 触发刷盘的数量阈值
  const size_t MIN_BATCH_SIZE = 200;         // 最小批次大小 (防抖)
  const size_t BATCH_TIMEOUT_MS = 200;       // 触发刷盘的时间阈值 (毫秒)
  const size_t MAX_QUEUE_SIZE = 100000;      // 队列最大深度 (防 OOM)

  // 性能监控
  inline static prometheus::Counter* rpc_counter = nullptr;

  void InitMetrics();

  // =========================================================
  // 4. 外部依赖
  // =========================================================
  std::shared_ptr<dts::common::DatabasePool> db_pool_;

  // =========================================================
  // 5. 内部处理逻辑
  // =========================================================

  // 后台线程的主循环函数
  void FlusherLoop();

  // 处理单个批次的入口：协调 DB 事务与 Redis Pipeline
  void ProcessBatch(std::vector<BatchItem>& batch);

  // 数据库批处理：负责将 BatchItem 写入 PostgreSQL
  void FlushDBBatch(pqxx::work& tx, std::vector<BatchItem>& batch);

  // Redis 批处理：负责构建 Pipeline 指令
  void AppendDagToPipeline(sw::redis::Pipeline& pipe,
                           const dts::service::SubmitDagRequest& proto_req,
                           const DagCommitContext& ctx);
};

}  // namespace dts::api_server