#include "db_batcher.hpp"
#include "logger.hpp"
#include <pqxx/pqxx>
#include <sstream>

namespace dts::scheduler {

DbBatcher::DbBatcher(std::shared_ptr<dts::common::DatabasePool> db_pool)
    : db_pool_(db_pool) {
  if (!db_pool_) {
    LOG_FATAL << "DbBatcher init with null db_pool";
  }
}

DbBatcher::~DbBatcher() { Stop(); }

void DbBatcher::Start() {
  if (stop_flag_) return;
  LOG_INFO << "DbBatcher starting...";
  worker_thread_ = std::thread(&DbBatcher::RunLoop, this);
}

void DbBatcher::Stop() {
  if (stop_flag_) return;
  LOG_INFO << "DbBatcher stopping...";
  stop_flag_ = true;
  cv_.notify_all();
  if (worker_thread_.joinable()) {
    worker_thread_.join();
  }
  LOG_INFO << "DbBatcher stopped.";
}

void DbBatcher::AddStatusUpdate(const std::string& task_id,
                                dts::task::TaskState state,
                                const std::string& result_json,
                                const std::string& error_msg) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push_back({task_id, state, result_json, error_msg});
  }
  if (queue_.size() >= kBatchSize) {
    cv_.notify_one();
  }
}

void DbBatcher::RunLoop() {
  std::vector<TaskUpdateOp> batch;
  batch.reserve(kBatchSize);

  while (!stop_flag_ || !queue_.empty()) {  // 即使 stop 了也要把剩下的刷完
    {
      std::unique_lock<std::mutex> lock(mutex_);

      // 等待条件：队列不空 OR 停止信号
      // 使用 wait_for 实现“每隔 kFlushIntervalMs 至少醒一次”
      cv_.wait_for(lock, std::chrono::milliseconds(kFlushIntervalMs),
                   [this] { return !queue_.empty() || stop_flag_; });

      if (queue_.empty()) {
        if (stop_flag_) break;  // 真的没事做了且要退出
        continue;               // 超时醒来，还是空的，继续等
      }

      // 搬运数据到局部 batch
      while (!queue_.empty() && batch.size() < kBatchSize) {
        batch.push_back(std::move(queue_.front()));
        queue_.pop_front();
      }
    }  // 解锁

    if (!batch.empty()) {
      FlushBatch(batch);
      batch.clear();
    }
  }
}

void DbBatcher::FlushBatch(std::vector<TaskUpdateOp>& batch) {
  if (batch.empty()) return;

  try {
    db_pool_->ExecuteTx([&](pqxx::work& tx) {
      std::stringstream ss;

      ss << "UPDATE task AS t SET "
         << "state = v.state::int, "
         << "result = v.result::jsonb, "
         << "error_msg = v.error_msg, "
         << "finish_ts = EXTRACT(EPOCH FROM NOW())::BIGINT "
         << "FROM (VALUES ";

      for (size_t i = 0; i < batch.size(); ++i) {
        const auto& op = batch[i];
        ss << "(" << tx.quote(op.task_id) << ", " << static_cast<int>(op.state)
           << ", " << tx.quote(op.result_json.empty() ? "{}" : op.result_json)
           << ", " << tx.quote(op.error_msg) << ")";

        if (i < batch.size() - 1) ss << ", ";
      }

      ss << ") AS v(task_id, state, result, error_msg) "
         << "WHERE t.task_id = v.task_id";

      tx.exec(ss.str());
    });

    LOG_INFO << "Flushed " << batch.size() << " task updates to DB.";

  } catch (const std::exception& e) {
    LOG_ERROR << "Failed to flush DB batch: " << e.what();
    // TODO
    //  [严重] 如果落盘失败，这批状态可能会丢失。
    //  在生产环境中，这里应该把 batch
    //  放回队列重试，或者写入本地日志文件防止丢数据。
  }
}

}  // namespace dts::scheduler