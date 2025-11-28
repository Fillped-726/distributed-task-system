#pragma once

#include <pqxx/pqxx>
#include <string>
#include <memory>
#include <stdexcept>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <glog/logging.h>

namespace dts {
namespace common {

/**
 * @brief 线程安全的 PostgreSQL 连接池
 */
class DatabasePool {
 public:
  DatabasePool(const std::string& conn_string, size_t pool_size)
      : m_conn_string(conn_string) {
    if (pool_size == 0) {
      LOG(FATAL) << "Database pool size must be > 0";
    }

    LOG(INFO) << "Initializing DatabasePool with size: " << pool_size;

    for (size_t i = 0; i < pool_size; ++i) {
      try {
        auto conn = std::make_unique<pqxx::connection>(m_conn_string);
        if (conn->is_open()) {
          m_pool.push(std::move(conn));
        } else {
          LOG(ERROR) << "Created connection is not open!";
        }
      } catch (const std::exception& e) {
        // 初始化阶段如果连不上数据库，建议直接 Crash (Fail Fast 原则)
        LOG(FATAL) << "Failed to create DB connection: " << e.what();
      }
    }
  }

  ~DatabasePool() {
    LOG(INFO) << "Destroying DatabasePool...";
    // unique_ptr 会自动释放连接
  }

  // 禁用拷贝，只能单例或引用传递
  DatabasePool(const DatabasePool&) = delete;
  DatabasePool& operator=(const DatabasePool&) = delete;

  /**
   * @brief 执行事务的模板方法 (Template Method / Around Pattern)
   */
  void ExecuteTx(const std::function<void(pqxx::work&)>& tx_logic) {
    // 1. 获取连接
    auto conn = AcquireConnection();
    // 标记连接是否需要被销毁（比如断开了）
    bool conn_broken = false;

    try {
      // 2. 开启事务
      pqxx::work tx(*conn);

      // 3. 执行业务
      tx_logic(tx);

      // 4. 提交
      tx.commit();
    } catch (const pqxx::broken_connection& e) {
      conn_broken = true;
      LOG(ERROR) << "Database connection broken during Tx: " << e.what();
      throw;  // 抛出异常让上层处理重试
    } catch (const std::exception& e) {
      LOG(WARNING) << "Transaction failed (rollback): " << e.what();
      throw;
    }

    // 5. 归还连接
    // 如果连接坏了，ReleaseConnection 内部会尝试重建
    ReleaseConnection(std::move(conn), conn_broken);
  }

 private:
  std::unique_ptr<pqxx::connection> AcquireConnection() {
    std::unique_lock<std::mutex> lock(m_mutex);

    // 记录等待开始时间，用于监控性能
    auto start = std::chrono::steady_clock::now();

    // 等待连接可用
    if (m_pool.empty()) {
      LOG(WARNING) << "DB Pool is empty, thread is waiting...";
    }

    m_cond.wait(lock, [this] { return !m_pool.empty(); });

    // 简单的性能监控：如果等待超过 1秒，打印日志
    auto duration = std::chrono::steady_clock::now() - start;
    if (duration > std::chrono::seconds(1)) {
      LOG(WARNING) << "AcquireConnection blocked for "
                   << std::chrono::duration_cast<std::chrono::milliseconds>(
                          duration)
                          .count()
                   << "ms";
    }

    auto conn = std::move(m_pool.front());
    m_pool.pop();
    return conn;
  }

  void ReleaseConnection(std::unique_ptr<pqxx::connection> conn,
                         bool force_reconnect = false) {
    std::lock_guard<std::mutex> lock(m_mutex);

    if (!force_reconnect && conn && conn->is_open()) {
      m_pool.push(std::move(conn));
    } else {
      // 连接已断开或被标记为损坏，尝试替换一个新的
      LOG(WARNING) << "Discarding broken connection and creating a new one.";
      try {
        auto new_conn = std::make_unique<pqxx::connection>(m_conn_string);
        if (new_conn->is_open()) {
          m_pool.push(std::move(new_conn));
        }
      } catch (const std::exception& e) {
        // 这是一个严重问题：连接池正在永久性缩小
        LOG(ERROR) << "CRITICAL: Failed to replenish DB pool: " << e.what();
      }
    }

    m_cond.notify_one();
  }

  std::string m_conn_string;
  std::mutex m_mutex;
  std::condition_variable m_cond;
  std::queue<std::unique_ptr<pqxx::connection>> m_pool;
};

}  // namespace common
}  // namespace dts