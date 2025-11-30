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
      ReleaseConnection(std::move(conn));
      LOG(ERROR) << "Database connection broken during Tx: " << e.what();
      throw;  // 抛出异常让上层处理重试
    } catch (const std::exception& e) {
      ReleaseConnection(std::move(conn));
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
    if (duration > std::chrono::milliseconds(10)) {
      LOG(WARNING) << "AcquireConnection blocked for "
                   << std::chrono::duration_cast<std::chrono::milliseconds>(
                          duration)
                          .count()
                   << "ms. Current pool size: " << m_pool.size();
    }

    auto conn = std::move(m_pool.front());
    m_pool.pop();
    return conn;
  }

  void ReleaseConnection(std::unique_ptr<pqxx::connection> conn,
                         bool force_reconnect = false) {
    // 1. 如果需要重连，先在锁外完成重连操作
    if (force_reconnect || (conn && !conn->is_open())) {
      LOG(WARNING) << "Connection broken, attempting to reconnect...";
      try {
        // 在锁外进行耗时的网络操作
        auto new_conn = std::make_unique<pqxx::connection>(m_conn_string);
        if (new_conn->is_open()) {
          conn = std::move(new_conn);  // 替换旧连接
        } else {
          // 重连失败，此时 conn 为空或者坏连接，下面处理
          conn.reset();
        }
      } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to replenish DB pool: " << e.what();
        conn.reset();  // 确保坏连接被销毁
      }
    }

    // 2. 只有在此处才加锁，将连接放回队列
    // 如果重连失败导致 conn
    // 为空，就不放回了（池子暂时缩小，等待下次扩容或监控报警）
    if (conn && conn->is_open()) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_pool.push(std::move(conn));
      m_cond.notify_one();
    } else {
      // 如果连接彻底丢了，这里需要一种机制来通知系统池子变小了，
      // 或者在这个分支里起一个异步线程去不断重试直到成功，
      // 否则池子会越来越小直到枯竭。
      LOG(ERROR) << "Connection pool size decreased!";
    }
  }

  std::string m_conn_string;
  std::mutex m_mutex;
  std::condition_variable m_cond;
  std::queue<std::unique_ptr<pqxx::connection>> m_pool;
};

}  // namespace common
}  // namespace dts