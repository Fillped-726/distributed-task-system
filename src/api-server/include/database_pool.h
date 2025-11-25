#pragma once

#include <pqxx/pqxx>
#include <string>
#include <memory>
#include <stdexcept>
#include <cstdlib>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional> // <-- 需要 std::function

/**
 * @brief 一个简单的、线程安全的 libpqxx 连接池
 */
class DatabasePool {
public:
    /**
     * @brief 构造函数
     * @param conn_string libpqxx 连接字符串
     * @param pool_size 池中的连接数 (e.g., 10)
     */
    DatabasePool(const std::string& conn_string, size_t pool_size) 
        : m_conn_string(conn_string) 
    {
        if (pool_size == 0) {
            throw std::runtime_error("连接池大小必须 > 0");
        }
        // 预先填充连接池
        for (size_t i = 0; i < pool_size; ++i) {
            try {
                auto conn = std::make_unique<pqxx::connection>(m_conn_string);
                m_pool.push(std::move(conn));
            } catch (const std::exception& e) {
                // (可以记录日志)
                throw std::runtime_error("无法创建连接池 (连接失败): " + std::string(e.what()));
            }
        }
    }

    ~DatabasePool() {
        // (析构时, m_pool 中的 unique_ptr 会自动释放所有连接)
    }

    // 禁用拷贝和赋值
    DatabasePool(const DatabasePool&) = delete;
    DatabasePool& operator=(const DatabasePool&) = delete;

    /**
     * @brief 线程安全地执行一个数据库事务
     *
     * @param tx_logic 一个接受 pqxx::work& 的 lambda 函数
     */
    void ExecuteTx(const std::function<void(pqxx::work&)>& tx_logic) {
        
        // 1. 获取连接 (线程安全)
        auto conn = AcquireConnection();

        try {
            // 2. 在连接上启动事务
            pqxx::work tx(*conn);
            
            // 3. 执行用户定义的业务逻辑
            tx_logic(tx);
            
            // 4. 提交
            tx.commit();

        } catch (const std::exception& e) {
            // (发生异常, tx 会在析构时自动回滚)
            // 释放连接, 然后重新抛出异常, 让上层(e.g., main.cpp)知道
            ReleaseConnection(std::move(conn));
            throw; 
        }

        // 5. 释放连接回池
        ReleaseConnection(std::move(conn));
    }


private:
    // "租借"一个连接
    std::unique_ptr<pqxx::connection> AcquireConnection() {
        std::unique_lock<std::mutex> lock(m_mutex);

        // 等待, 直到池中有一个可用的连接
        m_cond.wait(lock, [this] { return !m_pool.empty(); });

        // 从池中取出连接
        auto conn = std::move(m_pool.front());
        m_pool.pop();
        return conn;
    }

    // "归还"一个连接
    void ReleaseConnection(std::unique_ptr<pqxx::connection> conn) {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            // (简单的健康检查)
            if (conn && conn->is_open()) {
                m_pool.push(std::move(conn));
            } else {
                // 连接已损坏, 丢弃它, 并创建一个新的来补充
                try {
                    auto new_conn = std::make_unique<pqxx::connection>(m_conn_string);
                    m_pool.push(std::move(new_conn));
                } catch(...) {
                    // (如果连新连接都创建失败, 池会变小)
                }
            }
        } // 锁释放
        m_cond.notify_one(); // 唤醒一个等待的线程
    }

    std::string m_conn_string;
    std::mutex m_mutex;
    std::condition_variable m_cond;
    std::queue<std::unique_ptr<pqxx::connection>> m_pool;
};