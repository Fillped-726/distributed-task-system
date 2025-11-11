#pragma once

#include <pqxx/pqxx> // 引入 libpqxx
#include "logger.hpp"
#include <string>
#include <memory>
#include <stdexcept>
#include <cstdlib> // 用于 std::getenv

class PostgresConnection {
public:
    /**
     * @brief 构造函数, 从环境变量建立连接
     */
    PostgresConnection() {
        try {
            // 从环境变量安全地构建连接字符串
            const char* db_pass = std::getenv("DTS_DB_PASSWORD");
            const char* db_user = std::getenv("DTS_DB_USER");
            const char* db_name = std::getenv("DTS_DB_NAME");
            const char* db_host = std::getenv("DTS_DB_HOST");

            if (!db_pass || !db_user || !db_name || !db_host) {
                throw std::runtime_error("数据库环境变量未设置 (DTS_DB_...)");
            }

            std::string conn_string = "dbname=" + std::string(db_name) +
                                      " user=" + std::string(db_user) +
                                      " password=" + std::string(db_pass) +
                                      " hostaddr=" + std::string(db_host) +
                                      " port=5432"; // 默认端口

            // m_conn 是一个 unique_ptr, 在这里创建实例
            m_conn = std::make_unique<pqxx::connection>(conn_string);
            
            LOG(INFO) << "成功连接到数据库: " << m_conn->dbname();

        } catch (const pqxx::broken_connection& e) {
            LOG(ERROR) << "数据库连接失败: " << e.what();
            throw; // 重新抛出, 应用程序应停止
        }
    }

    // 获取底层连接对象的引用, 以便 TaskSubmitter 可以创建事务
    pqxx::connection& get_connection() {
        return *m_conn;
    }

private:
    // 使用智能指针管理连接的生命周期
    std::unique_ptr<pqxx::connection> m_conn;
};
