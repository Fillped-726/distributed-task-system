#pragma once

#include "dag.hpp" 

// 向前声明, 避免在头文件中包含重量级的 <pqxx/pqxx>
namespace pqxx {
class connection;
}

class PostgresConnection; // 我们的连接包装类

class TaskSubmitter {
public:
    /**
     * @brief 4.3 任务提交 API (使用真实的 pqxx::connection)
     * @param request 包含 DAG 定义的请求
     * @param conn    一个活动的 libpqxx 数据库连接
     * @return bool   true 表示成功, false 表示失败
     */
    // **已更新**: 依赖于真实的 pqxx::connection
    bool handleSubmitDag(dts::SubmitDagRequest& request, pqxx::connection& conn); 
};