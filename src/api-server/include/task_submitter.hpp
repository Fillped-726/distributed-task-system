#pragma once

#include "dag.hpp"
#include "dts/service/task_service.pb.h"
#include <pqxx/transaction.hxx>

namespace dts::api_server {

struct DagCommitContext {
  std::string job_id;
  std::unordered_map<std::string, std::string> natural_to_uuid;
  std::vector<std::string> entry_task_ids;
};

class TaskSubmitter {
 public:
  /**
   * @brief 4.3 任务提交 API (使用真实的 pqxx::connection)
   * @param request 包含 DAG 定义的请求
   * @param conn    一个活动的 libpqxx 数据库连接
   * @return bool   true 表示成功, false 表示失败
   */
  // **已更新**: 依赖于真实的 pqxx::connection
  std::optional<DagCommitContext> PersistDagToDB(
      const dts::service::SubmitDagRequest& proto_req, pqxx::work& tx);

  bool DispatchDagToRedis(const dts::service::SubmitDagRequest& proto_req,
                          const DagCommitContext& ctx);
};

}  // namespace dts::api_server