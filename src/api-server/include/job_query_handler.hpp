#pragma once

#include <memory>
#include <string>
#include <vector>
#include "dts/service/task_service.pb.h"  // Proto 定义
#include "database_pool.h"                // 你的 DB 连接池

namespace dts::api_server {

// 简化命名空间
using PbGetJobStatusRequest = dts::service::GetJobStatusRequest;
using PbGetJobStatusResponse = dts::service::GetJobStatusResponse;
using dts::task::TaskRuntimeDetail;  // Proto 中的 Message

class JobQueryHandler {
 public:
  // 构造函数，通常不需要做什么，或者可以做一些 metrics 初始化
  JobQueryHandler() = default;
  ~JobQueryHandler() = default;

  /**
   * @brief 处理 GetJobStatus 请求
   * 符合 AsyncServer::GetJobStatusFunc 的签名
   */
  void Handle(std::shared_ptr<dts::common::DatabasePool> db_conn,
              PbGetJobStatusRequest* req, PbGetJobStatusResponse* resp);

 private:
  // --- 内部辅助函数 ---

  // 1. 从 Redis 获取 Job 包含的任务 ID 列表
  // 返回 true 表示在 Redis 命中，false 表示需要去 DB 查
  bool GetTaskIdsFromRedis(const std::string& job_id,
                           std::vector<std::string>& out_task_ids);

  // 2. 从 DB 获取 Job 包含的任务 ID 列表 (兜底)
  void GetTaskIdsFromDB(std::shared_ptr<dts::common::DatabasePool> db,
                        const std::string& job_id,
                        std::vector<std::string>& out_task_ids);

  // 3. 混合查询任务详情 (Redis Pipeline + DB 补漏)
  void FetchTaskDetails(std::shared_ptr<dts::common::DatabasePool> db,
                        const std::vector<std::string>& task_ids,
                        bool need_payload,  // 是否需要详细结果(payload)
                        PbGetJobStatusResponse* resp);  // 直接填充 response
};

}  // namespace dts::api_server