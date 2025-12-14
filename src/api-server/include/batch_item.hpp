#include <future>
#include <optional>
#include <vector>
#include <chrono>
#include <queue>
#include <mutex>
#include <thread>
#include <condition_variable>

#include "dts/service/task_service.pb.h"

struct DagCommitContext {
  std::string job_id;
  std::unordered_map<std::string, std::string> natural_to_uuid;
  std::vector<std::string> entry_task_ids;
};

struct BatchItem {
  // 1. 原始请求数据
  dts::service::SubmitDagRequest req;

  // 3. 上下文/中间数据
  // 我们将在入队前预先生成好 UUID，这样 DB 和 Redis
  // 逻辑都能直接读取，不用重复生成
  DagCommitContext ctx;

  // 4. (可选) 调试/监控用：记录入队时间，用于观察排队延迟
  std::chrono::steady_clock::time_point enqueue_time;

  // 构造函数
  explicit BatchItem(const dts::service::SubmitDagRequest& r)
      : req(r), enqueue_time(std::chrono::steady_clock::now()) {}

  // 禁用拷贝，只能移动 (Move Only)，因为 std::promise 不可拷贝
  BatchItem(const BatchItem&) = delete;
  BatchItem& operator=(const BatchItem&) = delete;

  BatchItem(BatchItem&&) = default;
  BatchItem& operator=(BatchItem&&) = default;
};