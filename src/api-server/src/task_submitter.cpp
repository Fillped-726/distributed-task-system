// === Standard Library Includes ===
#include <array>
#include <chrono>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <vector>

// === Third-Party Includes ===
#include <pqxx/pqxx>

#include "task_submitter.hpp"

// === Project Includes ===
#include "dts/task/task.pb.h"
#include "logger.hpp"
#include "redis/RedisKeys.hpp"
#include "redis/RedisManager.hpp"
#include "task.hpp"
#include "uuid_generator.hpp"
#include "utils/TaskSerializer.hpp"

namespace dts::api_server {

// 简化命名空间引用
using RedisManager = dts::common::redis::RedisManager;
using TaskSerializer = dts::common::utils::TaskSerializer;
namespace keys = dts::common::redis::keys;

// =========================================================
// 匿名命名空间：内部辅助函数
// =========================================================
namespace {

/**
 * @brief 构建 Redis Stream XADD 指令所需的字段。
 * 目前用于向 Worker 传递最基本的 Task ID 和 Job ID。
 */
std::vector<std::pair<std::string, std::string>> BuildXAddFieldsFromProto(
    const dts::task::Task& /*client_task*/,  // 保留参数用于未来扩展
    const std::string& task_uuid, const std::string& job_id) {
  std::vector<std::pair<std::string, std::string>> fields;
  fields.reserve(2);  // 预分配

  // 1. 核心指针：Worker 拿到这个 ID 后，去 KV (Meta) 里查详情
  fields.emplace_back("id", task_uuid);

  // 2. 上下文：用于日志、隔离或快速判断
  fields.emplace_back("job", job_id);

  return fields;
}

}  // namespace

// =========================================================
// 构造与析构
// =========================================================

TaskSubmitter::TaskSubmitter(std::shared_ptr<dts::common::DatabasePool> pool)
    : db_pool_(pool) {
  if (!db_pool_) {
    throw std::runtime_error("TaskSubmitter initialized with null DBPool");
  }

  running_ = true;
  LOG_INFO << "TaskSubmitter initialized. Starting " << CONSUMER_THREAD_NUM
           << " flusher threads.";

  for (int i = 0; i < CONSUMER_THREAD_NUM; ++i) {
    flusher_threads_.emplace_back(&TaskSubmitter::FlusherLoop, this);
  }
}

TaskSubmitter::~TaskSubmitter() {
  LOG_INFO << "TaskSubmitter shutting down...";
  running_.store(false, std::memory_order_release);
  cv_.notify_all();

  for (auto& t : flusher_threads_) {
    if (t.joinable()) {
      t.join();
    }
  }
  LOG_INFO << "TaskSubmitter stopped.";
}

// =========================================================
// 核心逻辑：消费者循环 (Consumer Loop)
// =========================================================

void TaskSubmitter::FlusherLoop() {
  std::vector<BatchItem> current_batch;
  // 预分配内存，减少 vector 扩容开销
  current_batch.reserve(BATCH_SIZE_THRESHOLD);

  while (running_) {
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);

      // 等待条件：停止运行 OR 队列积压最小批次
      // 设置超时时间 BATCH_TIMEOUT_MS，保证即使量少也能定时刷盘
      cv_.wait_for(lock, std::chrono::milliseconds(BATCH_TIMEOUT_MS), [this] {
        return !running_ || pending_queue_.size() >= MIN_BATCH_SIZE;
      });

      if (!running_ && pending_queue_.empty()) break;

      // 虚假唤醒检查
      if (pending_queue_.empty()) {
        continue;
      }

      // 【关键优化】使用 swap O(1) 取出所有任务，尽快释放锁
      current_batch.swap(pending_queue_);
    }

    if (!current_batch.empty()) {
      ProcessBatch(current_batch);
      LOG_INFO << "Flusher processed batch size: " << current_batch.size();

      // 清理并保留 Capacity，避免下次循环重新分配内存
      current_batch.clear();
    }
  }
}

// =========================================================
// 核心逻辑：批处理执行 (Process Batch)
// =========================================================

void TaskSubmitter::ProcessBatch(std::vector<BatchItem>& batch) {
  if (batch.empty()) return;

  try {
    // ---------------------------------------------------------
    // Step 1: 数据库操作 (原子性写入)
    // ---------------------------------------------------------
    // 使用事务保证一批任务要么全进 DB，要么全不进
    db_pool_->ExecuteTx(
        [this, &batch](pqxx::work& tx) { this->FlushDBBatch(tx, batch); });

    // ---------------------------------------------------------
    // Step 2: Redis 操作 (Pipeline 批量发射)
    // ---------------------------------------------------------
    try {
      auto& redis_manager = RedisManager::GetInstance();
      auto& redis = redis_manager.GetConnection();
      auto pipe = redis.pipeline();

      // 装填所有请求的 Redis 指令
      for (const auto& item : batch) {
        AppendDagToPipeline(pipe, item.req, item.ctx);
      }

      // 一次性发射！
      pipe.exec();

    } catch (const std::exception& e) {
      // [严重错误] DB Commit 成功，但 Redis Push 失败
      // 此时 DB 中有数据，但 Worker 收不到消息。
      // TODO：后续配合“定时扫描补单”机制来修复这些“僵尸任务”。
      LOG_ERROR << "CRITICAL: Redis Batch Failed after DB Commit. "
                << "BatchSize: " << batch.size() << ", Error: " << e.what();
      throw;  // 重新抛出，统一由外层 catch 处理
    }

  } catch (const std::exception& e) {
    // ---------------------------------------------------------
    // 异常处理：全批次失败
    // ---------------------------------------------------------
    // 只要 DB 失败或 Redis 严重错误，这批任务就算提交失败。
    // 由于是 Fire-and-Forget 模式（无 Future），这里仅记录日志。
    LOG_ERROR << "CRITICAL: Async Batch Commit Failed! " << e.what();
  }
}

// =========================================================
// 数据库实现细节 (PostgreSQL)
// =========================================================

void TaskSubmitter::FlushDBBatch(pqxx::work& tx,
                                 std::vector<BatchItem>& batch) {
  if (batch.empty()) return;

  // 获取当前时间戳 (所有任务共用一个 submit_ts，减少系统调用)
  long long now_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

  // ---------------------------------------------------------
  // 1. 批量处理 Jobs (Temp Table 策略)
  // ---------------------------------------------------------
  // 目标：利用 COPY 的高速写入 + SQL 的冲突处理能力

  // 1.1 创建临时表 (ON COMMIT DROP 保证事务结束后自动清理)
  tx.exec(
      "CREATE TEMP TABLE tmp_jobs_batch (LIKE public.job INCLUDING DEFAULTS) "
      "ON COMMIT DROP");

  // 1.2 高速写入临时表 (COPY 协议)
  {
    auto job_stream = pqxx::stream_to::table(
        tx, {"tmp_jobs_batch"}, {"job_id", "idempotency_key", "state"});

    for (const auto& item : batch) {
      job_stream.write_values(item.ctx.job_id, item.req.idempotency_key(), "0");
    }
    job_stream.complete();
  }

  // 1.3 搬运到正式表 (处理冲突: ON CONFLICT DO NOTHING)
  tx.exec(R"(
        INSERT INTO public.job (job_id, idempotency_key, state)
        SELECT job_id, idempotency_key, state FROM tmp_jobs_batch
        ON CONFLICT (idempotency_key) DO NOTHING
    )");

  // ---------------------------------------------------------
  // 2. 批量处理 Tasks (直接 COPY 策略)
  // ---------------------------------------------------------
  // Task ID 是 UUID，天然无冲突，直接 stream 写入性能最高
  {
    auto task_stream = pqxx::stream_to::table(
        tx, {"task"},
        {"task_id", "job_id", "natural_id", "func_name", "func_params",
         "priority", "state", "pending_dependencies", "max_retry",
         "retry_count", "timeout_ms", "submit_ts"});

    for (auto& item : batch) {
      // 预计算入度 (In-Degree)
      std::unordered_map<std::string, int> dep_count;
      dep_count.reserve(item.req.edges_size());
      for (const auto& edge : item.req.edges()) {
        dep_count[edge.child_natural_id()]++;
      }

      for (const auto& task : item.req.tasks()) {
        const std::string& uuid =
            item.ctx.natural_to_uuid.at(task.natural_id());

        int pending = dep_count[task.natural_id()];  // 默认为0
        int initial_state = (pending == 0) ? 0 : 6;  // 0=PENDING, 6=WAITING

        // 记录 Entry Task (如有需要)
        if (pending == 0) {
          item.ctx.entry_task_ids.push_back(uuid);
        }

        task_stream.write_values(uuid,                // task_id
                                 item.ctx.job_id,     // job_id
                                 task.natural_id(),   // natural_id
                                 task.func_name(),    // func_name
                                 task.func_params(),  // func_params
                                 task.priority(),     // priority
                                 initial_state,       // state
                                 pending,             // pending_dependencies
                                 task.max_retry(),    // max_retry
                                 0,                   // retry_count
                                 task.timeout_ms(),   // timeout_ms
                                 now_ts               // submit_ts
        );
      }
    }
    task_stream.complete();
  }

  // ---------------------------------------------------------
  // 3. 批量处理 Edges (直接 COPY 策略)
  // ---------------------------------------------------------
  bool has_edges = false;
  for (const auto& item : batch) {
    if (item.req.edges_size() > 0) {
      has_edges = true;
      break;
    }
  }

  if (has_edges) {
    auto edge_stream = pqxx::stream_to::table(
        tx, {"task_edge"}, {"parent_task_id", "child_task_id"});

    for (const auto& item : batch) {
      for (const auto& edge : item.req.edges()) {
        // 直接使用 UUID 映射
        edge_stream.write_values(
            item.ctx.natural_to_uuid.at(edge.parent_natural_id()),
            item.ctx.natural_to_uuid.at(edge.child_natural_id()));
      }
    }
    edge_stream.complete();
  }

  // 注意：事务提交权交给上层 ProcessBatch，此处不 commit
}

// =========================================================
// 消息队列实现细节 (Redis Pipeline)
// =========================================================

void TaskSubmitter::AppendDagToPipeline(
    sw::redis::Pipeline& pipe, const dts::service::SubmitDagRequest& proto_req,
    const DagCommitContext& ctx) {
  if (ctx.job_id.empty()) return;

  const auto TTL = std::chrono::hours(24);

  // ---------------------------------------------------------
  // Phase 1: 处理边 (Edges) - 构建依赖关系
  // ---------------------------------------------------------
  std::unordered_map<std::string, int> dependency_map;
  dependency_map.reserve(proto_req.tasks_size());

  // 避免对同一个 Parent 重复发送 Expire 指令
  std::unordered_set<std::string> processed_parents;

  for (const auto& edge : proto_req.edges()) {
    // 内存计数
    dependency_map[edge.child_natural_id()]++;

    // Redis 指令：记录 Parent -> Children 集合
    const std::string& p_uuid =
        ctx.natural_to_uuid.at(edge.parent_natural_id());
    const std::string& c_uuid = ctx.natural_to_uuid.at(edge.child_natural_id());

    std::string children_key = keys::dag::Children(p_uuid);

    pipe.sadd(children_key, c_uuid);

    if (processed_parents.find(p_uuid) == processed_parents.end()) {
      pipe.expire(children_key, TTL);
      processed_parents.insert(p_uuid);
    }
  }

  // ---------------------------------------------------------
  // Phase 2: 处理任务 (Tasks) - 元数据与状态初始化
  // ---------------------------------------------------------
  std::vector<std::vector<std::pair<std::string, std::string>>>
      xadd_args_holder;
  xadd_args_holder.reserve(proto_req.tasks_size() / 2);

  for (const auto& client_task : proto_req.tasks()) {
    const std::string& uuid = ctx.natural_to_uuid.at(client_task.natural_id());

    // --- A. 存储任务元数据 (ProtoBuf String) ---
    dts::task::Task internal_task;
    internal_task.set_task_id(uuid);
    internal_task.set_job_id(ctx.job_id);
    internal_task.set_natural_id(client_task.natural_id());
    internal_task.set_func_name(client_task.func_name());
    internal_task.set_func_params(client_task.func_params());
    internal_task.set_priority(client_task.priority());
    internal_task.set_max_retry(client_task.max_retry());
    internal_task.set_timeout_ms(client_task.timeout_ms());

    std::string binary_payload;
    internal_task.SerializeToString(&binary_payload);

    pipe.set(keys::dag::TaskMeta(uuid), binary_payload, TTL);

    // --- B. 状态分发 (HSET vs XADD) ---
    int pending = dependency_map[client_task.natural_id()];  // 默认为0

    if (pending > 0) {
      // Case 1: 有依赖 -> 进入 Pending Hash 等待倒计数
      pipe.hset(keys::dag::DependencyHash(ctx.job_id), uuid,
                std::to_string(pending));
    } else {
      // Case 2: 无依赖 -> 直接进入 Ready Stream 队列
      auto fields = BuildXAddFieldsFromProto(client_task, uuid, ctx.job_id);

      // 保持 fields 的生命周期直到 pipe.exec() 执行
      xadd_args_holder.push_back(std::move(fields));
      auto& current_fields = xadd_args_holder.back();

      pipe.xadd(keys::stream::kTasks, "*", current_fields.begin(),
                current_fields.end());
    }
  }

  // 给依赖 Hash 设置过期时间
  if (proto_req.edges_size() > 0) {
    pipe.expire(keys::dag::DependencyHash(ctx.job_id), TTL);
  }
}

// =========================================================
// 外部接口实现 (Public API)
// =========================================================

std::string TaskSubmitter::SubmitDagAsync(
    const dts::service::SubmitDagRequest& proto_req) {
  // 1. 创建批处理单元
  BatchItem item(proto_req);

  // A. 生成 Job UUID
  item.ctx.job_id = dts::common::generate();
  std::string returned_job_id = item.ctx.job_id;

  // B. 生成 Task UUIDs 并建立映射
  item.ctx.natural_to_uuid.reserve(proto_req.tasks_size());
  for (const auto& task : proto_req.tasks()) {
    std::string uuid = dts::common::generate();
    item.ctx.natural_to_uuid[task.natural_id()] = uuid;
  }

  // 4. 入队 (Critical Section)
  bool need_notify = false;

  {
    std::lock_guard<std::mutex> lock(queue_mutex_);

    if (pending_queue_.size() >= MAX_QUEUE_SIZE) {
      // 1. 抛出异常 (背压保护)
      // 生产环境这里可以加个 Counter 监控，统计被限流的次数
      LOG_WARN << "SubmitDagAsync rejected: Queue Full ("
               << pending_queue_.size() << ")";
      throw std::runtime_error("Server is busy, queue full");
    }

    // 移动语义，零拷贝
    pending_queue_.push_back(std::move(item));

    // 只有当积累到一定量时才去唤醒消费者，减少上下文切换
    if (pending_queue_.size() >= MIN_BATCH_SIZE) {
      need_notify = true;
    }
  }

  if (need_notify) {
    cv_.notify_one();
  }

  return returned_job_id;
}

}  // namespace dts::api_server