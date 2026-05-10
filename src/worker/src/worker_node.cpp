#include "worker_node.h"
#include "logger.hpp"
#include <thread>
#include <chrono>
#include <nlohmann/json.hpp>

namespace dts {
namespace worker {

WorkerNode::WorkerNode(const std::string& worker_id,
                       const std::string& bind_addr,
                       const std::string& scheduler_address,
                       const std::string& advertise_addr)
    : worker_id_(worker_id),
      bind_addr_(bind_addr),
      running_(false),
      advertise_addr_(advertise_addr) {
  auto channel = grpc::CreateChannel(scheduler_address,
                                     grpc::InsecureChannelCredentials());
  scheduler_client_ = std::make_unique<SchedulerClient>(channel);

  unsigned int cores = std::thread::hardware_concurrency();
  thread_pool_ =
      std::make_unique<dts::common::ThreadPool>(cores > 0 ? cores : 4);

  service_impl_ = std::make_unique<WorkerServiceImpl>(
      [this](const dts::Task& task) {  // 注意：这里使用了你的 struct dts::Task
        this->OnTaskReceived(task);
      });
}

WorkerNode::~WorkerNode() { Stop(); }

void WorkerNode::Start() {
  if (running_) return;
  running_ = true;

  grpc::ServerBuilder builder;
  builder.AddListeningPort(bind_addr_, grpc::InsecureServerCredentials());
  builder.RegisterService(service_impl_.get());
  grpc_server_ = builder.BuildAndStart();
  LOG_INFO << "Worker gRPC Server listening on " << bind_addr_;

  if (!scheduler_client_->RegisterWorker(worker_id_, advertise_addr_)) {
    LOG_FATAL << "Failed to register worker to scheduler! Exiting...";
    exit(1);
  }

  heartbeat_thread_ = std::thread(&WorkerNode::HeartbeatLoop, this);
}

void WorkerNode::Await() {
  if (grpc_server_) {
    grpc_server_->Wait();
  }
}

void WorkerNode::Stop() {
  if (!running_) return;
  running_ = false;

  LOG_INFO << "Worker stopping...";

  if (grpc_server_) {
    grpc_server_->Shutdown();
  }

  if (heartbeat_thread_.joinable()) {
    heartbeat_thread_.join();
  }
}

// =================================================================
// 核心逻辑：执行器 (Executor Logic)
// =================================================================
void WorkerNode::OnTaskReceived(const dts::Task& task) {
  // 1. 查找任务处理函数
  // [修正] 使用 .func_name 而不是 .name()
  auto handler = TaskRegistry::GetInstance().Get(task.func_name);

  if (!handler) {
    LOG_ERROR << "Task implementation not found: " << task.func_name;

    // [修正] 使用 task.task_id 和 dts::TaskState
    // 注意：SchedulerClient 还需要适配，这里假设 SchedulerClient 已接受
    // dts::TaskState 如果 SchedulerClient 还是用的 Proto
    // Enum，我们需要强转，或者修改 Client 定义 暂时假设 UpdateTaskStatus
    // 接受的是你的 dts::TaskState
    scheduler_client_->UpdateTaskStatus(
        task.task_id, dts::TaskState::FAILED, task.job_id,
        "Worker does not have this task function registered", worker_id_);
    return;
  }

  // 2. 提交到线程池
  try {
    // [修正] 捕获 handler 和 task
    // 这里的 task 是值拷贝，确保线程安全
    thread_pool_->enqueue([this, task, handler]() {
      // [修正] 使用 .task_id
      dts::SetRequestId(task.task_id);
      LOG_INFO << "Start executing task: " << task.func_name;

      dts::TaskState final_state = dts::TaskState::FAILED;
      std::string result_json_str;
      std::string error_msg;

      try {
        // A. 执行业务逻辑
        // [修正] task.func_params 是 json 对象，需要 dump() 成 string 传给
        // handler 假设 handler 签名是 string(string)
        std::string params_str = task.func_params.dump();
        result_json_str = handler(params_str);

        final_state = dts::TaskState::SUCCESS;
        LOG_INFO << "Task execution finished successfully.";

      } catch (const std::exception& e) {
        error_msg = e.what();
        final_state = dts::TaskState::FAILED;
        LOG_ERROR << "Task execution failed: " << e.what();
      } catch (...) {
        error_msg = "Unknown exception";
        final_state = dts::TaskState::FAILED;
        LOG_ERROR << "Task execution failed with unknown error";
      }

      // C. 向 Scheduler 汇报结果
      scheduler_client_->UpdateTaskStatus(
          task.task_id, final_state, task.job_id, error_msg, result_json_str);
    });
  } catch (const std::exception& e) {
    LOG_ERROR << "Failed to enqueue task: " << e.what();
    scheduler_client_->UpdateTaskStatus(
        task.task_id, dts::TaskState::FAILED, task.job_id,
        std::string("Enqueue failed: ") + e.what(), worker_id_);
  }
}

void WorkerNode::HeartbeatLoop() {
  dts::SetRequestId("hb-" + worker_id_);
  LOG_INFO << "Heartbeat loop started.";

  while (running_) {
    // TODO: 后续可以对接真实的 Metrics
    float cpu_usage = 0.5f;
    // int running_tasks = thread_pool_->active_tasks(); // 如果有这个接口最好
    int running_tasks = 0;

    // ============================================================
    // [核心修改] 增加返回值检查与重注册逻辑
    // ============================================================

    // 1. 发送心跳，并获取结果
    // 注意：请确保 scheduler_client_->SendHeartbeat 在遇到 RPC 错误
    // 或者 Server 返回 "Worker not found" 时返回 false
    bool hb_success =
        scheduler_client_->SendHeartbeat(worker_id_, running_tasks, cpu_usage);

    if (!hb_success) {
      LOG_WARN << "[Self-Healing] Heartbeat failed! Scheduler may have evicted "
                  "this worker. "
               << "Attempting to re-register...";

      // 2. 尝试重新注册
      // 这里使用启动时保存的 advertise_addr_
      bool reg_success =
          scheduler_client_->RegisterWorker(worker_id_, advertise_addr_);

      if (reg_success) {
        LOG_INFO << "✅ [Self-Healing] Worker re-registered successfully. "
                    "Connection restored.";
        // 可选：立即补发一次心跳，或者等下一次循环
      } else {
        LOG_ERROR << "❌ [Self-Healing] Re-registration failed. Scheduler "
                     "might be down or unreachable.";
        // 此时不要 exit，继续保持循环，直到 Scheduler 恢复
      }
    }

    // ============================================================

    std::this_thread::sleep_for(std::chrono::seconds(5));
  }

  LOG_INFO << "Heartbeat loop stopped.";
}

}  // namespace worker
}  // namespace dts