// #include <gtest/gtest.h>
// #include <pqxx/pqxx>
// #include "task_repository.h"
// #include "database_pool.h"
// #include "dts/internal/internal_service.pb.h"
// #include "dts/task/task.pb.h"
// #include <cstdlib>

// using dts::common::DatabasePool;
// using dts::scheduler::TaskRepository;

// // 定义一些合法的 UUID 常量，方便测试使用
// const std::string JOB_UUID = "00000000-0000-0000-0000-000000000001";
// const std::string TASK_UUID_1 = "00000000-0000-0000-0000-000000000011";
// const std::string TASK_UUID_2 = "00000000-0000-0000-0000-000000000012";
// const std::string TASK_UUID_3 = "00000000-0000-0000-0000-000000000013";
// const std::string TASK_UUID_4 = "00000000-0000-0000-0000-000000000014";

// class TaskRepositoryTest : public ::testing::Test {
//  protected:
//   static std::shared_ptr<DatabasePool> db_pool;
//   static std::shared_ptr<TaskRepository> repo;

//   static void SetUpTestSuite() {
//     const char* env_url = std::getenv("DATABASE_URL");
//     std::string db_url =
//         env_url ? env_url : "postgresql://sakura:password@localhost/dts";
//     db_pool = std::make_shared<DatabasePool>(db_url, 2);
//     repo = std::make_shared<TaskRepository>(db_pool);
//   }

//   void SetUp() override {
//     // [修正] 表名改为 task_edge
//     db_pool->ExecuteTx([](pqxx::work& tx) {
//       tx.exec("TRUNCATE TABLE task_edge, task, job RESTART IDENTITY
//       CASCADE");
//     });
//   }

//   // --- 辅助函数：插入数据 ---
//   void InsertTask(pqxx::work& tx, const std::string& id,
//                   const std::string& jobId, int state, int pending_deps = 0,
//                   int max_retry = 3, int retry_count = 0) {
//     // 注意：natural_id 可以是普通字符串，但 task_id 必须是 UUID
//     tx.exec_params(R"(
//             INSERT INTO task (task_id, job_id, func_name, state,
//             pending_dependencies, max_retry, retry_count, priority,
//             natural_id, func_params) VALUES ($1::uuid, $2::uuid, 'test_func',
//             $3, $4, $5, $6, 10, 'node_' || $1, '{}')
//         )",
//                    id, jobId, state, pending_deps, max_retry, retry_count);
//   }

//   void InsertJob(pqxx::work& tx, const std::string& id) {
//     // state=1 (RUNNING)
//     tx.exec_params(
//         "INSERT INTO job (job_id, state, idempotency_key) VALUES ($1::uuid,
//         1, "
//         "'key_' || $1)",
//         id);
//   }

//   void InsertEdge(pqxx::work& tx, const std::string& jobId,
//                   const std::string& parent, const std::string& child) {
//     // [修正] 表名 task_edge
//     tx.exec_params(
//         "INSERT INTO task_edge (parent_task_id, child_task_id) VALUES "
//         "($1::uuid, $2::uuid)",
//         parent, child);
//   }
// };

// std::shared_ptr<DatabasePool> TaskRepositoryTest::db_pool;
// std::shared_ptr<TaskRepository> TaskRepositoryTest::repo;

// // ----------------------------------------------------------------
// // 测试 1: 拉取待调度任务
// // ----------------------------------------------------------------
// TEST_F(TaskRepositoryTest, GetPendingTasks) {
//   db_pool->ExecuteTx([this](pqxx::work& tx) {
//     InsertJob(tx, JOB_UUID);
//     InsertTask(tx, TASK_UUID_1, JOB_UUID, 0);  // PENDING
//     InsertTask(tx, TASK_UUID_2, JOB_UUID, 1);  // RUNNING
//     InsertTask(tx, TASK_UUID_3, JOB_UUID, 6);  // WAITING
//     InsertTask(tx, TASK_UUID_4, JOB_UUID, 0);  // PENDING
//   });

//   auto tasks = repo->GetPendingTasks(10);

//   ASSERT_EQ(tasks.size(), 2);
//   // 验证取到的是不是 1 和 4
//   bool found_1 = false, found_4 = false;
//   for (const auto& t : tasks) {
//     if (t.task_id == TASK_UUID_1) found_1 = true;
//     if (t.task_id == TASK_UUID_4) found_4 = true;
//   }
//   EXPECT_TRUE(found_1);
//   EXPECT_TRUE(found_4);
// }

// // ----------------------------------------------------------------
// // 测试 2: 抢占任务
// // ----------------------------------------------------------------
// TEST_F(TaskRepositoryTest, UpdateTaskToRunning) {
//   db_pool->ExecuteTx([this](pqxx::work& tx) {
//     InsertJob(tx, JOB_UUID);
//     InsertTask(tx, TASK_UUID_1, JOB_UUID, 0);
//   });

//   bool success = repo->UpdateTaskToRunning(TASK_UUID_1, "worker-01");
//   EXPECT_TRUE(success);

//   db_pool->ExecuteTx([](pqxx::work& tx) {
//     auto r = tx.exec_params(
//         "SELECT state, worker_id FROM task WHERE task_id=$1::uuid",
//         TASK_UUID_1);
//     EXPECT_EQ(r[0]["state"].as<int>(), 1);  // RUNNING
//     EXPECT_EQ(r[0]["worker_id"].as<std::string>(), "worker-01");
//   });

//   bool success2 = repo->UpdateTaskToRunning(TASK_UUID_1, "worker-02");
//   EXPECT_FALSE(success2);
// }

// // ----------------------------------------------------------------
// // 测试 3: DAG 流转 (A -> B)
// // ----------------------------------------------------------------
// TEST_F(TaskRepositoryTest, HandleSuccess_PropagateDag) {
//   // 场景：Task 1 -> Task 2
//   db_pool->ExecuteTx([this](pqxx::work& tx) {
//     InsertJob(tx, JOB_UUID);
//     InsertTask(tx, TASK_UUID_1, JOB_UUID, 1);     // A: RUNNING
//     InsertTask(tx, TASK_UUID_2, JOB_UUID, 6, 1);  // B: WAITING, deps=1
//     InsertEdge(tx, JOB_UUID, TASK_UUID_1, TASK_UUID_2);
//   });

//   dts::internal::UpdateTaskStatusRequest req;
//   req.set_task_id(TASK_UUID_1);
//   req.set_final_state(dts::task::SUCCESS);
//   req.set_result_json("{}");

//   bool success = repo->HandleTaskCompletion(&req);
//   EXPECT_TRUE(success);

//   db_pool->ExecuteTx([](pqxx::work& tx) {
//     // 验证 B (Task 2) 变 PENDING
//     auto r = tx.exec_params(
//         "SELECT state, pending_dependencies FROM task WHERE
//         task_id=$1::uuid", TASK_UUID_2);
//     EXPECT_EQ(r[0]["state"].as<int>(), 0);
//     EXPECT_EQ(r[0]["pending_dependencies"].as<int>(), 0);

//     // 验证 A (Task 1) 变 SUCCESS
//     auto r_a = tx.exec_params("SELECT state FROM task WHERE
//     task_id=$1::uuid",
//                               TASK_UUID_1);
//     EXPECT_EQ(r_a[0]["state"].as<int>(), 2);
//   });
// }

// // ----------------------------------------------------------------
// // 测试 4: 失败重试
// // ----------------------------------------------------------------
// TEST_F(TaskRepositoryTest, HandleFailure_Retry) {
//   db_pool->ExecuteTx([this](pqxx::work& tx) {
//     InsertJob(tx, JOB_UUID);
//     InsertTask(tx, TASK_UUID_1, JOB_UUID, 1, 0, 3, 0);  // max=3, cur=0
//   });

//   dts::internal::UpdateTaskStatusRequest req;
//   req.set_task_id(TASK_UUID_1);
//   req.set_final_state(dts::task::FAILED);
//   req.set_error_msg("Error");
//   req.set_result_json("{}");

//   bool success = repo->HandleTaskCompletion(&req);
//   EXPECT_TRUE(success);

//   db_pool->ExecuteTx([](pqxx::work& tx) {
//     auto r = tx.exec_params(
//         "SELECT state, retry_count FROM task WHERE task_id=$1::uuid",
//         TASK_UUID_1);
//     EXPECT_EQ(r[0]["state"].as<int>(), 0);  // PENDING
//     EXPECT_EQ(r[0]["retry_count"].as<int>(), 1);
//   });
// }

// // ----------------------------------------------------------------
// // 测试 5: 僵尸任务恢复
// // ----------------------------------------------------------------
// TEST_F(TaskRepositoryTest, RequeueOrphanedTasks) {
//   db_pool->ExecuteTx([this](pqxx::work& tx) {
//     InsertJob(tx, JOB_UUID);
//     // 死任务 (Task 1)
//     tx.exec_params(
//         "INSERT INTO task (task_id, job_id, state, worker_id, func_name, "
//         "natural_id, func_params) VALUES ($1::uuid, $2::uuid, 1, "
//         "'dead-worker', 'f', 'node_dead1', '{}')",
//         TASK_UUID_1, JOB_UUID);
//     // 活任务 (Task 2)
//     tx.exec_params(
//         "INSERT INTO task (task_id, job_id, state, worker_id, func_name, "
//         "natural_id, func_params) VALUES ($1::uuid, $2::uuid, 1, "
//         "'alive-worker', 'f', 'node_dead2', '{}')",
//         TASK_UUID_2, JOB_UUID);
//   });

//   int count = repo->RequeueOrphanedTasks("dead-worker");
//   EXPECT_EQ(count, 1);

//   db_pool->ExecuteTx([](pqxx::work& tx) {
//     auto r1 = tx.exec_params("SELECT state FROM task WHERE task_id=$1::uuid",
//                              TASK_UUID_1);
//     EXPECT_EQ(r1[0]["state"].as<int>(), 0);  // Reset

//     auto r2 = tx.exec_params("SELECT state FROM task WHERE task_id=$1::uuid",
//                              TASK_UUID_2);
//     EXPECT_EQ(r2[0]["state"].as<int>(), 1);  // Unchanged
//   });
// }

// TEST_F(TaskRepositoryTest, DiamondDependency) {
//   // 1. 构造菱形图
//   // A (RUNNING)
//   // B (WAITING, 1)
//   // C (WAITING, 1)
//   // D (WAITING, 2) <-- 重点
//   db_pool->ExecuteTx([this](pqxx::work& tx) {
//     InsertJob(tx, JOB_UUID);
//     InsertTask(tx, TASK_UUID_1, JOB_UUID, 1);     // A
//     InsertTask(tx, TASK_UUID_2, JOB_UUID, 6, 1);  // B
//     InsertTask(tx, TASK_UUID_3, JOB_UUID, 6, 1);  // C
//     InsertTask(tx, TASK_UUID_4, JOB_UUID, 6, 2);  // D

//     InsertEdge(tx, JOB_UUID, TASK_UUID_1, TASK_UUID_2);  // A->B
//     InsertEdge(tx, JOB_UUID, TASK_UUID_1, TASK_UUID_3);  // A->C
//     InsertEdge(tx, JOB_UUID, TASK_UUID_2, TASK_UUID_4);  // B->D
//     InsertEdge(tx, JOB_UUID, TASK_UUID_3, TASK_UUID_4);  // C->D
//   });

//   // 2. A 完成 -> B, C 应该激活 (Pending=0)
//   dts::internal::UpdateTaskStatusRequest req;
//   req.set_task_id(TASK_UUID_1);
//   req.set_final_state(dts::task::SUCCESS);
//   req.set_result_json("{}");
//   repo->HandleTaskCompletion(&req);

//   // 验证 B, C 状态
//   db_pool->ExecuteTx([](pqxx::work& tx) {
//     auto r = tx.exec_params(
//         "SELECT state FROM task WHERE task_id IN ($1::uuid, $2::uuid)",
//         TASK_UUID_2, TASK_UUID_3);
//     EXPECT_EQ(r[0][0].as<int>(), 0);  // PENDING
//     EXPECT_EQ(r[1][0].as<int>(), 0);  // PENDING
//   });

//   // 3. 模拟 B 完成 -> D 的 pending 应该从 2 变 1 (依然 WAITING)
//   req.set_task_id(TASK_UUID_2);
//   repo->HandleTaskCompletion(&req);

//   db_pool->ExecuteTx([](pqxx::work& tx) {
//     auto r = tx.exec_params(
//         "SELECT state, pending_dependencies FROM task WHERE
//         task_id=$1::uuid", TASK_UUID_4);
//     EXPECT_EQ(r[0]["state"].as<int>(), 6);                 // 依然 WAITING
//     EXPECT_EQ(r[0]["pending_dependencies"].as<int>(), 1);  // 剩 1 个依赖
//   });

//   // 4. 模拟 C 完成 -> D 应该激活
//   req.set_task_id(TASK_UUID_3);
//   repo->HandleTaskCompletion(&req);

//   db_pool->ExecuteTx([](pqxx::work& tx) {
//     auto r = tx.exec_params(
//         "SELECT state, pending_dependencies FROM task WHERE
//         task_id=$1::uuid", TASK_UUID_4);
//     EXPECT_EQ(r[0]["state"].as<int>(), 0);  // 终于 PENDING 了！
//     EXPECT_EQ(r[0]["pending_dependencies"].as<int>(), 0);
//   });
// }

// // ----------------------------------------------------------------
// // [新增] 测试 8: 状态机安全 (State Safety)
// // 防止已完成的任务被重复修改
// // ----------------------------------------------------------------
// TEST_F(TaskRepositoryTest, StateSafety_PreventIllegalTransition) {
//   db_pool->ExecuteTx([this](pqxx::work& tx) {
//     InsertJob(tx, JOB_UUID);
//     // 任务已经是 SUCCESS (2)
//     InsertTask(tx, TASK_UUID_1, JOB_UUID, 2);
//   });

//   // 尝试将其抢占为 RUNNING
//   // 你的 UpdateTaskToRunning SQL 应该有 WHERE state = 0 的判断
//   bool success = repo->UpdateTaskToRunning(TASK_UUID_1, "worker-new");

//   EXPECT_FALSE(success) << "Should not be able to run a finished task";
// }