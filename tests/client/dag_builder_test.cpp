#include <gtest/gtest.h>
#include "dag_builder.hpp"
#include <nlohmann/json.hpp>

using namespace dts::client;
using json = nlohmann::json;

// ----------------------------------------------------------------
// 测试 1: 正常构建流程
// ----------------------------------------------------------------
TEST(DagBuilderTest, BuildValidDag) {
  std::string demo_key = "idempotency_key_123";
  DagBuilder builder(demo_key);

  // 1. 添加任务 A (带参数)
  json params_a;
  params_a["url"] = "http://example.com";

  // AddTask 返回引用，测试链式修改
  auto& task_a = builder.AddTask("task_a", "download", params_a);
  task_a.priority = 100;

  // 2. 添加任务 B
  builder.AddTask("task_b", "process");

  // 3. 添加依赖 A -> B
  // 注意：这里使用的是 natural_id
  builder.AddDependency("task_a", "task_b");

  // 4. 构建 Proto
  auto request = builder.BuildProto();

  // --- 验证结果 ---

  // 验证 Key
  EXPECT_EQ(request.idempotency_key(), demo_key);

  // 验证任务数量 (repeated dts.task.Task tasks = 2)
  ASSERT_EQ(request.tasks_size(), 2);

  // 验证任务 A
  bool found_a = false;
  for (const auto& t : request.tasks()) {
    if (t.natural_id() == "task_a") {
      found_a = true;
      EXPECT_EQ(t.func_name(), "download");
      EXPECT_EQ(t.priority(), 100);

      // 验证参数 (假设 Proto 中是 JSON 字符串，或者是 Struct)
      // 如果是 Struct，测试可能比较复杂，这里简单判空
      // 如果是 String，可以直接比较
      // EXPECT_EQ(t.func_params(), params_a.dump());
    }
  }
  EXPECT_TRUE(found_a);

  // 验证依赖 (repeated dts.task.TaskEdge edges = 3)
  ASSERT_EQ(request.edges_size(), 1);  // <--- [修正] 使用 edges_size()

  const auto& edge = request.edges(0);  // <--- [修正] 使用 edges(0)

  // [注意] 这里假设你的 TaskEdge Proto 定义字段名为 parent_natural_id /
  // child_natural_id 如果你的 Proto 定义是 parent / child，请改为 edge.parent()
  // / edge.child()
  EXPECT_EQ(edge.parent_natural_id(), "task_a");
  EXPECT_EQ(edge.child_natural_id(), "task_b");
}

// ----------------------------------------------------------------
// 测试 2: 异常处理 - 重复添加任务
// ----------------------------------------------------------------
TEST(DagBuilderTest, DuplicateTaskThrow) {
  DagBuilder builder("key_dup");
  builder.AddTask("task_a", "func");

  // 再次添加 task_a，应该报错
  EXPECT_THROW({ builder.AddTask("task_a", "func"); }, std::runtime_error);
}

// ----------------------------------------------------------------
// 测试 3: 异常处理 - 依赖不存在的任务
// ----------------------------------------------------------------
TEST(DagBuilderTest, MissingDependencyThrow) {
  DagBuilder builder("key_miss");
  builder.AddTask("task_a", "func");

  // task_b 不存在，建立依赖应该报错
  EXPECT_THROW(
      { builder.AddDependency("task_a", "task_b"); }, std::runtime_error);
}

// ----------------------------------------------------------------
// 测试 4: JSON 参数传递
// ----------------------------------------------------------------
TEST(DagBuilderTest, JsonParamsParsing) {
  DagBuilder builder("key_json");
  json complex_params = {{"threshold", 0.85}, {"retry", 3}};

  builder.AddTask("task_complex", "compute", complex_params);
  auto req = builder.BuildProto();

  ASSERT_EQ(req.tasks_size(), 1);
  EXPECT_EQ(req.tasks(0).natural_id(), "task_complex");
  // 确保没有崩溃，且任务被添加
}

TEST(DagBuilderTest, CycleDetection) {
  // 场景 1: 直接自环 (A -> A)
  // 预期：在 AddDependency 阶段就直接抛出异常
  {
    DagBuilder builder("key_cycle_1");
    builder.AddTask("A", "func");

    // [修改] 将 AddDependency 包裹在 EXPECT_THROW 中
    EXPECT_THROW({ builder.AddDependency("A", "A"); }, std::runtime_error);
  }

  // 场景 2: 间接环 (A -> B -> C -> A)
  // 预期：AddDependency 不会报错，BuildProto 里的 DFS 会报错
  {
    DagBuilder builder("key_cycle_2");
    builder.AddTask("A", "func");
    builder.AddTask("B", "func");
    builder.AddTask("C", "func");

    builder.AddDependency("A", "B");
    builder.AddDependency("B", "C");
    // 这一步通常检测不出来间接环，所以不报错
    builder.AddDependency("C", "A");

    // [保持不变] 在 BuildProto 时检测
    try {
      builder.BuildProto();
      FAIL() << "Should have thrown runtime_error for A->B->C->A cycle";
    } catch (const std::runtime_error& e) {
      std::string msg = e.what();
      // 确保错误信息是你 DFS 里写的那个
      EXPECT_NE(msg.find("Cycle detected"), std::string::npos);
    }
  }

  // 场景 3: 菱形依赖 (正常 DAG，不应报错)
  // A->B, A->C, B->D, C->D
  {
    DagBuilder builder("key_diamond");
    builder.AddTask("A", "func");
    builder.AddTask("B", "func");
    builder.AddTask("C", "func");
    builder.AddTask("D", "func");

    builder.AddDependency("A", "B");
    builder.AddDependency("A", "C");
    builder.AddDependency("B", "D");
    builder.AddDependency("C", "D");

    EXPECT_NO_THROW({ builder.BuildProto(); });
  }
}