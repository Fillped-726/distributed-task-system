# DTS V2.0 重构计划

> 目标：基于 ADR 001~004 的架构决策，从零开始重构分布式任务调度系统。
> 策略：领域先行，推倒重写。不兼容 V1.0 代码。

---

## 目录骨架

```
distributed-task-system/
├── CMakePresets.json          # vcpkg + Debug/Release/ASan
├── vcpkg.json                 # manifest 依赖
├── PLan.md                    # 本文件
├── docs/
│   └── DTS_STATUS.md          # 项目现状（给 AI 看）
├── adr/                       # 架构决策记录
│   ├── 001-event-driven-scheduler.md
│   ├── 002-architecture-topology.md
│   ├── 003-concurrency-and-worker-isolation.md
│   └── 004-api-and-persistence.md
│
├── src/
│   ├── CMakeLists.txt         # 顶层构建
│   │
│   ├── scheduler/             # 【核心】调度器
│   │   ├── CMakeLists.txt
│   │   ├── domain/            # 🛡️ 护城河禁区（纯函数，无锁无IO）
│   │   │   ├── event.hpp         # 事件类型 (DagSubmitted, TaskSuccess, ...)
│   │   │   ├── command.hpp       # 命令类型 (DispatchTask, UpdateDb, ...)
│   │   │   ├── state.hpp         # DAG 状态定义
│   │   │   ├── state_machine.hpp # 纯函数状态机
│   │   │   └── state_machine.cpp
│   │   ├── engine/            # Event Loop + MPSC 队列
│   │   │   ├── event_bus.hpp     # IEventBus 接口 (MPSC封装)
│   │   │   ├── event_loop.hpp    # Event Loop 主循环
│   │   │   └── event_loop.cpp
│   │   ├── adapter/           # 基础设施适配器（IO 边界）
│   │   │   ├── http_server.hpp   # HTTP 服务 (drogon / cpp-httplib)
│   │   │   ├── redis_adapter.hpp # Redis Stream 读写
│   │   │   ├── pg_adapter.hpp    # PostgreSQL 异步写入
│   │   │   └── worker_dispatcher.hpp # IWorkerDispatcher 实现
│   │   └── main.cpp
│   │
│   ├── worker/                # 【核心】执行节点
│   │   ├── CMakeLists.txt
│   │   ├── main.cpp             # 主进程：Redis 连接 + fork
│   │   └── sandbox/             # 子进程执行沙箱
│   │       ├── runner.cpp       # 子进程入口：执行 + pipe 回传
│   │       └── task_registry.hpp
│   │
│   ├── common/                # 共享基础设施
│   │   ├── CMakeLists.txt
│   │   ├── types.hpp            # 公共类型
│   │   └── logging.hpp          # 日志封装
│   │
│   └── dts-cli/               # 【控制面】Go CLI（cobra + protobuf 跨语言）
│       ├── go.mod
│       ├── main.go
│       ├── cmd/                # submit, bench, watch, get
│       ├── pkg/                # client, dag/validator, parser
│       └── proto/              # .pb.go 生成文件
│
└── tests/                     # 测试
    ├── CMakeLists.txt
    ├── scheduler/
    │   └── state_machine_test.cpp
    └── worker/
```

---

## 依赖清单 (vcpkg.json)

```json
{
  "name": "dts",
  "version": "2.0.0",
  "dependencies": [
    "fmt",
    "nlohmann-json",
    "gtest",
    "redis-plus-plus",
    "hiredis",
    "libpqxx",
    "boost-asio",
    "moodycamel-concurrentqueue"
  ]
}
```

> HTTP 框架暂不引入，Phase 1 用 stdin/stdout 模拟 API 请求。
> 后续根据选型（drogon / cpp-httplib / Boost.Beast）再添加。

---

## Phase 1：领域层核心（纯函数状态机）

**目标**：不依赖任何外部依赖（除 C++20 标准库），写完就能编译、能跑单测。

### 任务清单

| # | 任务 | 产出 | 验收标准 |
|---|------|------|---------|
| 1.1 | 定义 Event 类型 | `event.hpp` | 包含 DagSubmitted, TaskSuccess, TaskFailed, WorkerTimeout, NodeCrash |
| 1.2 | 定义 Command 类型 | `command.hpp` | 包含 UpdateDagState, DispatchTask, UpdateDb, AckStream |
| 1.3 | 定义 DAG 状态结构 | `state.hpp` | 节点状态（Pending/Running/Success/Failed）、入度表、DAG 拓扑 |
| 1.4 | 实现状态机 | `state_machine.hpp/cpp` | 输入 Event → 更新 State → 输出 Command |
| 1.5 | 状态机单测 | `state_machine_test.cpp` | 覆盖：提交 DAG、完成单人务触发子任务、任务失败、环检测 |

**关键约束**：
- domain/ 目录不允许出现 `std::mutex` / `std::thread` / `std::future`
- 状态机方法签名必须是 `Process(Event) -> std::vector<Command>`

### Phase 1 不做的
- ❌ Redis/PG 连接
- ❌ HTTP 服务
- ❌ Worker 进程
- ❌ 任何 IO 或并发

---

## Phase 2：Scheduler 外壳（EventBus + Redis 适配）

**目标**：状态机能接真实数据跑起来。

| # | 任务 | 产出 | 验收标准 |
|---|------|------|---------|
| 2.1 | CMakePresets + vcpkg | `CMakePresets.json`, `vcpkg.json` | `cmake --preset debug` 成功 |
| 2.2 | EventBus（MPSC 队列） | `event_bus.hpp` | 多线程 push / 单线程 drain，无锁 |
| 2.3 | Event Loop 主循环 | `event_loop.hpp/cpp` | Drain → Process → ExecuteCommands |
| 2.4 | Redis Adapter（Stream 读写） | `redis_adapter.hpp/cpp` | XReadGroup / XAck / XADD 封装 |
| 2.5 | PG Adapter（Write-Behind） | `pg_adapter.hpp/cpp` | 双缓冲队列 + COPY 协议写入 |
| 2.6 | WorkerDispatcher（Stream 实现） | `worker_dispatcher.hpp/cpp` | 将 Command 转为 Redis Stream XADD |
| 2.7 | HTTP 服务 | `http_server.hpp/cpp` | 接收 JSON SubmitDag → 转 Event → 入 EventBus |
| 2.8 | 端到端集成测试 | — | 一条命令：提交 DAG → 打印调度过程 → 打印执行结果 |

### Phase 2 不做的
- ❌ Worker fork 沙箱（用 sleep 1 秒模拟执行）
- ❌ CLI 工具
- ❌ Active-Standby 主备切换

---

## Phase 3：Worker 执行隔离 + CLI

| # | 任务 | 产出 | 验收标准 |
|---|------|------|---------|
| 3.1 | Worker 主进程（Pull 模式） | `worker/main.cpp` | XREADGROUP 拉取任务 → fork → 回传结果 |
| 3.2 | fork 子进程执行沙箱 | `worker/sandbox/runner.cpp` | 子进程执行 + pipe 回传 + SIGKILL 超时 |
|| 3.3 | Go CLI | `dts-cli/` | cobra 子命令：submit, bench, watch, get；Kahn 算法环检测 |

---

## Phase 4：高可用 + 可观测性

| # | 任务 | 产出 |
|---|------|------|
| 4.1 | Scheduler Active-Standby | Redis SETNX 选主 + Follower 待命 |
| 4.2 | XCLAIM 故障自愈 | 僵尸任务检测 + 回收 |
| 4.3 | 启动恢复（快照 + 事件回放） | 从 PG/Redis 加载活跃 DAG |

---

## 首批编码指令（修正版）

> 关键纠正：基建先行半步。先搭 vcpkg + CMakePresets，再写领域层。
> 这样 GTest 是秒级可用的，不需要 FetchContent 编译半小时。

打开 IDE 后，按顺序做：

```
Step 0: 钉死基建
  0.1 写 vcpkg.json（只装 gtest 和 fmt，其他后续 Phase 2 再加）
  0.2 写 CMakePresets.json（配置 vcpkg toolchain 路径）
  0.3 写顶层 CMakeLists.txt（add_subdirectory 到 scheduler 和 tests）

Step 1: 划定禁区
  1.1 mkdir -p src/scheduler/domain/
  1.2 mkdir -p tests/scheduler/

Step 2: 领域建模（现代 C++ ADT）
  2.1 写 event.hpp — Event 类型（std::variant + std::visit 模式匹配）
  2.2 写 command.hpp — Command 类型
  2.3 写 state.hpp — DAG 状态定义（入度表、节点状态）

Step 3: 状态机 + TDD
  3.1 写 state_machine_test.cpp — 先写测试（#include <gtest/gtest.h>）
      覆盖：提交 DAG、完成任务触发子任务、任务失败、环检测
  3.2 写 state_machine.hpp + state_machine.cpp — 实现 Process()
  3.3 cmake --preset debug && cmake --build build/debug
  3.4 ctest --test-dir build/debug — 全绿通过
```

> 注：Phase 1 不要求编译 gtest 以外的任何第三方库。
> 然后进入 Phase 2 时再往 vcpkg.json 追加 redis-plus-plus、libpqxx 等。
