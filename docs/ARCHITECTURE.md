没问题，我们来构建一份专业、详尽且符合你现有架构（Master-Worker Push 模式 + 4个微服务组件）的 `docs/ARCHITECTURE.md`。

这份文档将作为你项目的**技术骨架**，主要回答“**系统是如何构成的**”以及“**数据是如何流转的**”这两个核心问题。

请直接复制以下内容到 `docs/ARCHITECTURE.md`：

---

# DTS Architecture Design Document

**Version**: 0.4.0
**Last Updated**: 2025-12
**Status**: Stable

## 1. High-Level Design (系统高层设计)

DTS (Distributed Task Scheduling System) 采用了 **Control Plane (控制面)** 与 **Data Plane (数据面)** 分离的设计思想。

* **Control Plane**: 由 `dts-cli` 和 `dts-scheduler` 组成，负责任务编排、拓扑校验、状态仲裁与调度分发。
* **Data Plane**: 由 `dts-portal` 和 `dts-worker` 组成，负责高吞吐的任务接入、数据持久化与计算执行。
* **State Store**: 采用 **Redis** (热数据/消息总线) + **PostgreSQL** (冷数据/持久化) 的分层存储架构。

---

## 2. Component Topology (物理组件拓扑)

系统编译后产出 4 个独立的二进制文件，支持分布式部署与独立扩缩容。

### 2.1 `dts-portal` (Ingestion Gateway)

* **Role**: **流量网关**。
* **Responsibility**:
* 暴露 gRPC `SubmitTask` 接口。
* **IO Batching**: 实现双缓冲队列 (`Double Buffering`)，聚合高频小请求。
* **Dual Write**: 负责将数据批量写入 PostgreSQL (使用 COPY 协议) 并推送至 Redis Pipeline。


* **Scalability**: 无状态，支持水平扩展 (Scale Out)，通过 L4/L7 负载均衡器分发流量。

### 2.2 `dts-scheduler` (Core Brain)

* **Role**: **调度中枢**。
* **Responsibility**:
* **Event Loop**: 通过 `XREADGROUP` 监听 Redis Stream 任务事件。
* **DAG Engine**: 维护任务依赖状态机，执行 Lua 脚本进行原子仲裁。
* **Active Push**: 维护 Worker 注册表与负载视图，**主动发起 gRPC 连接**将任务推送给 Worker。
* **Async Write-Back**: 独立的 `DbBatcher` 线程负责将任务最终状态回写 DB。


* **Deployment**: 通常部署为主备模式 (Primary-Standby) 或分片模式。

### 2.3 `dts-worker` (Execution Node)

* **Role**: **计算节点**。
* **Responsibility**:
* **RPC Server**: 暴露 `ExecuteTask` 接口，等待 Scheduler 调用。
* **Sandbox**: 启动子进程/线程执行具体业务逻辑 (Shell/Python/Binary)。
* **Heartbeat**: 定期向 Scheduler 汇报存活状态。


* **Scalability**: 纯无状态，可配合 K8s HPA 根据 CPU/Mem 指标自动扩容。

### 2.4 `dts-cli` (Control Tool)

* **Role**: **命令行客户端 (Go)**。
* **Responsibility**:
* **Client-side Fail-Fast**: 解析 YAML DAG，本地运行 Kahn 算法检测环路。
* **Benchmarking**: 内置高并发压测器 (`dts bench`)。



---

## 3. Data Architecture (数据架构)

### 3.1 PostgreSQL Schema (Persistence)

PostgreSQL 用于存储全量数据，保证 ACID 事务性。

* `tasks`: 存储任务静态元数据 (Payload, Priority, Timeout)。
* `task_status`: 存储任务运行时状态 (Pending, Running, Success, Failed)。
* `dag_edges`: 存储 DAG 依赖关系 (Parent -> Child)。
* **Optimization**: 仅通过 `COPY (BINARY)` 协议写入，避免 SQL 解析开销。

### 3.2 Redis Data Structures (Hot State)

Redis 承载所有高频的调度逻辑，作为系统的“内存加速层”。

| Key Pattern | Type | Purpose |
| --- | --- | --- |
| `dts:stream:pending` | **Stream** | 就绪任务队列。Scheduler 的消费源。 |
| `dts:task:meta:{id}` | **String** | 任务 Payload 缓存 (Protobuf Bytes)。 |
| `dts:dag:children:{id}` | **Set** | DAG 邻接表，存储该任务的下游子任务 ID。 |
| `dts:dag:indegree:{root_id}` | **Hash** | DAG 入度表。Field 为 TaskID，Value 为剩余依赖数。 |
| `dts:worker:load` | **ZSet** | Worker 负载表。Score 为当前并发数，用于负载均衡。 |

---

## 4. Core Workflows (核心流程)

### 4.1 任务提交与落盘 (Ingestion Flow)

1. **Submit**: User -> `dts-cli` -> gRPC -> `dts-portal`。
2. **Buffering**: `dts-portal` 将请求压入 `ActiveBuffer` (内存 vector)。
3. **Swap & Flush**: 达到阈值 (如 50ms 或 1000条)，交换 `Active`/`Standby` 指针。
4. **Persistence**:
* **Step 1**: 将 Payload 写入 PG `tasks` 表 (COPY)。
* **Step 2**: 将元数据和初始入度写入 Redis Pipeline。
* **Step 3**: 若任务入度为 0 (根任务)，直接 `XADD` 进 Stream。



### 4.2 调度与推送 (Dispatch Flow - Push Model)

1. **Pop**: `dts-scheduler` 通过 `XREADGROUP` 阻塞读取 Stream。
2. **Resolve**: 解析任务元数据。
3. **Load Balance**: 查询内存中的 `WorkerLoadMap`，选择负载最低的 Worker。
4. **Push**:
* Scheduler (Client) -> `ExecuteTask` -> Worker (Server).
* 若 RPC 成功，更新 Redis 中任务状态为 `RUNNING`。
* 若 RPC 失败 (Worker 网络波动)，重试或重新放回队列。



### 4.3 执行与反馈 (Execution & Feedback)

1. **Run**: `dts-worker` 执行业务逻辑。
2. **Report**: Worker 返回 RPC 响应 (Success/Failed) 给 Scheduler。
3. **Trigger**: Scheduler 执行 Lua 脚本：
* 标记当前任务完成。
* 获取所有子任务 (`SMEMBERS dts:dag:children:{id}`)。
* 对每个子任务执行 `HINCRBY -1`。
* 若减为 0，触发 `XADD`，子任务进入就绪状态。



---

## 5. Key Mechanisms (核心机制)

### 5.1 Protobuf Object Passthrough (对象透传)

为了极致优化 CPU，在 `dts-portal` 内部：

* gRPC 接收到的 `SubmitRequest` 对象指针被直接放入内存队列。
* 在写入 PG 时，直接读取 Proto 字段进行序列化。
* **Benefit**: 避免了将 Proto 对象转换为内部 C++ Class (DTO -> DO) 的内存分配与拷贝开销。

### 5.2 Optimistic Load Balancing (乐观负载均衡)

* **Problem**: Redis 中的 Worker 负载状态可能存在延迟。
* **Solution**: Scheduler 维护本地内存影子状态。
* **Pre-book**: 发起 Push 前，`LocalLoad++`。
* **Correction**: 收到 Worker 心跳或任务结束回调时，更新为真实值。


* **Benefit**: 实现了严格的 Round-Robin 分发，防止热点 Worker。

### 5.3 Client-side Fail-Fast (客户端快速失败)

* **Problem**: 环状 DAG 会导致调度死锁或无限循环。
* **Solution**: `dts-cli` (Go) 在提交前构建内存图，运行 **Kahn 算法**。
* **Benefit**: 0 服务端开销拦截非法 DAG。

---

## 6. Directory Structure (源码结构)

```text
.
├── CHANGELOG.md
├── CMakeFiles
│   ├── 3.28.3
│   │   ├── CMakeCXXCompiler.cmake
│   │   ├── CMakeDetermineCompilerABI_CXX.bin
│   │   ├── CMakeSystem.cmake
│   │   └── CompilerIdCXX
│   │       ├── CMakeCXXCompilerId.cpp
│   │       ├── a.out
│   │       └── tmp
│   ├── CMakeConfigureLog.yaml
│   ├── CMakeScratch
│   └── pkgRedirects
├── CMakeLists.txt
├── Dockerfile
├── README.md
├── benchmark_output.log
├── dag_schema.json
├── docker-compose.yml
├── docs
│   ├── ARCHITECTURE.md
│   └── TECHNICAL_DEEP_DIVE.md
├── init.sql
├── prometheus.yml
├── src
│   ├── CMakeLists.txt
│   ├── api-server
│   │   ├── CMakeLists.txt
│   │   ├── include
│   │   │   ├── api_server.hpp
│   │   │   ├── batch_item.hpp
│   │   │   ├── job_query_handler.hpp
│   │   │   └── task_submitter.hpp
│   │   └── src
│   │       ├── api_server.cpp
│   │       ├── job_query_handler.cpp
│   │       ├── main.cpp
│   │       └── task_submitter.cpp
│   ├── api-web
│   │   ├── CMakeLists.txt
│   │   ├── include
│   │   │   └── api_web.hpp
│   │   └── src
│   │       ├── api_web.cpp
│   │       └── main.cpp
│   ├── client
│   │   ├── CMakeLists.txt
│   │   ├── include
│   │   │   ├── async_tags.hpp
│   │   │   ├── dag_builder.hpp
│   │   │   ├── grpc_client.hpp
│   │   │   └── types.hpp
│   │   └── src
│   │       ├── dag_builder.cpp
│   │       └── grpc_client.cpp
│   ├── common
│   │   ├── CMakeLists.txt
│   │   ├── include
│   │   │   ├── converters.hpp
│   │   │   ├── coroutines
│   │   │   │   └── dts_coroutine.h
│   │   │   ├── dag.hpp
│   │   │   ├── database_pool.h
│   │   │   ├── drop
│   │   │   │   ├── hazard_pointer.hpp
│   │   │   │   ├── mpmc_queue.hpp
│   │   │   │   ├── thread_pool.cpp
│   │   │   │   └── thread_pool.hpp
│   │   │   ├── error
│   │   │   │   ├── grpc_error.h
│   │   │   │   └── head_error.h
│   │   │   ├── exceptions.hpp
│   │   │   ├── grpc_base.h
│   │   │   ├── id.hpp
│   │   │   ├── logger.hpp
│   │   │   ├── redis
│   │   │   │   ├── RedisConfig.hpp
│   │   │   │   ├── RedisKeys.hpp
│   │   │   │   ├── RedisManager.hpp
│   │   │   │   └── scripts
│   │   │   │       └── complete_task.lua
│   │   │   ├── resource_tracker.hpp
│   │   │   ├── task.hpp
│   │   │   ├── thread_pool.h
│   │   │   ├── utils
│   │   │   │   ├── TaskSerializer.hpp
│   │   │   │   ├── dts_metrics.h
│   │   │   │   ├── rpc_utils.h
│   │   │   │   └── utils.hpp
│   │   │   └── uuid_generator.hpp
│   │   ├── proto
│   │   │   └── dts
│   │   │       ├── common
│   │   │       │   ├── resource.proto
│   │   │       │   └── shard.proto
│   │   │       ├── error
│   │   │       │   ├── error.proto
│   │   │       │   ├── job_error.proto
│   │   │       │   └── sys_error.proto
│   │   │       ├── internal
│   │   │       │   └── internal_service.proto
│   │   │       ├── service
│   │   │       │   └── task_service.proto
│   │   │       └── task
│   │   │           ├── task.proto
│   │   │           └── task_state.proto
│   │   └── src
│   │       ├── config.cpp
│   │       ├── converters.cpp
│   │       ├── exceptions.cpp
│   │       ├── redis
│   │       │   └── RedisManager.cpp
│   │       └── resource_tracker.cpp
│   ├── dts-cli
│   │   ├── Makefile
│   │   ├── cmd
│   │   │   ├── bench.go
│   │   │   ├── get.go
│   │   │   ├── root.go
│   │   │   ├── submit.go
│   │   │   └── watch.go
│   │   ├── configs
│   │   │   └── job_real.yaml
│   │   ├── dts
│   │   ├── go.mod
│   │   ├── go.sum
│   │   ├── main.go
│   │   └── pkg
│   │       ├── client
│   │       ├── dag
│   │       │   └── validator.go
│   │       ├── parser
│   │       │   └── parser.go
│   │       └── proto
│   │           ├── common
│   │           │   ├── resource.pb.go
│   │           │   └── shard.pb.go
│   │           ├── error
│   │           │   ├── error.pb.go
│   │           │   ├── job_error.pb.go
│   │           │   └── sys_error.pb.go
│   │           ├── internal
│   │           │   ├── internal_service.pb.go
│   │           │   └── internal_service_grpc.pb.go
│   │           ├── service
│   │           │   ├── task_service.pb.go
│   │           │   └── task_service_grpc.pb.go
│   │           └── task
│   │               ├── task.pb.go
│   │               └── task_state.pb.go
│   ├── scheduler
│   │   ├── CMakeLists.txt
│   │   ├── include
│   │   │   ├── db_batcher.hpp
│   │   │   ├── scheduler_loop.h
│   │   │   ├── scheduler_service_impl.h
│   │   │   ├── task_repository.h
│   │   │   └── worker_manager.h
│   │   └── src
│   │       ├── db_batcher.cpp
│   │       ├── main.cpp
│   │       ├── scheduler_loop.cpp
│   │       ├── scheduler_service_impl.cpp
│   │       ├── task_repository.cpp
│   │       └── worker_manager.cpp
│   └── worker
│       ├── CMakeLists.txt
│       ├── include
│       │   ├── scheduler_client.h
│       │   ├── task_registry.h
│       │   ├── worker_node.h
│       │   └── worker_service.h
│       └── src
│           ├── main.cpp
│           ├── scheduler_client.cpp
│           ├── task_registry.cpp
│           ├── tasks.cpp
│           ├── worker_node.cpp
│           └── worker_service.cpp
└── tests
    ├── CMakeLists.txt
    ├── benchmark
    │   ├── CMakeLists.txt
    │   ├── include
    │   │   ├── config.hpp
    │   │   ├── metrics.hpp
    │   │   ├── monitor.hpp
    │   │   └── submitter.hpp
    │   ├── main.cpp
    │   └── src
    │       ├── http_benchmark.cpp
    │       ├── metrics.cpp
    │       ├── monitor.cpp
    │       └── submitter.cpp
    ├── client
    │   ├── CMakeLists.txt
    │   └── dag_builder_test.cpp
    ├── common
    │   ├── CMakeLists.txt
    │   ├── database_pool_test.cpp
    │   └── json_convert_test.cpp
    ├── scheduler
    │   ├── CMakeLists.txt
    │   ├── task_repository_test.cpp
    │   └── worker_manager_test.cpp
    └── worker
        ├── CMakeLists.txt
        └── worker_core_test.cpp

```