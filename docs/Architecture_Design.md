# System Architecture Design

## 1\. 总体架构 (System Overview)

DTS (Distributed Task Scheduling System) 是一个高性能、事件驱动的分布式任务调度平台。系统经过深度重构，摒弃了传统的“数据库轮询 (DB Polling)”模式，转而采用 **Redis Stream** 作为核心调度总线，结合 **PostgreSQL** 的持久化能力，实现了 **1600+ QPS** 的写入吞吐与 **亚毫秒级** 的调度延迟。

设计遵循 **“控制流与数据流分离”** 及 **“IO 隔离”** 原则，彻底解耦了任务提交、调度分发与状态落盘三个核心环节。

### 核心架构模式 (Architectural Patterns)

1.  **Claim Check Pattern (寄存票模式)**:

      * Redis Stream 中仅传输轻量级凭证 (`TaskID`, `JobID`)。
      * 重型业务数据 (`Payload`) 存储在 Redis KV 中，仅在 Worker 执行前被按需拉取。
      * **收益**: 最大化利用 Redis 网络带宽，吞吐量不再受业务数据大小限制。

2.  **Split-Phase Submission (分阶段提交)**:

      * 采用 **"Commit-then-Publish"** 策略：先通过 PostgreSQL `COPY` 协议完成持久化并 Commit，再通过 Redis Pipeline 推送任务。
      * **收益**: 解决了分布式环境下的竞态条件 (Race Condition)，并利用 IO 隔离防止 Redis 抖动影响数据库事务。

3.  **Write-Behind Persistence (异步回写)**:

      * 任务状态更新优先写入 Redis 内存，由 `DbBatcher` 组件在后台进行微批处理 (Micro-batching) 异步落盘。
      * **收益**: 将高频随机 IO 转化为低频顺序 IO，突破数据库 IOPS 物理极限。

-----

## 2\. 核心组件设计 (Component Design)

### 2.1 API-Server (接入与持久化层)

  * **角色**: 高吞吐写入网关。
  * **关键技术**: `Zero-Copy Protobuf`, `libpqxx stream_to (COPY)`, `Redis Pipeline`。
  * **职责**:
      * **Validation**: 接收 gRPC 请求，进行基础合法性校验。
      * **Persistence**: 使用 COPY 协议将 DAG 元数据流式写入 PostgreSQL。
      * **Publishing**: 构建 Redis Pipeline，一次性写入 Meta (KV)、Dependency (Hash)、Children (Set) 和 Stream (Queue)。

### 2.2 Scheduler (调度核心)

  * **角色**: 纯内存、事件驱动的调度器。
  * **关键技术**: `Redis Stream (XREADGROUP)`, `Optimistic Pre-booking`, `ThreadPool`。
  * **职责**:
      * **Event Loop**: 阻塞式拉取 Redis Stream 中的就绪任务。
      * **Load Balancing**: 维护 Worker 负载影子状态，通过 **Round-Robin** 算法选择最优节点。
      * **Dispatch**: 仅仅负责通过 gRPC 下发任务，**不进行任何数据库写操作**。
      * **Rescue**: 后台线程监控 `Pending List`，自动抢占并重发超时未 ACK 的任务。

### 2.3 Worker (执行节点)

  * **角色**: 无状态、异步执行单元。
  * **关键技术**: `Async ThreadPool`, `gRPC Retry`, `Dynamic Discovery`.
  * **职责**:
      * **Execution**: 接收任务 -\> 放入内部队列 -\> 异步执行业务逻辑。
      * **State Reporting**: 执行结束后，通过 RPC 调用 Scheduler 的 `UpdateTaskStatus` 接口。
      * **Idempotency**: 配合全局唯一 UUID，保证任务即使被重试也不会产生副作用。

### 2.4 Redis 7.0 (核心引擎)

  * **角色**: 调度总线与状态协调者。
  * **数据结构**:
      * `Stream`: 任务队列 (Pending -\> Running)。
      * `Hash`: 任务依赖计数器 (DAG Indegree)。
      * `Set`: DAG 拓扑关系 (Adjacency List)。
      * `KV`: 任务静态元数据 (Payload)。
      * `Lua Scripts`: 原子性执行 "减依赖 -\> 检查计数 -\> 触发下游" 的核心逻辑。

-----

## 3\. 核心工作流 (Core Workflow)

### 3.1 任务提交与发布 (Submission Phase)

1.  **Client**: 发送 Proto 请求 (参数未解析，保持二进制)。
2.  **API-Server (DB)**: 开启事务 -\> 使用 `COPY` 写入 Job/Task/Edge 表 -\> **Commit 事务**。
3.  **API-Server (Redis)**: 事务提交成功后，构建 Pipeline -\> 写入 Redis Meta/Dependency -\> 将入度为 0 的 TaskID 推入 Stream。

### 3.2 调度与分发 (Dispatch Phase)

1.  **Scheduler**: `XREADGROUP` 收到 TaskID。
2.  **Fetch**: 根据 ID 从 Redis KV 加载完整 Task Meta。
3.  **Balance**: 检查内存中的 Worker 负载表，预订 (Pre-book) 一个 Worker。
4.  **RPC**: 异步发送 `RunTask` 请求给 Worker。

### 3.3 执行与驱动 (Execution & Driving Phase)

1.  **Worker**: 执行业务逻辑 -\> 返回 `SUCCESS/FAILED` 给 Scheduler。
2.  **Scheduler (Batcher)**: 将状态更新放入内存队列 -\> `DbBatcher` 凑够 100 条后批量 `UPDATE` 数据库。
3.  **Scheduler (Lua)**: 调用 Redis Lua 脚本：
      * 获取当前任务的所有子节点。
      * 原子递减子节点的依赖计数 (Indegree)。
      * 若某子节点计数归零，自动 `XADD` 进 Stream (触发下一轮调度)。

-----

## 4\. 关键算法与机制 (Key Algorithms)

### 4.1 Redis Lua 原子驱动 (Atomic DAG Driving)

为了避免 C++ 端的分布式锁竞争，DAG 的流转逻辑完全下沉至 Redis：

```lua
-- 原子操作：减依赖并触发
local remain = redis.call('HINCRBY', KEYS[2], child_id, -1)
if remain == 0 then
    redis.call('XADD', KEYS[3], '*', 'payload', payload, ...)
end
```

### 4.2 乐观预订负载均衡 (Optimistic Pre-booking)

为了解决分布式环境下状态反馈滞后的问题 (Hotspot Worker)，调度器不再依赖 Worker 的心跳来做决策，而是维护一套**影子状态**：

  * **分发前**: `LoadMap[WorkerID]++` (立即生效)。
  * **收到心跳/回调**: `LoadMap[WorkerID] = RealLoad` (修正误差)。
  * **效果**: 即使在高并发下，也能实现完美的 Round-Robin 分发。

-----

## 5\. 性能指标 (Performance Baseline)

*测试环境: Intel Ultra 7 255HX (24 Cores), 16GB RAM, Local Docker Cluster*

| 模块 | 关键指标 | 数值 | 瓶颈分析 |
| :--- | :--- | :--- | :--- |
| **API Server** | **Write QPS** | **\~1,627** | 受限于 PostgreSQL 同步提交模式下的物理 IOPS 极限。 |
| **Scheduler** | **Dispatch TPS** | **\~3,200+** | Redis Stream 消费能力远超 DB 写入速度。 |
| **System** | **Avg Latency** | **\~17 ms** | 包含提交、调度、执行、落盘的全链路耗时。 |

-----

## 6\. 未来演进 (Future Roadmap)

1.  **Sweeper (一致性补救)**:
      * 开发后台扫描线程，定期检测 `Pending` 状态且 `Redis Key` 丢失的“孤儿任务”，重新推入 Redis，保证 Split-Phase 架构下的最终一致性。
2.  **API Micro-Batching (吞吐量突围)**:
      * 在 API Server 端实现微批提交，将单次请求聚合为 Batch Insert，旨在突破 10,000 QPS。
3.  **Dead Letter Queue (DLQ)**:
      * 完善坏消息处理机制，提供可视化界面查看和重试失败任务。