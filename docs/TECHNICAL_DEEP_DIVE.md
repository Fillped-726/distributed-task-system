这是一份深度硬核的 `docs/TECHNICAL_DEEP_DIVE.md`。

这份文档是你的**面试杀手锏**。它不再流水账地记录功能，而是采用 **"Problem - Solution - Implementation - Result"** 的结构，精准打击面试官最爱问的“你遇到了什么难点？怎么解决的？”这类问题。

请直接复制以下内容：

---

# Technical Deep Dive: Challenges & Solutions

**Version**: 0.4.0
**Context**: C++17/Go Hybrid Architecture, Master-Worker Push Model

---

## 1. The Persistence Bottleneck (持久化瓶颈)

### 🔴 Problem

在系统初期设计中，我们尝试直接使用 SQL `INSERT` 语句将任务写入 PostgreSQL。

* **现象**: 在并发提交达到 ~1,500 QPS 时，数据库 CPU 飙升至 100%，磁盘 I/O 饱和。
* **原因**:
1. **Transaction Overhead**: 每条 INSERT 都会触发一次完整的事务提交和 WAL (Write-Ahead Logging) 落盘。
2. **Parser Overhead**: 数据库需要对每条 SQL 进行词法分析和查询规划。
3. **Network RTT**: 高频的小包网络交互导致严重的延迟累积。



### 🟢 Solution: Async Group Commit & IO Batching

我们重构了 `dts-portal` 的接入层，实现了**异步组提交**机制。

#### Implementation Details

1. **Double Buffering (双缓冲队列)**:
* 在内存中维护两个 `std::vector<TaskRequest*>`，分别称为 `ActiveBuffer` 和 `StandbyBuffer`。
* **Lock Granularity Optimization (锁粒度优化)**: 接收线程只向 `ActiveBuffer` 写入。仅在 buffer 满或超时（如 50ms）时，刷盘线程获取互斥锁，交换两个 Buffer 的指针 (`std::swap`)。
* **收益**: 锁的持有时间从“IO 耗时级别”降低为“内存指针交换级别” (纳秒级)，几乎消除了生产者线程的阻塞。


2. **PostgreSQL COPY Protocol**:
* 利用 `libpqxx` 的 `pipeline` 和 `stream_to` 接口。
* 将聚合后的数据直接以 **Binary Format** (二进制流) 注入数据库，完全绕过 SQL 解析器。



#### 📊 Result

* 单机持久化吞吐量从 **1,600 QPS** 提升至 **20,000+ QPS** (受限于 CPU 序列化速度而非 IO)。
* 数据库 CPU 负载降低 60%。

---

## 2. Distributed Race Conditions (分布式竞态条件)

### 🔴 Problem

在 DAG (有向无环图) 调度中，当一个父任务完成时，需要触发其所有子任务的 `Indegree` (入度) 减 1。

* **场景**: 多个 Worker 同时汇报不同的父任务完成，导致并发修改同一个子任务的入度。
* **难点**: 如果在 C++ 应用层使用分布式锁 (如 Redlock)，会引入巨大的网络延迟和代码复杂度，导致调度延迟不可控。

### 🟢 Solution: Redis Lua Atomic Engine

我们将核心状态流转逻辑**下沉 (Push Down)** 到 Redis 端。

#### Implementation Details

* **Atomic Script execution**: Redis 保证 Lua 脚本以单线程原子方式执行，脚本执行期间不会插入其他命令。
* **Logic**:
```lua
-- 原子递减入度，并返回剩余值
local remain = redis.call('HINCRBY', 'dts:dag:indegree', child_id, -1)
if remain == 0 then
    -- 只有变为 0 的那一刻，才会触发调度
    redis.call('XADD', 'dts:stream:pending', '*', 'payload', payload)
end

```



#### 📊 Result

* 彻底移除了应用层的分布式锁。
* 调度决策延迟稳定在 **< 1ms** (仅取决于 Redis RTT)。

---

## 3. CPU Overhead from Serialization (序列化开销)

### 🔴 Problem

在性能分析 (Profiling) 中发现，`dts-portal` 消耗了大量 CPU 在 Protobuf 的序列化与反序列化上。

* **路径**: `gRPC Recv` -> `Deserialization` -> `DTO to Domain Object` -> `Business Logic` -> `Domain to DTO` -> `DB Write`.
* **浪费**: 很多时候我们只需要把数据透传给 DB 或 Redis，中间的 Object Conversion 是纯粹的算力浪费。

### 🟢 Solution: DTO Passthrough (对象透传)

我们采用了 **"Payload 不落地"** 的策略。

#### Implementation Details

1. **Pointer Passing**: 在 C++ Server 内部流转时，直接传递 `TaskRequest` 的智能指针 (`std::shared_ptr`)。
2. **Lazy Parsing**:
* `dts-portal` 不解析 `func_params` 具体内容，直接将其作为 `bytes` 写入 DB。
* `dts-scheduler` 也不解析，直接转发。
* 只有终端的 `dts-worker` 在真正执行前才进行解析。



#### 📊 Result

* API Server 的 CPU 使用率降低约 **30%**。
* 支持任意格式的任务参数 (JSON/Binary/Text)，因为中间层对内容无感知。

---

## 4. Flow Control in Push Model (推送模式下的流控)

### 🔴 Problem

系统从 Pull (Worker 抢占) 切换到 **Active Push (Scheduler 推送)** 模式后，面临一个新问题：
如果 Scheduler 推送速度超过 Worker 执行速度，会导致 Worker 的 TCP 接收缓冲区满，甚至 OOM (内存溢出)；或者 Scheduler 盲目轮询导致 "热点 Worker" 问题。

### 🟢 Solution: Optimistic Pre-booking (乐观预订)

#### Implementation Details

1. **Shadow State (影子状态)**: Scheduler 在内存中维护一份 `WorkerLoadMap` (Map<WorkerID, ActiveTasks>)。
2. **Pre-book (预订)**:
* 在从 Redis 取出任务**之前**，先检查是否有空闲 Worker。
* 若有，先在内存中 `Load++` (预占位)，再发起 gRPC 推送。


3. **Correction (修正)**:
* 当收到 Worker 的 RPC 响应 (Ack) 或 心跳包时，更新为真实负载。


4. **Backpressure (反压)**:
* 如果所有 Worker 的影子负载都达到阈值，Scheduler **停止调用** `XREADGROUP`，让任务堆积在 Redis Stream 中，从而自然地实现了背压。



#### 📊 Result

* 实现了精确的 **Round-Robin** 负载均衡。
* 从未发生 Worker 侧的过载崩溃。

---

## 5. Invalid DAG Cycles (环状依赖死锁)

### 🔴 Problem

如果用户（无意或恶意）提交了一个包含环路 (Cycle) 的 DAG (例如 A->B->A)，服务端如果照单全收：

1. 存储层会被垃圾数据填满。
2. 调度层永远无法触发这些任务，甚至可能因逻辑漏洞导致无限递归。

### 🟢 Solution: Client-Side Fail-Fast

我们将校验责任前移至 **Go CLI (Control Plane)**。

#### Implementation Details

1. **Graph Construction**: CLI 读取 YAML 后，在本地构建邻接表。
2. **Kahn's Algorithm**:
* 计算所有节点入度。
* 不断移除入度为 0 的节点。
* 若最终剩余节点数 > 0，则判定存在环。


3. **Rejection**: 直接在客户端报错退出，不发起任何网络请求。

#### 📊 Result

* **Zero Cost**: 非法请求对服务端造成 **0** 资源消耗。
* 提升了用户体验（报错即时，无需等待服务端异步反馈）。