# System Architecture Design

## 1. 总体架构 (System Overview)

DTS (Distributed Task Scheduling System) 采用分层微服务架构，明确分离了**接入层**、**持久化层**与**调度执行层**。系统设计遵循 **职责单一 (Single Responsibility)** 原则，通过 gRPC 实现各组件间的高效协作。

### 核心组件

1.  **API-Web (接入与校验层):**
    * **角色:** 面向前端/用户的直接入口。
    * **职责:** 接收 HTTP 请求，解析任务配置；负责构建 **DAG (有向无环图)** 结构；核心功能是执行 **拓扑排序检查 (Cycle Detection)**，直接拦截并终止存在环路的非法任务，实现 "Fail-Fast"。
    * **输出:** 将校验通过的 DAG 结构通过 gRPC 转发给 API-Server。

2.  **API-Server (逻辑与持久化层):**
    * **角色:** 系统的核心写入网关。
    * **职责:** 接收 API-Web 的 DAG 请求；计算初始任务依赖关系 (Indegree Calculation)；将 Job 和 Task 元数据原子性地写入 **PostgreSQL** 数据库。

3.  **Scheduler (调度核心):**
    * **角色:** 异步调度器 (Asynchronous Scheduler)。
    * **职责:**
        * **Fetch:** 持续从数据库中拉取 (Poll) “依赖满足” (Indegree=0) 的就绪任务。
        * **Dispatch:** 基于负载均衡策略，通过 gRPC 将任务分发给 Worker。
        * **Monitor:** 管理 Worker 集群的注册、心跳与状态维护。

4.  **Worker (执行节点):**
    * **角色:** 无状态计算单元。
    * **职责:** 接收任务指令并执行；通过 gRPC 向 Scheduler 汇报执行结果和自身健康状态。

5.  **PostgreSQL (存储层):**
    * **职责:** 存储 DAG 定义、任务状态流转日志及节点元数据。作为 Scheduler 和 API-Server 之间的解耦介质。

---

## 2. 通信协议设计 (gRPC Services)

系统内部通信全链路采用 gRPC (Protobuf)，根据业务边界划分为两大通信平面的服务：

### 2.1 任务提交平面 (Submission Plane)
* **交互方:** `API-Web` -> `API-Server`
* **核心服务:** `JobSubmissionService`
    * `SubmitJob(DagRequest)`: 传输经过校验的完整 DAG 对象。API-Server 收到后不再重复校验环路，专注于高吞吐写入。

### 2.2 调度协调平面 (Coordination Plane)
* **交互方:** `Scheduler` <-> `Worker`
* **核心服务:** `WorkerService` & `SchedulerCallbackService`
    * `RegisterWorker`: 节点启动时的握手注册。
    * `Heartbeat`: 定期保活，携带 Worker 当前负载信息。
    * `AssignTask`: Scheduler -> Worker，下发执行指令。
    * `UpdateTaskStatus`: Worker -> Scheduler，任务完成/失败后的状态回调。

---

## 3. 核心工作流 (Core Workflow)

### 3.1 任务提交流程
1.  **构建与校验:** 用户提交 JSON 配置 -> `API-Web` 解析并构建图 -> 运行 DFS/拓扑排序算法检测环路 -> 若有环直接报错返回。
2.  **持久化:** 无环 DAG -> gRPC -> `API-Server` -> 计算初始入度 -> 开启 PG 事务 -> 写入 `jobs` 和 `tasks` 表 -> 提交事务。

### 3.2 调度执行流程
1.  **获取任务:** `Scheduler` 扫描数据库中 `status=PENDING` 且 `indegree=0` 的任务。
2.  **分发:** 锁定任务状态为 `RUNNING` -> 选择健康 Worker -> gRPC `AssignTask`。
3.  **依赖释放:** 任务执行成功 -> Worker 汇报 -> Scheduler 更新 DB -> 触发下游任务 `indegree - 1` -> 若入度为0则流转为可调度状态。

---

## 4. 优化与演进 (Optimization Plan)

### 4.1 当前优化
* **预计算:** 依赖关系的计算前置到写入阶段 (API-Server)，减轻了 Scheduler 运行时的计算压力。
* **连接池:** 各模块均实现了数据库连接池，保证并发下的稳定性。

### 4.2 未来演进 (Planned)
* **Redis 缓存层:** 计划引入 Redis 作为热数据缓存。
    * **用途 1:** 缓存 Worker 的实时心跳和负载信息，减少对 PG 的高频更新。
    * **用途 2:** 实现分布式锁 (Distributed Lock)，支持 Scheduler 的多节点高可用部署 (HA)，防止重复调度。