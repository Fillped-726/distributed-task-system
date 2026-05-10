# ADR 002: 宏观拓扑与数据流转选型 (Active-Standby & Pull-Based Delivery)

## 元数据

- **时间**: 2026-05-10
- **状态**: 已采纳 ✅
- **影响范围**: 全局架构（scheduler, worker, 部署拓扑）

---

## 背景与痛点

V1.0 架构存在三个严重问题：

1. **强中心化单点故障** — Scheduler 宕机后整个调度集群不可用
2. **Worker 假死导致 RPC 超时堆积** — Push 模式下 Scheduler 维护的"内存影子状态"在 Worker 网络抖动时变成不可靠来源
3. **网络分区容错性差** — Scheduler 无法主动连接到跨网络环境（NAT/防火墙）的 Worker

需要在面试中能自证系统高可用性的拓扑架构。

---

## 考虑过的替代方案

### 方案 A：去中心化调度 (Gossip / Hash Ring)

彻底去中心化，无主节点。

- **问题**: 脑裂处理、Gossip 同步延迟、CAP 权衡，任何一点的复杂度都超出校招面试的安全防御范围
- **结论**: ❌ 否决

### 方案 B：改良版 Push 模式

Scheduler 增加超时重试、熔断器、死信队列来修补 Push 的缺陷。

- **问题**: Scheduler 仍需维护复杂的 Worker 影子状态和 RPC 连接池；无法根本解决网络分区场景下"Worker 假死 → 任务悬空"的问题
- **结论**: ❌ 否决

---

## 最终决策

### 战役一：控制面拓扑 — 中心化主备 (Active-Standby)

```
         ┌─────────────────┐
         │   Redis SETNX   │  ← Leader Election
         │   (分布式锁)     │
         └────────┬────────┘
                  │
         ┌────────┴────────┐
         │   Scheduler     │
         │   (Leader)      │ ← 唯一消费 Stream 的实例
         │                 │
         ├─────────────────┤
         │   Scheduler     │
         │   (Standby)     │ ← 待命，Leader 宕机后抢锁接管
         └─────────────────┘
```

- 多 Scheduler 实例通过 Redis 分布式锁（SETNX / Redlock）进行 Leader Election
- 同一时间仅 Leader 消费事件和处理 Event Loop
- Follower 实例待命，Leader 宕机后秒切
- 配合 ADR 001 的事件回放机制，Leader 切换时状态从 PG/Redis 快照恢复

### 战役二：数据面存储 — PG + Redis 保留

| 存储 | 职责 | 理由 |
|------|------|------|
| **PostgreSQL** | ACID 持久化底座、审计日志、管理后台查询 | 严谨的事务保证，复杂的关联查询能力 |
| **Redis** | 高吞吐状态中转（Stream）、分布式锁（SETNX）、Leader Election | 扛下了调度系统最要命的并发锁和队列推拉 |

不做替换，不过度设计。

### 战役三：任务分发 — Pull 模式 (Worker 主动拉取)

```
Scheduler 领域层 (ADR 001)
    │  输出 DispatchTaskCommand
    ▼
IWorkerDispatcher (纯虚接口 — 护城河)
    │
    ▼
RedisStreamDispatcher (实现类)
    │  XADD 将任务写入 Stream
    ▼
┌─────────────────────────────────┐
│          Redis Stream           │
│  (dts:stream:ready_tasks)       │
│  Worker 通过 XREADGROUP 拉取    │
└─────────────────────────────────┘
    ▲           ▲           ▲
    │           │           │
Worker-1    Worker-2    Worker-3
(XREADGROUP) (XREADGROUP) (XREADGROUP)
```

关键机制：
- Worker 使用 `XREADGROUP BLOCK 2000` 阻塞拉取 → 任务入队即被取走，延迟 <1ms
- **天然背压**：Worker 消费能力强就多拉，弱就少拉，无需 Scheduler 端流控
- **极简容灾**：Worker 宕机 → 不拉取 → 任务留在队列中 → 其他 Worker 拉走 → 无需 XCLAIM 这类复杂兜底
- **网络兼容**：Worker 可部署在任意网络环境，只需能连 Redis；Scheduler 不再需要主动连 Worker

---

## 护城河防御设计

定义纯虚接口 `IWorkerDispatcher`：

```cpp
// src/scheduler/dispatcher/IWorkerDispatcher.hpp
// 领域层与网络层之间的护城河

struct IWorkerDispatcher {
    virtual ~IWorkerDispatcher() = default;
    
    // 领域层输出 Command 后，由此接口处理投递
    // 实现类可以是 RedisStreamDispatcher / KafkaDispatcher / InMemoryDispatcher（单测用）
    virtual void Dispatch(const DispatchTaskCommand& cmd) = 0;
};
```

约束：
- 领域层状态机只输出 `DispatchTaskCommand`，不关心底层投递方式
- 单测中使用 `InMemoryDispatcher`，不需要依赖 Redis
- 后续切换底层队列（Redis Stream → Kafka → Pulsar）时只需新增实现类

---

## 妥协与风险

| 风险 | 缓解措施 |
|------|---------|
| Scheduler 强依赖 Redis 可用性 | Redis 集群/主从复制；Leader 切换时也从 PG 加载状态快照 |
| Redis 宕机导致调度中断 | 支持从 PG 恢复事件流到新的 Redis 实例 |
| Pull 模式可能带来任务窃取竞争 | Redis Stream 的 Consumer Group 机制天然处理了消息分派（每条消息仅投递给一个 Consumer） |

---

## 关联 ADR

- `../adr/001-event-driven-scheduler.md` — 领域层状态机设计

---

## 变更记录

| 日期 | 变更 |
|------|------|
| 2026-05-10 | 初稿 |
