# ADR 004: API 边界协议与持久化策略选型

## 元数据

- **时间**: 2026-05-10
- **状态**: 已采纳 ✅
- **影响范围**: API Server（接入层） + Scheduler（持久化层）

---

## 背景与痛点

1. **API 边界** — V1.0 对外 API 强绑定 gRPC，导致 C++ 编译极慢，且浏览器无法直连，阻碍了 Web Dashboard 的开发
2. **持久化策略** — 调度核心状态机需要极高的吞吐量，不能被同步写 PG 的 IO 延迟拖累
3. **读写混淆** — 任务提交/状态更新与查询历史混合在同一路径中，没有分离

---

## 最终决策

### 战役四：API 边界 — HTTP/JSON + Protobuf 双轨制

```
对外（浏览器 / curl / 第三方）
    │  HTTP/JSON
    ▼
┌──────────────────────┐
│  API Server (薄层)    │
│  同进程，不做独立二进制  │
│  (基于 drogon 或       │
│   cpp-httplib /         │
│   Boost.Beast)         │
└──────────┬───────────┘
           │  转化为 Event
           │  推入 MPSC EventBus
           ▼
┌──────────────────────┐
│  Scheduler 状态机     │
│  (ADR 001)           │
└──────────────────────┘

CLI 对内（C++ Protobuf）
    │  直接序列化 + 推 Event (不走 JSON)
    ▼
┌──────────────────────┐
│  API Server (同一入口) │
└──────────────────────┘
```

决策要点：
- 对外 HTTP/JSON，浏览器和 curl 无痛接入
- CLI 内部用 Protobuf 序列化，直接推入 EventBus，不走 JSON 解析
- API Server 与 Scheduler 同进程（不做独立网关二进制），是 EventBus 的一个 Producer
- 使用现代 C++ HTTP 框架（drogon / cpp-httplib / Boost.Beast），不带 gRPC 编译负担

### 内部通信协议

| 路径 | 协议 | 理由 |
|------|------|------|
| 外部 API（浏览器/curl） | HTTP/JSON | 生态友好，Web Dashboard 直连 |
| CLI 对内 | Protobuf（绕过 JSON） | 高性能，复用 proto 定义 |
| Scheduler ↔ Worker | Redis Stream | Pull 模式（ADR 002） |
| Scheduler ↔ PG | libpqxx (Write-Behind) | 批量异步回写 |

### CQRS（读写分离，逻辑分离）

```
写入路径: HTTP SubmitDag → Event (MPSC) → 状态机 → Redis → Write-Behind → PG
读取路径: 直接查 PG（任务状态 / DAG 拓扑 / 审计日志）
```

不引入 CQRS 框架，架构自然分离即可。

### 战役五：持久化策略 — Write-Behind 异步聚合写

```
状态机处理完成
    │
    ├──→ 同步写 Redis（高吞吐 KV/Stream，不影响状态机吞吐）
    │
    └──→ 变更记录 → MPSC 双缓冲队列
                        │
                        ▼
                   DbBatcher 线程（后台）
                        │  每 500ms 聚合
                        │  PG COPY 协议批量写入
                        ▼
                   PostgreSQL
```

关键点：
- **Redis 同步写**：状态机变更先写入 Redis（高吞吐，微秒级延迟）
- **PG 异步写**：变更记录通过 MPSC 队列交给 DbBatcher 线程，每 500ms 用 COPY 协议批量写入
- **复用 V1.0 双缓冲设计**：ActiveBuffer ↔ StandbyBuffer 的 swap 机制已验证过可用

### 容灾视角

| 故障场景 | 影响 | 恢复方式 |
|----------|------|---------|
| Scheduler 宕机 | 丢失 DbBatcher 缓冲区中的未落盘变更 | 启动时从 Redis 恢复活跃状态快照 |
| Scheduler + Redis 同时宕机 | 丢失最近 500ms 的终态变更 | PG 中已有之前写入的快照，丢失的仅为"最近 N 个任务已完成"的状态 |
| PG 宕机 | 调度不受影响（状态存在 Redis） | PG 恢复后 DbBatcher 继续回写 |

---

## 妥协与风险

| 风险 | 说明 | 缓解 |
|------|------|------|
| Write-Behind 丢失 | Redis + Scheduler 同时物理毁灭丢失 ≤500ms 终态数据 | 明确作为 Trade-off 接受；调度系统允许"任务完成状态"的极低概率丢失 |
| JSON 性能 | JSON 解析慢于 Protobuf | API 接入层非高频路径，不构成瓶颈；CLI 走 Protobuf 后门绕过 |
| 同进程 API Server | API Server 崩溃会影响 Scheduler | API Server 是 Scheduler 内的一个薄层，异常不会影响状态机核心 |

---

## 关联 ADR

- `../adr/001-event-driven-scheduler.md` — 状态机接收 Event
- `../adr/002-architecture-topology.md` — Pull 模式，不需要 gRPC 内部通信
- `../adr/003-concurrency-and-worker-isolation.md` — EventBus 的 MPSC 队列

---

## 变更记录

| 日期 | 变更 |
|------|------|
| 2026-05-10 | 初稿 |
