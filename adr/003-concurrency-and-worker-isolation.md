# ADR 003: 调度并发模型与 Worker 执行隔离策略

## 元数据

- **时间**: 2026-05-10
- **状态**: 已采纳 ✅
- **影响范围**: Scheduler 的 EventBus、Worker 的执行引擎

---

## 背景与痛点

1. **并发模型** — ADR 001 规定了领域层状态机是纯单线程、确定性的。但外部世界（Redis 回调、PG 回调、定时器、gRPC 请求、心跳检测）是极度并发的。如何高效、安全地把并发事件喂给单线程状态机？
2. **Worker 隔离** — V1.0 中 Worker 的任务执行缺乏隔离。一个任务 Coredump 会导致整个 Worker 进程崩溃，丢失在途任务，引起集群抖动。

---

## 最终决策

### 大山一：调度并发模型 — MPSC Event Loop

```
┌─────────┐  ┌─────────┐  ┌─────────┐
│ PG 线程  │  │Redis线程│  │心跳线程  │  ... 多生产者 (任意线程)
└────┬────┘  └────┬────┘  └────┬────┘
     │            │            │
     │     Push Event (lock-free)
     ▼            ▼            ▼
┌─────────────────────────────────────┐
│         MPSC 无锁队列                │
│  (moodycamel::ConcurrentQueue       │
│   或自研原子链表)                    │
└──────────────┬──────────────────────┘
               │  Drain 批量拉取
               ▼
┌─────────────────────────────────────┐
│    Event Loop (单消费者线程)          │
│     for (auto& event : drained) {   │
│       auto cmds = state_machine_->  │
│           Process(std::move(event));│
│       ExecuteCommands(cmds);        │
│     }                               │
└─────────────────────────────────────┘
```

关键设计：
- 坚决不上全局 `std::mutex` 大锁
- 使用成熟的无锁队列（`moodycamel::ConcurrentQueue` 或 AegisEngine 已有的 MPSC 原子链表）
- 网络 IO 线程、定时器线程、心跳线程作为 Producer，无锁 `Push(Event)`
- 唯一的 Event Loop 线程作为 Consumer，批量 `Drain()` → 喂给状态机
- 状态机处理完输出 Command → Event Loop 分发执行

### 大山二：Worker 执行隔离 — 进程级沙箱 (fork)

```
┌──────────────────────────────────┐
│      Worker 主进程（极轻量）       │
│  - Redis 长连接（XREADGROUP）     │
│  - 心跳汇报                       │
│  - 拉取任务                       │
│  - fork() 子进程执行              │
└──────────────┬───────────────────┘
               │ fork()
    ┌──────────┴──────────┐
    ▼                     ▼
┌────────────┐     ┌────────────┐
│ 子进程 A    │     │ 子进程 B    │
│ 执行 Task   │     │ 执行 Task   │
│ ↑ pipe 回传  │     │ ↑ pipe 回传  │
│ ↓ SIGKILL   │     │ ↓ SIGKILL   │
│   超时      │     │   超时      │
└────────────┘     └────────────┘
```

关键设计：
- Worker 主进程保持极轻量，仅负责维护 Redis 连接、心跳、任务拉取
- 任务执行通过 `fork()` 派生子进程
- 子进程完成 → 通过 pipe/共享内存将结果传回主进程
- 主进程上报结果到 Redis Stream → 拉取下一个任务
- 子进程超时 → `SIGKILL` 强制终止
- 子进程段错误 → 主进程收到 `SIGCHLD` → 标记任务失败 → 继续运行
- **Worker 主进程永不崩溃**

---

## 护城河防御设计

`EventBus` 接口化：

```cpp
// src/scheduler/engine/IEventBus.hpp
struct IEventBus {
    virtual ~IEventBus() = default;
    virtual void Publish(std::unique_ptr<Event> event) = 0;
    virtual std::vector<std::unique_ptr<Event>> Drain() = 0;
};
```

约束：
- 领域层状态机不感知底层队列实现
- 单测使用 `InMemoryEventBus`（`std::vector` + 假 MPSC），不依赖无锁队列
- 生产环境使用 `MpscEventBus`（`moodycamel::ConcurrentQueue` 或自研实现）

---

## 妥协与风险

| 风险 | 说明 | 缓解 |
|------|------|------|
| fork() COW 开销 | 大内存进程 fork 有页表复制开销 | Worker 主进程保持轻量，此开销可忽略 |
| 父子进程 IPC | 需要 pipe/共享内存回传结果 | 简单 KV 结果（成功/失败/payload），pipe 足够 |
| 无锁队列的 ABI 依赖 | moodycamel 是 header-only，编译时间长 | 可接受；如果用自研 MPSC 消除此风险 |

---

## 关联 ADR

- `../adr/001-event-driven-scheduler.md` — 状态机设计
- `../adr/002-architecture-topology.md` — 宏观拓扑

---

## 变更记录

| 日期 | 变更 |
|------|------|
| 2026-05-10 | 初稿 |
