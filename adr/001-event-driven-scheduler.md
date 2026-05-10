# ADR 001: 采用务实派事件驱动状态机重构调度器领域层

## 元数据

- **时间**: 2026-05-10
- **状态**: 已采纳 ✅
- **影响范围**: scheduler/domain/ 模块

---

## 背景与痛点

现有 V1.0 的调度逻辑与外部存储（Redis/PG）和 RPC（gRPC）深度耦合。导致：

1. **单元测试形同虚设** — 必须依赖真实的外部中间件或大量复杂的 Mock
2. **容灾自愈能力依赖中间件特性** — 高度绑定于 Redis Lua 原子性、Stream PEL 等 Redis 独有机制，难以在面试中结构化地自证"系统级"的容灾设计
3. **代码即文档失效** — 业务逻辑散布在 gRPC handler、Redis 调用、PG 写入之间，无法一眼看出调度核心的全貌

---

## 考虑过的替代方案

### 方案 A：主动式仓储抽象 (Active Repository Pattern)

在领域层定义 `ITaskQueue`、`IDagStorage` 接口，Scheduler 主动调用这些接口做状态变更。

- **优点**: 直觉、落地快、与 V1.0 设计一致
- **缺点**: 领域层仍有隐式 IO 副作用；重放逻辑不够纯粹；单测仍需要 Mock 接口
- **结论**: ❌ 否决

### 方案 C：全量事件溯源 (Full Event Sourcing)

记录所有 DAG 从创建到结束的全部 Event，重启时全量回放。

- **优点**: 极致的确定性，重放完全可预测
- **缺点**: 存储成本过高（已完成的 DAG 再回放无意义）；实现复杂
- **结论**: ❌ 否决

---

## 最终决策：务实版事件驱动状态机 ✅

### 核心设计

```
┌──────────────┐     Event      ┌─────────────────────────┐
│  适配器引擎   │ ─────────────→ │     领域状态机           │
│  (Adapter     │               │  (Pure State Machine)   │
│   Engine)     │               │                         │
│               │ ←───────────── │     Command             │
│  - gRPC       │    (输出)     │                         │
│  - Redis      │               │  - 输入 Event → 更新状态 │
│  - PG         │               │  - 输出 Command         │
└──────────────┘               └─────────────────────────┘
```

三要素：

| 要素 | 定义 | 示例 |
|------|------|------|
| **Event** | 已发生的事实（不可变） | `TaskSuccessEvent{task_id, job_id}` |
| **State** | 当前内存中的 DAG 状态 | `DagState{running_tasks, pending_deps}` |
| **Command** | 状态机建议执行的动作（不保证执行） | `DispatchTaskCommand{worker_id, task}` |

### 混合恢复机制

摒弃全局 EventStore。Scheduler 启动时：

1. 从 PG/Redis 加载所有活跃 DAG（RUNNING/PENDING）的**状态快照**
2. 从 Redis Stream PEL 加载在途任务的**增量事件**并回放
3. 状态机恢复就绪 → 开始消费新事件

### 快照与事件的竞态处理

"保存 DAG 快照"和"处理新到达事件"之间的竞态，由外层 Engine 的单线程 Event Loop 保证——状态机只在 Event Loop 中被调用，快照保存时先暂停事件消费。

---

## 护城河防御设计（代码级红线）

`src/scheduler/domain/` 目录下设定**绝对禁区**：

- ❌ 不允许出现 `std::mutex`、`std::thread`、`std::future`
- ❌ 不允许出现任何多线程原语
- ❌ 不允许直接调用 IO 操作（Redis/PG/gRPC）
- ✅ 领域层必须是**单线程、确定性**的
- ✅ 所有并发控制、分布式锁、重试逻辑必须拦截在外层的"引擎/基础设施层"

这是保证调度逻辑可测试性和一致性的唯一基石。

---

## 代码层预留

1. Event 和 Command 采用强类型系统（`std::variant` 或继承体系），为后续序列化持久化留出抓手
2. 状态机接口设计为函数式风格：`StateMachine::process(Event) -> std::vector<Command>`
3. 领域层暴露内部状态用于快照导出（`StateMachine::snapshot() -> DagSnapshot`）

---

## 妥协与风险

| 风险 | 缓解措施 |
|------|---------|
| 编码心智负担：从命令式切换到事件驱动 | 先在领域层以纯内存单测方式快速验证 Event/Command 类型设计 |
| 快照与事件的竞态条件 | 外层 Engine 的单线程 Event Loop + 快照前暂停消费 |
| 过度设计风险：调度系统是否需要这么复杂的状态机 | 状态机只在 scheduler/domain/ 内，不影响其他模块 |

---

## 关联文档

- `../docs/DTS_STATUS.md` — 项目现状文档
- `../docs/ARCHITECTURE_REVIEW.md` — 初步架构评审草稿（后续废弃）

---

## 变更记录

| 日期 | 变更 | 
|------|------|
| 2026-05-10 | 初稿 |
