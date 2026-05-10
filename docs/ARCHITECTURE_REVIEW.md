# DTS (Distributed Task Scheduling System) — 架构审视与重构方向

> 本文档用于在 Gemini 中深入讨论 DTS 的架构设计、问题诊断和重构选型。
> 目标是：以 AegisEngine (C++20+io_uring+Actor) 为底层基础设施，重写 DTS 为校招级产品。

---

## 一、现状概览

### 1.1 DTS 当前设计（旧项目）

**架构模式**: Master-Worker 推送模式（Active Push）

**四个二进制产物**:

| 组件 | 语言 | 职责 | 通信方式 |
|------|------|------|----------|
| `api-server` (Portal) | C++17 | 流量接入网关，gRPC SubmitDAG，双缓冲批处理写入 PG + Redis | gRPC (暴露) |
| `scheduler` | C++17 | 调度中枢，XREADGROUP 拉取 Stream，负载均衡，主动 Push | gRPC (客户端→Worker) |
| `worker` | C++17 | 计算节点，接收 Scheduler Push，执行任务，汇报结果 | gRPC (服务端) |
| `dts-cli` | Go | CLI 控制面，YAML 提交、Kahn 算法检查、压测 | gRPC |

**存储层**:
- Redis: 热数据（Stream 消息队列、DAG 依赖 Hash、任务元数据 KV、Worker 负载 ZSet）
- PostgreSQL: 冷数据（job 表、task 表、task_edge 表，ACID 持久化）

**核心工作机制**:
1. 用户通过 CLI 或 API 提交 DAG
2. api-server 双缓冲聚合请求 → PostgreSQL COPY 协议落盘 → Redis Pipeline 写元数据和 Stream
3. Scheduler XREADGROUP 阻塞消费 Stream → 负载均衡选 Worker → gRPC 推送任务
4. Worker 执行 → 汇报结果 → Scheduler 执行 Lua 脚本原子更新依赖 → 触发下游任务
5. 故障自愈: XPENDING 扫描僵尸任务 → XCLAIM 抢占 → 重新分发

**性能声称**: 21,500+ QPS (API Ingestion), 4800+ TPS (Scheduler Dispatch)

### 1.2 AegisEngine 当前能力（可用资产）

**底层架构**: C++20 + io_uring + Actor 模型 + thread-per-core + lock-free MPSC

**能力分层**:

```
common/ — ObjectPool, IntrusiveList, UniqueFd, SpinLock, ScopeGuard, aegisLog
core/   — Actor (MPSC 队列), ActorRegistry (64bit Handle), Scheduler, Worker (io_uring+drain)
          Task<T>/DetachedTask/MoveOnlyTask, HierarchicalTimeWheel
net/    — Socket (io_uring awaitable), Acceptor, Connection, Packet(SBO), PacketPool
          OutboxBatcher, Dispatcher, InternalLink, ClusterManager
rpc/    — 本地 RPC (零拷贝指针传递), 跨服 RPC (RpcEnvelope 36B packed)
          支持 Actor-to-Actor 本地直接调用 + 远程序列化传输
```

**二进制协议**: `[4B Magic 'AEGS'][4B Length][4B SeqID][4B MsgID][Protobuf Body]`
**内网协议**: `[4B frame_length][RpcEnvelope(36B packed)][Serialized Payload]`

**关键优势**:
- 无锁 Actor 模型 → 天然适合 Master-Worker 内部通信
- io_uring 异步 IO → 高性能网络层
- 自定义二进制协议 → 比 gRPC 轻量，无 Protobuf 编译负担
- 已在 Godot 客户端验证过完整的连接/拆包/心跳/粘包处理

---

## 二、现有问题诊断

### 2.1 架构层

1. **gRPC 锁死整个项目**
   - gRPC v1.60.0 FetchContent 暴力拉取，编译长达数十分钟
   - 所有组件间通信（api-server→scheduler, scheduler→worker）都绑定 gRPC
   - 实际上 DTS 的内部通信模式非常固定（点对点，少量 RPC 类型），gRPC 过于重量级

2. **Go CLI 模块是孤岛**
   - 独立的 go.mod，独立的 proto 生成（需要手动维护 .pb.go 文件）
   - 功能仅限于 YAML 提交 + 压测，跟 C++ 端没有代码级集成
   - Kahn 算法环检测其实 C++ 也能做

3. **api-web 模块（cpp-httplib）状态不明**
   - 存在 src/api-web/，有 CMakeLists.txt 和 main.cpp
   - README 未提及，看起来是实验性质，是否可用未知

4. **组件边界模糊**
   - api-server 同时承担了"接入"和"双缓冲写入"两个职责
   - scheduler 既做调度又做 RPC Server（接收 Worker 结果汇报）又做 DbBatcher
   - 单一职责原则未贯彻

5. **控制面与数据面未真正分离**
   - 所有组件通过 gRPC 直接点对点通信，没有消息总线抽象
   - Go CLI 直接调 api-server，没有网关层

### 2.2 代码质量

1. **依赖管理混乱**
   - CMakeLists.txt 中禁用系统包查找，同时 find_package(Boost) 和 find_package(ZLIB)
   - 无 vcpkg manifest，无 CMakePreset
   - build/ 目录包含了 grpc 子模块的全部编译产物（_deps/）

2. **测试不可用**
   - CMakeLists.txt 中 # add_subdirectory(tests) 被注释掉
   - 有 Test 目录和测试文件，但从未运行过

3. **AI 生成文档**
   - 结构尚可，但很多代码细节与实际不符
   - 性能数据无法验证（build/ 目录有 cmake 产物但不确定是否实际压测过）

4. **残留的 go 模块**
   - src/dts-cli/ 下有 go.mod，但 C++ 项目顶层目录也混入了 CMakeFiles/

### 2.3 与 AegisEngine 的衔接

- 零集成。DTS 完全独立、自包含，没有复用 Aegis 的任何组件
- 如果用 Aegis 重写，大部分基础设施层（网络、Actor、内存池）可以直接复用
- 但需要做架构调整：Aegis 是 thread-per-core，DTS 是传统多线程 + 锁

---

## 三、重构方向讨论

### 3.1 方案 A：轻度重构（保留 gRPC，只改代码质量）

- 引入 vcpkg + CMakePreset
- 修复测试，删掉 Go/Web 模块
- 用 Aegis 的 Actor 模型替代 scheduler 中的线程池 + 锁

**优点**: 改动最小，原有的架构设计不动
**缺点**: gRPC 编译噩梦仍在，项目的技术差异度不高（仍是 gRPC+PG+Redis 组合，其他调度系统也有）

### 3.2 方案 B：中度重构（用 Aegis 替换 gRPC）

- 内部通信（scheduler↔worker）改为 Aegis 自定义二进制协议 + InternalLink
- 对外暴露的 API 仍用 gRPC（或改为 Aegis 协议 + 网关桥接）
- 去掉 Go CLI，所有功能合并到 C++ CLI
- Aegis Actor 模型作为调度器/Scheduler 的核心运行时
- 丢掉 api-web

**优点**: 去掉 gRPC 内部依赖，编译速度大幅提升，展现"Aegis 驱动 DTS"的技术故事
**缺点**: API Server 对外接口需要重新设计（gRPC 保留与否），CLI 重写

### 3.3 方案 C：重度重构（分层解耦 + Aegis 深度绑定）

- **接入层**: 用 Aegis 的 Connection/Dispatcher 替代 gRPC，对外暴露 HTTP/WebSocket（通过 Aegis 网关）
- **调度层**: Scheduler 用 Aegis Actor 模型重写，每个 Worker 作为一个 Actor，Scheduler 作为一个 Actor，天然无锁消息通信
- **执行层**: Worker 用 Aegis Actor 模型重写，与 Scheduler 通过 InternalLink 通信
- **持久化层**: 保留 PG + Redis，但用 Aegis 的 io_uring 做异步 PG 写入
- **控制面**: C++ CLI（Aegis 客户端），去掉 Go
- **新增模块**: 可观测性（基于 Aegis 的 metrics 上报）、任务管理 Web UI

**优点**: 全链路由 Aegis 驱动，简历故事完整、技术深度最高
**缺点**: 工作量最大，需要重新设计大部分代码

### 3.4 关键决策点

| # | 决策问题 | 选项 | 推荐 |
|---|---------|------|------|
| 1 | **内部通信协议** | 保留 gRPC / 替换为 Aegis 协议 | 替换为 Aegis — 编译速度 + 技术差异化 |
| 2 | **对外 API** | 保留 gRPC / Aegis 协议 | 先保留 gRPC 作为 API，逐步替换 |
| 3 | **Go CLI** | 保留 / 用 C++ 重写 | 重写为 C++，统一构建体系 |
| 4 | **api-web** | 保留 / 删除 | 删除，校招不需要 REST + 调度两个端 |
| 5 | **组件拆分** | 保持 3 组件 / 细化 | 保持 3 组件（api-server, scheduler, worker） |
| 6 | **Actor 模型** | 仅 scheduler / 全系统 | 逐步推进，先 scheduler |
| 7 | **依赖管理** | FetchContent / vcpkg | vcpkg + CMakePreset |
| 8 | **PG 交互** | 同步 libpqxx / io_uring 异步 | 先同步，后续优化 |

---

## 四、AegisEngine 能做但 gRPC 不能做的

### 4.1 零拷贝 Actor 通信

```
gRPC: [序列化 → Protobuf → 网络 → 反序列化]  多次拷贝
Aegis (本地 Actor): [指针传递 → dispatch_msg → static_cast]  零拷贝
Aegis (远程 Actor): [SBO Packet → writev 批量发送]  一次拷贝
```

对于 scheduler 内部组件间通信（SchedulerLoop → DbBatcher → WorkerManager），用 Aegis Actor 可以直接省掉所有序列化开销。

### 4.2 io_uring 异步 IO

- PG 写入可以用 io_uring 的 `IORING_OP_WRITEV` / `IORING_OP_READV` 异步执行
- 而当前 DTS 用的是同步 `libpqxx` 阻塞在 ExecuteTx
- 虽然 PG 的异步需要额外工作（PG 的套接字在 io_uring 上等），但比 gRPC 的线程池阻塞模型灵活

### 4.3 Thread-per-Core + 无锁

- DTS 当前: ThreadPool(mutex+cv) + std::queue + 连接池(mutex+cv)
- Aegis: 每个核一个 Worker/io_uring/Actor 队列 → 零跨核锁
- Scheduler 的 WorkerManager（负载均衡、Worker 注册表）可以用 Actor 内存状态直接管理，不需要 Redis ZSet 辅助

---

## 五、建议讨论清单

请在 Gemini 中讨论以下问题：

1. **重构力度**: 选 A/B/C 哪个方案？还是分阶段（先 B 后 C）？
2. **对外 API**: 保留 gRPC 还是全切 Aegis 协议？如果保留 gRPC，怎么解决编译慢？
3. **数据库交互**: scheduler 的 DbBatcher 是否要保留？还是改为 Actor 模型异步？
4. **Worker 注册与发现**: 用 Aegis Actor 的内存状态代替 Redis ZSet，是否可靠？
5. **DAG 存储**: 是否保留 PG? 还是纯 Redis 内存调度 + 可插拔持久化？
6. **CLI 方案**: C++ CLI 怎么做？用 Aegis 的 Connection 直接连 api-server？
7. **测试策略**: 重构过程中怎么保证正确性？Test harness 用什么？
8. **发布物**: 最终产物是什么？几个二进制？部署方式（Docker 单机/k8s）？
9. **面试呈现**: 重构后怎么在简历和面试中讲清楚"从网络框架到调度系统"的技术演进？
10. **里程碑**: MVP 应该做到什么程度？最小可 Demo 是什么？
