# DTS — Distributed Task Scheduling System 现状文档

> 本文档用于提供给 AI 助手（如 Gemini）作为上下文参考，以便进行后续的架构评审、重构讨论和技术选型。

---

## 一、项目背景

这个项目是大三时期写的分布式任务调度系统。当时因为水平不够（刚学 C++ 不久），架构设计和代码质量都比较差，最终烂尾了。

现在作者已经完成了 AegisEngine（C++20 + io_uring + Actor 模型的游戏服务端框架），对 C++ 后台开发有了更深的理解，决定回来重新审视这个项目，把它打造成一个校招级的分布式任务调度系统产品。

**关键事实**：
- 当前代码是早期产物，质量较差，架构有硬伤
- 不会复用 AegisEngine 作为基础设施（Aegis 是学习项目，保持独立）
- 后续将大量使用成熟的开源库来提升工程能力（vcpkg、gRPC、Redis/PostgreSQL 的 C++ 客户端等）
- 目标：校招级产品，面试时能讲清楚设计决策和技术难点

---

## 二、目录结构

```
distributed-task-system/
├── CMakeLists.txt            # 顶层构建文件
├── README.md                 # 项目简介（AI 生成的，部分过时）
├── dag_schema.json           # DAG 提交示例
├── docker-compose.yml        # 一键启动 PG + Worker 集群
├── Dockerfile                # 镜像构建
├── init.sql                  # PostgreSQL 建表脚本
├── prometheus.yml            # Prometheus 监控配置
│
├── docs/
│   ├── ARCHITECTURE.md        # 架构文档（AI 生成的，参考价值有限）
│   ├── TECHNICAL_DEEP_DIVE.md # 技术深度解析（AI 生成的）
│   └── test.md               # Mermaid 架构图草稿
│
├── src/
│   ├── CMakeLists.txt        # 源码子目录构建
│   │
│   ├── api-server/           # 【核心】接入服务（Ingestion Gateway）
│   │   ├── CMakeLists.txt
│   │   ├── include/
│   │   └── src/
│   │
│   ├── scheduler/            # 【核心】调度服务（Core Brain）
│   │   ├── CMakeLists.txt
│   │   ├── include/
│   │   └── src/
│   │
│   ├── worker/               # 【核心】执行节点（Execution Node）
│   │   ├── CMakeLists.txt
│   │   ├── include/
│   │   └── src/
│   │
│   ├── common/               # 【共享】基础设施
│   │   ├── CMakeLists.txt
│   │   ├── include/ (头文件)
│   │   ├── src/     (实现)
│   │   └── proto/            # Protobuf 定义
│   │
│   ├── client/               # C++ 客户端库（gRPC stub 封装）
│   │   └── ...
│   │
│   ├── api-web/              # 【边缘模块】REST 接口（实验性，cpp-httplib）
│   │   └── ...
│   │
│   └── dts-cli/              # 【边缘模块】Go 语言 CLI
│       ├── go.mod
│       ├── main.go
│       ├── cmd/              # submit, bench, watch, get 子命令
│       ├── pkg/              # client, dag/validator, parser, proto/
│       └── configs/          # 示例 YAML
│
├── tests/                    # 测试代码（CMake 中已注释掉，不可用）
│   ├── CMakeLists.txt
│   ├── client/
│   ├── common/
│   ├── scheduler/
│   ├── worker/
│   └── benchmark/
│
└── build/                    # 构建产物（包含完整的 gRPC 编译缓存）
```

---

## 三、当前架构设计

### 3.1 整体架构

**Master-Worker 推送模式（Active Push）**，控制面与数据面分离。

```
User / CLI
    │
    ▼
┌──────────────┐     gRPC      ┌──────────────────┐
│  API Server  │ ────────────→ │  PostgreSQL      │
│ (C++, gRPC)  │    COPY 协议   │  (持久化存储)     │
│              │ ────────────→ │                  │
└──────┬───────┘               └──────────────────┘
       │
       │ Redis Pipeline
       ▼
┌──────────────┐               ┌──────────────────┐
│    Redis     │ XREADGROUP    │   Scheduler      │
│  (Stream +   │ ◄──────────── │  (调度中枢)       │
│   KV + Lua)  │               │                  │
└──────┬───────┘               └──────┬───────────┘
       │                              │
       │                  gRPC Active Push
       │                              ▼
       │                    ┌──────────────────┐
       │                    │     Worker       │
       │                    │  (执行节点)       │
       └─────────────────── │                  │
         汇报结果 gRPC       └──────────────────┘
```

### 3.2 组件职责

#### API Server (api-server)
- 对外暴露 gRPC `SubmitDag` 接口
- 双缓冲队列聚合请求（减少数据库写入次数）
- PostgreSQL COPY 协议批量写入任务元数据
- Redis Pipeline 批量写入 DAG 依赖关系和任务元数据
- 无状态，可水平扩展

#### Scheduler (scheduler)
- XREADGROUP 阻塞消费 Redis Stream 中的就绪任务
- 负载均衡选择 Worker（内存影子状态 + Round-Robin）
- gRPC 主动推送任务到 Worker
- 接收 Worker 汇报结果，执行 Lua 脚本进行 DAG 依赖原子更新
- DbBatcher 异步批量回写任务状态到 PostgreSQL
- XPENDING 扫描 + XCLAIM 任务故障自愈

#### Worker (worker)
- gRPC Server，暴露 `RunTask` 接口给 Scheduler
- 执行任务（目前有示例任务注册表 task_registry）
- 主动汇报执行结果（gRPC 回调 Scheduler）
- 定期心跳

#### common (共享库)
- Redis 操作封装（RedisManager: XAdd, XReadGroup, XAck, XClaim, XPending, Pipeline 等）
- 数据库连接池（DatabasePool: 基于 libpqxx 的事务回调模式）
- 线程池（ThreadPool: 固定线程 + std::queue + mutex/cv）
- Protobuf 定义（task, service, internal, error, common 等）
- 工具类（TaskSerializer, UUID 生成, Prometheus Metrics, 日志封装）

### 3.3 技术栈

| 类别 | 当前选择 | 说明 |
|------|---------|------|
| 语言 | C++17（实际用了少量 C++20 特性）+ Go（CLI） | 两套语言体系 |
| 构建 | CMake 3.20 + FetchContent | 无 vcpkg, 无 CMakePreset |
| RPC | gRPC v1.60.0（FetchContent 全量编译） | 编译极慢 |
| 序列化 | Protobuf | + gRPC 强制绑定 |
| 缓存/队列 | Redis 7.0（redis-plus-plus + hiredis） | Stream, Hash, Set, String, Lua |
| 持久化 | PostgreSQL 15（libpqxx） | COPY 协议写入 |
| 日志 | glog | Google 日志库 |
| 监控 | Prometheus（prometheus-cpp） | Pull/Push 模式 |
| 容器 | Docker + docker-compose | 一键启动集群 |
| HTTP | cpp-httplib（api-web 模块） | 实验性 REST 接口 |
| CLI | Go + cobra | 独立模块 |

### 3.4 数据流

**任务提交流程**：
1. 用户/CLI → gRPC SubmitDag → API Server
2. API Server 将请求放入 ActiveBuffer（双缓冲队列）
3. 达到阈值（1000条 或 50ms超时），swap 缓冲指针
4. PG COPY 协议写入 job + task + task_edge 表
5. Redis Pipeline 设置任务元数据 + DAG 依赖关系 + 就绪任务入 Stream

**调度流程**：
1. Scheduler XREADGROUP 阻塞读取 Redis Stream
2. 解析 Stream 信封 → 根据 task_id 从 Redis KV 取元数据
3. 负载均衡（内存影子状态 + Round-Robin）选择 Worker
4. gRPC 推送任务
5. 推送成功后 XACK 确认

**执行与反馈流程**：
1. Worker 执行任务
2. Worker 汇报结果（SUCCESS/FAILED）给 Scheduler
3. Scheduler 执行 Redis Lua 脚本：标记当前任务完成 → 递减子任务入度 → 入度归零的子任务入 Stream

**故障自愈**：
1. Scheduler 每 10s XPENDING 扫描滞留消息
2. 空闲时间 > 60s 的消息视为僵尸任务
3. XCLAIM 抢占所有权 → 重新分发

### 3.5 Protobuf 定义结构

```
src/common/proto/dts/
├── common/          resource.proto, shard.proto
├── error/           error.proto, job_error.proto, sys_error.proto
├── internal/        internal_service.proto  (Scheduler↔Worker 内部 RPC)
├── service/         task_service.proto       (对外暴露的 SubmitDag API)
└── task/            task.proto, task_state.proto
```

---

## 四、现有功能盘点

### 4.1 已完成（能跑/曾能跑）

- [x] API Server gRPC 服务（SubmitDag 接口）
- [x] 双缓冲批量写入 PostgreSQL + Redis
- [x] Scheduler Stream 消费 + 负载均衡分发
- [x] Worker 接收 + 执行 + 汇报
- [x] Redis Lua 原子 DAG 依赖更新
- [x] XPENDING/XCLAIM 故障自愈
- [x] Go CLI 提交 + 压测
- [x] Docker Compose 集群部署
- [x] Prometheus 监控指标集成

### 4.2 未完成/有问题

- [ ] 测试全部注释掉了，不可用、不可验证
- [ ] DbBatcher 在 scheduler 中只是框架，写完了但不确定是否能跑通
- [ ] DLL/dynamic loading 的任务插件机制只是预留了接口
- [ ] scheduler 的 gRPC Server（接收 Worker 结果汇报）和 gRPC Client（推送任务给 Worker）混在一起，设计混乱
- [ ] Worker 的心跳机制不够健壮
- [ ] 任务超时/重试机制未完整实现
- [ ] 无 Web UI / Dashboard
- [ ] 无操作日志/审计
- [ ] 无配置热更新

---

## 五、代码质量问题

### 5.1 构建系统

- **依赖管理混乱** — 用 FetchContent 拉 gRPC 全量源码（带子模块），cmake 编译一次要半小时。同时也用 find_package(Boost) 和 find_package(ZLIB)，策略不一致
- **无 vcpkg** — 没有 vcpkg.json manifest，依赖版本不固定
- **无 CMakePresets** — 没有 Debug/Release/ASan 等配置预设
- **build/ 目录脏** — 包含完整的 gRPC 编译缓存（_deps/），占用大量空间

### 5.2 代码风格

- 混合使用 `dts::` 和 `dts::api_server::` / `dts::scheduler::` / `dts::common::` 命名空间，但不一致
- 日志混用 `glog`（LOG_INFO）和自定义 `logger.hpp`（LOG_INFO <<），冗余
- 有些地方用 C++17 特征，有些地方用旧的 C++11 风格
- headers 里包含了大量不必要的 include

### 5.3 设计问题

- **gRPC 锁死整个项目** — 所有组件间通信都绑定 gRPC，导致编译慢、启动慢、调试困难
- **Go CLI 是孤岛** — 独立的 go.mod，手写的 .pb.go 文件（不是自动生成的），与 C++ 端脱节
- **api-web 模块处于薛定谔状态** — 存在、有 CMake 构建，但不清楚能不能工作，在 README 中未提及
- **Scheduler 职责过重** — 既做调度、又做 RPC Server、又做 DbBatcher，违背单一职责
- **测试不可用** — 有测试文件但没有构建、没有运行过
- **无 Dockerfile 优化** — Dockerfile 从零编译包括 gRPC 在内的全部依赖，镜像构建非常慢

---

## 六、项目中的"亮点"（面试可讲的技术点）

这些设计思路本身是合理的，可以作为重构时保留或改良的基础：

1. **Async Group Commit** — 双缓冲队列 + 条件变量 + 批量写入，解决了单条 INSERT 的性能瓶颈
2. **Claim Check Pattern** — Redis Stream 传 task_id 指针，Payload 存在 KV，减少网络传输
3. **Lua 原子调度** — 把 DAG 依赖检查下沉到 Redis Lua 脚本，消除分布式锁
4. **Optimistic Pre-booking** — 内存影子状态预占 Worker 负载，防止热点 Worker
5. **Write-Behind Persistence** — 先写 Redis，DbBatcher 异步攒批回写 PG，平滑数据库压力
6. **Stream PEL 故障自愈** — XPENDING + XCLAIM 实现任务零丢失

---

## 七、重构目标

1. **稳定可靠** — 有测试覆盖，能跑通端到端流程
2. **架构清晰** — 组件职责单一，依赖方向明确
3. **工程规范** — vcpkg + CMakePreset，构建可复现
4. **可面试** — 能讲清楚每一个设计决策和 trade-off
5. **开源友好** — 代码整洁、文档完整、CI 可用
