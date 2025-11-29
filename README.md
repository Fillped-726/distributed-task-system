# **Distributed Task Scheduling System (DTS)**

一个基于 C++20 微服务架构的高可用分布式任务调度系统，支持 DAG 任务依赖编排与故障自愈。

## **📖 项目简介 (Introduction)**

**DTS (Distributed Task Scheduling System)** 是一个高性能的分布式调度平台原型，旨在解决大规模离线任务调度中的复杂依赖管理与高可用问题。

系统基于 **Master-Worker** 架构设计，核心模块采用 **gRPC** 通信。支持 **DAG (有向无环图)** 拓扑排序与并发执行，利用 **PostgreSQL** 事务特性保证了任务状态流转的强一致性 (ACID)，并实现了 Worker 节点的故障自动检测与任务重试 (Failover)。

## **✨ 核心特性 (Key Features)**

* **DAG 依赖编排**: 支持复杂的任务依赖网络，基于 **入度表 (Indegree Table)** 算法实现高效的拓扑排序与并行分发。  
* **高可用与故障恢复 (Fault Tolerance)**:  
  * **心跳检测**: Scheduler 维护 Worker 存活状态，自动剔除僵死节点。  
  * **自动重入队 (Requeue)**: 当 Worker 宕机时，其名下 RUNNING 任务会被自动回滚并重新调度，确保任务零丢失。  
* **高性能通信**: 全链路采用 **gRPC (Protobuf)**，相比 RESTful 接口显著降低内部调用延迟。  
* **自研数据库连接池**: 基于 libpqxx 实现线程安全的连接池，支持 **Execute-Around** 事务模板与断线自动重连 (Self-Healing)。  
* **现代化工程**: 使用 Modern CMake 管理依赖 (FetchContent)，GoogleTest 单元测试覆盖率 \> 80%，支持 Docker 容器化部署。

## **🏗 系统架构 (Architecture)**

### **1\. 微服务拓扑图**

```mermaid
graph TD
    User[Client / Web UI] -->|HTTP JSON| Gateway[API Gateway]
    
    %% 变更点：Gateway 指向 API Server，不再直接给 Scheduler
    APIWeb -->|RPC / HTTP| APIServer[API Server]
    
    %% 变更点：API Server 将任务存入 DB
    APIServer -->|Insert Task| DB[(PostgreSQL)]
    
    %% 变更点：Scheduler 从 DB 获取数据
    Scheduler[Scheduler Core] -->|Poll / Fetch| DB
    
    subgraph Service Mesh
        Scheduler -->|gRPC / Assign| W1[Worker Node 1]
        Scheduler -->|gRPC / Assign| W2[Worker Node 2]
    end

    W1 -.->|Update Status| Scheduler
    W2 -.->|Update Status| Scheduler
    
    %% Scheduler 更新 DB 状态
    Scheduler -->|Update State| DB
```

### **2\. 任务状态流转机 (State Machine)**

```mermaid
stateDiagram-v2
    [*] --> PENDING: Submit
    PENDING --> RUNNING: Scheduler Dispatch
    RUNNING --> SUCCESS: Worker Callback
    RUNNING --> FAILED: Worker Callback

    RUNNING --> PENDING: ⚠️ Worker Crash (Fault Tolerance)
    FAILED --> PENDING: Manual Retry

    SUCCESS --> [*]
```

## **🛠 技术栈 (Tech Stack)**

| 类别 (Category) | 技术 (Technology) |
| :---- | :---- |
| **Language** | C++20 (Concepts, Smart Pointers, Lambda) |
| **Network** | gRPC, Protobuf, cpp-httplib |
| **Database** | PostgreSQL (libpqxx driver) |
| **Concurrency** | ThreadPool, std::mutex, std::condition\_variable |
| **Build & Test** | CMake, GoogleTest (GTest), GLog |
| **DevOps** | Docker, Docker Compose |

## **💻 核心实现细节 (Implementation Details)**

### **高性能数据库连接池 (DB Connection Pool)**

为了在高并发场景下复用 TCP 连接，利用 C++ RAII 机制与 std::condition\_variable 实现了线程安全的连接池。  
特别设计了 ExecuteTx 模板方法，封装了事务的开启、提交与异常回滚逻辑，极大降低了业务代码耦合度：
```cpp  
// 事务模板方法 (Simplified)  
void ExecuteTx(const std::function\<void(pqxx::work&)\>& tx\_logic) {  
    auto conn \= AcquireConnection(); // 阻塞式获取连接  
    bool conn\_broken \= false;  
    try {  
        pqxx::work tx(\*conn);  
        tx\_logic(tx); // 执行具体的业务 SQL  
        tx.commit();  
    } catch (const pqxx::broken\_connection& e) {  
        conn\_broken \= true; // 标记连接损坏  
        throw;  
    }   
    // 归还连接 (内部处理断线重连)  
    ReleaseConnection(std::move(conn), conn\_broken);  
}
```

### **故障恢复机制 (Fault Tolerance)**

Scheduler 包含一个 后台巡检线程 (Patrol Thread)，定期检查 Worker 的最后心跳时间。  
一旦发现 Worker 超时（默认 30s），系统将执行以下原子操作：

1. 将该 Worker 标记为 Offline。  
2. 执行 SQL: UPDATE task SET state=PENDING WHERE worker\_id=xxx AND state=RUNNING。  
3. 任务被释放回公共池，等待下一次调度。

## **🚀 快速开始 (Getting Started)**

### **前置要求**

* Linux (Ubuntu 20.04+) or WSL2  
* CMake 3.15+  
* PostgreSQL 12+

### **1\. 编译项目**

mkdir build && cd build  
cmake ..  
make \-j4

### **2\. 启动集群 (Docker 方式)**

(推荐) 无需配置本地环境，一键拉起 PG、Scheduler 和 Worker。

docker-compose up \-d \--build

## **📊 性能表现 (Performance)**

*测试环境：Intel i7-12700H, 16GB RAM*

* **并发调度**: 单 Scheduler 支持 500+ QPS 任务分发。  
* **调度延迟**: 平均调度延迟 \< 10ms。

**Author:** \[郝光磊\]