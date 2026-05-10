graph TD
    %% 定义样式
    classDef go fill:#e0f7fa,stroke:#006064,stroke-width:2px;
    classDef cpp fill:#ffecb3,stroke:#ff6f00,stroke-width:2px;
    classDef db fill:#e1bee7,stroke:#4a148c,stroke-width:2px;

    %% 角色层
    User((开发者/用户))

    %% 接入层
    subgraph Access_Layer [接入层]
        CLI[DTS-CLI <br/> (Go语言)]:::go
        API[API Server <br/> (C++ gRPC)]:::cpp
    end

    %% 核心控制层
    subgraph Control_Plane [控制面 - 大脑]
        Scheduler[Scheduler <br/> (C++ Core)]:::cpp
        Batcher(DB Batcher <br/> 批处理优化):::cpp
    end

    %% 数据层 (根据描述推断)
    subgraph Data_Layer [存储层]
        DB[(Database <br/> PG/MySQL)]:::db
        MQ[(Queue/Redis <br/> 选填:中间件)]:::db
    end

    %% 执行层
    subgraph Data_Plane [数据面 - 四肢]
        Worker1[Worker Node 1 <br/> (C++)]:::cpp
        Worker2[Worker Node 2 <br/> (C++)]:::cpp
        Registry(Task Registry <br/> 业务逻辑注册表):::cpp
    end

    %% 关系连线
    User -- "1. ./dts-cli submit" --> CLI
    CLI -- "2. gRPC SubmitTaskRequest" --> API
    API -- "3. 投递任务/写入" --> DB
    API -.-> MQ

    Scheduler -- "4. 扫描待执行任务" --> DB
    Batcher -- "批量回写状态" --> DB

    Worker1 -- "5. 长连接/Heartbeat (scheduler_client)" --> Scheduler
    Worker2 -- "拉取任务/上报结果" --> Scheduler
    
    Scheduler -- "6. 分配任务 (Dispatch)" --> Worker1

    %% 内部包含关系
    Scheduler --- Batcher
    Worker1 --- Registry