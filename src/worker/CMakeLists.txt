cmake_minimum_required(VERSION 3.20)
project(dts_worker LANGUAGES CXX)

#----------------------------------------------------------
# 1. 核心业务库 (dts_worker_core)
#    包含 Worker 的所有逻辑，但不包含 main 函数
#----------------------------------------------------------
add_library(dts_worker_core STATIC
    src/worker_node.cpp
    src/worker_service.cpp
    src/scheduler_client.cpp
    src/task_registry.cpp
)

# 统一使用 C++20
target_compile_features(dts_worker_core PUBLIC cxx_std_20)

# 头文件路径
# 1. 让 Worker 内部代码能找到 "worker/include/..."
# 2. 继承 dts_common 后，能自动找到 "common/include/..."
target_include_directories(dts_worker_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# 链接依赖
target_link_libraries(dts_worker_core
    PUBLIC
        dts_common          # 核心基础库 (Proto, Logger, ThreadPool)
        nlohmann_json::nlohmann_json
        
        # Worker 既是 Server (接收 RunTask) 也是 Client (发送 Heartbeat)
        # 所以需要 gRPC
        grpc++
        grpc++_reflection 
)

#----------------------------------------------------------
# 2. 可执行文件 (入口 Main)
#----------------------------------------------------------
add_executable(dts_worker 

    src/tasks.cpp
    src/main.cpp

)

# 只需要链接核心库
target_link_libraries(dts_worker
    PRIVATE
        dts_worker_core
)

#----------------------------------------------------------
# 3. 安装规则
#----------------------------------------------------------
include(GNUInstallDirs)

install(TARGETS dts_worker
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)