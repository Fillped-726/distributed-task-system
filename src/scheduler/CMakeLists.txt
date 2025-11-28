cmake_minimum_required(VERSION 3.20)
project(dts_scheduler LANGUAGES CXX)

#----------------------------------------------------------
# 1. 核心业务库 (dts_scheduler_core)
#    职责：读取 DB，管理 DAG 状态机，派发任务给 Worker
#----------------------------------------------------------
add_library(dts_scheduler_core STATIC
    src/scheduler_service_impl.cpp
    src/task_repository.cpp
    src/worker_manager.cpp
    src/scheduler_loop.cpp
)

# C++20 标准
target_compile_features(dts_scheduler_core PUBLIC cxx_std_20)

# 头文件路径
target_include_directories(dts_scheduler_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# 链接依赖
target_link_libraries(dts_scheduler_core
    PUBLIC
        dts_common          # 基础库 (Proto, Logger, UUID)
        libpqxx::pqxx       # 数据库驱动 (Scheduler 必须读写 DB)
        nlohmann_json::nlohmann_json
        

        grpc++
)

#----------------------------------------------------------
# 2. 可执行文件 (入口 Main)
#----------------------------------------------------------
add_executable(dts_scheduler src/main.cpp)

# 链接核心库
target_link_libraries(dts_scheduler
    PRIVATE
        dts_scheduler_core
)

#----------------------------------------------------------
# 3. 安装规则
#----------------------------------------------------------
include(GNUInstallDirs)

install(TARGETS dts_scheduler
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)