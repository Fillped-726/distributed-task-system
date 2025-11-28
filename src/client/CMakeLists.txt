cmake_minimum_required(VERSION 3.20)
project(dts_client)

# ---- 定义客户端库 ----
add_library(dts_client STATIC
    src/grpc_client.cpp
    src/dag_builder.cpp
)

# ---- 头文件暴露 ----
target_include_directories(dts_client
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

# ---- 链接依赖 ----
target_link_libraries(dts_client
    PUBLIC
        dts_common   # 继承 common 的所有 Proto 定义和工具
    PRIVATE
        # 如果 client 内部有些特殊处理不需要暴露给使用者，写在 PRIVATE
)