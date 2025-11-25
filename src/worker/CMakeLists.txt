# 这里的版本号继承自根目录，通常不需要重新指定，但为了严谨可以写上
cmake_minimum_required(VERSION 3.20)

# 定义项目名称
project(dts_worker)

# 1. 收集源文件
# --------------------------------------------------------
set(WORKER_SRCS
    src/main.cpp
    src/worker_node.cpp
    src/worker_service.cpp
    src/scheduler_client.cpp
    src/task_registry.cpp
    src/tasks.cpp
)

# 2. 生成可执行文件
# --------------------------------------------------------
add_executable(dts_worker ${WORKER_SRCS})

# 3. 设置 C++ 标准
# --------------------------------------------------------
# 我们使用了 std::shared_mutex (C++17) 和 std::invoke_result (C++17)
target_compile_features(dts_worker PRIVATE cxx_std_17)

# 4. 设置头文件搜索路径
# --------------------------------------------------------
target_include_directories(dts_worker PRIVATE
    include                  # 允许引用 "worker_node.h" 等
    ${CMAKE_SOURCE_DIR}      # 允许从根目录引用，例如 "common/logger.hpp" 或 "worker/include/..."
)

# 5. 链接依赖库
# --------------------------------------------------------
# 注意：这里假设你在根目录已经定义了 proto 相关的库目标 (target)
# 如果你的 Proto 库名字不是 dts_proto_lib，请修改为实际名字
target_link_libraries(dts_worker PRIVATE
    common_core
    grpc++             # gRPC C++ 库
    grpc++_reflection  # 支持 gRPC 反射 (可选，方便调试)            
    nlohmann_json::nlohmann_json # JSON 库
    Threads::Threads         # 线程库 (pthread)
)

# (可选) 如果需要安装到系统目录
# install(TARGETS dts_worker RUNTIME DESTINATION bin)