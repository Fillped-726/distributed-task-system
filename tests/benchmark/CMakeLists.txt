cmake_minimum_required(VERSION 3.20)
project(dts_benchmark)

# -------------------------------------------------------------
# 1. 编译选项优化
# -------------------------------------------------------------
# 压测工具必须全速运行，强制开启 O3 优化 (除非显式指定了 Debug)
if(NOT CMAKE_BUILD_TYPE MATCHES "Debug")
    add_compile_options(-O3)
endif()

# -------------------------------------------------------------
# 2. 定义可执行文件
# -------------------------------------------------------------
# 这里假设你把合并后的代码保存为了 benchmark_unified.cpp
add_executable(dts_benchmark 
    main.cpp
    src/metrics.cpp
    src/submitter.cpp
    src/monitor.cpp)

# -------------------------------------------------------------
# 3. 头文件路径
# -------------------------------------------------------------
target_include_directories(dts_benchmark PRIVATE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

# -------------------------------------------------------------
# 4. 链接依赖 (直接使用根目录 FetchContent 提供的 Target)
# -------------------------------------------------------------
target_link_libraries(dts_benchmark PRIVATE

    dts_client
    pqxx
    # C. gRPC & Protobuf (来自根目录 FetchContent grpc)
    grpc++
    protobuf::libprotobuf
    # D. 日志库 (来自根目录 FetchContent glog)
    glog::glog
    # E. 系统库
    Threads::Threads
)

target_compile_options(dts_benchmark PRIVATE
    # [-g] 必须：生成 Debug 符号
    -g
    # [-fno-omit-frame-pointer] 必须：禁止栈帧指针省略，Profiler 依赖它来展开调用栈
    -fno-omit-frame-pointer 
    # [-O2/O3] 必须：在 profiling 时依然保持优化，否则测出来的是 Debug 性能
    -O2
)
