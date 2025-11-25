# --- 1. 项目基本设置 ---

# 设置 CMake 最低版本 (与你的根 CMakeLists.txt 保持一致)
cmake_minimum_required(VERSION 3.20)

# (可选) 在子目录中设置 C++ 标准是一个好习惯
# 确保与根项目一致
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- 2. 查找我们所有的源文件 ---

file(GLOB_RECURSE SCHEDULER_SOURCES
    "src/*.cpp"
)

# --- 3. 定义可执行文件 ---
# (现在它会使用 *所有* 的 .cpp 文件)
add_executable(scheduler_server ${SCHEDULER_SOURCES})


# --- 4. 指定头文件搜索路径 ---

target_include_directories(scheduler_server
    PRIVATE
        "include/"
        
)

# --- 5. 链接依赖库 ---

target_link_libraries(scheduler_server
    PRIVATE
        # gRPC 库
        common_core
        api_server_lib
        grpc++
        grpc++_reflection
        # Protobuf 库
        protobuf::libprotobuf
        # PostgreSQL 库 (libpqxx)
        pqxx
        # Pthreads (用于 std::thread)
        Threads::Threads
)