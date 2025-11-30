cmake_minimum_required(VERSION 3.20)
project(DistributedTaskSystem LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ---------- 强制隔离系统包 (针对 gRPC/Proto) ----------
set(CMAKE_DISABLE_FIND_PACKAGE_Protobuf TRUE)
set(CMAKE_DISABLE_FIND_PACKAGE_absl TRUE)
set(CMAKE_DISABLE_FIND_PACKAGE_c-ares TRUE)
set(CMAKE_DISABLE_FIND_PACKAGE_re2 TRUE)
set(CMAKE_DISABLE_FIND_PACKAGE_ZLIB TRUE)

# ---------- sanitizer / tidy ----------
option(COMMON_ENABLE_ASAN "enable AddressSanitizer" OFF)
if (COMMON_ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

option(COMMON_ENABLE_TIDY "enable clang-tidy" OFF)

include(FetchContent)

# ---------- 1. gRPC + Protobuf (核心依赖) ----------
set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
# ... (保持你原有的插件设置不变) ...
set(gRPC_BUILD_GRPC_CPP_PLUGIN ON  CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_NODE_PLUGIN   OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_PHP_PLUGIN    OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF CACHE BOOL "" FORCE)
set(gRPC_BUILD_GRPC_RUBY_PLUGIN   OFF CACHE BOOL "" FORCE)

set(protobuf_BUILD_PROTOC_BINARIES ON  CACHE BOOL "" FORCE)
set(protobuf_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(protobuf_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)

# ... (保持 gRPC provider 设置不变) ...
set(gRPC_PROTOBUF_PROVIDER "module" CACHE STRING "" FORCE)
set(gRPC_ABSL_PROVIDER     "module" CACHE STRING "" FORCE)
set(gRPC_RE2_PROVIDER      "module" CACHE STRING "" FORCE)
set(gRPC_CARES_PROVIDER    "module" CACHE STRING "" FORCE)
set(gRPC_ZLIB_PROVIDER     "module" CACHE STRING "" FORCE)
set(gRPC_INSTALL OFF CACHE BOOL "" FORCE)
set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
set(absl_INSTALL OFF CACHE BOOL "" FORCE)
set(utf8_range_INSTALL OFF CACHE BOOL "" FORCE)
set(re2_INSTALL OFF CACHE BOOL "" FORCE)
set(zlib_INSTALL OFF CACHE BOOL "" FORCE)
set(c-ares_INSTALL OFF CACHE BOOL "" FORCE)

# 2. 禁止安装其他库
set(glog_INSTALL OFF CACHE BOOL "" FORCE)
set(json_INSTALL OFF CACHE BOOL "" FORCE) # nlohmann_json
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
set(benchmark_INSTALL_DOCS OFF CACHE BOOL "" FORCE)

# 3. [特例] Protobuf 的特殊开关
# 防止 protobuf 尝试导出它自己的 target
set(protobuf_BUILD_EXPORT OFF CACHE BOOL "" FORCE) 
set(protobuf_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    grpc
    GIT_REPOSITORY https://github.com/grpc/grpc.git
    GIT_TAG        v1.60.0
    GIT_SHALLOW    FALSE
    GIT_SUBMODULES_RECURSE TRUE
)
FetchContent_MakeAvailable(grpc)

# [优化] 使用生成器表达式获取路径，比硬编码路径更安全
# 注意：这俩变量在此时是字符串，真正获取路径是在 add_custom_command 执行时
set(PROTOC_EXEC "$<TARGET_FILE:protobuf::protoc>")
set(GRPC_PLUGIN "$<TARGET_FILE:grpc_cpp_plugin>")

# ---------- 2. 工具库 (JSON, Redis, GTest) ----------

# [优化] nlohmann_json 改为 FetchContent
FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG        v3.11.3
)
# nlohmann_json 提供了 nlohmann_json::nlohmann_json 目标
FetchContent_MakeAvailable(nlohmann_json)

FetchContent_Declare(
    hiredis
    GIT_REPOSITORY https://github.com/redis/hiredis.git
    GIT_TAG        v1.2.0
    CMAKE_ARGS     -DBUILD_TESTS=OFF -DBUILD_EXAMPLES=OFF
)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.17.0
    CMAKE_ARGS     -DBUILD_GMOCK=ON -DINSTALL_GTEST=OFF
)
FetchContent_MakeAvailable(hiredis googletest)

# ---------- 3. glog ----------
set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)
set(WITH_UNWIND OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    glog
    GIT_REPOSITORY https://github.com/google/glog.git
    GIT_TAG        v0.6.0
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(glog)

# ---------- 4. Web & DB ----------

# httplib
FetchContent_Declare(
    httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG        v0.14.3  
)
FetchContent_MakeAvailable(httplib)

# libpqxx (C++ Postgres)
# [重要] libpqxx 编译需要依赖底层的 libpq (C库)
# 所以我们必须先 find_package(PostgreSQL)，让 libpqxx 能找到它
find_package(PostgreSQL REQUIRED)

FetchContent_Declare(
    libpqxx
    GIT_REPOSITORY https://gitee.com/mirrors/libpqxx.git
    GIT_TAG        7.9.0 
    CMAKE_ARGS     
        -DBUILD_TESTS=OFF 
        -DCMAKE_CXX_STANDARD=17
        -DCMAKE_CXX_STANDARD_REQUIRED=ON
        -DSKIP_BUILD_TEST=ON
)
FetchContent_MakeAvailable(libpqxx)

# ---------- 5. 系统级库 (保持 find_package) ----------

# Boost (太大，不建议 Fetch)
find_package(Boost 1.83 REQUIRED COMPONENTS system filesystem)

# 手动处理 Boost UUID
find_path(BOOST_UUID_INCLUDE_DIR boost/uuid/uuid.hpp
    PATHS ${Boost_INCLUDE_DIRS}
    NO_DEFAULT_PATH
)
if(NOT BOOST_UUID_INCLUDE_DIR)
    message(FATAL_ERROR "Boost UUID headers not found!")
endif()

if(NOT TARGET Boost::uuid)
    add_library(Boost::uuid INTERFACE IMPORTED)
    set_target_properties(Boost::uuid PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${Boost_INCLUDE_DIRS}"
    )
endif()

# 线程支持
find_package(Threads REQUIRED)

# ---------- 业务与测试 ----------
add_subdirectory(src)

enable_testing()
add_subdirectory(tests)

# ---------- 质量工具 ----------
if (COMMON_ENABLE_TIDY)
    set_target_properties(common PROPERTIES
        CXX_CLANG_TIDY "clang-tidy;-checks=bugprone-*,performance-*,clang-analyzer-*"
    )
endif()