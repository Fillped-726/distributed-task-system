cmake_minimum_required(VERSION 3.20)
project(DistributedTaskSystem LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ---------- 强制隔离系统包 ----------
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

# ---------- FetchContent ----------
include(FetchContent)

# gRPC + protobuf + abseil + ...
set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
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

set(gRPC_PROTOBUF_PROVIDER "module" CACHE STRING "" FORCE)
set(gRPC_ABSL_PROVIDER     "module" CACHE STRING "" FORCE)
set(gRPC_RE2_PROVIDER      "module" CACHE STRING "" FORCE)
set(gRPC_CARES_PROVIDER    "module" CACHE STRING "" FORCE)
set(gRPC_ZLIB_PROVIDER     "module" CACHE STRING "" FORCE)
set(gRPC_INSTALL OFF CACHE BOOL "" FORCE)
set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
set(absl_INSTALL OFF CACHE BOOL "" FORCE)
set(utf8_range_INSTALL OFF CACHE BOOL "" FORCE)   # protobuf 依赖
set(cares_INSTALL OFF CACHE BOOL "" FORCE)        # grpc 依赖
set(re2_INSTALL OFF CACHE BOOL "" FORCE)
set(zlib_INSTALL OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    grpc
    GIT_REPOSITORY https://github.com/grpc/grpc.git
    GIT_TAG        v1.60.0
    GIT_SHALLOW    FALSE
    GIT_SUBMODULES_RECURSE TRUE
)
FetchContent_MakeAvailable(grpc)

# hiredis
FetchContent_Declare(
    hiredis
    GIT_REPOSITORY https://github.com/redis/hiredis.git
    GIT_TAG        v1.2.0
    CMAKE_ARGS     -DBUILD_TESTS=OFF -DBUILD_EXAMPLES=OFF
)
# googletest
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.17.0
    CMAKE_ARGS     -DBUILD_GMOCK=ON -DINSTALL_GTEST=OFF
)
FetchContent_MakeAvailable(hiredis googletest)

# ---------- glog ----------
# 1. 先关掉 glog 自带的 unwinder / gflags 等可能踩坑的开关
set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)   # 你有自己的 gflags 再用
set(WITH_UNWIND OFF CACHE BOOL "" FORCE)   # 避免依赖 libunwind
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    glog
    GIT_REPOSITORY https://github.com/google/glog.git
    GIT_TAG        v0.6.0          # 2023 LTS，稳定且 C++20 友好
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(glog)

# 2. 把 glog 的头文件和库一次性暴露给全局
#    下面两个变量是 glog 官方导出的，保证版本兼容
set(GLOG_INCLUDE_DIR "${glog_SOURCE_DIR}/src" "${glog_BINARY_DIR}")  # 公共头文件
set(GLOG_LIBRARY glog::glog) 

find_package(Boost 1.80 REQUIRED COMPONENTS system filesystem)
find_package(nlohmann_json REQUIRED)
find_package(PostgreSQL REQUIRED)

set(PROTOC_EXEC  "${grpc_BINARY_DIR}/third_party/protobuf/protoc")
set(GRPC_PLUGIN  "${grpc_BINARY_DIR}/grpc_cpp_plugin")

# 拉取 cpp-httplib
FetchContent_Declare(
    httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git

    GIT_TAG v0.14.3  
)
FetchContent_MakeAvailable(httplib)

# --- 拉取 libpqxx C++ 库 ---
FetchContent_Declare(
    libpqxx
    GIT_REPOSITORY https://gitee.com/mirrors/libpqxx.git
    GIT_TAG        7.9.0 
    CMAKE_ARGS     
        -DBUILD_TESTS=OFF 
        

        -DCMAKE_CXX_STANDARD=17
        -DCMAKE_CXX_STANDARD_REQUIRED=ON

)

set(FETCHCONTENT_OVERRIDE_FIND_PACKAGE_PostgreSQL TRUE)
FetchContent_MakeAvailable(libpqxx)

# ---------- 业务 ----------
add_subdirectory(src/common)
add_subdirectory(src/api-server)
add_subdirectory(src/worker/task-executor)
add_subdirectory(src/client)
add_subdirectory(src/api-web)

# ---------- 测试 ----------
enable_testing()
add_subdirectory(tests)

# ---------- 质量工具 ----------
if (COMMON_ENABLE_TIDY)
    set_target_properties(common PROPERTIES
        CXX_CLANG_TIDY "clang-tidy;-checks=bugprone-*,performance-*,clang-analyzer-*"
    )
endif()
