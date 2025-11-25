cmake_minimum_required(VERSION 3.20)
project(api-server LANGUAGES CXX)

#----------------------------------------------------------
# 0. 通用变量
#----------------------------------------------------------
set(TARGET_NAME api-server)

#----------------------------------------------------------
# 1. 先建 OBJECT 库，放所有实现（不含 main）
#----------------------------------------------------------
add_library(api_server_lib OBJECT
    src/api_server.cpp
    src/task_submitter.cpp
)

target_compile_features(api_server_lib PUBLIC cxx_std_20)

# 头文件搜索路径
target_include_directories(api_server_lib
    SYSTEM PRIVATE
        ${grpc_SOURCE_DIR}/third_party/abseil-cpp
        ${grpc_SOURCE_DIR}/third_party/protobuf/src
        ${grpc_BINARY_DIR}/third_party/protobuf/src
        ${grpc_SOURCE_DIR}/include
        ${grpc_BINARY_DIR}/include
        $<TARGET_PROPERTY:common,INTERFACE_INCLUDE_DIRECTORIES>
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/api-server>
)

# 依赖库
target_link_libraries(api_server_lib
    PUBLIC
        common
        common_core   # 已含 protobuf
        grpc++
        glog::glog
        Boost::system
        Boost::uuid
        Boost::filesystem
        nlohmann_json::nlohmann_json
        pqxx
)

#----------------------------------------------------------
# 2. 可执行文件，只当入口
#----------------------------------------------------------
add_executable(${TARGET_NAME}
    src/main.cpp
)
target_link_libraries(${TARGET_NAME} PRIVATE api_server_lib)

#----------------------------------------------------------
# 3. 可选 clang-tidy
#----------------------------------------------------------
if(COMMON_ENABLE_TIDY)
    set_target_properties(${TARGET_NAME} PROPERTIES
        CXX_CLANG_TIDY "clang-tidy;-checks=bugprone-*,performance-*,clang-analyzer-*"
    )
endif()

#----------------------------------------------------------
# 4. 安装
#----------------------------------------------------------
install(TARGETS ${TARGET_NAME}  # 只装可执行文件
    RUNTIME DESTINATION bin
)
install(DIRECTORY include/
    DESTINATION include/api-server
    FILES_MATCHING PATTERN "*.hpp"
)
install(DIRECTORY include/
    DESTINATION include/api-server
    FILES_MATCHING PATTERN "*.hpp"
)
