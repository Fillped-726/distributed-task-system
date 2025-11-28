cmake_minimum_required(VERSION 3.20)
project(dts_common)

# ---- 1. 确定路径 (依赖根目录 FetchContent 产生的变量) ----
# 这里的变量 protobuf_SOURCE_DIR 由根目录 FetchContent_MakeAvailable(grpc) 递归产生
if(NOT DEFINED protobuf_SOURCE_DIR)
    message(FATAL_ERROR "protobuf_SOURCE_DIR is not defined. Ensure FetchContent is executed in root CMakeLists.txt")
endif()

set(PROTOBUF_STD_INC_DIR "${protobuf_SOURCE_DIR}/src")
set(PROTO_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(PROTO_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

# ---- 2. 定义 Proto 文件 ----
set(DTS_PROTO_FILES
    dts/common/resource.proto
    dts/common/shard.proto
    dts/error/error.proto
    dts/error/job_error.proto
    dts/error/sys_error.proto
    dts/task/task_state.proto
    dts/task/task.proto
    dts/internal/internal_service.proto
    dts/service/task_service.proto
)

# ---- 3. 批量生成 Proto 代码 ----
set(PROTO_SRCS)
set(PROTO_HDRS)

foreach(PROTO_REL ${DTS_PROTO_FILES})
    set(_ABS_FILE "${PROTO_SRC_DIR}/${PROTO_REL}")
    
    # 计算文件名 (e.g. dts/task/task)
    string(REGEX REPLACE "\\.proto$" "" _FILE_WE ${PROTO_REL})
    
    set(_PB_CC   "${PROTO_GEN_DIR}/${_FILE_WE}.pb.cc")
    set(_PB_H    "${PROTO_GEN_DIR}/${_FILE_WE}.pb.h")
    set(_GRPC_CC "${PROTO_GEN_DIR}/${_FILE_WE}.grpc.pb.cc")
    set(_GRPC_H  "${PROTO_GEN_DIR}/${_FILE_WE}.grpc.pb.h")

    get_filename_component(_OUT_DIR ${_PB_CC} DIRECTORY)
    file(MAKE_DIRECTORY ${_OUT_DIR})

    add_custom_command(
        OUTPUT ${_PB_CC} ${_PB_H} ${_GRPC_CC} ${_GRPC_H}
        
        # [关键修复] 使用生成器表达式获取 protoc 可执行文件
        COMMAND $<TARGET_FILE:protobuf::protoc>
        
        ARGS --proto_path=${PROTO_SRC_DIR}
             # [关键修复] 指定标准库路径 (解决 google/protobuf/struct.proto not found)
             --proto_path=${PROTOBUF_STD_INC_DIR}
             --cpp_out=${PROTO_GEN_DIR}
             --grpc_out=${PROTO_GEN_DIR}
             # [关键修复] 使用生成器表达式获取 gRPC 插件路径
             --plugin=protoc-gen-grpc=$<TARGET_FILE:grpc_cpp_plugin>
             ${_ABS_FILE}
             
        # [关键修复] 显式依赖这两个 Target，确保它们先被编译出来
        DEPENDS ${_ABS_FILE} protobuf::protoc grpc_cpp_plugin
        
        COMMENT "Generating gRPC files for ${_FILE_WE}"
        VERBATIM
    )

    list(APPEND PROTO_SRCS ${_PB_CC} ${_GRPC_CC})
    list(APPEND PROTO_HDRS ${_PB_H} ${_GRPC_H})
endforeach()

# ---- 4. 定义 dts_common 库 ----
add_library(dts_common STATIC
    src/logger.cpp
    src/config.cpp
    src/exceptions.cpp
    src/converters.cpp
    ${PROTO_SRCS}
)

# ---- 5. 包含路径 ----
target_include_directories(dts_common
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${PROTO_GEN_DIR}>
        $<INSTALL_INTERFACE:include>
)

# ---- 6. 链接依赖 ----
# 注意：使用你要求的 grpc++ (非 namespaced)
target_link_libraries(dts_common
    PUBLIC
        protobuf::libprotobuf
        grpc++              
        nlohmann_json::nlohmann_json
        glog::glog
        libpqxx::pqxx
        Boost::system
        Boost::filesystem
        Boost::uuid
)

# ---- 7. 安装规则 ----
include(GNUInstallDirs)
install(TARGETS dts_common
    EXPORT DtsTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY ${PROTO_GEN_DIR}/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} FILES_MATCHING PATTERN "*.h")