cmake_minimum_required(VERSION 3.20)
project(Common LANGUAGES CXX)

# ---- 2. 路径宏 ----
set(PROTO_ROOT      "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(PROTO_OUT_ROOT  "${CMAKE_CURRENT_BINARY_DIR}/src/common/generated")
file(MAKE_DIRECTORY "${PROTO_OUT_ROOT}")

# import 根目录
list(APPEND PROTO_IMPORT_DIRS "${PROTO_ROOT}")
# 让 dts/common/resource.proto 能被 import
list(APPEND PROTO_IMPORT_DIRS "${PROTO_ROOT}")

# ---- 3. 封装函数（与之前相同，但不再 find_package） ----
function(dts_proto_target TARGET_NAME PROTO_FILE)
    get_filename_component(_ABS_PROTO "${PROTO_FILE}" ABSOLUTE)

    file(RELATIVE_PATH _REL_PROTO "${PROTO_ROOT}" "${_ABS_PROTO}")

    string(REGEX REPLACE "\\.proto$" "" _REL_PROTO_WE "${_REL_PROTO}")

    set(_PB_HDR "${PROTO_OUT_ROOT}/${_REL_PROTO_WE}.pb.h")
    set(_PB_SRC "${PROTO_OUT_ROOT}/${_REL_PROTO_WE}.pb.cc")
    set(_GRPC_HDR "${PROTO_OUT_ROOT}/${_REL_PROTO_WE}.grpc.pb.h")
    set(_GRPC_SRC "${PROTO_OUT_ROOT}/${_REL_PROTO_WE}.grpc.pb.cc")

    get_filename_component(_OUT_DIR "${_PB_SRC}" DIRECTORY)
    file(MAKE_DIRECTORY "${_OUT_DIR}")

    add_custom_command(
        OUTPUT  ${_PB_SRC} ${_PB_HDR} ${_GRPC_SRC} ${_GRPC_HDR}
        COMMAND protobuf::protoc
        ARGS --proto_path=${PROTO_ROOT}
             --proto_path=${protobuf_SOURCE_DIR}/src
             --cpp_out=${PROTO_OUT_ROOT}
             --grpc_out=${PROTO_OUT_ROOT}
             --plugin=protoc-gen-grpc=$<TARGET_FILE:grpc_cpp_plugin>
             "${_ABS_PROTO}"
        DEPENDS "${_ABS_PROTO}" protobuf::protoc grpc_cpp_plugin
        COMMENT "Protoc ${_REL_PROTO_WE}" # 使用新变量
        VERBATIM
    )

    add_library(${TARGET_NAME} INTERFACE)
    target_include_directories(${TARGET_NAME} INTERFACE
        "$<BUILD_INTERFACE:${PROTO_OUT_ROOT}>"
        "$<INSTALL_INTERFACE:include>"
    )
    target_link_libraries(${TARGET_NAME} INTERFACE
        protobuf::libprotobuf
        grpc++
    )
    add_custom_target(${TARGET_NAME}.sources SOURCES
        ${_PB_HDR} ${_PB_SRC} ${_GRPC_HDR} ${_GRPC_SRC}
    )
endfunction()

set(_ALL_PROTO_SRCS)
set(_ALL_PROTO_HDRS)
# ---- 4. 逐个 proto 建立 target ----
dts_proto_target(dts.common.resource   proto/dts/common/resource.proto)
list(APPEND _ALL_PROTO_SRCS "${PROTO_OUT_ROOT}/dts/common/resource.pb.cc") 
list(APPEND _ALL_PROTO_HDRS "${PROTO_OUT_ROOT}/dts/common/resource.pb.h")

dts_proto_target(dts.common.shard      proto/dts/common/shard.proto)
list(APPEND _ALL_PROTO_SRCS "${PROTO_OUT_ROOT}/dts/common/shard.pb.cc") 
list(APPEND _ALL_PROTO_HDRS "${PROTO_OUT_ROOT}/dts/common/shard.pb.h")

dts_proto_target(dts.error.error       proto/dts/error/error.proto)
list(APPEND _ALL_PROTO_SRCS "${PROTO_OUT_ROOT}/dts/error/error.pb.cc") 
list(APPEND _ALL_PROTO_HDRS "${PROTO_OUT_ROOT}/dts/error/error.pb.h")

dts_proto_target(dts.error.job_error   proto/dts/error/job_error.proto)
list(APPEND _ALL_PROTO_SRCS "${PROTO_OUT_ROOT}/dts/error/job_error.pb.cc") 
list(APPEND _ALL_PROTO_HDRS "${PROTO_OUT_ROOT}/dts/error/job_error.pb.h")

dts_proto_target(dts.error.sys_error   proto/dts/error/sys_error.proto)
list(APPEND _ALL_PROTO_SRCS "${PROTO_OUT_ROOT}/dts/error/sys_error.pb.cc") 
list(APPEND _ALL_PROTO_HDRS "${PROTO_OUT_ROOT}/dts/error/sys_error.pb.h")

dts_proto_target(dts.task.state        proto/dts/task/task_state.proto)
list(APPEND _ALL_PROTO_SRCS "${PROTO_OUT_ROOT}/dts/task/task_state.pb.cc")
list(APPEND _ALL_PROTO_HDRS "${PROTO_OUT_ROOT}/dts/task/task_state.pb.h")

dts_proto_target(dts.task              proto/dts/task/task.proto)
list(APPEND _ALL_PROTO_SRCS "${PROTO_OUT_ROOT}/dts/task/task.pb.cc")
list(APPEND _ALL_PROTO_HDRS "${PROTO_OUT_ROOT}/dts/task/task.pb.h")

dts_proto_target(dts.service.task      proto/dts/service/task_service.proto)
list(APPEND _ALL_PROTO_SRCS "${PROTO_OUT_ROOT}/dts/service/task_service.pb.cc")
list(APPEND _ALL_PROTO_SRCS "${PROTO_OUT_ROOT}/dts/service/task_service.grpc.pb.cc") 
list(APPEND _ALL_PROTO_HDRS "${PROTO_OUT_ROOT}/dts/service/task_service.pb.h")
list(APPEND _ALL_PROTO_HDRS "${PROTO_OUT_ROOT}/dts/service/task_service.grpc.pb.h") 

# ---- 5. 建立依赖链 ----
target_link_libraries(dts.task         INTERFACE dts.common.resource dts.task.state)
target_link_libraries(dts.service.task INTERFACE dts.task)

# ---- 6. INSTALL / EXPORT （2025 必备） ----
include(GNUInstallDirs)
install(DIRECTORY "${PROTO_OUT_ROOT}/"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
        FILES_MATCHING PATTERN "*.h")       

# ---------- 库 ----------
add_library(common_core STATIC
    src/logger.cpp
    src/config.cpp
    src/exceptions.cpp
    src/thread_pool.cpp
    src/converters.cpp
    ${_ALL_PROTO_SRCS}
)

# 关键：PIC + 导出头文件 + 三方 include
set_target_properties(common_core PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_include_directories(common_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${PROTO_OUT_ROOT}>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/third_party/protobuf/src>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/third_party/abseil-cpp>
    # 安装头文件放到 common/ 子目录，避免冲突
        $<INSTALL_INTERFACE:include/common>   
)

target_link_libraries(common_core
    PUBLIC
        Boost::system
        Boost::filesystem
        nlohmann_json::nlohmann_json
        glog::glog
        dts.common.resource
        dts.task.state
        dts.task
        dts.service.task
    PRIVATE
        protobuf::libprotobuf
        grpc++
)

# ---------- 接口库（只导出头文件 + 依赖） ----------
add_library(common INTERFACE)
target_link_libraries(common INTERFACE common_core)
target_include_directories(common INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${PROTO_BINARY_DIR}>
    $<INSTALL_INTERFACE:include/common>
)

# ---------- install ----------
# 把静态库 + 头文件 + 导出文件一起装
install(TARGETS common_core common
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin
        # 关键：不写 EXPORT 参数！
)

install(DIRECTORY include/
        DESTINATION include/common
        FILES_MATCHING PATTERN "*.hpp"
)
