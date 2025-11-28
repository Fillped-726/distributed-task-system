# 1. 引入 GTest (如果根目录没引入，这里需要写；如果根目录引入了，这里可以直接用)
# 假设根目录已经 FetchContent 了 GTest，这里直接配置查找
find_package(GTest REQUIRED)

# 2. 包含公共头文件路径 (所有子测试都需要)
include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/src/common/include
    ${CMAKE_SOURCE_DIR}/src/scheduler/include
    ${CMAKE_SOURCE_DIR}/src/worker/include
)

# 3. 添加子目录
add_subdirectory(common)
add_subdirectory(scheduler)
# add_subdirectory(worker) # 等你写 worker 测试时再打开