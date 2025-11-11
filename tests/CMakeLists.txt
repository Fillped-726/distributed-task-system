cmake_minimum_required(VERSION 3.20)
project(CommonTests LANGUAGES CXX)

# # ---------- 任务序列化 ----------
# add_executable(task_test unit/common-test/task_test.cpp)
# target_link_libraries(task_test PRIVATE
#     common
#     nlohmann_json::nlohmann_json
#     GTest::gtest
#     GTest::gtest_main
# )
# target_compile_features(task_test PUBLIC cxx_std_20)
# add_test(NAME TaskSerializationTest COMMAND task_test)


# # ---------- 线程池测试 ----------
# add_executable(thread_pool_test unit/common-test/thread_pool_test.cpp)
# target_link_libraries(thread_pool_test PRIVATE
#     common                
#     GTest::gtest_main
# )
# target_compile_features(thread_pool_test PUBLIC cxx_std_20)
# add_test(NAME ThreadPoolTest COMMAND thread_pool_test)

# # ---------- 任务调度器测试 ----------
# add_executable(task_executor_test unit/worker-test/task_executor_test.cpp)
# target_link_libraries(task_executor_test PRIVATE
#     task_executor
#     common
#     nlohmann_json::nlohmann_json
#     Boost::system
#     GTest::gtest
#     GTest::gtest_main
# )
# target_compile_features(task_executor_test PUBLIC cxx_std_20)
# add_test(NAME TaskExecutorTest COMMAND task_executor_test)

# # ---------- gRPC API-Server 单元测试 ----------
# add_executable(api_server_test
#     unit/api-server-test/api_server_test.cpp
# )
# # 2. proto 文件已经编进 common_core，直接链接即可
# target_link_libraries(api_server_test PRIVATE
#     client 
#     common_core      # 包含 task.grpc.pb.cc / task.pb.cc 的 object
#     api_server_lib
#     GTest::gtest_main
#     grpc++
#     glog::glog
#     pthread
# )
# target_compile_features(api_server_test PUBLIC cxx_std_20)
# add_test(NAME ApiServerTest COMMAND api_server_test)


# 1. 自动查找 connectivity-test 目录下的所有源文件
file(GLOB CONNECTIVITY_TEST_SOURCES "unit/connectivity-test/*.cpp")

# 2. 创建测试可执行文件
add_executable(connectivity_test ${CONNECTIVITY_TEST_SOURCES})

# 3. 链接所需的库
target_link_libraries(connectivity_test PRIVATE
    common
    api_web_lib
    client 
    
    # --- ApiServer 的第三方依赖 ---
    nlohmann_json::nlohmann_json  # (来自你的示例，ApiServer 也需要)
    httplib::httplib              # (ApiServer 依赖 httplib, 假设这是它的目标名)
    
    # --- gRPC 依赖 (如果 common 没有 PUBLIC 链接它们) ---
    grpc++                  
    protobuf::libprotobuf         

    # --- 测试框架库 ---
    GTest::gtest                  # (GTest 基础)
    GTest::gtest_main             # (GTest 的 main 函数)
    GTest::gmock                  # (用于 MockGrpcClient)
)

# 4. 设置 C++ 标准 (与你的示例保持一致)
target_compile_features(connectivity_test PUBLIC cxx_std_20)

# 5. 将其注册为 CTest 测试
add_test(NAME ApiConnectivityTest COMMAND connectivity_test)
