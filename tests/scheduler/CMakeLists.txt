add_executable(scheduler_test
    worker_manager_test.cpp
    task_repository_test.cpp 
)

target_link_libraries(scheduler_test PRIVATE
    dts_scheduler_core  
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(scheduler_test)