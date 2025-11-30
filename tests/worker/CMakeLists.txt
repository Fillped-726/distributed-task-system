add_executable(worker_test
    worker_core_test.cpp
)

target_link_libraries(worker_test PRIVATE
    dts_worker_core 
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(worker_test)