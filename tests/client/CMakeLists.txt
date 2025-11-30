add_executable(client_test
    dag_builder_test.cpp
)

target_link_libraries(client_test PRIVATE
    dts_client       
    dts_common       
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(client_test)