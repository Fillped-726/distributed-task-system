add_executable(common_test
    json_convert_test.cpp
)

target_link_libraries(common_test PRIVATE
    dts_common          
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(common_test)