cmake_minimum_required(VERSION 3.20)

add_library(client STATIC)

target_include_directories(client
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_sources(client
    PRIVATE
        src/exceptions.cpp
        src/grpc_client.cpp
        src/dag_builder.cpp
    PUBLIC
        FILE_SET HEADERS
        BASE_DIRS include
        FILES
            include/types.hpp
            include/exceptions.hpp
            include/async_tags.hpp
            include/grpc_client.hpp
)

target_link_libraries(client
    PUBLIC
        common_core
        grpc++
        protobuf::libprotobuf
)