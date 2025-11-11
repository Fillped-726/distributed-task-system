# --- 1. 定义 API 服务器库 (api_web_lib) ---

set(API_WEB_LIB_SOURCES
    src/api_web.cpp
)

# 定义库
add_library(api_web_lib ${API_WEB_LIB_SOURCES})

# 设置 C++ 标准
set_target_properties(api_web_lib PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

# 设置包含目录
target_include_directories(api_web_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# 链接库的依赖
target_link_libraries(api_web_lib    
    PRIVATE
        client
        httplib::httplib
        nlohmann_json::nlohmann_json
)

# --- 2. 定义可执行文件 (my_dts_server) ---

add_executable(my_dts_server
    src/main.cpp
)

# --- 3. 链接可执行文件 ---

target_link_libraries(my_dts_server
    PRIVATE
        client
        api_web_lib
)

# C++ 标准也应用到可执行文件
set_target_properties(my_dts_server PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)