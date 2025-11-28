cmake_minimum_required(VERSION 3.20)
project(dts_api_server LANGUAGES CXX)

#----------------------------------------------------------
# 1. 核心业务库 (Static Library)
#    将实现与 main 分离，方便后续写单元测试 (如测试 TaskSubmitter)
#----------------------------------------------------------
add_library(dts_api_server_core STATIC
    src/api_server.cpp
    src/task_submitter.cpp
)

# 使用 C++20 (继承自根目录设置，这里再次强调也可以)
target_compile_features(dts_api_server_core PUBLIC cxx_std_20)

# 头文件搜索路径
# 使用 Generator Expressions 区分构建时和安装时的路径
target_include_directories(dts_api_server_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# 链接依赖
target_link_libraries(dts_api_server_core
    PUBLIC
        dts_common          # 包含日志、Proto定义、线程池等
        httplib::httplib    # Web 框架 (根目录 FetchContent 引入)
        libpqxx::pqxx       # 数据库驱动
        nlohmann_json::nlohmann_json
        

        Boost::system
        Boost::uuid
        Boost::filesystem
)

#----------------------------------------------------------
# 2. 可执行文件 (入口)
#----------------------------------------------------------
add_executable(dts_api_server src/main.cpp)

# 只需要链接核心库，头文件和传递依赖会自动带过来
target_link_libraries(dts_api_server
    PRIVATE
        dts_api_server_core
)

#----------------------------------------------------------
# 3. 静态分析 (Clang-Tidy)
#----------------------------------------------------------
if(COMMON_ENABLE_TIDY)
    set_target_properties(dts_api_server_core PROPERTIES
        CXX_CLANG_TIDY "clang-tidy;-checks=bugprone-*,performance-*,clang-analyzer-*"
    )
endif()

#----------------------------------------------------------
# 4. 安装规则
#----------------------------------------------------------
include(GNUInstallDirs)

# 安装可执行文件
install(TARGETS dts_api_server
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# 安装头文件 (如果其他模块需要引用 API Server 的内部头文件)
install(DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/api-server
    FILES_MATCHING PATTERN "*.hpp"
)