cmake_minimum_required(VERSION 3.20)
project(dts_api_web LANGUAGES CXX)

#----------------------------------------------------------
# 1. 核心业务库 (Static Library)
#----------------------------------------------------------
add_library(dts_api_web_core STATIC
    src/api_web.cpp
)

# 强制 C++20 (根据你的原配置)
target_compile_features(dts_api_web_core PUBLIC cxx_std_20)

# 头文件路径 (Modern CMake 写法)
target_include_directories(dts_api_web_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# 链接依赖
target_link_libraries(dts_api_web_core
    PUBLIC
        dts_client              # <--- [关键] 使用我们定义的 SDK 库
        httplib::httplib        # Web 框架
        nlohmann_json::nlohmann_json
        glog::glog              # 建议加上日志库
)

#----------------------------------------------------------
# 2. 可执行文件 (入口)
#----------------------------------------------------------
# 建议改名 dts_api_web，方便在 bin 目录查找
add_executable(dts_api_web src/main.cpp)

target_link_libraries(dts_api_web
    PRIVATE
        dts_api_web_core
)

#----------------------------------------------------------
# 3. 安装规则
#----------------------------------------------------------
include(GNUInstallDirs)

install(TARGETS dts_api_web
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)