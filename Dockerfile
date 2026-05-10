# ==============================================================================
# 阶段 1: 构建环境 (Builder)
# ==============================================================================
FROM ubuntu:24.04 AS builder

# 换源 (保持不变)
RUN sed -i 's@//.*archive.ubuntu.com@//mirrors.aliyun.com@g' /etc/apt/sources.list.d/ubuntu.sources \
 && sed -i 's@//.*security.ubuntu.com@//mirrors.aliyun.com@g' /etc/apt/sources.list.d/ubuntu.sources

ENV DEBIAN_FRONTEND=noninteractive

# 安装构建依赖
# [新增] libcurl4-openssl-dev: prometheus-cpp 依赖它来发送 HTTP 请求
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    pkg-config \
    libssl-dev \
    zlib1g-dev \
    libpq-dev \
    libboost-all-dev \
    libhiredis-dev \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# 编译并安装
# 注意：FetchContent 拉取的 prometheus-cpp 通常会静态链接到你的二进制文件中
# 所以不需要单独拷贝 prometheus 的 .so 文件，除非你显式开启了共享库编译
RUN --mount=type=cache,target=/app/build \
    cmake -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/app/dist && \
    cmake --build build -j$(nproc) && \
    cmake --install build

# ==============================================================================
# 阶段 2: 运行时环境 (Runtime)
# ==============================================================================
FROM ubuntu:24.04

# 换源
RUN sed -i 's@//.*archive.ubuntu.com@//mirrors.aliyun.com@g' /etc/apt/sources.list.d/ubuntu.sources \
 && sed -i 's@//.*security.ubuntu.com@//mirrors.aliyun.com@g' /etc/apt/sources.list.d/ubuntu.sources

ENV DEBIAN_FRONTEND=noninteractive

# 安装运行时库
# [修改] libhiredis-dev -> libhiredis1.1.0 (运行时只需要库文件，不需要头文件)
# [新增] libcurl4: prometheus-cpp 运行时需要
# [新增] zlib1g: prometheus-cpp 运行时需要
RUN apt-get update && apt-get install -y \
    libpq5 \
    libssl3 \
    libboost-system1.83.0 \
    libboost-filesystem1.83.0 \
    libhiredis1.1.0 \
    libcurl4 \
    zlib1g \
    ca-certificates \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 从 builder 拷贝
# 注意：如果 prometheus-cpp 是静态链接的，/app/dist/lib 下可能只有 .a 文件
# 拷贝它们到运行时虽然没坏处，但实际上运行时并不需要 .a 文件。
# 关键是你的可执行文件 dts_* 已经包含了 prometheus 的代码。
COPY --from=builder /app/dist/lib/ /usr/local/lib/
COPY --from=builder /app/dist/bin/dts_scheduler /app/dts_scheduler
COPY --from=builder /app/dist/bin/dts_worker /app/dts_worker
COPY --from=builder /app/dist/bin/dts_api_server /app/dts_api_server
COPY --from=builder /app/dist/bin/dts_api_web /app/dts_api_web

# 刷新动态库缓存
RUN ldconfig

# 创建用户
RUN useradd -m dts_user
RUN chown -R dts_user:dts_user /app
USER dts_user

CMD ["/bin/bash"]