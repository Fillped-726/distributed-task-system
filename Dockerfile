# ==============================================================================
# 阶段 1: 构建环境 (Builder)
# ==============================================================================
FROM ubuntu:24.04 AS builder

# 换源
RUN sed -i 's@//.*archive.ubuntu.com@//mirrors.aliyun.com@g' /etc/apt/sources.list.d/ubuntu.sources \
 && sed -i 's@//.*security.ubuntu.com@//mirrors.aliyun.com@g' /etc/apt/sources.list.d/ubuntu.sources

ENV DEBIAN_FRONTEND=noninteractive

# 安装依赖
RUN apt-get update && apt-get install -y \
    libhiredis-dev \
    build-essential \
    cmake \
    git \
    pkg-config \
    libssl-dev \
    zlib1g-dev \
    libpq-dev \
    libboost-all-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# [核心修改] 编译并安装到 /app/dist
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
RUN apt-get update && apt-get install -y \
    libpq5 \
    libssl3 \
    libboost-system1.83.0 \
    libboost-filesystem1.83.0 \
    libhiredis-dev \
    ca-certificates \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# [核心修改] 从安装目录拷贝
COPY --from=builder /app/dist/lib/ /usr/local/lib/
COPY --from=builder /app/dist/bin/dts_scheduler /app/dts_scheduler
COPY --from=builder /app/dist/bin/dts_worker /app/dts_worker
COPY --from=builder /app/dist/bin/dts_api_server /app/dts_api_server
COPY --from=builder /app/dist/bin/dts_api_web /app/dts_api_web


RUN ldconfig

# 创建用户
RUN useradd -m dts_user
RUN chown -R dts_user:dts_user /app
USER dts_user

CMD ["/bin/bash"]