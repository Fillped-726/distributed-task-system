# Makefile for dts-cli

# 1. 自动获取 go.mod 中的 module 名字 (例如: dts-cli)
MODULE_NAME := $(shell go list -m)

# 2. 目录定义
# 获取当前 Makefile 所在目录
CLI_ROOT := $(CURDIR)
# Proto 源文件目录 (根据你之前的结构，向上两级找到 common/proto)
PROTO_SRC  := $(abspath $(CLI_ROOT)/../common/proto)
# 生成代码的目标目录
OUT_DIR    := pkg/proto

# 3. 确保工具链路径正确 (强制将 GOPATH/bin 加入 PATH)
export PATH := $(shell go env GOPATH)/bin:$(PATH)

.PHONY: proto
proto:
	@echo "========================================"
	@echo "🛠️  正在为 $(MODULE_NAME) 生成 Proto 代码..."
	@echo "📍 Proto 源: $(PROTO_SRC)"
	@echo "📂 目标目录: $(OUT_DIR)"
	@echo "========================================"

	@# 1. 检查目录是否存在
	@if [ ! -d "$(PROTO_SRC)" ]; then \
		echo "❌ 错误: 找不到 Proto 源目录 $(PROTO_SRC)"; exit 1; \
	fi

	@# 2. 清理旧代码 (但保留 pkg/proto 文件夹本身)
	@rm -rf $(OUT_DIR)/*

	@# 3. 运行 protoc
	@# --go_out=. 和 --go_opt=module=... 的组合拳会将代码精确生成到 pkg/proto 下
	protoc \
		--proto_path=$(PROTO_SRC) \
		--go_out=. \
		--go_opt=module=$(MODULE_NAME) \
		--go-grpc_out=. \
		--go-grpc_opt=module=$(MODULE_NAME) \
		$(shell find $(PROTO_SRC) -name "*.proto")

	@echo "✅ 生成成功！文件结构如下："
	@tree $(OUT_DIR) -L 2 || ls -R $(OUT_DIR)