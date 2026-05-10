package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// rootCmd 代表基础命令 "dts"
var rootCmd = &cobra.Command{
	Use:   "dts",
	Short: "DTS - 分布式任务调度系统控制台",
	Long:  `一个基于 Go 语言构建的 CLI 工具，用于管理 C++ 分布式任务调度系统。`,
}

// Execute 是 main 函数调用的入口
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
