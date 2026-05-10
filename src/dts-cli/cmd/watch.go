package cmd

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pbService "dts-cli/pkg/proto/service"
	pbTask "dts-cli/pkg/proto/task"
)

var watchCmd = &cobra.Command{
	Use:   "watch",
	Short: "实时监控任务流 (Live Log)",
	Long:  `通过 gRPC Server Streaming 实时订阅并打印任务执行状态变更流。`,
	Run: func(cmd *cobra.Command, args []string) {
		runWatch()
	},
}

func init() {
	rootCmd.AddCommand(watchCmd)
}

func runWatch() {
	// 1. 建立连接
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("❌ 连接失败: %v", err)
	}
	defer conn.Close()
	client := pbService.NewTaskServiceClient(conn)

	// 2. 发起订阅请求
	// 只需要一个 ID 标识，告诉服务端"我是谁"
	req := &pbService.SubscribeRequest{
		ClientId: "dts-cli-monitor",
	}

	// 这是一个长连接流
	stream, err := client.ListenResults(context.Background(), req)
	if err != nil {
		log.Fatalf("❌ 订阅失败: %v", err)
	}

	fmt.Println("📡 已连接到 DTS 服务端，正在监听实时日志... (按 Ctrl+C 退出)")
	fmt.Println("---------------------------------------------------------------")
	fmt.Printf("%-20s | %-15s | %-10s | %s\n", "Timestamp", "TaskID", "Status", "Message")
	fmt.Println("---------------------------------------------------------------")

	// 3. 循环接收 (核心逻辑)
	for {
		// Recv() 会阻塞，直到服务端推送一条新消息，或者连接断开
		msg, err := stream.Recv()
		
		if err == io.EOF {
			// 服务端主动关闭了流
			fmt.Println("⚠️ 服务端关闭了连接")
			break
		}
		if err != nil {
			log.Fatalf("❌ 流读取错误: %v", err)
		}

		// 4. 美化打印
		printTaskLog(msg.Task)
	}
}

func printTaskLog(t *pbTask.Task) {
	if t == nil {
		return
	}

	// 简单的颜色代码
	const (
		Reset  = "\033[0m"
		Green  = "\033[32m"
		Red    = "\033[31m"
		Yellow = "\033[33m"
		Blue   = "\033[34m"
	)

	// 根据状态上色
	statusColor := Reset
	switch t.State {
	case pbTask.TaskState_SUCCESS:
		statusColor = Green
	case pbTask.TaskState_FAILED:
		statusColor = Red
	case pbTask.TaskState_RUNNING:
		statusColor = Blue
	case pbTask.TaskState_PENDING:
		statusColor = Yellow
	}

	timestamp := time.Now().Format("15:04:05.000")
	
	// 格式化输出
	fmt.Printf("%s | %s | %s%-10s%s | %s\n", 
		timestamp, 
		t.NaturalId, 
		statusColor, t.State.String(), Reset,
		t.ErrorMsg, // 如果有错误信息就打印
	)
}