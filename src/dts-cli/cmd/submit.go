package cmd

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// 引入咱们自己写的包
	"dts-cli/pkg/dag"
	"dts-cli/pkg/parser"
	pb "dts-cli/pkg/proto/service"
)

var submitCmd = &cobra.Command{
	Use:   "submit [yaml_file]",
	Short: "Submit a job defined in a YAML file",
	Example: "  dts submit ./configs/job_example.yaml",
	Args:  cobra.ExactArgs(1), // 强制要求 1 个参数
	Run: func(cmd *cobra.Command, args []string) {
		filename := args[0]
		fmt.Printf("📂 Reading job config from: %s\n", filename)

		// 1. 解析 YAML -> Proto
		pbReq, err := parser.ParseAndConvertToProto(filename)
		if err != nil {
			log.Fatalf("❌ Config Error: %v", err)
		}

		// 2. 客户端侧校验 (Fail-Fast)
		fmt.Println("🔍 Validating DAG structure...")
		if err := dag.Validate(pbReq); err != nil {
			log.Fatalf("❌ Validation Failed: %v", err)
		}
		fmt.Println("✅ DAG Validation Passed!")

		// 3. gRPC 连接 (以后可以封装到 pkg/client 单例中)
		// 注意：这里的 localhost:50051 需要和你 C++ Server 端口一致
		conn, err := grpc.NewClient("localhost:45403", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("❌ Connect Failed: %v", err)
		}
		defer conn.Close()

		client := pb.NewTaskServiceClient(conn)

		// 4. 发送请求
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		fmt.Println("🚀 Sending request to DTS Server...")
		resp, err := client.SubmitDag(ctx, pbReq)
		if err != nil {
			log.Fatalf("❌ RPC Failed: %v", err)
		}

		// 5. 打印结果
		if resp.Header != nil && resp.Header.Error != nil {
             // 假设 Error 是一共有 oneof 类型的
			log.Printf("⚠️ Server Business Error: %v\n", resp.Header.Error)
		} else {
			fmt.Printf("🎉 Job Submitted Successfully! Job ID: %s\n", resp.JobId)
		}
	},
}

func init() {
	rootCmd.AddCommand(submitCmd)
}