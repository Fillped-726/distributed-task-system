package cmd

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pbCommon "dts-cli/pkg/proto/common" // 根据你的 go.mod 路径调整
	pbService "dts-cli/pkg/proto/service"
	pbTask "dts-cli/pkg/proto/task"
)

var (
	concurrency int // 并发数
	totalReq    int // 总请求数
)

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Run benchmark test against DTS server",
	Long:  `启动高并发 Goroutines 对 C++ 服务端进行压力测试。`,
	Run: func(cmd *cobra.Command, args []string) {
		runBenchmark()
	},
}

func init() {
	rootCmd.AddCommand(benchCmd)
	// 定义命令行参数 (flags)
	benchCmd.Flags().IntVarP(&concurrency, "concurrency", "c", 10, "Number of concurrent workers")
	benchCmd.Flags().IntVarP(&totalReq, "total", "n", 1000, "Total number of requests to send")
}

func runBenchmark() {
	fmt.Printf("🔥 Starting Benchmark: %d workers, %d requests...\n", concurrency, totalReq)

	// 1. 建立连接 (为了压测准确，我们创建一个共享连接池，或者每个 worker 一个连接)
	// 这里简单起见，所有协程复用一个 gRPC 连接 (gRPC-Go 内部支持多路复用)
	conn, err := grpc.NewClient("localhost:45403", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()
	client := pbService.NewTaskServiceClient(conn)

	// 2. 准备计数器和同步原语
	var successCount int64 = 0
	var failCount int64 = 0
	var wg sync.WaitGroup

	// 计算每个 Worker 需要完成的任务量
	reqPerWorker := totalReq / concurrency

	startTime := time.Now()

	// 3. 启动 Goroutines (Workers)
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			defer wg.Done()
			
			// 每个 Worker 跑循环
			for j := 0; j < reqPerWorker; j++ {
				// 构造一个简单的 Mock 请求 (不读文件，纯测网络和处理性能)
				req := buildMockRequest(workerID, j)
				
				// 设置短超时，压测场景下 fast fail
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				
				_, err := client.SubmitDag(ctx, req)
				if err == nil {
					atomic.AddInt64(&successCount, 1)
				} else {
					atomic.AddInt64(&failCount, 1)
					// 可选：打印错误 (高并发下建议关掉，否则刷屏)
					// fmt.Printf("Err: %v\n", err)
				}
				cancel()
			}
		}(i)
	}

	// 4. 等待所有 Worker 完成
	wg.Wait()
	duration := time.Since(startTime)

	// 5. 打印报告
	printReport(duration, successCount, failCount)
}

// 构造一个最小化的合法 DAG 请求
func buildMockRequest(workerID, seq int) *pbService.SubmitDagRequest {
	taskID := fmt.Sprintf("bench-%d-%d", workerID, seq)
	return &pbService.SubmitDagRequest{
		ClientId:       "bench-tool",
		IdempotencyKey: fmt.Sprintf("key-%d-%d-%d", time.Now().UnixNano(), workerID, seq),
		Tasks: []*pbTask.Task{
			{
				NaturalId: taskID,
				FuncName:  "echo", 
				Priority:  1,
				Required: &pbCommon.Resource{CpuCore: 0.1, MemMb: 10},
			},
		},
		// 只有一个节点，没有边，最简 DAG
		Edges: []*pbTask.TaskEdge{}, 
	}
}

func printReport(d time.Duration, success, fail int64) {
	total := success + fail
	qps := float64(success) / d.Seconds()

	fmt.Println("\n===================================")
	fmt.Printf("✅ Benchmark Finished in %v\n", d)
	fmt.Println("===================================")
	fmt.Printf("Total Requests : %d\n", total)
	fmt.Printf("Success        : %d\n", success)
	fmt.Printf("Failed         : %d\n", fail)
	fmt.Printf("-----------------------------------\n")
	fmt.Printf("🚀 QPS         : %.2f (Requests/Sec)\n", qps)
	fmt.Println("===================================")
}