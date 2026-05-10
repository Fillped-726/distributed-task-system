package cmd

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// 【关键修正】: 确保这里正确引入了两个生成的包
	// pbService 对应 api/dts/service (task_service.proto)
	pbService "dts-cli/pkg/proto/service"

	// pbTask 对应 api/dts/task (task.proto, task_state.proto)
	// JobState, TaskState, TaskRuntimeDetail 都在这里
	pbTask "dts-cli/pkg/proto/task"
)

var (
	showDetail bool
)

var getCmd = &cobra.Command{
	Use:   "get [job_id]",
	Short: "Get job status and details",
	Example: "  dts get job-20231224-001\n  dts get job-20231224-001 --detail",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		jobID := args[0]
		queryJobStatus(jobID)
	},
}

func init() {
	rootCmd.AddCommand(getCmd)
	getCmd.Flags().BoolVarP(&showDetail, "detail", "d", false, "Show detailed task execution list")
}

func queryJobStatus(jobID string) {
	conn, err := grpc.NewClient("localhost:45403", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("❌ Connection failed: %v", err)
	}
	defer conn.Close()
	client := pbService.NewTaskServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pbService.GetJobStatusRequest{
		JobId:        jobID,
		IncludeTasks: showDetail,
	}

	resp, err := client.GetJobStatus(ctx, req)
	if err != nil {
		log.Fatalf("❌ RPC failed: %v", err)
	}

	if resp.Header != nil && resp.Header.Error != nil {
		fmt.Printf("⚠️ Server Error: %v\n", resp.Header.Error)
		return
	}

	printJobSummary(resp)

	// resp.TaskDetails 的元素类型是 *pbTask.TaskRuntimeDetail
	if showDetail && len(resp.TaskDetails) > 0 {
		printTaskTable(resp.TaskDetails)
	}
}

// ---------------------------------------------------------
// UI 组件
// ---------------------------------------------------------

func printJobSummary(resp *pbService.GetJobStatusResponse) {
	fmt.Println("\n📋 Job Status Summary")
	fmt.Println("========================================")
	
	fmt.Printf("Job ID       : %s\n", resp.JobId)
	
	// 【关键修正】这里 resp.State 是 pbTask.JobState 类型
	fmt.Printf("State        : %s\n", colorizeJobState(resp.State))
	
	startTime := time.UnixMilli(resp.SubmitTime)
	var duration time.Duration
	
	// 这里做个简单判断，避免时间戳为 0 显示 1970 年
	if resp.SubmitTime == 0 {
		fmt.Printf("Submitted At : -\n")
		fmt.Printf("Duration     : -\n")
	} else {
		if resp.FinishTime > 0 {
			duration = time.UnixMilli(resp.FinishTime).Sub(startTime)
		} else {
			duration = time.Since(startTime)
		}
		fmt.Printf("Submitted At : %s\n", startTime.Format("2006-01-02 15:04:05"))
		fmt.Printf("Duration     : %s\n", duration.Round(time.Millisecond))
	}

	if resp.TotalTasks > 0 {
		fmt.Printf("Progress     : %s\n", drawProgressBar(int(resp.FinishedTasks), int(resp.TotalTasks)))
	}
	fmt.Println("========================================")
}

func drawProgressBar(current, total int) string {
	const barWidth = 20
	if total == 0 {
		return "[?]"
	}
	percent := float64(current) / float64(total)
	if percent > 1.0 {
		percent = 1.0
	}
	
	filled := int(math.Round(float64(barWidth) * percent))
	empty := barWidth - filled
	// 防止 panic (runtime error: slice bounds out of range)
	if empty < 0 { empty = 0 }
	if filled > barWidth { filled = barWidth }
	
	bar := strings.Repeat("█", filled) + strings.Repeat("░", empty)
	return fmt.Sprintf("[%s] %.1f%% (%d/%d)", bar, percent*100, current, total)
}

// 【关键修正】参数类型必须是 []*pbTask.TaskRuntimeDetail
func printTaskTable(tasks []*pbTask.TaskRuntimeDetail) {
	fmt.Println("\n👇 Task Runtime Details")
	
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TASK_ID\tSTATUS\tWORKER\tRETRY\tDURATION\tMESSAGE")
	
	for _, t := range tasks {
		durStr := "-"
		if t.StartTime > 0 {
			var end int64 = t.EndTime
			if end == 0 {
				end = time.Now().UnixMilli()
			}
			d := time.Duration(end - t.StartTime) * time.Millisecond
			durStr = d.Round(time.Millisecond).String()
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\n",
			limitStr(t.TaskId, 20),
			colorizeTaskState(t.Status), // t.Status 是 pbTask.TaskState
			limitStr(t.WorkerId, 15),
			t.RetryCount,
			durStr,
			t.ErrorMessage,
		)
	}
	w.Flush()
	fmt.Println()
}

func limitStr(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen-3] + "..."
	}
	return s
}

// ---------------------------------------------------------
// 状态染色函数：明确接收 pbTask 包下的枚举
// ---------------------------------------------------------

// 修正：接收 pbTask.JobState
func colorizeJobState(s pbTask.JobState) string {
	switch s {
	case pbTask.JobState_JOB_SUCCESS:
		return fmt.Sprintf("\033[32m✔ %s\033[0m", s.String()) // Green
	case pbTask.JobState_JOB_FAILED:
		return fmt.Sprintf("\033[31m✖ %s\033[0m", s.String()) // Red
	case pbTask.JobState_JOB_RUNNING:
		return fmt.Sprintf("\033[34m⟳ %s\033[0m", s.String()) // Blue
	case pbTask.JobState_JOB_PENDING:
		return fmt.Sprintf("\033[33m%s\033[0m", s.String()) // Yellow
	default:
		return s.String()
	}
}

// 修正：接收 pbTask.TaskState
func colorizeTaskState(s pbTask.TaskState) string {
	switch s {
	case pbTask.TaskState_SUCCESS:
		return fmt.Sprintf("\033[32m%s\033[0m", "SUCCESS")
	case pbTask.TaskState_FAILED:
		return fmt.Sprintf("\033[31m%s\033[0m", "FAILED")
	case pbTask.TaskState_RUNNING:
		return fmt.Sprintf("\033[34m%s\033[0m", "RUNNING")
	case pbTask.TaskState_PENDING:
		return "\033[33mPENDING\033[0m"
	case pbTask.TaskState_WAITING_DEPS:
		return "\033[90mWAITING\033[0m" // Gray
	default:
		return s.String()
	}
}