package dag

import (
	"fmt"
	// 引入生成的代码，路径取决于你的 go.mod 模块名
	// 假设你的 go.mod 第一行是 module task-gateway
	pb "dts-cli/pkg/proto/service"
)

// ValidateDAG 核心算法：Kahn's Algorithm (拓扑排序)
// 输入：SubmitDagRequest
// 输出：是否有环，错误信息
func Validate(req *pb.SubmitDagRequest) error {
	// 1. 建立邻接表和入度表
	// string -> []string
	adj := make(map[string][]string)
	// string -> int
	inDegree := make(map[string]int)
	// 用于快速检查节点是否存在
	exists := make(map[string]struct{})

	// 2. 初始化所有节点
	for _, task := range req.Tasks {
		if task.NaturalId == "" {
			return fmt.Errorf("task missing natural_id")
		}
		inDegree[task.NaturalId] = 0
		exists[task.NaturalId] = struct{}{}
	}

	// 3. 构建图 (根据 Edges)
	for _, edge := range req.Edges {
		// 校验边两端的节点是否存在
		if _, ok := exists[edge.ParentNaturalId]; !ok {
			return fmt.Errorf("edge parent '%s' not found", edge.ParentNaturalId)
		}
		if _, ok := exists[edge.ChildNaturalId]; !ok {
			return fmt.Errorf("edge child '%s' not found", edge.ChildNaturalId)
		}

		// 记录图结构：Parent -> Children
		adj[edge.ParentNaturalId] = append(adj[edge.ParentNaturalId], edge.ChildNaturalId)
		// 子节点入度 +1
		inDegree[edge.ChildNaturalId]++
	}

	// 4. 拓扑排序
	// 队列：存放入度为 0 的节点
	queue := make([]string, 0)
	for id, d := range inDegree {
		if d == 0 {
			queue = append(queue, id)
		}
	}

	processedCount := 0
	for len(queue) > 0 {
		// Pop
		curr := queue[0]
		queue = queue[1:]
		processedCount++

		// 遍历邻居
		for _, neighbor := range adj[curr] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	// 5. 判定
	totalTasks := len(req.Tasks)
	if processedCount != totalTasks {
		return fmt.Errorf("cycle detected or disconnected graph: processed %d/%d tasks", processedCount, totalTasks)
	}

	return nil
}