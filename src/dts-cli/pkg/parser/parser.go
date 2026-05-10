package parser

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v3"

	// 确保这里路径正确
	pbCommon "dts-cli/pkg/proto/common"
	pb "dts-cli/pkg/proto/service"
	pbTask "dts-cli/pkg/proto/task"
)

// 1. 定义与 YAML 对应的 Go 结构体
type YamlTask struct {
	ID       string                 `yaml:"id"`
	Cmd      string                 `yaml:"cmd"`
	Priority uint32                 `yaml:"priority"` // 新增
	Deps     []string               `yaml:"deps"`
	Params   map[string]interface{} `yaml:"params"`   // 新增：使用 map 接收任意参数
	CPU      float64                `yaml:"cpu"`
	MemMB    uint64                 `yaml:"mem_mb"`
}

type YamlJob struct {
	ClientID       string     `yaml:"client_id"`       // 新增
	IdempotencyKey string     `yaml:"idempotency_key"` // 新增
	Tasks          []YamlTask `yaml:"tasks"`
}

// 2. 解析并转换
func ParseAndConvertToProto(filename string) (*pb.SubmitDagRequest, error) {
	// 读取文件
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("read file error: %v", err)
	}

	// 解析 YAML
	var job YamlJob
	if err := yaml.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("yaml parse error: %v", err)
	}

	// 转换为 Proto Request
	req := &pb.SubmitDagRequest{
		ClientId:       job.ClientID,       // 映射 client_id
		IdempotencyKey: job.IdempotencyKey, // 映射 idempotency_key
		Tasks:          make([]*pbTask.Task, 0),
		Edges:          make([]*pbTask.TaskEdge, 0),
	}

	// 如果 YAML 里没写 ClientID，给个默认值防止报错
	if req.ClientId == "" {
		req.ClientId = "dts-cli-default"
	}

	for _, t := range job.Tasks {
		// 【关键点】将 YAML 里的 params 对象序列化为 JSON 字符串
		// 对应 JSON 里的 "func_params": "{\"url\": ...}"
		var paramsJson []byte
		if t.Params != nil {
			paramsJson, _ = json.Marshal(t.Params)
		} else {
			paramsJson = []byte("{}")
		}

		// 构建 Task Proto
		pbT := &pbTask.Task{
			NaturalId:  t.ID,
			FuncName:   t.Cmd,
			Priority:   t.Priority,      // 映射 priority
			FuncParams: string(paramsJson), // 赋值转换后的 JSON 串
			
			// 资源预留默认值，防止 C++ 端除零错误
			Required: &pbCommon.Resource{
				CpuCore: t.CPU,
				MemMb:   t.MemMB,
			},
		}
		req.Tasks = append(req.Tasks, pbT)

		// 构建依赖边 (将嵌套结构拍平为 Edge List)
		// 这完全等价于你 JSON 中的 "dependencies" 数组
		for _, depParentID := range t.Deps {
			edge := &pbTask.TaskEdge{
				ParentNaturalId: depParentID, // from
				ChildNaturalId:  t.ID,        // to
			}
			req.Edges = append(req.Edges, edge)
		}
	}

	return req, nil
}