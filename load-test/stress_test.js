import http from 'k6/http';
import { check, sleep } from 'k6';

// ----------------------------------------------------------------
// 1. 配置压测阶梯 (Ramping up)
// ----------------------------------------------------------------
export const options = {
    stages: [
        { duration: '30s', target: 500 },  // 快速上升到 500 并发
        { duration: '1m', target: 2000 },  // 核心压力阶段：2000 并发持续 1 分钟
        { duration: '30s', target: 0 },    // 降压
    ],
    thresholds: {
        http_req_failed: ['rate<0.01'],    // 错误率必须小于 1%
        http_req_duration: ['p(95)<100'], // 95% 的请求应该在 100ms 内完成
    },
};

// ----------------------------------------------------------------
// 2. 模拟动态数据生成
// ----------------------------------------------------------------
function generatePayload() {
    // 使用当前时间戳 + VU(虚拟用户ID) + 迭代次数，保证幂等键绝对唯一
    const uniqueId = `${Date.now()}_${__VU}_${__ITER}`;
    
    return JSON.stringify({
        "client_id": `tester_vu_${__VU}`,
        "idempotency_key": `key_${uniqueId}`,
        "tasks": [
            {
                "natural_id": `download_${uniqueId}`,
                "func_name": "download_data",
                "func_params": "{\"url\": \"http://example.com/data.csv\"}",
                "priority": 10
            },
            {
                "natural_id": `process_${uniqueId}`,
                "func_name": "process_data",
                "func_params": "{\"threshold\": 0.8}",
                "priority": 10
            },
            {
                "natural_id": `upload_${uniqueId}`,
                "func_name": "upload_results",
                "func_params": "{\"dest\": \"s3://bucket/report.pdf\"}",
                "priority": 10
            }
        ],
        "dependencies": [
            { "from_natural_id": `download_${uniqueId}`, "to_natural_id": `process_${uniqueId}` },
            { "from_natural_id": `process_${uniqueId}`, "to_natural_id": `upload_${uniqueId}` }
        ]
    });
}

// ----------------------------------------------------------------
// 3. 压测主逻辑
// ----------------------------------------------------------------
export default function () {
    const url = 'http://api-web:8080/api/v1/job/submit'; // 替换为你的实际接口地址
    const payload = generatePayload();
    const params = {
        headers: {
            'Content-Type': 'application/json',
            'X-Test-Label': 'dts-stress-test' // 方便在 OTel 中过滤
        },
    };

    const res = http.post(url, payload, params);

    // 检查是否提交成功
    check(res, {
        'status is 200': (r) => r.status === 200,
        'has response id': (r) => r.json().hasOwnProperty('id') || r.body.includes('id'),
    });

    // 这里根据你的 QPS 需求调整 sleep。
    // 如果要跑满 20,000 QPS，可能需要去掉 sleep 或者增加并发数。
    // sleep(0.01); 
}