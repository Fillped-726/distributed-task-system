-- ============================================================================
-- SCRIPT: complete_task.lua
-- 作用：原子性地处理任务完成，推进 DAG 依赖，并将就绪任务推入 Stream
-- ============================================================================
-- KEYS[1]: dts:dag:children:{parent_task_id}
-- KEYS[2]: dts:dag:deps:{job_id}
-- KEYS[3]: dts:stream:tasks
-- KEYS[4]: dts:stream:errors  
-- ARGV[1]: job_id
-- ARGV[1]: job_id                             (用于拼接 Meta Key)
-- ============================================================================

-- 1. 获取所有子任务 (SMEMBERS)
local children = redis.call('SMEMBERS', KEYS[1])

-- [幂等性检查]
-- 如果没有子任务，或者 Key 已经被删除了(说明之前处理过了)，直接返回 0
if #children == 0 then
    return 0
end

local triggered_count = 0

-- 2. 遍历所有子任务
for _, child_id in ipairs(children) do
    
    -- 2.1 扣减依赖计数 (HINCRBY key field -1)
    -- remain 是扣减后的剩余依赖数
    local remain = redis.call('HINCRBY', KEYS[2], child_id, -1)

    -- 2.2 判断是否就绪
    if remain == 0 then
        -- 依赖满足！准备推入队列

        -- A. 获取该子任务的元数据 (Payload)
        -- Key 格式: dts:task:meta:{child_id}
        local meta_key = "dts:task:meta:" .. child_id
        local payload = redis.call('GET', meta_key)

        if payload then
            -- B. 推入 Stream (XADD)
            -- 格式: XADD key * payload <binary> task_id <id> job_id <id> priority <p> ...
            -- 注意：这里为了节省 Lua 参数，我们假设 Payload 里包含了所有 Worker 需要的数据
            -- 如果需要 priority 等字段，建议在 Meta 里存 JSON 或 Proto，或者在 ARGV 传更多信息
            -- 这里简单演示推 Payload 和 IDs
            redis.call('XADD', KEYS[3], '*', 
                       'payload', payload, 
                       'task_id', child_id, 
                       'job_id', ARGV[1])
            
            triggered_count = triggered_count + 1

            -- [内存优化] 既然任务已经进队列了，Hash 里的计数器就不需要了
            redis.call('HDEL', KEYS[2], child_id)
        else
            -- 异常逻辑：推入错误 Stream
            -- 结构化存储，方便后续分析
            redis.call('XADD', KEYS[4], '*',
                       'type', 'MISSING_META',
                       'job_id', ARGV[1],
                       'task_id', child_id,
                       'reason', 'Meta data not found when dependency met')
        end
    elseif remain < 0 then
        -- 异常逻辑：推入错误 Stream
        redis.call('XADD', KEYS[4], '*',
                   'type', 'NEGATIVE_DEPS',
                   'job_id', ARGV[1],
                   'task_id', child_id,
                   'current_val', tostring(remain))
    end
end

-- 3. [关键步骤] 过河拆桥
-- 删除父子关系 Key。确保下次重试时 SMEMBERS 返回空，实现幂等性。
redis.call('DEL', KEYS[1])

return triggered_count