-- ============================================================
-- DTS Database Initialization Script
-- 自动启用必要的扩展并创建表结构
-- ============================================================

-- 1. 启用 UUID 生成扩展 (对应 uuid_generate_v4())
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 2. 创建自动更新 updated_at 的函数 (对应 trg_job_updated_at)
CREATE OR REPLACE FUNCTION public.fn_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- Table: public.job
-- ============================================================
CREATE TABLE IF NOT EXISTS public.job
(
    job_id uuid NOT NULL DEFAULT uuid_generate_v4(),
    idempotency_key text COLLATE pg_catalog."default" NOT NULL,
    state smallint NOT NULL DEFAULT 0,
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT job_pkey PRIMARY KEY (job_id),
    CONSTRAINT job_idempotency_key_key UNIQUE (idempotency_key)
);

COMMENT ON TABLE public.job IS '作业(工作流)表，作为所有 tasks 的容器并处理 API 幂等性。';
COMMENT ON COLUMN public.job.idempotency_key IS 'API 提交的幂等键，防止重复创建工作流。';

-- Index for job
CREATE INDEX IF NOT EXISTS idx_job_cleanup
    ON public.job USING btree
    (updated_at ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True);

-- Trigger for job
CREATE OR REPLACE TRIGGER trg_job_updated_at
    BEFORE UPDATE 
    ON public.job
    FOR EACH ROW
    EXECUTE FUNCTION public.fn_set_updated_at();


-- ============================================================
-- Table: public.task
-- ============================================================
CREATE TABLE IF NOT EXISTS public.task
(
    task_id uuid NOT NULL,
    job_id uuid NOT NULL,
    natural_id text COLLATE pg_catalog."default" NOT NULL,
    func_name text COLLATE pg_catalog."default" NOT NULL,
    func_params jsonb,
    required jsonb,
    shard jsonb,
    state smallint NOT NULL DEFAULT 0,
    priority smallint NOT NULL DEFAULT 0,
    retry_count smallint NOT NULL DEFAULT 0,
    max_retry smallint NOT NULL DEFAULT 0,
    timeout_ms integer NOT NULL DEFAULT 0,
    pending_dependencies integer NOT NULL DEFAULT 0,
    error_msg text COLLATE pg_catalog."default",
    submit_ts bigint,
    start_ts bigint,
    finish_ts bigint,
    client_id text COLLATE pg_catalog."default",
    result jsonb,
    worker_id character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT task_pkey PRIMARY KEY (task_id),
    CONSTRAINT uq_job_natural_id UNIQUE (job_id, natural_id),
    CONSTRAINT fk_task_to_job FOREIGN KEY (job_id)
        REFERENCES public.job (job_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
);

COMMENT ON TABLE public.task IS '任务表，存储 DAG 中的每一个独立工作单元。';
COMMENT ON COLUMN public.task.job_id IS '它所属的作业(job)的 UUID。';
COMMENT ON COLUMN public.task.natural_id IS '用户定义的业务 ID (e.g., "task_A")，用于解析依赖关系。';
COMMENT ON COLUMN public.task.func_name IS '此任务需要执行的函数名。';
COMMENT ON COLUMN public.task.func_params IS '此任务执行时所需的参数 (JSON 格式)。';
COMMENT ON COLUMN public.task.pending_dependencies IS '此任务还有多少个未完成的前置依赖。';

-- Indexes for task
CREATE INDEX IF NOT EXISTS idx_task_cleanup
    ON public.task USING btree
    (finish_ts ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    WHERE state = ANY (ARRAY[2, 3]);

CREATE INDEX IF NOT EXISTS idx_task_scheduler_pull
    ON public.task USING btree
    (state ASC NULLS LAST, priority DESC NULLS FIRST, submit_ts ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    WHERE state = 0;

CREATE INDEX IF NOT EXISTS idx_task_worker_id
    ON public.task USING btree
    (worker_id COLLATE pg_catalog."default" ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True);


-- ============================================================
-- Table: public.task_edge
-- ============================================================
CREATE TABLE IF NOT EXISTS public.task_edge
(
    parent_task_id uuid NOT NULL,
    child_task_id uuid NOT NULL,
    CONSTRAINT task_edge_pkey PRIMARY KEY (parent_task_id, child_task_id),
    CONSTRAINT fk_edge_to_child FOREIGN KEY (child_task_id)
        REFERENCES public.task (task_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE,
    CONSTRAINT fk_edge_to_parent FOREIGN KEY (parent_task_id)
        REFERENCES public.task (task_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
);

COMMENT ON TABLE public.task_edge IS 'DAG 依赖边表，存储任务之间的父子关系 (使用 task.task_id UUID)。';