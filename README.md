# 实时数据入湖平台

面向流式数据接入、实时转换、增量合并与湖上物化查询的实时数据入湖治理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 主题注册 | `topic_registry` | 数据主题 | 主题编号、主题名称、消息格式 |
| 流式计算 | `flink_job` | 流式任务 | 任务编号、消费主题、转换逻辑 |
| 消息接入 | `kafka_connector` | 接入配置 | 配置编号、集群地址、消费组名 |
| Schema演进 | `schema_evolve` | 演进规则 | 规则编号、关联主题、新增字段 |
| 增量合并 | `upsert_merge` | 合并策略 | 策略编号、目标表名、合并键列 |
| 湖上分区 | `lake_partition` | 分区定义 | 分区编号、目标表名、分区列名 |
| 流式质量 | `stream_quality` | 质量规则 | 规则编号、监控主题、校验类型 |
| Iceberg管理 | `iceberg_table` | 表元数据 | 表编号、表名称、存储格式 |
| 水位追踪 | `watermark_track` | 水位标记 | 标记编号、数据主题、分区编号 |
| 死信处理 | `dead_letter` | 死信记录 | 记录编号、原始主题、原始分区 |
| 物化查询 | `materialized_view` | 物化视图 | 视图编号、查询SQL、基表名 |
| CDC捕获 | `change_capture` | CDC配置 | 配置编号、源数据库、源表名 |
| 合并清理 | `compact_policy` | 清理策略 | 策略编号、目标表名、触发条件 |
| 流式血缘 | `stream_lineage` | 血缘记录 | 记录编号、上游主题、下游表名 |
| 流式补数 | `backfill_stream` | 补数任务 | 任务编号、目标表名、起始偏移 |
| 下沉分发 | `sink_dispatch` | 分发规则 | 规则编号、湖表名、目标系统 |
| 数据生命周期 | `ttl_policy` | 生命周期 | 策略编号、目标表名、热数据保留 |
| 实时指标 | `realtime_metrics` | 指标定义 | 指标编号、指标名称、聚合窗口 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 死信重放与流式血缘联动

死信「标记重放」不允许只改处置状态，必须与流式血缘的有效边在同一次提交里落库：

1. 读取死信来源记录（订单主题、分区、原始消息体），生成**重放血缘回执**
   （内部表 `replay_lineage_receipts`），回执保留重放批次 `RPLB-xxxx` 与有效血缘边。
2. 固定顺序：先停用该主题下的异常血缘边（`stream_lineage_edges` 中状态为「异常」
   的边置为「已停用」），再建立本批次的重放边；同来源同目标已存在有效边时先停用
   旧边，保证一个批次只形成一个可查询边集，不产生重复边。
3. 全部成功后处置状态才置为「已重放」；任一步失败整体回滚到提交前快照，
   杜绝「页面已重放、图上仍异常」的错位。
4. 任务中断后处置状态保持「待处置」，可凭进行中的回执调
   `POST /api/dead_letter/{id}/replay/start` / `/replay/resume` 恢复同一批次；
   流式血缘侧通过 `replay_lineage_receipt_refs` 保留对应的回执来源记录，
   两侧可按批次号互相核对。

相关接口：

- `GET  /api/dead_letter/{id}/replay_receipts` 死信侧回执（批次 + 有效边）
- `POST /api/dead_letter/{id}/replay/start` 建立批次并停用异常边（中断点）
- `POST /api/dead_letter/{id}/replay/resume` 恢复批次，同步有效边并完成处置
- `GET  /api/stream_lineage/edges` 血缘边表（有效/异常/已停用 + 批次号）
- `GET  /api/stream_lineage/replay_receipts` 流式血缘侧回执来源引用

领域：数据工程
子方向：数仓与数据湖
