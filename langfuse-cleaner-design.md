# Langfuse Data Cleaner — 设计文档

> 一个极简的 Langfuse 数据清理工具，Web 页面管理定时清理任务，FastAPI + uv + JSON 文件存储。

---

## 1. 背景与目标

### 1.1 问题

Castrel 自建 Langfuse（`langfuse.cloudwise.com`，docker-compose 部署，v3 OSS 版）缺少数据保留功能，数据无限增长。

### 1.2 目标

- Web 页面管理清理任务（创建、编辑、启停、立即执行）
- 支持选择 Langfuse 项目、选择清理范围（7/15/30 天以前）、cron 表达式配置执行时间
- 每次只删 200 条，分批执行直到完成
- 所有存储基于 JSON 文件，无需数据库
- 简单密码登录保护
- 配置写 `.env`，uv 管理环境

### 1.3 非目标

不做断点续传、不做执行日志持久化、不做 ClickHouse TTL、不做多用户、不做高可用。

---

## 2. 架构

```
浏览器 (login.html / tasks.html)
       │ HTTP fetch
┌──────▼──────────────────────────┐
│  FastAPI 后端                     │
│  ├─ /api/auth/login  登录认证     │
│  ├─ /api/projects    docker exec 项目列表 │
│  └─ /api/tasks       任务 CRUD + 启停 + 执行 │
│  ├─ APScheduler      内存 cron 调度  │
│  └─ data/tasks.json  任务持久化     │
└──────┬──────────────────────────┘
       │ docker exec (psql)            HTTP Basic Auth (每任务独立 key)
┌──────▼──────────────────────────┐
│  Langfuse 部署环境                │
│  ├─ PostgreSQL 容器 (项目列表)      │
│  └─ Langfuse Public API           │
│     GET    /api/public/traces     │
│     DELETE /api/public/traces     │
└─────────────────────────────────┘
```

| 层 | 技术 | 理由 |
|---|---|---|
| 前端 | 纯 HTML + 原生 JS | 2 个页面，无构建工具 |
| 后端 | FastAPI + uv | 异步 I/O，uv 管理依赖 |
| 调度 | APScheduler | 进程内 cron，不碰系统 crontab |
| 存储 | 单文件 `data/tasks.json` | 数据量极小，无并发问题 |
| 项目列表 | docker exec + psql | 同机部署，无需 API Key |
| API Key | 每任务独立存储 | 隔离各项目权限，非全局共享 |

---

## 3. 数据模型

### 3.1 目录结构

```
langfuse-cleaner/
├── pyproject.toml
├── .env                          # 环境变量（gitignore）
├── .env.example                  # 环境变量模板
├── main.py                       # FastAPI 入口，全部逻辑
├── static/
│   ├── login.html
│   └── tasks.html
└── data/
    └── tasks.json                # 任务数据
```

### 3.2 .env

```bash
# 登录密码（明文，首次启动写死）
LOGIN_PASSWORD=admin123

# JWT 签名密钥（随机字符串即可）
JWT_SECRET=dev-secret-change-me

# Langfuse 连接信息
LANGFUSE_HOST=https://langfuse.cloudwise.com

# Langfuse 数据库容器名（用于 docker exec 获取项目列表）
POSTGRES_CONTAINER=langfuse-postgres-1
```

### 3.3 tasks.json

```json
[
  {
    "id": "a1b2c3d4",
    "name": "清理生产项目30天前数据",
    "project_id": "proj_abc123",
    "project_name": "castrel-prod",
    "public_key": "pk-lf-...",
    "secret_key": "sk-lf-...",
    "retention_days": 30,
    "batch_size": 200,
    "schedule": "0 3 * * *",
    "status": "active",
    "created_at": "2026-07-15T10:00:00Z",
    "updated_at": "2026-07-15T10:30:00Z"
  }
]
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | string | 8 位 hex |
| `name` | string | 任务名称 |
| `project_id` | string | Langfuse 项目 ID |
| `project_name` | string | 项目名称（冗余展示用） |
| `public_key` | string | 项目级别的 Public Key（pk-lf-...） |
| `secret_key` | string | 项目级别的 Secret Key（sk-lf-...） |
| `retention_days` | int | 7 / 15 / 30 |
| `batch_size` | int | 每批删除条数，默认 200 |
| `schedule` | string | 5 字段 cron（分 时 日 月 周） |
| `status` | string | `active` / `paused` / `deleted` |
| `created_at` | string | ISO 8601 |
| `updated_at` | string | ISO 8601 |

---

## 4. API 接口

### 4.1 认证

所有 `/api/` 接口（除 login）需带 `Authorization: Bearer <token>`。

JWT HS256，payload 为空，24h 过期。前端登录后存 localStorage。

```
POST /api/auth/login
  Body: {"password": "xxx"}
  → 200: {"token": "xxx"}
  → 401: {"error": "密码错误"}
```

### 4.2 接口清单

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/auth/login` | 登录 |
| GET | `/api/projects` | Langfuse 项目列表 |
| GET | `/api/tasks` | 任务列表 |
| POST | `/api/tasks` | 创建任务 |
| PUT | `/api/tasks/{id}` | 更新任务 |
| DELETE | `/api/tasks/{id}` | 删除任务（软删除） |
| POST | `/api/tasks/{id}/execute` | 立即执行 |
| POST | `/api/tasks/{id}/toggle` | 切换启停（active ↔ paused） |

接口精简为 8 个，启停合并为一个 toggle。

### 4.3 关键接口

**GET /api/projects** — 通过 `docker exec` 直连 PostgreSQL 获取项目列表，返回 `[{id, name}]`。无需 API Key。

**POST /api/tasks** — 请求体：

```json
{
  "name": "...",
  "project_id": "proj_xxx",
  "project_name": "xxx",
  "public_key": "pk-lf-...",
  "secret_key": "sk-lf-...",
  "retention_days": 30,
  "batch_size": 200,
  "schedule": "0 3 * * *"
}
```

校验：`name` 必填，`project_id` 必填，`public_key` / `secret_key` 必填且需分别以 `pk-lf-` / `sk-lf-` 开头，`retention_days` 必须为 7/15/30，`batch_size` 50-200，`schedule` 合法 cron 5 字段。

**POST /api/tasks/{id}/execute** — 异步执行，后台跑完输出到 stdout。返回 202。

---

## 5. 核心逻辑

### 5.1 文件存储

单文件读写，`threading.Lock` 互斥：

```python
class TaskStore:
    def __init__(self, path="data/tasks.json"): ...
    def list(self) -> list[dict]: ...
    def get(self, id: str) -> dict | None: ...
    def insert(self, task: dict) -> None: ...
    def update(self, id: str, data: dict) -> None: ...
    def soft_delete(self, id: str) -> None: ...
```

### 5.2 清理流程

```python
async def cleanup(host, public_key, secret_key, retention_days, batch_size):
    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    
    # 1. 分页收集所有过期 trace ID
    ids = []
    page = 1
    while True:
        resp = GET /traces?toTimestamp={cutoff}&page={page}&limit=50
        ids += [t.id for t in resp.data]
        if page >= resp.meta.totalPages: break
        page += 1
    
    # 2. 分批删除
    for i in range(0, len(ids), batch_size):
        batch = ids[i:i+batch_size]
        DELETE /traces {"traceIds": batch}
        sleep(0.5)
    
    return len(ids)
```

### 5.3 调度

```python
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()

# 启动时恢复
for t in store.list():
    if t["status"] == "active":
        scheduler.add_job(run_task, CronTrigger.from_crontab(t["schedule"]),
                          args=[t["id"]], id=t["id"])

# 任务变更时动态 add_job / remove_job / reschedule_job
```

---

## 6. 前端

### 6.1 login.html

居中卡片：密码输入框 + 登录按钮。失败显示红字提示。

### 6.2 tasks.html

顶部"新建任务"按钮，下方任务卡片列表。

每张卡片：任务名称、项目名、范围（"30天以前"）、cron 表达式、状态标签、操作按钮（启停 / 立即执行 / 编辑）。

**弹窗表单**：

| 字段 | 控件 |
|---|---|
| 任务名称 | 文本输入框 |
| Langfuse 项目 | 下拉选择（加载 `/api/projects`） |
| 时间范围 | 下拉选择：7天以前 / 15天以前 / 30天以前 |
| cron 表达式 | 文本输入框，5 字段标准 cron |
| 每批数量 | 数字输入框，默认 200，范围 50-200 |

编辑模式下预填现有数据。

---

## 7. 部署

### 7.1 依赖 (`pyproject.toml`)

```toml
[project]
name = "langfuse-cleaner"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "fastapi",
    "uvicorn",
    "apscheduler",
    "pyjwt",
    "httpx",
]
```

### 7.2 启动

```bash
cp .env.example .env
# 编辑 .env，填入 Langfuse 密钥和登录密码

uv sync
uv run uvicorn main:app --host 127.0.0.1 --port 8899
```

### 7.3 systemd（可选）

```ini
[Service]
WorkingDirectory=/opt/langfuse-cleaner
ExecStart=/opt/langfuse-cleaner/.venv/bin/uvicorn main:app --host 127.0.0.1 --port 8899
```
