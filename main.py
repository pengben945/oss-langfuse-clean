"""
Langfuse Data Cleaner — FastAPI 后端
单文件应用，包含所有业务逻辑：认证、任务 CRUD、清理执行、cron 调度。
"""

import asyncio
import json
import logging
import os
import secrets
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import httpx
import jwt
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ── 配置 ──────────────────────────────────────────────────────────────

load_dotenv()

LOGIN_PASSWORD = os.getenv("LOGIN_PASSWORD", "admin123")
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
LANGFUSE_HOST = os.getenv("LANGFUSE_HOST", "https://langfuse.cloudwise.com").rstrip("/")
POSTGRES_CONTAINER = os.getenv("POSTGRES_CONTAINER", "langfuse-postgres-1")

# MinIO / S3 对象存储配置（可选，不配则跳过文件清理）
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "").strip()
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "").strip()
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "").strip()
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "langfuse").strip()
MINIO_EVENTS_PREFIX = os.getenv("MINIO_EVENTS_PREFIX", "events").strip().rstrip("/")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").strip().lower() in ("true", "1", "yes")

JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 24
DATA_DIR = "data"
TASKS_FILE = os.path.join(DATA_DIR, "tasks.json")

# ── 日志 ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("langfuse-cleaner")
# 抑制 httpx / httpcore 的请求日志
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ── 请求模型 ──────────────────────────────────────────────────────────


class LoginBody(BaseModel):
    password: str


class TaskBody(BaseModel):
    name: str
    project_id: str
    project_name: str = ""
    public_key: str = ""
    secret_key: str = ""
    retention_days: int
    batch_size: int = 200
    schedule: str


# ── 错误处理 ──────────────────────────────────────────────────────────


class AppError(Exception):
    """应用异常，message 会直接返回给前端。"""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message


# ── JSON 文件存储 ─────────────────────────────────────────────────────


class TaskStore:
    """线程安全的 JSON 文件存储，管理清理任务。"""

    def __init__(self, path: str = TASKS_FILE):
        self._path = path
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            self._write([])

    def _read(self) -> list[dict]:
        with self._lock:
            try:
                with open(self._path, encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                return []

    def _write(self, data: list[dict]) -> None:
        with self._lock:
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

    def list(self) -> list[dict]:
        """返回所有非删除状态的任务。"""
        return [t for t in self._read() if t.get("status") != "deleted"]

    def get(self, task_id: str) -> dict | None:
        for t in self._read():
            if t["id"] == task_id and t.get("status") != "deleted":
                return t
        return None

    def insert(self, task: dict) -> dict:
        tasks = self._read()
        task["id"] = secrets.token_hex(4)  # 8 位 hex
        task["created_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        task["updated_at"] = task["created_at"]
        if "status" not in task:
            task["status"] = "active"
        if "batch_size" not in task:
            task["batch_size"] = 200
        tasks.append(task)
        self._write(tasks)
        return task

    def update(self, task_id: str, data: dict) -> dict | None:
        tasks = self._read()
        for t in tasks:
            if t["id"] == task_id and t.get("status") != "deleted":
                # 不允许通过 update 修改 id, created_at, status（status 走 toggle）
                for key in ("name", "project_id", "project_name", "public_key", "secret_key",
                            "retention_days", "batch_size", "schedule"):
                    if key in data:
                        t[key] = data[key]
                t["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                self._write(tasks)
                return t
        return None

    def soft_delete(self, task_id: str) -> bool:
        tasks = self._read()
        for t in tasks:
            if t["id"] == task_id and t.get("status") != "deleted":
                t["status"] = "deleted"
                t["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                self._write(tasks)
                return True
        return False

    def set_status(self, task_id: str, status: str) -> dict | None:
        """设置任务状态：active / paused"""
        tasks = self._read()
        for t in tasks:
            if t["id"] == task_id and t.get("status") != "deleted":
                t["status"] = status
                t["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                self._write(tasks)
                return t
        return None


# ── 全局存储 ──────────────────────────────────────────────────────────

store = TaskStore()

# ── 认证 ──────────────────────────────────────────────────────────────

security = HTTPBearer()


def create_jwt() -> str:
    payload = {
        "sub": "admin",
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_jwt(token: str) -> dict:
    """验证 JWT，抛出 AppError 如果无效。"""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise AppError(401, "Token 已过期，请重新登录")
    except jwt.InvalidTokenError:
        raise AppError(401, "Token 无效，请重新登录")


def get_current_user(cred: HTTPAuthorizationCredentials = Depends(security)):
    verify_jwt(cred.credentials)
    return "admin"


# ── Langfuse API 客户端 ───────────────────────────────────────────────


async def langfuse_get(task: dict, path: str, params: dict | None = None) -> dict:
    """GET 请求 Langfuse Public API，使用任务自身的 API Key 认证。"""
    url = f"{LANGFUSE_HOST}{path}"
    auth = httpx.BasicAuth(task["public_key"], task["secret_key"])
    async with httpx.AsyncClient(timeout=30, trust_env=False) as client:
        resp = await client.get(url, auth=auth, params=params)
        if resp.status_code == 401:
            raise AppError(502, f"Langfuse 认证失败，请检查任务「{task['name']}」的 API Key")
        if resp.status_code >= 400:
            raise AppError(502, f"Langfuse API 错误: {resp.status_code} — {resp.text[:200]}")
        return resp.json()


async def langfuse_delete(task: dict, path: str, body: dict) -> dict:
    """DELETE 请求 Langfuse Public API，使用任务自身的 API Key 认证。"""
    url = f"{LANGFUSE_HOST}{path}"
    auth = httpx.BasicAuth(task["public_key"], task["secret_key"])
    async with httpx.AsyncClient(timeout=30, trust_env=False) as client:
        resp = await client.request("DELETE", url, auth=auth, json=body)
        if resp.status_code == 401:
            raise AppError(502, f"Langfuse 认证失败，请检查任务「{task['name']}」的 API Key")
        if resp.status_code >= 400:
            raise AppError(502, f"Langfuse API 错误: {resp.status_code} — {resp.text[:200]}")
        return resp.json()


# ── 清理逻辑 ──────────────────────────────────────────────────────────


async def run_cleanup(task: dict) -> int:
    """
    逐段清理 — 删除 retention_days 以前的旧数据（保留最近 N 天）。
    把时间范围拆成多个 4 小时窗口，向后追溯直到没有更多数据，
    避免一次查询时间范围过大导致 Langfuse 数据库 524。
    返回删除的总条数。
    """
    retention_days = task["retention_days"]
    batch_size = task.get("batch_size", 200)
    project_id = task["project_id"]
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=retention_days)
    hours_per_window = 4

    logger.info("=" * 60)
    logger.info(f"🧹 开始清理: {task['name']}")
    logger.info(f"   任务 ID: {task['id']}")
    logger.info(f"   保留天数: {retention_days} 天, 删除 {cutoff.strftime('%Y-%m-%dT%H:%M:%SZ')} 之前的数据")
    logger.info("=" * 60)

    total_deleted = 0
    total_batches = 0

    # 从 cutoff 开始，每次向前追溯 4 小时窗口
    window_idx = 0
    max_empty_windows = 24  # 连续 4 天空窗口则认为已无更多历史数据，停止
    consecutive_empty = 0

    while consecutive_empty < max_empty_windows:
        segment_end = cutoff - timedelta(hours=window_idx * hours_per_window)
        segment_start = segment_end - timedelta(hours=hours_per_window)
        start_iso = segment_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_iso = segment_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        label = segment_start.strftime("%m-%d %H:%M")
        window_idx += 1

        logger.info(f"📅 [{window_idx}] {label}")

        # 收集当前窗口的 trace ID
        day_ids: list[str] = []
        page = 1
        while True:
            try:
                params = {
                    "projectId": project_id,
                    "fromTimestamp": start_iso,
                    "toTimestamp": end_iso,
                    "page": page,
                    "limit": 50,
                }
                resp = await langfuse_get(task, "/api/public/traces", params)
                data = resp.get("data", [])
                day_ids.extend(t["id"] for t in data)
                meta = resp.get("meta", {})
                total_pages = meta.get("totalPages", 1)
                if page % 10 == 0 or page >= total_pages:
                    logger.info(f"   收集 {label}: 第 {page}/{total_pages} 页, 累计 {len(day_ids)} 条")
                if page >= total_pages:
                    break
                page += 1
            except AppError as e:
                logger.error(f"   ⚠ 收集失败 ({label}): {e.message}")
                break

        if not day_ids:
            consecutive_empty += 1
            continue

        consecutive_empty = 0  # 有数据，重置空窗口计数

        # 分批删除
        day_total = len(day_ids)
        day_deleted = 0
        for i in range(0, day_total, batch_size):
            batch = day_ids[i : i + batch_size]
            try:
                await langfuse_delete(task, "/api/public/traces", {"traceIds": batch})
                day_deleted += len(batch)
                total_batches += 1
                logger.info(f"   🗑️  删除 {len(batch)} 条, 窗口进度 {day_deleted}/{day_total}")
                await asyncio.sleep(0.5)
            except AppError as e:
                logger.error(f"   ⚠ 删除失败: {e.message}")
                await asyncio.sleep(1)

        total_deleted += day_deleted

    logger.info("-" * 60)
    logger.info(f"✅ 清理完成: 共 {total_batches} 批, "
                f"删除 {total_deleted} 条 trace")

    # ── MinIO 事件文件清理（可选，有 MinIO 配置才执行） ──
    minio_deleted = await cleanup_minio_events(task)
    if minio_deleted > 0:
        logger.info(f"🗄️  MinIO: {minio_deleted} 个过期事件文件已删除")

    logger.info("=" * 60)
    return total_deleted


# ── MinIO 事件文件清理 ──────────────────────────────────────────────────

# 用于 boto3 同步操作的线程池，避免阻塞 async event loop
_s3_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="s3")


def _minio_configured() -> bool:
    return bool(MINIO_ENDPOINT and MINIO_ACCESS_KEY and MINIO_SECRET_KEY)


async def cleanup_minio_events(task: dict) -> int:
    """
    删除 MinIO 中过期的 raw event 文件。
    返回删除的文件数，如果 MinIO 未配置或出错则返回 0。
    """
    if not _minio_configured():
        logger.info("ℹ️  MinIO 未配置，跳过文件清理")
        return 0

    retention_days = task["retention_days"]
    project_id = task["project_id"]
    bucket = MINIO_BUCKET
    prefixes = [
        f"{MINIO_EVENTS_PREFIX}/{project_id}/",
        f"{MINIO_EVENTS_PREFIX}/otel/{project_id}/",
    ]
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    logger.info(f"🗄️  开始 MinIO 清理: project={project_id}, "
                f"保留 {retention_days} 天, cutoff={cutoff.strftime('%Y-%m-%dT%H:%M:%SZ')}")

    try:
        loop = asyncio.get_running_loop()
        deleted = await loop.run_in_executor(
            _s3_executor,
            _delete_expired_objects,
            bucket, prefixes, cutoff,
        )
        return deleted
    except Exception as e:
        logger.error(f"⚠ MinIO 清理异常: {e}")
        return 0


def _delete_expired_objects(bucket: str, prefixes: list[str], cutoff: datetime) -> int:
    """在 MinIO 中列出过期对象并分批删除（同步函数，在线程池中运行）。"""
    import boto3
    import botocore.exceptions

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            use_ssl=MINIO_SECURE,
            verify=False,
            config=boto3.session.Config(
                connect_timeout=10,
                read_timeout=30,
                retries={"max_attempts": 2},
            ),
        )
    except Exception as e:
        logger.error(f"⚠ MinIO 客户端创建失败: {e}")
        return 0

    total_deleted = 0

    for prefix in prefixes:
        logger.info(f"   扫描前缀: {prefix}")
        expired_keys: list[str] = []

        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if "Contents" not in page:
                    continue
                for obj in page["Contents"]:
                    if obj["LastModified"].replace(tzinfo=timezone.utc) < cutoff:
                        expired_keys.append(obj["Key"])
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "NoSuchBucket":
                logger.error(f"   ⚠ Bucket '{bucket}' 不存在")
            elif code == "AccessDenied":
                logger.error(f"   ⚠ MinIO 权限不足")
            else:
                logger.error(f"   ⚠ 列出对象失败: {e}")
            return 0
        except botocore.exceptions.EndpointConnectionError as e:
            logger.error(f"   ⚠ MinIO 连接失败: {e}")
            return 0

        if not expired_keys:
            logger.info(f"   前缀 {prefix} 下无过期对象")
            continue

        logger.info(f"   前缀 {prefix} 下找到 {len(expired_keys)} 个过期对象，开始分批删除")

        # 按 1000 个一批删除（S3 API 限制）
        batch_size = 1000
        for i in range(0, len(expired_keys), batch_size):
            batch = expired_keys[i:i + batch_size]
            try:
                resp = s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": k} for k in batch]},
                )
                deleted_count = len(batch) - len(resp.get("Errors", []))
                total_deleted += deleted_count

                if resp.get("Errors"):
                    for err in resp["Errors"]:
                        logger.warning(f"      删除失败: {err['Key']} — {err['Message']}")

                logger.info(f"   🗑️  第 {i // batch_size + 1} 批: 删除 {deleted_count} 个")
                # 批间短暂休眠避免打满 MinIO
                import time
                time.sleep(0.1)
            except botocore.exceptions.ClientError as e:
                logger.error(f"   ⚠ 批量删除失败: {e}")
                import time
                time.sleep(1)

    logger.info(f"   MinIO 清理完成: 共删除 {total_deleted} 个文件")
    return total_deleted

scheduler = AsyncIOScheduler()


def _job_id(task_id: str) -> str:
    return f"cleanup_{task_id}"


def add_scheduled_job(task_id: str, cron_expr: str) -> None:
    """添加一个 cron 任务到调度器。"""
    job_id = _job_id(task_id)
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    scheduler.add_job(
        _execute_scheduled,
        CronTrigger.from_crontab(cron_expr),
        args=[task_id],
        id=job_id,
        replace_existing=True,
    )
    logger.info(f"已注册任务 {task_id}: {cron_expr}")


def remove_scheduled_job(task_id: str) -> None:
    """从调度器移除一个任务。"""
    job_id = _job_id(task_id)
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        logger.info(f"已移除任务 {task_id}")


def _execute_scheduled(task_id: str) -> None:
    """调度器回调 — 通过 event loop 投递后台任务。"""
    task = store.get(task_id)
    if task is None or task["status"] != "active":
        logger.info(f"任务 {task_id} 不存在或已暂停，跳过")
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # 调度器在线程中执行，不在事件循环内
        loop = asyncio.get_event_loop()
    asyncio.run_coroutine_threadsafe(_cleanup_wrapper(task), loop)


async def _cleanup_wrapper(task: dict) -> None:
    """用于调度器触发的一次清理执行。"""
    try:
        await run_cleanup(task)
    except AppError as e:
        logger.error(f"[{task['id']}] 清理出错: {e.message}")
    except Exception as e:
        logger.error(f"[{task['id']}] 清理异常: {e}")


def restore_jobs() -> None:
    """启动时恢复所有 active 状态的任务到调度器。"""
    for task in store.list():
        if task["status"] == "active":
            add_scheduled_job(task["id"], task["schedule"])
    logger.info(f"调度器已启动，已恢复 {len(scheduler.get_jobs())} 个任务")


# ── 校验 ──────────────────────────────────────────────────────────────


VALID_RETENTION_DAYS = {3, 7, 15, 30}


def validate_task_body(body: dict) -> None:
    """校验任务请求体，不合法时抛出 AppError。"""
    if not body.get("name", "").strip():
        raise AppError(400, "任务名称不能为空")
    if not body.get("project_id", "").strip():
        raise AppError(400, "项目 ID 不能为空")
    pk = body.get("public_key", "").strip()
    sk = body.get("secret_key", "").strip()
    if not pk:
        raise AppError(400, "Public Key 不能为空")
    if not sk:
        raise AppError(400, "Secret Key 不能为空")
    if not pk.startswith("pk-lf-"):
        raise AppError(400, "Public Key 格式不正确，应以 pk-lf- 开头")
    if not sk.startswith("sk-lf-"):
        raise AppError(400, "Secret Key 格式不正确，应以 sk-lf- 开头")
    rd = body.get("retention_days")
    if rd not in VALID_RETENTION_DAYS:
        raise AppError(400, f"retention_days 必须为 {sorted(VALID_RETENTION_DAYS)} 之一")
    bs = body.get("batch_size", 200)
    if bs < 50 or bs > 200:
        raise AppError(400, "batch_size 必须在 50-200 之间")
    schedule = body.get("schedule", "")
    # 简单 cron 5 字段校验
    parts = schedule.strip().split()
    if len(parts) != 5:
        raise AppError(400, "schedule 必须是合法的 5 字段 cron 表达式")
    try:
        CronTrigger.from_crontab(schedule)
    except Exception:
        raise AppError(400, "schedule cron 表达式不合法")


# ── App 初始化 ────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动
    logger.info("Langfuse Cleaner 启动")
    scheduler.start()
    restore_jobs()
    yield
    # 关闭
    scheduler.shutdown(wait=False)
    logger.info("Langfuse Cleaner 停止")


app = FastAPI(title="Langfuse Cleaner", lifespan=lifespan)


# ── 全局异常处理 ─────────────────────────────────────────────────────


@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"error": exc.message})


# ── 认证接口 ──────────────────────────────────────────────────────────


@app.post("/api/auth/login")
async def login(body: LoginBody):
    if body.password != LOGIN_PASSWORD:
        raise AppError(401, "密码错误")
    token = create_jwt()
    return {"token": token}


# ── 项目接口 ──────────────────────────────────────────────────────────


@app.get("/api/projects")
async def list_projects(user: str = Depends(get_current_user)):
    """通过 Docker exec 直连 PostgreSQL 获取 Langfuse 项目列表。"""
    try:
        result = subprocess.run(
            ["docker", "exec", POSTGRES_CONTAINER,
             "psql", "-U", "postgres", "-d", "postgres",
             "-t", "-A", "-F", "|",
             "-c", "SELECT id, name FROM projects ORDER BY name;"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            raise AppError(500, f"查询数据库失败: {result.stderr.strip() or result.stdout.strip()}")
        projects = []
        for line in result.stdout.strip().split("\n"):
            if "|" in line:
                pid, name = line.split("|", 1)
                projects.append({"id": pid.strip(), "name": name.strip()})
        return projects
    except FileNotFoundError:
        raise AppError(500, "未找到 docker 命令，请确认 docker 已安装且在 PATH 中")
    except subprocess.TimeoutExpired:
        raise AppError(500, "查询数据库超时")


# ── 任务 CRUD ─────────────────────────────────────────────────────────


@app.get("/api/tasks")
async def list_tasks(user: str = Depends(get_current_user)):
    return store.list()


@app.post("/api/tasks")
async def create_task(body: TaskBody, user: str = Depends(get_current_user)):
    validate_task_body(body.model_dump())
    task = store.insert({
        "name": body.name.strip(),
        "project_id": body.project_id.strip(),
        "project_name": body.project_name.strip(),
        "public_key": body.public_key.strip(),
        "secret_key": body.secret_key.strip(),
        "retention_days": body.retention_days,
        "batch_size": body.batch_size,
        "schedule": body.schedule.strip(),
    })
    if task["status"] == "active":
        add_scheduled_job(task["id"], task["schedule"])
    return task


@app.put("/api/tasks/{task_id}")
async def update_task(task_id: str, body: TaskBody, user: str = Depends(get_current_user)):
    existing = store.get(task_id)
    if existing is None:
        raise AppError(404, "任务不存在")
    validate_task_body(body.model_dump())
    updated = store.update(task_id, {
        "name": body.name.strip(),
        "project_id": body.project_id.strip(),
        "project_name": body.project_name.strip(),
        "public_key": body.public_key.strip(),
        "secret_key": body.secret_key.strip(),
        "retention_days": body.retention_days,
        "batch_size": body.batch_size,
        "schedule": body.schedule.strip(),
    })
    if updated and existing["schedule"] != body.schedule:
        add_scheduled_job(task_id, body.schedule)
    return updated


@app.delete("/api/tasks/{task_id}")
async def delete_task(task_id: str, user: str = Depends(get_current_user)):
    existing = store.get(task_id)
    if existing is None:
        raise AppError(404, "任务不存在")
    store.soft_delete(task_id)
    remove_scheduled_job(task_id)
    return {"message": "已删除"}


@app.post("/api/tasks/{task_id}/toggle")
async def toggle_task(task_id: str, user: str = Depends(get_current_user)):
    task = store.get(task_id)
    if task is None:
        raise AppError(404, "任务不存在")
    new_status = "paused" if task["status"] == "active" else "active"
    updated = store.set_status(task_id, new_status)
    if new_status == "active":
        add_scheduled_job(task_id, task["schedule"])
    else:
        remove_scheduled_job(task_id)
    return updated


@app.post("/api/tasks/{task_id}/execute")
async def execute_task(task_id: str, user: str = Depends(get_current_user)):
    task = store.get(task_id)
    if task is None:
        raise AppError(404, "任务不存在")
    # 异步后台执行
    asyncio.create_task(_cleanup_wrapper(task))
    return JSONResponse(status_code=202, content={"message": "已开始执行"})


# ── 静态文件 ──────────────────────────────────────────────────────────

# 确保静态文件目录存在
os.makedirs("static", exist_ok=True)

app.mount("/static", StaticFiles(directory="static", html=True), name="static")


@app.get("/")
async def root():
    return RedirectResponse(url="/static/login.html")
