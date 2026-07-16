#!/bin/bash
# Langfuse Cleaner — 后台启动脚本
set -e

cd "$(dirname "$0")"

# 检查 .env 是否存在
if [ ! -f .env ]; then
    echo "⚠ 未找到 .env 文件，从 .env.example 复制一份并编辑"
    cp .env.example .env
    echo "请编辑 .env 填入 Langfuse API 密钥后重新运行"
    exit 1
fi

# 确保依赖已安装
if [ ! -d .venv ]; then
    echo "👉 首次运行，安装依赖..."
    uv sync
fi

# 后台启动
nohup uv run uvicorn main:app --host 0.0.0.0 --port 8899 > app.log 2>&1 &
PID=$!

echo "✅ Langfuse Cleaner 已启动"
echo "   PID: $PID"
echo "   地址: http://116.204.118.233:8899"
echo "   日志: tail -f app.log"
echo "   停止: kill $PID"
