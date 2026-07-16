#!/bin/bash
# Langfuse Cleaner — 停止脚本
set -e

# 查找 uvicorn 进程
PID=$(pgrep -f "uvicorn main:app.*8899" || true)

if [ -z "$PID" ]; then
    echo "未找到运行中的进程"
    exit 0
fi

echo "正在停止 PID: $PID ..."
kill $PID

# 等待进程退出
sleep 1
if kill -0 $PID 2>/dev/null; then
    echo "进程未响应，强制终止..."
    kill -9 $PID
fi

echo "已停止"
