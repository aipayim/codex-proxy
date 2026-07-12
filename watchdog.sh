#!/bin/bash
# Codex Proxy watchdog - 每 10 秒检测进程存活，崩溃自动重启
# 由 /etc/wsl.conf [boot] command 引导，零 CPU 占用
PROXY_DIR="/mnt/e/codex-proxy"
PID_FILE="$PROXY_DIR/proxy.pid"
LOG="$PROXY_DIR/proxy.log"

while true; do
  if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
    : # running
  else
    echo "[watchdog] $(date) proxy not running, starting..." >> "$LOG"
    nohup node "$PROXY_DIR/proxy.js" >> "$LOG" 2>&1 &
    echo $! > "$PID_FILE"
  fi
  sleep 10
done
