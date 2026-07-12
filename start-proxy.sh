#!/bin/bash
# 通过 systemd 风格启动代理 + watchdog
# 适用于 WSL2（无 systemd）环境
# 用法:  bash start-proxy.sh          # 前台启动（手动用）
#        bash start-proxy.sh --boot   # 后台启动（WSL 开机用）
PROXY_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$PROXY_DIR/proxy.pid"
LOG="$PROXY_DIR/proxy.log"
WATCHDOG="$PROXY_DIR/watchdog.sh"

if [ "$1" = "--boot" ]; then
  # 由 /etc/wsl.conf 调用：后台运行
  setsid nohup bash "$WATCHDOG" > /dev/null 2>&1 &
  exit 0
fi

# 手动前台启动（等价 systemctl start codex-proxy）
echo "Starting proxy + watchdog in background..."
setsid nohup bash "$WATCHDOG" > /dev/null 2>&1 &
echo "Watchdog PID: $!"
sleep 2
echo "Status:"
curl -sf http://localhost:3456/__status > /dev/null 2>&1 && echo "  Proxy is RUNNING" || echo "  Proxy starting..."
echo ""
echo "Stop proxy:   pkill -f 'node.*proxy\.js'"
echo "Stop watchdog: pkill -f watchdog.sh"
echo "View log:     tail -f $LOG"
