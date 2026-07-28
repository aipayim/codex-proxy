#!/bin/bash
PROXY_DIR="/mnt/e/codex-proxy"
PID_FILE="$PROXY_DIR/proxy.pid"
LOG="$PROXY_DIR/proxy.log"
LOCK_FILE="$PROXY_DIR/.watchdog.lock"
STARTUP_GRACE=30

exec 200>"$LOCK_FILE"
flock -n 200 || exit 0

CHILD_PID=""
START_TIME=0

is_our_proxy() {
  local pid=$1
  [ -n "$pid" ] && [ -f "/proc/$pid/cmdline" ] && cat "/proc/$pid/cmdline" 2>/dev/null | tr '\0' ' ' | grep -q "proxy.js"
}

while true; do
  NEED_START=false

  BOUND_PID=""
  if ss -tlnp 2>/dev/null | grep -q ":3456"; then
    BOUND_PID=$(ss -tlnp 2>/dev/null | grep ":3456" | head -1 | sed 's/.*pid=\([0-9]*\).*/\1/')
  fi

  if [ -n "$BOUND_PID" ]; then
    if is_our_proxy "$BOUND_PID"; then
      CHILD_PID="$BOUND_PID"
      START_TIME=0
    else
      echo "[watchdog] $(date) WARNING: port :3456 used by PID $BOUND_PID (not proxy.js), not killing" >> "$LOG"
    fi
  else
    if [ -n "$CHILD_PID" ] && kill -0 "$CHILD_PID" 2>/dev/null && [ "$START_TIME" -gt 0 ]; then
      ELAPSED=$(( $(date +%s) - START_TIME ))
      if [ "$ELAPSED" -ge "$STARTUP_GRACE" ]; then
        echo "[watchdog] $(date) PID $CHILD_PID still not listening after ${ELAPSED}s, killing" >> "$LOG"
        kill "$CHILD_PID" 2>/dev/null
        CHILD_PID=""
        NEED_START=true
      fi
    else
      CHILD_PID=""
      NEED_START=true
    fi
  fi

  if $NEED_START; then
    echo "[watchdog] $(date) starting proxy..." >> "$LOG"
    node "$PROXY_DIR/proxy.js" >> "$LOG" 2>&1 &
    CHILD_PID=$!
    START_TIME=$(date +%s)
  fi
  sleep 10
done
