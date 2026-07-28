#!/bin/bash
PROXY_DIR="/mnt/e/codex-proxy"
PID_FILE="$PROXY_DIR/proxy.pid"
LOG="$PROXY_DIR/proxy.log"
LOCK_FILE="$PROXY_DIR/.watchdog.lock"
STARTUP_GRACE=30

exec 200>"$LOCK_FILE"
flock -n 200 || exit 0

CHILD_PID=""
START_TIME=$(date +%s)
PROXY_ABS="$PROXY_DIR/proxy.js"

is_our_proxy() {
  local pid=$1
  [ -z "$pid" ] && return 1
  [ ! -f "/proc/$pid/cmdline" ] && return 1
  local cmdline
  cmdline=$(cat "/proc/$pid/cmdline" 2>/dev/null | tr '\0' ' ')
  echo "$cmdline" | grep -q "$PROXY_ABS"
}

get_port_pid() {
  ss -ltnp "sport = :3456" 2>/dev/null | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2
}

stop_our_proxy() {
  local pid=$1
  if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then return 0; fi
  echo "[watchdog] $(date) stopping stale proxy PID $pid (SIGTERM)" >> "$LOG"
  kill "$pid" 2>/dev/null
  local i=0
  while [ $i -lt 10 ]; do
    kill -0 "$pid" 2>/dev/null || return 0
    sleep 1
    i=$((i + 1))
  done
  echo "[watchdog] $(date) PID $pid did not exit, sending SIGKILL" >> "$LOG"
  kill -9 "$pid" 2>/dev/null
  sleep 1
}

# On startup: verify existing PID_FILE
if [ -f "$PID_FILE" ]; then
  EXISTING_PID=$(cat "$PID_FILE" 2>/dev/null)
  if [ -n "$EXISTING_PID" ] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    if is_our_proxy "$EXISTING_PID"; then
      PORT_PID=$(get_port_pid)
      if [ -n "$PORT_PID" ] && [ "$PORT_PID" = "$EXISTING_PID" ]; then
        CHILD_PID="$EXISTING_PID"
      fi
    fi
  fi
fi

while true; do
  NEED_START=false

  BOUND_PID=$(get_port_pid)

  if [ -n "$BOUND_PID" ]; then
    if is_our_proxy "$BOUND_PID"; then
      CHILD_PID="$BOUND_PID"
    else
      echo "[watchdog] $(date) WARNING: port :3456 used by PID $BOUND_PID (not our proxy.js), not killing" >> "$LOG"
    fi
  else
    if [ -n "$CHILD_PID" ] && kill -0 "$CHILD_PID" 2>/dev/null; then
      # Old process is alive but port is free — likely draining
      NOW=$(date +%s)
      DRAIN_AGE=$((NOW - START_TIME))
      if [ "$DRAIN_AGE" -ge 30 ]; then
        echo "[watchdog] $(date) drain timeout ${DRAIN_AGE}s, killing orphan PID $CHILD_PID" >> "$LOG"
        kill -9 "$CHILD_PID" 2>/dev/null
        sleep 1
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
    node "$PROXY_ABS" >> "$LOG" 2>&1 &
    CHILD_PID=$!
    START_TIME=$(date +%s)
  fi
  sleep 10
done
