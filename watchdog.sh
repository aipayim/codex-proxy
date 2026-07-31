#!/bin/bash
PROXY_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$PROXY_DIR/proxy.pid"
LOG="$PROXY_DIR/proxy.log"
CONFIG_FILE="$PROXY_DIR/config.json"
LOG_ROTATOR="$PROXY_DIR/proxy-log-rotator.js"
LOCK_FILE="$PROXY_DIR/.watchdog.lock"
WATCHDOG_RELOAD_FILE="$PROXY_DIR/.watchdog-reload"
STARTUP_GRACE=30

exec 200>"$LOCK_FILE"
flock -n 200 || exit 0

CHILD_PID=""
START_TIME=$(date +%s)
PROXY_ABS="$PROXY_DIR/proxy.js"
if [ "$1" = "--rotate-log-only" ]; then
  if [ -f "$LOG_ROTATOR" ]; then
    node "$LOG_ROTATOR" "$LOG" "$CONFIG_FILE" < /dev/null
  else
    echo "[watchdog] $(date) log rotator is missing; proxy.log was not rotated" >&2
    exit 1
  fi
  exit 0
fi

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

watchdog_log() {
  # The proxy's rotator owns proxy.log while the listener is live.  Do not
  # create an unbounded direct-append fallback if that required helper is gone.
  local message=$1
  local bound_pid
  bound_pid=$(get_port_pid)
  if [ -f "$LOG_ROTATOR" ] && is_our_proxy "$bound_pid"; then
    printf '%s\n' "$message" >> "$LOG"
  else
    printf '%s\n' "$message" >&2
  fi
}

stop_our_proxy() {
  local pid=$1
  if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then return 0; fi
  watchdog_log "[watchdog] $(date) stopping stale proxy PID $pid (SIGTERM)"
  kill "$pid" 2>/dev/null
  local i=0
  while [ $i -lt 10 ]; do
    kill -0 "$pid" 2>/dev/null || return 0
    sleep 1
    i=$((i + 1))
  done
  watchdog_log "[watchdog] $(date) PID $pid did not exit, sending SIGKILL"
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
  # Dashboard restarts request a supervisor re-exec after the old proxy exits.
  # exec keeps this process as the single lock owner while re-reading this file.
  if [ -f "$WATCHDOG_RELOAD_FILE" ]; then
    rm -f "$WATCHDOG_RELOAD_FILE"
    watchdog_log "[watchdog] $(date) reloading watchdog after dashboard restart"
    exec /bin/bash "$PROXY_DIR/watchdog.sh"
  fi

  NEED_START=false

  BOUND_PID=$(get_port_pid)

  if [ -n "$BOUND_PID" ]; then
    if is_our_proxy "$BOUND_PID"; then
      CHILD_PID="$BOUND_PID"
    else
      watchdog_log "[watchdog] $(date) WARNING: port :3456 used by PID $BOUND_PID (not our proxy.js), not killing"
    fi
  else
    if [ -n "$CHILD_PID" ] && kill -0 "$CHILD_PID" 2>/dev/null; then
      # Old process is alive but port is free — likely draining
      NOW=$(date +%s)
      DRAIN_AGE=$((NOW - START_TIME))
      if [ "$DRAIN_AGE" -ge 30 ]; then
        watchdog_log "[watchdog] $(date) drain timeout ${DRAIN_AGE}s, killing orphan PID $CHILD_PID"
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
    if [ -f "$LOG_ROTATOR" ]; then
      watchdog_log "[watchdog] $(date) starting proxy..."
      # Keep the proxy PID as the supervised child while a separate process owns
      # the log descriptor and can rotate it without copy-truncate races.
      node "$PROXY_ABS" > >(node "$LOG_ROTATOR" "$LOG" "$CONFIG_FILE") 2>&1 &
      CHILD_PID=$!
      START_TIME=$(date +%s)
    else
      watchdog_log "[watchdog] $(date) ERROR: proxy-log-rotator.js is required; refusing unrotated proxy logging"
    fi
  fi
  sleep 10
done
