#!/usr/bin/env bash
# Launch one configured resume command in a visible Windows terminal.
# Arguments are passed separately so project paths and commands are not shell-concatenated.
set -uo pipefail

PROJECT_PATH="${1:?missing project path}"
COMMAND="${2:?missing command}"
PID_FILE="${3:?missing pid file}"
STATUS_FILE="${4:?missing status file}"
TITLE="${5:-Codex Resume}"
CMD_PATH="${6:-/mnt/c/Windows/System32/cmd.exe}"
WSL_PATH="${CODEX_RESUME_WSL_PATH:-/mnt/c/Windows/System32/wsl.exe}"

# cmd.exe's `start` treats any leading "/" as a switch, so a POSIX path like
# /mnt/c/... must be converted to its Windows backslash form before `start`.
WSL_WIN_PATH="$WSL_PATH"
if [[ "$WSL_WIN_PATH" == /mnt/* ]]; then
  if command -v wslpath >/dev/null 2>&1; then
    WSL_WIN_PATH="$(wslpath -w "$WSL_WIN_PATH" 2>/dev/null || true)"
  fi
  if [[ -z "$WSL_WIN_PATH" || "$WSL_WIN_PATH" == /* ]]; then
    WSL_WIN_PATH="C:${WSL_PATH#/mnt/c}"
  fi
fi

write_status() {
  local phase="$1"
  local pid="${2:-0}"
  local exit_code="${3:-}"
  local now
  now="$(date +%s)000"
  printf '%s\t%s\t%s\t%s\n' "$phase" "$pid" "$now" "$exit_code" > "$STATUS_FILE"
}

RUNNER_FILE="$(mktemp /tmp/codex-resume-runner.XXXXXX)" || {
  write_status "launcher_failed" 0 1
  exit 1
}

write_runner_value() {
  printf '%s=%q\n' "$1" "$2"
}

{
  printf '%s\n' '#!/usr/bin/env bash'
  write_runner_value PROJECT_PATH "$PROJECT_PATH"
  write_runner_value COMMAND "$COMMAND"
  write_runner_value PID_FILE "$PID_FILE"
  write_runner_value STATUS_FILE "$STATUS_FILE"
  write_runner_value RUNNER_FILE "$RUNNER_FILE"
  cat <<'RUNNER'
set -uo pipefail

write_status() {
  local phase="$1"
  local pid="${2:-0}"
  local exit_code="${3:-}"
  local now
  now="$(date +%s)000"
  printf '%s\t%s\t%s\t%s\n' "$phase" "$pid" "$now" "$exit_code" > "$STATUS_FILE"
}

command_pid=""
cleanup() {
  rm -f -- "$PID_FILE" "$RUNNER_FILE"
}
terminate() {
  if [[ "$command_pid" =~ ^[0-9]+$ ]] && (( command_pid > 1 )); then
    kill -- "-$command_pid" 2>/dev/null || kill "$command_pid" 2>/dev/null || true
  fi
  write_status "terminated" "${command_pid:-0}" 143
  exit 143
}

trap cleanup EXIT
trap terminate HUP INT TERM

if ! cd -- "$PROJECT_PATH"; then
  write_status "cd_failed" 0 1
  exit 1
fi

write_status "starting" 0 ""
setsid bash -lc "$COMMAND" &
command_pid=$!
printf 'pgid:%s\n' "$command_pid" > "$PID_FILE"
write_status "running" "$command_pid" ""

wait "$command_pid"
exit_code=$?
if (( exit_code == 0 )); then
  write_status "exited" "$command_pid" "$exit_code"
else
  write_status "failed" "$command_pid" "$exit_code"
fi
exit "$exit_code"
RUNNER
} > "$RUNNER_FILE"

chmod 700 "$RUNNER_FILE"
CMD_OUT="$( { "$CMD_PATH" /d /s /c start "$TITLE" "$WSL_WIN_PATH" bash -l "$RUNNER_FILE"; } 2>&1 )"
launcher_code=$?
if (( launcher_code != 0 )); then
  write_status "launcher_failed" 0 "$launcher_code"
  if [ -n "$CMD_OUT" ]; then
    CMD_OUT_MSG="$(printf '%s' "$CMD_OUT" | iconv -f GBK -t UTF-8//IGNORE 2>/dev/null || true)"
    [ -z "$CMD_OUT_MSG" ] && CMD_OUT_MSG="$CMD_OUT"
    CMD_OUT_MSG="$(printf '%s' "$CMD_OUT_MSG" | tr '\r\n' '  ' | tr -s ' ' | cut -c1-200)"
    printf '\t%s' "$CMD_OUT_MSG" >> "$STATUS_FILE"
  fi
  rm -f -- "$RUNNER_FILE"
  exit "$launcher_code"
fi

exit 0
