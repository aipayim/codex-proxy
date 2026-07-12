#!/bin/bash
# resume-codex.sh - 在 Windows 新可见终端中打开 WSL 并执行指定命令
# 用法: resume-codex.sh <projectPath> <command...>
# 由 proxy.js 的 triggerResume() 调用

PROJECT_PATH="$1"
shift
COMMAND="$*"

# 单引号包裹命令，内部单引号转义
ESCAPED_COMMAND=$(printf '%s\n' "$COMMAND" | sed "s/'/'\\\\''/g")

exec /mnt/c/Windows/System32/cmd.exe /c start "Codex Resume" \
  /mnt/c/Windows/System32/wsl.exe bash -l -c \
  "cd '${PROJECT_PATH}' && ${ESCAPED_COMMAND}"
