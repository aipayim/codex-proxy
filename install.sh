#!/bin/bash
# Codex Proxy Installer — 全环境引导脚本
# 支持 WSL2（watchdog 守护）和 Linux systemd 环境
# 用法:
#   bash install.sh                     # 交互式全安装
#   WRAPPER_DIR=~/bin CODEX_BIN=/path/to/codex bash install.sh  # 自定义路径
set -e

PROXY_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROXY_DIR"

echo "=== Codex Proxy Installer ==="
echo "Proxy directory: $PROXY_DIR"
echo ""

# ── 环境检测 ──────────────────────────────────────────────
IS_WSL=false
if grep -qi microsoft /proc/version 2>/dev/null; then
  IS_WSL=true
  echo "[detect] WSL2 environment"
fi

HAS_SYSTEMD=false
if command -v systemctl &>/dev/null; then
  HAS_SYSTEMD=true
  echo "[detect] systemd available"
fi

# ── 1. npm 依赖 ───────────────────────────────────────────
echo ""
echo "── Step 1/6: Install npm dependencies ──"
if [ ! -d "node_modules" ]; then
  npm install
  echo "[ok] npm dependencies installed"
else
  echo "[ok] node_modules already exists (skip)"
fi

# ── 2. 创建默认配置文件（如不存在） ──────────────────────────
echo ""
echo "── Step 2/6: Create default config files ──"

if [ ! -f "config.json" ]; then
  cat > config.json << 'CONFIG'
{
  "webhookUrl": "",
  "prices": { "inputPer1M": 0, "outputPer1M": 0 },
  "bytesPerToken": 3,
  "notifications": { "sound": true, "desktop": true },
  "autoRecover": true,
  "autoRecoverInterval": 1,
  "autoRecoverCodes": [401, 402, 403, 429, 500, 502, 503, 504],
  "autoRecoverDiscarded": false,
  "autoRecoverDaily": false,
  "autoRecoverDailyDays": 1,
  "autoRecoverDailyHour": 0,
  "autoRecoverDailyMinute": 3,
  "autoRecoverPoll": false,
  "autoRecoverPollInterval": 5,
  "autoRecoverPollCodes": [500, 502, 503, 504],
  "autoRecoverDelays": [800],
  "roundRobin": false,
  "lockAfterFailCount": 3,
  "lockFailCodes": ["401", "403"],
  "enableAutoLock": true,
  "logFile": true,
  "logRetentionDays": 7,
  "logDetail": "full",
  "weeklySortBy": "priority",
  "autoResume": false,
  "autoResumeIdleMinutes": 10,
  "autoResumeDebounceMinutes": 3,
  "cmdPath": "/mnt/c/Windows/System32/cmd.exe",
  "autoResumeProjects": []
}
CONFIG
  echo "[ok] Created config.json with default settings"
else
  echo "[ok] config.json already exists (skip)"
fi

if [ ! -f "state.json" ]; then
  echo '{"keys":[],"activeKey":null,"dailyLog":{}}' > state.json
  echo "[ok] Created empty state.json"
else
  echo "[ok] state.json already exists (skip)"
fi

if [ ! -f "keys.json" ]; then
  echo '[]' > keys.json
  echo "[ok] Created empty keys.json (add your API keys via dashboard)"
else
  echo "[ok] keys.json already exists (skip)"
fi

# ── 3. 检测 Node.js 版本 ───────────────────────────────────
echo ""
echo "── Step 3/6: Check Node.js version ──"
NODE_V=$(node -v 2>/dev/null || echo "none")
if [ "$NODE_V" = "none" ]; then
  echo "[error] Node.js not found. Install Node.js >= 16 first."
  echo "  https://nodejs.org/"
  exit 1
fi
NODE_MAJOR=$(echo "$NODE_V" | sed 's/v//' | cut -d. -f1)
if [ "$NODE_MAJOR" -lt 16 ]; then
  echo "[error] Node.js $NODE_V detected, need >= 16"
  exit 1
fi
echo "[ok] Node.js $NODE_V"

# ── 4. 安装系统级服务 ─────────────────────────────────────
echo ""
echo "── Step 4/6: Install system service ──"

if $HAS_SYSTEMD && ! $IS_WSL; then
  # ── Linux + systemd ──────────────────────────────────
  SERVICE_SRC="codex-proxy.service"
  SERVICE_DST="/etc/systemd/system/codex-proxy.service"
  if [ -f "$SERVICE_SRC" ]; then
    echo "[*] Installing systemd service..."
    sed "s|{{PROXY_DIR}}|$PROXY_DIR|g" "$SERVICE_SRC" > "$SERVICE_DST"
    systemctl daemon-reload
    systemctl enable codex-proxy
    systemctl restart codex-proxy
    echo "[ok] systemd service installed and started"
    echo "       Status: systemctl status codex-proxy"
    echo "       Logs:   journalctl -u codex-proxy -f"
  else
    echo "[warn] codex-proxy.service not found, skip systemd"
  fi

elif $IS_WSL; then
  # ── WSL2 环境 ─────────────────────────────────────────
  echo "[*] WSL2 detected — setting up watchdog daemon..."

  # ── 4a. 创建 /usr/local/bin/codex-watchdog.sh ────────
  WATCHDOG_BOOT="/usr/local/bin/codex-watchdog.sh"
  cat > "$WATCHDOG_BOOT" << WDBOOT
#!/bin/bash
# WSL 开机启动：启动 Codex Proxy watchdog
# 由 /etc/wsl.conf [boot] command 调用
# 如需开机时运行额外命令（如修复网络路由），在此行前添加
$PROXY_DIR/start-proxy.sh --boot
exit 0
WDBOOT
  chmod +x "$WATCHDOG_BOOT"
  echo "[ok] Created $WATCHDOG_BOOT"

  # ── 4b. 配置 /etc/wsl.conf ───────────────────────────
  if [ -f /etc/wsl.conf ]; then
    if grep -q "codex-watchdog" /etc/wsl.conf 2>/dev/null; then
      echo "[ok] /etc/wsl.conf already has codex-watchdog entry (skip)"
    else
      echo "" >> /etc/wsl.conf
      echo "# Codex Proxy watchdog (added by install.sh)" >> /etc/wsl.conf
      echo "[boot]" >> /etc/wsl.conf
      echo "command = $WATCHDOG_BOOT" >> /etc/wsl.conf
      echo "[ok] Appended watchdog entry to /etc/wsl.conf"
    fi
  else
    cat > /etc/wsl.conf << WSL_CONF
[network]
generateResolvConf = true

# Codex Proxy watchdog — 开机自动启动代理
[boot]
command = $WATCHDOG_BOOT
WSL_CONF
    echo "[ok] Created /etc/wsl.conf"
  fi

  # ── 4c. 存活检测 ─────────────────────────────────────
  # 检查 watchdog 是否已在运行
  WATCHDOG_RUNNING=false
  if [ -f "$PROXY_DIR/proxy.pid" ] && kill -0 $(cat "$PROXY_DIR/proxy.pid") 2>/dev/null; then
    WATCHDOG_RUNNING=true
  fi
  if $WATCHDOG_RUNNING; then
    echo "[ok] Proxy is already running"
  else
    echo "[info] Proxy not running — start manually: bash start-proxy.sh"
  fi

else
  # ── 其他（无 systemd 也非 WSL） ──────────────────────
  echo "[warn] No systemd found and not WSL — skipping service installation"
  echo "       Start manually: node \"$PROXY_DIR/proxy.js\" &"
fi

# ── 5. 创建 codex 包装脚本 ────────────────────────────────
echo ""
echo "── Step 5/6: Create codex wrapper script ──"

WRAPPER_DIR="${WRAPPER_DIR:-$HOME/bin}"
mkdir -p "$WRAPPER_DIR"

# 尝试自动检测 codex 二进制路径
CODEX_BIN="${CODEX_BIN:-}"
if [ -z "$CODEX_BIN" ]; then
  for candidate in \
    /usr/lib/node_modules/@openai/codex/bin/codex.js \
    /usr/local/bin/codex \
    /opt/codex/bin/codex \
    /mnt/e/codex/bin/codex \
    /mnt/c/Users/*/codex/bin/codex \
    "$HOME/codex/bin/codex"; do
    if [ -f "$candidate" ] && [ -x "$candidate" ]; then
      CODEX_BIN="$candidate"
      break
    fi
  done
fi
if [ -z "$CODEX_BIN" ]; then
  CODEX_BIN=$(command -v codex 2>/dev/null || echo "/usr/local/bin/codex")
fi

cat > "$WRAPPER_DIR/codex" << WRAPPER
#!/bin/bash
# Codex CLI 包装脚本
# 在启动 codex 前确保代理 + 守护进程正在运行
# 由 install.sh 自动生成
PROXY_DIR="$PROXY_DIR"

if ! curl -sf http://localhost:3456/__status > /dev/null 2>&1; then
  echo "[codex] Starting proxy..." >&2
  if [ -f "\$PROXY_DIR/watchdog.sh" ]; then
    setsid nohup bash "\$PROXY_DIR/watchdog.sh" > /dev/null 2>&1 &
    sleep 3
  else
    nohup node "\$PROXY_DIR/proxy.js" > "\$PROXY_DIR/proxy.log" 2>&1 &
    sleep 2
  fi
fi

exec $CODEX_BIN "\$@"
WRAPPER
chmod +x "$WRAPPER_DIR/codex"
echo "[ok] Wrapper script created at $WRAPPER_DIR/codex"
echo "     Target codex binary: $CODEX_BIN"

# ── 5b. 确保 WRAPPER_DIR 在 PATH 中 ──────────────────────
BASHRC="$HOME/.bashrc"
if [ -f "$BASHRC" ]; then
  if grep -q "export PATH=\"\$HOME/bin:\$PATH\"" "$BASHRC" 2>/dev/null; then
    echo "[ok] PATH already configured in .bashrc"
  else
    echo "" >> "$BASHRC"
    echo "# Codex Proxy: ensure wrapper is in PATH" >> "$BASHRC"
    echo "export PATH=\"\$HOME/bin:\$PATH\"" >> "$BASHRC"
    echo "[ok] Added PATH to .bashrc"
  fi
fi

# login shell (bash -l) 也加载 .bashrc
PROFILE="$HOME/.profile"
if [ -f "$PROFILE" ]; then
  if grep -q "\. ~/.bashrc" "$PROFILE" 2>/dev/null || grep -q "source ~/.bashrc" "$PROFILE" 2>/dev/null; then
    : # already sources .bashrc
  else
    echo "" >> "$PROFILE"
    echo "# Source .bashrc for login shells" >> "$PROFILE"
    echo "if [ -f ~/.bashrc ]; then . ~/.bashrc; fi" >> "$PROFILE"
    echo "[ok] Added .bashrc sourcing to .profile"
  fi
fi

# ── 6. 完成 ───────────────────────────────────────────────
echo ""
echo "── Step 6/6: Summary ──"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Proxy directory : $PROXY_DIR"
echo " Environment     : $($IS_WSL && echo 'WSL2' || echo 'Linux')"
echo " Node.js         : $NODE_V"
if $IS_WSL; then
  echo " Watchdog        : $([ -f "$PROXY_DIR/watchdog.sh" ] && echo 'installed' || echo 'missing')"
  echo " /etc/wsl.conf   : $(grep -q codex-watchdog /etc/wsl.conf 2>/dev/null && echo 'configured' || echo 'skip')"
fi
echo " Wrapper script  : $WRAPPER_DIR/codex → $CODEX_BIN"
echo ""
echo " Next steps:"
echo "   1. Add your API keys via dashboard http://localhost:3456/"
echo "      or edit keys.json directly"
if $IS_WSL; then
  echo "   2. Restart WSL for boot auto-start:"
  echo "      Windows PowerShell: wsl --shutdown"
  echo "      Then reopen WSL terminal"
fi
echo "   3. Open dashboard: http://localhost:3456/"
echo "   4. Run codex:      codex"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "=== Install complete ==="
