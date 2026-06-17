#!/bin/bash
# Codex Proxy Installer — systemd service + dependency setup
set -e

PROXY_DIR="$(dirname "$(readlink -f "$0")")"
cd "$PROXY_DIR"

echo "=== Codex Proxy Installer ==="
echo "Proxy directory: $PROXY_DIR"

# 1. Install npm dependencies
if [ ! -d "node_modules" ]; then
  echo "[*] Installing dependencies..."
  npm install
else
  echo "[✓] Dependencies already installed"
fi

# 2. Create config.json if not exists
if [ ! -f "config.json" ]; then
  echo '{"webhookUrl":"","prices":{"inputPer1M":0,"outputPer1M":0},"bytesPerToken":3,"notifications":{"sound":true,"desktop":true}}' > config.json
  echo "[✓] Created default config.json"
fi

# 3. Create state backup
if [ ! -f "state.json" ]; then
  echo '{"keys":[],"activeKey":null,"dailyLog":{}}' > state.json
  echo "[✓] Created empty state.json"
fi

# 4. Install systemd service
if command -v systemctl &> /dev/null; then
  SERVICE_SRC="codex-proxy.service"
  SERVICE_DST="/etc/systemd/system/codex-proxy.service"
  if [ -f "$SERVICE_SRC" ]; then
    echo "[*] Installing systemd service..."
    sed "s|{{PROXY_DIR}}|$PROXY_DIR|g" "$SERVICE_SRC" > "$SERVICE_DST"
    systemctl daemon-reload
    systemctl enable codex-proxy
    systemctl restart codex-proxy
    echo "[✓] systemd service installed and started"
    echo "    Status: systemctl status codex-proxy"
    echo "    Logs:   journalctl -u codex-proxy -f"
  fi
else
  echo "[!] systemctl not found — skipping service installation"
  echo "    Start manually: node \"$PROXY_DIR/proxy.js\" &"
fi

# 5. Create wrapper script (optional: set WRAPPER_DIR and CODEX_BIN env vars to customize)
WRAPPER_DIR="${WRAPPER_DIR:-$HOME/bin}"
CODEX_BIN="${CODEX_BIN:-/usr/lib/node_modules/@openai/codex/bin/codex.js}"
mkdir -p "$WRAPPER_DIR"
cat > "$WRAPPER_DIR/codex" << WRAPPER
#!/bin/bash
# Auto-start proxy if not running, then exec real codex
PROXY_DIR="$PROXY_DIR"
if ! curl -sf http://localhost:3456/__status > /dev/null 2>&1; then
  echo "[codex] Starting proxy..." >&2
  nohup node "\$PROXY_DIR/proxy.js" > "\$PROXY_DIR/proxy.log" 2>&1 &
  sleep 2
fi
exec $CODEX_BIN "\$@"
WRAPPER
chmod +x "$WRAPPER_DIR/codex"
echo "[✓] Wrapper script created at $WRAPPER_DIR/codex"
echo "    Make sure $WRAPPER_DIR is in your PATH"

echo ""
echo "=== Installation complete ==="
echo "Dashboard: http://localhost:3456/"
echo ""
