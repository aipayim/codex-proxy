const { test, before, after } = require("node:test");
const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { WebSocket } = require("ws");

const PROXY = path.resolve(__dirname, "..", "proxy.js");
const TEST_PORT = 13456;
const TEST_TOKEN = "test-secret-token";

function writeTestFiles(dir, { adminToken = "" } = {}) {
  fs.writeFileSync(path.join(dir, "config.json"), JSON.stringify({
    groups: { A: TEST_PORT },
    adminToken,
    notifications: { desktop: false, sound: false },
    logFile: false,
    rateLimit: false,
  }));
  fs.writeFileSync(path.join(dir, "keys.json"), JSON.stringify([]));
}

function startProxy(dir) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [PROXY], {
      cwd: dir,
      env: {
        ...process.env,
        PROXY_CONFIG_FILE: path.join(dir, "config.json"),
        PROXY_KEYS_FILE: path.join(dir, "keys.json"),
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    let ready = false;
    const onData = (chunk) => {
      if (!ready && chunk.toString().includes(`localhost:${TEST_PORT}`)) {
        ready = true;
        child.stdout.removeListener("data", onData);
        resolve(child);
      }
    };
    child.stdout.on("data", onData);
    child.stderr.on("data", () => {});
    child.on("error", reject);
    // Fallback: give it 4s to start
    setTimeout(() => {
      if (!ready) { ready = true; resolve(child); }
    }, 4000);
  });
}

function stopProxy(child) {
  return new Promise((resolve) => {
    child.on("exit", resolve);
    child.kill("SIGTERM");
    setTimeout(() => { try { child.kill("SIGKILL"); } catch {} }, 2000);
  });
}

function wsConnect(url, { origin } = {}) {
  return new Promise((resolve) => {
    const headers = origin ? { origin } : {};
    const ws = new WebSocket(url, { headers });
    const result = { opened: false, closed: false, closeCode: null, gotStatus: false };
    ws.on("open", () => { result.opened = true; });
    ws.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === "status") result.gotStatus = true;
      } catch {}
    });
    ws.on("close", (code) => { result.closed = true; result.closeCode = code; resolve(result); });
    ws.on("error", () => { result.closed = true; resolve(result); });
    // Resolve after 1.5s if still open (connection accepted and stable)
    setTimeout(() => { if (!result.closed) { ws.close(); resolve(result); } }, 1500);
  });
}

// ── Suite A: no adminToken ────────────────────────────────────────────────────
test("no-auth suite", async (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "codex-proxy-test-"));
  writeTestFiles(dir, { adminToken: "" });
  const child = await startProxy(dir);

  await t.test("connects and receives status without token", async () => {
    const r = await wsConnect(`ws://127.0.0.1:${TEST_PORT}`);
    assert.ok(r.gotStatus, "expected status message");
  });

  await t.test("rejects a disallowed origin", async () => {
    const r = await wsConnect(`ws://127.0.0.1:${TEST_PORT}`, { origin: "http://evil.example.com" });
    assert.ok(!r.gotStatus, "should not receive status from disallowed origin");
  });

  await stopProxy(child);
  fs.rmSync(dir, { recursive: true, force: true });
});

// ── Suite B: adminToken configured ───────────────────────────────────────────
test("token-auth suite", async (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "codex-proxy-test-"));
  writeTestFiles(dir, { adminToken: TEST_TOKEN });
  const child = await startProxy(dir);

  await t.test("accepts correct token", async () => {
    const r = await wsConnect(`ws://127.0.0.1:${TEST_PORT}?token=${encodeURIComponent(TEST_TOKEN)}`);
    assert.ok(r.gotStatus, "expected status message with correct token");
  });

  await t.test("rejects missing token", async () => {
    const r = await wsConnect(`ws://127.0.0.1:${TEST_PORT}`);
    assert.ok(!r.gotStatus, "should not receive status without token");
  });

  await t.test("rejects wrong token", async () => {
    const r = await wsConnect(`ws://127.0.0.1:${TEST_PORT}?token=wrongtoken`);
    assert.ok(!r.gotStatus, "should not receive status with wrong token");
  });

  await t.test("rejects disallowed origin even with correct token", async () => {
    const r = await wsConnect(
      `ws://127.0.0.1:${TEST_PORT}?token=${encodeURIComponent(TEST_TOKEN)}`,
      { origin: "http://evil.example.com" }
    );
    assert.ok(!r.gotStatus, "should not receive status from disallowed origin");
  });

  await stopProxy(child);
  fs.rmSync(dir, { recursive: true, force: true });
});
