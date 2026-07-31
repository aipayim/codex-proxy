#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawn, spawnSync } = require("child_process");

const ROOT = __dirname;
const HELPER = path.join(ROOT, "codex-sqlite-log-maintainer.py");

function runPython(args, options = {}) {
  const result = spawnSync("python3", args, {
    cwd: ROOT,
    encoding: "utf8",
    timeout: options.timeout || 30000,
    env: options.env || process.env,
    maxBuffer: 1024 * 1024,
  });
  if (result.error) throw result.error;
  return result;
}

function runHelper(homeDir, args) {
  const env = { ...process.env, HOME: homeDir };
  const result = runPython([HELPER, ...args], { env });
  let payload;
  try {
    payload = JSON.parse(String(result.stdout || "").trim());
  } catch (error) {
    throw new Error(`helper did not return JSON: ${String(result.stdout || result.stderr || error.message).slice(0, 240)}`);
  }
  return { status: result.status, payload };
}

function makeFixture(homeDir) {
  const codexDir = path.join(homeDir, ".codex");
  fs.mkdirSync(codexDir, { recursive: true, mode: 0o700 });
  const databasePath = path.join(codexDir, "logs_2.sqlite");
  const wrongSchemaPath = path.join(codexDir, "logs_wrong.sqlite");
  const source = [
    "import sqlite3, sys, time",
    "db, wrong = sys.argv[1], sys.argv[2]",
    "now = int(time.time())",
    "con = sqlite3.connect(db)",
    "con.execute('CREATE TABLE logs (id INTEGER PRIMARY KEY, ts INTEGER NOT NULL, ts_nanos INTEGER NOT NULL, payload TEXT)')",
    "con.execute('CREATE INDEX idx_logs_ts ON logs(ts)')",
    "con.execute('CREATE TABLE padding (payload BLOB)')",
    "con.execute('INSERT INTO padding(payload) VALUES (zeroblob(1300000))')",
    "for i in range(3): con.execute('INSERT INTO logs(ts, ts_nanos, payload) VALUES (?, ?, ?)', (now - 7200 - i, 0, 'old'))",
    "for i in range(2): con.execute('INSERT INTO logs(ts, ts_nanos, payload) VALUES (?, ?, ?)', (now - 60 - i, 0, 'recent'))",
    "con.commit()",
    "con.close()",
    "bad = sqlite3.connect(wrong)",
    "bad.execute('CREATE TABLE logs (id INTEGER PRIMARY KEY, created_at TEXT)')",
    "bad.commit()",
    "bad.close()",
  ].join("\n");
  const result = runPython(["-c", source, databasePath, wrongSchemaPath]);
  if (result.status !== 0) throw new Error(String(result.stderr || "fixture setup failed"));
  return { databasePath, wrongSchemaPath };
}

function countRows(databasePath) {
  const source = [
    "import sqlite3, sys, time",
    "con = sqlite3.connect(sys.argv[1])",
    "cutoff = int(time.time()) - 3600",
    "print(con.execute('SELECT COUNT(*) FROM logs WHERE ts < ?', (cutoff,)).fetchone()[0])",
    "print(con.execute('SELECT COUNT(*) FROM logs WHERE ts >= ?', (cutoff,)).fetchone()[0])",
    "con.close()",
  ].join("\n");
  const result = runPython(["-c", source, databasePath]);
  if (result.status !== 0) throw new Error(String(result.stderr || "row count failed"));
  return String(result.stdout).trim().split(/\s+/).map(Number);
}

function waitForLock(holder) {
  return new Promise((resolve, reject) => {
    let stderr = "";
    const timeout = setTimeout(() => reject(new Error(`lock holder did not become ready: ${stderr}`)), 5000);
    holder.stdout.once("data", chunk => {
      clearTimeout(timeout);
      if (String(chunk).trim() === "ready") resolve();
      else reject(new Error(`unexpected lock holder output: ${String(chunk)}`));
    });
    holder.stderr.on("data", chunk => { stderr += String(chunk); });
    holder.once("error", error => {
      clearTimeout(timeout);
      reject(error);
    });
    holder.once("exit", code => {
      if (code !== 0) {
        clearTimeout(timeout);
        reject(new Error(`lock holder exited early (${code}): ${stderr}`));
      }
    });
  });
}

async function testBusyDatabase(homeDir, databasePath) {
  const holderSource = [
    "import sqlite3, sys, time",
    "con = sqlite3.connect(sys.argv[1], timeout=1)",
    "con.execute('BEGIN IMMEDIATE')",
    "print('ready', flush=True)",
    "time.sleep(4)",
    "con.rollback()",
    "con.close()",
  ].join("\n");
  const holder = spawn("python3", ["-c", holderSource, databasePath], {
    cwd: ROOT,
    env: { ...process.env, HOME: homeDir },
    stdio: ["ignore", "pipe", "pipe"],
  });
  await waitForLock(holder);
  try {
    const busy = runHelper(homeDir, [
      "cleanup", "--path", databasePath, "--threshold-mib", "1", "--retain-hours", "1",
      "--batch-rows", "2", "--max-batches", "2", "--busy-timeout-ms", "1000",
    ]);
    assert.strictEqual(busy.status, 0, "a busy database is a cooperative skip, not a helper crash");
    assert.strictEqual(busy.payload.ok, true);
    assert.strictEqual(busy.payload.result, "skipped_busy");
  } finally {
    try { holder.kill("SIGTERM"); } catch {}
  }
}

async function testBusySchemaCheck(homeDir, databasePath) {
  const holderSource = [
    "import sqlite3, sys, time",
    "con = sqlite3.connect(sys.argv[1], timeout=1)",
    "con.execute('BEGIN EXCLUSIVE')",
    "print('ready', flush=True)",
    "time.sleep(4)",
    "con.rollback()",
    "con.close()",
  ].join("\n");
  const holder = spawn("python3", ["-c", holderSource, databasePath], {
    cwd: ROOT,
    env: { ...process.env, HOME: homeDir },
    stdio: ["ignore", "pipe", "pipe"],
  });
  await waitForLock(holder);
  try {
    const check = runHelper(homeDir, ["check", "--path", databasePath, "--busy-timeout-ms", "100"]);
    assert.strictEqual(check.status, 2, "a busy schema check must not claim the database is valid");
    assert.strictEqual(check.payload.ok, false);
    assert.strictEqual(check.payload.errorCode, "database_busy");

    const cleanup = runHelper(homeDir, [
      "cleanup", "--path", databasePath, "--threshold-mib", "1", "--retain-hours", "1",
      "--batch-rows", "2", "--max-batches", "2", "--busy-timeout-ms", "100",
    ]);
    assert.strictEqual(cleanup.status, 0, "a busy schema check during cleanup must be a cooperative skip");
    assert.strictEqual(cleanup.payload.ok, true);
    assert.strictEqual(cleanup.payload.result, "skipped_busy");
  } finally {
    try { holder.kill("SIGTERM"); } catch {}
  }
}

function testSourceContracts() {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  const fallback = fs.readFileSync(path.join(ROOT, "dashboard.html"), "utf8");
  const helper = fs.readFileSync(HELPER, "utf8");
  const pathStart = source.indexOf("function normalizeCodexLogMaintenancePath(");
  const pathEnd = source.indexOf("\nfunction normalizeCodexLogMaintenanceConfig", pathStart);
  assert.ok(pathStart >= 0 && pathEnd > pathStart, "server-side WSL path normalizer must be present");
  const normalizePath = new Function(source.slice(pathStart, pathEnd) + "; return normalizeCodexLogMaintenancePath;")();
  assert.strictEqual(normalizePath("\\\\wsl.localhost\\Ubuntu\\root\\.codex\\logs_2.sqlite"), "/root/.codex/logs_2.sqlite");
  assert.strictEqual(normalizePath("\\\\wsl$\\Ubuntu\\root\\.codex\\logs_2.sqlite"), "/root/.codex/logs_2.sqlite");
  assert.strictEqual(normalizePath("E:\\codex-proxy\\logs_2.sqlite"), "E:\\codex-proxy\\logs_2.sqlite");
  assert.match(source, /const CODEX_SQLITE_LOG_MAINTAINER_FILE = path\.join\(__dirname, "codex-sqlite-log-maintainer\.py"\);/);
  assert.match(source, /spawn\("python3", args, \{ stdio: \["ignore", "pipe", "pipe"\], windowsHide: true \}\)/);
  assert.match(source, /pathname === "\/__codex-log-maintenance\/check"/);
  assert.match(source, /pathname === "\/__codex-log-maintenance\/run"/);
  assert.match(source, /await invokeCodexLogMaintainer\("check", cur\.codexLogMaintenance\)/);
  assert.match(source, /codexLogMaintenanceRuntime: getCodexLogMaintenanceRuntimeStatus\(\)/);
  assert.match(source, /id="cfgCodexLogMaintenancePath"/);
  assert.match(source, /function normalizeCodexLogMaintenancePathInput\(\)/);
  assert.match(source, /function checkCodexLogMaintenancePath\(\)/);
  assert.match(source, /function runCodexLogMaintenanceNow\(\)/);
  assert.match(fallback, /id="cfgCodexLogMaintenancePath"/);
  assert.match(fallback, /function normalizeCodexLogMaintenancePathInput\(\)/);
  assert.match(fallback, /function checkCodexLogMaintenancePath\(\)/);
  assert.match(helper, /database_busy/);
  assert.doesNotMatch(helper, /VACUUM/i);
}

async function main() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "codex-proxy-sqlite-maintenance-test-"));
  try {
    const homeDir = path.join(tempDir, "home");
    const { databasePath, wrongSchemaPath } = makeFixture(homeDir);

    const valid = runHelper(homeDir, ["check", "--path", databasePath]);
    assert.strictEqual(valid.status, 0);
    assert.strictEqual(valid.payload.ok, true);
    assert.strictEqual(valid.payload.result, "valid");
    assert.ok(valid.payload.totalBytes >= 1024 * 1024, "fixture must reach the one-MiB cleanup threshold");

    const missing = runHelper(homeDir, ["check", "--path", path.join(homeDir, ".codex", "logs_missing.sqlite")]);
    assert.strictEqual(missing.status, 2);
    assert.strictEqual(missing.payload.ok, false);
    assert.strictEqual(missing.payload.errorCode, "not_found");

    const outside = runHelper(homeDir, ["check", "--path", path.join(tempDir, "logs_outside.sqlite")]);
    assert.strictEqual(outside.status, 2);
    assert.strictEqual(outside.payload.errorCode, "invalid_path");

    const wrongSchema = runHelper(homeDir, ["check", "--path", wrongSchemaPath]);
    assert.strictEqual(wrongSchema.status, 2);
    assert.strictEqual(wrongSchema.payload.errorCode, "invalid_schema");

    const belowThreshold = runHelper(homeDir, [
      "cleanup", "--path", databasePath, "--threshold-mib", "102400", "--retain-hours", "1",
    ]);
    assert.strictEqual(belowThreshold.status, 0);
    assert.strictEqual(belowThreshold.payload.result, "below_threshold");
    assert.deepStrictEqual(countRows(databasePath), [3, 2], "a below-threshold run must not delete any log rows");

    const cleaned = runHelper(homeDir, [
      "cleanup", "--path", databasePath, "--threshold-mib", "1", "--retain-hours", "1",
      "--batch-rows", "2", "--max-batches", "3", "--busy-timeout-ms", "1000",
    ]);
    assert.strictEqual(cleaned.status, 0);
    assert.strictEqual(cleaned.payload.ok, true);
    assert.strictEqual(cleaned.payload.result, "cleaned");
    assert.strictEqual(cleaned.payload.deletedRows, 3);
    assert.deepStrictEqual(countRows(databasePath), [0, 2], "cleanup must remove only rows outside the retention window");

    await testBusyDatabase(homeDir, databasePath);
    await testBusySchemaCheck(homeDir, databasePath);
    testSourceContracts();
    console.log("Codex SQLite log maintenance: PASS");
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

main().catch(error => {
  console.error(error && error.stack || error);
  process.exitCode = 1;
});
