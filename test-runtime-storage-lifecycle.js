#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawnSync } = require("child_process");
const vm = require("vm");

const ROOT = __dirname;

function loadStorageHarness(tempDir) {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  const cutoff = source.lastIndexOf("// Start servers");
  if (cutoff < 0) throw new Error("proxy startup marker not found");
  const sandbox = {
    require,
    console,
    Buffer,
    URL,
    __dirname: tempDir,
    __filename: path.join(tempDir, "proxy.js"),
    setInterval: () => 0,
    clearInterval: () => {},
    setTimeout: () => 0,
    clearTimeout: () => {},
    process: { pid: 999994, argv: [], env: {}, on: () => {}, exit: () => {} },
  };
  sandbox.globalThis = sandbox;
  const harness = `
    ;globalThis.__storageLifecycleTest = {
      setConfig: patch => {
        Object.assign(config, patch || {});
        lastStateCompactionAt = 0;
        stateBucketAddsSinceCompaction = 0;
      },
      setState: value => {
        state = value;
        lastStateCompactionAt = 0;
        stateBucketAddsSinceCompaction = 0;
        invalidateStateStatusBucketCache();
      },
      getState: () => state,
      compact: now => compactState(now, { force: true }),
      save: () => saveState(true),
      backup: () => backupState(),
      reload: () => loadState(),
      setAccounts: value => { accounts = value || []; },
      status: () => buildStatusData(),
      stateFile: STATE_FILE,
      backupFile: STATE_FILE + ".bak",
      resetSummary: () => { logMinuteBuckets.clear(); logDailySummaries.clear(); },
      seedSummary: (count, payloadSize) => {
        for (let i = 0; i < count; i++) {
          const aggregate = createLogAggregate();
          aggregate.events["synthetic_" + i] = "x".repeat(payloadSize);
          logMinuteBuckets.set(i, aggregate);
        }
      },
      summaryBytes: () => Buffer.byteLength(serializeBoundedLogSummary(), "utf8"),
      summaryMinuteCount: () => logMinuteBuckets.size,
    };
  `;
  vm.runInNewContext(source.slice(0, cutoff) + harness, sandbox, { filename: "proxy.js", timeout: 10000 });
  return sandbox.__storageLifecycleTest;
}

function hourKey(time) {
  return new Date(time).toISOString().slice(0, 13).replace("T", "-");
}

function dayKey(time) {
  return new Date(time).toISOString().slice(0, 10);
}

function makeBucket(modelCount = 1, padding = "") {
  const models = {};
  for (let i = 0; i < modelCount; i++) {
    models[`model-${i}`] = { requests: i + 1, inputBytes: i, outputBytes: i, padding };
  }
  return {
    requests: 1,
    inputBytes: 1,
    outputBytes: 1,
    totalCost: 0,
    totalDuration: 1,
    totalTtfb: 1,
    models,
  };
}

function testStateCompaction(harness) {
  const now = Date.UTC(2026, 6, 31, 12, 0, 0);
  const oldHour = hourKey(now - 40 * 86400000);
  const recentHour = hourKey(now - 2 * 3600000);
  const oldDay = dayKey(now - 200 * 86400000);
  const recentDay = dayKey(now - 2 * 86400000);
  const state = {
    keys: [{
      activatedAt: now - 100 * 86400000,
      status: "active",
      rateWindow: { requests: [{ time: now - 2 * 60000 }, { time: now - 2 * 3600000 }], windowStart: now - 2 * 3600000 },
      stats: {
        totalRequests: 2,
        successRequests: 2,
        failRequests: 0,
        inputBytes: 2,
        outputBytes: 2,
        totalDuration: 2,
        totalTtfb: 2,
        totalCost: 0,
        hourly: { [oldHour]: makeBucket(), [recentHour]: makeBucket(20) },
        daily: { [oldDay]: { requests: 1 }, [recentDay]: { requests: 1 } },
      },
    }],
    activeKey: 0,
    dailyLog: { obsolete: true },
    autoResume: { projects: { stale_project_1: { phase: "exited" } } },
  };
  harness.setConfig({
    rateLimit: false,
    stateHourlyRetentionDays: 31,
    stateDailyRetentionDays: 30,
    stateMaxMiB: 32,
    autoResume: false,
    autoResumeProjects: [],
  });
  harness.setState(state);
  assert.strictEqual(harness.compact(now), true);
  const result = harness.getState();
  assert.ok(!Object.prototype.hasOwnProperty.call(result, "dailyLog"), "legacy dailyLog must be removed");
  assert.ok(!Object.prototype.hasOwnProperty.call(result.keys[0], "rateWindow"), "rateWindow must be removed when rate limiting is disabled");
  assert.ok(!result.keys[0].stats.hourly[oldHour], "expired hourly bucket must be removed");
  assert.ok(result.keys[0].stats.hourly[recentHour], "recent hourly bucket must remain");
  assert.ok(!result.keys[0].stats.daily[oldDay], "expired daily bucket must be removed");
  assert.ok(result.keys[0].stats.daily[recentDay], "recent daily bucket must remain");
  assert.ok(Object.keys(result.keys[0].stats.hourly[recentHour].models).length <= 12, "hourly model dimensions must be bounded");
  assert.ok(!result.autoResume.projects.stale_project_1, "stale auto-resume project state must be removed");
}

function testStateByteBudgetAndAtomicRecovery(harness, tempDir) {
  const now = Date.UTC(2026, 6, 31, 12, 0, 0);
  const hourly = {};
  // Keep the fixture small while still forcing the 4 MiB budget path. The
  // production state contains bounded model dimensions, so a few oversized
  // buckets are enough to exercise oldest-bucket removal and recovery.
  const padding = "p".repeat(1100000);
  for (let i = 0; i < 5; i++) hourly[hourKey(now - i * 3600000)] = makeBucket(1, padding);
  harness.setConfig({
    rateLimit: true,
    stateHourlyRetentionDays: 31,
    stateDailyRetentionDays: 30,
    stateMaxMiB: 4,
    autoResume: false,
    autoResumeProjects: [],
  });
  harness.setState({
    keys: [{
      activatedAt: now - 3600000,
      status: "active",
      stats: {
        totalRequests: 230,
        successRequests: 230,
        failRequests: 0,
        inputBytes: 230,
        outputBytes: 230,
        totalDuration: 230,
        totalTtfb: 230,
        totalCost: 0,
        hourly,
        daily: {},
      },
    }],
    activeKey: 0,
  });
  assert.strictEqual(harness.save(), true);
  const stateBytes = fs.statSync(harness.stateFile).size;
  assert.ok(stateBytes <= 4 * 1024 * 1024, `state.json must stay within configured budget (got ${stateBytes})`);
  assert.ok(!fs.existsSync(`${harness.stateFile}.999994.tmp`), "temporary state file must not remain after atomic save");

  harness.backup();
  assert.ok(fs.existsSync(harness.backupFile), "state backup must be created");
  fs.writeFileSync(harness.stateFile, "{broken", "utf8");
  harness.reload();
  const restored = harness.getState();
  assert.ok(Array.isArray(restored.keys) && restored.keys.length === 1, "backup recovery must restore key state");
  assert.ok(fs.statSync(harness.stateFile).size <= 4 * 1024 * 1024, "recovered state must remain bounded");
  assert.ok(fs.existsSync(path.join(tempDir, "state.json.bak")), "backup file must remain available after recovery");
}

function testStatusPayloadUsesRecentBuckets(harness) {
  const now = Date.UTC(2026, 6, 31, 12, 0, 0);
  const hourly = {};
  const daily = {};
  for (let i = 0; i < 900; i++) hourly[hourKey(now - i * 3600000)] = makeBucket();
  for (let i = 0; i < 60; i++) daily[dayKey(now - i * 86400000)] = { requests: 1 };
  harness.setConfig({ rateLimit: false, stateHourlyRetentionDays: 365, stateDailyRetentionDays: 3650, autoResume: false });
  harness.setAccounts([{ key: "sk-test-status", url: "https://upstream.example", reset: "daily", status: "active", group: "A" }]);
  harness.setState({ keys: [{ activatedAt: now, status: "active", stats: { totalRequests: 1, successRequests: 1, failRequests: 0, inputBytes: 0, outputBytes: 0, totalDuration: 0, totalTtfb: 0, totalCost: 0, hourly, daily } }], activeKey: 0 });
  const status = harness.status();
  assert.strictEqual(status.length, 1);
  assert.ok(Object.keys(status[0].hourly).length <= 31 * 24 + 48, "status must not broadcast all hourly history");
  assert.ok(Object.keys(status[0].daily).length <= 35, "status must not broadcast all daily history");
}

function testLogSummaryBudget(harness) {
  harness.resetSummary();
  harness.seedSummary(48, 220000);
  const bytes = harness.summaryBytes();
  assert.ok(bytes <= 8 * 1024 * 1024, `log summary must stay within 8 MiB (got ${bytes})`);
  assert.ok(harness.summaryMinuteCount() < 48, "oldest minute rollups must be discarded at the summary cap");
}

function shellQuote(value) {
  return `'${String(value).replace(/'/g, "'\\''")}'`;
}

function runRotator(logFile, configFile, input) {
  // The watchdog connects the rotator through a shell process-substitution
  // pipe. Use the same shell-pipe shape here instead of a nested Node stdin
  // pipe, which is not delivered reliably inside the managed test sandbox.
  const inputFile = `${logFile}.input`;
  fs.writeFileSync(inputFile, input || Buffer.alloc(0), { mode: 0o600 });
  const command = [
    "exec",
    shellQuote(process.execPath),
    shellQuote(path.join(ROOT, "proxy-log-rotator.js")),
    shellQuote(logFile),
    shellQuote(configFile),
    "<",
    shellQuote(inputFile),
  ].join(" ");
  const result = spawnSync("bash", ["-c", command], {
    cwd: ROOT,
    encoding: "utf8",
    timeout: 30000,
    maxBuffer: 1024 * 1024,
  });
  try { fs.rmSync(inputFile, { force: true }); } catch {}
  if (result.error && ["EPERM", "EACCES"].includes(result.error.code)) return null;
  if (result.error || result.status !== 0) {
    const detail = String(result.stderr || result.error?.message || "").trim();
    throw new Error(`rotator exited with ${result.status == null ? "unknown" : result.status}: ${detail}`);
  }
  return String(result.stderr || "");
}

function testProxyLogRotatorSource() {
  const source = fs.readFileSync(path.join(ROOT, "proxy-log-rotator.js"), "utf8");
  assert.match(source, /process\.stdin\.on\("data", writeChunk\)/);
  assert.match(source, /process\.stdin\.on\("end", finish\)/);
  assert.match(source, /process\.stdin\.resume\(\)/);
  assert.match(source, /configTimer = setInterval\(/);
  assert.match(source, /if \(configTimer\) clearInterval\(configTimer\)/);
  assert.doesNotMatch(source, /configTimer\.unref\(\)/, "rotator must stay alive until stdin EOF");
  assert.match(source, /proxyLogMaxMiB/);
  assert.match(source, /proxyLogKeepFiles/);

  const watchdog = fs.readFileSync(path.join(ROOT, "watchdog.sh"), "utf8");
  assert.match(watchdog, /watchdog_log\(\)/);
  assert.match(watchdog, /proxy-log-rotator\.js is required; refusing unrotated proxy logging/);
  assert.doesNotMatch(watchdog, /node "\$PROXY_ABS" >> "\$LOG"/, "watchdog must not reintroduce an unbounded direct log fallback");
}

function readRotatorParts(logFile) {
  const directory = path.dirname(logFile);
  const base = path.basename(logFile);
  const names = fs.readdirSync(directory).filter(name => name === base || /^proxy\.log\.\d+$/.test(name));
  const archives = names.filter(name => name !== base).sort((left, right) => Number(right.slice(base.length + 1)) - Number(left.slice(base.length + 1)));
  return [...archives, base].map(name => fs.readFileSync(path.join(directory, name)));
}

async function testProxyLogRotation(tempDir) {
  const configFile = path.join(tempDir, "rotator-config.json");
  fs.writeFileSync(configFile, JSON.stringify({ proxyLogMaxMiB: 1, proxyLogKeepFiles: 3 }), "utf8");
  const logFile = path.join(tempDir, "proxy.log");
  const input = Buffer.alloc(2 * 1024 * 1024 + 32768);
  for (let i = 0; i < input.length; i++) input[i] = i % 251;
  const firstRun = runRotator(logFile, configFile, input);
  if (firstRun === null) {
    console.log("proxy log rotator subprocess: SKIPPED (nested process execution unavailable)");
    return;
  }
  const parts = readRotatorParts(logFile);
  assert.ok(parts.length <= 4, "rotator must retain the active file plus configured archives");
  assert.ok(parts.every(part => part.length <= 1024 * 1024), "each proxy.log segment must stay within its limit");
  assert.deepStrictEqual(Buffer.concat(parts), input, "rotation must not lose the tail when stdin ends during backpressure");

  const legacy = Buffer.alloc(3 * 1024 * 1024 + 131072, 7);
  fs.writeFileSync(logFile, legacy);
  await runRotator(logFile, configFile);
  const migratedParts = readRotatorParts(logFile);
  assert.strictEqual(migratedParts[migratedParts.length - 1].length, 0, "legacy migration must leave a fresh active log");
  assert.ok(migratedParts.slice(0, -1).every(part => part.length <= 1024 * 1024), "legacy archives must be bounded");
  assert.deepStrictEqual(Buffer.concat(migratedParts.slice(0, -1)), legacy.subarray(legacy.length - 3 * 1024 * 1024), "legacy migration must preserve the newest bounded tail");
}

async function main() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "codex-proxy-storage-test-"));
  try {
    const harness = loadStorageHarness(tempDir);
    testStateCompaction(harness);
    testStateByteBudgetAndAtomicRecovery(harness, tempDir);
    testStatusPayloadUsesRecentBuckets(harness);
    testLogSummaryBudget(harness);
    testProxyLogRotatorSource();
    await testProxyLogRotation(tempDir);
    console.log("runtime storage lifecycle: PASS");
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

main().catch(error => {
  console.error(`[runtime-storage-test] ${error.stack || error.message}`);
  process.exitCode = 1;
});
