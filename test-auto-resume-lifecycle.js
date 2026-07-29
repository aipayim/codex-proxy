#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

function loadAutoResumeHarness(proxyDir) {
  const source = fs.readFileSync(path.join(proxyDir, "proxy.js"), "utf8");
  const cutoff = source.lastIndexOf("// Start servers");
  if (cutoff < 0) throw new Error("proxy startup marker not found");

  const memoryFiles = new Map();
  const fsMock = {
    ...fs,
    readFileSync(file, encoding) {
      if (memoryFiles.has(file)) return memoryFiles.get(file);
      return fs.readFileSync(file, encoding);
    },
    writeFileSync(file, data) {
      memoryFiles.set(file, String(data));
    },
  };
  const sandbox = {
    require: moduleName => moduleName === "fs" ? fsMock : require(moduleName),
    console,
    Buffer,
    URL,
    __dirname: proxyDir,
    __filename: path.join(proxyDir, "proxy.js"),
    setInterval: () => 0,
    clearInterval: () => {},
    setTimeout: () => 0,
    clearTimeout: () => {},
    process: { pid: 999997, argv: [], env: {}, on: () => {}, exit: () => {} },
  };
  sandbox.globalThis = sandbox;
  const harness = `
    ;LOG_FILE_ENABLED = false;
    let autoResumeTestLaunches = [];
    triggerResume = (project, index) => {
      autoResumeTestLaunches.push({ name: project.name, index });
    };
    globalThis.__autoResumeLifecycleTest = {
      configure: (idleMinutes = 30, debounceMinutes = 10) => {
        config.autoResume = true;
        config.autoResumeIdleMinutes = idleMinutes;
        config.autoResumeDebounceMinutes = debounceMinutes;
        config.autoResumeProjects = [{ name: "test", path: "/tmp", cmd: "noop" }];
      },
      reset: (keyUseTime, resumeTime = 0) => {
        state = { keys: [], activeKey: null, dailyLog: {} };
        autoResumeStateReady = true;
        lastKeyUseTime = keyUseTime;
        lastResumeTime = resumeTime;
        lastRequestTime = keyUseTime;
        autoResumeTestLaunches = [];
      },
      setLastRequestTime: value => { lastRequestTime = value; },
      checkAt: value => checkAutoResume(value),
      recordKeyUse,
      snapshot: () => ({
        lastKeyUseTime,
        lastRequestTime,
        lastResumeTime,
        launches: autoResumeTestLaunches.slice(),
        runtimeFile: AUTO_RESUME_RUNTIME_FILE,
      }),
      runtimeFileContents: () => memoryFiles.get(AUTO_RESUME_RUNTIME_FILE) || "",
      restoreFromRuntime: () => {
        config.autoResume = false;
        state = { keys: [], activeKey: null, dailyLog: {} };
        autoResumeStateReady = false;
        lastKeyUseTime = 0;
        lastResumeTime = 0;
        restoreAutoResumeRuntimeState();
        return { lastKeyUseTime, lastResumeTime };
      },
    };
  `;
  sandbox.memoryFiles = memoryFiles;
  vm.runInNewContext(source.slice(0, cutoff) + harness, sandbox, { filename: "proxy.js", timeout: 5000 });
  return sandbox.__autoResumeLifecycleTest;
}

function testUsesActualKeyApplicationTime(harness) {
  const keyUseAt = 1700000000000;
  harness.configure(30, 10);
  harness.reset(keyUseAt);

  // A recent inbound request must not postpone recovery when no Key was applied.
  harness.setLastRequestTime(keyUseAt + 29 * 60000);
  harness.checkAt(keyUseAt + 30 * 60000);
  let snapshot = harness.snapshot();
  assert.strictEqual(snapshot.launches.length, 1);
  assert.strictEqual(snapshot.lastKeyUseTime, keyUseAt);
  assert.strictEqual(snapshot.lastRequestTime, keyUseAt + 29 * 60000);

  const nextKeyUseAt = keyUseAt + 31 * 60000;
  harness.recordKeyUse(2, nextKeyUseAt);
  snapshot = harness.snapshot();
  assert.strictEqual(snapshot.lastKeyUseTime, nextKeyUseAt);
  const persisted = JSON.parse(harness.runtimeFileContents());
  assert.strictEqual(persisted.lastKeyUseTime, nextKeyUseAt);

  harness.checkAt(nextKeyUseAt + 29 * 60000);
  assert.strictEqual(harness.snapshot().launches.length, 1);
  harness.checkAt(nextKeyUseAt + 30 * 60000);
  assert.strictEqual(harness.snapshot().launches.length, 2);
}

function testRestoresDurableHeartbeat(harness) {
  const keyUseAt = 1700001000000;
  harness.configure(30, 10);
  harness.reset(keyUseAt);
  harness.recordKeyUse(4, keyUseAt);

  const restored = harness.restoreFromRuntime();
  assert.strictEqual(restored.lastKeyUseTime, keyUseAt);
}

function testKeyUseIsRecordedAtUpstreamFlush(proxyDir) {
  const source = fs.readFileSync(path.join(proxyDir, "proxy.js"), "utf8");
  assert.match(source, /proxyReq\.once\("finish", \(\) => recordKeyUse\(idx\)\);/);
  const checkStart = source.indexOf("function checkAutoResume(");
  const checkEnd = source.indexOf("function triggerResume(", checkStart);
  assert.ok(checkStart >= 0 && checkEnd > checkStart, "auto-resume check function not found");
  assert.doesNotMatch(source.slice(checkStart, checkEnd), /lastRequestTime/);
}

function main() {
  const harness = loadAutoResumeHarness(__dirname);
  testUsesActualKeyApplicationTime(harness);
  testRestoresDurableHeartbeat(harness);
  testKeyUseIsRecordedAtUpstreamFlush(__dirname);
  console.log("auto-resume lifecycle: PASS");
}

try {
  main();
} catch (error) {
  console.error(`[auto-resume-lifecycle-test] ${error.stack || error.message}`);
  process.exitCode = 1;
}
