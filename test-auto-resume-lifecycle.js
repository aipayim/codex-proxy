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
    process: { pid: 999997, argv: [], env: {}, on: () => {}, exit: () => {}, kill: () => true },
  };
  sandbox.globalThis = sandbox;
  const harness = `
    ;LOG_FILE_ENABLED = false;
    saveState = () => {};
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
      setActiveRequests: count => {
        Object.keys(activeRequests).forEach(key => { delete activeRequests[key]; });
        if (count > 0) activeRequests[0] = Array.from({ length: count }, () => ({ start: Date.now(), model: "test" }));
      },
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
      setMemoryFile: (file, value) => memoryFiles.set(file, String(value)),
      readLease: readAutoResumePid,
      readStatus: readAutoResumeRunnerStatus,
      isOwned: isOwnedAutoResumeProcess,
      projectFiles: autoResumeProjectFiles,
      buildCommand: buildAutoResumeCommand,
      commandWarning: getAutoResumeCommandWarning,
      normalizeProjects: normalizeAutoResumeProjects,
      setProjectCommand: command => { config.autoResumeProjects[0].cmd = command; },
      refreshStatus: refreshAutoResumeProjectStatus,
      setProjectState: (project, index, projectState) => {
        const runtime = getAutoResumeRuntimeState();
        runtime.projects[autoResumeProjectId(project, index)] = { ...projectState };
      },
      getProjectState: (project, index) => {
        const runtime = getAutoResumeRuntimeState();
        return { ...(runtime.projects[autoResumeProjectId(project, index)] || {}) };
      },
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

  // A failed/returned runner must not cause an unbounded replay while the
  // same Key-idle episode remains in effect.
  harness.checkAt(keyUseAt + 90 * 60000);
  assert.strictEqual(harness.snapshot().launches.length, 1);

  // Correcting the configured command starts a fresh, explicitly requested
  // recovery attempt without needing to manufacture a new upstream request.
  harness.setProjectCommand("noop-fixed");
  harness.checkAt(keyUseAt + 91 * 60000);
  assert.strictEqual(harness.snapshot().launches.length, 2);

  const nextKeyUseAt = keyUseAt + 31 * 60000;
  harness.recordKeyUse(2, nextKeyUseAt);
  snapshot = harness.snapshot();
  assert.strictEqual(snapshot.lastKeyUseTime, nextKeyUseAt);
  const persisted = JSON.parse(harness.runtimeFileContents());
  assert.strictEqual(persisted.lastKeyUseTime, nextKeyUseAt);

  harness.checkAt(nextKeyUseAt + 29 * 60000);
  assert.strictEqual(harness.snapshot().launches.length, 2);
  harness.checkAt(nextKeyUseAt + 30 * 60000);
  assert.strictEqual(harness.snapshot().launches.length, 3);
}

function testWaitsForInFlightRequest(harness) {
  const keyUseAt = 1700002000000;
  harness.configure(30, 10);
  harness.reset(keyUseAt);
  harness.setActiveRequests(1);
  harness.checkAt(keyUseAt + 30 * 60000);
  assert.strictEqual(harness.snapshot().launches.length, 0, "active request must defer recovery");
  harness.setActiveRequests(0);
  harness.checkAt(keyUseAt + 31 * 60000);
  assert.strictEqual(harness.snapshot().launches.length, 1, "recovery must proceed after the request finishes");
}

function procStat(pid, startTicks) {
  const fields = Array(20).fill("0");
  fields[0] = "S";
  fields[19] = String(startTicks);
  return `${pid} (script) ${fields.join(" ")}`;
}

function testLeaseIdentityAndStatusCompatibility(harness) {
  const project = { name: "test", path: "/tmp", cmd: "true" };
  const files = harness.projectFiles(project, 0);
  const runId = "019f825b-fd3b-70d0-8edc-bd19b593586b";
  harness.setMemoryFile(files.pidFile, JSON.stringify({ schema: 1, runId, pid: 4242, pgid: 4242, createdAt: 1700000000000, processStartTicks: 998877 }));
  harness.setMemoryFile("/proc/4242/stat", procStat(4242, 998877));
  const lease = harness.readLease(files.pidFile);
  assert.strictEqual(lease.runId, runId);
  assert.strictEqual(lease.processStartTicks, 998877);
  assert.strictEqual(harness.isOwned(lease), true, "matching PID start ticks prove lease ownership");

  harness.setMemoryFile("/proc/4242/stat", procStat(4242, 998878));
  assert.strictEqual(harness.isOwned(lease), false, "PID reuse must not be treated as an owned runner");

  harness.setMemoryFile(files.statusFile, JSON.stringify({
    schema: 2, phase: "terminated", pid: 4242, updatedAt: 1700000001000,
    exitCode: 143, runId, startedAt: 1700000000000, signal: "TERM",
    origin: "runner_signal", detail: "runner received TERM",
  }));
  const status = harness.readStatus(files.statusFile);
  assert.deepStrictEqual({ phase: status.phase, exitCode: status.exitCode, signal: status.signal, origin: status.origin }, {
    phase: "terminated", exitCode: 143, signal: "TERM", origin: "runner_signal",
  });

  harness.setMemoryFile(files.statusFile, "failed\t42\t1700000002000\t2\tlegacy failure");
  const legacy = harness.readStatus(files.statusFile);
  assert.strictEqual(legacy.phase, "failed");
  assert.strictEqual(legacy.origin, "legacy");
  assert.strictEqual(legacy.detail, "legacy failure");
}

function testFixedSessionTemplate(harness) {
  const sessionId = "019f825b-fd3b-70d0-8edc-bd19b593586b";
  const command = harness.buildCommand({
    cmd: "codex resume {sessionId} 'continue'",
    resumeMode: "fixed_session",
    sessionId,
  });
  assert.ok(command.includes(`'${sessionId}'`), "fixed session ID must be shell-quoted into the command template");
  const normalized = harness.normalizeProjects([{ name: "p", path: "D:\\work", cmd: "codex resume {sessionId}", resumeMode: "fixed_session", sessionId }]);
  assert.deepStrictEqual(JSON.parse(JSON.stringify(normalized[0])), { name: "p", path: "D:\\work", cmd: "codex resume {sessionId}", resumeMode: "fixed_session", sessionId });

  const commandMode = { cmd: "codex resume --last {sessionId}", resumeMode: "command", sessionId };
  assert.strictEqual(harness.buildCommand(commandMode), commandMode.cmd, "command mode must not expand a retained session placeholder");
  assert.match(harness.commandWarning(commandMode, commandMode.cmd), /resume --last/, "command mode must retain the ambiguous-session warning");
  assert.strictEqual(harness.commandWarning({ cmd: "codex resume --last {sessionId}", resumeMode: "fixed_session", sessionId }, `codex resume --last '${sessionId}'`), "");
}

function testRejectsUnidentifiedLegacyStatusAfterLaunch(harness) {
  const project = { name: "test", path: "/tmp", cmd: "true" };
  const files = harness.projectFiles(project, 0);
  const runId = "019f825b-fd3b-70d0-8edc-bd19b593586b";
  harness.setProjectState(project, 0, { phase: "launching", runnerRunId: runId, runnerUpdatedAt: 1700000000000 });

  harness.setMemoryFile(files.statusFile, "failed\t4242\t1700000001000\t2\tlegacy failure");
  harness.refreshStatus(project, 0);
  assert.strictEqual(harness.getProjectState(project, 0).phase, "launching", "legacy status must not overwrite an identified launch");

  harness.setMemoryFile(files.statusFile, JSON.stringify({
    schema: 2, phase: "exited", pid: 4242, updatedAt: 1700000002000,
    exitCode: 0, runId, startedAt: 1700000000000, signal: "", origin: "command", detail: "command exited normally",
  }));
  harness.refreshStatus(project, 0);
  const updated = harness.getProjectState(project, 0);
  assert.strictEqual(updated.phase, "exited", "matching run id must update the launch state");
  assert.strictEqual(updated.runnerRunId, runId);
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
  assert.doesNotMatch(source, /cleanupStaleCodexProcesses/);
  assert.doesNotMatch(source, /collectCodexDescendantPids/);
  assert.match(source, /One attempt per continuous idle episode/);
  assert.match(source, /processStartTicks/);
  assert.match(source, /projectFingerprint/);
  assert.match(source, /auto_resume_launcher_returned/);
}

function main() {
  const harness = loadAutoResumeHarness(__dirname);
  testUsesActualKeyApplicationTime(harness);
  testWaitsForInFlightRequest(harness);
  testRestoresDurableHeartbeat(harness);
  testLeaseIdentityAndStatusCompatibility(harness);
  testFixedSessionTemplate(harness);
  testRejectsUnidentifiedLegacyStatusAfterLaunch(harness);
  testKeyUseIsRecordedAtUpstreamFlush(__dirname);
  console.log("auto-resume lifecycle: PASS");
}

try {
  main();
} catch (error) {
  console.error(`[auto-resume-lifecycle-test] ${error.stack || error.message}`);
  process.exitCode = 1;
}
