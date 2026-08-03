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
  const signals = [];
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
  const processMock = {
    pid: 999997,
    argv: [],
    env: {},
    on: () => {},
    exit: () => {},
    kill(target, signal = 0) {
      if (signal === 0 || signal === undefined) {
        if (target > 1 && memoryFiles.has(`/proc/${target}/stat`)) return true;
        if (target < -1) {
          const pgid = Math.abs(target);
          for (const [file, value] of memoryFiles) {
            const match = /^\/proc\/(\d+)\/stat$/.exec(file);
            if (!match) continue;
            const end = String(value).lastIndexOf(") ");
            if (end < 0) continue;
            const fields = String(value).slice(end + 2).trim().split(/\s+/);
            if (Number(fields[2]) === pgid) return true;
          }
        }
        const error = new Error("no such process");
        error.code = "ESRCH";
        throw error;
      }
      signals.push({ target, signal });
      return true;
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
    process: processMock,
    autoResumeLifecycleSignals: signals,
  };
  sandbox.globalThis = sandbox;
  const harness = `
    ;LOG_FILE_ENABLED = false;
    saveState = () => {};
    let autoResumeTestLaunches = [];
    let autoResumeTestEvents = [];
    triggerResume = (project, index) => {
      autoResumeTestLaunches.push({ name: project.name, index });
    };
    addEventLog = (type, _keyIndex, message) => {
      autoResumeTestEvents.push({ type, message });
    };
    globalThis.__autoResumeLifecycleTest = {
      configure: (idleMinutes = 30, debounceMinutes = 10, stallMinutes = 20, maxStallRestarts = 1) => {
        config.autoResume = true;
        config.autoResumeIdleMinutes = idleMinutes;
        config.autoResumeDebounceMinutes = debounceMinutes;
        config.autoResumeRunnerStallMinutes = stallMinutes;
        config.autoResumeRunnerMaxStallRestarts = maxStallRestarts;
        config.autoResumeProjects = [{ name: "test", path: "/tmp", cmd: "noop" }];
      },
      reset: (keyUseTime, resumeTime = 0) => {
        memoryFiles.clear();
        state = { keys: [], activeKey: null, dailyLog: {} };
        autoResumeStateReady = true;
        lastKeyUseTime = keyUseTime;
        lastResumeTime = resumeTime;
        lastRequestTime = keyUseTime;
        autoResumeTestLaunches = [];
        autoResumeTestEvents = [];
        autoResumeLifecycleSignals.length = 0;
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
        events: autoResumeTestEvents.slice(),
        signals: autoResumeLifecycleSignals.slice(),
        runtimeFile: AUTO_RESUME_RUNTIME_FILE,
      }),
      runtimeFileContents: () => memoryFiles.get(AUTO_RESUME_RUNTIME_FILE) || "",
      setMemoryFile: (file, value) => memoryFiles.set(file, String(value)),
      removeMemoryFile: file => memoryFiles.delete(file),
      readLease: readAutoResumePid,
      readStatus: readAutoResumeRunnerStatus,
      isOwned: isOwnedAutoResumeProcess,
      projectFiles: autoResumeProjectFiles,
      buildCommand: buildAutoResumeCommand,
      commandWarning: getAutoResumeCommandWarning,
      normalizeProjects: normalizeAutoResumeProjects,
      normalizeAutoResumeConfig: value => normalizeAutoResumeConfig({ ...(value || {}) }),
      setProjectCommand: command => { config.autoResumeProjects[0].cmd = command; },
      project: () => config.autoResumeProjects[0],
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

function testStallConfigNormalization(harness) {
  const defaults = harness.normalizeAutoResumeConfig({});
  assert.strictEqual(defaults.autoResumeRunnerStallMinutes, 20);
  assert.strictEqual(defaults.autoResumeRunnerMaxStallRestarts, 1);

  const disabled = harness.normalizeAutoResumeConfig({ autoResumeRunnerStallMinutes: 0, autoResumeRunnerMaxStallRestarts: 0 });
  assert.strictEqual(disabled.autoResumeRunnerStallMinutes, 0, "zero must disable stall recovery");
  assert.strictEqual(disabled.autoResumeRunnerMaxStallRestarts, 0, "zero must disable stall restarts");

  const clamped = harness.normalizeAutoResumeConfig({ autoResumeRunnerStallMinutes: 99999, autoResumeRunnerMaxStallRestarts: 99 });
  assert.strictEqual(clamped.autoResumeRunnerStallMinutes, 1440);
  assert.strictEqual(clamped.autoResumeRunnerMaxStallRestarts, 3);
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

function procStat(pid, startTicks, pgid = pid) {
  const fields = Array(20).fill("0");
  fields[0] = "S";
  fields[2] = String(pgid);
  fields[19] = String(startTicks);
  return `${pid} (script) ${fields.join(" ")}`;
}

function seedManagedRunner(harness, options = {}) {
  const keyUseAt = options.keyUseAt || 1700010000000;
  const idleMinutes = options.idleMinutes || 30;
  const stallMinutes = options.stallMinutes || 20;
  const maxStallRestarts = options.maxStallRestarts === undefined ? 1 : options.maxStallRestarts;
  const pid = options.pid || 4242;
  const pgid = options.pgid || pid;
  const startTicks = options.startTicks || 998877;
  const runId = options.runId || "runner-stall-1";
  const runnerStartedAt = options.runnerStartedAt || keyUseAt + idleMinutes * 60000;
  harness.configure(idleMinutes, 10, stallMinutes, maxStallRestarts);
  harness.reset(keyUseAt);
  const project = harness.project();
  const files = harness.projectFiles(project, 0);
  harness.setProjectState(project, 0, {
    idleEpochKeyUseTime: keyUseAt,
    attemptCount: 1,
    lastAttemptAt: runnerStartedAt,
    lastAttemptOutcome: "running",
    phase: "running",
    runnerRunId: runId,
    runnerStartedAt,
    launchRequestedAt: runnerStartedAt,
    runnerPid: pid,
    processStartTicks: startTicks,
    stallRestartCount: options.stallRestartCount || 0,
    stallPhase: options.stallPhase || "",
  });
  if (options.lease !== false) {
    harness.setMemoryFile(files.pidFile, JSON.stringify({
      schema: 1,
      runId: options.leaseRunId || runId,
      pid,
      pgid,
      createdAt: runnerStartedAt,
      processStartTicks: options.leaseStartTicks || startTicks,
    }));
  }
  if (options.proc !== false) {
    harness.setMemoryFile(`/proc/${pid}/stat`, procStat(pid, options.procStartTicks || startTicks, options.procPgid || pgid));
  }
  return { keyUseAt, idleMinutes, stallMinutes, pid, pgid, startTicks, runId, runnerStartedAt, project, files };
}

function testManagedRunnerStallSignalsOnlyVerifiedGroup(harness) {
  const runner = seedManagedRunner(harness);
  harness.checkAt(runner.runnerStartedAt + (runner.stallMinutes - 1) * 60000);
  assert.deepStrictEqual(harness.snapshot().signals, [], "runner must receive a full stall grace period");

  harness.setActiveRequests(1);
  harness.checkAt(runner.runnerStartedAt + runner.stallMinutes * 60000);
  assert.deepStrictEqual(harness.snapshot().signals, [], "an active proxy request must defer stall recovery");

  harness.setActiveRequests(0);
  const stalledAt = runner.runnerStartedAt + runner.stallMinutes * 60000;
  harness.checkAt(stalledAt);
  let snapshot = harness.snapshot();
  assert.deepStrictEqual(snapshot.signals, [{ target: -runner.pgid, signal: "SIGTERM" }], "only the verified negative process group may receive TERM");
  assert.ok(snapshot.events.some(event => event.type === "auto_resume_stall_terminating"));

  harness.checkAt(stalledAt + 15000);
  assert.strictEqual(harness.snapshot().signals.length, 1, "TERM must not repeat during its grace period");
}

function testManagedRunnerForceKillAndOneTimeRelaunch(harness) {
  const runner = seedManagedRunner(harness, { pid: 4343, startTicks: 998878 });
  const stalledAt = runner.runnerStartedAt + runner.stallMinutes * 60000;
  harness.checkAt(stalledAt);
  harness.checkAt(stalledAt + 30001);
  let snapshot = harness.snapshot();
  assert.deepStrictEqual(snapshot.signals, [
    { target: -runner.pgid, signal: "SIGTERM" },
    { target: -runner.pgid, signal: "SIGKILL" },
  ], "a still-owned group may receive one delayed SIGKILL, never a bare PID signal");

  harness.removeMemoryFile(`/proc/${runner.pid}/stat`);
  harness.checkAt(stalledAt + 31000);
  snapshot = harness.snapshot();
  assert.strictEqual(snapshot.launches.length, 1, "replacement may launch only after the owned group has exited");
  assert.strictEqual(harness.getProjectState(runner.project, 0).stallRestartCount, 1);
  assert.ok(snapshot.events.some(event => event.type === "auto_resume_stall_relaunching"));

  const secondRunId = "runner-stall-2";
  const secondStartedAt = stalledAt + 32000;
  harness.setProjectState(runner.project, 0, {
    idleEpochKeyUseTime: runner.keyUseAt,
    attemptCount: 2,
    lastAttemptAt: secondStartedAt,
    phase: "running",
    runnerRunId: secondRunId,
    runnerStartedAt: secondStartedAt,
    launchRequestedAt: secondStartedAt,
    runnerPid: 4444,
    processStartTicks: 998879,
    stallRestartCount: 1,
    stallPhase: "",
  });
  harness.setMemoryFile(runner.files.pidFile, JSON.stringify({ schema: 1, runId: secondRunId, pid: 4444, pgid: 4444, createdAt: secondStartedAt, processStartTicks: 998879 }));
  harness.setMemoryFile("/proc/4444/stat", procStat(4444, 998879));
  harness.checkAt(secondStartedAt + runner.stallMinutes * 60000);
  snapshot = harness.snapshot();
  assert.strictEqual(snapshot.signals.length, 2, "the configured one-time restart limit must prevent a third runner");
  assert.ok(snapshot.events.some(event => event.type === "auto_resume_stall_restart_exhausted"));
}

function testManagedRunnerStallResetsAfterRealKeyUse(harness) {
  const runner = seedManagedRunner(harness, { idleMinutes: 10, stallMinutes: 20, pid: 4545, startTicks: 998880 });
  const nextKeyUseAt = runner.runnerStartedAt + 1000;
  harness.recordKeyUse(1, nextKeyUseAt);
  harness.checkAt(nextKeyUseAt + 15 * 60000);
  assert.deepStrictEqual(harness.snapshot().signals, [], "a new real Key application must reset the runner stall clock");
}

function testManagedRunnerStallRejectsUnverifiedIdentity(harness) {
  const pgrpMismatch = seedManagedRunner(harness, { pid: 4646, pgid: 4646, procPgid: 9999, startTicks: 998881 });
  harness.checkAt(pgrpMismatch.runnerStartedAt + pgrpMismatch.stallMinutes * 60000);
  let snapshot = harness.snapshot();
  assert.deepStrictEqual(snapshot.signals, [], "a mismatched process group must never be signaled");
  assert.ok(snapshot.events.some(event => event.type === "auto_resume_stall_ownership_unknown"));

  const runIdMismatch = seedManagedRunner(harness, { pid: 4747, startTicks: 998882, leaseRunId: "runner-other" });
  harness.checkAt(runIdMismatch.runnerStartedAt + runIdMismatch.stallMinutes * 60000);
  assert.deepStrictEqual(harness.snapshot().signals, [], "a mismatched run ID must never be signaled");

  const legacyLease = seedManagedRunner(harness, { pid: 4848, startTicks: 998883, lease: false });
  harness.setMemoryFile(legacyLease.files.pidFile, "pgid:4848");
  harness.checkAt(legacyLease.runnerStartedAt + legacyLease.stallMinutes * 60000);
  assert.deepStrictEqual(harness.snapshot().signals, [], "a legacy lease must never be signaled");

  const reusedPid = seedManagedRunner(harness, { pid: 4949, startTicks: 998884, procStartTicks: 998885 });
  harness.checkAt(reusedPid.runnerStartedAt + reusedPid.stallMinutes * 60000);
  assert.deepStrictEqual(harness.snapshot().signals, [], "a reused PID start tick must never be signaled");
}

function testLeaseIdentityAndStatusCompatibility(harness) {
  harness.configure();
  harness.reset(1700000000000);
  const project = { name: "test", path: "/tmp", cmd: "true" };
  const files = harness.projectFiles(project, 0);
  const runId = "019f825b-fd3b-70d0-8edc-bd19b593586b";
  harness.setMemoryFile(files.pidFile, JSON.stringify({ schema: 1, runId, pid: 4242, pgid: 4242, createdAt: 1700000000000, processStartTicks: 998877 }));
  harness.setMemoryFile("/proc/4242/stat", procStat(4242, 998877));
  const lease = harness.readLease(files.pidFile);
  assert.strictEqual(lease.runId, runId);
  assert.strictEqual(lease.processStartTicks, 998877);
  assert.strictEqual(harness.isOwned(lease), true, "matching PID start ticks prove lease ownership");

  harness.setMemoryFile("/proc/4242/stat", procStat(4242, 998877, 4243));
  assert.strictEqual(harness.isOwned(lease), false, "a mismatched process group must not prove lease ownership");

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
  assert.match(source, /One initial launch per continuous idle episode/);
  assert.match(source, /auto_resume_stall_terminating/);
  assert.match(source, /process\.kill\(-lease\.pgid, "SIGTERM"\)/);
  assert.match(source, /process\.kill\(-lease\.pgid, "SIGKILL"\)/);
  assert.match(source, /processStartTicks/);
  assert.match(source, /projectFingerprint/);
  assert.match(source, /auto_resume_launcher_returned/);
}

function main() {
  const harness = loadAutoResumeHarness(__dirname);
  testStallConfigNormalization(harness);
  testUsesActualKeyApplicationTime(harness);
  testWaitsForInFlightRequest(harness);
  testRestoresDurableHeartbeat(harness);
  testLeaseIdentityAndStatusCompatibility(harness);
  testManagedRunnerStallSignalsOnlyVerifiedGroup(harness);
  testManagedRunnerForceKillAndOneTimeRelaunch(harness);
  testManagedRunnerStallResetsAfterRealKeyUse(harness);
  testManagedRunnerStallRejectsUnverifiedIdentity(harness);
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
