#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawnSync } = require("child_process");

const ROOT = __dirname;
const LAUNCHER = path.join(ROOT, "resume-codex.sh");

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function waitFor(check, label, timeoutMs = 6000) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      const value = check();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await sleep(40);
  }
  throw new Error(`timed out waiting for ${label}${lastError ? `: ${lastError.message}` : ""}`);
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function readProcIdentity(pid) {
  const raw = fs.readFileSync(`/proc/${pid}/stat`, "utf8").trim();
  const end = raw.lastIndexOf(") ");
  if (end < 0) throw new Error(`invalid proc stat for ${pid}`);
  const fields = raw.slice(end + 2).trim().split(/\s+/);
  return { pgid: Number(fields[2]), startTicks: Number(fields[19]) };
}

function writeExecutable(file, source) {
  fs.writeFileSync(file, source, { encoding: "utf8", mode: 0o700 });
}

function runLauncher(projectDir, command, pidFile, statusFile, fakeCmd, runId, env) {
  const result = spawnSync("bash", [LAUNCHER, projectDir, command, pidFile, statusFile, "runner-test", fakeCmd, runId], {
    cwd: ROOT,
    env,
    encoding: "utf8",
    timeout: 10000,
  });
  if (result.error) throw result.error;
  assert.strictEqual(result.status, 0, `launcher must return after fake cmd accepts start: ${result.stderr || result.stdout}`);
}

function childProcessesAvailable() {
  const probe = spawnSync("bash", ["-n", LAUNCHER], { cwd: ROOT, encoding: "utf8", timeout: 5000 });
  if (probe.error && ["EPERM", "EACCES"].includes(probe.error.code)) return false;
  if (probe.error) throw probe.error;
  assert.strictEqual(probe.status, 0, probe.stderr || "resume launcher bash syntax check failed");
  return true;
}

async function main() {
  const launcherSource = fs.readFileSync(LAUNCHER, "utf8");
  assert.match(launcherSource, /current_start_ticks/, "runner must verify child start time before signalling");
  assert.doesNotMatch(launcherSource, /kill "\$command_pid"/, "runner must not fall back to a bare PID kill");
  if (!childProcessesAvailable()) {
    console.log("Resume runner lifecycle: SKIP (child-process sandbox unavailable)");
    return;
  }
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "codex-proxy-resume-runner-test-"));
  let managedRunnerPid = 0;
  try {
    const projectDir = path.join(tempDir, "project");
    fs.mkdirSync(projectDir);
    const fakeCmd = path.join(tempDir, "fake-cmd.sh");
    const runnerPidFile = path.join(tempDir, "runner.pid");
    writeExecutable(fakeCmd, `#!/usr/bin/env bash
set -eu
runner=""
for arg in "$@"; do runner="$arg"; done
bash "$runner" </dev/null >/dev/null 2>&1 &
child=$!
if [ -n "\${FAKE_RUNNER_PID_FILE:-}" ]; then printf '%s\\n' "$child" > "$FAKE_RUNNER_PID_FILE"; fi
exit 0
`);

    const baseEnv = { ...process.env, FAKE_RUNNER_PID_FILE: runnerPidFile };
    const shortPidFile = path.join(tempDir, "short.pid");
    const shortStatusFile = path.join(tempDir, "short.status");
    runLauncher(projectDir, "true", shortPidFile, shortStatusFile, fakeCmd, "runner-short-1", baseEnv);
    const shortStatus = await waitFor(() => {
      const value = readJson(shortStatusFile);
      return value.phase === "exited" ? value : null;
    }, "normal runner completion");
    assert.deepStrictEqual({ schema: shortStatus.schema, phase: shortStatus.phase, exitCode: shortStatus.exitCode, runId: shortStatus.runId, origin: shortStatus.origin }, {
      schema: 2, phase: "exited", exitCode: 0, runId: "runner-short-1", origin: "command",
    });
    assert.strictEqual(fs.existsSync(shortPidFile), false, "terminal runner must remove only its own lease");

    fs.rmSync(runnerPidFile, { force: true });
    const signalPidFile = path.join(tempDir, "signal.pid");
    const signalStatusFile = path.join(tempDir, "signal.status");
    runLauncher(projectDir, "sleep 30", signalPidFile, signalStatusFile, fakeCmd, "runner-signal-1", baseEnv);
    managedRunnerPid = Number(await waitFor(() => {
      const value = fs.readFileSync(runnerPidFile, "utf8").trim();
      return /^\d+$/.test(value) ? value : null;
    }, "runner process id"));
    const runningStatus = await waitFor(() => {
      const value = readJson(signalStatusFile);
      return value.phase === "running" ? value : null;
    }, "runner running status");
    assert.strictEqual(runningStatus.runId, "runner-signal-1");
    assert.ok(fs.existsSync(signalPidFile), "running runner must hold a lease");

    const lease = readJson(signalPidFile);
    assert.strictEqual(lease.schema, 1);
    assert.strictEqual(lease.runId, "runner-signal-1");
    assert.strictEqual(lease.pgid, lease.pid, "lease PGID must be the dedicated command process group");
    assert.ok(Number.isSafeInteger(lease.pid) && lease.pid > 1, "lease must identify the command process");
    assert.ok(Number.isSafeInteger(lease.processStartTicks) && lease.processStartTicks > 0, "lease must include the command start tick");
    const procIdentity = readProcIdentity(lease.pid);
    assert.strictEqual(procIdentity.pgid, lease.pgid, "setsid command must own the leased process group");
    assert.strictEqual(procIdentity.startTicks, lease.processStartTicks, "lease start tick must match the live command");

    process.kill(-lease.pgid, "SIGTERM");
    await sleep(200);
    const afterTermStatus = readJson(signalStatusFile);
    if (afterTermStatus.phase === "running") {
      // Some script(1) builds let the PTY child finish its TERM handling
      // before the wrapper exits. This mirrors the proxy's delayed second
      // stage without making the regression test wait 30 seconds.
      process.kill(-lease.pgid, "SIGKILL");
    }
    const terminatedStatus = await waitFor(() => {
      const value = readJson(signalStatusFile);
      return ["terminated", "exited", "failed"].includes(value.phase) ? value : null;
    }, "process-group terminal status");
    assert.strictEqual(terminatedStatus.phase, "terminated", "process-group signal must produce a terminated runner status");
    assert.strictEqual(terminatedStatus.origin, "command_signal");
    assert.ok(terminatedStatus.signal, "terminal status must report a signal after process-group shutdown");
    assert.ok(terminatedStatus.exitCode >= 128 && terminatedStatus.exitCode <= 192, "terminal status must preserve a signal exit code");
    await waitFor(() => !fs.existsSync(signalPidFile), "lease cleanup");
    managedRunnerPid = 0;

    console.log("Resume runner lifecycle: PASS");
  } finally {
    if (managedRunnerPid > 1) {
      try { process.kill(managedRunnerPid, "SIGTERM"); } catch { /* already exited */ }
    }
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

main().catch(error => {
  console.error(`[resume-runner-lifecycle-test] ${error.stack || error.message}`);
  process.exitCode = 1;
});
