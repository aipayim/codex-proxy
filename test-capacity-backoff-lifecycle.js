#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

function loadCapacityHarness(proxyDir) {
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
    rename() {},
    renameSync() {},
    unlinkSync() {},
    mkdirSync() {},
    statSync() {
      return { isDirectory: () => true };
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
    setTimeout: () => ({ unref: () => {} }),
    clearTimeout: () => {},
    process: { pid: 999995, argv: [], env: {}, on: () => {}, exit: () => {} },
  };
  sandbox.globalThis = sandbox;
  const harness = `
    ;LOG_FILE_ENABLED = false;
    globalThis.__capacityTest = {
      configure: (backoffSeconds, maxWaitSeconds) => {
        config.capacityBackoffSeconds = backoffSeconds;
        config.capacityMaxWaitSeconds = maxWaitSeconds;
      },
      reset: () => {
        state = { keys: [], activeKey: null, dailyLog: {} };
        accounts = [
          { key: "sk-test-1", url: "http://upstream.test", reset: "never", status: "active", models: [], priority: 0, group: "A" },
          { key: "sk-test-2", url: "http://upstream.test", reset: "never", status: "active", models: [], priority: 0, group: "A" },
        ];
      },
      markCapacityBackoff,
      markStreamTerminalFailure,
      markSuccess,
      markFailure,
      classifyUpstreamErrorMessage,
      inCooldown,
      checkAllFailed,
      getKeyState,
      config,
    };
  `;
  sandbox.memoryFiles = memoryFiles;
  vm.runInNewContext(source.slice(0, cutoff) + harness, sandbox, { filename: "proxy.js", timeout: 5000 });
  return sandbox.__capacityTest;
}

function testBackoffDoesNotRecordHardFailure(harness) {
  harness.configure(60, 300);
  harness.reset();
  harness.markCapacityBackoff(0);
  const ks = harness.getKeyState(0);
  assert.strictEqual(ks.failCode, null, "capacity backoff must not set failCode");
  assert.strictEqual(ks.failPeriod, null, "capacity backoff must not set failPeriod");
  assert.ok(ks.capUntil > Date.now(), "capUntil must be in the future");
  assert.strictEqual(harness.inCooldown(0), true, "key must be skipped while backed off");
  assert.strictEqual(harness.checkAllFailed(), false, "pure backoff must not count as all failed");
}

function testBackoffExpiresAndRecovers(harness) {
  harness.configure(60, 300);
  harness.reset();
  harness.markCapacityBackoff(0);
  const ks = harness.getKeyState(0);
  ks.capUntil = Date.now() - 1000;
  assert.strictEqual(harness.inCooldown(0), false, "key recovers after capUntil expires");
}

function testMarkSuccessClearsBackoff(harness) {
  harness.configure(60, 300);
  harness.reset();
  harness.markCapacityBackoff(0);
  assert.ok(harness.getKeyState(0).capUntil > Date.now());
  harness.markSuccess(0);
  assert.strictEqual(harness.getKeyState(0).capUntil, 0, "markSuccess must clear capUntil");
  assert.strictEqual(harness.getKeyState(0).failCode, null);
  assert.strictEqual(harness.inCooldown(0), false);
}

function testSseCapacityFailureUsesTransientBackoff(harness) {
  harness.configure(60, 300);
  harness.reset();
  harness.markStreamTerminalFailure(0, { terminalReason: "model_at_capacity" }, false);
  const ks = harness.getKeyState(0);
  assert.ok(ks.capUntil > Date.now(), "SSE capacity failure must set a temporary backoff");
  assert.strictEqual(ks.failCode, null, "SSE capacity failure must not set a hard failure code");
  assert.strictEqual(ks.failPeriod, null, "SSE capacity failure must not set a failure period");
  assert.strictEqual(harness.inCooldown(0), true, "SSE capacity failure must temporarily skip the Key");
  assert.strictEqual(harness.checkAllFailed(), false, "SSE capacity backoff must not report all keys failed");
}

function testOtherSseFailuresRemainFailures(harness) {
  harness.configure(60, 300);
  harness.reset();
  harness.markStreamTerminalFailure(0, { terminalReason: "upstream_eof_without_completed" }, false);
  const ks = harness.getKeyState(0);
  assert.strictEqual(ks.failCode, 0, "non-capacity SSE failures must retain the existing failure path");
  assert.ok(ks.failTime > 0, "non-capacity SSE failures must record their failure time");
  assert.strictEqual(ks.capUntil, 0, "non-capacity SSE failures must not be mislabeled as capacity backoff");
}

function testCancelledSseDoesNotChangeKeyState(harness) {
  harness.configure(60, 300);
  harness.reset();
  harness.markStreamTerminalFailure(0, { terminalReason: "model_at_capacity" }, true);
  const ks = harness.getKeyState(0);
  assert.strictEqual(ks.capUntil, 0, "a downstream cancellation is not an upstream capacity signal");
  assert.strictEqual(ks.failCode, null, "a downstream cancellation must not change failure state");
}

function testCheckAllFailedRequiresHardFailures(harness) {
  harness.configure(60, 300);
  harness.reset();
  harness.markCapacityBackoff(0);
  harness.markCapacityBackoff(1);
  assert.strictEqual(harness.checkAllFailed(), false, "keys only in transient backoff are not all failed");
  harness.getKeyState(0).failCode = "401";
  harness.getKeyState(1).failCode = "403";
  assert.strictEqual(harness.checkAllFailed(), true, "real hard failures still trigger all-failed");
}

function testQuota429ClassifiedAsInsufficientQuota(harness) {
  assert.strictEqual(
    harness.classifyUpstreamErrorMessage('error: code=429 reason="WEEKLY_LIMIT_EXCEEDED" message="weekly usage limit exceeded" metadata=map[]'),
    "insufficient_quota",
    "weekly limit exceeded must be classified as insufficient_quota",
  );
  assert.strictEqual(
    harness.classifyUpstreamErrorMessage('error: code=429 reason="MONTHLY_LIMIT_EXCEEDED" message="monthly usage limit exceeded" metadata=map[]'),
    "insufficient_quota",
    "monthly limit exceeded must be classified as insufficient_quota",
  );
  assert.strictEqual(harness.classifyUpstreamErrorMessage("You exceeded your current quota"), "insufficient_quota", "quota wording is insufficient_quota");
  assert.strictEqual(harness.classifyUpstreamErrorMessage("Rate limit exceeded, please slow down"), "upstream_api_error", "transient rate-limit wording must stay transient, not become quota");
  assert.strictEqual(harness.classifyUpstreamErrorMessage("Selected model is at capacity. Please try a different model."), "model_at_capacity", "capacity wording stays transient");
  assert.strictEqual(harness.classifyUpstreamErrorMessage("boom"), "upstream_api_error", "unrecognized errors remain generic");
}

function testQuota429HardFailurePutsKeyInPeriodCooldown(harness) {
  harness.reset();
  harness.markFailure(0, 429);
  const ks = harness.getKeyState(0);
  assert.strictEqual(ks.failCode, 429, "quota-classified 429 must be recorded as a hard failure");
  assert.strictEqual(ks.capUntil, 0, "quota-classified 429 must not use the short capacity backoff");
  assert.strictEqual(harness.inCooldown(0), true, "quota-exhausted key must leave rotation for the current period");
}

function testFailReasonTracking(harness) {
  harness.reset();
  assert.strictEqual(harness.getKeyState(0).failReason, null, "fresh key state must default failReason to null");
  harness.markFailure(0, 429, "insufficient_quota");
  assert.strictEqual(harness.getKeyState(0).failReason, "insufficient_quota", "markFailure must persist the classified reason");
  harness.markFailure(1, 500);
  assert.strictEqual(harness.getKeyState(1).failReason, null, "markFailure without a reason must not fabricate one");
  harness.markSuccess(0);
  assert.strictEqual(harness.getKeyState(0).failReason, null, "markSuccess must clear failReason");
}

function testDefaultsAndFreshKeyState(harness) {
  assert.strictEqual(harness.config.capacityBackoffSeconds, 60, "default backoff is 60s");
  assert.strictEqual(harness.config.capacityMaxWaitSeconds, 300, "default max wait is 300s");
  harness.reset();
  const fresh = harness.getKeyState(7);
  assert.strictEqual(fresh.capUntil, 0, "fresh key state must include capUntil:0");
}

function testSourceContracts(proxyDir) {
  const source = fs.readFileSync(path.join(proxyDir, "proxy.js"), "utf8");
  assert.match(source, /function markCapacityBackoff\(idx\)/);
  assert.match(source, /function markStreamTerminalFailure\(idx, lifecycle, clientCancelled\)/);
  assert.match(source, /lifecycle && lifecycle\.terminalReason === "model_at_capacity"/);
  assert.match(source, /markStreamTerminalFailure\(idx, lifecycle, clientCancelled\);/);
  assert.match(source, /capacityRetry: isCapacity/);
  assert.match(source, /const isCapacity = \(statusCode === 429 && reason !== "insufficient_quota"\) \|\| reason === "model_at_capacity"/);
  assert.match(source, /if \(r\.capacityRetry\)/);
  assert.match(source, /enqueueRequest\(method, headers, body, clientRes, pathname, group, extraTransform, r, true\)/);
  assert.match(source, /r\.capacity \? Math\.max\(1, Number\(config\.capacityMaxWaitSeconds\)\) \* 1000 : QUEUE_TIMEOUT/);
  assert.match(source, /capacity: capacity === true/);
  assert.match(source, /parseInt\(c\.capacityBackoffSeconds\) \|\| 60/);
  assert.match(source, /parseInt\(c\.capacityMaxWaitSeconds\) \|\| 300/);
  assert.match(source, /quota\|insufficient\|billing\|billing_hard_limit\|_limit_exceeded\|usage limit\|usage_limit\|daily limit\|weekly limit\|monthly limit/, "classifier must recognize quota/usage-limit wording");
  assert.doesNotMatch(source, /\|limit exceeded\|/, "generic rate-limit wording must not be treated as quota");
  assert.match(source, /setImmediate\(\(\) => \{ try \{ processQueue\(\); \} catch \(e\) \{\} \}\)/, "enqueueRequest must drain promptly");
  assert.match(source, /processQueue\(\); \} catch \(e\) \{\} \}, 5000/, "a steady queue drain timer must exist");
  assert.doesNotMatch(source, /requestQueue\.filter\(r => r\.time > qcut/, "stale queue entries must not be silently dropped");
}

function main() {
  const harness = loadCapacityHarness(__dirname);
  testBackoffDoesNotRecordHardFailure(harness);
  testBackoffExpiresAndRecovers(harness);
  testMarkSuccessClearsBackoff(harness);
  testSseCapacityFailureUsesTransientBackoff(harness);
  testOtherSseFailuresRemainFailures(harness);
  testCancelledSseDoesNotChangeKeyState(harness);
  testCheckAllFailedRequiresHardFailures(harness);
  testQuota429ClassifiedAsInsufficientQuota(harness);
  testQuota429HardFailurePutsKeyInPeriodCooldown(harness);
  testFailReasonTracking(harness);
  testDefaultsAndFreshKeyState(harness);
  testSourceContracts(__dirname);
  console.log("capacity backoff lifecycle: PASS");
}

try {
  main();
} catch (error) {
  console.error(`[capacity-backoff-lifecycle-test] ${error.stack || error.message}`);
  process.exitCode = 1;
}
