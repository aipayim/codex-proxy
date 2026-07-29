#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

function loadRestartHarness(proxyDir) {
  const source = fs.readFileSync(path.join(proxyDir, "proxy.js"), "utf8");
  const cutoff = source.lastIndexOf("// Start servers");
  if (cutoff < 0) throw new Error("proxy startup marker not found");
  const watchdogReloadWrites = [];
  const fsMock = {
    ...fs,
    writeFileSync(file, data, options) {
      watchdogReloadWrites.push({ file, data: String(data), options });
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
    process: { pid: 999996, argv: [], env: {}, on: () => {}, exit: () => {} },
  };
  sandbox.globalThis = sandbox;
  sandbox.watchdogReloadWrites = watchdogReloadWrites;
  const harness = `
    ;LOG_FILE_ENABLED = false;
    let restartTestQueuedResponses = [];
    globalThis.__restartLifecycleTest = {
      beginRestartDrain,
      cancelRestartDrain,
      requestForcedRestart,
      buildRestartStatus,
      requestWatchdogReload,
      getDashboardHTML,
      watchdogReloadWritesForTest: () => globalThis.watchdogReloadWrites.slice(),
      isRestarting: () => globalThis._restarting === true,
      setActiveRequestsForTest: (count) => {
        for (const key of Object.keys(activeRequests)) delete activeRequests[key];
        if (count > 0) activeRequests.restartTest = Array.from({ length: count }, () => ({}));
      },
      queueRequestsForTest: (count) => {
        restartTestQueuedResponses = Array.from({ length: count }, () => ({
          destroyed: false,
          headersSent: false,
          statusCode: 0,
          body: "",
          writeHead(statusCode) { this.statusCode = statusCode; this.headersSent = true; },
          end(body) { this.body = String(body || ""); },
        }));
        requestQueue = restartTestQueuedResponses.map(clientRes => ({ clientRes }));
        return restartTestQueuedResponses;
      },
      queuedResponsesForTest: () => restartTestQueuedResponses,
    };
  `;
  vm.runInNewContext(source.slice(0, cutoff) + harness, sandbox, { filename: "proxy.js", timeout: 5000 });
  return sandbox.__restartLifecycleTest;
}

function testDrainCanBeCancelled(harness) {
  const startedAt = 1700000000000;
  harness.setActiveRequestsForTest(2);
  const queued = harness.queueRequestsForTest(2);
  const started = harness.beginRestartDrain(startedAt);

  assert.strictEqual(started.ok, true);
  assert.ok(started.restartId);
  assert.strictEqual(started.phase, "draining");
  assert.strictEqual(started.activeRequests, 2);
  assert.strictEqual(started.cancelledQueuedRequests, 2);
  assert.strictEqual(started.canCancel, true);
  assert.strictEqual(started.canForce, false);
  assert.strictEqual(harness.isRestarting(), true);
  for (const response of queued) {
    assert.strictEqual(response.statusCode, 503);
    assert.match(response.body, /proxy is restarting/);
  }

  const beforeForce = harness.requestForcedRestart(startedAt + 29999);
  assert.strictEqual(beforeForce.ok, false);
  assert.strictEqual(beforeForce.retryAfterMs, 1);
  assert.strictEqual(beforeForce.canForce, false);

  const cancelled = harness.cancelRestartDrain(startedAt + 10000);
  assert.strictEqual(cancelled.ok, true);
  assert.strictEqual(cancelled.cancelledQueuedRequests, 2);
  assert.strictEqual(cancelled.phase, "ready");
  assert.strictEqual(cancelled.canCancel, false);
  assert.strictEqual(cancelled.queuedRequests, 0);
  assert.strictEqual(harness.isRestarting(), false);
  assert.strictEqual(harness.queuedResponsesForTest().length, 2);
}

function testForceNeedsExplicitDelay(harness) {
  const startedAt = 1700000100000;
  harness.setActiveRequestsForTest(3);
  const started = harness.beginRestartDrain(startedAt);
  assert.strictEqual(started.ok, true);

  const beforeForce = harness.buildRestartStatus(startedAt + 29999);
  assert.strictEqual(beforeForce.canForce, false);
  assert.strictEqual(beforeForce.forceAvailableInMs, 1);

  const forced = harness.requestForcedRestart(startedAt + 30000);
  assert.strictEqual(forced.ok, true);
  assert.strictEqual(forced.phase, "stopping");
  assert.strictEqual(forced.interruptedActiveRequests, 3);
  assert.strictEqual(forced.canCancel, false);
  assert.strictEqual(forced.canForce, false);
  assert.strictEqual(harness.cancelRestartDrain(startedAt + 30001).ok, false);
}

function testDashboardControls(harness) {
  const html = harness.getDashboardHTML();
  assert.match(html, /restartCancelBtn/);
  assert.match(html, /restartForceBtn/);
  assert.match(html, /function cancelPendingRestart\(\)/);
  assert.match(html, /function forcePendingRestart\(\)/);
  assert.match(html, /\/__restart\/cancel/);
  assert.match(html, /\/__restart\/force/);
  for (const match of html.matchAll(/<script>([\s\S]*?)<\/script>/g)) new Function(match[1]);
}

function testWatchdogReloadContract(harness, proxyDir) {
  assert.strictEqual(harness.requestWatchdogReload(), true);
  const writes = harness.watchdogReloadWritesForTest();
  assert.strictEqual(writes.length, 1);
  assert.strictEqual(writes[0].file, path.join(proxyDir, ".watchdog-reload"));
  assert.match(writes[0].data, /instanceId/);

  const watchdog = fs.readFileSync(path.join(proxyDir, "watchdog.sh"), "utf8");
  assert.match(watchdog, /WATCHDOG_RELOAD_FILE=/);
  assert.match(watchdog, /exec \/bin\/bash "\$PROXY_DIR\/watchdog\.sh"/);
}

function main() {
  const harness = loadRestartHarness(__dirname);
  testDrainCanBeCancelled(harness);
  testForceNeedsExplicitDelay(harness);
  testDashboardControls(harness);
  testWatchdogReloadContract(harness, __dirname);
  console.log("restart lifecycle: PASS");
}

try {
  main();
} catch (error) {
  console.error(`[restart-lifecycle-test] ${error.stack || error.message}`);
  process.exitCode = 1;
}
