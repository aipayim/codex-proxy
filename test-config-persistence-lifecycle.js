#!/usr/bin/env node
"use strict";

const assert = require("assert");
const crypto = require("crypto");
const fs = require("fs");
const os = require("os");
const path = require("path");

const ROOT = __dirname;

function loadConfigPersistenceHarness(configFile) {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  const start = source.indexOf("let configSaveQueue = Promise.resolve();");
  const end = source.indexOf("\nfunction normalizeCodexLogMaintenancePath", start);
  assert.ok(start >= 0 && end > start, "config save queue and atomic writer must be present");
  const factory = new Function(
    "fs", "path", "crypto", "CONFIG_FILE", "__dirname", "process",
    `${source.slice(start, end)}; return { enqueueConfigSave, writeConfigAtomically };`,
  );
  return factory(fs, path, crypto, configFile, path.dirname(configFile), { pid: 999991 });
}

async function testQueuePreservesIndependentUpdates(harness, configFile) {
  let releaseFirst;
  let firstStartedResolve;
  const firstStarted = new Promise(resolve => { firstStartedResolve = resolve; });
  let secondStarted = false;

  const first = harness.enqueueConfigSave(async () => {
    const current = JSON.parse(fs.readFileSync(configFile, "utf8"));
    await new Promise(resolve => {
      releaseFirst = resolve;
      firstStartedResolve();
    });
    current.first = true;
    harness.writeConfigAtomically(current);
  });
  const second = harness.enqueueConfigSave(async () => {
    secondStarted = true;
    const current = JSON.parse(fs.readFileSync(configFile, "utf8"));
    current.second = true;
    harness.writeConfigAtomically(current);
  });

  await firstStarted;
  await Promise.resolve();
  assert.strictEqual(secondStarted, false, "the second save must wait for the first read/validate/write transaction");
  releaseFirst();
  await Promise.all([first, second]);

  assert.deepStrictEqual(JSON.parse(fs.readFileSync(configFile, "utf8")), { initial: true, first: true, second: true });
}

async function testQueueSurvivesRejectedSave(harness, configFile) {
  await assert.rejects(
    harness.enqueueConfigSave(async () => { throw new Error("intentional validation failure"); }),
    /intentional validation failure/,
  );
  await harness.enqueueConfigSave(async () => {
    const current = JSON.parse(fs.readFileSync(configFile, "utf8"));
    current.afterFailure = true;
    harness.writeConfigAtomically(current);
  });
  assert.strictEqual(JSON.parse(fs.readFileSync(configFile, "utf8")).afterFailure, true);
}

function testAtomicWriterArtifacts(configFile, tempDir) {
  const entries = fs.readdirSync(tempDir);
  assert.ok(!entries.some(name => name.startsWith(".config.json.") && name.endsWith(".tmp")), "completed saves must not leave temporary configuration files");
  const mode = fs.statSync(configFile).mode & 0o777;
  assert.strictEqual(mode, 0o600, "configuration writes must keep sensitive configuration owner-readable only");
}

function testSourceContracts() {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  assert.match(source, /let configSaveQueue = Promise\.resolve\(\);/);
  assert.match(source, /function enqueueConfigSave\(work\)/);
  assert.match(source, /configSaveQueue = run\.catch\(\(\) => \{\}\);/);
  assert.match(source, /function writeConfigAtomically\(value\)/);
  assert.match(source, /fs\.openSync\(temporaryFile, "wx", 0o600\)/);
  assert.match(source, /fs\.renameSync\(temporaryFile, CONFIG_FILE\)/);
  assert.match(source, /await enqueueConfigSave\(async \(\) => \{/);
}

async function main() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "codex-proxy-config-persistence-test-"));
  try {
    const configFile = path.join(tempDir, "config.json");
    fs.writeFileSync(configFile, JSON.stringify({ initial: true }), { encoding: "utf8", mode: 0o600 });
    const harness = loadConfigPersistenceHarness(configFile);
    await testQueuePreservesIndependentUpdates(harness, configFile);
    await testQueueSurvivesRejectedSave(harness, configFile);
    testAtomicWriterArtifacts(configFile, tempDir);
    testSourceContracts();
    console.log("Config persistence lifecycle: PASS");
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

main().catch(error => {
  console.error(error && error.stack || error);
  process.exitCode = 1;
});
