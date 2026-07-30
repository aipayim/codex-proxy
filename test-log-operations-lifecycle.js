#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { Worker } = require("worker_threads");

const ROOT = __dirname;
const WORKER_FILE = path.join(ROOT, "log-query-worker.js");

function runWorker(workerData) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(WORKER_FILE, { workerData });
    worker.once("message", message => {
      if (message && message.ok === true) resolve(message.result);
      else reject(new Error(message && message.error ? message.error : "worker failed"));
      worker.terminate();
    });
    worker.once("error", reject);
    worker.once("exit", code => {
      if (code !== 0) reject(new Error(`worker exited with ${code}`));
    });
  });
}

function writeJsonl(logDir, date, entries) {
  fs.writeFileSync(path.join(logDir, `${date}.jsonl`), entries.map(entry => JSON.stringify(entry)).join("\n") + "\n", "utf8");
}

function request(id, time, overrides = {}) {
  return {
    id,
    time,
    idx: 1,
    group: "A",
    method: "POST",
    path: "/v1/responses",
    url: "https://upstream.example/v1",
    reqModel: "gpt-test",
    status: 200,
    duration: 120,
    ttfb: 30,
    ...overrides,
  };
}

async function testCursorPagination(logDir) {
  const dayOne = Date.UTC(2026, 6, 29, 12, 0, 0);
  const dayTwo = Date.UTC(2026, 6, 30, 12, 0, 0);
  writeJsonl(logDir, "2026-07-29", [request("a", dayOne), request("b", dayOne + 1)]);
  writeJsonl(logDir, "2026-07-30", [request("c", dayTwo, { status: 502, group: "B" }), request("d", dayTwo + 1, { streamOutcome: "failed" })]);

  const seen = [];
  let cursor = null;
  do {
    const result = await runWorker({
      kind: "query",
      logDir,
      query: { limit: 2, cursor },
      maxLimit: 100,
      maxScanBytes: 1024 * 1024,
    });
    seen.push(...result.entries.map(entry => entry.id));
    cursor = result.nextCursor;
  } while (cursor);
  assert.deepStrictEqual(seen, ["d", "c", "b", "a"]);
  assert.strictEqual(new Set(seen).size, seen.length, "cursor pages must not duplicate entries");
}

async function testFilteredHistoryAndSummary(logDir) {
  const result = await runWorker({
    kind: "query",
    logDir,
    query: { limit: 50, status: "5xx", group: "B" },
    maxLimit: 100,
    maxScanBytes: 1024 * 1024,
  });
  assert.deepStrictEqual(result.entries.map(entry => entry.id), ["c"]);

  const summary = await runWorker({ kind: "summary", logDir });
  const day = summary.days["2026-07-30"];
  assert.ok(day, "summary must include persisted log day");
  assert.strictEqual(day.requests, 2);
  assert.strictEqual(day.error5xx, 1);
  assert.strictEqual(day.streamFailed, 1);

  const withoutCurrentDay = await runWorker({ kind: "summary", logDir, excludeDays: ["2026-07-30"] });
  assert.ok(!withoutCurrentDay.days["2026-07-30"], "summary rebuild must avoid double-counting the active log day");
}

async function testRecentHistoryTail(logDir) {
  const result = await runWorker({
    kind: "query",
    logDir,
    query: { limit: 100 },
    maxLimit: 100,
    maxScanBytes: 1024 * 1024,
  });
  assert.deepStrictEqual(result.entries.map(entry => entry.id), ["d", "c", "b", "a"], "recent fallback must receive the persisted tail in newest-first order");
  assert.strictEqual(result.filesAvailable, 2, "recent fallback must report recognized persisted log files");
  assert.strictEqual(result.filesScanned, 2, "recent fallback must report files it scanned");
}

function testRecentMemoryHistoryMerge() {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  const start = source.indexOf("function logEntryFingerprint(entry)");
  const end = source.indexOf("\nfunction startHistoricalLogQuery", start);
  assert.ok(start >= 0 && end > start, "recent log merge helpers must be present");
  const merge = new Function("LOG_RECENT_DEFAULT_LIMIT", `${source.slice(start, end)}; return mergeRecentLogEntries;`)(50);
  const memory = [
    { id: "new", time: 300 },
    { id: "shared", time: 200 },
  ];
  const historical = [
    { id: "new", time: 300 },
    { id: "shared", time: 200 },
    { id: "old", time: 100 },
  ];
  assert.deepStrictEqual(merge(memory, historical, 3).map(entry => entry.id), ["new", "shared", "old"], "recent fallback must merge persisted entries without duplicating in-memory entries");
}

function testUnfilteredLogQueryTimeBounds() {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  const start = source.indexOf("function parseLogQuery(url, format)");
  const end = source.indexOf("\nfunction logEntryMatchesQuery", start);
  assert.ok(start >= 0 && end > start, "log query parser must be present");
  const parseLogQuery = new Function("LOG_EXPORT_MAX_LIMIT", "LOG_HISTORY_MAX_LIMIT", "LOG_RECENT_DEFAULT_LIMIT", `${source.slice(start, end)}; return parseLogQuery;`)(2000, 100, 50);
  const unfiltered = parseLogQuery(new URL("http://localhost/__logs"), "");
  assert.strictEqual(unfiltered.since, undefined, "missing since must not become the Unix epoch");
  assert.strictEqual(unfiltered.until, undefined, "missing until must not become the Unix epoch");
  assert.strictEqual(unfiltered.mode, "recent", "an unfiltered log query must remain recent mode");
  const explicitEpoch = parseLogQuery(new URL("http://localhost/__logs?since=0&until=0"), "");
  assert.strictEqual(explicitEpoch.since, 0, "an explicit epoch lower bound must remain valid");
  assert.strictEqual(explicitEpoch.until, 0, "an explicit epoch upper bound must remain valid");
}

function testSourceContracts() {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  assert.match(source, /pathname === "\/__log-overview"/);
  assert.match(source, /pathname === "\/__incidents"/);
  assert.match(source, /pathname === "\/__incident-action"/);
  assert.match(source, /pathname === "\/__logs\/rebuild-summary"/);
  assert.match(source, /type:\s*"log_subscribe"/);
  assert.match(source, /LOG_FAST_PAGE_SIZE = 50/);
  assert.match(source, /if \(!summaryWasLoaded && LOG_FILE_ENABLED\)/);
  assert.match(source, /excludeDays: \[getLogDateKey\(Date\.now\(\)\)\]/);
  assert.match(source, /if \(logCleanupInFlight \|\| LOG_RETENTION_DAYS <= 0\) return;/);
  assert.match(source, /function mergeRecentLogEntries\(memoryEntries, historicalEntries, limit\)/);
  assert.match(source, /if \(value == null \|\| String\(value\)\.trim\(\) === ""\) return undefined;/);
  assert.match(source, /const fallbackQuery = \{ \.\.\.query, mode: "history", cursor: null, limit: LOG_HISTORY_MAX_LIMIT \};/);
  assert.match(source, /if \(memoryEntries\.length >= query\.limit\)/);
  assert.doesNotMatch(source, /if \(!LOG_FILE_ENABLED \|\| memoryEntries\.length >= query\.limit\)/);
  assert.match(source, /historyUnavailable/);
  assert.match(source, /historyFilesAvailable/);
  assert.match(source, /logRecentSource/);
  assert.match(source, /\.btn:disabled\{cursor:not-allowed;opacity:\.55\}/);
  assert.match(source, /id="logBrowseHistoryBtn"/);
  assert.match(source, /function toggleLogHistory\(\)/);
  assert.match(source, /id="logIncidentRefreshBtn"/);
  assert.match(source, /id="logIncidentRefreshStatus"/);
  assert.match(source, /refreshLogOperations\(\{interactive:true\}\)/);
  assert.match(source, /logIncidentRefreshStatus="已刷新 "/);
  assert.doesNotMatch(source, /_logCache/);
  assert.doesNotMatch(source, /countAllEntries/);
  assert.doesNotMatch(source, /logWrittenCount/);
}

async function main() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "codex-proxy-log-test-"));
  try {
    await testCursorPagination(tempDir);
    await testFilteredHistoryAndSummary(tempDir);
    await testRecentHistoryTail(tempDir);
    testRecentMemoryHistoryMerge();
    testUnfilteredLogQueryTimeBounds();
    testSourceContracts();
    console.log("log query, summary, and incident lifecycle: PASS");
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

main().catch(error => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
