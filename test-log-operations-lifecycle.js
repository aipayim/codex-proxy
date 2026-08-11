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

async function testPersistedErrorMessageSearch(logDir) {
  const time = Date.UTC(2026, 6, 31, 12, 0, 0);
  writeJsonl(logDir, "2026-07-31", [
    request("capacity", time, {
      status: 429,
      upstreamErrorReason: "model_at_capacity",
      streamErrorMsg: "Selected model is at capacity. Please try a different model.",
    }),
  ]);
  const result = await runWorker({
    kind: "query",
    logDir,
    query: { limit: 50, q: "at capacity" },
    maxLimit: 100,
    maxScanBytes: 1024 * 1024,
  });
  assert.deepStrictEqual(result.entries.map(entry => entry.id), ["capacity"], "persisted upstream error text must be searchable after restart");
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
  assert.match(source, /if \(logCleanupInFlight \|\| logRotationInFlight\) return Promise\.resolve\(false\);/);
  assert.match(source, /const expired = LOG_RETENTION_DAYS > 0/);
  assert.match(source, /const budgetBytes =/);
  assert.match(source, /function mergeRecentLogEntries\(memoryEntries, historicalEntries, limit\)/);
  assert.match(source, /if \(value == null \|\| String\(value\)\.trim\(\) === ""\) return undefined;/);
  assert.match(source, /const fallbackQuery = \{ \.\.\.query, mode: "history", cursor: null, limit: LOG_HISTORY_MAX_LIMIT \};/);
  assert.match(source, /if \(memoryEntries\.length >= query\.limit\)/);
  assert.doesNotMatch(source, /if \(!LOG_FILE_ENABLED \|\| memoryEntries\.length >= query\.limit\)/);
  assert.match(source, /historyUnavailable/);
  assert.match(source, /historyFilesAvailable/);
  assert.match(source, /logRecentSource/);
  assert.match(source, /streamErrorMsg, entry\.upstreamErrorReason, entry\.terminalSource/);
  assert.match(source, /"upstreamErrorReason", "streamErrorMsg", "terminalSource"/);
  assert.match(source, /\.btn:disabled\{cursor:not-allowed;opacity:\.55\}/);
  assert.match(source, /id="logBrowseHistoryBtn"/);
  assert.match(source, /function toggleLogHistory\(\)/);
  assert.match(source, /id="logIncidentRefreshBtn"/);
  assert.match(source, /id="logIncidentRefreshStatus"/);
  assert.match(source, /refreshLogOperations\(\{interactive:true\}\)/);
  assert.match(source, /logIncidentRefreshStatus="Refreshed "/);
  assert.doesNotMatch(source, /_logCache/);
  assert.doesNotMatch(source, /countAllEntries/);
  assert.doesNotMatch(source, /logWrittenCount/);
  assert.match(source, /data-mode="health" onclick="setTrendMode\('health'\)"[^>]*>💚 健康<\/span><span class="trend-tab" data-mode="upstream" onclick="setTrendMode\('upstream'\)"[^>]*>🔺 上游<\/span><span class="trend-tab" data-mode="downstream"/);
  assert.match(source, /urls:\{\},urlsKeys:\{\},clients:\{\}\};\n  \}\n  const allModels=\{\};\n  const allUrls=\{\};\n  const allClients=\{\};/);
  assert.match(source, /try\{ uKey=new URL\(a\.url\)\.hostname; \}catch\(e\)\{ uKey=a\.url; \}/);
  assert.match(source, /h\.urls\[uKey\]=\s*\(h\.urls\[uKey\]\|\|0\)/);
  assert.match(source, /}else if\(trendMode==="health"\)\{\n    vals=keys\.map\(k=>\{const s=hMap\[k\]\.status;return\(s\.ok\|\|0\)/);
  assert.match(source, /}else if\(trendMode==="upstream"\)\{\n    vals=keys\.map\(k=>\{const u=hMap\[k\]\.urls\|\|\{\};return Object\.values\(u\)\.reduce\(\(s,n\)=>s\+n,0\);\}\);/);
  assert.match(source, /}else if\(trendMode==="upstream"\)\{\n    const sortedUrls=Object\.keys\(allUrls\)\.sort\(\(a,b\)=>allUrls\[b\]-allUrls\[a\]\);/);
  assert.match(source, /const topUrls=sortedUrls\.slice\(0,8\);/);
  assert.match(source, /urlColorMap\["\(other\)"\]="#6b7280";/);
  assert.match(source, /lines\.push\("  "\+t\("trend\.urlReq",\{u,id:uk\.join\(",#"\),n:uv\}\)\);/);
  assert.match(source, /allUrls\["\(other\)"\]=otherTotal;/);
  assert.match(source, /trendLegend/);
  assert.match(source, /function classifyClientApp\(ua\)/);
  const fwdStart = source.indexOf("function forwardRequest(");
  const fwdEnd = source.indexOf("function forwardWithPriority(", fwdStart);
  const fwdBody = source.slice(fwdStart, fwdEnd);
  assert.ok(fwdStart >= 0 && fwdEnd > fwdStart, "forwardRequest must be present before forwardWithPriority");
  assert.match(fwdBody, /const client = classifyClientApp\(headers\["user-agent"\]\);/);
  assert.ok(fwdBody.indexOf("const client =") < fwdBody.indexOf("recordRequest("), "forwardRequest must declare client before any recordRequest call");
  assert.ok((fwdBody.match(/recordRequest\(/g) || []).length >= 5, "forwardRequest must forward client to every recordRequest call site");
  assert.match(source, /url: acct\.url, client \};/);
  assert.match(source, /if \(client\) \{/);
  assert.match(source, /s\.hourly\[hk\]\.clients\[client\]\s*=\s*\(s\.hourly\[hk\]\.clients\[client\]\s*\|\|\s*0\)\s*\+\s*1;/);
  assert.match(source, /s\.daily\[d\]\.clients\[client\]\s*=\s*\(s\.daily\[d\]\.clients\[client\]\s*\|\|\s*0\)\s*\+\s*1;/);
  assert.match(source, /if\(v\.clients\)\{/);
  assert.match(source, /}else if\(trendMode==="downstream"\)\{\n    vals=keys\.map\(k=>\{const c=hMap\[k\]\.clients\|\|\{\};return Object\.values\(c\)\.reduce\(\(s,n\)=>s\+n,0\);\}\);/);
  assert.match(source, /}else if\(trendMode==="downstream"\)\{\n    const sortedClients=Object\.keys\(allClients\)\.sort\(\(a,b\)=>allClients\[b\]-allClients\[a\]\);/);
  assert.match(source, /const topClients=sortedClients\.slice\(0,8\);/);
  assert.match(source, /clientColorMap\["\(other\)"\]="#6b7280";/);
  assert.match(source, /allClients\["\(other\)"\]=otherTotal;/);
  assert.match(source, /lines\.push\("  "\+t\("trend\.clientReq",\{c,n:cv\}\)\);/);
  assert.match(source, /legendClients\.map\(c=>'<span class="trend-legend-item"/);
  assert.match(source, /"group", "client", "method", "path", "status"/);
  assert.match(source, /entry\.url, entry\.client, entry\.reqModel/);
  assert.match(source, /"Client: "\+\(entry\.client\|\|""\)/);
  assert.doesNotMatch(source, /if\(!h\.streams\[soKey\]\)/);
  assert.doesNotMatch(source, /const dColors=\{upstream_done/);
}

function testClassifyClientApp() {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  const start = source.indexOf("function classifyClientApp(");
  const end = source.indexOf("\nfunction recordRequest(", start);
  assert.ok(start >= 0 && end > start, "classifyClientApp definition must be found");
  const classifyClientApp = new Function(source.slice(start, end) + "; return classifyClientApp;")();

  const cases = [
    ["Codex/1.0", "Codex CLI"],
    ["Claude-Code/2.0.0 cli/2.0.0", "Claude Code"],
    ["Cursor/0.45.0 (Cursor AI)", "Cursor"],
    ["Chatbox/1.0.0", "Chatbox"],
    ["Cherry Studio/1.2.3", "Cherry Studio"],
    ["NextChat/2.15.0", "NextChat"],
    ["LobeChat/1.0.0", "LobeChat"],
    ["OpenAI/Python 1.30.0", "OpenAI SDK (Python)"],
    ["openai-node/4.60.0", "OpenAI SDK (Node)"],
    ["openai-typescript/4.50.0", "OpenAI SDK (TS)"],
    ["openai/v0.9.0", "OpenAI SDK"],
    ["vercel-ai/3.2.0", "Vercel AI SDK"],
    ["python-requests/2.31.0", "Python 脚本"],
    ["curl/8.5.0", "curl"],
    ["Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36", "Mozilla/5.0"],
    ["", "(未知)"],
    [undefined, "(未知)"],
  ];
  for (const [ua, expected] of cases) {
    assert.strictEqual(classifyClientApp(ua), expected, `classifyClientApp(${JSON.stringify(ua)})`);
  }
  assert.ok(classifyClientApp("some-very-long-custom-client-agent-string-that-exceeds-the-limit").length <= 40, "fallback label must be truncated to 40 chars");
  assert.strictEqual(classifyClientApp("foo/1.0"), "foo/1.0");
}

function testRecordRequestClientAccumulation() {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  const start = source.indexOf("function recordRequest(");
  const end = source.indexOf("\nfunction recordStreamOutcome(", start);
  assert.ok(start >= 0 && end > start, "recordRequest must be present");
  const stats = { hourly: {}, daily: {}, totalRequests: 0, successRequests: 0, failRequests: 0 };
  const state = { activeKey: null };
  const config = { rateLimit: false };
  const recordRequest = new Function(
    "getKeyState", "today", "saveState", "broadcastStatus", "recordSliding", "state", "config",
    `let estimateCost = (i, o) => 0;
     let stateBucketAddsSinceCompaction = 0;
     const invalidateStateStatusBucketCache = () => {};
     const STATE_HOURLY_MODEL_LIMIT = 12;
     const STATE_HOURLY_MODEL_NAME_MAX_CHARS = 160;
     const STATE_HOURLY_OTHER_MODEL = "(其他)";
     ${source.slice(start, end)}
     return recordRequest;`
  )(
    () => ({ stats }),
    () => "2026-07-31",
    () => {},
    () => {},
    () => {},
    state,
    config
  );
  const hk = "2026-07-31-" + String(new Date().getHours()).padStart(2, "0");
  recordRequest(0, true, 100, 200, 500, 100, "gpt-test", 200, "Codex CLI");
  recordRequest(0, false, 0, 0, 50, null, "gpt-test", 429, "Codex CLI");
  recordRequest(0, true, 10, 20, 30, 5, "gpt-test", 200, "curl/8.0");
  assert.deepStrictEqual(stats.hourly[hk].clients, { "Codex CLI": 2, "curl/8.0": 1 });
  assert.deepStrictEqual(stats.daily["2026-07-31"].clients, { "Codex CLI": 2, "curl/8.0": 1 });
  recordRequest(0, true, 1, 1, 1, 1, "gpt-test", 200, undefined);
  assert.strictEqual(stats.hourly[hk].clients["Codex CLI"], 2, "undefined client must not be recorded");
  assert.strictEqual(stats.hourly[hk].requests, 4);
}

async function main() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "codex-proxy-log-test-"));
  try {
    await testCursorPagination(tempDir);
    await testFilteredHistoryAndSummary(tempDir);
    await testRecentHistoryTail(tempDir);
    await testPersistedErrorMessageSearch(tempDir);
    testRecentMemoryHistoryMerge();
    testUnfilteredLogQueryTimeBounds();
    testClassifyClientApp();
    testRecordRequestClientAccumulation();
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
