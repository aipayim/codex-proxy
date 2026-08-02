#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const vm = require("vm");

function loadTaskInsightHarness(proxyDir, tmpDir) {
  const source = fs.readFileSync(path.join(proxyDir, "proxy.js"), "utf8");
  const cutoff = source.lastIndexOf("// Start servers");
  if (cutoff < 0) throw new Error("proxy startup marker not found");
  const sandbox = {
    require,
    console,
    Buffer,
    URL,
    __dirname: tmpDir,
    __filename: path.join(tmpDir, "proxy.js"),
    setInterval: () => 0,
    clearInterval: () => {},
    setTimeout: () => 0,
    clearTimeout: () => {},
    process: { pid: 999997, argv: [], env: {}, on: () => {}, exit: () => {} },
  };
  sandbox.globalThis = sandbox;
  const harness = `;globalThis.__tiTest={
    setConfig(c){ config = c; },
    getConfig(){ return config; },
    normalizeTaskInsightConfig,
    taskInsightExtractRequest,
    taskInsightPreExtract,
    taskInsightSignal,
    taskInsightActive,
    makeUsageTransform,
    taskInsightJoin,
    taskInsightBaseKey,
    createTaskSession,
    finalizeTaskSession,
    taskInsightSessionIdleMs,
    taskInsightAddRequestMetrics,
    taskInsightBuildMetrics,
    taskInsightProjectHint,
    taskInsightPrune,
    extractFilePaths,
    pushUnique,
    taskSessions,
    TASK_INSIGHT_DEFAULT,
    TASK_INSIGHT_SESSION_IDLE_MS,
    TASK_INSIGHT_MAX_MEMORY_SESSIONS,
    TASK_SSE_LINE_MAX,
    TASK_INSIGHT_INSTRUCTION_MAX_CHARS,
    TASK_INSIGHT_TOOLS_MAX,
    TASK_INSIGHT_FILES_MAX,
    TASK_DIR
  };`;
  vm.runInNewContext(source.slice(0, cutoff) + harness, sandbox, { filename: "proxy.js", timeout: 8000 });
  return sandbox.__tiTest;
}

function sseLine(obj) {
  return "data: " + JSON.stringify(obj) + "\n\n";
}

function runTransform(harness, lines, model = "test-model") {
  return new Promise((resolve, reject) => {
    const tr = harness.makeUsageTransform(0, 0, 0, 0, model);
    let out = "";
    tr.on("data", c => { out += c.toString(); });
    tr.once("error", reject);
    tr.once("end", () => resolve({ tr, out }));
    for (const line of lines) tr.write(line);
    tr.end();
  });
}

function assertJsonEqual(actual, expected, message) {
  assert.strictEqual(JSON.stringify(actual), JSON.stringify(expected), message);
}

function testNormalize(harness) {
  const def = harness.normalizeTaskInsightConfig(undefined);
  assert.strictEqual(def.enabled, false);
  assertJsonEqual(def.signals, { instructions: false, tools: false, usage: false, correlate: false });
  assert.strictEqual(def.retentionDays, harness.TASK_INSIGHT_DEFAULT.retentionDays);
  assert.strictEqual(def.distill.engine, "ollama");
  assert.strictEqual(def.distill.model, "");
  assert.strictEqual(def.distill.baseUrl, "");
  assert.strictEqual(def.distill.dailyBudgetYuan, 1);
  assert.strictEqual(def.distill.report, "daily");

  const full = harness.normalizeTaskInsightConfig({
    enabled: true,
    signals: { instructions: true, tools: "yes", usage: 1, correlate: true },
    retentionDays: 5,
    distill: { enabled: true, engine: "proxy", model: "  qwen-7b  ", baseUrl: "http://127.0.0.1:11434/v1", dailyBudgetYuan: -3, report: "weekly" },
  });
  assert.strictEqual(full.enabled, true);
  assertJsonEqual(full.signals, { instructions: true, tools: false, usage: false, correlate: true });
  assert.strictEqual(full.retentionDays, 5);
  assert.strictEqual(full.distill.enabled, true);
  assert.strictEqual(full.distill.engine, "proxy");
  assert.strictEqual(full.distill.model, "qwen-7b");
  assert.strictEqual(full.distill.dailyBudgetYuan, 1, "negative budget falls back to default");
  assert.strictEqual(full.distill.report, "weekly");

  assert.strictEqual(harness.normalizeTaskInsightConfig({ retentionDays: 0 }).retentionDays, 1, "retention clamped to min");
  assert.strictEqual(harness.normalizeTaskInsightConfig({ retentionDays: 999999 }).retentionDays, 3650, "retention clamped to max");
  assert.strictEqual(harness.normalizeTaskInsightConfig({ distill: { engine: "nope" } }).distill.engine, "ollama", "invalid engine falls back");
  assert.strictEqual(harness.normalizeTaskInsightConfig({ distill: { dailyBudgetYuan: 0 } }).distill.dailyBudgetYuan, 0, "zero budget is allowed");
  assert.strictEqual(harness.normalizeTaskInsightConfig([]).enabled, false, "non-object value treated as empty");
}

function testExtract(harness) {
  harness.setConfig({ taskInsight: { enabled: true, signals: { instructions: true, tools: true, usage: true, correlate: false } } });
  const sig = harness.taskInsightExtractRequest({
    model: "gpt-5",
    instructions: "  修复登录页，  先读取相关文件。  ",
    tools: [{ type: "function", name: "read_file" }, { name: "write_file" }],
    input: [{ type: "function_call", name: "read_file", arguments: '{"path":"src/foo.js"}' }],
  });
  assertJsonEqual(sig.instructions, ["修复登录页， 先读取相关文件。"]);
  assertJsonEqual(sig.tools, ["read_file", "write_file"]);
  assertJsonEqual(sig.files, ["src/foo.js"]);

  const chat = harness.taskInsightExtractRequest({ model: "m", tools: [{ type: "function", function: { name: "apply_patch" } }] });
  assertJsonEqual(chat.tools, ["apply_patch"], "chat-format tools (t.function.name)");

  const long = harness.taskInsightExtractRequest({ instructions: "x".repeat(500) });
  assert.strictEqual(long.instructions[0].length, harness.TASK_INSIGHT_INSTRUCTION_MAX_CHARS, "instructions truncated");

  harness.setConfig({ taskInsight: { enabled: true, signals: { instructions: false, tools: false } } });
  const off = harness.taskInsightExtractRequest({ instructions: "hi", tools: [{ name: "t" }], input: [{ type: "function_call", name: "t", arguments: '{"path":"a.txt"}' }] });
  assertJsonEqual(off, { instructions: [], tools: [], files: [] }, "signals off yields empty extraction");

  harness.setConfig({ taskInsight: { enabled: true, signals: { instructions: true, tools: true, usage: true, correlate: false } } });
  const pre = harness.taskInsightPreExtract({ instructions: "pre", tools: [], input: [] });
  assertJsonEqual(pre.instructions, ["pre"], "preExtract helper works");
  assert.strictEqual(harness.taskInsightPreExtract(null), null);
  assertJsonEqual(harness.taskInsightPreExtract({}), { instructions: [], tools: [], files: [] });
}

async function testScan(harness) {
  harness.setConfig({ taskInsight: { enabled: true, signals: { usage: true, tools: true, instructions: false, correlate: false } } });

  const chatTool = sseLine({ id: "x", object: "chat.completion.chunk", model: "gpt-5", choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: "call_1", function: { name: "read_file", arguments: '{"path":"src/foo.js"}' } }] }, finish_reason: null }] });
  const chatUsage = sseLine({ id: "x", object: "chat.completion.chunk", model: "gpt-5", choices: [], usage: { prompt_tokens: 120, completion_tokens: 45 } });
  const { tr, out } = await runTransform(harness, [chatTool, chatUsage, "data: [DONE]\n\n"]);
  assertJsonEqual(tr._taskTools, ["read_file"], "chat delta tool_calls name captured");
  assertJsonEqual(tr._taskFiles, ["src/foo.js"], "chat delta tool_calls args path captured");
  assertJsonEqual(tr.insightUsage, { input_tokens: 120, output_tokens: 45 }, "chat usage captured");
  assert.strictEqual(out, chatTool + chatUsage + "data: [DONE]\n\n", "bytes pass through unchanged");

  const resp = sseLine({ type: "response.output_item.done", item: { type: "function_call", name: "apply_patch", arguments: '{"files":["a.txt","b.txt"]}' } });
  const { tr: tr2 } = await runTransform(harness, [resp]);
  assertJsonEqual(tr2._taskTools, ["apply_patch"], "responses-format function_call captured");
  assertJsonEqual(tr2._taskFiles, ["a.txt", "b.txt"]);

  const anth = sseLine({ type: "content_block_start", content_block: { type: "tool_use", name: "Read", input: { file_path: "src/foo.js" } } });
  const { tr: tr3 } = await runTransform(harness, [anth]);
  assertJsonEqual(tr3._taskTools, ["Read"], "anthropic tool_use captured");
  assertJsonEqual(tr3._taskFiles, ["src/foo.js"]);

  const tooLong = "data: " + JSON.stringify({ type: "response.output_item.done", item: { type: "function_call", name: "read_file", arguments: '{"path":"' + "x".repeat(harness.TASK_SSE_LINE_MAX) + '.txt"}' } });
  const { tr: tr4 } = await runTransform(harness, [tooLong]);
  assertJsonEqual(tr4._taskTools, [], "over-long line skipped by scanner");
  assertJsonEqual(tr4._taskFiles, []);
  assert.strictEqual(tr4.insightUsage, null);
}

function testSessions(harness) {
  harness.taskSessions.clear();
  harness.setConfig({ taskInsight: { enabled: true, signals: { instructions: false, tools: false, usage: false, correlate: false } }, autoResumeIdleMinutes: 10 });

  const t0 = 1700000000000;
  const s1 = harness.taskInsightJoin(t0, "codex", "A", null);
  assert.strictEqual(s1.requestCount, 1);
  const s2 = harness.taskInsightJoin(t0 + 1000, "codex", "A", null);
  assert.strictEqual(s2, s1, "same client/group joins existing session");
  assert.strictEqual(s2.requestCount, 2);
  assert.strictEqual(harness.taskSessions.size, 1);

  const idle = harness.taskInsightSessionIdleMs();
  const s3 = harness.taskInsightJoin(t0 + 1000 + idle + 1, "codex", "A", null);
  assert.notStrictEqual(s3, s1, "idle gap starts a new session");
  assert.strictEqual(s1.status, "completed", "old session finalized as completed");
  assert.strictEqual(s3.requestCount, 1);
  assert.strictEqual(harness.taskSessions.size, 1);

  harness.setConfig({ taskInsight: { enabled: true, signals: { correlate: true, instructions: false, tools: false, usage: false } } });
  const sp1 = harness.taskInsightJoin(t0, "codex", "A", { id: "proj-1", name: "Alpha" });
  const sp2 = harness.taskInsightJoin(t0 + 1000, "codex", "A", { id: "proj-2", name: "Beta" });
  assert.notStrictEqual(sp2, sp1, "project switch starts a new session");
  assert.strictEqual(sp1.projectId, "proj-1");
  assert.strictEqual(sp2.projectId, "proj-2");
  assert.strictEqual(sp1.status, "completed");

  harness.setConfig({ taskInsight: { enabled: true, signals: { instructions: false, tools: false, usage: false, correlate: false } } });
  harness.taskSessions.clear();
  const failed = harness.taskInsightJoin(t0, "c", "A", null);
  harness.taskInsightAddRequestMetrics(failed, { success: false, model: "m", time: t0, inputBytes: 0, outputBytes: 0, inputTokens: 0, outputTokens: 0, statusCode: 502, dur: 10 });
  harness.finalizeTaskSession(failed, t0 + 10);
  assert.strictEqual(failed.status, "failed", "all failures finalize as failed");

  harness.taskSessions.clear();
  const partial = harness.taskInsightJoin(t0, "c", "A", null);
  harness.taskInsightAddRequestMetrics(partial, { success: true, model: "m", time: t0, inputBytes: 0, outputBytes: 0, inputTokens: 0, outputTokens: 0, statusCode: 200, dur: 10 });
  harness.taskInsightAddRequestMetrics(partial, { success: false, model: "m", time: t0 + 1, inputBytes: 0, outputBytes: 0, inputTokens: 0, outputTokens: 0, statusCode: 502, dur: 10 });
  harness.finalizeTaskSession(partial, t0 + 10);
  assert.strictEqual(partial.status, "partial", "mixed results finalize as partial");

  harness.taskSessions.clear();
  const good = harness.taskInsightJoin(t0, "c", "A", null);
  harness.taskInsightAddRequestMetrics(good, { success: true, model: "gpt-5", time: t0, inputBytes: 0, outputBytes: 0, inputTokens: 0, outputTokens: 0, statusCode: 200, dur: 10 });
  assertJsonEqual(good.models, ["gpt-5"]);
}

function testBuildMetrics(harness) {
  const lc = { terminalKind: "completed", terminalReason: "upstream_done", inputTokens: 0, outputTokens: 0 };
  const m = harness.taskInsightBuildMetrics(lc, { insightUsage: { input_tokens: 10, output_tokens: 5 }, _taskTools: ["a"], _taskFiles: [] }, 100, 200, 50, 200, true, "gpt-5", "upstream_done");
  assert.strictEqual(m.success, true);
  assert.strictEqual(m.inputTokens, 10);
  assert.strictEqual(m.outputTokens, 5);
  assert.strictEqual(m.inputBytes, 100);
  assert.strictEqual(m.outputBytes, 200);
  assertJsonEqual(m.tools, ["a"]);
  assert.strictEqual(m.model, "gpt-5");

  const failed = harness.taskInsightBuildMetrics({ terminalKind: "failed", terminalReason: "upstream_eof_without_done" }, null, 0, 0, 0, 200, false, "gpt-5", "upstream_eof_without_done");
  assert.strictEqual(failed.success, false, "failed terminal kind maps to failure");
}

function testPrune(harness) {
  const tasksDir = path.join(harness.TASK_DIR);
  fs.mkdirSync(tasksDir, { recursive: true });
  const oldFile = path.join(tasksDir, "2026-01-01.jsonl");
  const newFile = path.join(tasksDir, "2026-08-02.jsonl");
  fs.writeFileSync(oldFile, "{}\n");
  fs.writeFileSync(newFile, "{}\n");
  harness.setConfig({ taskInsight: { enabled: true, retentionDays: 1, signals: { instructions: false, tools: false, usage: false, correlate: false } } });
  harness.taskInsightPrune(Date.now());
  assert.strictEqual(fs.existsSync(oldFile), false, "stale day file pruned");
  assert.strictEqual(fs.existsSync(newFile), true, "recent day file kept");
  fs.rmSync(tasksDir, { recursive: true, force: true });
}

async function main() {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ti-test-"));
  const harness = loadTaskInsightHarness(__dirname, tmpDir);
  testNormalize(harness);
  testExtract(harness);
  await testScan(harness);
  testSessions(harness);
  testBuildMetrics(harness);
  testPrune(harness);
  fs.rmSync(tmpDir, { recursive: true, force: true });
  console.log("task insight: PASS");
}

main().catch(error => {
  console.error(`[task-insight-test] ${error.stack || error.message}`);
  process.exitCode = 1;
});
