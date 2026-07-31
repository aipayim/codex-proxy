#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

function loadStreamHarness(proxyDir) {
  const source = fs.readFileSync(path.join(proxyDir, "proxy.js"), "utf8");
  const cutoff = source.lastIndexOf("// Start servers");
  if (cutoff < 0) throw new Error("proxy startup marker not found");
  const sandbox = {
    require,
    console,
    Buffer,
    URL,
    __dirname: proxyDir,
    __filename: path.join(proxyDir, "proxy.js"),
    setInterval: () => 0,
    clearInterval: () => {},
    setTimeout: () => 0,
    clearTimeout: () => {},
    process: { pid: 999998, argv: [], env: {}, on: () => {}, exit: () => {} },
  };
  sandbox.globalThis = sandbox;
  const harness = ";globalThis.__streamLifecycleTest={createResponsesLifecycle,createChatToResponsesStream,sanitizeUpstreamErrorMessage,extractUpstreamErrorMessage,classifyUpstreamErrorMessage,captureUpstreamErrorMessage};";
  vm.runInNewContext(source.slice(0, cutoff) + harness, sandbox, { filename: "proxy.js", timeout: 5000 });
  return sandbox.__streamLifecycleTest;
}

function eventNames(output) {
  return [...output.matchAll(/^event: ([^\n]+)$/gm)].map(match => match[1]);
}

function createLifecycleStream(harness, model) {
  const lifecycle = harness.createResponsesLifecycle(model);
  const transform = harness.createChatToResponsesStream(lifecycle);
  lifecycle._transform = transform;
  transform._lifecycle = lifecycle;
  return { lifecycle, transform };
}

function runStream(harness, chunks, model = "test-model") {
  return new Promise((resolve, reject) => {
    const { lifecycle, transform } = createLifecycleStream(harness, model);
    let output = "";
    transform.on("data", chunk => { output += chunk.toString(); });
    transform.once("error", reject);
    transform.once("end", () => resolve({ lifecycle, output }));
    for (const chunk of chunks) transform.write(chunk);
    transform.end();
  });
}

function chatChunk(delta, options = {}) {
  return JSON.stringify({
    id: "chatcmpl_test",
    object: "chat.completion.chunk",
    model: options.model || "test-model",
    choices: [{ index: 0, delta, finish_reason: options.finishReason || null }],
    ...(options.usage ? { usage: options.usage } : {}),
  });
}

async function testCompletedStream(harness) {
  const payload = chatChunk({ content: "hello" }, { usage: { prompt_tokens: 3, completion_tokens: 2 } });
  const { lifecycle, output } = await runStream(harness, [`data: ${payload}\n`, "data: [DONE]\n"]);
  assert.strictEqual(lifecycle.terminalKind, "completed");
  assert.strictEqual(lifecycle.terminalReason, "upstream_done");
  assert.strictEqual(lifecycle.sawDone, true);
  assert.strictEqual(lifecycle.fullContent, "hello");
  assert.deepStrictEqual(eventNames(output), [
    "response.created",
    "response.in_progress",
    "response.output_text.delta",
    "response.output_text.done",
    "response.completed",
  ]);
  assert.match(output, /data: \[DONE\]\n\n$/);
}

async function testDoneWithoutTrailingNewline(harness) {
  const payload = chatChunk({ content: "tail" });
  const { lifecycle, output } = await runStream(harness, [`data: ${payload}\n\ndata: [DONE]`]);
  assert.strictEqual(lifecycle.terminalKind, "completed");
  assert.strictEqual(lifecycle.terminalReason, "upstream_done");
  assert.strictEqual(lifecycle.sawDone, true);
  assert.ok(eventNames(output).includes("response.completed"));
  assert.ok(!eventNames(output).includes("response.failed"));
}

async function testEofWithoutDoneFails(harness) {
  const payload = chatChunk({ content: "partial" });
  const { lifecycle, output } = await runStream(harness, [`data: ${payload}\n`]);
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "upstream_eof_without_done");
  assert.ok(eventNames(output).includes("response.failed"));
  assert.ok(!eventNames(output).includes("response.completed"));
  assert.ok(!output.includes("data: [DONE]"));
}

async function testEmptyStreamFails(harness) {
  const { lifecycle, output } = await runStream(harness, []);
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "upstream_eof_without_done");
  assert.deepStrictEqual(eventNames(output), ["response.created", "response.in_progress", "response.failed"]);
}

async function testToolCallCompletion(harness) {
  const first = chatChunk({ tool_calls: [{ index: 0, id: "call_test", function: { name: "write_file", arguments: "{\"path\":\"" } }] });
  const second = chatChunk({ tool_calls: [{ index: 0, function: { arguments: "a.txt\"}" } }] }, { finishReason: "tool_calls" });
  const { lifecycle, output } = await runStream(harness, [`data: ${first}\n`, `data: ${second}\n`, "data: [DONE]\n"]);
  const names = eventNames(output);
  assert.strictEqual(lifecycle.terminalKind, "completed");
  assert.ok(names.includes("response.output_item.added"));
  assert.ok(names.includes("response.function_call_arguments.starting"));
  assert.ok(names.includes("response.function_call_arguments.delta"));
  assert.ok(names.includes("response.function_call_arguments.done"));
  assert.ok(names.includes("response.output_item.done"));
  assert.ok(names.includes("response.completed"));
  assert.strictEqual(names.filter(name => name === "response.function_call_arguments.done").length, 1);
}

function testClientCancellation(harness) {
  const { lifecycle, transform } = createLifecycleStream(harness, "test-model");
  let output = "";
  transform.on("data", chunk => { output += chunk.toString(); });
  assert.strictEqual(lifecycle.noteClientCancelled("client_disconnect"), true);
  assert.strictEqual(lifecycle.terminalKind, "cancelled");
  assert.strictEqual(lifecycle.terminalReason, "client_disconnect");
  assert.ok(!output.includes("response.completed"));
  assert.ok(!output.includes("response.failed"));
  transform.destroy();
}

function sseError(message, extra = {}) {
  return JSON.stringify({ error: { message, type: extra.type || "server_error", code: extra.code || null }, ...extra });
}

async function testModelAtCapacityError(harness) {
  const { lifecycle, output } = await runStream(harness, [`data: ${sseError("The model 'gpt-5' is at capacity. Please try a different model.")}\n`]);
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "model_at_capacity");
  assert.match(lifecycle.upstreamErrorMessage, /at capacity/);
  assert.ok(eventNames(output).includes("response.failed"));
  assert.ok(!eventNames(output).includes("response.completed"));
  assert.ok(!output.includes("data: [DONE]"));
}

async function testInsufficientQuotaError(harness) {
  const { lifecycle } = await runStream(harness, [`data: ${sseError("You exceeded your current quota, please check your plan and billing details.")}\n`]);
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "insufficient_quota");
  assert.match(lifecycle.upstreamErrorMessage, /quota/);
}

async function testGenericApiError(harness) {
  const { lifecycle } = await runStream(harness, [`data: ${sseError("The upstream had an internal error.")}\n`]);
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "upstream_api_error");
  assert.strictEqual(lifecycle.upstreamErrorMessage, "The upstream had an internal error.");
}

async function testMidStreamError(harness) {
  const payload = chatChunk({ content: "partial" });
  const { lifecycle } = await runStream(harness, [`data: ${payload}\n`, `data: ${sseError("The upstream is currently busy. Please retry later.")}\n`]);
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "model_at_capacity");
  assert.strictEqual(lifecycle.fullContent, "partial");
}

function testHttpErrorBodyClassificationAndSanitization(harness) {
  const body = JSON.stringify({
    error: {
      message: "Selected model is at capacity. Please try a different model. authorization=Bearer sk-live-secret-token",
    },
  });
  const message = harness.extractUpstreamErrorMessage(body);
  assert.match(message, /at capacity/i);
  assert.ok(!message.includes("sk-live-secret-token"));
  assert.match(message, /authorization=\[redacted\]/i);
  assert.strictEqual(harness.classifyUpstreamErrorMessage(message), "model_at_capacity");
  assert.strictEqual(harness.classifyUpstreamErrorMessage("You exceeded your current quota"), "insufficient_quota");
}

async function testHttpErrorCapture(harness) {
  const { EventEmitter } = require("events");
  class FakeResponse extends EventEmitter {
    constructor() {
      super();
      this.destroyed = false;
    }
    resume() {}
    destroy() {
      this.destroyed = true;
      this.emit("close");
    }
  }
  const response = new FakeResponse();
  const captured = new Promise(resolve => harness.captureUpstreamErrorMessage(response, resolve));
  response.emit("data", Buffer.from(JSON.stringify({ error: { message: "Selected model is at capacity" } })));
  response.emit("end");
  assert.strictEqual(await captured, "Selected model is at capacity");
  assert.strictEqual(response.destroyed, true);
}

function loadPriorityHarness(proxyDir, forwardResults) {
  const source = fs.readFileSync(path.join(proxyDir, "proxy.js"), "utf8");
  const start = source.indexOf("function forwardWithPriority(");
  const end = source.indexOf("\n// --- WebSocket ---", start);
  assert.ok(start >= 0 && end > start, "forwardWithPriority must be present");
  const downstreamEvents = [];
  let picked = 0;
  const accounts = [
    { status: "active", group: "A", url: "https://one.example" },
    { status: "active", group: "A", url: "https://two.example" },
  ];
  const forwardWithPriority = new Function(
    "accounts", "pickKey", "inCooldown", "forwardRequest", "enqueueRequest", "addDownstreamTerminalLog", "console",
    `${source.slice(start, end)}; return forwardWithPriority;`
  )(
    accounts,
    () => picked++,
    () => false,
    (idx, method, headers, body, clientRes, pathname, done) => done(forwardResults[idx]),
    () => { throw new Error("unexpected queue"); },
    (idx, options) => downstreamEvents.push({ idx, options }),
    { log() {}, error() {} }
  );
  return { forwardWithPriority, downstreamEvents };
}

function createClientResponse() {
  return {
    destroyed: false,
    writableEnded: false,
    headersSent: false,
    statusCode: 0,
    body: "",
    writeHead(status) { this.statusCode = status; this.headersSent = true; },
    end(body) { this.body = String(body || ""); this.writableEnded = true; },
  };
}

function testFinalDownstreamFailureOnly(proxyDir) {
  const firstFailure = { switched: true, idx: 0, code: 429, reason: "model_at_capacity", errorMessage: "Selected model is at capacity", source: "upstream_http_error" };
  const recovered = loadPriorityHarness(proxyDir, [firstFailure, { switched: false, idx: 1 }]);
  const recoveredClient = createClientResponse();
  recovered.forwardWithPriority("POST", {}, Buffer.from(JSON.stringify({ model: "gpt-test" })), recoveredClient, "/v1/responses", null, "A");
  assert.strictEqual(recovered.downstreamEvents.length, 0, "successful fallback must not create a downstream failure event");
  assert.strictEqual(recoveredClient.headersSent, false, "successful fallback owns the response stream");

  const finalFailure = loadPriorityHarness(proxyDir, [firstFailure, { ...firstFailure, idx: 1, code: 503, reason: "model_at_capacity" }]);
  const failedClient = createClientResponse();
  finalFailure.forwardWithPriority("POST", {}, Buffer.from(JSON.stringify({ model: "gpt-test" })), failedClient, "/v1/responses", null, "A");
  assert.strictEqual(finalFailure.downstreamEvents.length, 1, "only the final failure must create a downstream event");
  assert.strictEqual(finalFailure.downstreamEvents[0].idx, 1);
  assert.strictEqual(finalFailure.downstreamEvents[0].options.reason, "model_at_capacity");
  assert.strictEqual(failedClient.statusCode, 502);
}

function testRecordStreamOutcomeAggregation() {
  const source = fs.readFileSync(path.join(__dirname, "proxy.js"), "utf8");
  const start = source.indexOf("function recordStreamOutcome(");
  const end = source.indexOf("\nfunction markSuccess(", start);
  assert.ok(start >= 0 && end > start, "recordStreamOutcome must be present");
  const stats = { hourly: {} };
  const saveCalls = [];
  const scheduledSaves = [];
  let broadcasts = 0;
  const recordStreamOutcome = new Function("getKeyState", "today", "saveState", "scheduleStateSave", "broadcastStatus", `let stateBucketAddsSinceCompaction = 0; const invalidateStateStatusBucketCache = () => {}; ${source.slice(start, end)}; return recordStreamOutcome;`)(
    () => ({ stats }),
    () => "2026-07-31",
    force => { saveCalls.push(force); },
    () => { scheduledSaves.push("scheduled"); },
    () => { broadcasts++; }
  );
  recordStreamOutcome(0, "model_at_capacity");
  recordStreamOutcome(0, "model_at_capacity");
  recordStreamOutcome(0, "upstream_api_error", true);
  recordStreamOutcome(0, "upstream_done", true);
  const hk = "2026-07-31-" + String(new Date().getHours()).padStart(2, "0");
  assert.deepStrictEqual(stats.hourly[hk].streamOutcomes, { model_at_capacity: 2, upstream_api_error: 1, upstream_done: 1 });
  assert.deepStrictEqual(saveCalls, [true]);
  assert.deepStrictEqual(scheduledSaves, ["scheduled"]);
  assert.strictEqual(broadcasts, 2);
}

function testDownstreamTrendByClientApp(proxyDir) {
  const source = fs.readFileSync(path.join(proxyDir, "proxy.js"), "utf8");
  assert.match(source, /const sortedClients=Object\.keys\(allClients\)\.sort\(\(a,b\)=>allClients\[b\]-allClients\[a\]\);/);
  assert.match(source, /clientColorMap\[c\]=modelColors\[i%modelColors\.length\];/);
  assert.match(source, /const topClients=sortedClients\.slice\(0,8\);/);
  assert.match(source, /lines\.push\("  "\+c\+": "\+cv\+"次"\);/);
  assert.match(source, /for\(const c of sortedClients\)\{if\(!topClients\.includes\(c\)\)otherTotal\+=allClients\[c\]\|\|0;\}/);
  assert.doesNotMatch(source, /const kdata=data\.find\(item=>String\(item&&item\.idx\)===String\(ki\)\);/);
  assert.doesNotMatch(source, /const dOrder=\[/);
}

async function main() {
  const harness = loadStreamHarness(__dirname);
  await testCompletedStream(harness);
  await testDoneWithoutTrailingNewline(harness);
  await testEofWithoutDoneFails(harness);
  await testEmptyStreamFails(harness);
  await testToolCallCompletion(harness);
  testClientCancellation(harness);
  await testModelAtCapacityError(harness);
  await testInsufficientQuotaError(harness);
  await testGenericApiError(harness);
  await testMidStreamError(harness);
  testHttpErrorBodyClassificationAndSanitization(harness);
  await testHttpErrorCapture(harness);
  testFinalDownstreamFailureOnly(__dirname);
  testRecordStreamOutcomeAggregation();
  testDownstreamTrendByClientApp(__dirname);
  console.log("stream lifecycle: PASS");
}

main().catch(error => {
  console.error(`[stream-lifecycle-test] ${error.stack || error.message}`);
  process.exitCode = 1;
});
