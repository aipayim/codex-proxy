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
  const harness = ";globalThis.__streamLifecycleTest={createResponsesLifecycle,createChatToResponsesStream};";
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

async function main() {
  const harness = loadStreamHarness(__dirname);
  await testCompletedStream(harness);
  await testDoneWithoutTrailingNewline(harness);
  await testEofWithoutDoneFails(harness);
  await testEmptyStreamFails(harness);
  await testToolCallCompletion(harness);
  testClientCancellation(harness);
  console.log("stream lifecycle: PASS");
}

main().catch(error => {
  console.error(`[stream-lifecycle-test] ${error.stack || error.message}`);
  process.exitCode = 1;
});
