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
  const harness = ";globalThis.__streamLifecycleTest={createResponsesLifecycle,createChatToResponsesStream,createNativeResponsesTerminalProbe,createMessagesLifecycle,createChatLifecycle,createChatToMessagesStream,createMessagesToChatStream,normalizeResponsesTimeout,normalizeResponsesStreamConfig,sanitizeUpstreamErrorMessage,extractUpstreamErrorMessage,classifyUpstreamErrorMessage,captureUpstreamErrorMessage};";
  vm.runInNewContext(source.slice(0, cutoff) + harness, sandbox, { filename: "proxy.js", timeout: 5000 });
  return sandbox.__streamLifecycleTest;
}

function eventNames(output) {
  return [...output.matchAll(/^event: ([^\n]+)$/gm)].map(match => match[1]);
}

function eventPayloads(output, name) {
  const escapedName = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return [...output.matchAll(new RegExp(`^event: ${escapedName}\\ndata: ([^\\n]+)$`, "gm"))]
    .map(match => JSON.parse(match[1]));
}

function chatPayloads(output) {
  return [...output.matchAll(/^data: (\{[^\n]+\})$/gm)].map(match => JSON.parse(match[1]));
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

function createNativeResponsesStream(harness, model = "test-model") {
  const transform = harness.createNativeResponsesTerminalProbe(model);
  return { lifecycle: transform._lifecycle, transform };
}

function runNativeResponsesStream(harness, chunks, model = "test-model") {
  return new Promise((resolve, reject) => {
    const { lifecycle, transform } = createNativeResponsesStream(harness, model);
    const output = [];
    transform.on("data", chunk => { output.push(Buffer.from(chunk)); });
    transform.once("error", reject);
    transform.once("end", () => resolve({ lifecycle, output: Buffer.concat(output) }));
    for (const chunk of chunks) transform.write(chunk);
    transform.end();
  });
}

function createChatToMessagesStream(harness, model = "test-model") {
  const lifecycle = harness.createMessagesLifecycle(model);
  const transform = harness.createChatToMessagesStream(lifecycle);
  return { lifecycle, transform };
}

function runChatToMessagesStream(harness, chunks, model = "test-model") {
  return new Promise((resolve, reject) => {
    const { lifecycle, transform } = createChatToMessagesStream(harness, model);
    let output = "";
    transform.on("data", chunk => { output += chunk.toString(); });
    transform.once("error", reject);
    transform.once("end", () => resolve({ lifecycle, output }));
    for (const chunk of chunks) transform.write(chunk);
    transform.end();
  });
}

function createMessagesToChatStream(harness, model = "test-model") {
  const lifecycle = harness.createChatLifecycle(model);
  const transform = harness.createMessagesToChatStream(lifecycle);
  return { lifecycle, transform };
}

function runMessagesToChatStream(harness, chunks, model = "test-model") {
  return new Promise((resolve, reject) => {
    const { lifecycle, transform } = createMessagesToChatStream(harness, model);
    let output = "";
    transform.on("data", chunk => { output += chunk.toString(); });
    transform.once("error", reject);
    transform.once("end", () => resolve({ lifecycle, output }));
    for (const chunk of chunks) transform.write(chunk);
    transform.end();
  });
}

function nativeResponseEvent(type, response, extra = {}) {
  return `event: ${type}\ndata: ${JSON.stringify({ type, response, ...extra })}\n\n`;
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

function messagesEvent(type, payload = {}) {
  return `event: ${type}\ndata: ${JSON.stringify({ type, ...payload })}\n\n`;
}

function doneCount(output) {
  return (output.match(/^data: \[DONE\]$/gm) || []).length;
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

async function testNativeResponsesCompletedPassthrough(harness) {
  const response = { id: "resp_native_complete", object: "response", created_at: 1, model: "native-model", status: "completed" };
  const raw = nativeResponseEvent("response.created", { ...response, status: "in_progress" }) + nativeResponseEvent("response.completed", response);
  const { lifecycle, output } = await runNativeResponsesStream(harness, [Buffer.from(raw)]);
  assert.strictEqual(output.toString("utf8"), raw, "completed native stream must remain byte-for-byte unchanged");
  assert.strictEqual(lifecycle.responseId, "resp_native_complete");
  assert.strictEqual(lifecycle.terminalKind, "completed");
  assert.strictEqual(lifecycle.terminalReason, "upstream_done");
  assert.strictEqual(lifecycle.terminalSource, "native_responses_sse");
  assert.strictEqual(eventNames(output.toString("utf8")).filter(name => name === "response.completed").length, 1);
}

async function testNativeResponsesEofAddsFailedTerminal(harness) {
  const partial = nativeResponseEvent("response.created", { id: "resp_native_partial", object: "response", created_at: 1, model: "native-model", status: "in_progress" }) +
    "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n";
  const { lifecycle, output } = await runNativeResponsesStream(harness, [Buffer.from(partial)]);
  const text = output.toString("utf8");
  assert.ok(text.startsWith(partial), "native prefix must be preserved before a synthetic terminal");
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "upstream_eof_without_completed");
  assert.strictEqual(lifecycle.terminalSource, "native_responses_terminal_guard");
  assert.strictEqual(eventNames(text).filter(name => name === "response.failed").length, 1);
  assert.ok(!text.includes("data: [DONE]"));
  assert.ok(!eventNames(text).includes("response.completed"));
}

async function testNativeResponsesExplicitFailureIsNotDuplicated(harness) {
  const response = {
    id: "resp_native_failed",
    object: "response",
    created_at: 1,
    model: "native-model",
    status: "failed",
    error: { code: "server_error", message: "Selected model is at capacity. Please try a different model." },
  };
  const raw = nativeResponseEvent("response.failed", response);
  const { lifecycle, output } = await runNativeResponsesStream(harness, [Buffer.from(raw)]);
  assert.strictEqual(output.toString("utf8"), raw);
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "model_at_capacity");
  assert.strictEqual(eventNames(output.toString("utf8")).filter(name => name === "response.failed").length, 1);
}

async function testNativeResponsesIncompleteIsNotDuplicated(harness) {
  const response = {
    id: "resp_native_incomplete",
    object: "response",
    created_at: 1,
    model: "native-model",
    status: "incomplete",
    incomplete_details: { reason: "max_output_tokens" },
  };
  const raw = nativeResponseEvent("response.incomplete", response);
  const { lifecycle, output } = await runNativeResponsesStream(harness, [Buffer.from(raw)]);
  assert.strictEqual(output.toString("utf8"), raw);
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "upstream_incomplete");
  assert.ok(!eventNames(output.toString("utf8")).includes("response.failed"));
}

async function testNativeResponsesSplitUtf8AndEventFrames(harness) {
  const response = { id: "resp_native_utf8", object: "response", created_at: 1, model: "native-model", status: "completed" };
  const raw = nativeResponseEvent("response.created", { ...response, status: "in_progress" }) +
    "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"中文\"}\n\n" +
    nativeResponseEvent("response.completed", response);
  const bytes = Buffer.from(raw);
  const chinese = bytes.indexOf(Buffer.from("中"));
  const chunks = [bytes.subarray(0, chinese + 1), bytes.subarray(chinese + 1, chinese + 2), bytes.subarray(chinese + 2, chinese + 7), bytes.subarray(chinese + 7)];
  const { lifecycle, output } = await runNativeResponsesStream(harness, chunks);
  assert.strictEqual(output.toString("utf8"), raw);
  assert.strictEqual(lifecycle.terminalKind, "completed");
  assert.strictEqual(lifecycle.responseId, "resp_native_utf8");
}

async function testNativeResponsesErrorGetsSingleFailedTerminal(harness) {
  const raw = "event: error\ndata: {\"error\":{\"message\":\"Selected model is at capacity. Please try a different model.\"}}\n\n";
  const { lifecycle, output } = await runNativeResponsesStream(harness, [Buffer.from(raw)]);
  const text = output.toString("utf8");
  assert.ok(text.startsWith(raw));
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "model_at_capacity");
  assert.strictEqual(eventNames(text).filter(name => name === "response.failed").length, 1);
  assert.ok(!eventNames(text).includes("response.completed"));
}

async function testChatToMessagesCompleted(harness) {
  const payload = chatChunk({ content: "hello" }, { usage: { prompt_tokens: 3, completion_tokens: 2 } });
  const { lifecycle, output } = await runChatToMessagesStream(harness, [`data: ${payload}\n`, "data: [DONE]\n"]);
  assert.strictEqual(lifecycle.terminalKind, "completed");
  assert.strictEqual(lifecycle.terminalReason, "upstream_done");
  assert.strictEqual(lifecycle.sawDone, true);
  assert.strictEqual(lifecycle.fullContent, "hello");
  assert.strictEqual(eventNames(output).filter(name => name === "message_stop").length, 1);
  assert.strictEqual(eventNames(output).filter(name => name === "error").length, 0);
  assert.strictEqual(eventNames(output).filter(name => name === "content_block_stop").length, 1);
}

async function testChatToMessagesDoneWithoutContent(harness) {
  const { lifecycle, output } = await runChatToMessagesStream(harness, ["data: [DONE]"]);
  assert.strictEqual(lifecycle.terminalKind, "completed");
  assert.deepStrictEqual(eventNames(output), ["message_start", "message_delta", "message_stop"]);
}

async function testChatToMessagesReasoningOnlyClosesOnlyOpenedBlock(harness) {
  const payload = chatChunk({ reasoning_content: "reasoning" });
  const { lifecycle, output } = await runChatToMessagesStream(harness, [`data: ${payload}\n`, "data: [DONE]\n"]);
  assert.strictEqual(lifecycle.terminalKind, "completed");
  assert.deepStrictEqual(eventPayloads(output, "content_block_start").map(event => event.index), [0]);
  assert.deepStrictEqual(eventPayloads(output, "content_block_stop").map(event => event.index), [0]);
}

async function testChatToMessagesToolBlocksUseContiguousIndices(harness) {
  const payload = chatChunk({ tool_calls: [
    { index: 0, id: "tool_a", function: { name: "alpha", arguments: "{\"a\":1}" } },
    { index: 1, id: "tool_b", function: { name: "beta", arguments: "{\"b\":2}" } },
  ] }, { finishReason: "tool_calls" });
  const { lifecycle, output } = await runChatToMessagesStream(harness, [`data: ${payload}\n`, "data: [DONE]\n"]);
  assert.strictEqual(lifecycle.terminalKind, "completed");
  assert.deepStrictEqual(eventPayloads(output, "content_block_start").map(event => event.index), [0, 1]);
  assert.deepStrictEqual(eventPayloads(output, "content_block_stop").map(event => event.index), [0, 1]);
}

async function testChatToMessagesEofFailsWithoutMessageStop(harness) {
  const payload = chatChunk({ content: "partial" });
  const { lifecycle, output } = await runChatToMessagesStream(harness, [`data: ${payload}\n`]);
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "upstream_eof_without_done");
  assert.strictEqual(eventNames(output).filter(name => name === "error").length, 1);
  assert.strictEqual(eventNames(output).filter(name => name === "message_stop").length, 0);
  assert.match(output, /Upstream chat stream ended before its \[DONE\] terminal/);
}

async function testChatToMessagesExplicitErrorFails(harness) {
  const raw = "data: {\"error\":{\"message\":\"Selected model is at capacity. Please try a different model.\"}}\n\n";
  const { lifecycle, output } = await runChatToMessagesStream(harness, [raw]);
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "model_at_capacity");
  assert.strictEqual(eventNames(output).filter(name => name === "error").length, 1);
  assert.strictEqual(eventNames(output).filter(name => name === "message_stop").length, 0);
}

async function testChatToMessagesSplitUtf8AndDoneTail(harness) {
  const raw = `data: ${chatChunk({ content: "中文" })}\n\ndata: [DONE]`;
  const bytes = Buffer.from(raw);
  const chinese = bytes.indexOf(Buffer.from("中"));
  const chunks = [bytes.subarray(0, chinese + 1), bytes.subarray(chinese + 1, chinese + 2), bytes.subarray(chinese + 2, chinese + 7), bytes.subarray(chinese + 7)];
  const { lifecycle, output } = await runChatToMessagesStream(harness, chunks);
  assert.strictEqual(lifecycle.terminalKind, "completed");
  assert.strictEqual(lifecycle.fullContent, "中文");
  assert.strictEqual(eventNames(output).filter(name => name === "message_stop").length, 1);
  assert.strictEqual(eventNames(output).filter(name => name === "error").length, 0);
}

function testChatToMessagesClientCancellation(harness) {
  const { lifecycle, transform } = createChatToMessagesStream(harness);
  let output = "";
  transform.on("data", chunk => { output += chunk.toString(); });
  assert.strictEqual(lifecycle.noteClientCancelled("client_disconnect"), true);
  assert.strictEqual(lifecycle.terminalKind, "cancelled");
  assert.strictEqual(eventNames(output).length, 0);
  transform.destroy();
}

async function testMessagesToChatCompletedOnce(harness) {
  const raw = messagesEvent("message_start", { message: { model: "anthropic-test", usage: { input_tokens: 5 } } }) +
    messagesEvent("content_block_delta", { index: 0, delta: { type: "text_delta", text: "hello" } }) +
    messagesEvent("message_delta", { delta: { stop_reason: "end_turn" }, usage: { output_tokens: 2 } }) +
    'data: {"type":"message_stop"}';
  const { lifecycle, output } = await runMessagesToChatStream(harness, [raw]);
  assert.strictEqual(lifecycle.terminalKind, "completed");
  assert.strictEqual(lifecycle.terminalReason, "upstream_done");
  assert.strictEqual(doneCount(output), 1);
  assert.match(output, /\"content\":\"hello\"/);
  assert.ok(!output.includes('"error"'));
}

async function testMessagesToChatEofFailsWithoutDone(harness) {
  const raw = messagesEvent("message_start", { message: { model: "anthropic-test", usage: { input_tokens: 5 } } }) +
    messagesEvent("content_block_delta", { index: 0, delta: { type: "text_delta", text: "partial" } });
  const { lifecycle, output } = await runMessagesToChatStream(harness, [raw]);
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "upstream_eof_without_done");
  assert.strictEqual(doneCount(output), 0);
  assert.match(output, /Upstream Messages stream ended before message_stop/);
}

async function testMessagesToChatExplicitErrorFails(harness) {
  const raw = messagesEvent("error", { error: { type: "overloaded_error", message: "Selected model is at capacity. Please try a different model." } });
  const { lifecycle, output } = await runMessagesToChatStream(harness, [raw]);
  assert.strictEqual(lifecycle.terminalKind, "failed");
  assert.strictEqual(lifecycle.terminalReason, "model_at_capacity");
  assert.strictEqual(doneCount(output), 0);
  assert.match(output, /\"error\"/);
}

async function testMessagesToChatSplitUtf8AndStopTail(harness) {
  const raw = messagesEvent("message_start", { message: { model: "anthropic-test", usage: { input_tokens: 1 } } }) +
    messagesEvent("content_block_delta", { index: 0, delta: { type: "text_delta", text: "中文" } }) +
    'data: {"type":"message_stop"}';
  const bytes = Buffer.from(raw);
  const chinese = bytes.indexOf(Buffer.from("中"));
  const chunks = [bytes.subarray(0, chinese + 1), bytes.subarray(chinese + 1, chinese + 2), bytes.subarray(chinese + 2, chinese + 7), bytes.subarray(chinese + 7)];
  const { lifecycle, output } = await runMessagesToChatStream(harness, chunks);
  assert.strictEqual(lifecycle.terminalKind, "completed");
  assert.strictEqual(lifecycle.fullContent, "中文");
  assert.strictEqual(doneCount(output), 1);
}

async function testMessagesToChatToolCallsKeepIdsAndIndices(harness) {
  const raw = messagesEvent("message_start", { message: { model: "anthropic-test", usage: { input_tokens: 1 } } }) +
    messagesEvent("content_block_start", { index: 0, content_block: { type: "tool_use", id: "tool_a", name: "alpha", input: {} } }) +
    messagesEvent("content_block_delta", { index: 0, delta: { type: "input_json_delta", partial_json: "{\"a\":1}" } }) +
    messagesEvent("content_block_start", { index: 2, content_block: { type: "tool_use", id: "tool_b", name: "beta", input: {} } }) +
    messagesEvent("content_block_delta", { index: 2, delta: { type: "input_json_delta", partial_json: "{\"b\":2}" } }) +
    'data: {"type":"message_stop"}';
  const { lifecycle, output } = await runMessagesToChatStream(harness, [raw]);
  assert.strictEqual(lifecycle.terminalKind, "completed");
  const toolDeltas = chatPayloads(output)
    .map(payload => payload.choices && payload.choices[0] && payload.choices[0].delta && payload.choices[0].delta.tool_calls)
    .filter(Boolean)
    .flat();
  assert.deepStrictEqual(toolDeltas.map(call => call.index), [0, 0, 1, 1]);
  assert.deepStrictEqual(toolDeltas.filter(call => call.id).map(call => ({ id: call.id, name: call.function.name, type: call.type })), [
    { id: "tool_a", name: "alpha", type: "function" },
    { id: "tool_b", name: "beta", type: "function" },
  ]);
  assert.strictEqual(doneCount(output), 1);
}

function testNativeResponsesClientCancellation(harness) {
  const { lifecycle, transform } = createNativeResponsesStream(harness);
  let output = "";
  transform.on("data", chunk => { output += chunk.toString(); });
  assert.strictEqual(lifecycle.noteClientCancelled("client_disconnect"), true);
  assert.strictEqual(lifecycle.terminalKind, "cancelled");
  assert.ok(!output.includes("response.failed"));
  transform.destroy();
}

function testResponsesTimeoutConfigNormalization(harness) {
  assert.strictEqual(harness.normalizeResponsesTimeout(0, 1), 0);
  assert.strictEqual(harness.normalizeResponsesTimeout(30000, 1), 60000);
  assert.strictEqual(harness.normalizeResponsesTimeout(999999999, 1), 86400000);
  const config = harness.normalizeResponsesStreamConfig({ responsesStreamLifetime: 0, responsesIdleTimeout: 0 });
  assert.strictEqual(config.responsesStreamLifetime, 0);
  assert.strictEqual(config.responsesIdleTimeout, 0);
  const defaults = harness.normalizeResponsesStreamConfig({});
  assert.strictEqual(defaults.responsesIdleTimeout, 5400000, "idle timeout default must be 90 minutes");
  assert.strictEqual(defaults.responsesNoProgressTimeout, 900000, "no-progress watchdog default must be 15 minutes");
  const zero = harness.normalizeResponsesStreamConfig({ responsesNoProgressTimeout: 0 });
  assert.strictEqual(zero.responsesNoProgressTimeout, 900000, "no-progress watchdog cannot be disabled to 0; falls back to default");
}

function testNoProgressWatchdogContracts(proxyDir) {
  const source = fs.readFileSync(path.join(proxyDir, "proxy.js"), "utf8");
  assert.match(source, /const RESPONSES_IDLE_TIMEOUT_DEFAULT_MS = 90 \* 60 \* 1000;/, "idle default must be 90 minutes, not 90 hours");
  assert.match(source, /RESPONSES_NO_PROGRESS_DEFAULT_MS = 15 \* 60 \* 1000/, "watchdog default must be 15 minutes");
  assert.match(source, /const lastActivity = transform\.lastActivity \|\| reqStart/, "watchdog must measure time since the last real upstream chunk");
  assert.match(source, /terminateAttachedStream\("no_progress_timeout"\)/, "watchdog must force a failure terminal");
  assert.match(source, /"no_progress_timeout",\n\s*"client_disconnect"/, "no_progress_timeout must be a recognized terminal reason");
  assert.match(source, /progressTimer\) \{ clearInterval\(progressTimer\); progressTimer = null; \}/, "watchdog timer must be cleared on cleanup");
}

function testNativeResponsesRouteContract(proxyDir) {
  const source = fs.readFileSync(path.join(proxyDir, "proxy.js"), "utf8");
  assert.match(source, /pathname === "\/responses" && req\.method === "POST" && body/);
  assert.match(source, /requestBody && requestBody\.stream === true/);
  assert.match(source, /nativeResponsesProbe = createNativeResponsesTerminalProbe\(requestBody\.model \|\| ""\)/);
  assert.match(source, /forwardWithPriority\(req\.method, req\.headers, body, res, pathname, nativeResponsesProbe, groupName\)/);
  assert.match(source, /const codingProtocolStream = lifecycle && \(lifecycle\.protocol === "responses" \|\| lifecycle\.protocol === "messages"\);/);
  assert.match(source, /const streamLifetime = codingProtocolStream \? RESPONSES_STREAM_LIFETIME : STREAM_LIFETIME/);
  assert.match(source, /const upstreamIdleTimeout = codingProtocolStream \? RESPONSES_IDLE_TIMEOUT : TIMEOUT/);
  assert.match(source, /if \(extraTransform\) reqHeaders\["accept-encoding"\] = "identity";/);
  assert.match(source, /delete safeHeaders\["content-length"\];/);
  assert.match(source, /apiRes\.unpipe\(transform\);\n\s*apiRes\.destroy\(\);/);
  // The Messages route now goes through the unified protocol-aware forwarder,
  // which still builds the Messages lifecycle + Chat→Messages stream converter.
  assert.match(source, /forwardProtocolAware\("messages", req\.method, req\.headers, body, res, pathname, groupName/);
  assert.match(source, /const lifecycle = createMessagesLifecycle\(model \|\| cb\.model \|\| ""\);/);
  assert.match(source, /createChatToMessagesStream\(lifecycle\)/);
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
  assert.strictEqual(harness.classifyUpstreamErrorMessage("Our servers are currently overloaded. Please try again later."), "model_at_capacity");
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
  await testNativeResponsesCompletedPassthrough(harness);
  await testNativeResponsesEofAddsFailedTerminal(harness);
  await testNativeResponsesExplicitFailureIsNotDuplicated(harness);
  await testNativeResponsesIncompleteIsNotDuplicated(harness);
  await testNativeResponsesSplitUtf8AndEventFrames(harness);
  await testNativeResponsesErrorGetsSingleFailedTerminal(harness);
  testNativeResponsesClientCancellation(harness);
  await testChatToMessagesCompleted(harness);
  await testChatToMessagesDoneWithoutContent(harness);
  await testChatToMessagesReasoningOnlyClosesOnlyOpenedBlock(harness);
  await testChatToMessagesToolBlocksUseContiguousIndices(harness);
  await testChatToMessagesEofFailsWithoutMessageStop(harness);
  await testChatToMessagesExplicitErrorFails(harness);
  await testChatToMessagesSplitUtf8AndDoneTail(harness);
  testChatToMessagesClientCancellation(harness);
  await testMessagesToChatCompletedOnce(harness);
  await testMessagesToChatEofFailsWithoutDone(harness);
  await testMessagesToChatExplicitErrorFails(harness);
  await testMessagesToChatSplitUtf8AndStopTail(harness);
  await testMessagesToChatToolCallsKeepIdsAndIndices(harness);
  testResponsesTimeoutConfigNormalization(harness);
  testNativeResponsesRouteContract(__dirname);
  testNoProgressWatchdogContracts(__dirname);
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
