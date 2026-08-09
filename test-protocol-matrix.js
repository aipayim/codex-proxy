#!/usr/bin/env node
"use strict";

// Full protocol adaptation matrix tests:
//   downstream (chat / messages / responses) x upstream (chat / messages / responses)
// Covers request conversion, non-streaming response conversion, streaming SSE
// conversion, native passthrough probes, dynamic protocol detection, and the
// any-model auto-adaptation fallback.

const assert = require("assert");
const fs = require("fs");
const path = require("path");
const vm = require("vm");
const { EventEmitter } = require("events");

function makeHttpsMock(routes) {
  const calls = [];
  const httpsMock = {
    request(arg1, arg2, arg3) {
      let opts;
      let cb;
      if (typeof arg2 === "function") { opts = arg1; cb = arg2; }
      else { opts = arg2; cb = arg3; }
      const call = { opts };
      calls.push(call);
      const req = new EventEmitter();
      const res = new EventEmitter();
      req.setTimeout = () => req;
      req.destroy = (err) => setTimeout(() => req.emit("error", err), 0);
      req.write = (chunk) => { call.body = chunk; };
      req.end = () => {
        const route = routes(opts);
        if (route.timeout) { req.destroy(new Error("mock timeout")); return; }
        setTimeout(() => {
          cb(res);
          res.statusCode = route.status;
          res.headers = route.headers || {};
          const raw = Buffer.from(route.body == null ? "" : (typeof route.body === "string" ? route.body : JSON.stringify(route.body)), "utf8");
          res.emit("data", raw);
          res.emit("end");
        }, 0);
      };
      return req;
    },
  };
  httpsMock.__calls = calls;
  return httpsMock;
}

function loadHarness(proxyDir) {
  const source = fs.readFileSync(path.join(proxyDir, "proxy.js"), "utf8");
  const cutoff = source.lastIndexOf("// Start servers");
  if (cutoff < 0) throw new Error("proxy startup marker not found");

  const memoryFiles = new Map();
  const fsMock = {
    ...fs,
    promises: { ...fs.promises },
    readFileSync(file, encoding) {
      if (memoryFiles.has(file)) return memoryFiles.get(file);
      return fs.readFileSync(file, encoding);
    },
    writeFileSync(file, data) { memoryFiles.set(file, String(data)); },
    renameSync() {},
    unlinkSync() {},
    mkdirSync() {},
    statSync() { return { isDirectory: () => true }; },
  };

  let routes = () => ({ status: 500, body: { error: "no route" } });
  let httpsMock = makeHttpsMock(routes);
  const discHttp = {
    setRoutes(fn) { routes = fn; httpsMock = makeHttpsMock(fn); },
    calls() { return httpsMock.__calls; },
  };
  const httpsDelegator = { request: (a, b, c) => httpsMock.request(a, b, c) };
  const sandbox = {
    require: moduleName => {
      if (moduleName === "fs") return fsMock;
      if (moduleName === "https") return httpsDelegator;
      return require(moduleName);
    },
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
    ;globalThis.__protoTest = {
      config, accounts, upstreamModelsCache, upstreamProtocolCache,
      isMessagesNative, isResponsesNative,
      upstreamProtocolForUrl, upstreamProtocolFor,
      classifyUpstreamCapability, resolveUpstreamModel, pickNearestModel,
      probeUpstreamModels, ensureUpstreamCapability, getCachedCapability,
      refreshUpstreamCapabilities,
      buildForwardPlan,
      messagesToChatRequest, chatToMessagesRequest, chatToResponsesRequest,
      responsesToChatRequest, messagesToResponsesRequest, responsesToMessagesRequest,
      chatToMessagesResponse, messagesToChatResponse, chatToResponsesResponse,
      responsesToChatResponse, messagesToResponsesResponse, responsesToMessagesResponse,
      createMessagesLifecycle, createResponsesLifecycle, createChatLifecycle,
      createChatToMessagesStream, createMessagesToChatStream, createChatToResponsesStream,
      createResponsesToChatStream, createMessagesToResponsesStream, createResponsesToMessagesStream,
      createNativeMessagesTerminalProbe, createNativeResponsesTerminalProbe,
      createNonStreamingBodyConverter,
      setAccounts: (arr) => { accounts = arr; upstreamModelsCache.clear(); },
      clearCache: () => upstreamModelsCache.clear(),
      clearProtocolCache: () => upstreamProtocolCache.clear(),
    };
  `;
  sandbox.discHttp = discHttp;
  sandbox.memoryFiles = memoryFiles;
  vm.runInNewContext(source.slice(0, cutoff) + harness, sandbox, { filename: "proxy.js", timeout: 8000 });
  const result = sandbox.__protoTest;
  result.discHttp = discHttp;
  return result;
}

function wire(lifecycle, transform) {
  if (lifecycle) lifecycle._transform = transform;
  if (transform && lifecycle) transform._lifecycle = lifecycle;
  return transform;
}

function runTransform(transform, chunks) {
  return new Promise((resolve, reject) => {
    let out = "";
    transform.on("data", c => { out += c.toString(); });
    transform.on("end", () => resolve(out));
    transform.on("error", reject);
    for (const c of chunks) transform.write(c);
    transform.end();
  });
}

function parseSse(out) {
  const events = [];
  for (const block of out.split(/\n\n/)) {
    let evt = "";
    const data = [];
    for (const line of block.split("\n")) {
      if (line.startsWith("event:")) evt = line.slice(6).trim();
      else if (line.startsWith("data:")) data.push(line.slice(5).trim());
    }
    if (data.length) events.push({ event: evt, data: data.join("\n") });
  }
  return events;
}

function chatChunk(delta, extra = {}) {
  return `data: ${JSON.stringify({ id: "c1", object: "chat.completion.chunk", model: "m", choices: [{ index: 0, delta, finish_reason: extra.finishReason || null }], ...(extra.usage ? { usage: extra.usage } : {}) })}\n\n`;
}
function msgEvent(event, payload) {
  return `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
}
function respEvent(event, payload) {
  return `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
}

const chatReq = {
  model: "gpt-5.6",
  messages: [
    { role: "system", content: "You are helpful." },
    { role: "user", content: "Hi" },
    { role: "assistant", content: null, tool_calls: [{ id: "call_1", type: "function", function: { name: "get_weather", arguments: '{"city":"bj"}' } }] },
    { role: "tool", tool_call_id: "call_1", content: "sunny" },
  ],
  tools: [{ type: "function", function: { name: "get_weather", description: "w", parameters: { type: "object" } } }],
  max_tokens: 100,
  temperature: 0.5,
  stream: true,
};

const msgsReq = {
  model: "claude-sonnet-4-5",
  system: "You are helpful.",
  messages: [
    { role: "user", content: "Hi" },
    { role: "assistant", content: [{ type: "text", text: "ok" }, { type: "tool_use", id: "toolu_1", name: "get_weather", input: { city: "bj" } }] },
    { role: "user", content: [{ type: "tool_result", tool_use_id: "toolu_1", content: "sunny" }] },
  ],
  tools: [{ name: "get_weather", description: "w", input_schema: { type: "object" } }],
  max_tokens: 200,
  stream: true,
};

const respReq = {
  model: "gpt-5.6",
  instructions: "You are helpful.",
  input: [
    { type: "message", role: "user", content: [{ type: "input_text", text: "Hi" }] },
    { type: "function_call", call_id: "fc_1", name: "get_weather", arguments: '{"city":"bj"}' },
    { type: "function_call_output", call_id: "fc_1", output: "sunny" },
  ],
  tools: [{ type: "function", name: "get_weather", description: "w", parameters: { type: "object" } }],
  max_output_tokens: 100,
  stream: true,
};

function testProtocolIdentification(t) {
  t.setAccounts([
    { key: "k1", url: "https://api.anthropic.com", status: "active", group: "A" },
    { key: "k2", url: "https://api.openai.com/v1", status: "active", group: "A" },
    { key: "k3", url: "https://relay.example/v1", status: "active", group: "A" },
  ]);
  t.clearCache();
  assert.strictEqual(t.upstreamProtocolFor(0), "messages");
  assert.strictEqual(t.upstreamProtocolFor(1), "responses");
  assert.strictEqual(t.upstreamProtocolFor(2), "chat");
  // Cached dynamic protocol is used for unknown hosts only.
  t.upstreamModelsCache.set("https://relay.example/v1", { models: [], protocol: "messages", capability: "unknown", failed: true, fetchedAt: Date.now() });
  assert.strictEqual(t.upstreamProtocolFor(2), "messages");
  assert.strictEqual(t.upstreamProtocolFor(0), "messages", "static whitelist must win over cached protocol");
  console.log("matrix: upstream protocol identification (whitelist + cached dynamic): PASS");
}

function testDynamicProtocolDetection(t) {
  t.setAccounts([
    { key: "sk-a", url: "https://chat-relay.example/v1", status: "active", group: "A" },
    { key: "sk-b", url: "https://msg-relay.example/v1", status: "active", group: "A" },
    { key: "sk-c", url: "https://resp-relay.example/v1", status: "active", group: "A" },
  ]);
  t.clearCache();
  t.discHttp.setRoutes((opts) => {
    if (opts.hostname === "chat-relay.example") {
      if (opts.path === "/v1/models") return { status: 200, body: { object: "list", data: [{ id: "gpt-5.6" }] } };
      return { status: 404, body: {} };
    }
    if (opts.hostname === "msg-relay.example") {
      if (opts.path === "/v1/models") return { status: 404, body: {} };
      if (opts.path === "/v1/messages") return { status: 400, body: { error: "bad body" } };
      return { status: 404, body: {} };
    }
    if (opts.hostname === "resp-relay.example") {
      if (opts.path === "/v1/models") return { status: 404, body: {} };
      if (opts.path === "/v1/messages") return { status: 404, body: {} };
      if (opts.path === "/v1/responses") return { status: 200, body: { id: "r1" } };
      return { status: 404, body: {} };
    }
    return { status: 404, body: {} };
  });
  return t.probeUpstreamModels(0).then((cap0) => {
    assert.strictEqual(cap0.capability, "gpt");
    assert.strictEqual(cap0.protocol, "chat", "models 200 implies OpenAI-compatible chat");
    assert.strictEqual(t.upstreamProtocolFor(0), "chat");
    return t.probeUpstreamModels(1);
  }).then((cap1) => {
    assert.strictEqual(cap1.protocol, "messages");
    assert.strictEqual(t.upstreamProtocolFor(1), "messages");
    return t.probeUpstreamModels(2);
  }).then((cap2) => {
    assert.strictEqual(cap2.protocol, "responses");
    assert.strictEqual(t.upstreamProtocolFor(2), "responses");
    console.log("matrix: dynamic protocol detection (models fail → endpoint probes): PASS");
  });
}

function testRequestConverters(t) {
  // Chat → Messages (existing contract, still used)
  const cm = t.chatToMessagesRequest("", chatReq);
  assert.strictEqual(cm.model, "gpt-5.6");
  assert.ok(Array.isArray(cm.messages));
  const roles = cm.messages.map(m => m.role);
  assert.ok(roles.includes("assistant"));
  assert.strictEqual(cm.tools[0].name, "get_weather");
  assert.strictEqual(cm.max_tokens, 100);

  // Chat → Responses
  const cr = t.chatToResponsesRequest("", chatReq);
  assert.strictEqual(cr.instructions, "You are helpful.");
  assert.strictEqual(cr.max_output_tokens, 100);
  assert.strictEqual(cr.temperature, 0.5);
  const crTypes = cr.input.map(i => i.type);
  assert.strictEqual(JSON.stringify(crTypes), JSON.stringify(["message", "message", "function_call", "function_call_output"]));
  const fc = cr.input.find(i => i.type === "function_call");
  assert.strictEqual(fc.name, "get_weather");
  assert.strictEqual(fc.arguments, '{"city":"bj"}');
  assert.strictEqual(cr.tools[0].name, "get_weather");

  // Messages → Chat (existing contract)
  const mc = t.messagesToChatRequest("", msgsReq);
  assert.strictEqual(mc.model, "claude-sonnet-4-5");
  assert.strictEqual(mc.tools[0].function.name, "get_weather");
  assert.strictEqual(mc.max_tokens, 200);

  // Messages → Responses
  const mr = t.messagesToResponsesRequest("", msgsReq);
  assert.strictEqual(mr.instructions, "You are helpful.");
  assert.strictEqual(mr.max_output_tokens, 200);
  const mrTypes = mr.input.map(i => i.type);
  assert.strictEqual(JSON.stringify(mrTypes), JSON.stringify(["message", "message", "function_call", "function_call_output"]));
  const mfc = mr.input.find(i => i.type === "function_call");
  assert.strictEqual(mfc.name, "get_weather");
  assert.strictEqual(JSON.stringify(JSON.parse(mfc.arguments)), JSON.stringify({ city: "bj" }));

  // Responses → Chat (existing contract)
  const rc = t.responsesToChatRequest("", respReq);
  const rcSystem = rc.messages.find(m => m.role === "system");
  assert.strictEqual(rcSystem.content, "You are helpful.");
  assert.strictEqual(rc.max_tokens, 100);
  const rcAssistant = rc.messages.find(m => m.role === "assistant");
  assert.strictEqual(rcAssistant.tool_calls[0].function.name, "get_weather");

  // Responses → Messages
  const rm = t.responsesToMessagesRequest("", respReq);
  assert.strictEqual(rm.system, "You are helpful.");
  assert.strictEqual(rm.max_tokens, 100);
  const rmAssistant = rm.messages.find(m => m.role === "assistant");
  const rmToolUse = rmAssistant.content.find(c => c.type === "tool_use");
  assert.strictEqual(rmToolUse.name, "get_weather");
  assert.strictEqual(JSON.stringify(rmToolUse.input), JSON.stringify({ city: "bj" }));
  const rmResult = rm.messages.find(m => m.role === "user" && Array.isArray(m.content)).content.find(c => c.type === "tool_result");
  assert.strictEqual(rmResult.tool_use_id, "fc_1");
  console.log("matrix: request converters (all six directions): PASS");
}

function testNonStreamingResponseConverters(t) {
  const chatResp = {
    object: "chat.completion",
    model: "gpt-5.6",
    choices: [{ index: 0, message: { role: "assistant", content: "hi", reasoning_content: "think", tool_calls: [] }, finish_reason: "stop" }],
    usage: { prompt_tokens: 1, completion_tokens: 2 },
  };
  const msgsResp = {
    id: "msg_x", type: "message", role: "assistant", model: "claude-sonnet-4-5",
    content: [{ type: "thinking", thinking: "think" }, { type: "text", text: "hi" }, { type: "tool_use", id: "tu1", name: "get_weather", input: { city: "bj" } }],
    stop_reason: "tool_use",
    usage: { input_tokens: 3, output_tokens: 4 },
  };
  const responsesResp = {
    id: "resp_x", object: "response", status: "completed", model: "gpt-5.6",
    output: [
      { type: "reasoning", summary: "think", content: "think" },
      { type: "message", role: "assistant", content: [{ type: "output_text", text: "hi" }] },
      { type: "function_call", id: "fc_1", name: "get_weather", arguments: '{"city":"bj"}' },
    ],
    usage: { input_tokens: 5, output_tokens: 6 },
  };

  const cm = t.chatToMessagesResponse("", chatResp);
  assert.strictEqual(cm.content[0].type, "thinking");
  assert.strictEqual(cm.content[1].text, "hi");
  assert.strictEqual(cm.usage.input_tokens, 1);

  const mc = t.messagesToChatResponse("", msgsResp);
  assert.strictEqual(mc.choices[0].message.content, "hi");
  assert.strictEqual(mc.choices[0].finish_reason, "tool_calls");

  const cr = t.chatToResponsesResponse("", chatResp);
  assert.ok(cr.output[0].content[0].text.includes("hi"));

  const rc = t.responsesToChatResponse("", responsesResp);
  assert.ok(rc.choices[0].message.content.includes("hi"));
  assert.strictEqual(rc.choices[0].finish_reason, "tool_calls");
  assert.strictEqual(rc.choices[0].message.tool_calls[0].function.name, "get_weather");
  assert.strictEqual(rc.usage.prompt_tokens, 5);

  const mr = t.messagesToResponsesResponse("", msgsResp);
  const mrMessage = mr.output.find(o => o.type === "message");
  assert.strictEqual(mrMessage.content[0].text, "hi");
  const mrFc = mr.output.find(o => o.type === "function_call");
  assert.strictEqual(mrFc.name, "get_weather");
  assert.strictEqual(mr.usage.input_tokens, 3);

  const rm = t.responsesToMessagesResponse("", responsesResp);
  const rmText = rm.content.find(c => c.type === "text");
  assert.strictEqual(rmText.text, "hi");
  const rmTu = rm.content.find(c => c.type === "tool_use");
  assert.strictEqual(rmTu.name, "get_weather");
  assert.strictEqual(JSON.stringify(rmTu.input), JSON.stringify({ city: "bj" }));
  assert.strictEqual(rm.usage.input_tokens, 5);
  console.log("matrix: non-streaming response converters: PASS");
}

async function testStreamingConverters(t) {
  // Chat SSE → Messages SSE (existing)
  const lc1 = t.createMessagesLifecycle("claude-x");
  const tr1 = wire(lc1, t.createChatToMessagesStream(lc1));
  const out1 = parseSse(await runTransform(tr1, [
    chatChunk({ role: "assistant", content: "Hel" }),
    chatChunk({ content: "lo" }),
    chatChunk({}, { finishReason: "stop", usage: { prompt_tokens: 1, completion_tokens: 2 } }),
    "data: [DONE]\n\n",
  ]));
  const names1 = out1.map(e => e.event);
  assert.ok(names1.includes("message_start"));
  assert.ok(names1.includes("content_block_start"));
  assert.ok(names1.includes("message_delta"));
  assert.ok(names1.includes("message_stop"));
  assert.strictEqual(lc1.terminalKind, "completed");

  // Messages SSE → Chat SSE (existing)
  const lc2 = t.createChatLifecycle("gpt-x");
  const tr2 = wire(lc2, t.createMessagesToChatStream(lc2));
  const out2 = parseSse(await runTransform(tr2, [
    msgEvent("message_start", { type: "message_start", message: { id: "m1", type: "message", role: "assistant", content: [], model: "claude-x", stop_reason: null, usage: { input_tokens: 1, output_tokens: 0 } } }),
    msgEvent("content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }),
    msgEvent("content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "Hi" } }),
    msgEvent("message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 2 } }),
    msgEvent("message_stop", { type: "message_stop" }),
  ]));
  const chatOut2 = out2.filter(e => e.event === "" && e.data !== "[DONE]").map(e => JSON.parse(e.data));
  assert.ok(chatOut2.some(c => c.choices[0].delta.content === "Hi"));
  assert.ok(out2.some(e => e.data === "[DONE]"));
  assert.strictEqual(lc2.terminalKind, "completed");

  // Chat SSE → Responses SSE (existing)
  const lc3 = t.createResponsesLifecycle("gpt-x");
  const tr3 = wire(lc3, t.createChatToResponsesStream(lc3));
  const out3 = parseSse(await runTransform(tr3, [chatChunk({ content: "Hi" }), chatChunk({}, { finishReason: "stop" }), "data: [DONE]\n\n"]));
  const names3 = out3.map(e => e.event);
  assert.ok(names3.includes("response.created"));
  assert.ok(names3.includes("response.output_text.delta"));
  assert.ok(names3.includes("response.completed"));
  assert.strictEqual(lc3.terminalKind, "completed");

  // Responses SSE → Chat SSE (new)
  const lc4 = t.createChatLifecycle("gpt-x");
  const tr4 = wire(lc4, t.createResponsesToChatStream(lc4));
  const out4 = parseSse(await runTransform(tr4, [
    respEvent("response.created", { type: "response.created", response: { id: "r1", object: "response", created_at: 1, model: "gpt-x", status: "in_progress", output: [] } }),
    respEvent("response.output_text.delta", { type: "response.output_text.delta", delta: "Hel" }),
    respEvent("response.output_text.delta", { type: "response.output_text.delta", delta: "lo" }),
    respEvent("response.completed", { type: "response.completed", response: { id: "r1", object: "response", status: "completed", model: "gpt-x", output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text: "Hello" }] }], usage: { input_tokens: 3, output_tokens: 4 } } }),
  ]));
  const chatOut4 = out4.filter(e => e.event === "" && e.data !== "[DONE]").map(e => JSON.parse(e.data));
  const content4 = chatOut4.map(c => c.choices[0].delta.content).filter(Boolean).join("");
  assert.strictEqual(content4, "Hello");
  const finish4 = chatOut4.find(c => c.choices[0].finish_reason);
  assert.strictEqual(finish4.choices[0].finish_reason, "stop");
  const usage4 = chatOut4.find(c => c.usage);
  assert.strictEqual(usage4.usage.completion_tokens, 4);
  assert.ok(out4.some(e => e.data === "[DONE]"));
  assert.strictEqual(lc4.terminalKind, "completed");

  // Messages SSE → Responses SSE (new)
  const lc5 = t.createResponsesLifecycle("claude-x");
  const tr5 = wire(lc5, t.createMessagesToResponsesStream(lc5));
  const out5 = parseSse(await runTransform(tr5, [
    msgEvent("message_start", { type: "message_start", message: { id: "m1", type: "message", role: "assistant", content: [], model: "claude-x", stop_reason: null, usage: { input_tokens: 2, output_tokens: 0 } } }),
    msgEvent("content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }),
    msgEvent("content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "Hi" } }),
    msgEvent("content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "thinking_delta", thinking: "think" } }),
    msgEvent("message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 3 } }),
    msgEvent("message_stop", { type: "message_stop" }),
  ]));
  const names5 = out5.map(e => e.event);
  assert.ok(names5.includes("response.created"));
  assert.ok(names5.includes("response.output_text.delta"));
  assert.ok(names5.includes("response.completed"));
  assert.strictEqual(lc5.terminalKind, "completed");
  assert.strictEqual(lc5.inputTokens, 2);
  assert.strictEqual(lc5.outputTokens, 3);

  // Responses SSE → Messages SSE (new)
  const lc6 = t.createMessagesLifecycle("claude-x");
  const tr6 = wire(lc6, t.createResponsesToMessagesStream(lc6));
  const out6 = parseSse(await runTransform(tr6, [
    respEvent("response.created", { type: "response.created", response: { id: "r1", object: "response", created_at: 1, model: "gpt-x", status: "in_progress", output: [] } }),
    respEvent("response.output_text.delta", { type: "response.output_text.delta", delta: "Hi" }),
    respEvent("response.completed", { type: "response.completed", response: { id: "r1", object: "response", status: "completed", model: "gpt-x", output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text: "Hi" }] }], usage: { input_tokens: 1, output_tokens: 2 } } }),
  ]));
  const names6 = out6.map(e => e.event);
  assert.ok(names6.includes("message_start"));
  assert.ok(names6.includes("content_block_start"));
  const textDeltas = out6.filter(e => e.event === "content_block_delta").map(e => JSON.parse(e.data));
  assert.ok(textDeltas.some(d => d.delta.type === "text_delta" && d.delta.text === "Hi"));
  assert.ok(names6.includes("message_delta"));
  assert.ok(names6.includes("message_stop"));
  assert.strictEqual(lc6.terminalKind, "completed");

  // Responses SSE tool call → Messages SSE tool_use (new)
  const lc7 = t.createMessagesLifecycle("claude-x");
  const tr7 = wire(lc7, t.createResponsesToMessagesStream(lc7));
  const out7 = parseSse(await runTransform(tr7, [
    respEvent("response.created", { type: "response.created", response: { id: "r1", object: "response", created_at: 1, model: "gpt-x", status: "in_progress", output: [] } }),
    respEvent("response.output_item.added", { type: "response.output_item.added", output_index: 0, item: { type: "function_call", id: "fc_1", name: "get_weather", arguments: "", status: "in_progress" } }),
    respEvent("response.function_call_arguments.delta", { type: "response.function_call_arguments.delta", item_id: "fc_1", output_index: 0, arguments: '{"city":"' }),
    respEvent("response.function_call_arguments.delta", { type: "response.function_call_arguments.delta", item_id: "fc_1", output_index: 0, arguments: 'bj"}' }),
    respEvent("response.completed", { type: "response.completed", response: { id: "r1", object: "response", status: "completed", model: "gpt-x", output: [{ type: "function_call", id: "fc_1", name: "get_weather", arguments: '{"city":"bj"}' }], usage: { input_tokens: 1, output_tokens: 2 } } }),
  ]));
  const starts7 = out7.filter(e => e.event === "content_block_start").map(e => JSON.parse(e.data));
  const toolStart = starts7.find(s => s.content_block.type === "tool_use");
  assert.ok(toolStart, "must emit a tool_use content_block_start");
  assert.strictEqual(toolStart.content_block.name, "get_weather");
  const msgDelta7 = out7.filter(e => e.event === "message_delta").map(e => JSON.parse(e.data));
  assert.strictEqual(msgDelta7[0].delta.stop_reason, "tool_use");
  console.log("matrix: streaming converters (all six directions + tool calls): PASS");
}

async function testNativeMessagesProbe(t) {
  const probe1 = t.createNativeMessagesTerminalProbe("claude-x");
  const out1 = await runTransform(probe1, [
    msgEvent("message_start", { type: "message_start", message: { id: "m1", type: "message", role: "assistant", content: [], model: "claude-x", stop_reason: null, usage: { input_tokens: 1, output_tokens: 0 } } }),
    msgEvent("message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 2 } }),
    msgEvent("message_stop", { type: "message_stop" }),
  ]);
  assert.strictEqual(probe1._lifecycle.terminalKind, "completed");
  assert.strictEqual(probe1._lifecycle.inputTokens, 1);
  assert.strictEqual(probe1._lifecycle.outputTokens, 2);
  // passthrough keeps upstream bytes verbatim
  assert.ok(out1.includes("message_start"));

  const probe2 = t.createNativeMessagesTerminalProbe("claude-x");
  const out2 = await runTransform(probe2, [msgEvent("message_start", { type: "message_start", message: { id: "m1", type: "message", role: "assistant", content: [], model: "claude-x", stop_reason: null, usage: {} } })]);
  assert.strictEqual(probe2._lifecycle.terminalKind, "failed");
  const errEvents = parseSse(out2).filter(e => e.event === "error");
  assert.ok(errEvents.length >= 1, "premature EOF must emit a terminal error event");
  console.log("matrix: native Messages passthrough probe (terminal guard + usage): PASS");
}

function testBuildForwardPlanMatrix(t) {
  const headers = { authorization: "Bearer client", "content-type": "application/json", "anthropic-version": "2023-06-01" };
  for (const downstream of ["chat", "messages", "responses"]) {
    for (const up of ["chat", "messages", "responses"]) {
      for (const streaming of [true, false]) {
        const body = Buffer.from(JSON.stringify({ model: "m1", stream: streaming }));
        const plan = t.buildForwardPlan(downstream, up, "POST", headers, body, { model: "m1", stream: streaming }, streaming);
        assert.strictEqual(plan.path, { chat: "/v1/chat/completions", messages: "/v1/messages", responses: "/v1/responses" }[up], `path for ${downstream}→${up}`);
        if (downstream === up) {
          assert.strictEqual(plan.converted, false);
          if (streaming && downstream !== "chat") assert.ok(plan.transform, `native probe transform for ${downstream}→${up}`);
          else assert.strictEqual(plan.transform, null);
        } else {
          assert.strictEqual(plan.converted, true);
          assert.ok(plan.transform, `transform must exist for ${downstream}→${up}`);
        }
        if (streaming && downstream !== up) {
          assert.strictEqual(plan.lifecycle.protocol, downstream, `lifecycle protocol ${downstream}→${up}`);
          assert.ok(plan.transform._lifecycle === plan.lifecycle);
        }
        // streaming same-protocol passthrough uses native terminal probes for
        // messages/responses; chat passthrough relies on the default handler
        if (streaming && downstream === up) {
          if (downstream === "chat") assert.strictEqual(plan.lifecycle, null);
          else assert.ok(plan.lifecycle, `native probe lifecycle for ${downstream}→${up}`);
        }
      }
    }
  }
  console.log("matrix: buildForwardPlan 9x2 matrix: PASS");
}

function testAnyModelAdaptation(t) {
  const capDeepseek = t.classifyUpstreamCapability(["deepseek-chat", "deepseek-reasoner"]);
  assert.strictEqual(t.resolveUpstreamModel({ model: null }, capDeepseek, "claude-opus-4-5"), "deepseek-reasoner");
  assert.strictEqual(t.resolveUpstreamModel({ model: null }, capDeepseek, "claude-haiku-4-5"), "deepseek-chat");
  assert.strictEqual(t.resolveUpstreamModel({ model: null }, capDeepseek, "gpt-5.6"), "deepseek-reasoner");
  const capGpt = t.classifyUpstreamCapability(["gpt-5.6-sol", "gpt-4o-mini"]);
  assert.strictEqual(t.resolveUpstreamModel({ model: null }, capGpt, "claude-sonnet-4-5"), "gpt-5.6-sol");
  const capClaude = t.classifyUpstreamCapability(["claude-opus-4-5", "claude-haiku-4-5"]);
  assert.strictEqual(t.resolveUpstreamModel({ model: null }, capClaude, "gpt-5.6"), "claude-opus-4-5");
  assert.strictEqual(t.resolveUpstreamModel({ model: null }, capClaude, "claude-haiku-4-5"), "claude-haiku-4-5");
  console.log("matrix: any-model auto-adaptation: PASS");
}

async function testPersistentProtocolCache(t) {
  // Regression: a Messages/Responses relay with no /v1/models must stay
  // correctly classified (dynamic protocol persists in its own 24h cache)
  // even after a subsequent failed model probe replaces the capability entry
  // with one that carries no protocol field.
  t.setAccounts([
    { key: "sk-a", url: "https://msg-relay.example/v1", status: "active", group: "A" },
    { key: "sk-b", url: "https://chat-relay.example/v1", status: "active", group: "A" },
  ]);
  t.clearCache();
  t.clearProtocolCache();
  t.discHttp.setRoutes((opts) => {
    if (opts.hostname === "msg-relay.example") {
      if (opts.path === "/v1/models") return { status: 404, body: {} };
      if (opts.path === "/v1/messages") return { status: 400, body: { error: "bad body" } };
      return { status: 404, body: {} };
    }
    if (opts.hostname === "chat-relay.example") {
      if (opts.path === "/v1/models") return { status: 404, body: {} };
      return { status: 404, body: {} };
    }
    return { status: 404, body: {} };
  });

  const cap = await t.probeUpstreamModels(0);
  assert.strictEqual(cap.failed, true);
  assert.strictEqual(t.upstreamProtocolFor(0), "messages", "dynamic detection must classify messages relay");

  // Simulate a later failed model probe that loses the protocol field.
  t.upstreamModelsCache.set("https://msg-relay.example/v1", { models: [], protocol: "chat", capability: "unknown", failed: true, fetchedAt: Date.now() });
  assert.strictEqual(t.upstreamProtocolFor(0), "messages", "persistent protocol cache must survive a capability refresh that loses protocol");

  // A fresh failed probe (probeUpstreamModels will skip due to failed-TTL, so
  // force a full re-probe via the cache-clearing helper below).
  t.upstreamModelsCache.delete("https://msg-relay.example/v1");
  const cap2 = await t.probeUpstreamModels(0);
  assert.strictEqual(cap2.protocol, "messages");
  assert.strictEqual(t.upstreamProtocolFor(0), "messages", "re-probe must re-assert the same protocol");

  // Chat relays stay chat even though their model probe failed.
  await t.probeUpstreamModels(1);
  assert.strictEqual(t.upstreamProtocolFor(1), "chat");
  console.log("matrix: persistent protocol cache across failed model probes: PASS");
}

function testChatRouteGateUsesDynamicProtocol() {
  const source = fs.readFileSync(path.join(__dirname, "proxy.js"), "utf8");
  const gate = source.slice(source.indexOf("groupHasNative"), source.indexOf("// No Responses/Messages-native"));
  assert.ok(!gate.includes("isMessagesNative(a.url) || isResponsesNative(a.url)"), "chat gate must not rely on static URL whitelist alone");
  assert.ok(gate.includes("upstreamProtocolFor(i) !== \"chat\""), "chat gate must route through dynamic protocol detection");
  const refresh = source.slice(source.indexOf("function refreshUpstreamCapabilities"), source.indexOf("function detectUpstreamProtocol"));
  assert.ok(refresh.includes("probeUpstreamModels"), "periodic refresh must re-probe upstream capabilities");
  console.log("matrix: chat /v1/chat/completions gate uses dynamic protocol: PASS");
}

async function main() {
  const t = loadHarness(__dirname);
  testProtocolIdentification(t);
  await testDynamicProtocolDetection(t);
  await testPersistentProtocolCache(t);
  testChatRouteGateUsesDynamicProtocol(t);
  testRequestConverters(t);
  testNonStreamingResponseConverters(t);
  await testStreamingConverters(t);
  await testNativeMessagesProbe(t);
  testBuildForwardPlanMatrix(t);
  testAnyModelAdaptation(t);
  console.log("test-protocol-matrix: ALL PASS");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
