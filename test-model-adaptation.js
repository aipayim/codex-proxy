#!/usr/bin/env node
"use strict";

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
          const raw = Buffer.from(route.body == null ? "" : JSON.stringify(route.body), "utf8");
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
    process: { pid: 999994, argv: [], env: {}, on: () => {}, exit: () => {} },
  };
  sandbox.globalThis = sandbox;
  const harness = `
    ;globalThis.__adaptTest = {
      config,
      accounts,
      upstreamModelsCache,
      classifyUpstreamCapability,
      parseUpstreamModels,
      modelStrengthRank,
      pickNearestModel,
      resolveUpstreamModel,
      probeUpstreamModels,
      ensureUpstreamCapability,
      getUpstreamCapability,
      setAccounts: (arr) => { accounts = arr; upstreamModelsCache.clear(); },
      clearCache: () => upstreamModelsCache.clear(),
    };
  `;
  sandbox.discHttp = discHttp;
  sandbox.memoryFiles = memoryFiles;
  vm.runInNewContext(source.slice(0, cutoff) + harness, sandbox, { filename: "proxy.js", timeout: 5000 });
  const result = sandbox.__adaptTest;
  result.discHttp = discHttp;
  return result;
}

function main() {
  const t = loadHarness(__dirname);

  const eq = (a, b) => assert.strictEqual(JSON.stringify(a), JSON.stringify(b), "array mismatch");

  // --- classifyUpstreamCapability ---
  assert.strictEqual(t.classifyUpstreamCapability(["gpt-5.6-sol", "gpt-5.5", "gpt-4o-mini"]).capability, "gpt");
  assert.strictEqual(t.classifyUpstreamCapability(["claude-opus-4-5", "claude-sonnet-4-5", "claude-haiku-4-5"]).capability, "claude");
  assert.strictEqual(t.classifyUpstreamCapability(["gpt-5.6-sol", "claude-sonnet-4-5"]).capability, "mixed");
  assert.strictEqual(t.classifyUpstreamCapability(["deepseek-chat", "qwen-max"]).capability, "unknown");
  assert.strictEqual(t.classifyUpstreamCapability([]).capability, "unknown");
  const gptOnly = t.classifyUpstreamCapability(["gpt-5.6-sol", "gpt-5.5", "gpt-4o-mini"]);
  eq(gptOnly.gptModels, ["gpt-5.6-sol", "gpt-5.5", "gpt-4o-mini"]);
  eq(gptOnly.claudeModels, []);
  const claudeOnly = t.classifyUpstreamCapability(["claude-opus-4-5", "claude-haiku-4-5"]);
  eq(claudeOnly.claudeModels, ["claude-opus-4-5", "claude-haiku-4-5"]);
  eq(claudeOnly.gptModels, []);

  // --- parseUpstreamModels ---
  eq(t.parseUpstreamModels(JSON.stringify({ object: "list", data: [{ id: "gpt-5.6" }, { id: "claude-sonnet-4-5" }] })), ["gpt-5.6", "claude-sonnet-4-5"]);
  eq(t.parseUpstreamModels(JSON.stringify({ data: [{ id: "a" }, { id: "b" }] })), ["a", "b"]);
  eq(t.parseUpstreamModels("not json"), []);
  eq(t.parseUpstreamModels(JSON.stringify({})), []);

  // --- modelStrengthRank ---
  assert.ok(t.modelStrengthRank("gpt-5.6-sol") > t.modelStrengthRank("gpt-4o-mini"));
  assert.ok(t.modelStrengthRank("claude-opus-4-5") > t.modelStrengthRank("claude-sonnet-4-5"));
  assert.ok(t.modelStrengthRank("claude-sonnet-4-5") > t.modelStrengthRank("claude-haiku-4-5"));

  // --- pickNearestModel ---
  const gptModels = ["gpt-5.6-sol", "gpt-5.5", "gpt-4o-mini"];
  assert.strictEqual(t.pickNearestModel(["only-model"], "claude-opus-4-5"), "only-model");
  assert.strictEqual(t.pickNearestModel(gptModels, "claude-opus-4-5"), "gpt-5.6-sol");
  assert.strictEqual(t.pickNearestModel(gptModels, "claude-haiku-4-5"), "gpt-4o-mini");
  assert.strictEqual(t.pickNearestModel(gptModels, "claude-sonnet-4-5"), "gpt-5.6-sol");
  assert.strictEqual(t.pickNearestModel([], "anything"), null);

  // --- resolveUpstreamModel ---
  const acctNoModel = { model: null };
  const acctWithModel = { model: "claude-sonnet-5" };
  const capGpt = t.classifyUpstreamCapability(["gpt-5.6-sol", "gpt-4o-mini"]);
  const capClaude = t.classifyUpstreamCapability(["claude-opus-4-5", "claude-sonnet-4-5", "claude-haiku-4-5"]);
  const capMixed = t.classifyUpstreamCapability(["gpt-5.6-sol", "claude-sonnet-4-5"]);
  const capUnknown = t.classifyUpstreamCapability(["deepseek-chat"]);

  // acct.model always wins
  assert.strictEqual(t.resolveUpstreamModel(acctWithModel, capGpt, "claude-sonnet-4-5"), "claude-sonnet-5");

  // claude req → gpt-only upstream: map to a gpt model
  assert.strictEqual(t.resolveUpstreamModel(acctNoModel, capGpt, "claude-sonnet-4-5"), "gpt-5.6-sol");
  assert.strictEqual(t.resolveUpstreamModel(acctNoModel, capGpt, "claude-haiku-4-5"), "gpt-4o-mini");

  // claude req → claude upstream: passthrough
  assert.strictEqual(t.resolveUpstreamModel(acctNoModel, capClaude, "claude-sonnet-4-5"), "claude-sonnet-4-5");

  // claude req → mixed upstream: passthrough
  assert.strictEqual(t.resolveUpstreamModel(acctNoModel, capMixed, "claude-sonnet-4-5"), "claude-sonnet-4-5");

  // claude req → unknown capability: passthrough
  assert.strictEqual(t.resolveUpstreamModel(acctNoModel, capUnknown, "claude-sonnet-4-5"), "claude-sonnet-4-5");
  assert.strictEqual(t.resolveUpstreamModel(acctNoModel, null, "claude-sonnet-4-5"), "claude-sonnet-4-5");

  // gpt req → claude-only upstream: map to a claude model
  assert.strictEqual(t.resolveUpstreamModel(acctNoModel, capClaude, "gpt-5.6"), "claude-opus-4-5");
  assert.strictEqual(t.resolveUpstreamModel(acctNoModel, capClaude, "gpt-4o-mini"), "claude-haiku-4-5");

  // gpt req → gpt upstream: passthrough (keeps Codex CLI working)
  assert.strictEqual(t.resolveUpstreamModel(acctNoModel, capGpt, "gpt-5.6-sol"), "gpt-5.6-sol");

  // non-claude/gpt model names: passthrough
  assert.strictEqual(t.resolveUpstreamModel(acctNoModel, capGpt, "deepseek-chat"), "deepseek-chat");

  // --- probeUpstreamModels ---
  t.setAccounts([
    { key: "sk-probe", url: "https://gpt-relay.example/v1", status: "active", group: "A" },
    { key: "sk-probe2", url: "https://claude-relay.example/v1", status: "active", group: "A" },
    { key: "sk-bad", url: "https://down-relay.example/v1", status: "active", group: "A" },
  ]);
  t.clearCache();
  t.discHttp.setRoutes((opts) => {
    if (opts.hostname === "gpt-relay.example") {
      return { status: 200, body: { object: "list", data: [{ id: "gpt-5.6-sol" }, { id: "gpt-4o-mini" }] } };
    }
    if (opts.hostname === "claude-relay.example") {
      return { status: 200, body: { object: "list", data: [{ id: "claude-opus-4-5" }, { id: "claude-haiku-4-5" }] } };
    }
    return { status: 500, body: { error: "down" } };
  });

  return t.probeUpstreamModels(0).then((cap0) => {
    assert.strictEqual(cap0.capability, "gpt");
    eq(cap0.gptModels, ["gpt-5.6-sol", "gpt-4o-mini"]);
    return t.probeUpstreamModels(1);
  }).then((cap1) => {
    assert.strictEqual(cap1.capability, "claude");
    eq(cap1.claudeModels, ["claude-opus-4-5", "claude-haiku-4-5"]);
    return t.probeUpstreamModels(2);
  }).then((cap2) => {
    assert.strictEqual(cap2.capability, "unknown");
    // probe is cached: second call hits cache
    return t.ensureUpstreamCapability(0);
  }).then((cap0cached) => {
    assert.strictEqual(cap0cached.capability, "gpt");
    // resolve against the actual probed capability
    const cachedGpt = t.getUpstreamCapability(0);
    assert.strictEqual(t.resolveUpstreamModel({ model: null }, cachedGpt, "claude-sonnet-4-5"), "gpt-5.6-sol");
    const cachedClaude = t.getUpstreamCapability(1);
    assert.strictEqual(t.resolveUpstreamModel({ model: null }, cachedClaude, "gpt-5.6"), "claude-opus-4-5");
    console.log("test-model-adaptation: PASS (classification, nearest-pick, bidirectional resolve, probe+cache)");
  });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
