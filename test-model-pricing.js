#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const path = require("path");

const ROOT = __dirname;

function loadPricingHarness(config) {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  const constantsStart = source.indexOf("const MODEL_PRICING_MAX_RULES =");
  const constantsEnd = source.indexOf("\nconst DEFAULT_CODEX_LOG_MAINTENANCE", constantsStart);
  const defaultStart = source.indexOf("function normalizeDefaultPricing(");
  const normalizeStart = source.indexOf("function normalizeModelPricing(");
  const defaultEnd = normalizeStart;
  const normalizeEnd = source.indexOf("\nfunction normalizeRuntimeStorageConfig", normalizeStart);
  const resolveStart = source.indexOf("function resolveModelPricing(");
  const estimateEnd = source.indexOf("\n// --- State ---", resolveStart);
  assert.ok(constantsStart >= 0 && constantsEnd > constantsStart, "model-pricing constants must be present");
  assert.ok(defaultStart >= 0 && defaultEnd > defaultStart, "default pricing normalizer must be present");
  assert.ok(normalizeStart >= 0 && normalizeEnd > normalizeStart, "model-pricing normalizer must be present");
  assert.ok(resolveStart >= 0 && estimateEnd > resolveStart, "model-pricing resolver and estimator must be present");
  const factory = new Function(
    "config",
    `${source.slice(constantsStart, constantsEnd)}\n${source.slice(defaultStart, defaultEnd)}\n${source.slice(normalizeStart, normalizeEnd)}\n${source.slice(resolveStart, estimateEnd)}\nreturn { normalizeDefaultPricing, normalizeModelPricing, resolveModelPricing, estimateCost };`,
  );
  return factory(config);
}

function assertClose(actual, expected, message) {
  assert.ok(Math.abs(actual - expected) < 1e-12, `${message}: expected ${expected}, got ${actual}`);
}

function testRulesAndValidation() {
  const harness = loadPricingHarness({ prices: { inputPer1M: 1, outputPer1M: 2 }, bytesPerToken: 4, modelPricing: [] });
  assert.deepStrictEqual(harness.normalizeDefaultPricing({ inputPer1M: 1.5, outputPer1M: 2.5 }, 4), {
    prices: { inputPer1M: 1.5, outputPer1M: 2.5 }, bytesPerToken: 4,
  });
  assert.deepStrictEqual(harness.normalizeDefaultPricing({ inputPer1M: -1, outputPer1M: 2 }, 0), {
    prices: { inputPer1M: 0, outputPer1M: 0 }, bytesPerToken: 3,
  }, "invalid legacy defaults must fall back safely at runtime");
  assert.throws(() => harness.normalizeDefaultPricing({ inputPer1M: -1, outputPer1M: 2 }, 3, true, { prices: true }), /prices requires/);
  assert.throws(() => harness.normalizeDefaultPricing({ inputPer1M: 1, outputPer1M: 2 }, 0, true, { bytesPerToken: true }), /bytesPerToken must be/);
  assert.deepStrictEqual(harness.normalizeModelPricing(null), []);
  assert.deepStrictEqual(harness.normalizeModelPricing({ rules: [{ model: " gpt-5.6-sol ", inputPer1M: 5, outputPer1M: 15, bytesPerToken: 3 }] }), [
    { model: "gpt-5.6-sol", inputPer1M: 5, outputPer1M: 15, bytesPerToken: 3 },
  ]);
  assert.deepStrictEqual(harness.normalizeModelPricing([
    { model: "same", inputPer1M: 1, outputPer1M: 2, bytesPerToken: 3 },
    { model: "same", inputPer1M: 4, outputPer1M: 5, bytesPerToken: 6 },
    { model: "bad", inputPer1M: -1, outputPer1M: 2, bytesPerToken: 3 },
  ]), [{ model: "same", inputPer1M: 1, outputPer1M: 2, bytesPerToken: 3 }]);
  assert.throws(() => harness.normalizeModelPricing([
    { model: "same", inputPer1M: 1, outputPer1M: 2, bytesPerToken: 3 },
    { model: "same", inputPer1M: 4, outputPer1M: 5, bytesPerToken: 6 },
  ], true), /unique model name/);
  assert.throws(() => harness.normalizeModelPricing([
    { model: "bad-bpt", inputPer1M: 1, outputPer1M: 2, bytesPerToken: 0.09 },
  ], true), /bytesPerToken between 0\.1 and 100/);
}

function testExactMatchFallbackAndByteEstimation() {
  const config = {
    prices: { inputPer1M: 1, outputPer1M: 2 },
    bytesPerToken: 4,
    modelPricing: [{ model: "gpt-5.6-sol", inputPer1M: 10, outputPer1M: 20, bytesPerToken: 2 }],
  };
  const harness = loadPricingHarness(config);
  assert.deepStrictEqual(harness.resolveModelPricing(" gpt-5.6-sol "), config.modelPricing[0], "trimmed exact model name must match");
  assert.deepStrictEqual(harness.resolveModelPricing("gpt-5.6-sol-preview"), {
    inputPer1M: 1, outputPer1M: 2, bytesPerToken: 4,
  }, "a prefix-related model must fall back rather than partially match");
  assert.deepStrictEqual(harness.resolveModelPricing("GPT-5.6-SOL"), {
    inputPer1M: 1, outputPer1M: 2, bytesPerToken: 4,
  }, "matching must remain case-sensitive");
  assertClose(harness.estimateCost("gpt-5.6-sol", 200, 100), 0.002, "matched rule must use its own bytesPerToken and prices");
  assertClose(harness.estimateCost("other", 200, 100), 0.0001, "unmatched model must use the global fallback");

  const requestPricing = { ...harness.resolveModelPricing("gpt-5.6-sol") };
  const historicalCost = harness.estimateCost("gpt-5.6-sol", 200, 100, requestPricing);
  config.modelPricing[0] = { model: "gpt-5.6-sol", inputPer1M: 100, outputPer1M: 200, bytesPerToken: 2 };
  assertClose(historicalCost, 0.002, "a request-start price snapshot must survive a later rule change");
  assertClose(harness.estimateCost("gpt-5.6-sol", 200, 100), 0.02, "new requests must use the new active rule");
}

function loadRecordRequestHarness(config) {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  const resolveStart = source.indexOf("function resolveModelPricing(");
  const estimateEnd = source.indexOf("\n// --- State ---", resolveStart);
  const recordStart = source.indexOf("function recordRequest(");
  const recordEnd = source.indexOf("\nfunction recordStreamOutcome", recordStart);
  assert.ok(resolveStart >= 0 && estimateEnd > resolveStart, "cost functions must be present");
  assert.ok(recordStart >= 0 && recordEnd > recordStart, "request recorder must be present");
  const factory = new Function(
    "config", "getKeyState", "state", "today", "recordSliding", "trimRateWindow", "saveState", "broadcastStatus",
    "STATE_HOURLY_MODEL_NAME_MAX_CHARS", "STATE_HOURLY_MODEL_LIMIT", "STATE_HOURLY_OTHER_MODEL",
    `let stateBucketAddsSinceCompaction=0; function invalidateStateStatusBucketCache(){}\n${source.slice(resolveStart, estimateEnd)}\n${source.slice(recordStart, recordEnd)}\nreturn recordRequest;`,
  );
  const keyState = { stats: { totalRequests: 0, successRequests: 0, failRequests: 0, inputBytes: 0, outputBytes: 0, totalCost: 0, daily: {}, hourly: {} } };
  const state = { activeKey: null };
  const recordRequest = factory(
    config, () => keyState, state, () => new Date().toISOString().slice(0, 10), () => {}, () => {}, () => {}, () => {},
    160, 12, "(其他)",
  );
  return { keyState, state, recordRequest };
}

function testRecordedModelCosts() {
  const config = {
    prices: { inputPer1M: 1, outputPer1M: 2 },
    bytesPerToken: 4,
    modelPricing: [{ model: "matched", inputPer1M: 10, outputPer1M: 20, bytesPerToken: 2 }],
    rateLimit: false,
  };
  const harness = loadRecordRequestHarness(config);
  harness.recordRequest(0, true, 200, 100, 10, 2, "matched", 200, "Codex CLI");
  const hourly = Object.values(harness.keyState.stats.hourly)[0];
  const daily = Object.values(harness.keyState.stats.daily)[0];
  assertClose(harness.keyState.stats.totalCost, 0.002, "key total must record matched-model cost at request time");
  assertClose(daily.totalCost, 0.002, "daily total must record matched-model cost");
  assertClose(hourly.totalCost, 0.002, "hourly total must feed the cost trend");
  assertClose(hourly.models.matched.totalCost, 0.002, "hourly model dimension must retain the model cost");
  assert.strictEqual(hourly.models.matched.requests, 1, "hourly model dimension must retain request count");
}

function testForwardRequestFreezesPricing() {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  const start = source.indexOf("function forwardRequest(");
  const end = source.indexOf("\nfunction forwardWithPriority", start);
  assert.ok(start >= 0 && end > start, "forwardRequest must be present");
  const body = source.slice(start, end);
  assert.match(body, /const resolvedModel = acct\.model \|\| cleanModel \|\| reqModel \|\| null;/, "key-level upstream model override must remain the pricing-model priority");
  assert.match(body, /const effectiveModel = adaptedModel \|\| resolvedModel;/, "pricing must use the adapted (real) model with requested-model fallback");
  assert.match(body, /const requestPricing = \{ \.\.\.resolveModelPricing\(effectiveModel\) \};/);
  assert.ok((body.match(/recordRequest\([^;]*requestPricing\)/g) || []).length >= 5, "every forwardRequest accounting path must use its request-start pricing snapshot");
}

function testHourlyModelCompactionKeepsCosts() {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  const start = source.indexOf("function trimHourlyModelDimensions(");
  const end = source.indexOf("\nfunction trimRateWindow", start);
  assert.ok(start >= 0 && end > start, "hourly model compactor must be present");
  const trimHourlyModelDimensions = new Function(
    "STATE_HOURLY_MODEL_LIMIT", "STATE_HOURLY_OTHER_MODEL",
    `${source.slice(start, end)}; return trimHourlyModelDimensions;`,
  )(12, "(其他)");
  const models = {};
  for (let i = 0; i < 13; i++) models[`model-${i}`] = { requests: i + 1, inputBytes: i, outputBytes: i, totalCost: i / 1000 };
  const before = Object.values(models).reduce((sum, value) => sum + value.totalCost, 0);
  assert.strictEqual(trimHourlyModelDimensions(models), true);
  assert.ok(Object.keys(models).length <= 12, "hourly model dimensions must remain bounded");
  const after = Object.values(models).reduce((sum, value) => sum + (value.totalCost || 0), 0);
  assertClose(after, before, "compacting low-volume model dimensions must not lose recorded cost");
}

function testDashboardPricingConfigSurface() {
  const source = fs.readFileSync(path.join(ROOT, "proxy.js"), "utf8");
  const start = source.indexOf("function getDashboardHTML()");
  const end = source.indexOf("\n// --- HTTP Server ---", start);
  assert.ok(start >= 0 && end > start, "embedded dashboard function must be present");
  const getDashboardHTML = new Function(`${source.slice(start, end)}; return getDashboardHTML;`)();
  const embeddedHtml = getDashboardHTML();
  const standaloneHtml = fs.readFileSync(path.join(ROOT, "dashboard.html"), "utf8");
  for (const html of [embeddedHtml, standaloneHtml]) {
    assert.match(html, /id="cfgModelPricingArea"/);
    assert.match(html, /function collectModelPricingRules\(\)/);
    assert.match(html, /modelPricing,/);
    assert.match(html, /max="100"/);
    assert.ok(!html.includes("\0"), "dashboard HTML must not contain a NUL byte");
    const scripts = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(match => match[1]);
    assert.strictEqual(scripts.length, 1, "dashboard must contain one parseable script block");
    new Function(scripts[0]);
  }
}

function testInstallerDefaultConfig() {
  const installer = fs.readFileSync(path.join(ROOT, "install.sh"), "utf8");
  const match = installer.match(/cat > config\.json << 'CONFIG'\n([\s\S]*?)\nCONFIG/);
  assert.ok(match, "install.sh must contain the default config JSON");
  const defaultConfig = JSON.parse(match[1]);
  assert.deepStrictEqual(defaultConfig.modelPricing, [], "a fresh installation must preserve the global-pricing fallback by default");
}

function main() {
  testRulesAndValidation();
  testExactMatchFallbackAndByteEstimation();
  testRecordedModelCosts();
  testForwardRequestFreezesPricing();
  testHourlyModelCompactionKeepsCosts();
  testDashboardPricingConfigSurface();
  testInstallerDefaultConfig();
  console.log("Model pricing: PASS");
}

try {
  main();
} catch (error) {
  console.error(error && error.stack || error);
  process.exitCode = 1;
}
