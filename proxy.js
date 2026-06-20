const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");
const { Transform } = require("stream");
const { WebSocketServer } = require("ws");

const PORT = 3456;
const KEYS_FILE = path.join(__dirname, "keys.json");
const STATE_FILE = path.join(__dirname, "state.json");
const CONFIG_FILE = path.join(__dirname, "config.json");
const TIMEOUT = 300000;
const PRIORITY = { daily: 0, weekly: 1, never: 2 };
const HTTP_MOD = { "http:": http, "https:": https };
const TZ = "Asia/Shanghai";
const MAX_LOG = 500;
const QUEUE_TIMEOUT = 30000;

let accounts = [];
let state = { keys: [], activeKey: null, dailyLog: {} };
const activeRequests = {};
const requestLog = [];
const slidingWindows = {};
const pathStats = {};
let requestQueue = [];
let queueProcessing = false;
let config = { webhookUrl: "", prices: { inputPer1M: 0, outputPer1M: 0 }, bytesPerToken: 3, notifications: { sound: true, desktop: true } };
let wss = null;
const wsClients = new Set();
let lastBroadcast = "{}";
let allFailedNotified = false;
let autoRecoverTimer = null;
let autoRecoverNextTime = 0;

function loadConfig() {
  try {
    const raw = fs.readFileSync(CONFIG_FILE, "utf-8");
    const c = JSON.parse(raw);
    if (c.webhookUrl) config.webhookUrl = c.webhookUrl;
    if (c.prices) { config.prices.inputPer1M = c.prices.inputPer1M || 0; config.prices.outputPer1M = c.prices.outputPer1M || 0; }
    if (c.bytesPerToken) config.bytesPerToken = c.bytesPerToken;
    if (c.notifications) { config.notifications.sound = c.notifications.sound !== false; config.notifications.desktop = c.notifications.desktop !== false; }
    config.autoRecover = c.autoRecover !== false;
    config.autoRecoverInterval = Math.max(0.5, c.autoRecoverInterval || 1);
    config.autoRecoverCodes = Array.isArray(c.autoRecoverCodes) ? c.autoRecoverCodes : [401,402,403,429,500,502,503,504];
    config.autoRecoverDiscarded = c.autoRecoverDiscarded === true;
  } catch { /* defaults */ }
  if (autoRecoverTimer) { clearInterval(autoRecoverTimer); autoRecoverTimer = null; }
  if (config.autoRecover) {
    const ms = config.autoRecoverInterval * 3600000;
    autoRecoverNextTime = Date.now() + Math.max(60000, ms);
    autoRecoverTimer = setInterval(() => {
      autoRecoverNextTime = Date.now() + Math.max(60000, config.autoRecoverInterval * 3600000);
      autoRecover();
    }, Math.max(60000, ms));
  } else {
    autoRecoverNextTime = 0;
  }
}

function autoRecover(){
  if (!config.autoRecover) return;
  const codes = config.autoRecoverCodes || [];
  const checkDiscarded = config.autoRecoverDiscarded === true;
  const toCheck = [];
  for (let i = 0; i < accounts.length; i++) {
    const ks = getKeyState(i);
    if (!ks.failCode && ks.status !== "discarded") continue;
    if (ks.status === "discarded" && !checkDiscarded) continue;
    if (ks.failCode && !codes.includes(ks.failCode)) continue;
    toCheck.push(i);
  }
  if (!toCheck.length) return;
  console.log(`[proxy] auto-recover: checking ${toCheck.length} key(s)...`);
  toCheck.forEach(i => {
    const acct = accounts[i];
    if (!acct) return;
    const targetUrl = new URL(acct.url);
    const mod = HTTP_MOD[targetUrl.protocol] || https;
    const opts = {
      hostname: targetUrl.hostname,
      port: targetUrl.port || (targetUrl.protocol === "http:" ? 80 : 443),
      path: "/v1/models",
      method: "GET",
      headers: { authorization: "Bearer " + acct.key, "content-type": "application/json" },
      timeout: 15000,
    };
    const testReq = mod.request(opts, testRes => {
      let data = "";
      testRes.on("data", c => data += c);
      testRes.on("end", () => {
        if (testRes.statusCode === 200) {
          const ks = getKeyState(i);
          ks.failCode = null;
          ks.failTime = null;
          ks.failPeriod = "";
          if (ks.status === "discarded") ks.status = "active";
          allFailedNotified = false;
          saveState();
          broadcastStatus();
          console.log(`[proxy] auto-recover: #${i+1} recovered (was ${ks.status||"cooled"})`);
        }
      });
    });
    testReq.on("error", () => {});
    testReq.on("timeout", () => { testReq.destroy(); });
    testReq.end();
  });
}

function addLog(entry) {
  requestLog.push(entry);
  if (requestLog.length > MAX_LOG) requestLog.splice(0, requestLog.length - MAX_LOG);
}

function computeHealthScore(ks, idx) {
  const s = ks.stats || {};
  if (ks.status === "discarded") return 0;
  let score = 100;
  if (inCooldown(idx)) score -= 30;
  if (ks.failCode) score -= 20;
  if (s.totalRequests > 0) {
    const rate = s.successRequests / s.totalRequests;
    if (rate < 0.5) score -= 20;
    else if (rate < 0.8) score -= 10;
  }
  const r5 = slidingRate(idx, 300000);
  if (r5 !== null && r5 < 0.5) score -= 15;
  else if (r5 !== null && r5 < 0.8) score -= 5;
  return Math.max(0, Math.min(100, score));
}

function today() { return tzDate(TZ); }
function tzDate(tz) {
  return new Intl.DateTimeFormat("en-CA", { timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date());
}
function tzWeekPeriod(tz) {
  const s = tzDate(tz);
  const [y, m, d] = s.split("-").map(Number);
  const date = new Date(y, m - 1, d);
  const dayNum = (date.getDay() + 6) % 7 + 1;
  date.setDate(date.getDate() - dayNum + 3);
  const ys = new Date(date.getFullYear(), 0, 1);
  const wn = Math.ceil((((date - ys) / 86400000) + 1) / 7);
  return `${y}-W${String(wn).padStart(2, "0")}`;
}
function keyPeriod(reset) {
  return reset === "weekly" ? tzWeekPeriod(TZ) : tzDate(TZ);
}
function isConsecutivePeriod(prev, curr, reset) {
  if (reset === "daily") {
    const p = new Date(prev + "T00:00:00+08:00"), c = new Date(curr + "T00:00:00+08:00");
    return (c - p) === 86400000;
  }
  if (reset === "weekly") {
    const r = /(\d+)-W(\d+)/;
    const [, py, pw] = prev.match(r), [, cy, cw] = curr.match(r);
    return (+cy * 100 + +cw) - (+py * 100 + +pw) === 1;
  }
  return false;
}
function fmtBytes(n) {
  if (!n) return "0B";
  if (n >= 1048576) return (n / 1048576).toFixed(1) + "MB";
  if (n >= 1024) return (n / 1024).toFixed(1) + "KB";
  return n + "B";
}

// --- Sliding window ---
function recordSliding(idx, success, duration) {
  if (!slidingWindows[idx]) slidingWindows[idx] = [];
  slidingWindows[idx].push({ time: Date.now(), success, duration: duration || 0 });
  const cutoff = Date.now() - 3600000;
  slidingWindows[idx] = slidingWindows[idx].filter(e => e.time > cutoff);
}
function slidingRate(idx, windowMs) {
  const entries = slidingWindows[idx] || [];
  const cutoff = Date.now() - windowMs;
  const recent = entries.filter(e => e.time > cutoff);
  if (!recent.length) return null;
  const ok = recent.filter(e => e.success).length;
  return ok / recent.length;
}
function slidingPercentile(idx, pct) {
  const entries = slidingWindows[idx] || [];
  const durations = entries.map(e => e.duration).filter(d => d > 0);
  if (!durations.length) return null;
  durations.sort((a, b) => a - b);
  const i = Math.ceil(pct / 100 * durations.length) - 1;
  return durations[Math.max(0, i)];
}

// --- Path stats ---
function recordPath(pathname, method, inputBytes, outputBytes, duration) {
  if (!pathStats[pathname]) pathStats[pathname] = { requests: 0, inputBytes: 0, outputBytes: 0, totalDuration: 0 };
  pathStats[pathname].requests++;
  pathStats[pathname].inputBytes += inputBytes || 0;
  pathStats[pathname].outputBytes += outputBytes || 0;
  pathStats[pathname].totalDuration += duration || 0;
}

// --- Cost estimation ---
function estimateCost(inputBytes, outputBytes) {
  const bpt = config.bytesPerToken || 3;
  const inTokens = inputBytes / bpt;
  const outTokens = outputBytes / bpt;
  const cost = (inTokens / 1000000) * (config.prices.inputPer1M || 0) + (outTokens / 1000000) * (config.prices.outputPer1M || 0);
  return cost;
}

// --- State ---
function loadState() {
  try {
    state = JSON.parse(fs.readFileSync(STATE_FILE, "utf-8"));
  } catch {
    state = { keys: [], activeKey: null, dailyLog: {} };
  }
}
function saveState() {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
  } catch (e) {
    console.error(`[proxy] Failed to save state: ${e.message}`);
  }
}
function backupState() {
  try {
    fs.copyFileSync(STATE_FILE, STATE_FILE + ".bak");
  } catch { /* ignore */ }
}
setInterval(backupState, 3600000);

function getKeyState(idx) {
  while (state.keys.length <= idx) state.keys.push({ failCode: null, failTime: null, failPeriod: null, status: "active", stats: null });
  const ks = state.keys[idx];
  if (ks.status === undefined) ks.status = "active";
  if (ks.failPeriod === undefined) ks.failPeriod = null;
  if (!ks.stats) {
    ks.stats = { totalRequests: 0, successRequests: 0, failRequests: 0, inputTokens: 0, outputTokens: 0, inputBytes: 0, outputBytes: 0, lastUsed: null, daily: {}, hourly: {}, totalDuration: 0, totalTtfb: 0, totalCost: 0 };
  } else {
    if (ks.stats.inputBytes === undefined) ks.stats.inputBytes = 0;
    if (ks.stats.outputBytes === undefined) ks.stats.outputBytes = 0;
    if (ks.stats.totalDuration === undefined) ks.stats.totalDuration = 0;
    if (ks.stats.totalTtfb === undefined) ks.stats.totalTtfb = 0;
    if (ks.stats.totalCost === undefined) ks.stats.totalCost = 0;
    if (!ks.stats.hourly) ks.stats.hourly = {};
    if (ks.stats.daily) {
      for (const d of Object.keys(ks.stats.daily)) {
        if (ks.stats.daily[d].inputBytes === undefined) ks.stats.daily[d].inputBytes = 0;
        if (ks.stats.daily[d].outputBytes === undefined) ks.stats.daily[d].outputBytes = 0;
        if (ks.stats.daily[d].totalDuration === undefined) ks.stats.daily[d].totalDuration = 0;
        if (ks.stats.daily[d].totalTtfb === undefined) ks.stats.daily[d].totalTtfb = 0;
        if (ks.stats.daily[d].totalCost === undefined) ks.stats.daily[d].totalCost = 0;
      }
    }
  }
  return ks;
}

function recordRequest(idx, success, inputBytes, outputBytes, duration, ttfb) {
  const ks = getKeyState(idx);
  const s = ks.stats;
  const cost = estimateCost(inputBytes || 0, outputBytes || 0);
  s.totalRequests++;
  if (success) s.successRequests++; else s.failRequests++;
  s.lastUsed = Date.now();
  if (inputBytes) s.inputBytes += inputBytes;
  if (outputBytes) s.outputBytes += outputBytes;
  s.totalCost = (s.totalCost || 0) + cost;
  if (duration) {
    s.totalDuration = (s.totalDuration || 0) + duration;
    s.totalTtfb = (s.totalTtfb || 0) + (ttfb || 0);
  }
  const d = today();
  if (!s.daily[d]) s.daily[d] = { requests: 0, inputTokens: 0, outputTokens: 0, inputBytes: 0, outputBytes: 0, totalDuration: 0, totalTtfb: 0, totalCost: 0 };
  s.daily[d].requests++;
  if (inputBytes) s.daily[d].inputBytes += inputBytes;
  if (outputBytes) s.daily[d].outputBytes += outputBytes;
  s.daily[d].totalCost = (s.daily[d].totalCost || 0) + cost;
  if (duration) {
    s.daily[d].totalDuration = (s.daily[d].totalDuration || 0) + duration;
    s.daily[d].totalTtfb = (s.daily[d].totalTtfb || 0) + (ttfb || 0);
  }

  const now = new Date();
  const hk = d + "-" + String(now.getHours()).padStart(2, "0");
  if (!s.hourly[hk]) s.hourly[hk] = { requests: 0, inputBytes: 0, outputBytes: 0 };
  s.hourly[hk].requests++;
  if (inputBytes) s.hourly[hk].inputBytes += inputBytes;
  if (outputBytes) s.hourly[hk].outputBytes += outputBytes;

  state.activeKey = idx;
  recordSliding(idx, success, duration);
  saveState();
  broadcastStatus();
}

function markSuccess(idx) {
  const ks = getKeyState(idx);
  ks.failCode = null;
  ks.failTime = null;
  allFailedNotified = false;
  saveState();
  processQueue();
  broadcastStatus();
}

function markFailure(idx, code) {
  const ks = getKeyState(idx);
  const acct = accounts[idx];
  const curr = keyPeriod(acct.reset);

  if (acct.reset !== "never") {
    if (ks.failPeriod && ks.failPeriod !== curr && isConsecutivePeriod(ks.failPeriod, curr, acct.reset)) {
      ks.status = "discarded";
      console.log(`[proxy] #${idx+1} DISCARDED (consecutive ${acct.reset} failure: ${ks.failPeriod} → ${curr})`);
    }
  }

  ks.failCode = code;
  ks.failTime = Date.now();
  ks.failPeriod = curr;
  saveState();
  broadcastStatus();

  // Webhook + notification when all keys failed
  const allFailed = checkAllFailed();
  if (allFailed && !allFailedNotified) {
    allFailedNotified = true;
    sendWebhook("all_keys_failed", { time: new Date().toISOString(), accounts: accounts.length });
    broadcastNotification("all_keys_failed");
  }
}

function checkAllFailed() {
  for (let i = 0; i < accounts.length; i++) {
    if (accounts[i].status !== "active") continue;
    if (!inCooldown(i)) return false;
  }
  return true;
}

function inCooldown(idx) {
  const acct = accounts[idx];
  const ks = getKeyState(idx);
  if (ks.status === "discarded") return true;
  if (acct.reset === "never") return !!ks.failCode;
  if (!ks.failCode || !ks.failPeriod) return false;
  const curr = keyPeriod(acct.reset);
  return ks.failPeriod === curr;
}

function pickKey() {
  const groups = [[], [], []];
  for (let i = 0; i < accounts.length; i++) {
    if (accounts[i].status !== "active") continue;
    const ks = getKeyState(i);
    if (ks.status !== "discarded") groups[PRIORITY[accounts[i].reset] ?? 0].push(i);
  }
  for (const g of groups) {
    const a = g.filter(i => !inCooldown(i));
    if (a.length) return a[0];
  }
  for (const g of groups) if (g.length) return g[0];
  return -1;
}

// --- Request Queue ---
function enqueueRequest(method, headers, body, clientRes, pathname) {
  requestQueue.push({ method, headers, body, clientRes, pathname, time: Date.now() });
  console.log(`[proxy] Queue depth: ${requestQueue.length}`);
  clientRes.on("close", () => {
    const i = requestQueue.findIndex(r => r.clientRes === clientRes);
    if (i >= 0) { requestQueue.splice(i, 1); if (!clientRes.destroyed) clientRes.destroy(); }
  });
}

function processQueue() {
  if (queueProcessing) return;
  queueProcessing = true;
  const now = Date.now();
  const batch = [...requestQueue];
  requestQueue = [];
  for (const r of batch) {
    if (now - r.time > QUEUE_TIMEOUT) {
      if (!r.clientRes.destroyed && !r.clientRes.headersSent) {
        r.clientRes.writeHead(503, { "content-type": "application/json" });
        r.clientRes.end(JSON.stringify({ error: "request timeout in queue" }));
      }
      continue;
    }
    const idx = pickKey();
    if (idx < 0 || inCooldown(idx)) {
      requestQueue.push(r);
      continue;
    }
    forwardRequest(idx, r.method, r.headers, r.body, r.clientRes, r.pathname, (result) => {
      if (result.switched) {
        requestQueue.push(r);
      }
    });
  }
  queueProcessing = false;
}

function loadAccounts() {
  const raw = fs.readFileSync(KEYS_FILE, "utf-8");
  const parsed = JSON.parse(raw);
  const oldAccounts = accounts;
  accounts = parsed.filter(a => a.key && a.url).map(a => ({
    key: a.key.trim(),
    url: a.url.replace(/\/+$/, ""),
    reset: a.reset || "daily",
    remark: a.remark || "",
    status: a.status || "active",
  }));
  if (!accounts.length) { console.error("[proxy] No valid accounts, reverting"); accounts = oldAccounts; return; }
  loadState();

  const labels = ["每日重置", "每周重置", "永不过期"];
  const groups = [[], [], []];
  for (let i = 0; i < accounts.length; i++) groups[PRIORITY[accounts[i].reset] ?? 0].push(i);
  console.log(`[proxy] Loaded ${accounts.length} accounts`);
  for (let gi = 0; gi < groups.length; gi++) {
    for (const i of groups[gi]) {
      const a = accounts[i], m = a.key.match(/^(sk-[^-]+)/);
      const ks = getKeyState(i), tag = a.remark ? ` (${a.remark})` : "";
      const disc = ks.status === "discarded" ? "废弃" : "";
      const user = a.status !== "active" ? a.status : "";
      const st = user ? `✗ ${user}` : (disc ? `✗ ${disc}` : (inCooldown(i) ? `✗ 冷却中 (${ks.failCode})` : "✓ 可用"));
      const s = ks.stats;
      const t = s ? ` | ${s.totalRequests}次请求 ${fmtBytes(s.inputBytes+s.outputBytes)}` : "";
      console.log(`       ${i+1}. ${m ? m[1] : a.key.slice(0,12)}... → ${a.url} [${labels[gi]}]${tag} ${st}${t}`);
    }
  }
  broadcastStatus();
}

function makeUsageTransform(idx, inputBytes, reqStart, ttfb) {
  let outputBytes = 0;
  return new Transform({
    transform(chunk, encoding, cb) {
      outputBytes += chunk.length;
      this.push(chunk);
      cb();
    },
    flush(cb) {
      const duration = Date.now() - reqStart;
      recordRequest(idx, true, inputBytes, outputBytes, duration, ttfb);
      cb();
    }
  });
}

function activeDecr(idx) {
  if (activeRequests[idx] !== undefined) {
    activeRequests[idx] = Math.max(0, activeRequests[idx] - 1);
    if (activeRequests[idx] === 0) delete activeRequests[idx];
  }
}

function forwardRequest(idx, method, headers, body, clientRes, pathname, onDone) {
  activeRequests[idx] = (activeRequests[idx] || 0) + 1;
  const reqStart = Date.now();
  let ttfb = null;

  const acct = accounts[idx];
  const targetUrl = new URL(acct.url);
  const mod = HTTP_MOD[targetUrl.protocol] || https;

  const reqHeaders = { ...headers };
  delete reqHeaders.host;
  delete reqHeaders["content-length"];
  delete reqHeaders.connection;
  reqHeaders["authorization"] = `Bearer ${acct.key}`;

  const options = {
    hostname: targetUrl.hostname,
    port: targetUrl.port || (targetUrl.protocol === "http:" ? 80 : 443),
    path: targetUrl.pathname.replace(/\/+$/, "") + pathname,
    method,
    headers: reqHeaders,
    timeout: TIMEOUT,
  };

  const logEntry = { time: Date.now(), idx: idx + 1, method, path: pathname, url: acct.url };

  const proxyReq = mod.request(options, (apiRes) => {
    if (onDone.done) { apiRes.destroy(); return; }
    ttfb = Date.now() - reqStart;

    const isKeyError = apiRes.statusCode === 401 || apiRes.statusCode === 402 || apiRes.statusCode === 403 || apiRes.statusCode === 429;
    const isServerError = apiRes.statusCode >= 500 && apiRes.statusCode < 600;

    if (isKeyError || isServerError) {
      if (onDone.done) { apiRes.destroy(); return; }
      onDone.done = true;
      const dur = Date.now() - reqStart;
      activeDecr(idx);
      markFailure(idx, apiRes.statusCode);
      recordRequest(idx, false, 0, 0, dur);
      recordPath(pathname, method, 0, 0, dur);
      Object.assign(logEntry, { status: apiRes.statusCode, inputBytes: body ? body.length : 0, outputBytes: 0, duration: dur, ttfb: null });
      addLog(logEntry);
      apiRes.destroy();
      onDone({ switched: true, code: apiRes.statusCode });
      return;
    }

    markSuccess(idx);

    const a = accounts[idx];
    const m = a.key.match(/^(sk-[^-\s]+)/);
    const preview = m ? m[1] : a.key.slice(0, 12);

    const safeHeaders = { ...apiRes.headers };
    delete safeHeaders["transfer-encoding"];
    safeHeaders["x-proxy-account"] = `${idx + 1}/${accounts.length}`;
    safeHeaders["x-proxy-key"] = `${preview}...`;
    safeHeaders["x-proxy-url"] = a.url;

    if (clientRes.headersSent) { apiRes.destroy(); return; }
    clientRes.writeHead(apiRes.statusCode, safeHeaders);

    const inputBytes = body ? body.length : 0;
    const transform = makeUsageTransform(idx, inputBytes, reqStart, ttfb);
    apiRes.pipe(transform).pipe(clientRes);
    let cleaned = false;
    const cleanup = () => {
      if (cleaned) return;
      cleaned = true;
      activeDecr(idx);
      const dur = Date.now() - reqStart;
      recordPath(pathname, method, inputBytes, 0, dur);
      Object.assign(logEntry, { status: apiRes.statusCode, inputBytes, outputBytes: 0, duration: dur, ttfb });
      addLog(logEntry);
      broadcastStatus();
    };
    apiRes.on("end", cleanup);
    apiRes.on("error", (err) => {
      console.error(`[proxy] #${idx+1} Stream error: ${err.message}`);
      if (!cleaned) markFailure(idx, 0);
      cleanup();
      if (!clientRes.destroyed) clientRes.end();
    });
    onDone({ switched: false });
  });

  proxyReq.on("error", (err) => {
    if (onDone.done) return;
    onDone.done = true;
    const dur = Date.now() - reqStart;
    activeDecr(idx);
    console.error(`[proxy] #${idx + 1} Error: ${err.message}`);
    markFailure(idx, 0);
    recordRequest(idx, false, 0, 0, dur);
    recordPath(pathname, method, 0, 0, dur);
    Object.assign(logEntry, { status: 0, inputBytes: body ? body.length : 0, outputBytes: 0, duration: dur, ttfb: null });
    addLog(logEntry);
    onDone({ switched: true, error: err });
  });

  proxyReq.on("timeout", () => {
    if (onDone.done) return;
    onDone.done = true;
    const dur = Date.now() - reqStart;
    activeDecr(idx);
    console.error(`[proxy] #${idx + 1} Timeout`);
    proxyReq.destroy();
    markFailure(idx, 0);
    recordRequest(idx, false, 0, 0, dur);
    recordPath(pathname, method, 0, 0, dur);
    Object.assign(logEntry, { status: 0, inputBytes: body ? body.length : 0, outputBytes: 0, duration: dur, ttfb: null });
    addLog(logEntry);
    onDone({ switched: true, error: new Error("timeout") });
  });

  if (body) proxyReq.write(body);
  proxyReq.end();
}

function forwardWithPriority(method, headers, body, clientRes, pathname) {
  let responded = false;
  const usedKeys = new Set();
  const activeCount = accounts.filter(a => a.status === "active").length;
  function attempt() {
    const idx = pickKey();
    if (responded) return;
    if (idx < 0 || (usedKeys.has(idx) && usedKeys.size >= activeCount)) {
      if (idx < 0) {
        console.log(`[proxy] No available keys, queueing request`);
        enqueueRequest(method, headers, body, clientRes, pathname);
        responded = true;
        return;
      }
      console.error(`[proxy] All accounts exhausted`);
      if (!clientRes.destroyed && !clientRes.headersSent) {
        clientRes.writeHead(502, { "content-type": "application/json" });
        clientRes.end(JSON.stringify({ error: "All keys exhausted" }));
      }
      responded = true;
      return;
    }
    usedKeys.add(idx);
    const a = accounts[idx];
    const tag = a.remark ? ` (${a.remark})` : "";
    console.log(`[proxy] → #${idx + 1}${tag} ${a.url}`);
    forwardRequest(idx, method, headers, body, clientRes, pathname, (r) => {
      if (r.switched && usedKeys.size < activeCount) { console.log(`[proxy] #${idx+1} → ${r.code||"err"}, switching...`); return attempt(); }
      responded = true;
    });
  }
  attempt();
}

// --- WebSocket ---
function setupWebSocket(server) {
  wss = new WebSocketServer({ server });
  wss.on("connection", (ws) => {
    wsClients.add(ws);
    const data = buildStatusData();
    const msg = JSON.stringify({ type: "status", data });
    ws.send(msg);
    ws.on("close", () => wsClients.delete(ws));
    ws.on("error", () => wsClients.delete(ws));
  });
}

function broadcastStatus() {
  const data = buildStatusData();
  const msg = JSON.stringify({ type: "status", data });
  lastBroadcast = msg;
  for (const ws of wsClients) {
    if (ws.readyState === 1) ws.send(msg);
  }
}

function broadcastNotification(type) {
  const msg = JSON.stringify({ type: "notification", notificationType: type, time: new Date().toISOString() });
  for (const ws of wsClients) {
    if (ws.readyState === 1) ws.send(msg);
  }
}

function buildStatusData() {
  const data = [];
  for (let i = 0; i < accounts.length; i++) {
    const a = accounts[i];
    if (a.status !== "active") continue;
    const ks = getKeyState(i);
    const m = a.key.match(/^(sk-[^-]+)/);
    const s = ks.stats || {};
    const avgDur = s.totalRequests > 0 ? Math.round((s.totalDuration || 0) / s.totalRequests) : 0;
    const avgTtfb = s.successRequests > 0 ? Math.round((s.totalTtfb || 0) / s.successRequests) : 0;
    data.push({
      idx: i + 1,
      key: a.key.length > 16 ? a.key.slice(0, 6) + "..." + a.key.slice(-4) : a.key,
      url: a.url,
      reset: a.reset,
      remark: a.remark || "",
      available: !inCooldown(i),
      status: ks.status || "active",
      failCode: ks.failCode,
      failTime: ks.failTime,
      failPeriod: ks.failPeriod,
      active: (activeRequests[i] || 0) > 0,
      activeRequests: activeRequests[i] || 0,
      healthScore: computeHealthScore(ks, i),
      avgDuration: avgDur,
      avgTtfb: avgTtfb,
      p50: slidingPercentile(i, 50),
      p95: slidingPercentile(i, 95),
      p99: slidingPercentile(i, 99),
      sliding5mRate: slidingRate(i, 300000),
      sliding1hRate: slidingRate(i, 3600000),
      totalCost: s.totalCost || 0,
      ...s,
      daily: s.daily || {},
      hourly: s.hourly || {},
    });
  }
  return data;
}

// --- Webhook ---
function sendWebhook(event, payload) {
  if (!config.webhookUrl) return;
  const url = config.webhookUrl;
  const body = JSON.stringify({ event, ...payload, proxy: { accounts: accounts.length, queueDepth: requestQueue.length } });
  try {
    const u = new URL(url);
    const mod = HTTP_MOD[u.protocol] || https;
    const req = mod.request(url, { method: "POST", headers: { "content-type": "application/json" } });
    req.write(body);
    req.end();
  } catch { /* ignore webhook errors */ }
}

// --- Prometheus ---
function getPrometheusMetrics() {
  const lines = ['# HELP codex_proxy_accounts_total Total accounts', '# TYPE codex_proxy_accounts_total gauge'];
  lines.push(`codex_proxy_accounts_total ${accounts.length}`);
  lines.push('# HELP codex_proxy_keys_active Active keys', '# TYPE codex_proxy_keys_active gauge');
  let activeCount = 0;
  for (let i = 0; i < accounts.length; i++) {
    if (accounts[i].status === "active" && !inCooldown(i)) activeCount++;
  }
  lines.push(`codex_proxy_keys_active ${activeCount}`);
  lines.push('# HELP codex_proxy_queue_depth Request queue depth', '# TYPE codex_proxy_queue_depth gauge');
  lines.push(`codex_proxy_queue_depth ${requestQueue.length}`);
  for (let i = 0; i < accounts.length; i++) {
    if (accounts[i].status !== "active") continue;
    const ks = getKeyState(i);
    const s = ks.stats || {};
    const idx = i + 1;
    lines.push(`# HELP codex_proxy_key_requests_total Total requests per key`, `# TYPE codex_proxy_key_requests_total counter`);
    lines.push(`codex_proxy_key_requests_total{key="${idx}",url="${accounts[i].url}"} ${s.totalRequests || 0}`);
    lines.push(`# HELP codex_proxy_key_bytes_total Total bytes per key`, `# TYPE codex_proxy_key_bytes_total counter`);
    lines.push(`codex_proxy_key_bytes_total{key="${idx}",type="input"} ${s.inputBytes || 0}`);
    lines.push(`codex_proxy_key_bytes_total{key="${idx}",type="output"} ${s.outputBytes || 0}`);
    lines.push(`# HELP codex_proxy_key_health_score Health score per key`, `# TYPE codex_proxy_key_health_score gauge`);
    lines.push(`codex_proxy_key_health_score{key="${idx}"} ${computeHealthScore(ks, i)}`);
  }
  lines.push('# HELP codex_proxy_request_queue_max_wait_seconds Max queue wait time', '# TYPE codex_proxy_request_queue_max_wait_seconds gauge');
  lines.push(`codex_proxy_request_queue_max_wait_seconds ${QUEUE_TIMEOUT / 1000}`);
  return lines.join("\n");
}

// --- Dashboard HTML ---
function getDashboardHTML() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Codex 代理监控</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:clamp(8px,2vw,20px)}
h1{font-size:clamp(16px,3vw,20px);margin-bottom:4px;color:#f1f5f9}
.sub{color:#94a3b8;font-size:clamp(10px,1.5vw,12px);margin-bottom:clamp(8px,2vw,16px)}
.top-row{display:flex;gap:clamp(6px,1.5vw,12px);margin-bottom:clamp(8px,2vw,16px);flex-wrap:wrap}
.sum-item{background:#1e293b;border-radius:8px;padding:clamp(6px,1.5vw,10px) clamp(8px,2vw,16px);text-align:center;border:1px solid #334155;min-width:60px;flex:1}
.sum-num{font-size:clamp(16px,3vw,22px);font-weight:700}
.sum-label{font-size:clamp(8px,1.2vw,10px);color:#94a3b8;margin-top:1px}
.s-ok .sum-num{color:#4ade80}
.s-fail .sum-num{color:#f87171}
.s-active .sum-num{color:#60a5fa}
.s-token .sum-num{color:#fbbf24}
.s-score .sum-num{color:#c084fc}
.controls{display:flex;gap:6px;margin-bottom:10px;flex-wrap:wrap;align-items:center}
.controls select,.controls input{background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:3px 6px;border-radius:4px;font-size:11px}
.controls label{color:#94a3b8;font-size:11px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(clamp(280px,40vw,320px),1fr));gap:clamp(6px,1vw,10px)}
.card{background:#1e293b;border-radius:8px;padding:clamp(8px,1.5vw,12px);border:1px solid #334155;transition:border-color .2s}
.card.active{border-color:#3b82f6;box-shadow:0 0 12px #3b82f688}
.card.failed{border-color:#ef4444;background:#1e1b1b}
.card-ok{border-color:#22c55e}
.card .toggle-body{cursor:pointer;-webkit-user-select:none;user-select:none}
.card .cbody.collapsed{display:none}
.ctop{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;gap:4px}
.idx{font-weight:700;font-size:clamp(12px,2vw,14px);color:#94a3b8}
.idx.active-idx{color:#60a5fa}
.badge{font-size:clamp(9px,1.2vw,10px);padding:1px 5px;border-radius:3px;font-weight:600;white-space:nowrap}
.bd-daily{background:#1e3a5f;color:#60a5fa}
.bd-weekly{background:#3b1f5e;color:#c084fc}
.bd-never{background:#3b2f1e;color:#fbbf24}
.bd-active{background:#1e3a5f;color:#93c5fd}
.bd-score{background:#2d1f3f;color:#c084fc;border:1px solid #a855f7}
.cbody{font-size:clamp(11px,1.5vw,12px)}
.row{display:flex;justify-content:space-between;padding:2px 0;gap:4px}
.label{color:#94a3b8;flex-shrink:0}
.val{color:#e2e8f0;text-align:right;word-break:break-all;max-width:60%}
.sbar{margin-top:6px;padding-top:6px;border-top:1px solid #334155;display:flex;justify-content:space-between;align-items:center;font-size:clamp(10px,1.3vw,12px)}
.btn-act{color:#94a3b8;cursor:pointer;padding:2px 4px;border-radius:4px;font-size:12px;line-height:1}
.btn-act:hover{background:#334155;color:#e2e8f0}
.dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:4px;flex-shrink:0}
.d-ok{background:#22c55e;box-shadow:0 0 4px #22c55e66}
.d-fail{background:#ef4444;box-shadow:0 0 4px #ef444466}
.d-pending{background:#f59e0b;box-shadow:0 0 4px #f59e0b66}
.cooldown{color:#f87171;font-size:clamp(10px,1.3vw,11px)}
.rem{color:#38bdf8;font-size:clamp(10px,1.3vw,11px)}
.uurl{color:#64748b;font-size:clamp(9px,1.2vw,10px);word-break:break-all}
.hist{padding:2px 0;font-size:clamp(10px,1.3vw,11px);color:#94a3b8;display:flex;justify-content:space-between}
.hist-bar{height:4px;background:#334155;border-radius:2px;margin:2px 0;overflow:hidden}
.hist-fill{height:100%;background:#3b82f6;border-radius:2px}
.tabs{display:flex;gap:6px;margin-bottom:10px;flex-wrap:wrap}
.tab{padding:3px 8px;border-radius:4px;font-size:clamp(10px,1.3vw,11px);cursor:pointer;background:#334155;color:#94a3b8;border:1px solid transparent;white-space:nowrap}
.tab.on{background:#1e3a5f;color:#60a5fa;border-color:#3b82f6}
.tab:hover{background:#475569}
.btn{background:#334155;border:1px solid #475569;color:#e2e8f0;padding:3px 8px;border-radius:4px;cursor:pointer;font-size:clamp(11px,1.4vw,12px);white-space:nowrap}
.btn:hover{background:#475569}
.btn-p{background:#1e3a5f;border-color:#3b82f6}
.btn-p:hover{background:#1e4a7f}
.btn-d{background:#3b1f1e;border-color:#7f3f3e}
.btn-d:hover{background:#5f2f2e}
.btn-s{background:#1e3b1e;border-color:#22c55e}
.btn-s:hover{background:#2d5f2d}
.meter{height:4px;background:#334155;border-radius:2px;overflow:hidden;margin:4px 0}
.meter-fill{height:100%;border-radius:2px;transition:width .3s}
.modal{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.6);z-index:100;padding:clamp(8px,2vw,20px)}
.modal.on{display:flex;align-items:flex-start;justify-content:center}
.mcontent{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:clamp(10px,2vw,16px);max-width:960px;width:100%;max-height:90vh;overflow-y:auto;margin-top:10px}
.mtitle{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;font-size:clamp(14px,2vw,16px);font-weight:700}
.mtable{width:100%;border-collapse:collapse;font-size:clamp(11px,1.4vw,12px)}
.mtable th,.mtable td{padding:4px 6px;text-align:left;border-bottom:1px solid #334155}
.mtable th{color:#94a3b8;font-weight:600;position:sticky;top:0;background:#1e293b}
.mtable input,.mtable select{background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:3px;width:100%;font-size:clamp(11px,1.4vw,12px);box-sizing:border-box}
.mtable input:focus,.mtable select:focus{outline:none;border-color:#3b82f6}
.mtable .del{color:#f87171;cursor:pointer;text-align:center;font-size:14px}
.mtable .del:hover{color:#ef4444}
.mfoot{display:flex;gap:8px;justify-content:flex-end;margin-top:12px;flex-wrap:wrap}
.key-mask{color:#94a3b8;font-size:clamp(10px,1.3vw,11px);cursor:pointer;font-family:monospace}
.key-mask:hover{color:#e2e8f0;text-decoration:underline}
.alert{background:#3b1f1e;border:1px solid #ef4444;border-radius:8px;padding:clamp(8px,1.5vw,10px) clamp(10px,2vw,14px);margin-bottom:clamp(8px,2vw,12px);font-size:clamp(12px,1.8vw,13px);color:#f87171;display:none;align-items:center;gap:8px}
.alert svg{flex-shrink:0}
.trend-wrap{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:clamp(8px,1.5vw,12px);margin-bottom:12px;overflow-x:auto}
.trend-title{font-size:clamp(12px,1.5vw,13px);color:#94a3b8;margin-bottom:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.trend-bars{display:flex;align-items:flex-end;gap:2px;height:clamp(60px,10vw,80px);padding:4px 0}
.trend-bar{flex:1;background:#3b82f6;border-radius:2px 2px 0 0;position:relative;min-width:4px;transition:height .3s}
.trend-bar:hover{background:#60a5fa}
.trend-labels{display:flex;gap:2px;margin-top:4px}
.trend-label{flex:1;font-size:clamp(7px,1vw,8px);color:#64748b;text-align:center;min-width:4px;overflow:hidden;white-space:nowrap}
.log-table{width:100%;border-collapse:collapse;font-size:clamp(10px,1.3vw,11px)}
.log-table th,.log-table td{padding:2px 4px;text-align:left;border-bottom:1px solid #334155;white-space:nowrap}
.log-table th{color:#94a3b8;font-weight:600;position:sticky;top:0;background:#1e293b}
.log-status{font-weight:600}
.log-s200{color:#4ade80}
.log-s401{color:#f59e0b}
.log-s429{color:#f87171}
.log-s0{color:#ef4444}
.log-time{color:#64748b;font-size:clamp(9px,1.2vw,10px)}
.log-dur{color:#94a3b8}
.file-banner{display:none;background:#1e3a5f;border:1px solid #3b82f6;border-radius:8px;padding:clamp(8px,1.5vw,10px) clamp(10px,2vw,14px);margin-bottom:clamp(8px,2vw,12px);font-size:clamp(12px,1.8vw,13px);color:#93c5fd;align-items:center;gap:10px}
.file-banner.on{display:flex}
@media(max-width:600px){
  .controls{flex-direction:column;align-items:stretch}
  .controls select,.controls input{width:100%}
  .top-row .sum-item{min-width:45%}
  .grid{grid-template-columns:1fr}
  .mtable{font-size:10px}
  .mtable input,.mtable select{font-size:10px}
  .modal{padding:4px}
  .row{flex-direction:column;align-items:flex-start;gap:0}
  .val{text-align:left;max-width:100%}
}
</style>
</head>
<body>
<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
<h1>Codex 多 Key 代理监控</h1>
<div style="display:flex;gap:6px;flex-wrap:wrap">
<button class="btn" onclick="openLogs()">📋 日志</button>
<button class="btn" onclick="exportCSV()">⬇ 导出 CSV</button>
<button class="btn btn-p" onclick="openMgr()">⚙ 管理 Key</button>
<button class="btn btn-s" onclick="openConfig()">⚙ 配置</button>
</div>
</div>
<div class="sub" id="sub">加载中...</div>
<div id="alert" class="alert">⚠️ 所有 Key 均不可用，请求将全部失败！</div>
<div class="top-row" id="summary"></div>
<div class="controls" id="controls">
  <label>排序</label>
  <select id="sortBy"><option value="idx">默认顺序</option><option value="score">健康评分</option><option value="latency">平均延迟</option><option value="rate5m">5分钟成功率</option></select>
  <label>筛选</label>
  <select id="filterBy"><option value="all">全部</option><option value="available">可用</option><option value="cooldown">冷却中</option><option value="discarded">废弃</option></select>
  <label>趋势</label>
  <select id="trendRange"><option value="24h">24小时</option><option value="7d">7天</option><option value="30d">30天</option></select>
  <label>搜索</label>
  <input id="searchBox" placeholder="搜索 ID/备注/地址..." style="width:140px">
  <button class="btn" style="padding:0 6px;font-size:11px" onclick="toggleAllCollapse()" title="全部折叠/展开">📂</button>
</div>
<div id="batchBar" style="display:none;margin-bottom:8px;padding:6px 8px;background:#1e293b;border:1px solid #475569;border-radius:6px;gap:6px;flex-wrap:wrap;align-items:center">
  <span style="color:#94a3b8;font-size:12px" id="batchCount">已选 0 个</span>
  <button class="btn" style="font-size:11px" onclick="batchActionCards('reset')">🔄 批量重置</button>
  <button class="btn" style="font-size:11px;color:#f87171" onclick="batchActionCards('shield')">🔇 批量屏蔽</button>
</div>
<div id="trend" class="trend-wrap" style="display:none">
<div class="trend-title"><span>流量趋势</span><span id="trendRangeLabel" style="font-size:10px;color:#64748b">24h</span></div>
<div class="trend-bars" id="trendBars"></div>
<div class="trend-labels" id="trendLabels"></div>
</div>
<div class="tabs" id="tabs"></div>
<div class="grid" id="grid"></div>

<div class="modal" id="mgrModal">
<div class="mcontent">
<div class="mtitle"><span>Key 管理</span><button class="btn" onclick="closeMgr()">✕</button></div>
<div style="font-size:11px;color:#94a3b8;margin-bottom:8px">修改后点击保存，代理自动重载配置</div>
<div style="display:flex;gap:6px;margin-bottom:8px;flex-wrap:wrap;align-items:center">
  <input id="mgrSearch" placeholder="搜索备注/地址..." oninput="renderMgr()" style="flex:1;min-width:100px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px">
  <button class="btn" style="font-size:11px" onclick="selectAllMgr(true)">全选</button>
  <button class="btn" style="font-size:11px" onclick="selectAllMgr(false)">取消</button>
  <button class="btn" style="font-size:11px" onclick="batchShieldMgr()">🔇 批量屏蔽</button>
  <button class="btn" style="font-size:11px" onclick="batchResetMgr()">🔄 批量重置</button>
  <button class="btn" style="font-size:11px;color:#f87171" onclick="batchDeleteMgr()">✕ 批量删除</button>
  <button class="btn" style="font-size:11px" onclick="importKeys()">📋 导入</button>
  <button class="btn" style="font-size:11px" onclick="batchTestMgr()">🔍 批量测试</button>
</div>
<div id="batchTestResults" style="display:none;margin-bottom:8px;padding:8px;background:#1e293b;border:1px solid #475569;border-radius:6px;max-height:200px;overflow-y:auto;font-size:11px;font-family:monospace">
  <div style="color:#94a3b8;margin-bottom:4px">批量测试结果</div>
  <div id="batchTestList"></div>
  <div id="batchTestSummary" style="margin-top:4px;color:#94a3b8"></div>
  <div style="margin-top:6px;display:flex;gap:4px">
    <button class="btn" style="font-size:11px;display:none" id="batchTestResetBtn" onclick="batchTestResetPassed()">🔄 重置通过测试的 Key</button>
    <button class="btn" style="font-size:11px" onclick="document.getElementById('batchTestResults').style.display='none'">收起</button>
  </div>
</div>
<table class="mtable"><thead><tr>
<th style="width:24px"><input type="checkbox" id="mgrSelectAll" onchange="selectAllMgr(this.checked)"></th>
<th style="width:30px">#</th><th style="min-width:140px">Key</th><th style="min-width:150px">URL</th><th style="width:60px">重置</th><th style="min-width:100px">备注</th><th style="width:80px"></th>
</tr></thead><tbody id="mgrBody"></tbody></table>
<div class="mfoot">
<button class="btn" onclick="addKeyRow()">+ 添加一行</button>
<div style="flex:1"></div>
<button class="btn" onclick="closeMgr()">取消</button>
<button class="btn btn-p" onclick="saveKeys()">保存</button>
</div>
</div></div>

<div class="modal" id="configModal">
<div class="mcontent">
<div class="mtitle"><span>系统配置</span><button class="btn" onclick="closeConfig()">✕</button></div>
<div style="font-size:11px;color:#94a3b8;margin-bottom:12px" id="configStatus">修改后自动保存</div>
<div style="display:grid;grid-template-columns:1fr 2fr;gap:10px;font-size:12px">
  <div style="color:#94a3b8;padding:4px 0">💰 输入价格（每百万token）</div>
  <div><input id="cfgPriceIn" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100px" placeholder="0"></div>
  <div style="color:#94a3b8;padding:4px 0">💰 输出价格（每百万token）</div>
  <div><input id="cfgPriceOut" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100px" placeholder="0"></div>
  <div style="color:#94a3b8;padding:4px 0">🔤 每 token 字节数</div>
  <div><input id="cfgBpt" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100px" value="3" placeholder="3"></div>
  <div style="color:#94a3b8;padding:4px 0">🔔 桌面通知</div>
  <div><label><input type="checkbox" id="cfgDesktop"> 全部 Key 失效时通知</label></div>
  <div style="color:#94a3b8;padding:4px 0">🔊 声音提醒</div>
  <div><label><input type="checkbox" id="cfgSound"> 全部 Key 失效时响铃</label></div>
  <div style="color:#94a3b8;padding:4px 0">🌐 Webhook URL</div>
  <div><input id="cfgWebhook" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100%" placeholder="https://qyapi.weixin.qq.com/..."></div>
  <div style="color:#94a3b8;padding:4px 0">🔄 自动恢复冷却 Key</div>
  <div><label><input type="checkbox" id="cfgAutoRecover"> 定时检测并恢复</label></div>
  <div style="color:#94a3b8;padding:4px 0">⏱ 探测间隔（小时）</div>
  <div><input id="cfgAutoInterval" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:80px" placeholder="1" title="最小 0.5 小时"></div>
  <div style="color:#94a3b8;padding:4px 0">🔢 检测的失败码</div>
  <div><input id="cfgAutoCodes" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100%" placeholder="401,402,403,429,500,502,503,504" title="401=API Key 无效或已过期&#10;402=额度不足，账号已欠费&#10;403=权限不足，Key 无访问权限&#10;429=请求过频繁，触发了速率限制&#10;500=上游服务器内部错误&#10;502=上游网关错误&#10;503=服务暂时不可用&#10;504=上游超时"></div>
  <div style="color:#94a3b8;padding:4px 0">🚫 包含 discarded Key</div>
  <div><label><input type="checkbox" id="cfgAutoDiscarded"> 连续两周期失败的也检测</label></div>
</div>
<div style="font-size:11px;color:#64748b;margin-bottom:8px" id="cfgAutoCountdown">⏳ 下次检测: --</div>
<div class="mfoot"><button class="btn" onclick="restartProxy()" style="color:#f87171">🔄 重启代理</button><div style="flex:1"></div><button class="btn btn-p" onclick="saveConfig()">保存</button></div>
</div></div>

<div class="modal" id="logModal">
<div class="mcontent">
<div class="mtitle"><span>实时请求日志</span><button class="btn" onclick="closeLogs()">✕</button></div>
<div style="font-size:11px;color:#94a3b8;margin-bottom:8px">最近 500 条请求记录，实时推送</div>
<div style="overflow-x:auto"><table class="log-table"><thead><tr>
<th>时间</th><th>#</th><th>方法</th><th>路径</th><th>状态</th><th>↑B</th><th>↓B</th><th>耗时</th><th>首字节</th>
</tr></thead><tbody id="logBody"></tbody></table></div>
</div></div>

<script>
const L={"daily":"每日","weekly":"每周","never":"永久"};
const C={"daily":"bd-daily","weekly":"bd-weekly","never":"bd-never"};
let data=[],curDate="",fullKeys={};
let sortBy="idx",filterBy="all",trendRange="24h",searchQ="";
let ws=null,wsReconnectTimer=null,pollTimer=null;
let wsFailed=false;
let autoRecoverNextTime=0;

async function httpLoad(){
  try{
    const r=await fetch("http://localhost:3456/__status");
    if(!r.ok)throw new Error("HTTP "+r.status);
    data=await r.json();render();
  }catch(e){
    if(!wsFailed)document.getElementById("sub").textContent="连接失败，正在重试...";
  }
}

function connectWS(){
  wsFailed=false;
  ws=new WebSocket("ws://localhost:3456");
  ws.onmessage=function(e){
    try{
      const msg=JSON.parse(e.data);
      if(msg.type==="status"){data=msg.data;render()}
      if(msg.type==="notification"&&msg.notificationType==="all_keys_failed"){showAlert("所有 Key 均不可用！");playAlert();sendDesktop()}
    }catch(e){}
  };
  ws.onclose=function(){
    wsFailed=true;
    if(pollTimer)clearInterval(pollTimer);
    pollTimer=setInterval(httpLoad,5000);
    httpLoad();
    wsReconnectTimer=setTimeout(connectWS,5000);
  };
  ws.onerror=function(){ws.close()};
}

function playAlert(){try{var a=new AudioContext(),o=a.createOscillator(),g=a.createGain();o.type="sine";o.frequency.value=880;g.gain.value=.3;o.connect(g);g.connect(a.destination);o.start();o.stop(a.currentTime+.3)}catch(e){}}
function sendDesktop(){try{if(config.notifications.desktop!==false&&Notification.permission==="granted")new Notification("Codex Proxy",{body:"所有 Key 已不可用！",icon:""})}catch(e){}}

async function loadKeys(){
  try{
    const r=await fetch("http://localhost:3456/__keys");
    if(r.ok){const arr=await r.json();arr.forEach((k,i)=>{fullKeys[i+1]=k.key})}
  }catch(e){}
}

async function loadConfigUI(){
  try{
    const r=await fetch("http://localhost:3456/__config");
    if(!r.ok)return;
    const c=await r.json();
    document.getElementById("cfgPriceIn").value=c.prices?.inputPer1M||"";
    document.getElementById("cfgPriceOut").value=c.prices?.outputPer1M||"";
    document.getElementById("cfgBpt").value=c.bytesPerToken||3;
    document.getElementById("cfgWebhook").value=c.webhookUrl||"";
    document.getElementById("cfgDesktop").checked=c.notifications?.desktop!==false;
    document.getElementById("cfgSound").checked=c.notifications?.sound!==false;
    document.getElementById("cfgAutoRecover").checked=c.autoRecover!==false;
    document.getElementById("cfgAutoInterval").value=c.autoRecoverInterval||1;
    document.getElementById("cfgAutoCodes").value=(c.autoRecoverCodes||[401,402,403,429,500,502,503,504]).join(",");
    document.getElementById("cfgAutoDiscarded").checked=c.autoRecoverDiscarded===true;
    if(c.autoRecoverNextTime)autoRecoverNextTime=parseInt(c.autoRecoverNextTime);else autoRecoverNextTime=0;
    if(window.autoCountTimer)clearInterval(window.autoCountTimer);
    window.autoCountTimer=setInterval(updateAutoCountdown,1000);
    updateAutoCountdown();
  }catch(e){}
}
function updateAutoCountdown(){
  const el=document.getElementById("cfgAutoCountdown");
  if(!el)return;
  if(!autoRecoverNextTime||autoRecoverNextTime<=Date.now()){el.textContent="⏳ 下次检测: --";return;}
  const diff=Math.ceil((autoRecoverNextTime-Date.now())/1000);
  const h=Math.floor(diff/3600),m=Math.floor((diff%3600)/60),s=diff%60;
  el.textContent="⏳ 下次检测: "+h+"h "+String(m).padStart(2,"0")+"m "+String(s).padStart(2,"0")+"s";
}

function renderTrend(){
  const now=new Date();
  const hours=trendRange==="7d"?168:(trendRange==="30d"?720:24);
  const hMap={};
  for(let i=hours-1;i>=0;i--){
    const d=new Date(now-i*3600000);
    const y=d.getFullYear(),m=String(d.getMonth()+1).padStart(2,"0"),dd=String(d.getDate()).padStart(2,"0"),hh=String(d.getHours()).padStart(2,"0");
    hMap[y+"-"+m+"-"+dd+"-"+hh]=0;
  }
  for(const a of data){
    if(!a.hourly)continue;
    for(const [hk,v] of Object.entries(a.hourly)){
      if(hMap[hk]!==undefined)hMap[hk]+=(v.inputBytes||0)+(v.outputBytes||0);
    }
  }
  const keys=Object.keys(hMap),vals=Object.values(hMap);
  const max=Math.max(...vals,1);
  const bars=document.getElementById("trendBars");
  const labels=document.getElementById("trendLabels");
  bars.innerHTML=vals.map(v=>'<div class="trend-bar" style="height:'+Math.max(2,v/max*80)+'px" title="'+fmtBytes(v)+'"></div>').join("");
  const labelStep=trendRange==="30d"?24:(trendRange==="7d"?12:1);
  labels.innerHTML=keys.map((k,i)=>{
    const hh=k.slice(-2);
    const mmdd=k.slice(5,10);
    let text=trendRange==="30d"?mmdd:(trendRange==="7d"?mmdd+" "+hh+":00":hh+":00");
    const vis=i%labelStep===0;
    return '<div class="trend-label" style="'+(vis?"":"visibility:hidden;font-size:0")+'">'+text+'</div>';
  }).join("");
  document.getElementById("trend").style.display=vals.some(v=>v>0)?"block":"none";
  document.getElementById("trendRangeLabel").textContent={"24h":"24小时","7d":"7天","30d":"30天"}[trendRange];
}

function render(){
  const now=Date.now();
  const tot=data.length,ok=data.filter(x=>x.available).length,fail=tot-ok;
  const concurrent=data.reduce((s,x)=>s+(x.activeRequests||0),0);
  const allBytes=data.reduce((s,x)=>s+(x.inputBytes||0)+(x.outputBytes||0),0);
  const allReq=data.reduce((s,x)=>s+(x.totalRequests||0),0);
  const avgScore=tot>0?Math.round(data.reduce((s,x)=>s+(x.healthScore||100),0)/tot):100;
  const totalCost=data.reduce((s,x)=>s+(x.totalCost||0),0);
  const q="http://localhost:3456/";

  document.getElementById("sub").textContent="最后更新: "+new Date().toLocaleString("zh-CN")+" | 实时推送";

  document.getElementById("alert").style.display=(tot>0&&ok===0)?"flex":"none";

  document.getElementById("summary").innerHTML=
    '<div class="sum-item s-ok"><div class="sum-num">'+ok+'/'+tot+'</div><div class="sum-label">可用</div></div>'+
    '<div class="sum-item s-fail"><div class="sum-num">'+fail+'</div><div class="sum-label">冷却中</div></div>'+
    '<div class="sum-item s-active"><div class="sum-num">'+concurrent+'</div><div class="sum-label">并发请求</div></div>'+
    '<div class="sum-item s-token"><div class="sum-num">'+fmtBytes(allBytes)+'</div><div class="sum-label">总流量</div></div>'+
    '<div class="sum-item s-token"><div class="sum-num">'+allReq+'</div><div class="sum-label">总请求</div></div>'+
    '<div class="sum-item s-score"><div class="sum-num">'+avgScore+'</div><div class="sum-label">健康评分</div></div>'+
    (totalCost>0?'<div class="sum-item s-token"><div class="sum-num">$'+totalCost.toFixed(4)+'</div><div class="sum-label">预估费用</div></div>':'');

  renderTrend();

  const dates=new Set();
  data.forEach(x=>{if(x.daily)Object.keys(x.daily).forEach(d=>dates.add(d))});
  const sorted=[...dates].sort().reverse();
  curDate=sorted.includes(curDate)?curDate:(sorted[0]||todayStr());
  const tabsHtml=sorted.map(d=>'<span class="tab'+(d===curDate?' on':'')+'" onclick="curDate=\\''+d+'\\';render()">'+d+'</span>').join("");
  document.getElementById("tabs").innerHTML='<span class="tab'+(curDate==='all'?' on':'')+'" onclick="curDate=\\'all\\';render()">全部</span>'+tabsHtml;

  let filtered=data;
  if(filterBy==="available")filtered=filtered.filter(x=>x.available);
  else if(filterBy==="cooldown")filtered=filtered.filter(x=>!x.available&&x.status!=="discarded");
  else if(filterBy==="discarded")filtered=filtered.filter(x=>x.status==="discarded");
  if(searchQ){const q=searchQ.toLowerCase();filtered=filtered.filter(x=>String(x.idx).includes(q)||(x.remark||"").toLowerCase().includes(q)||x.url.toLowerCase().includes(q))}
  if(sortBy==="score")filtered.sort((a,b)=>(b.healthScore||0)-(a.healthScore||0));
  else if(sortBy==="latency")filtered.sort((a,b)=>(a.avgDuration||0)-(b.avgDuration||0));
  else if(sortBy==="rate5m"){
    filtered.sort((a,b)=>{
      const ra=a.sliding5mRate!==null?a.sliding5mRate:-1;
      const rb=b.sliding5mRate!==null?b.sliding5mRate:-1;
      return rb-ra;
    });
  }

  let html="";
  for(const a of filtered){
    const isDiscard=a.status==="discarded";
    const isActive=a.active,isFail=a.failCode&&!a.available&&!isDiscard,isOk=a.available&&!a.failCode;
    const c=isDiscard?"failed":(isFail?"failed":(isActive?"active":(isOk?"card-ok":"")));
    const dot=isDiscard?"d-fail":(a.available?(a.failCode?"d-pending":"d-ok"):"d-fail");
    const st=isDiscard?"已废弃":(a.available?(a.failCode?"待恢复":"可用"):"冷却中");
    let cd="";
    if(isDiscard){cd="已被标记废弃"}
    else if(a.failCode&&a.failPeriod&&!a.available){
      const r=a.reset;
      if(r==="never"){cd="永久失效"}
      else if(r==="daily"){cd="本日已用完，明天0点重置"}
      else{cd="本周已用完，下周一0点重置"}
    }
    const rg=a.remark?'<div class="rem">📝 '+esc(a.remark)+'</div>':"";
    const req=a.totalRequests||0,suc=a.successRequests||0;
    const ib=a.inputBytes||0,ob=a.outputBytes||0;
    const daily=curDate==='all'?null:(a.daily||{})[curDate];
    const score=a.healthScore||100;
    const avgD=a.avgDuration?fmtDur(a.avgDuration):"";
    const avgT=a.avgTtfb?fmtDur(a.avgTtfb):"";
    const p50=a.p50!==null?fmtDur(a.p50):"-";
    const p95=a.p95!==null?fmtDur(a.p95):"-";
    const p99=a.p99!==null?fmtDur(a.p99):"-";
    const r5=a.sliding5mRate!==null?(a.sliding5mRate*100).toFixed(0)+"%":"-";
    const r1=a.sliding1hRate!==null?(a.sliding1hRate*100).toFixed(0)+"%":"-";
    const cost=a.totalCost?("$"+a.totalCost.toFixed(6)):"";
    const meterColor=score>=80?"#22c55e":(score>=50?"#f59e0b":"#ef4444");

    html+='<div class="card '+c+'" id="card-'+a.idx+'">'+
      '<div class="ctop"><input type="checkbox" class="card-cb" data-idx="'+a.idx+'" onchange="updateBatchBar()" style="margin-right:4px;accent-color:#3b82f6">'+
      '<span class="idx'+(isActive?' active-idx':'')+'">#'+a.idx+(isActive?' ◄':'')+'</span>'+
      '<span style="display:flex;gap:3px;align-items:center;flex-wrap:wrap">'+
      '<span class="badge '+C[a.reset]+'">'+L[a.reset]+'</span>'+
      (isActive?' <span class="badge bd-active">'+a.activeRequests+'并发</span>':'')+
      (isDiscard?' <span class="badge" style="background:#3b1f1e;color:#f87171;border:1px solid #ef4444">已废弃</span>':'')+
      ' <span class="badge bd-score">'+score+'分</span>'+
      '<span class="btn" style="padding:0 4px;font-size:9px" onclick="toggleCollapse('+a.idx+')" title="折叠">▼</span></span></div>'+
      '<div class="meter"><div class="meter-fill" style="width:'+score+'%;background:'+meterColor+'"></div></div>'+
      '<div class="cbody" id="body-'+a.idx+'">'+
      '<div class="row"><span class="label">Key</span><span class="val"><span class="key-mask" data-idx="'+a.idx+'" onclick="var i=this.dataset.idx,f=fullKeys[i];if(!f){loadKeys();var t=this;setTimeout(function(){f=fullKeys[i];if(f)t.textContent=t.textContent===maskKey(f)?f:maskKey(f)},300)}else this.textContent=this.textContent===maskKey(f)?f:maskKey(f)">'+a.key+'</span></span></div>'+
      '<div class="row"><span class="label">地址</span><span class="val uurl">'+esc(a.url)+'</span></div>'+
      rg+
      (a.failCode?'<div class="row"><span class="label">失败码</span><span class="val" title="'+(FAIL_MEAN[a.failCode]||'')+'">'+a.failCode+'</span></div>':"")+
      (a.failTime?'<div class="row"><span class="label">最后失败</span><span class="val" style="color:#f87171">'+fmtDur(Date.now()-a.failTime)+'前</span></div>':"")+
      (cd?'<div class="row"><span class="label">冷却剩余</span><span class="val cooldown">'+cd+'</span></div>':"")+
      '<div class="row"><div class="label">请求</div><div class="val">'+req+'次 (成功'+suc+' 失败'+(req-suc)+')</div></div>'+
      '<div class="row"><div class="label">流量</div><div class="val">↑'+fmtBytes(ib)+' / ↓'+fmtBytes(ob)+'</div></div>'+
      (cost?'<div class="row"><div class="label">预估费用</div><div class="val">'+cost+'</div></div>':"")+
      (avgD?'<div class="row"><div class="label">平均延迟</div><div class="val">'+avgD+'</div></div>':"")+
      (avgT?'<div class="row"><div class="label">平均首字节</div><div class="val">'+avgT+'</div></div>':"")+
      '<div class="row" style="border-top:1px solid #334155;padding-top:4px;margin-top:4px"><div class="label">P50 / P95 / P99</div><div class="val">'+p50+' / '+p95+' / '+p99+'</div></div>'+
      '<div class="row"><div class="label">滑动成功率</div><div class="val">5分钟: '+r5+' | 1小时: '+r1+'</div></div>';

    if(daily){
      const db=daily.inputBytes||0,do_=daily.outputBytes||0;
      html+='<div class="row" style="border-top:1px solid #334155;padding-top:4px;margin-top:4px;color:#93c5fd">'+
        '<div class="label">'+curDate+'</div><div class="val">'+daily.requests+'次 '+fmtBytes(db+do_)+'</div></div>';
    }else if(curDate==='all'&&a.daily){
      const ds=Object.keys(a.daily).sort().reverse().slice(0,5);
      const maxBytes=Math.max(...ds.map(d=>(a.daily[d].inputBytes||0)+(a.daily[d].outputBytes||0)),1);
      for(const d of ds){
        const dd=a.daily[d],b=(dd.inputBytes||0)+(dd.outputBytes||0);
        html+='<div class="hist"><span>'+d+'</span><span>'+dd.requests+'次 '+fmtBytes(b)+'</span></div>'+
          '<div class="hist-bar"><div class="hist-fill" style="width:'+(b/maxBytes*100)+'%"></div></div>';
      }
    }else if(curDate!=='all'&&!daily&&a.daily){
      html+='<div class="row" style="color:#64748b"><div class="label">'+curDate+'</div><div class="val">无记录</div></div>';
    }

    html+='</div><div class="sbar"><span><span class="dot '+dot+'"></span>'+st+'</span>'+
      '<span style="display:flex;gap:3px;align-items:center">'+
      (!isDiscard?'<span class="btn-act" onclick="cardShield('+a.idx+')" title="屏蔽此 Key（不再参与调度）">🔇</span>':'')+
      (isDiscard?'<span class="btn-act" onclick="cardReset('+a.idx+')" title="重置此 Key">🔄</span>':'')+
      (!isDiscard&&a.failCode?'<span class="btn-act" onclick="cardReset('+a.idx+')" title="重置冷却">🔄</span>':'')+
      '<span class="btn-act" onclick="cardTest('+a.idx+')" title="测试连通性">🔍</span>'+
      '</span></div></div>';
  }
  document.getElementById("grid").innerHTML=html;
  updateBatchBar();
}

function toggleCollapse(idx){
  const body=document.getElementById("body-"+idx);
  if(body)body.classList.toggle("collapsed");
}

function todayStr(){return new Date().toISOString().slice(0,10)}
function fmtBytes(n){if(!n)return"0B";if(n>=1048576)return(n/1048576).toFixed(1)+"MB";if(n>=1024)return(n/1024).toFixed(1)+"KB";return n+"B"}
function fmtDur(ms){if(ms>=1000)return(ms/1000).toFixed(2)+"s";return ms+"ms"}
function maskKey(k){return k&&k.length>12?k.slice(0,6)+'...'+k.slice(-4):(k||'')}
function esc(s){const d=document.createElement("div");d.textContent=s;return d.innerHTML}
const FAIL_MEAN={"401":"API Key 无效或已过期","402":"额度不足，账号已欠费","403":"权限不足，Key 无访问权限","429":"请求过频繁，触发了速率限制","500":"上游服务器内部错误","502":"上游网关错误","503":"服务暂时不可用","504":"上游超时"};
function toggleAllCollapse(){
  const all=document.querySelectorAll("#grid .cbody");
  const first=all[0];
  if(!first)return;
  const isCollapsed=first.classList.contains("collapsed");
  all.forEach(b=>b.classList.toggle("collapsed",!isCollapsed));
}
function showAlert(txt){document.getElementById("sub").textContent=txt}
function cardReset(idx){
  fetch("http://localhost:3456/__reset-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx})})
    .then(r=>r.json()).then(j=>{if(j.ok)loadKeys()}).catch(()=>{});
}
function cardTest(idx){
  const d=data.find(x=>x.idx===idx);
  if(!d){alert("Key #"+idx+" 数据不可用");return}
  const fullKey=fullKeys[idx];
  if(!fullKey){loadKeys();alert("Key 未加载，请重试");return}
  const btns=document.querySelectorAll("#card-"+idx+" .btn-act");
  const btn=btns[btns.length-1];
  if(btn)btn.textContent="⏳";
  fetch("http://localhost:3456/__test-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({key:fullKey,url:d.url})})
    .then(r=>r.json()).then(j=>{
      if(btn)btn.textContent="🔍";
      if(j.ok)alert("Key #"+idx+" 测试成功！"+(j.model?" 模型: "+j.model:"")+(j.duration?" 耗时: "+j.duration+"ms":""));
      else alert("Key #"+idx+" 测试失败: "+(j.error||"未知错误"));
    }).catch(e=>{
      if(btn)btn.textContent="🔍";
      alert("Key #"+idx+" 测试请求失败: "+e.message);
    });
}
loadKeys();connectWS();if(Notification.permission==="default")Notification.requestPermission();
setTimeout(function(){if(!data.length)httpLoad()},3000);

document.getElementById("sortBy").addEventListener("change",function(){sortBy=this.value;render()});
document.getElementById("filterBy").addEventListener("change",function(){filterBy=this.value;render()});
document.getElementById("trendRange").addEventListener("change",function(){trendRange=this.value;render()});
document.getElementById("searchBox").addEventListener("input",function(){searchQ=this.value;render()});

let mgrKeys=[];
let logInterval=null;

async function openMgr(){
  try{
    const r=await fetch("http://localhost:3456/__keys");
    mgrKeys=(await r.json()).filter(k=>k.status!=="deleted");
  }catch(e){
    mgrKeys=[{key:"",url:"",reset:"weekly",remark:""}];
  }
  if(!mgrKeys.length)mgrKeys=[{key:"",url:"",reset:"weekly",remark:""}];
  renderMgr();
  document.getElementById("mgrModal").classList.add("on");
}
function closeMgr(){document.getElementById("mgrModal").classList.remove("on")}
let mgrSearchCache=[],dragIdx=-1;
function renderMgr(){
  const q=(document.getElementById("mgrSearch").value||"").toLowerCase();
  const tbody=document.getElementById("mgrBody");
  tbody.innerHTML="";
  const filtered=[];const grp={};
  for(let i=0;i<mgrKeys.length;i++){
    const k=mgrKeys[i];
    if(q&&!(k.remark||"").toLowerCase().includes(q)&&!(k.url||"").toLowerCase().includes(q)&&!(k.key||"").toLowerCase().includes(q))continue;
    filtered.push(i);
    const g=(k.remark||"").split(/[，,\s]/)[0]||(k.url||"").replace(/https?:\\/\\//,"").slice(0,16)||"未分类";
    if(!grp[g])grp[g]=[];
    grp[g].push(i);
  }
  mgrSearchCache=filtered;
  const groups=Object.keys(grp);
  for(let gi=0;gi<groups.length;gi++){
    const g=groups[gi],items=grp[g];
    const hdr=document.createElement("tr");
    hdr.style.background="#1e293b";hdr.style.color="#94a3b8";
    hdr.innerHTML='<td colspan="7" style="padding:6px 8px;font-size:11px;font-weight:600;border-bottom:1px solid #334155">📁 '+esc(g)+' ('+items.length+')</td>';
    tbody.appendChild(hdr);
    for(let ii=0;ii<items.length;ii++){
      const i=items[ii],k=mgrKeys[i],sh=k.status==="shielded";
      const tr=document.createElement("tr");
      tr.draggable=true;
      tr.ondragstart=function(){dragIdx=i};
      tr.ondragover=function(e){e.preventDefault()};
      tr.ondrop=function(e){
        e.preventDefault();
        if(dragIdx<0||dragIdx===i)return;
        const item=mgrKeys.splice(dragIdx,1)[0];
        mgrKeys.splice(i,0,item);
        dragIdx=-1;
        renderMgr();
      };
      tr.style.cursor="grab";
      tr.innerHTML='<td><input type="checkbox" class="mgr-cb" value="'+i+'"></td>'+
        '<td>'+(i+1)+'</td>'+
        '<td style="display:flex;align-items:center;gap:4px"><input class="kkey" value="'+esc(k.key||"")+'" placeholder="sk-..." style="flex:1">'+(sh?'<span class="badge" style="background:#3b1f1e;color:#f87171;white-space:nowrap">已屏蔽</span>':'')+'</td>'+
        '<td><input class="kurl" value="'+esc(k.url||"")+'" placeholder="https://..."></td>'+
        '<td><select class="kreset"><option value="daily"'+(k.reset==="daily"?" selected":"")+'>每日</option><option value="weekly"'+(k.reset==="weekly"?" selected":"")+'>每周</option><option value="never"'+(k.reset==="never"?" selected":"")+'>永久</option></select></td>'+
        '<td><input class="kremark" value="'+esc(k.remark||"")+'" placeholder="备注"></td>'+
        '<td style="display:flex;gap:4px;align-items:center;white-space:nowrap">'+
          '<span class="del" onclick="testKey('+i+')" title="测试连通性">🔍</span>'+
          '<span class="del" onclick="resetKeyStatus('+i+')" title="重置状态（清除冷却/废弃）">🔄</span>'+
          '<span class="del" onclick="toggleShield('+i+')" title="'+(sh?'恢复使用':'屏蔽')+'">'+(sh?'🔄':'🔇')+'</span>'+
          '<span class="del" onclick="delKeyRow('+i+')">✕</span></td>';
      tbody.appendChild(tr);
    }
  }
  document.getElementById("mgrSelectAll").checked=false;
}
function addKeyRow(){mgrKeys.push({key:"",url:"",reset:"weekly",remark:""});renderMgr()}
function toggleShield(i){mgrKeys[i].status=mgrKeys[i].status==="shielded"?"active":"shielded";renderMgr()}
async function resetKeyStatus(i){
  try{
    const r=await fetch("http://localhost:3456/__reset-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx:i+1})});
    const j=await r.json();
    if(j.ok)loadKeys();
  }catch(e){}
}
function delKeyRow(i){
  if(!confirm('确定要删除 Key #'+(i+1)+'？\\n删除后不再显示和调用，可在 keys.json 中恢复。'))return;
  mgrKeys[i].status="deleted";renderMgr();
  setTimeout(function(){var a=collectMgr();if(a.length)fetch("http://localhost:3456/__keys",{method:"PUT",headers:{"content-type":"application/json"},body:JSON.stringify(a,null,2)}).then(r=>r.json()).then(j=>{if(j.ok)loadKeys()})},100);
}
function selectAllMgr(sel){
  document.querySelectorAll("#mgrBody .mgr-cb").forEach(c=>c.checked=sel);
  document.getElementById("mgrSelectAll").checked=sel;
}
function getSelectedMgr(){
  const cbs=document.querySelectorAll("#mgrBody .mgr-cb:checked");
  return [...cbs].map(c=>parseInt(c.value)).filter(i=>i>=0&&i<mgrKeys.length);
}
function batchShieldMgr(){
  const sel=getSelectedMgr();
  if(!sel.length){alert("请先勾选要屏蔽的 Key");return}
  sel.forEach(i=>{mgrKeys[i].status="shielded"});
  renderMgr();
}
function batchResetMgr(){
  const sel=getSelectedMgr();
  if(!sel.length){alert("请先勾选要重置的 Key");return}
  sel.forEach(i=>{
    mgrKeys[i].status="active";
    fetch("http://localhost:3456/__reset-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx:i+1})}).catch(()=>{});
  });
  renderMgr();
}
function batchDeleteMgr(){
  const sel=getSelectedMgr();
  if(!sel.length){alert("请先勾选要删除的 Key");return}
  if(!confirm('确定要删除选中的 '+sel.length+' 个 Key？\\n删除后不再显示和调用，可在 keys.json 中恢复。'))return;
  sel.forEach(i=>{mgrKeys[i].status="deleted"});
  renderMgr();
  setTimeout(function(){var a=collectMgr();if(a.length)fetch("http://localhost:3456/__keys",{method:"PUT",headers:{"content-type":"application/json"},body:JSON.stringify(a,null,2)}).then(r=>r.json()).then(j=>{if(j.ok)loadKeys()})},100);
}
function collectMgr(){
  const result=mgrKeys.map(k=>({key:k.key,url:k.url,reset:k.reset,remark:k.remark||"",status:k.status&&k.status!=="active"?k.status:void 0}));
  const rows=document.getElementById("mgrBody").children;
  for(let i=0;i<rows.length;i++){
    const r=rows[i];
    if(r.tagName!=="TR")continue;
    const sidx=parseInt(r.querySelector(".mgr-cb")?.value||"-1");
    if(sidx<0||sidx>=result.length)continue;
    const key=r.querySelector(".kkey")?.value.trim();
    if(!key)continue;
    result[sidx].key=key;
    result[sidx].url=r.querySelector(".kurl").value.trim();
    result[sidx].reset=r.querySelector(".kreset").value;
    result[sidx].remark=r.querySelector(".kremark").value.trim();
    result[sidx].status=mgrKeys[sidx].status&&mgrKeys[sidx].status!=="active"?mgrKeys[sidx].status:void 0;
  }
  return result.filter(k=>k.key&&k.url);
}
async function saveKeys(){
  const arr=collectMgr();
  if(!arr.length){alert("至少需要一个有效的 Key");return}
  try{
    const r=await fetch("http://localhost:3456/__keys",{
      method:"PUT",
      headers:{"content-type":"application/json"},
      body:JSON.stringify(arr,null,2)
    });
    const j=await r.json();
    if(j.error){alert("保存失败: "+j.error);return}
    closeMgr();loadKeys();
  }catch(e){alert("保存失败: "+e.message)}
}
function importKeys(){
  const txt=prompt("批量导入 Key\\n每行一个，格式：sk-xxx 或 sk-xxx https://url 或者 sk-xxx https://url 每日/每周/永久 备注\\n\\n粘贴后点击确定即可添加");
  if(!txt)return;
  const lines=txt.trim().split("\\n").filter(l=>l.trim());
  let added=0;
  for(const line of lines){
    const parts=line.trim().split(/\s+/);
    if(!parts[0]||!parts[0].startsWith("sk-"))continue;
    const key=parts[0];
    const url=parts[1]||"https://api.fenno.ai";
    const resetMap={"daily":"每日","weekly":"每周","never":"永久","每日":"daily","每周":"weekly","永久":"never"};
    const reset=resetMap[parts[2]]||"weekly";
    const remark=parts.slice(3).join(" ")||"";
    mgrKeys.push({key,url,reset,remark});
    added++;
  }
  if(added)renderMgr();
  alert("成功添加 "+added+" 个 Key"+(lines.length-added>0?"，"+(lines.length-added)+" 行被跳过（格式错误）":""));
}
async function testKey(i){
  const k=mgrKeys[i];
  if(!k||!k.key){alert("Key 为空，无法测试");return}
  const btn=document.querySelector("#mgrBody .mgr-cb[value='"+i+"']")?.closest("tr")?.querySelector(".del");
  if(btn)btn.textContent="⏳";
  try{
    const r=await fetch("http://localhost:3456/__test-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({key:k.key,url:k.url})});
    const j=await r.json();
    if(btn)btn.textContent="🔍";
    if(j.ok){alert("Key #"+(i+1)+" 测试成功！"+(j.model?" 模型: "+j.model:"")+(j.duration?" 耗时: "+j.duration+"ms":""))}
    else{alert("Key #"+(i+1)+" 测试失败: "+(j.error||"未知错误"))}
  }catch(e){
    if(btn)btn.textContent="🔍";
    alert("Key #"+(i+1)+" 测试请求失败: "+e.message)
  }
}
let batchTestPassed=[];
async function batchTestMgr(){
  const sel=getSelectedMgr();
  if(!sel.length){alert("请先勾选要测试的 Key");return}
  const area=document.getElementById("batchTestResults");
  const list=document.getElementById("batchTestList");
  const summary=document.getElementById("batchTestSummary");
  const resetBtn=document.getElementById("batchTestResetBtn");
  if(!area||!list)return;
  batchTestPassed=[];
  area.style.display="block";
  list.innerHTML="";
  summary.textContent="测试中...";
  resetBtn.style.display="none";
  for(const i of sel){
    const k=mgrKeys[i];
    const line=document.createElement("div");
    line.id="btr-"+i;
    if(!k||!k.key){line.textContent="⏭️ #"+(i+1)+" Key 为空，跳过";list.appendChild(line);continue}
    line.textContent="⏳ #"+(i+1)+" 测试中...";
    list.appendChild(line);
    try{
      const r=await fetch("http://localhost:3456/__test-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({key:k.key,url:k.url})});
      const j=await r.json();
      if(j.ok){
        batchTestPassed.push(i);
        line.textContent="✅ #"+(i+1)+" 成功"+(j.model?" 模型: "+j.model:"")+(j.duration?" ("+j.duration+"ms)":"");
      }else{
        line.textContent="❌ #"+(i+1)+" 失败: "+(j.error||"未知错误");
      }
    }catch(e){
      line.textContent="❌ #"+(i+1)+" 请求异常: "+e.message;
    }
  }
  const total=sel.length;
  const passed=batchTestPassed.length;
  summary.textContent="测试完成 — "+passed+" 个通过, "+(total-passed)+" 个失败";
  if(passed>0){resetBtn.style.display="inline-block";resetBtn.textContent="🔄 重置通过测试的 Key ("+passed+"个)"}
}
function closeBatchTestResults(){
  document.getElementById("batchTestResults").style.display="none";
}
async function batchTestResetPassed(){
  if(!batchTestPassed.length)return;
  for(const i of batchTestPassed){
    try{await fetch("http://localhost:3456/__reset-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx:i+1})})}catch(e){}
  }
  batchTestPassed=[];
  document.getElementById("batchTestResults").style.display="none";
  loadKeys();
}
function exportCSV(){window.open("http://localhost:3456/__export")}

async function openLogs(){
  document.getElementById("logModal").classList.add("on");
  await loadLogs();
}
function closeLogs(){
  document.getElementById("logModal").classList.remove("on");
}
async function loadLogs(){
  try{
    const r=await fetch("http://localhost:3456/__logs?limit=200");
    if(!r.ok)return;
    const logs=await r.json();
    const tbody=document.getElementById("logBody");
    tbody.innerHTML=logs.slice().reverse().map(e=>{
      const s=e.status||0;
      const sc="log-s"+s;
      const tm=new Date(e.time);
      const ts=String(tm.getHours()).padStart(2,"0")+":"+String(tm.getMinutes()).padStart(2,"0")+":"+String(tm.getSeconds()).padStart(2,"0");
      return '<tr><td class="log-time">'+ts+'</td><td>#'+e.idx+'</td><td>'+e.method+'</td><td style="max-width:200px;overflow:hidden;text-overflow:ellipsis">'+esc(e.path)+'</td><td class="log-status '+sc+'">'+s+'</td><td>'+fmtBytes(e.inputBytes||0)+'</td><td>'+fmtBytes(e.outputBytes||0)+'</td><td class="log-dur">'+fmtDur(e.duration||0)+'</td><td class="log-dur">'+(e.ttfb?fmtDur(e.ttfb):"-")+'</td></tr>';
    }).join("")||'<tr><td colspan="9" style="text-align:center;color:#64748b;padding:20px">暂无请求记录</td></tr>';
  }catch(e){}
}

function openConfig(){loadConfigUI();document.getElementById("configModal").classList.add("on")}
function closeConfig(){document.getElementById("configModal").classList.remove("on")}
async function saveConfig(){
  const c={
    prices:{inputPer1M:parseFloat(document.getElementById("cfgPriceIn").value)||0,outputPer1M:parseFloat(document.getElementById("cfgPriceOut").value)||0},
    bytesPerToken:parseInt(document.getElementById("cfgBpt").value)||3,
    webhookUrl:document.getElementById("cfgWebhook").value.trim(),
    notifications:{desktop:document.getElementById("cfgDesktop").checked,sound:document.getElementById("cfgSound").checked},
    autoRecover:document.getElementById("cfgAutoRecover").checked,
    autoRecoverInterval:parseFloat(document.getElementById("cfgAutoInterval").value)||1,
    autoRecoverCodes:(document.getElementById("cfgAutoCodes").value||"").split(",").map(s=>parseInt(s.trim())).filter(n=>!isNaN(n)),
    autoRecoverDiscarded:document.getElementById("cfgAutoDiscarded").checked
  };
  try{
    const r=await fetch("http://localhost:3456/__config",{method:"PUT",headers:{"content-type":"application/json"},body:JSON.stringify(c)});
    const j=await r.json();
    document.getElementById("configStatus").textContent=j.ok?"已保存":"保存失败";
  }catch(e){document.getElementById("configStatus").textContent="保存失败: "+e.message}
}
function restartProxy(){
  if(!confirm("确定要重启代理进程？\\n正在进行的请求可能中断，但 codex 会自动重试。"))return;
  fetch("http://localhost:3456/__restart",{method:"POST"})
    .then(r=>r.json()).then(j=>{if(j.ok)setTimeout(function(){location.reload()},1500)})
    .catch(()=>{});
}
function selectAllCards(sel){
  document.querySelectorAll("#grid .card-cb").forEach(c=>c.checked=sel);
}
function batchActionCards(action){
  const cbs=document.querySelectorAll("#grid .card-cb:checked");
  const sel=[...cbs].map(c=>parseInt(c.dataset.idx)).filter(i=>i>0);
  if(!sel.length){alert("请先勾选要操作的 Key");return}
  if(action==="shield"){
    if(!confirm("确定屏蔽选中的 "+sel.length+" 个 Key？"))return;
    sel.forEach(i=>fetch("http://localhost:3456/__patch-key-status",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx:i,status:"shielded"})}).catch(()=>{}));
  }else if(action==="reset"){
    sel.forEach(i=>fetch("http://localhost:3456/__reset-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx:i})}).catch(()=>{}));
  }
}
function cardShield(idx){
  if(!confirm("确定屏蔽 Key #"+idx+"？屏蔽后不再参与调度，可在管理弹窗恢复。"))return;
  fetch("http://localhost:3456/__patch-key-status",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx,status:"shielded"})}).then(()=>setTimeout(loadKeys,200)).catch(()=>{});
}
function updateBatchBar(){
  const cbs=document.querySelectorAll("#grid .card-cb:checked");
  const bar=document.getElementById("batchBar");
  const cnt=document.getElementById("batchCount");
  if(!bar||!cnt)return;
  if(cbs.length){bar.style.display="flex";cnt.textContent="已选 "+cbs.length+" 个"}
  else bar.style.display="none";
}
</script>
</body>
</html>`;
}

// --- HTTP Server ---
const server = http.createServer((req, res) => {
  const pathname = (req.url || "/").split("?")[0];

  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET, PUT, POST, OPTIONS",
      "access-control-allow-headers": "content-type",
      "access-control-max-age": "86400",
    });
    res.end();
    return;
  }

  if (req.method === "GET" && (pathname === "/" || pathname === "/dashboard")) {
    res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    res.end(getDashboardHTML());
    return;
  }

  if (req.method === "GET" && pathname === "/__status") {
    res.writeHead(200, { "content-type": "application/json", "access-control-allow-origin": "*" });
    res.end(JSON.stringify(buildStatusData(), null, 2));
    return;
  }

  const cors = { "access-control-allow-origin": "*", "content-type": "application/json; charset=utf-8" };

  if (pathname === "/__config") {
    if (req.method === "GET") {
      res.writeHead(200, cors);
      res.end(JSON.stringify({ ...config, autoRecoverNextTime }, null, 2));
      return;
    }
    if (req.method === "PUT") {
      let body = "";
      req.on("data", c => body += c);
      req.on("end", () => {
        try {
          const c = JSON.parse(body);
          const cur = JSON.parse(fs.readFileSync(CONFIG_FILE, "utf-8"));
          Object.assign(cur, c);
          fs.writeFileSync(CONFIG_FILE, JSON.stringify(cur, null, 2));
          loadConfig();
          res.writeHead(200, cors);
          res.end(JSON.stringify({ ok: true }));
        } catch (e) {
          res.writeHead(400, cors);
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return;
    }
    res.writeHead(405, cors);
    res.end(JSON.stringify({ error: "method not allowed" }));
    return;
  }

  if (pathname === "/__test-key") {
    if (req.method === "POST") {
      let body = "";
      req.on("data", c => body += c);
      req.on("end", () => {
        try {
          const { key, url } = JSON.parse(body);
          if (!key || !url) throw new Error("key and url required");
          const targetUrl = new URL(url);
          const mod = HTTP_MOD[targetUrl.protocol] || https;
          const opts = {
            hostname: targetUrl.hostname,
            port: targetUrl.port || (targetUrl.protocol === "http:" ? 80 : 443),
            path: "/v1/models",
            method: "GET",
            headers: { authorization: "Bearer " + key, "content-type": "application/json" },
            timeout: 15000,
          };
          const t0 = Date.now();
          let responded = false;
          const testReq = mod.request(opts, testRes => {
            const dur = Date.now() - t0;
            let data = "";
            testRes.on("data", c => data += c);
            testRes.on("end", () => {
              if (responded) return;
              responded = true;
              if (testRes.statusCode === 200) {
                let model = "";
                try {
                  const j = JSON.parse(data);
                  if (j.data && j.data.length) {
                    const openaiPrefixes=["gpt-","o1-","o3-","dall-e-","text-embedding-","whisper-","tts-","babbage-","curie-","davinci-"];
                    const m=j.data.find(item=>openaiPrefixes.some(p=>item.id.startsWith(p)));
                    model=m?m.id:j.data[0].id;
                  }
                } catch (e) {}
                res.writeHead(200, cors);
                res.end(JSON.stringify({ ok: true, status: testRes.statusCode, duration: dur, model }));
              } else {
                res.writeHead(200, cors);
                res.end(JSON.stringify({ ok: false, status: testRes.statusCode, error: "HTTP " + testRes.statusCode + ": " + data.slice(0, 200) }));
              }
            });
          });
          testReq.on("error", e => {
            if (responded) return;
            responded = true;
            res.writeHead(200, cors);
            res.end(JSON.stringify({ ok: false, error: "请求失败: " + e.message }));
          });
          testReq.on("timeout", () => {
            if (responded) return;
            responded = true;
            testReq.destroy();
            res.writeHead(200, cors);
            res.end(JSON.stringify({ ok: false, error: "超时" }));
          });
          testReq.end();
        } catch (e) {
          res.writeHead(400, cors);
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return;
    }
    res.writeHead(405, cors);
    res.end(JSON.stringify({ error: "method not allowed" }));
    return;
  }

  if (pathname === "/__keys") {
    if (req.method === "GET") {
      const raw = JSON.parse(fs.readFileSync(KEYS_FILE, "utf-8"));
      res.writeHead(200, cors);
      res.end(JSON.stringify(raw, null, 2));
      return;
    }
    if (req.method === "PUT") {
      let body = "";
      req.on("data", c => body += c);
      req.on("end", () => {
        try {
          const arr = JSON.parse(body);
          if (!Array.isArray(arr)) throw new Error("must be an array");
          for (const k of arr) {
            if (!k.key || !k.url) throw new Error("each entry needs key + url");
            if (!k.key.startsWith("sk-")) throw new Error("key must start with sk-");
          }
          const raw = JSON.stringify(arr, null, 2);
          fs.writeFileSync(KEYS_FILE, raw, "utf-8");
          loadAccounts();
          res.writeHead(200, cors);
          res.end(JSON.stringify({ ok: true, count: arr.length }));
        } catch (e) {
          res.writeHead(400, cors);
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return;
    }
    res.writeHead(405, cors);
    res.end(JSON.stringify({ error: "method not allowed" }));
    return;
  }

  if (pathname === "/__reset-key") {
    if (req.method === "POST") {
      let body = "";
      req.on("data", c => body += c);
      req.on("end", () => {
        try {
          const { idx } = JSON.parse(body);
          const ai = idx - 1;
          if (typeof idx !== "number" || ai < 0 || ai >= accounts.length) throw new Error("invalid idx");
          const ks = getKeyState(ai);
          ks.failCode = null;
          ks.failTime = null;
          ks.failPeriod = "";
          if (ks.status === "discarded") ks.status = "active";
          allFailedNotified = false;
          saveState();
          broadcastStatus();
          console.log(`[proxy] #${idx} state reset manually`);
          res.writeHead(200, cors);
          res.end(JSON.stringify({ ok: true }));
        } catch (e) {
          res.writeHead(400, cors);
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return;
    }
    res.writeHead(405, cors);
    res.end(JSON.stringify({ error: "method not allowed" }));
    return;
  }

  if (pathname === "/__logs") {
    if (req.method === "GET") {
      const limit = Math.min(parseInt(req.url.split("?limit=")[1] || "100", 10), MAX_LOG);
      const last = requestLog.slice(-limit);
      res.writeHead(200, cors);
      res.end(JSON.stringify(last, null, 2));
      return;
    }
    res.writeHead(405, cors);
    res.end(JSON.stringify({ error: "method not allowed" }));
    return;
  }

  if (pathname === "/__patch-key-status") {
    if (req.method === "POST") {
      let body = "";
      req.on("data", c => body += c);
      req.on("end", () => {
        try {
          const { idx, status } = JSON.parse(body);
          if (typeof idx !== "number" || !["active","shielded"].includes(status)) throw new Error("invalid idx or status");
          const raw = JSON.parse(fs.readFileSync(KEYS_FILE, "utf-8"));
          const ai = idx - 1;
          if (ai < 0 || ai >= raw.length) throw new Error("invalid idx");
          raw[ai].status = status;
          fs.writeFileSync(KEYS_FILE, JSON.stringify(raw, null, 2), "utf-8");
          loadAccounts();
          allFailedNotified = false;
          saveState();
          broadcastStatus();
          res.writeHead(200, cors);
          res.end(JSON.stringify({ ok: true }));
        } catch (e) {
          res.writeHead(400, cors);
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return;
    }
    res.writeHead(405, cors);
    res.end(JSON.stringify({ error: "method not allowed" }));
    return;
  }

  if (pathname === "/__restart") {
    if (req.method === "POST") {
      res.writeHead(200, cors);
      res.end(JSON.stringify({ ok: true, message: "restarting" }));
      setTimeout(() => {
        console.log("[proxy] restarting...");
        const child = require("child_process").spawn(process.argv[0], process.argv.slice(1), { detached: true, stdio: "inherit" });
        child.unref();
        process.exit(0);
      }, 200);
      return;
    }
    res.writeHead(405, cors);
    res.end(JSON.stringify({ error: "method not allowed" }));
    return;
  }

  if (pathname === "/__export") {
    const rows = accounts.map((a, i) => {
      const ks = getKeyState(i);
      const s = ks.stats || {};
      return {
        idx: i + 1, key: a.key, url: a.url, reset: a.reset, remark: a.remark || "",
        status: ks.status || "active", failCode: ks.failCode || "",
        totalRequests: s.totalRequests || 0, successRequests: s.successRequests || 0, failRequests: s.failRequests || 0,
        inputBytes: s.inputBytes || 0, outputBytes: s.outputBytes || 0,
        avgDuration: s.totalRequests > 0 ? Math.round((s.totalDuration || 0) / s.totalRequests) : 0,
        healthScore: computeHealthScore(ks, i),
        totalCost: s.totalCost || 0,
      };
    });
    const esc = v => `"${String(v).replace(/"/g, '""')}"`;
    const header = "idx,key,url,reset,remark,status,failCode,totalRequests,successRequests,failRequests,inputBytes,outputBytes,avgDuration(ms),healthScore,totalCost";
    const csv = header + "\n" + rows.map(r => [r.idx, esc(r.key), esc(r.url), r.reset, esc(r.remark), r.status, r.failCode, r.totalRequests, r.successRequests, r.failRequests, r.inputBytes, r.outputBytes, r.avgDuration, r.healthScore, r.totalCost].join(",")).join("\n");
    res.writeHead(200, { ...cors, "content-disposition": "attachment; filename=codex-proxy-export.csv" });
    res.end(csv);
    return;
  }

  if (pathname === "/__pathstats") {
    res.writeHead(200, cors);
    res.end(JSON.stringify(pathStats, null, 2));
    return;
  }

  if (pathname === "/metrics") {
    res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
    res.end(getPrometheusMetrics());
    return;
  }

  const chunks = [];
  req.on("data", (c) => chunks.push(c));
  req.on("end", () => {
    const body = chunks.length ? Buffer.concat(chunks) : null;
    if (!req.headers["authorization"]) {
      res.writeHead(400, cors);
      res.end(JSON.stringify({ error: "Missing Authorization header" }));
      return;
    }
    console.log(`[proxy] ${req.method} ${pathname}`);
    forwardWithPriority(req.method, req.headers, body, res, pathname);
  });
  req.on("error", (e) => { console.error(`[proxy] ${e.message}`); if (!res.destroyed) res.end(); });
});

server.on("error", (e) => console.error(`[proxy] ${e.message}`));

server.listen(PORT, "localhost", () => {
  setupWebSocket(server);
  const n = (() => { try { return JSON.parse(fs.readFileSync(KEYS_FILE, "utf-8")).length; } catch { return 0; } })();
  console.log(`
╔══════════════════════════════════════════════╗
║    Codex Multi-Key Proxy v3                 ║
║──────────────────────────────────────────────║
║  Listen:  http://localhost:${PORT}                    ║
║  Accounts: ${n}  │  Dashboard: /                   ║
║──────────────────────────────────────────────║
║  WebSocket push, sliding window (5m/1h),     ║
║  P50/P95/P99, path stats, cost estimate,     ║
║  request queue, webhook, Prometheus /metrics  ║
╚══════════════════════════════════════════════╝`);
  loadAccounts();
  loadConfig();
});
