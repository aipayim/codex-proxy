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
const MAX_LOG = 2000;
const QUEUE_TIMEOUT = 30000;
const LOG_DIR = path.join(__dirname, "logs");
let LOG_RETENTION_DAYS = 7;
let LOG_FILE_ENABLED = true;
let LOG_DETAIL = "full";
let logStream = null;
let logDate = null;

let accounts = [];
let state = { keys: [], activeKey: null, dailyLog: {} };
const activeRequests = {};
const requestLog = [];
const slidingWindows = {};
const pathStats = {};
let requestQueue = [];
let queueProcessing = false;
let config = { webhookUrl: "", prices: { inputPer1M: 0, outputPer1M: 0 }, bytesPerToken: 3, notifications: { sound: true, desktop: true }, roundRobin: false };
let wss = null;
const wsClients = new Set();
let lastBroadcast = "{}";
let allFailedNotified = false;
let autoRecoverTimer = null;
let autoRecoverNextTime = 0;
let autoRecoverDailyTimer = null;
let autoRecoverDailyNextTime = 0;
let _rrCursor = 0;
let _boostKey = -1;

process.on("uncaughtException", err => {
  console.error("[proxy] UNCAUGHT EXCEPTION:", err.stack || err.message);
});
process.on("unhandledRejection", (reason) => {
  console.error("[proxy] UNHANDLED REJECTION:", reason instanceof Error ? reason.stack : reason);
});

// Periodic cleanup to prevent memory leaks
setInterval(() => {
  const cutoff = Date.now() - 3600000;
  for (const idx in slidingWindows) {
    slidingWindows[idx] = slidingWindows[idx].filter(e => e.time > cutoff);
  }
  for (const p in pathStats) {
    if (pathStats[p].requests === 0) delete pathStats[p];
  }
  // Trim stale queue entries
  const qcut = Date.now() - 30000;
  requestQueue = requestQueue.filter(r => r.time > qcut && !r.clientRes.destroyed);
}, 600000); // every 10 minutes

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
    config.autoRecoverDaily = c.autoRecoverDaily === true;
    config.autoRecoverDailyDays = Math.max(1, parseInt(c.autoRecoverDailyDays) || 1);
    config.autoRecoverDailyHour = Math.min(23, Math.max(0, (n=>isNaN(n)?8:n)(parseInt(c.autoRecoverDailyHour))));
    config.autoRecoverDailyMinute = Math.min(59, Math.max(0, (n=>isNaN(n)?0:n)(parseInt(c.autoRecoverDailyMinute))));
    config.roundRobin = c.roundRobin === true;
    config.enableAutoLock = c.enableAutoLock !== false;
    config.lockAfterFailCount = Math.max(1, c.lockAfterFailCount || 3);
    config.lockFailCodes = Array.isArray(c.lockFailCodes) ? c.lockFailCodes : ["401","403"];
    LOG_RETENTION_DAYS = c.logRetentionDays != null ? c.logRetentionDays : 7;
    LOG_FILE_ENABLED = c.logFile !== false;
    LOG_DETAIL = c.logDetail === "basic" ? "basic" : "full";
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
  if (autoRecoverDailyTimer) { clearTimeout(autoRecoverDailyTimer); autoRecoverDailyTimer = null; }
  if (config.autoRecoverDaily) {
    autoRecoverDailyNextTime = calcNextDailyRun(Date.now(), config.autoRecoverDailyDays, config.autoRecoverDailyHour, config.autoRecoverDailyMinute);
    scheduleDailyRecover();
  } else {
    autoRecoverDailyNextTime = 0;
  }
}

function calcNextDailyRun(from, days, hour, min){
  const d = new Date(from);
  d.setSeconds(0, 0);
  d.setHours(hour, min, 0, 0);
  while (d.getTime() <= from) d.setDate(d.getDate() + days);
  return d.getTime();
}
function scheduleDailyRecover(){
  const delay = Math.max(0, autoRecoverDailyNextTime - Date.now());
  autoRecoverDailyTimer = setTimeout(() => {
    autoRecover();
    autoRecoverDailyNextTime = calcNextDailyRun(autoRecoverDailyNextTime, config.autoRecoverDailyDays, config.autoRecoverDailyHour, config.autoRecoverDailyMinute);
    scheduleDailyRecover();
  }, delay);
}
function autoRecover(){
  if (!config.autoRecover && !config.autoRecoverDaily) return;
  console.log(`[proxy] auto-recover: fired (daily=${config.autoRecoverDaily})`);
  const codes = config.autoRecoverCodes || [];
  const checkDiscarded = config.autoRecoverDiscarded === true;
  const toCheck = [];
  for (let i = 0; i < accounts.length; i++) {
    const ks = getKeyState(i);
    if (ks.status === "shielded" || ks.status === "locked") continue;
    if (!ks.failCode && ks.status !== "discarded") continue;
    if (ks.status === "discarded" && !checkDiscarded) continue;
    if (ks.failCode && !codes.includes(ks.failCode)) continue;
    toCheck.push(i);
  }
  if (!toCheck.length) { console.log(`[proxy] auto-recover: 0 keys to check`); return; }
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
        } else {
          console.log(`[proxy] auto-recover: #${i+1} test returned ${testRes.statusCode}, not recovered`);
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
  if (LOG_FILE_ENABLED) writeLogEntry(entry);
  broadcastLog(entry);
}

function ensureLogStream() {
  const today = new Date().toISOString().slice(0, 10);
  if (logDate === today && logStream) return;
  if (logStream) { try { logStream.end(); } catch(e) {} }
  try {
    if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });
    logStream = fs.createWriteStream(path.join(LOG_DIR, today + ".jsonl"), { flags: "a" });
    logDate = today;
  } catch(e) { /* fail silently */ }
}

function writeLogEntry(entry) {
  try {
    ensureLogStream();
    logStream.write(JSON.stringify(entry) + "\n");
  } catch(e) { /* fail silently */ }
}

function cleanOldLogs() {
  try {
    if (!fs.existsSync(LOG_DIR)) return;
    const files = fs.readdirSync(LOG_DIR);
    const now = Date.now();
    for (const f of files) {
      if (!f.endsWith(".jsonl")) continue;
      const dateStr = f.replace(".jsonl", "");
      const fd = new Date(dateStr);
      if (isNaN(fd.getTime())) continue;
      if ((now - fd.getTime()) / 86400000 > LOG_RETENTION_DAYS) {
        fs.unlinkSync(path.join(LOG_DIR, f));
      }
    }
  } catch(e) { /* fail silently */ }
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
function getWeeklyEpoch(act, resetDay) {
  const d = new Date(act);
  if (resetDay !== undefined && resetDay !== null && resetDay !== "") {
    const jsDay = d.getDay(); // 0=Sun...6=Sat
    const isoDay = jsDay === 0 ? 7 : jsDay; // 1=Mon...7=Sun
    const diff = (isoDay - Number(resetDay) + 7) % 7;
    d.setDate(d.getDate() - diff);
    d.setHours(0, 0, 0, 0);
  }
  return d.getTime();
}
function keyPeriod(reset, idx) {
  if (reset === "weekly" && idx !== undefined) {
    const ks = getKeyState(idx);
    const acct = accounts[idx];
    const act = ks.activatedAt || Date.now();
    const epoch = getWeeklyEpoch(act, acct ? acct.resetDay : null);
    return String(Math.floor((Date.now() - epoch) / (7 * 86400000)));
  }
  return reset === "weekly" ? tzWeekPeriod(TZ) : tzDate(TZ);
}
function isConsecutivePeriod(prev, curr, reset) {
  if (reset === "daily") {
    const p = new Date(prev + "T00:00:00+08:00"), c = new Date(curr + "T00:00:00+08:00");
    return (c - p) === 86400000;
  }
  if (reset === "weekly") {
    return Number(curr) - Number(prev) === 1;
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
  // Migrate old ISO week failPeriod (e.g. "2026-W28") → null for new per-key cycle format
  if (state.keys) {
    for (const ks of state.keys) {
      if (ks.failPeriod && /^\d{4}-W\d{2}$/.test(ks.failPeriod)) {
        ks.failPeriod = null;
        ks.failCode = null;
        ks.failTime = null;
      }
    }
  }
}
let _saveThrottle = 0;
function saveState(force) {
  const now = Date.now();
  if (!force && now - _saveThrottle < 2000) return; // at most every 2s
  _saveThrottle = now;
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
  while (state.keys.length <= idx) state.keys.push({ failCode: null, failTime: null, failPeriod: null, failCount: 0, status: "active", stats: null });
  const ks = state.keys[idx];
  if (ks.status === undefined) ks.status = "active";
  if (ks.failPeriod === undefined) ks.failPeriod = null;
  if (ks.activatedAt === undefined) ks.activatedAt = Date.now();
  if (ks.failCount === undefined) ks.failCount = 0;
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
  ks.failCount = 0;
  allFailedNotified = false;
  saveState();
  processQueue();
  broadcastStatus();
}

function markFailure(idx, code) {
  const ks = getKeyState(idx);
  const acct = accounts[idx];
  const curr = keyPeriod(acct.reset, idx);

  if (acct.reset !== "never") {
    if (ks.failPeriod && ks.failPeriod !== curr && isConsecutivePeriod(ks.failPeriod, curr, acct.reset)) {
      ks.status = "discarded";
      console.log(`[proxy] #${idx+1} DISCARDED (consecutive ${acct.reset} failure: ${ks.failPeriod} → ${curr})`);
    }
  }

  // Track consecutive failures for lockable codes
  if (config.enableAutoLock !== false) {
    const raw = config.lockFailCodes || ["401", "403"];
    const lockCodes = (Array.isArray(raw) ? raw : raw.split(",")).map(s => parseInt((s && s.trim) ? s.trim() : s));
    if (lockCodes.includes(code)) {
      const same = ks.failCode === code && ks.failPeriod === curr;
      ks.failCount = same ? (ks.failCount || 0) + 1 : 1;
      if (ks.failCount >= (config.lockAfterFailCount || 3)) {
        ks.status = "locked";
        console.log(`[proxy] #${idx+1} LOCKED (${ks.failCount}x ${code})`);
      }
    } else if (ks.failCount) {
      ks.failCount = 0;
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
  if (ks.status === "discarded" || ks.status === "locked") return true;
  if (acct.reset === "never") return !!ks.failCode;
  if (!ks.failCode || !ks.failPeriod) return false;
  const curr = keyPeriod(acct.reset, idx);
  return ks.failPeriod === curr;
}

function pickKey(model) {
  function matchesModel(a) {
    if (!model) return true;
    if (!a.models || !a.models.length) return true;
    return a.models.includes(model);
  }
  // Boost: 持续高优
  if (_boostKey >= 0 && _boostKey < accounts.length) {
    if (matchesModel(accounts[_boostKey]) && accounts[_boostKey].status === "active" && !inCooldown(_boostKey) && getKeyState(_boostKey).status !== "discarded") {
      return _boostKey;
    }
    // boosted key no longer available, auto-clear
    _boostKey = -1;
    broadcastStatus();
  }

  if (config.roundRobin) {
    const groups = [[], [], []];
    for (let i = 0; i < accounts.length; i++) {
      if (accounts[i].status !== "active") continue;
      if (!matchesModel(accounts[i])) continue;
      const ks = getKeyState(i);
      if (ks.status === "discarded" || ks.status === "locked") continue;
      groups[PRIORITY[accounts[i].reset] ?? 0].push(i);
    }
    for (const g of groups) g.sort((a, b) => (accounts[b].priority || 0) - (accounts[a].priority || 0) || a - b);
    for (let gi = 0; gi <= 2; gi++) {
      const g = groups[gi];
      if (!g.length) continue;
      const avail = g.filter(i => !inCooldown(i));
      const pool = avail.length ? avail : g;
      if (_rrCursor >= pool.length) _rrCursor = 0;
      return pool[_rrCursor++];
    }
    return -1;
  }

  const groups = [[], [], []];
  for (let i = 0; i < accounts.length; i++) {
    if (accounts[i].status !== "active") continue;
    if (!matchesModel(accounts[i])) continue;
    const ks = getKeyState(i);
    if (ks.status !== "discarded" && ks.status !== "locked") groups[PRIORITY[accounts[i].reset] ?? 0].push(i);
  }
  for (const g of groups) g.sort((a, b) => (accounts[b].priority || 0) - (accounts[a].priority || 0) || a - b);
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
    let rmodel = null;
    try { const parsed = JSON.parse(r.body.toString()); rmodel = parsed.model || null; } catch(e) {}
    const idx = pickKey(rmodel);
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
  _boostKey = -1; // clear boost on key reload
  const raw = fs.readFileSync(KEYS_FILE, "utf-8");
  const parsed = JSON.parse(raw);
  const oldAccounts = accounts;
  accounts = parsed.filter(a => a.key && a.url).map(a => ({
    key: a.key.trim(),
    url: a.url.replace(/\/+$/, ""),
    reset: a.reset || "daily",
    remark: a.remark || "",
    status: a.status || "active",
    priority: a.priority || 0,
    models: a.models || [],
    model: a.model || null,
    resetDay: a.resetDay || null,
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
  if (body && LOG_DETAIL !== "basic") {
    try { const p = JSON.parse(body.toString()); logEntry.reqModel = p.model || null; } catch(e) {}
  }
  if (acct.model) logEntry.overrideModel = acct.model;

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
    apiRes.on("close", cleanup);
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

  if (body) {
    let bodyToWrite = body;
    if (acct.model) {
      try {
        const parsed = JSON.parse(body.toString());
        parsed.model = acct.model;
        bodyToWrite = Buffer.from(JSON.stringify(parsed));
      } catch(e) {}
    }
    proxyReq.write(bodyToWrite);
  }
  proxyReq.end();
}

function forwardWithPriority(method, headers, body, clientRes, pathname) {
  let responded = false;
  const usedKeys = new Set();
  const activeCount = accounts.filter(a => a.status === "active").length;
  let retries = 0;
  const MAX_RETRIES = Math.max(activeCount * 2, 10);
  let model = null;
  try { const parsed = JSON.parse(body.toString()); model = parsed.model || null; } catch(e) {}
  function attempt() {
    if (retries >= MAX_RETRIES) {
      if (responded) return;
      console.error(`[proxy] Max retries (${MAX_RETRIES}) reached, queueing`);
      enqueueRequest(method, headers, body, clientRes, pathname);
      responded = true;
      return;
    }
    retries++;
    const idx = pickKey(model);
    if (responded) return;
    if (idx < 0 || (usedKeys.has(idx) && inCooldown(idx))) {
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
      if (r.switched && !clientRes.destroyed && !clientRes.headersSent) {
        clientRes.writeHead(502, { "content-type": "application/json" });
        clientRes.end(JSON.stringify({ error: "All keys exhausted" }));
      }
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
    const msg = JSON.stringify({ type: "status", data, boostedIdx: _boostKey >= 0 ? _boostKey + 1 : -1 });
    ws.send(msg);
    ws.on("close", () => wsClients.delete(ws));
    ws.on("error", () => wsClients.delete(ws));
  });
}

function broadcastStatus() {
  const data = buildStatusData();
  const msg = JSON.stringify({ type: "status", data, boostedIdx: _boostKey >= 0 ? _boostKey + 1 : -1 });
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

function broadcastLog(entry) {
  if (!wsClients.size) return;
  const msg = JSON.stringify({ type: "log", data: entry });
  for (const ws of wsClients) {
    if (ws.readyState === 1) ws.send(msg);
  }
}

function buildStatusData() {
  const data = [];
  for (let i = 0; i < accounts.length; i++) {
    const a = accounts[i];
    if (a.status !== "active" && a.status !== "shielded") continue;
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
      models: a.models || [],
      model: a.model || null,
      resetDay: a.resetDay || null,
      activatedAt: ks.activatedAt || null,
      available: !inCooldown(i),
      status: ks.status || "active",
      failCode: ks.failCode,
      failTime: ks.failTime,
      failPeriod: ks.failPeriod,
      failCount: ks.failCount,
      locked: ks.status === "locked",
      shielded: a.status === "shielded",
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
      nextResetDay: (function(){
        if (a.reset !== "weekly") return null;
        const act = ks.activatedAt || Date.now();
        const epoch = getWeeklyEpoch(act, a.resetDay);
        const cyc = Math.floor((Date.now() - epoch) / (7 * 86400000));
        const next = epoch + (cyc + 1) * 7 * 86400000;
        return ["周日","周一","周二","周三","周四","周五","周六"][new Date(next).getDay()];
      })(),
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
.btn-act.boost-on{color:#4ade80;background:#1a3a2e}
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
.tabs{display:flex;gap:6px;margin-bottom:10px;overflow-x:auto;padding-bottom:2px}
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
.mcontent{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:clamp(10px,2vw,16px);max-width:1200px;width:100%;max-height:90vh;overflow-y:auto;margin-top:10px}
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
   <select id="filterBy"><option value="all">全部</option><option value="available">可用</option><option value="cooldown">冷却中</option><option value="discarded">废弃</option><option value="locked">🔒 锁死</option><option value="shielded">屏蔽</option></select>
  <label>重置</label>
  <select id="resetFilter"><option value="all">全部</option><option value="daily">每日重置</option><option value="weekly">每周重置</option><option value="never">永不过期</option></select>
  <label>趋势</label>
  <select id="trendRange"><option value="24h">24小时</option><option value="7d">7天</option><option value="30d">30天</option></select>
  <label>搜索</label>
  <input id="searchBox" placeholder="ID/备注/地址..." style="width:120px">
  <label>状态码</label>
  <input id="statusCodeBox" placeholder="如 401" style="width:60px">
  <label>模型</label>
  <input id="modelSearchBox" placeholder="模型名" style="width:80px">
  <button class="btn" style="padding:0 6px;font-size:11px" onclick="toggleAllCollapse()" title="全部折叠/展开">📂</button>
  <span style="color:#94a3b8;font-size:11px;margin-left:8px" id="filterCount"></span>
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
  <input id="mgrSearch" placeholder="搜索备注/地址..." oninput="renderMgr()" style="width:120px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px">
  <input id="mgrCodeFilter" placeholder="状态码" oninput="renderMgr()" style="width:60px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" title="按状态码筛选，如 401">
  <input id="mgrModelFilter" placeholder="指定模型" oninput="renderMgr()" style="width:100px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" title="按指定模型搜索，子串匹配">
  <select id="mgrStatusFilter" onchange="renderMgr()" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;font-size:11px">
    <option value="">全部状态</option>
    <option value="available">可用</option>
    <option value="cooldown">冷却中</option>
    <option value="discarded">废弃</option>
    <option value="locked">锁死</option>
    <option value="shielded">屏蔽</option>
  </select>
  <button class="btn" style="font-size:11px" onclick="selectAllMgr(true)">全选</button>
  <button class="btn" style="font-size:11px" onclick="clearMgrSearch()">取消</button>
  <button class="btn" style="font-size:11px" onclick="batchShieldMgr()">🔇 批量屏蔽</button>
  <button class="btn" style="font-size:11px" onclick="batchResetMgr()">🔄 批量重置</button>
  <button class="btn" style="font-size:11px;color:#f87171" onclick="batchDeleteMgr()">✕ 批量删除</button>
  <button class="btn" style="font-size:11px" onclick="importKeys()">📋 导入</button>
  <button class="btn" style="font-size:11px" onclick="batchTestMgr()">🔍 批量测试</button>
</div>
<div style="font-size:11px;color:#94a3b8;margin-bottom:6px" id="mgrCount">共 0 个</div>
<div id="batchTestResults" style="display:none;margin-bottom:8px;padding:8px;background:#1e293b;border:1px solid #475569;border-radius:6px;max-height:200px;overflow-y:auto;font-size:11px;font-family:monospace">
  <div style="color:#94a3b8;margin-bottom:4px">批量测试结果</div>
  <div id="batchTestList"></div>
  <div id="batchTestSummary" style="margin-top:4px;color:#94a3b8"></div>
  <div style="margin-top:6px;display:flex;gap:4px">
    <button class="btn" style="font-size:11px;display:none" id="batchTestResetBtn" onclick="batchTestResetPassed()">🔄 重置通过测试的 Key</button>
    <button class="btn" style="font-size:11px;display:none" id="batchTestResetAllBtn" onclick="batchTestResetAll()">🔄 重置所有 Key 的状态码</button>
    <button class="btn" style="font-size:11px" onclick="document.getElementById('batchTestResults').style.display='none'">收起</button>
  </div>
</div>
<table class="mtable"><thead><tr>
<th style="width:24px"><input type="checkbox" id="mgrSelectAll" onchange="selectAllMgr(this.checked)"></th>
<th style="width:30px">#</th><th style="min-width:140px">Key</th><th style="">URL</th><th style="width:50px">状态码</th><th style="width:130px">重置</th><th style="width:50px">优先</th><th style="width:80px">指定模型</th><th style="width:80px">覆盖模型</th><th style="max-width:80px">备注</th><th style="width:80px"></th>
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
  <div style="color:#94a3b8;padding:4px 0">📅 固定时间检测</div>
  <div><label><input type="checkbox" id="cfgAutoRecoverDaily"> 每 <input id="cfgAutoDailyDays" type="number" min="1" max="365" style="width:40px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 天 <input id="cfgAutoDailyTime" type="time" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 固定检测</label></div>
  <div style="color:#94a3b8;padding:4px 0">🔢 检测的失败码</div>
  <div><input id="cfgAutoCodes" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100%" placeholder="401,402,403,429,500,502,503,504" title="401=API Key 无效或已过期&#10;402=额度不足，账号已欠费&#10;403=权限不足，Key 无访问权限&#10;429=请求过频繁，触发了速率限制&#10;500=上游服务器内部错误&#10;502=上游网关错误&#10;503=服务暂时不可用&#10;504=上游超时"></div>
  <div style="color:#94a3b8;padding:4px 0">🚫 包含 discarded Key</div>
  <div><label><input type="checkbox" id="cfgAutoDiscarded"> 连续两周期失败的也检测</label></div>
  <div style="color:#94a3b8;padding:4px 0">🔁 轮询均摊流量</div>
  <div><label><input type="checkbox" id="cfgRoundRobin"> 启用后可用 key 按优先层层轮流使用，而非固定顺序</label></div>
  <div style="color:#94a3b8;padding:4px 0;grid-column:1/-1;border-bottom:1px solid #334155;margin-bottom:4px">📋 日志配置</div>
  <div style="color:#94a3b8;padding:4px 0">启用文件日志</div>
  <div><label><input type="checkbox" id="cfgLogFile" checked> 保留 <input id="cfgLogRetention" type="number" min="0" max="365" style="width:40px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 天自动清理</label></div>
  <div style="color:#94a3b8;padding:4px 0">日志详情级别</div>
  <div><label><select id="cfgLogDetail" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"><option value="full">完整</option><option value="basic">简洁</option></select> 简洁模式不记录模型名</label></div>
  <div style="color:#94a3b8;padding:4px 0">🔒 连续失败锁死阈值</div>
  <div><input id="cfgLockCount" type="number" min="1" max="20" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:60px" value="3" title="连续 N 次失败后自动锁死该 Key"> 次</div>
  <div style="color:#94a3b8;padding:4px 0">🎯 锁死监控错误码</div>
  <div><input id="cfgLockCodes" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100%" value="401,403" placeholder="401,403" title="只有这些错误码会计入连续失败计数"></div>
  <div style="color:#94a3b8;padding:4px 0">🔒 启用自动锁死</div>
  <div><label><input type="checkbox" id="cfgEnableAutoLock" checked> 开启后连续失败达到阈值将自动锁死 Key</label></div>
</div>
<div style="font-size:11px;color:#64748b;margin-bottom:4px" id="cfgAutoCountdown">⏳ 下次检测（间隔）: --</div>
<div style="font-size:11px;color:#64748b;margin-bottom:8px" id="cfgAutoDailyCountdown">⏳ 下次检测（固定）: --</div>
<div class="mfoot"><button class="btn" onclick="restartProxy()" style="color:#f87171">🔄 重启代理</button><div style="flex:1"></div><button class="btn btn-p" onclick="saveConfig()">保存</button></div>
</div></div>

<div class="modal" id="logModal">
<div class="mcontent">
<div class="mtitle"><span>实时请求日志</span><button class="btn" onclick="closeLogs()">✕</button></div>
<div style="font-size:11px;color:#94a3b8;margin-bottom:6px">最近 2000 条请求记录，实时推送</div>
<div class="log-filters" style="display:flex;gap:6px;margin-bottom:6px;flex-wrap:wrap;align-items:center">
<input id="logKeyFilter" placeholder="Key #" style="width:50px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
<input id="logStatusFilter" placeholder="状态码" style="width:60px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
<input id="logModelFilter" placeholder="模型" style="width:100px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
<select id="logTimeFilter" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
<option value="">全部时间</option><option value="5">最近 5 分钟</option><option value="15">最近 15 分钟</option><option value="60">最近 1 小时</option>
</select>
<button class="btn" onclick="reloadLogs()" style="font-size:11px;padding:2px 8px">🔍 搜索</button>
<button class="btn" onclick="exportLogs()" style="font-size:11px;padding:2px 8px">⬇ CSV</button>
</div>
<div style="overflow-x:auto"><table class="log-table"><thead><tr>
<th>时间</th><th>#</th><th>方法</th><th>模型</th><th>路径</th><th>状态</th><th>↑B</th><th>↓B</th><th>耗时</th><th>首字节</th>
</tr></thead><tbody id="logBody"></tbody></table></div>
</div></div>

<script>
const L={"daily":"每日","weekly":"每周","never":"永久"};
const C={"daily":"bd-daily","weekly":"bd-weekly","never":"bd-never"};
let data=[],curDate="",fullKeys={};
let sortBy="idx",filterBy="all",trendRange="24h",searchQ="",statusCodeQ="",modelSQ="";
let ws=null,wsReconnectTimer=null,pollTimer=null;
let wsFailed=false;
let autoRecoverNextTime=0,autoRecoverDailyNextTime=0;
let collapsedCards={};

async function httpLoad(){
  try{
    const r=await fetch("http://localhost:3456/__status");
    if(!r.ok)throw new Error("HTTP "+r.status);
    const j=await r.json();data=j.keys||j;boostedIdx=j.boostedIdx||-1;render();
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
      if(msg.type==="status"){data=msg.data;boostedIdx=msg.boostedIdx||-1;render()}
      if(msg.type==="notification"&&msg.notificationType==="all_keys_failed"){showAlert("所有 Key 均不可用！");playAlert();sendDesktop()}
      if(msg.type==="log"&&document.getElementById("logModal").classList.contains("on")){
        const tbody=document.getElementById("logBody");
        if(tbody){const tr=document.createElement("tr");tr.innerHTML=makeLogRow(msg.data);tbody.insertBefore(tr,tbody.firstChild);if(tbody.children.length>500)tbody.removeChild(tbody.lastChild)}
      }
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
    document.getElementById("cfgAutoRecoverDaily").checked=c.autoRecoverDaily===true;
    document.getElementById("cfgAutoDailyDays").value=c.autoRecoverDailyDays||1;
    const dh=c.autoRecoverDailyHour!=null?c.autoRecoverDailyHour:8,dm=c.autoRecoverDailyMinute!=null?c.autoRecoverDailyMinute:0;
    document.getElementById("cfgAutoDailyTime").value=String(dh).padStart(2,"0")+":"+String(dm).padStart(2,"0");
    document.getElementById("cfgAutoCodes").value=(c.autoRecoverCodes||[401,402,403,429,500,502,503,504]).join(",");
    document.getElementById("cfgAutoDiscarded").checked=c.autoRecoverDiscarded===true;
    document.getElementById("cfgRoundRobin").checked=c.roundRobin===true;
    document.getElementById("cfgLockCount").value=c.lockAfterFailCount||3;
    document.getElementById("cfgLockCodes").value=(c.lockFailCodes||["401","403"]).join(",");
    document.getElementById("cfgLogFile").checked=c.logFile!==false;
    document.getElementById("cfgLogRetention").value=c.logRetentionDays||7;
    document.getElementById("cfgLogDetail").value=c.logDetail||"full";
    document.getElementById("cfgEnableAutoLock").checked=c.enableAutoLock!==false;
    if(c.autoRecoverNextTime)autoRecoverNextTime=parseInt(c.autoRecoverNextTime);else autoRecoverNextTime=0;
    if(c.autoRecoverDailyNextTime)autoRecoverDailyNextTime=parseInt(c.autoRecoverDailyNextTime);else autoRecoverDailyNextTime=0;
    if(window.autoCountTimer)clearInterval(window.autoCountTimer);
    window.autoCountTimer=setInterval(updateAutoCountdown,1000);
    updateAutoCountdown();
  }catch(e){}
}
function updateAutoCountdown(){
  const el=document.getElementById("cfgAutoCountdown");
  if(el){
    if(!autoRecoverNextTime||autoRecoverNextTime<=Date.now()){el.textContent="⏳ 下次检测（间隔）: --";}
    else{const diff=Math.ceil((autoRecoverNextTime-Date.now())/1000);const h=Math.floor(diff/3600),m=Math.floor((diff%3600)/60),s=diff%60;el.textContent="⏳ 下次检测（间隔）: "+h+"h "+String(m).padStart(2,"0")+"m "+String(s).padStart(2,"0")+"s";}
  }
  const dailyEl=document.getElementById("cfgAutoDailyCountdown");
  if(dailyEl){
    if(!autoRecoverDailyNextTime||autoRecoverDailyNextTime<=Date.now()){dailyEl.textContent="⏳ 下次检测（固定）: --";}
    else{const diff=Math.ceil((autoRecoverDailyNextTime-Date.now())/1000);const days=Math.floor(diff/86400);const h=Math.floor((diff%86400)/3600),m=Math.floor((diff%3600)/60),s=diff%60;dailyEl.textContent="⏳ 下次检测（固定）: "+days+"d "+h+"h "+String(m).padStart(2,"0")+"m "+String(s).padStart(2,"0")+"s";}
  }
}

function renderTrend(){
  const now=new Date();
  const hours=trendRange==="7d"?168:(trendRange==="30d"?720:24);
  const hMap={};
  for(let i=hours-1;i>=0;i--){
    const d=new Date(now-i*3600000);
    const y=d.getFullYear(),m=String(d.getMonth()+1).padStart(2,"0"),dd=String(d.getDate()).padStart(2,"0"),hh=String(d.getHours()).padStart(2,"0");
    hMap[y+"-"+m+"-"+dd+"-"+hh]={bytes:0,input:0,output:0,req:0,keys:{}};
  }
  for(const a of data){
    if(!a.hourly)continue;
    const ai=a.idx;
    for(const [hk,v] of Object.entries(a.hourly)){
      if(hMap[hk]===undefined)continue;
      const h=hMap[hk];
      const ib=v.inputBytes||0,ob=v.outputBytes||0;
      h.input+=ib;h.output+=ob;h.bytes+=ib+ob;h.req+=v.requests||0;
      if(!h.keys[ai])h.keys[ai]={bytes:0,req:0};
      h.keys[ai].bytes+=ib+ob;
      h.keys[ai].req+=v.requests||0;
    }
  }
  const keys=Object.keys(hMap);
  const vals=keys.map(k=>hMap[k].bytes);
  const max=Math.max(...vals,1);
  const bars=document.getElementById("trendBars");
  const labels=document.getElementById("trendLabels");
  bars.innerHTML=keys.map((k,i)=>{
    const h=hMap[k];
    const lines=[];
    const mmdd=k.slice(0,10),hh=k.slice(11);
    lines.push(mmdd+" "+hh+":00~"+String(Number(hh)+1).padStart(2,"0")+":00");
    lines.push("合计: ↑"+fmtBytes(h.input)+" / ↓"+fmtBytes(h.output)+" | "+h.req+"次");
    const kidx=Object.keys(h.keys).sort((a,b)=>h.keys[b].bytes-h.keys[a].bytes);
    for(const ki of kidx){
      const kv=h.keys[ki];
      lines.push("  #"+ki+"  "+fmtBytes(kv.bytes)+"  "+kv.req+"次");
    }
    return '<div class="trend-bar" style="height:'+Math.max(2,h.bytes/max*80)+'px" title="'+esc(lines.join("\\n"))+'"></div>';
  }).join("");
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
  const activeData=data.filter(x=>!x.shielded);
  const tot=activeData.length,ok=activeData.filter(x=>x.available&&!x.locked).length,fail=activeData.filter(x=>!x.available&&!x.locked).length,locked=activeData.filter(x=>x.locked).length;
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
    (locked?'<div class="sum-item" style="background:#7c3aed20;border:1px solid #7c3aed40"><div class="sum-num" style="color:#a78bfa">'+locked+'</div><div class="sum-label" style="color:#a78bfa">🔒 锁死</div></div>':'')+
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
  if(filterBy!=="locked")filtered=filtered.filter(x=>!x.locked);
  if(filterBy!=="shielded")filtered=filtered.filter(x=>!x.shielded);
  if(filterBy==="available")filtered=filtered.filter(x=>x.available);
  else if(filterBy==="cooldown")filtered=filtered.filter(x=>!x.available&&x.status!=="discarded");
  else if(filterBy==="discarded")filtered=filtered.filter(x=>x.status==="discarded");
  else if(filterBy==="shielded")filtered=filtered.filter(x=>x.shielded);
  if(searchQ){const q=searchQ.toLowerCase();filtered=filtered.filter(x=>String(x.idx).includes(q)||(x.remark||"").toLowerCase().includes(q)||x.url.toLowerCase().includes(q)||(x.models||[]).some(m=>m.toLowerCase().includes(q)))}
  if(statusCodeQ){filtered=filtered.filter(x=>x.failCode&&String(x.failCode)===statusCodeQ)}
  if(modelSQ){const q=modelSQ.toLowerCase();filtered=filtered.filter(x=>(x.models||[]).some(m=>m.toLowerCase().includes(q)))}
  const resetType=document.getElementById("resetFilter").value;
  if(resetType!=="all")filtered=filtered.filter(x=>x.reset===resetType);
  document.getElementById("filterCount").textContent="显示 "+filtered.length+" / "+data.length+" 个";
  const shieldedCount=data.filter(x=>x.shielded).length;
  if(shieldedCount>0)document.getElementById("filterCount").textContent+="，屏蔽 "+shieldedCount+" 个";
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
    const isBoosted=boostedIdx===a.idx;
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
      else{cd="本周已用完，"+(a.nextResetDay||"周一")+"0点重置"}
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
      (isBoosted?' <span class="badge" style="background:#1a3a2e;color:#4ade80;border:1px solid #22c55e">⚡ 已优先</span>':'')+
      ' <span class="badge bd-score">'+score+'分</span>'+
      '<span class="btn" style="padding:0 4px;font-size:9px" onclick="toggleCollapse('+a.idx+')" title="折叠">▼</span></span></div>'+
      '<div class="meter"><div class="meter-fill" style="width:'+score+'%;background:'+meterColor+'"></div></div>'+
      '<div class="cbody" id="body-'+a.idx+'">'+
      '<div class="row"><span class="label">Key</span><span class="val"><span class="key-mask" data-idx="'+a.idx+'" onclick="var i=this.dataset.idx,f=fullKeys[i];if(!f){loadKeys();var t=this;setTimeout(function(){f=fullKeys[i];if(f)t.textContent=t.textContent===maskKey(f)?f:maskKey(f)},300)}else this.textContent=this.textContent===maskKey(f)?f:maskKey(f)">'+a.key+'</span></span></div>'+
      '<div class="row"><span class="label">地址</span><span class="val uurl">'+esc(a.url)+'</span></div>'+
      rg+
      (a.models&&a.models.length?'<div class="row"><span class="label">指定模型</span><span class="val">'+esc(a.models.join(', '))+'</span></div>':'<div class="row"><span class="label">指定模型</span><span class="val" style="color:#64748b">通用</span></div>')+
      (a.model?'<div class="row"><span class="label">覆盖模型</span><span class="val" style="color:#fbbf24">'+esc(a.model)+'</span></div>':"")+
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
      (a.available&&!isDiscard?'<span class="btn-act'+(isBoosted?' boost-on':'')+'" onclick="boostKey('+a.idx+')" title="'+(isBoosted?'点击取消优先':'下一个请求优先使用此 Key')+'">'+(isBoosted?'✅':'⚡')+'</span>':'')+
      '<span class="btn-act" onclick="cardTest('+a.idx+')" title="测试连通性">🔍</span>'+
      '</span></div></div>';
  }
  document.getElementById("grid").innerHTML=html;
  updateBatchBar();
  for(const idx in collapsedCards){const b=document.getElementById("body-"+idx);if(b)b.classList.toggle("collapsed",collapsedCards[idx])}
}

function toggleCollapse(idx){
  const body=document.getElementById("body-"+idx);
  if(body){body.classList.toggle("collapsed");collapsedCards[idx]=body.classList.contains("collapsed")}
}

function todayStr(){return new Date().toISOString().slice(0,10)}
function fmtBytes(n){if(!n)return"0B";if(n>=1048576)return(n/1048576).toFixed(1)+"MB";if(n>=1024)return(n/1024).toFixed(1)+"KB";return n+"B"}
function fmtDur(ms){if(ms>=1000)return(ms/1000).toFixed(2)+"s";return ms+"ms"}
function maskKey(k){return k&&k.length>12?k.slice(0,6)+'...'+k.slice(-4):(k||'')}
function esc(s){const d=document.createElement("div");d.textContent=s;return d.innerHTML}
const FAIL_MEAN={"401":"API Key 无效或已过期","402":"额度不足，账号已欠费","403":"权限不足，Key 无访问权限","429":"请求过频繁，触发了速率限制","500":"上游服务器内部错误","502":"上游网关错误","503":"服务暂时不可用","504":"上游超时"};
function toggleAllCollapse(){
  const all=document.querySelectorAll("#grid .cbody");
  if(!all.length)return;
  const isCollapsed=all[0].classList.contains("collapsed");
  all.forEach(b=>{b.classList.toggle("collapsed",!isCollapsed);const idx=b.id.replace("body-","");collapsedCards[idx]=!isCollapsed});
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
function boostKey(idx){
  fetch("http://localhost:3456/__boost-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx})})
    .then(r=>r.json()).catch(()=>{});
}
loadKeys();connectWS();if(Notification.permission==="default")Notification.requestPermission();
setTimeout(function(){if(!data.length)httpLoad()},3000);

document.getElementById("sortBy").addEventListener("change",function(){sortBy=this.value;render()});
document.getElementById("filterBy").addEventListener("change",function(){filterBy=this.value;render()});
document.getElementById("resetFilter").addEventListener("change",function(){render()});
document.getElementById("trendRange").addEventListener("change",function(){trendRange=this.value;render()});
document.getElementById("searchBox").addEventListener("input",function(){searchQ=this.value;render()});
document.getElementById("statusCodeBox").addEventListener("input",function(){statusCodeQ=this.value.trim();render()});
document.getElementById("modelSearchBox").addEventListener("input",function(){modelSQ=this.value.trim();render()});

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
let mgrSearchCache=[],dragIdx=-1,grpCache=null;
let mgrCollapsed={},mgrCollapsedExpandedAll=true;
function toggleGroup(g){
  mgrCollapsed[g]=!mgrCollapsed[g];
  renderMgr();
}
function toggleAllMgrGroups(){
  const groups=Object.keys(grpCache||{});
  const allCollapsed=mgrCollapsedExpandedAll;
  groups.forEach(g=>mgrCollapsed[g]=allCollapsed);
  mgrCollapsedExpandedAll=!allCollapsed;
  renderMgr();
}
function unlockKey(i){
  if(!confirm('解锁 #'+(i+1)+'？将清除锁死状态，Key 恢复正常使用。'))return;
  fetch("http://localhost:3456/__reset-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx:i+1})})
    .then(r=>r.json()).then(j=>{if(j.ok){mgrKeys[i]._locked=undefined;renderMgr();loadKeys()}});
}
function renderMgr(){
  const q=(document.getElementById("mgrSearch").value||"").toLowerCase();
  const codeFilter=document.getElementById("mgrCodeFilter").value.trim();
  const mf=(document.getElementById("mgrModelFilter").value||"").toLowerCase().trim();
  const statusFilter=document.getElementById("mgrStatusFilter").value;
  const tbody=document.getElementById("mgrBody");
  tbody.innerHTML="";
  const filtered=[];const grp={};
  for(let i=0;i<mgrKeys.length;i++){
    const k=mgrKeys[i];
    if(q&&!(k.remark||"").toLowerCase().includes(q)&&!(k.url||"").toLowerCase().includes(q)&&!(k.key||"").toLowerCase().includes(q)&&!String(k._failCode||"").includes(q))continue;
    if(codeFilter&&String(k._failCode||"")!==codeFilter)continue;
    if(mf&&!((k.models||k._models||[])||[]).some(m=>m.toLowerCase().includes(mf)))continue;
    if(statusFilter==="available"&&k._available!==true)continue;
    if(statusFilter==="cooldown"&&k._available!==false)continue;
    if(statusFilter==="discarded"&&k.status!=="discarded")continue;
    if(statusFilter==="locked"&&k._locked!==true)continue;
    if(statusFilter==="shielded"&&k.status!=="shielded")continue;
    filtered.push(i);
    const g=(k.remark||"").split(/[，,\s]/)[0]||(k.url||"").replace(/https?:\\/\\//,"").slice(0,16)||"未分类";
    if(!grp[g])grp[g]=[];
    grp[g].push(i);
  }
  grpCache=grp;
  mgrSearchCache=filtered;
  document.getElementById("mgrCount").textContent="共 "+mgrKeys.length+" 个"+(filtered.length<mgrKeys.length?"，筛选后 "+filtered.length+" 个":"");
  const groups=Object.keys(grp);
  for(let gi=0;gi<groups.length;gi++){
    const g=groups[gi],items=grp[g];
    const collapsed=mgrCollapsed[g]===true;
    const hdr=document.createElement("tr");
    hdr.style.background="#1e293b";hdr.style.cursor="pointer";
    hdr.onclick=function(){toggleGroup(g)};
    hdr.innerHTML='<td colspan="11" style="padding:6px 8px;font-size:11px;font-weight:600;border-bottom:1px solid #334155;user-select:none">'+
      (collapsed?'▶':'▼')+' '+esc(g)+' ('+items.length+')</td>';
    tbody.appendChild(hdr);
    if(collapsed)continue;
    for(let ii=0;ii<items.length;ii++){
      const i=items[ii],k=mgrKeys[i],sh=k.status==="shielded",lk=k._locked===true;
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
      let badges='';
      if(sh)badges+='<span class="badge" style="background:#3b1f1e;color:#f87171;white-space:nowrap">已屏蔽</span>';
      if(lk)badges+='<span class="badge" style="background:#2e1065;color:#a78bfa;white-space:nowrap;margin-left:2px">🔒 锁死</span>';
      const fc=k._failCode||"";
      const fcBadge=fc?'<span class="badge" style="background:#1e293b;color:'+(fc==="429"?"#fbbf24":fc==="401"?"#fb923c":fc==="403"?"#f87171":"#94a3b8")+';border:1px solid #475569">'+fc+'</span>':'';
      tr.innerHTML='<td><input type="checkbox" class="mgr-cb" value="'+i+'"></td>'+
        '<td>'+(i+1)+'</td>'+
        '<td style="display:flex;align-items:center;gap:4px"><input class="kkey" value="'+esc(k.key||"")+'" placeholder="sk-..." style="flex:1">'+badges+'</td>'+
        '<td><input class="kurl" value="'+esc(k.url||"")+'" placeholder="https://..." style="width:100%"></td>'+
        '<td style="text-align:center">'+fcBadge+'</td>'+
        '<td style="display:flex;gap:4px;align-items:center">'+
        '<select class="kreset" onchange="var d=this.parentNode.querySelector(\\'.kresetday\\');d.style.display=this.value===\\'weekly\\'?\\'inline-block\\':\\'none\\'"><option value="daily"'+(k.reset==="daily"?" selected":"")+'>每日</option><option value="weekly"'+(k.reset==="weekly"?" selected":"")+'>每周</option><option value="never"'+(k.reset==="never"?" selected":"")+'>永久</option></select>'+
        '<select class="kresetday" style="display:'+(k.reset==="weekly"?"inline-block":"none")+';width:60px;font-size:10px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px">'+
          '<option value="">自动</option>'+
          '<option value="1"'+(k.resetDay=="1"?" selected":"")+'>周一</option>'+
          '<option value="2"'+(k.resetDay=="2"?" selected":"")+'>周二</option>'+
          '<option value="3"'+(k.resetDay=="3"?" selected":"")+'>周三</option>'+
          '<option value="4"'+(k.resetDay=="4"?" selected":"")+'>周四</option>'+
          '<option value="5"'+(k.resetDay=="5"?" selected":"")+'>周五</option>'+
          '<option value="6"'+(k.resetDay=="6"?" selected":"")+'>周六</option>'+
          '<option value="7"'+(k.resetDay=="7"?" selected":"")+'>周日</option>'+
        '</select></td>'+
        '<td><input class="kprio" type="number" value="'+(k.priority||0)+'" style="width:40px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;text-align:center" min="0" title="数值越大优先级越高，启用轮询后生效"></td>'+
        '<td><input class="kmodels" value="'+esc((k.models||[]).join(', '))+'" placeholder="指定模型名" style="width:80px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px" title="逗号分隔，如 gpt-5.5, gpt-5.4-mini"></td>'+
        '<td><input class="kmodel" value="'+esc(k.model||"")+'" placeholder="覆盖模型" style="width:80px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px" title="非空时转发请求时强制替换 model 为此值"></td>'+
        '<td><input class="kremark" value="'+esc(k.remark||"")+'" placeholder="备注" style="width:100%"></td>'+
        '<td style="display:flex;gap:4px;align-items:center;white-space:nowrap">'+
          '<span class="del" onclick="testKey('+i+')" title="#'+(i+1)+' 测试连通性">🔍</span>'+
          '<span class="del" onclick="resetKeyStatus('+i+')" title="#'+(i+1)+' 重置状态（清除冷却/废弃/锁死）">🔄</span>'+
          '<span class="del" onclick="toggleShield('+i+')" title="#'+(i+1)+' '+(sh?'恢复使用':'屏蔽')+'">'+(sh?'🔄':'🔇')+'</span>'+
          (lk?'<span class="del" onclick="unlockKey('+i+')" title="#'+(i+1)+' 解锁 Key" style="color:#a78bfa">🔓</span>':'')+
          '<span class="del" onclick="delKeyRow('+i+')" title="#'+(i+1)+' 删除">✕</span></td>';
      tbody.appendChild(tr);
    }
  }
  document.getElementById("mgrSelectAll").checked=false;
}
function addKeyRow(){mgrKeys.push({key:"",url:"",reset:"weekly",remark:"",priority:0,models:[],model:null,resetDay:void 0});renderMgr()}
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
function clearMgrSearch(){
  document.getElementById("mgrSearch").value="";
  document.getElementById("mgrCodeFilter").value="";
  document.getElementById("mgrModelFilter").value="";
  document.getElementById("mgrStatusFilter").value="";
  renderMgr();
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
  const result=mgrKeys.map(k=>({key:k.key,url:k.url,reset:k.reset,remark:k.remark||"",priority:k.priority||0,models:k.models||[],model:k.model||null,resetDay:k.resetDay||void 0,status:k.status&&k.status!=="active"?k.status:void 0}));
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
    const resetDayEl=r.querySelector(".kresetday");
    result[sidx].resetDay=resetDayEl?resetDayEl.value||void 0:void 0;
    const prioEl=r.querySelector(".kprio");
    result[sidx].priority=prioEl?parseInt(prioEl.value)||0:result[sidx].priority;
    const modelsEl=r.querySelector(".kmodels");
    const raw=modelsEl?modelsEl.value.trim():"";
    result[sidx].models=raw?raw.split(",").map(s=>s.trim()).filter(Boolean):[];
    const modelEl=r.querySelector(".kmodel");
    result[sidx].model=modelEl?modelEl.value.trim()||null:result[sidx].model;
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
let batchTestPassed=[],batchTestResults=[];
async function batchTestMgr(){
  const sel=getSelectedMgr();
  if(!sel.length){alert("请先勾选要测试的 Key");return}
  const area=document.getElementById("batchTestResults");
  const list=document.getElementById("batchTestList");
  const summary=document.getElementById("batchTestSummary");
  const resetBtn=document.getElementById("batchTestResetBtn");
  const resetAllBtn=document.getElementById("batchTestResetAllBtn");
  if(!area||!list)return;
  batchTestPassed=[];batchTestResults=[];
  area.style.display="block";
  list.innerHTML="";
  summary.textContent="测试中...";
  resetBtn.style.display="none";
  if(resetAllBtn)resetAllBtn.style.display="none";
  for(const i of sel){
    const k=mgrKeys[i];
    const line=document.createElement("div");
    line.id="btr-"+i;
    if(!k||!k.key){line.textContent="⏭️ #"+(i+1)+" Key 为空，跳过";list.appendChild(line);continue}
    batchTestResults.push({idx:i, ok:false, status:null});
    line.textContent="⏳ #"+(i+1)+" 测试中...";
    list.appendChild(line);
    try{
      const r=await fetch("http://localhost:3456/__test-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({key:k.key,url:k.url})});
      const j=await r.json();
      const cr=batchTestResults[batchTestResults.length-1];
      if(j.ok){
        batchTestPassed.push(i);
        if(cr){cr.ok=true;cr.status=j.status}
        line.textContent="✅ #"+(i+1)+" 成功"+(j.model?" 模型: "+j.model:"")+(j.duration?" ("+j.duration+"ms)":"");
      }else{
        if(cr)cr.status=j.status||null;
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
  if(resetAllBtn&&batchTestResults.length>0){resetAllBtn.style.display="inline-block";resetAllBtn.textContent="🔄 重置所有 Key 的状态码 ("+batchTestResults.length+"个)"}
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
async function batchTestResetAll(){
  if(!batchTestResults.length)return;
  for(const r of batchTestResults){
    try{
      await fetch("http://localhost:3456/__apply-test-result",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx:r.idx+1,failCode:r.ok?null:r.status})})
    }catch(e){}
  }
  batchTestResults=[];
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
function buildLogFilterQuery(){
  const p=new URLSearchParams();
  const k=document.getElementById("logKeyFilter")?.value.trim();
  if(k)p.set("key",k);
  const s=document.getElementById("logStatusFilter")?.value.trim();
  if(s)p.set("status",s);
  const m=document.getElementById("logModelFilter")?.value.trim();
  if(m)p.set("model",m);
  const t=document.getElementById("logTimeFilter")?.value;
  if(t){const ms=parseInt(t)*60000;p.set("since",Date.now()-ms)}
  p.set("limit","500");
  return p.toString();
}
async function loadLogs(){
  try{
    const q=buildLogFilterQuery();
    const r=await fetch("http://localhost:3456/__logs?"+q);
    if(!r.ok)return;
    const logs=await r.json();
    renderLogs(logs);
  }catch(e){}
}
function reloadLogs(){loadLogs()}
function exportLogs(){
  const q=buildLogFilterQuery();
  window.open("http://localhost:3456/__logs?"+q+"&format=csv");
}
function makeLogRow(e){
  const s=e.status||0;
  const sc="log-s"+s;
  const tm=new Date(e.time);
  const ts=String(tm.getHours()).padStart(2,"0")+":"+String(tm.getMinutes()).padStart(2,"0")+":"+String(tm.getSeconds()).padStart(2,"0");
  const mdl=e.overrideModel||e.reqModel||"";
  return '<td class="log-time">'+ts+'</td><td>#'+e.idx+'</td><td>'+e.method+'</td><td style="max-width:80px;overflow:hidden;text-overflow:ellipsis" title="'+esc(mdl)+'">'+esc(mdl)+'</td><td style="max-width:200px;overflow:hidden;text-overflow:ellipsis">'+esc(e.path)+'</td><td class="log-status '+sc+'">'+s+'</td><td>'+fmtBytes(e.inputBytes||0)+'</td><td>'+fmtBytes(e.outputBytes||0)+'</td><td class="log-dur">'+fmtDur(e.duration||0)+'</td><td class="log-dur">'+(e.ttfb?fmtDur(e.ttfb):"-")+'</td>';
}
function renderLogs(logs){
  const tbody=document.getElementById("logBody");
  tbody.innerHTML=logs.slice().reverse().map(e=>{
    return '<tr>'+makeLogRow(e)+'</tr>';
  }).join("")||'<tr><td colspan="10" style="text-align:center;color:#64748b;padding:20px">暂无请求记录</td></tr>';
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
    autoRecoverDaily:document.getElementById("cfgAutoRecoverDaily").checked,
    autoRecoverDailyDays:parseInt(document.getElementById("cfgAutoDailyDays").value)||1,
    autoRecoverDailyHour:(n=>isNaN(n)?8:n)(parseInt((document.getElementById("cfgAutoDailyTime").value||"08:00").split(":")[0])),
    autoRecoverDailyMinute:(n=>isNaN(n)?0:n)(parseInt((document.getElementById("cfgAutoDailyTime").value||"08:00").split(":")[1])),
    autoRecoverCodes:(document.getElementById("cfgAutoCodes").value||"").split(",").map(s=>parseInt(s.trim())).filter(n=>!isNaN(n)),
    autoRecoverDiscarded:document.getElementById("cfgAutoDiscarded").checked,
    roundRobin:document.getElementById("cfgRoundRobin").checked,
    enableAutoLock:document.getElementById("cfgEnableAutoLock").checked,
    lockAfterFailCount:parseInt(document.getElementById("cfgLockCount").value)||3,
    lockFailCodes:(document.getElementById("cfgLockCodes").value||"").split(",").map(s=>s.trim()).filter(s=>s),
    logFile:document.getElementById("cfgLogFile").checked,
    logRetentionDays:parseInt(document.getElementById("cfgLogRetention").value)||7,
    logDetail:document.getElementById("cfgLogDetail").value
  };
  try{
    const r=await fetch("http://localhost:3456/__config",{method:"PUT",headers:{"content-type":"application/json"},body:JSON.stringify(c)});
    const j=await r.json();
    if(j.ok){closeConfig()}else{document.getElementById("configStatus").textContent="保存失败"};
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
    const data = buildStatusData();
    res.end(JSON.stringify({ keys: data, boostedIdx: _boostKey >= 0 ? _boostKey + 1 : -1 }, null, 2));
    return;
  }

  const cors = { "access-control-allow-origin": "*", "content-type": "application/json; charset=utf-8" };

  if (pathname === "/__config") {
    if (req.method === "GET") {
      res.writeHead(200, cors);
      res.end(JSON.stringify({ ...config, autoRecoverNextTime, autoRecoverDailyNextTime }, null, 2));
      return;
    }
    if (req.method === "PUT") {
      const bodyChunks = [];
      req.on("data", c => bodyChunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(bodyChunks).toString('utf-8');
        try {
          const c = JSON.parse(body);
          const cur = JSON.parse(fs.readFileSync(CONFIG_FILE, "utf-8"));
          Object.assign(cur, c);
          fs.writeFileSync(CONFIG_FILE, JSON.stringify(cur, null, 2));
          const savedNextTime = autoRecoverNextTime;
          const savedDailyNextTime = autoRecoverDailyNextTime;
          const savedInterval = config.autoRecoverInterval;
          const savedDailyDays = config.autoRecoverDailyDays;
          const savedDailyHour = config.autoRecoverDailyHour;
          const savedDailyMin = config.autoRecoverDailyMinute;
          loadConfig();
          if (config.autoRecover && savedNextTime > Date.now() && savedInterval === config.autoRecoverInterval) {
            autoRecoverNextTime = savedNextTime;
          }
          if (config.autoRecoverDaily && savedDailyNextTime > Date.now() &&
              savedDailyDays === config.autoRecoverDailyDays &&
              savedDailyHour === config.autoRecoverDailyHour &&
              savedDailyMin === config.autoRecoverDailyMinute) {
            autoRecoverDailyNextTime = savedDailyNextTime;
            if (autoRecoverDailyTimer) clearTimeout(autoRecoverDailyTimer);
            scheduleDailyRecover();
          }
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
      const bodyChunks = [];
      req.on("data", c => bodyChunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(bodyChunks).toString('utf-8');
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
      for (let i = 0; i < raw.length; i++) {
        const ks = i < accounts.length ? getKeyState(i) : state.keys[i];
        if (ks && ks.status === "locked") raw[i]._locked = true;
        if (ks && (ks.failCode || ks.failCode === 0)) raw[i]._failCode = ks.failCode;
        if (i < accounts.length && accounts[i]) { raw[i]._available = !inCooldown(i); if (accounts[i].models && accounts[i].models.length) raw[i]._models = accounts[i].models; }
      }
      res.writeHead(200, cors);
      res.end(JSON.stringify(raw, null, 2));
      return;
    }
    if (req.method === "PUT") {
      const bodyChunks = [];
      req.on("data", c => bodyChunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(bodyChunks).toString('utf-8');
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
      const bodyChunks = [];
      req.on("data", c => bodyChunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(bodyChunks).toString('utf-8');
        try {
          const { idx } = JSON.parse(body);
          const ai = idx - 1;
          if (typeof idx !== "number" || ai < 0 || ai >= accounts.length) throw new Error("invalid idx");
          const ks = getKeyState(ai);
          ks.failCode = null;
          ks.failTime = null;
          ks.failPeriod = "";
          ks.failCount = 0;
          if (ks.status === "discarded" || ks.status === "locked") ks.status = "active";
          allFailedNotified = false;
          saveState(true);
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

  if (pathname === "/__apply-test-result") {
    if (req.method === "POST") {
      const bodyChunks = [];
      req.on("data", c => bodyChunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(bodyChunks).toString('utf-8');
        try {
          const { idx, failCode } = JSON.parse(body);
          if (typeof idx !== "number" || idx < 1 || idx > accounts.length) throw new Error("invalid idx");
          const ai = idx - 1;
          if (failCode && failCode !== 200) {
            markFailure(ai, failCode);
          } else {
            const ks = getKeyState(ai);
            ks.failCode = null;
            ks.failTime = null;
            ks.failPeriod = "";
            ks.failCount = 0;
            if (ks.status === "discarded" || ks.status === "locked") ks.status = "active";
            allFailedNotified = false;
            saveState(true);
            broadcastStatus();
          }
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
      const u = new URL(req.url, "http://x");
      const limit = Math.min(parseInt(u.searchParams.get("limit") || "200", 10), MAX_LOG);
      const key = u.searchParams.get("key");
      const status = u.searchParams.get("status");
      const model = u.searchParams.get("model");
      const since = u.searchParams.get("since");
      const until = u.searchParams.get("until");
      const offset = parseInt(u.searchParams.get("offset") || "0", 10);
      const format = u.searchParams.get("format");
      let entries = requestLog;
      if (since) { const t = parseInt(since, 10); if (t) entries = entries.filter(e => e.time >= t); }
      if (until) { const t = parseInt(until, 10); if (t) entries = entries.filter(e => e.time <= t); }
      if (key) { const keys = key.split(",").map(s => parseInt(s.trim(), 10)).filter(n => !isNaN(n)); if (keys.length) entries = entries.filter(e => keys.includes(e.idx)); }
      if (status) { entries = entries.filter(e => { const s = e.status || 0; if (status.endsWith("xx")) { const prefix = parseInt(status, 10); return !isNaN(prefix) && Math.floor(s / 100) === prefix; } return String(s) === status; }); }
      if (model) { const ml = model.toLowerCase(); entries = entries.filter(e => (e.reqModel||"").toLowerCase().includes(ml) || (e.overrideModel||"").toLowerCase().includes(ml)); }
      entries = entries.slice(-limit - offset, entries.length - offset);
      if (entries.length > limit) entries = entries.slice(entries.length - limit);
      if (format === "csv") {
        const header = "time,idx,method,path,status,inputBytes,outputBytes,duration,ttfb,reqModel,overrideModel";
        const rows = entries.map(e => [e.time, e.idx, e.method, e.path, e.status||0, e.inputBytes||0, e.outputBytes||0, e.duration||0, e.ttfb||"", e.reqModel||"", e.overrideModel||""].join(","));
        res.writeHead(200, { ...cors, "content-type": "text/csv", "content-disposition": "attachment; filename=proxy-logs.csv" });
        res.end(header + "\n" + rows.join("\n"));
        return;
      }
      res.writeHead(200, cors);
      res.end(JSON.stringify(entries, null, 2));
      return;
    }
    res.writeHead(405, cors);
    res.end(JSON.stringify({ error: "method not allowed" }));
    return;
  }

  if (pathname === "/__export-logs") {
    if (req.method === "GET") {
      const u = new URL(req.url, "http://x");
      const date = u.searchParams.get("date");
      const key = u.searchParams.get("key");
      const status = u.searchParams.get("status");
      const model = u.searchParams.get("model");
      const format = u.searchParams.get("format") || "csv";
      const entries = [];
      function filterEntry(e) {
        if (key) { const keys = key.split(",").map(s => parseInt(s.trim(), 10)).filter(n => !isNaN(n)); if (keys.length && !keys.includes(e.idx)) return false; }
        if (status) { const s = e.status || 0; if (status.endsWith("xx")) { const p = parseInt(status, 10); if (isNaN(p) || Math.floor(s / 100) !== p) return false; } else if (String(s) !== status) return false; }
        if (model) { const ml = model.toLowerCase(); if (!(e.reqModel||"").toLowerCase().includes(ml) && !(e.overrideModel||"").toLowerCase().includes(ml)) return false; }
        return true;
      }
      if (date) {
        const filePath = path.join(LOG_DIR, date + ".jsonl");
        try {
          if (fs.existsSync(filePath)) {
            const lines = fs.readFileSync(filePath, "utf-8").trim().split("\n");
            for (const line of lines) {
              if (!line) continue;
              try { const e = JSON.parse(line); if (filterEntry(e)) entries.push(e); } catch(e2) {}
            }
          }
        } catch(e) { /* file not found */ }
      } else {
        for (const e of requestLog) { if (filterEntry(e)) entries.push(e); }
      }
      if (format === "jsonl") {
        res.writeHead(200, { ...cors, "content-type": "application/x-ndjson", "content-disposition": "attachment; filename=proxy-logs.jsonl" });
        res.end(entries.map(e => JSON.stringify(e)).join("\n"));
        return;
      }
      const header = "time,idx,method,path,status,inputBytes,outputBytes,duration,ttfb,reqModel,overrideModel";
      const rows = entries.map(e => [e.time, e.idx, e.method, e.path, e.status||0, e.inputBytes||0, e.outputBytes||0, e.duration||0, e.ttfb||"", e.reqModel||"", e.overrideModel||""].join(","));
      res.writeHead(200, { ...cors, "content-type": "text/csv", "content-disposition": "attachment; filename=proxy-logs.csv" });
      res.end(header + "\n" + rows.join("\n"));
      return;
    }
    res.writeHead(405, cors);
    res.end(JSON.stringify({ error: "method not allowed" }));
    return;
  }

  if (pathname === "/__patch-key-status") {
    if (req.method === "POST") {
      const bodyChunks = [];
      req.on("data", c => bodyChunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(bodyChunks).toString('utf-8');
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

  if (pathname === "/__boost-key") {
    if (req.method === "POST") {
      const bodyChunks = [];
      req.on("data", c => bodyChunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(bodyChunks).toString('utf-8');
        try {
          const { idx } = JSON.parse(body);
          if (typeof idx !== "number" || idx < 1 || idx > accounts.length) throw new Error("invalid idx");
          const ai = idx - 1;
          // Toggle: same idx → clear, different idx → set
          if (_boostKey === ai) {
            _boostKey = -1;
            console.log(`[proxy] #${idx} boost cancelled`);
          } else {
            _boostKey = ai;
            console.log(`[proxy] #${idx} boosted`);
          }
          broadcastStatus();
          res.writeHead(200, cors);
          res.end(JSON.stringify({ ok: true, boosted: _boostKey >= 0 }));
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

  if (pathname === "/__verify-model") {
    if (req.method === "POST") {
      const bodyChunks = [];
      req.on("data", c => bodyChunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(bodyChunks).toString('utf-8');
        try {
          const { idx, key, url } = JSON.parse(body);
          let targetKey, targetUrl;
          if (idx !== undefined) {
            const ai = idx - 1;
            if (ai < 0 || ai >= accounts.length) throw new Error("invalid idx");
            targetKey = accounts[ai].key;
            targetUrl = new URL(accounts[ai].url);
          } else if (key && url) {
            targetKey = key;
            targetUrl = new URL(url);
          } else {
            throw new Error("provide idx or key+url");
          }
          const mod = HTTP_MOD[targetUrl.protocol] || https;
          const probeBody = JSON.stringify({
            model: "gpt-4o",
            messages: [{ role: "user", content: "ok" }],
            max_tokens: 1,
            stream: true,
          });
          const opts = {
            hostname: targetUrl.hostname,
            port: targetUrl.port || (targetUrl.protocol === "http:" ? 80 : 443),
            path: "/v1/chat/completions",
            method: "POST",
            headers: {
              authorization: "Bearer " + targetKey,
              "content-type": "application/json",
              "content-length": Buffer.byteLength(probeBody),
              "user-agent": "OpenAI/Node.js",
              accept: "text/event-stream",
            },
            timeout: 20000,
          };
          const t0 = Date.now();
          let responded = false;
          const probeReq = mod.request(opts, probeRes => {
            const dur = Date.now() - t0;
            let firstChunk = "";
            let model = "";
            let fullData = "";
            probeRes.on("data", c => {
              if (!firstChunk) {
                firstChunk = c.toString();
                const m = firstChunk.match(/"model"\s*:\s*"([^"]+)"/);
                if (m) model = m[1];
              }
              fullData += c;
            });
            probeRes.on("end", () => {
              if (responded) return;
              responded = true;
              if (probeRes.statusCode === 200 && model) {
                res.writeHead(200, cors);
                res.end(JSON.stringify({ ok: true, model, duration: dur }));
              } else if (probeRes.statusCode === 200) {
                res.writeHead(200, cors);
                res.end(JSON.stringify({ ok: true, model: "unknown", duration: dur, raw: fullData.slice(0, 300) }));
              } else {
                res.writeHead(200, cors);
                res.end(JSON.stringify({ ok: false, status: probeRes.statusCode, error: "HTTP " + probeRes.statusCode + ": " + fullData.slice(0, 200) }));
              }
            });
          });
          probeReq.on("error", e => {
            if (responded) return;
            responded = true;
            res.writeHead(200, cors);
            res.end(JSON.stringify({ ok: false, error: "请求失败: " + e.message }));
          });
          probeReq.on("timeout", () => {
            if (responded) return;
            responded = true;
            probeReq.destroy();
            res.writeHead(200, cors);
            res.end(JSON.stringify({ ok: false, error: "超时" }));
          });
          probeReq.write(probeBody);
          probeReq.end();
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

cleanOldLogs();

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
