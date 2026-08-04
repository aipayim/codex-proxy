const crypto = require("crypto");
const http = require("http");
const https = require("https");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { URL } = require("url");
const { Transform } = require("stream");
const { StringDecoder } = require("string_decoder");
const { spawn, spawnSync } = require("child_process");
const { Worker } = require("worker_threads");
const { WebSocketServer } = require("ws");

const PORT = 3456;
const servers = {};
const KEYS_FILE = path.join(__dirname, "keys.json");
const STATE_FILE = path.join(__dirname, "state.json");
const CONFIG_FILE = path.join(__dirname, "config.json");
const RESUME_SCRIPT = path.join(__dirname, "resume-codex.sh");
const AUTO_RESUME_RUNTIME_FILE = path.join(__dirname, ".auto-resume-runtime.json");
const WATCHDOG_RELOAD_FILE = path.join(__dirname, ".watchdog-reload");
const CODEX_SQLITE_LOG_MAINTAINER_FILE = path.join(__dirname, "codex-sqlite-log-maintainer.py");
const TIMEOUT = 1500000;
const PRIORITY = { daily: 0, weekly: 1, never: 2, hourly: 0 };
const HTTP_MOD = { "http:": http, "https:": https };
const TZ = "Asia/Shanghai";
const MAX_LOG = 2000;
const QUEUE_TIMEOUT = 30000;
const RESTART_FORCE_MIN_WAIT_MS = 30000;
let STREAM_LIFETIME = 1800000;
const RESPONSES_TIMEOUT_MAX_MS = 24 * 60 * 60 * 1000;
const RESPONSES_IDLE_TIMEOUT_DEFAULT_MS = 90 * 60 * 1000;
let RESPONSES_STREAM_LIFETIME = 0;
let RESPONSES_IDLE_TIMEOUT = RESPONSES_IDLE_TIMEOUT_DEFAULT_MS;
const RESPONSES_NO_PROGRESS_DEFAULT_MS = 15 * 60 * 1000;
const RESPONSES_NO_PROGRESS_MAX_MS = 24 * 60 * 60 * 1000;
let RESPONSES_NO_PROGRESS_TIMEOUT = RESPONSES_NO_PROGRESS_DEFAULT_MS;
const LOG_DIR = path.join(__dirname, "logs");
const LOG_QUERY_WORKER_FILE = path.join(__dirname, "log-query-worker.js");
const LOG_SUMMARY_FILE = path.join(LOG_DIR, ".log-summary.json");
const LOG_INCIDENT_STATE_FILE = path.join(LOG_DIR, ".log-incidents.json");
const LOG_FILE_NAME_RE = /^(\d{4}-\d{2}-\d{2})(?:\.(\d{1,6}))?\.jsonl$/;
const PID_FILE = path.join(__dirname, "proxy.pid");
const UPSTREAM_REPOSITORY = "aipayim/codex-proxy";
const UPSTREAM_REPOSITORY_URL = "https://github.com/aipayim/codex-proxy";
const UPSTREAM_LATEST_RELEASE_API = "https://api.github.com/repos/aipayim/codex-proxy/releases/latest";
const BUILD_INFO_FILE = path.join(__dirname, "build-info.json");
const RELEASE_MANIFEST_FILE = path.join(__dirname, "release-manifest.json");
const RELEASE_BASELINE_FILE = path.join(__dirname, "release-baseline.txt");
const BUILD_INFO_MAX_BYTES = 16 * 1024;
const RELEASE_MANIFEST_MAX_BYTES = 64 * 1024;
const RELEASE_BASELINE_MAX_BYTES = 4 * 1024;
const RELEASE_INTEGRITY_FILE_MAX_BYTES = 8 * 1024 * 1024;
const AUTO_RESUME_RUNTIME_FILE_MAX_BYTES = 8 * 1024;
const AUTO_RESUME_LAUNCH_TIMEOUT_MS = 120000;
const AUTO_RESUME_STATUS_MAX_BYTES = 64 * 1024;
const AUTO_RESUME_RUNNER_STALL_MINUTES_DEFAULT = 20;
const AUTO_RESUME_RUNNER_STALL_MINUTES_MAX = 1440;
const AUTO_RESUME_RUNNER_MAX_STALL_RESTARTS_DEFAULT = 1;
const AUTO_RESUME_RUNNER_MAX_STALL_RESTARTS_MAX = 3;
const AUTO_RESUME_STALL_TERM_GRACE_MS = 30000;
const GIT_PROBE_TIMEOUT_MS = 1000;
const RELEASE_INTEGRITY_FILES = new Set([
  "proxy.js",
  "package.json",
  "package-lock.json",
  "dashboard.html",
  "install.sh",
  "start-proxy.sh",
  "watchdog.sh",
  "resume-codex.sh",
  "edit-keys.sh",
  "codex-proxy.service",
  "log-query-worker.js",
  "proxy-log-rotator.js",
  "codex-sqlite-log-maintainer.py",
]);
const REQUIRED_RELEASE_INTEGRITY_FILES = new Set(["proxy.js", "package.json", "log-query-worker.js", "proxy-log-rotator.js", "codex-sqlite-log-maintainer.py"]);
const UPDATE_CHECK_TTL_MS = 60 * 60 * 1000;
const UPDATE_CHECK_MIN_REFRESH_MS = 60 * 1000;
const UPDATE_REQUEST_TIMEOUT_MS = 8000;
const UPDATE_MAX_RESPONSE_BYTES = 64 * 1024;
const UPDATE_MAX_RELEASE_NOTES_CHARS = 24000;
const LOG_RECENT_DEFAULT_LIMIT = 50;
const LOG_HISTORY_MAX_LIMIT = 100;
const LOG_EXPORT_MAX_LIMIT = 2000;
const LOG_QUERY_MAX_SCAN_BYTES = 64 * 1024 * 1024;
const LOG_QUERY_TIMEOUT_MS = 30000;
const LOG_QUERY_MAX_QUEUE = 3;
const LOG_ROLLUP_RETENTION_MS = 24 * 60 * 60 * 1000;
const LOG_SUMMARY_SCHEMA = 1;
const LOG_SUMMARY_PERSIST_DELAY_MS = 30000;
const LOG_INCIDENT_PERSIST_DELAY_MS = 3000;
const LOG_INCIDENT_EVALUATION_DELAY_MS = 3000;
const LOG_LATENCY_BOUNDS = [50, 100, 250, 500, 1000, 2500, 5000, 10000, 20000, 30000, 60000, 120000, 300000, 900000, 1800000];
const LOG_DIMENSION_LIMITS = { upstream: 32, model: 48, path: 32, group: 16, key: 64 };
const UPSTREAM_ERROR_CAPTURE_MAX_BYTES = 16 * 1024;
const UPSTREAM_ERROR_CAPTURE_TIMEOUT_MS = 5000;
const LOG_ERROR_MESSAGE_MAX_CHARS = 300;
const DEFAULT_STATE_HOURLY_RETENTION_DAYS = 35;
const DEFAULT_STATE_DAILY_RETENTION_DAYS = 180;
const STATE_HOURLY_RETENTION_MIN_DAYS = 31;
const STATE_HOURLY_RETENTION_MAX_DAYS = 365;
const STATE_DAILY_RETENTION_MIN_DAYS = 30;
const STATE_DAILY_RETENTION_MAX_DAYS = 3650;
const DEFAULT_STATE_MAX_MIB = 32;
const STATE_MAX_MIB_MIN = 4;
const STATE_MAX_MIB_LIMIT = 256;
const STATE_HOURLY_MODEL_LIMIT = 12;
const STATE_HOURLY_MODEL_NAME_MAX_CHARS = 160;
const STATE_HOURLY_OTHER_MODEL = "(其他)";
const STATUS_HOURLY_BUCKET_LIMIT = 31 * 24 + 48;
const STATUS_DAILY_BUCKET_LIMIT = 35;
const STATUS_BROADCAST_INTERVAL_MS = 1000;
const STATUS_HEARTBEAT_MS = 10000;
const STATUS_WS_BUFFER_LIMIT = 4 * 1024 * 1024;
const RATE_WINDOW_MS = 60000;
const STATE_COMPACTION_INTERVAL_MS = 10 * 60 * 1000;
const STATE_COMPACTION_BUCKET_ADD_THRESHOLD = 100;
const DEFAULT_PROXY_LOG_MAX_MIB = 10;
const DEFAULT_PROXY_LOG_KEEP_FILES = 5;
const PROXY_LOG_MAX_MIB_LIMIT = 100;
const PROXY_LOG_KEEP_FILES_LIMIT = 20;
const DEFAULT_LOG_MAX_MIB = 256;
const LOG_MAX_MIB_MIN = 16;
const LOG_MAX_MIB_LIMIT = 4096;
const DEFAULT_LOG_SEGMENT_MAX_MIB = 16;
const LOG_SEGMENT_MAX_MIB_MIN = 1;
const LOG_SEGMENT_MAX_MIB_LIMIT = 256;
const LOG_SUMMARY_MAX_BYTES = 8 * 1024 * 1024;
const LOG_SUMMARY_MEASURE_INTERVAL = 30;
const LOG_ENTRY_MAX_BYTES = 512 * 1024;
const MODEL_PRICING_MAX_RULES = 50;
const MODEL_PRICING_MODEL_MAX_CHARS = 80;
const MODEL_PRICING_PRICE_MAX = 1000000;
const MODEL_PRICING_BPT_MIN = 0.1;
const MODEL_PRICING_BPT_MAX = 100;
const DEFAULT_CODEX_LOG_MAINTENANCE = Object.freeze({
  enabled: false,
  dbPath: path.join(os.homedir(), ".codex", "logs_2.sqlite"),
  thresholdMiB: 2048,
  retainHours: 12,
  checkIntervalMinutes: 15,
});
const CODEX_LOG_MAINTENANCE_THRESHOLD_MIN_MIB = 64;
const CODEX_LOG_MAINTENANCE_THRESHOLD_MAX_MIB = 102400;
const CODEX_LOG_MAINTENANCE_RETAIN_HOURS_MIN = 1;
const CODEX_LOG_MAINTENANCE_RETAIN_HOURS_MAX = 8760;
const CODEX_LOG_MAINTENANCE_INTERVAL_MINUTES_MIN = 5;
const CODEX_LOG_MAINTENANCE_INTERVAL_MINUTES_MAX = 1440;
const CODEX_LOG_MAINTENANCE_INITIAL_DELAY_MS = 5000;
const CODEX_LOG_MAINTENANCE_BUSY_TIMEOUT_MS = 1000;
const CODEX_LOG_MAINTENANCE_BATCH_ROWS = 1000;
const CODEX_LOG_MAINTENANCE_MAX_BATCHES = 5;
const CODEX_LOG_MAINTAINER_TIMEOUT_MS = 30000;
const CODEX_LOG_MAINTENANCE_IDLE_GRACE_MS = 60000;
const CODEX_LOG_MAINTENANCE_CLEAN_TIMEOUT_MS = 180000;
const CODEX_LOG_MAINTENANCE_CLEAN_MAX_BATCHES = 500;
const CODEX_LOG_MAINTAINER_MAX_OUTPUT_BYTES = 32 * 1024;
let LOG_RETENTION_DAYS = 7;
let LOG_FILE_ENABLED = true;
let LOG_DETAIL = "full";
// --- Task Insight (流水任务解析/提炼) ---
const TASK_DIR = path.join(__dirname, "tasks");
const TASK_BUDGET_FILE = path.join(TASK_DIR, ".distill-budget.json");
const TASK_INSIGHT_DEFAULT = Object.freeze({
  enabled: false,
  signals: Object.freeze({ instructions: false, tools: false, usage: false, correlate: false }),
  retentionDays: 30,
  distill: Object.freeze({ enabled: false, engine: "ollama", model: "", baseUrl: "", dailyBudgetYuan: 1, report: "daily" }),
});
const TASK_INSIGHT_RETENTION_MIN_DAYS = 1;
const TASK_INSIGHT_RETENTION_MAX_DAYS = 3650;
const TASK_INSIGHT_INSTRUCTION_MAX_CHARS = 200;
const TASK_INSIGHT_TOOLS_MAX = 40;
const TASK_INSIGHT_FILES_MAX = 40;
const TASK_INSIGHT_MODELS_MAX = 20;
const TASK_INSIGHT_REASONS_MAX = 12;
const TASK_INSIGHT_REQUEST_LOG_MAX = 200;
const TASK_INSIGHT_SESSION_IDLE_MS = 10 * 60 * 1000;
const TASK_INSIGHT_CORRELATE_WINDOW_MS = 45 * 60 * 1000;
const TASK_INSIGHT_SWEEP_MS = 30 * 1000;
const TASK_INSIGHT_MAX_MEMORY_SESSIONS = 2000;
const TASK_SSE_SCAN_BUFFER_MAX = 512 * 1024;
const TASK_SSE_LINE_MAX = 32 * 1024;
const TASK_FILE_SCAN_PREFIX_CHARS = 6000;
const TASK_DISTILL_INTERVAL_MS = 10 * 60 * 1000;
const TASK_DISTILL_TIMEOUT_MS = 120000;
const TASK_DISTILL_MAX_INPUT_CHARS = 6000;
const TASK_DISTILL_BATCH_MAX = 20;
const TASK_READ_MAX_BYTES_PER_FILE = 16 * 1024 * 1024;
const TASK_FILE_NAME_RE = /^(\d{4}-\d{2}-\d{2})\.jsonl$/;
const TASK_DISTILL_ENGINES = ["ollama", "proxy", "external"];
// --- Protocol compatibility hosts ---
const RESPONSES_NATIVE_HOSTS = ["api.openai.com", "api.ofox.ai"];
const MESSAGES_NATIVE_HOSTS = ["api.anthropic.com"];
function isResponsesNative(url) {
  try { return RESPONSES_NATIVE_HOSTS.includes(new URL(url).hostname); } catch(e) { return false; }
}
function isMessagesNative(url) {
  try { return MESSAGES_NATIVE_HOSTS.includes(new URL(url).hostname); } catch(e) { return false; }
}
const CACHE_CONTROL_COMPATIBLE_HOSTS = [
  "dashscope.aliyuncs.com",
  "dashscope-intl.aliyuncs.com",
  "dashscope-us.aliyuncs.com",
];
function supportsCacheControl(url) {
  if (!url) return false;
  try {
    const host = new URL(url).hostname;
    if (CACHE_CONTROL_COMPATIBLE_HOSTS.includes(host)) return true;
    if (host.endsWith(".maas.aliyuncs.com")) return true;
    return false;
  } catch(e) { return false; }
}
function isBailian(url) { return supportsCacheControl(url); }
// --- End protocol hosts ---
let logStream = null;
let logDate = null;
let logStreamBytes = 0;
let logRotationInFlight = false;
const pendingLogLines = [];
let lastLogWriteDropWarningAt = 0;
let logCleanupInFlight = false;

let accounts = [];
let state = { keys: [], activeKey: null };
const activeRequests = {};
const requestLog = [];
const slidingWindows = {};
const pathStats = {};
let requestQueue = [];
let queueProcessing = false;
const INSTANCE_ID = crypto.randomUUID();
const INSTANCE_STARTED_AT = Date.now();
let restartState = { phase: "ready", startedAt: null, id: "", cancelledQueuedRequests: 0, warned: false };
let restartDrainTimer = null;
const updateCheckState = {
  checkedAt: 0,
  lastNetworkCheckAt: 0,
  etag: "",
  latest: null,
  lastError: "",
  inFlight: null,
};
let localBuildProvenance = null;
  let config = { webhookUrl: "", prices: { inputPer1M: 0, outputPer1M: 0 }, bytesPerToken: 3, modelPricing: [], notifications: { sound: true, desktop: true }, roundRobin: false, rateLimit: true, maxRequestsPerMin: 10, maxTokensPerMin: 0, defaultResetHours: 5, autoResume: false, autoResumeIdleMinutes: 10, autoResumeDebounceMinutes: 3, autoResumeRunnerStallMinutes: AUTO_RESUME_RUNNER_STALL_MINUTES_DEFAULT, autoResumeRunnerMaxStallRestarts: AUTO_RESUME_RUNNER_MAX_STALL_RESTARTS_DEFAULT, autoResumeProjects: [], cmdPath: "/mnt/c/Windows/System32/cmd.exe", logMaxMiB: DEFAULT_LOG_MAX_MIB, logSegmentMaxMiB: DEFAULT_LOG_SEGMENT_MAX_MIB, stateHourlyRetentionDays: DEFAULT_STATE_HOURLY_RETENTION_DAYS, stateDailyRetentionDays: DEFAULT_STATE_DAILY_RETENTION_DAYS, stateMaxMiB: DEFAULT_STATE_MAX_MIB, proxyLogMaxMiB: DEFAULT_PROXY_LOG_MAX_MIB, proxyLogKeepFiles: DEFAULT_PROXY_LOG_KEEP_FILES, updateBaselineTag: "", logIncidents: {}, codexLogMaintenance: { ...DEFAULT_CODEX_LOG_MAINTENANCE }, capacityBackoffSeconds: 60, capacityMaxWaitSeconds: 300, responsesStreamLifetime: 0, responsesIdleTimeout: RESPONSES_IDLE_TIMEOUT_DEFAULT_MS, responsesNoProgressTimeout: RESPONSES_NO_PROGRESS_DEFAULT_MS };
let wss = null;
const wsClients = new Set();
let lastBroadcast = "{}";
let statusBroadcastTimer = null;
let statusHeartbeatTimer = null;
let allFailedNotified = false;
let autoRecoverTimer = null;
let autoRecoverNextTime = 0;
let autoRecoverDailyTimer = null;
let autoRecoverDailyNextTime = 0;
let autoRecoverPollTimer = null;
let autoRecoverPollNextTime = 0;
let lastRequestTime = Date.now();
let lastKeyUseTime = 0;
let lastResumeTime = 0;
let taskInsightSweepTimer = null;
let taskDistillTimer = null;
const taskSessions = new Map();
const taskDistillState = { day: "", spentYuan: 0, lastRunAt: 0, running: false, pending: 0, lastError: "" };
let autoResumeTimer = null;
let codexLogMaintenanceTimer = null;
let codexLogMaintenanceInFlight = null;
let codexLogMaintenanceRuntime = {
  phase: "disabled",
  lastCheckAt: 0,
  lastCompletedAt: 0,
  nextCheckAt: 0,
  lastResult: "not_configured",
  lastError: "",
  databaseBytes: 0,
  walBytes: 0,
  totalBytes: 0,
  totalMiB: 0,
  deletedRows: 0,
  batches: 0,
  physicalBytesBefore: 0,
  physicalBytesAfter: 0,
  physicalBytesDelta: 0,
};
const logMinuteBuckets = new Map();
const logDailySummaries = new Map();
const logSummaryPendingEntries = [];
const logIncidents = new Map();
const groupPauses = new Map();
const logWorkerQueue = [];
let activeLogWorkerJob = null;
let logSummaryPersistTimer = null;
let logSummaryPersisting = false;
let logSummaryDirty = false;
let logIncidentPersistTimer = null;
let logIncidentPersisting = false;
let logIncidentDirty = false;
let logIncidentEvaluationTimer = null;
let logSummaryLoaded = false;
let logIncidentsLoaded = false;
let logSummaryRebuildState = { phase: "idle", startedAt: 0, finishedAt: 0, error: "", rebuiltDays: 0 };
let autoResumeStateReady = false;
let autoResumeRuntimeWriteErrorLogged = false;
const autoResumeLaunches = new Map();
let _rrCursor = 0;
let _weeklyLastDay = null;
let _weeklySubCursors = {};
let _boostKey = -1;
let _boostBatch = [];
let _boostBatchMode = "";
let _boostBatchCursor = 0;
let _boostBatchPool = [];
let _boostBatchPoolIdx = 0;
let lastStateCompactionAt = 0;
let stateBucketAddsSinceCompaction = 0;
let lastStateBudgetWarningAt = 0;
let lastLogSummaryBudgetWarningAt = 0;
let stateStatusBucketSelectionCache = new WeakMap();

function getActiveRequestCount() {
  return Object.values(activeRequests).reduce((sum, requests) => sum + (Array.isArray(requests) ? requests.length : 0), 0);
}

function buildRestartStatus(now = Date.now()) {
  const draining = restartState.phase === "draining";
  const restartStartedAt = Number.isFinite(restartState.startedAt) ? restartState.startedAt : null;
  const elapsed = draining && restartStartedAt !== null ? Math.max(0, now - restartStartedAt) : 0;
  return {
    instanceId: INSTANCE_ID,
    startedAt: INSTANCE_STARTED_AT,
    phase: restartState.phase,
    restartStartedAt: restartState.startedAt,
    restartId: restartState.id || "",
    activeRequests: getActiveRequestCount(),
    queuedRequests: requestQueue.length,
    cancelledQueuedRequests: restartState.cancelledQueuedRequests || 0,
    canCancel: draining,
    canForce: draining && elapsed >= RESTART_FORCE_MIN_WAIT_MS,
    forceAvailableInMs: draining ? Math.max(0, RESTART_FORCE_MIN_WAIT_MS - elapsed) : 0,
  };
}

function clearRestartDrainTimer() {
  if (restartDrainTimer !== null) {
    clearTimeout(restartDrainTimer);
    restartDrainTimer = null;
  }
}

function requestWatchdogReload() {
  try {
    fs.writeFileSync(WATCHDOG_RELOAD_FILE, JSON.stringify({
      requestedAt: Date.now(),
      instanceId: INSTANCE_ID,
    }) + "\n", { mode: 0o600 });
    return true;
  } catch (error) {
    console.error(`[proxy] Failed to request watchdog reload: ${error.message}`);
    return false;
  }
}

function exitForWatchdogRestart(message) {
  const reloadRequested = requestWatchdogReload();
  console.log(`[proxy] ${message}; exiting for ${reloadRequested ? "watchdog reload" : "watchdog restart"}...`);
  process.exit(0);
}

function rejectQueuedRequestsForRestart() {
  const queued = requestQueue;
  requestQueue = [];
  for (const request of queued) {
    if (!request.clientRes.destroyed && !request.clientRes.headersSent) {
      request.clientRes.writeHead(503, { "content-type": "application/json", "retry-after": "5" });
      request.clientRes.end(JSON.stringify({ error: "proxy is restarting" }));
    }
  }
  return queued.length;
}

function scheduleRestartDrainCheck(restartId, delay) {
  clearRestartDrainTimer();
  restartDrainTimer = setTimeout(() => {
    restartDrainTimer = null;
    if (restartState.phase !== "draining" || restartState.id !== restartId) return;

    const total = getActiveRequestCount();
    if (total === 0) {
      restartState = { ...restartState, phase: "stopping" };
      addEventLog("restart_stopping", 0, "排空完成，正在退出并请求 watchdog 重载", "");
      broadcastStatus();
      exitForWatchdogRestart(`drain complete (${Date.now()-restartState.startedAt}ms)`);
      return;
    }

    if (!restartState.warned && Date.now() - restartState.startedAt >= RESTART_FORCE_MIN_WAIT_MS) {
      restartState = { ...restartState, warned: true };
      console.log(`[proxy] WARNING: ${total} active requests still draining after ${RESTART_FORCE_MIN_WAIT_MS/1000}s. Waiting for completion or an explicit force restart.`);
    }
    scheduleRestartDrainCheck(restartId, 1000);
  }, delay);
}

function beginRestartDrain(now = Date.now()) {
  if (globalThis._restarting || restartState.phase !== "ready") {
    return { ok: false, error: "restart already in progress", ...buildRestartStatus(now) };
  }
  globalThis._restarting = true;
  restartState = {
    phase: "draining",
    startedAt: now,
    id: "restart_" + crypto.randomUUID(),
    cancelledQueuedRequests: 0,
    warned: false,
  };
  const cancelledQueuedRequests = rejectQueuedRequestsForRestart();
  restartState = { ...restartState, cancelledQueuedRequests };
  addEventLog("restart_requested", 0, `安全重启排空已开始，在途请求: ${getActiveRequestCount()}，已取消排队请求: ${cancelledQueuedRequests}`, "");
  broadcastStatus();
  scheduleRestartDrainCheck(restartState.id, 250);
  return { ok: true, message: "draining active requests before restart", ...buildRestartStatus(now) };
}

function cancelRestartDrain(now = Date.now()) {
  if (restartState.phase !== "draining") {
    return { ok: false, error: "restart is no longer cancellable", ...buildRestartStatus(now) };
  }
  const cancelledQueuedRequests = restartState.cancelledQueuedRequests || 0;
  const activeRequestsAtCancel = getActiveRequestCount();
  clearRestartDrainTimer();
  restartState = { phase: "ready", startedAt: null, id: "", cancelledQueuedRequests: 0, warned: false };
  globalThis._restarting = false;
  addEventLog("restart_cancelled", 0, `安全重启已取消，继续等待 ${activeRequestsAtCancel} 个在途请求；已拒绝的 ${cancelledQueuedRequests} 个排队请求不会恢复`, "");
  processQueue();
  broadcastStatus();
  return { ok: true, message: "restart cancelled; new requests are accepted", ...buildRestartStatus(now), cancelledQueuedRequests, activeRequestsAtCancel };
}

function requestForcedRestart(now = Date.now()) {
  if (restartState.phase !== "draining") {
    return { ok: false, error: "restart is not draining", ...buildRestartStatus(now) };
  }
  const elapsed = Math.max(0, now - restartState.startedAt);
  if (elapsed < RESTART_FORCE_MIN_WAIT_MS) {
    return {
      ok: false,
      error: "force restart is not available yet",
      retryAfterMs: RESTART_FORCE_MIN_WAIT_MS - elapsed,
      ...buildRestartStatus(now),
    };
  }
  const interruptedActiveRequests = getActiveRequestCount();
  clearRestartDrainTimer();
  restartState = { ...restartState, phase: "stopping" };
  addEventLog("restart_forced", 0, `强制重启已确认，将中断 ${interruptedActiveRequests} 个在途请求`, "");
  broadcastStatus();
  return { ok: true, message: "force restart accepted", interruptedActiveRequests, ...buildRestartStatus(now) };
}

function exitForForcedRestartAfterResponse(res) {
  let scheduled = false;
  const scheduleExit = () => {
    if (scheduled) return;
    scheduled = true;
    setTimeout(() => {
      if (restartState.phase !== "stopping") return;
      exitForWatchdogRestart("force restart requested, exiting immediately");
    }, 25);
  };
  if (res && typeof res.once === "function") res.once("finish", scheduleExit);
  setTimeout(scheduleExit, 250);
}

function parseReleaseVersion(value) {
  const match = /^v?(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$/i.exec(String(value || "").trim());
  return match ? [Number(match[1]), Number(match[2]), Number(match[3])] : null;
}

function compareReleaseVersions(left, right) {
  const a = parseReleaseVersion(left);
  const b = parseReleaseVersion(right);
  if (!a || !b) return null;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return a[i] > b[i] ? 1 : -1;
  }
  return 0;
}

function normalizeUpdateBaselineTag(value) {
  const match = /^v?(\d+)\.(\d+)\.(\d+)$/i.exec(String(value || "").trim());
  return match ? `v${match[1]}.${match[2]}.${match[3]}` : "";
}

function sha256Hex(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function readSmallLocalFile(filePath, maxBytes) {
  try {
    const stat = fs.lstatSync(filePath);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size < 0 || stat.size > maxBytes) return null;
    return fs.readFileSync(filePath);
  } catch {
    return null;
  }
}

function parseLocalJson(buffer) {
  try {
    return JSON.parse(buffer.toString("utf8"));
  } catch {
    return null;
  }
}

function normalizeCommit(value) {
  const commit = String(value || "").trim();
  return /^[0-9a-f]{7,64}$/i.test(commit) ? commit.toLowerCase() : null;
}

function inspectReleaseArtifactBuild() {
  const infoBuffer = readSmallLocalFile(BUILD_INFO_FILE, BUILD_INFO_MAX_BYTES);
  if (!infoBuffer) return { comparable: false, reason: "release-metadata-missing" };
  const info = parseLocalJson(infoBuffer);
  const baselineTag = info && info.schema === 1 && info.channel === "official-release"
    ? normalizeUpdateBaselineTag(info.releaseTag)
    : "";
  if (!baselineTag || info.manifestFile !== path.basename(RELEASE_MANIFEST_FILE) || !/^[0-9a-f]{64}$/i.test(String(info.manifestSha256 || ""))) {
    return { comparable: false, reason: "release-metadata-invalid" };
  }

  const manifestBuffer = readSmallLocalFile(RELEASE_MANIFEST_FILE, RELEASE_MANIFEST_MAX_BYTES);
  if (!manifestBuffer || sha256Hex(manifestBuffer) !== String(info.manifestSha256).toLowerCase()) {
    return { comparable: false, reason: "release-manifest-mismatch" };
  }
  const manifest = parseLocalJson(manifestBuffer);
  if (!manifest || manifest.schema !== 1 || normalizeUpdateBaselineTag(manifest.releaseTag) !== baselineTag || !manifest.files || typeof manifest.files !== "object" || Array.isArray(manifest.files)) {
    return { comparable: false, reason: "release-manifest-invalid" };
  }

  const fileNames = Object.keys(manifest.files);
  if (!fileNames.length || fileNames.length > RELEASE_INTEGRITY_FILES.size || [...REQUIRED_RELEASE_INTEGRITY_FILES].some(name => !fileNames.includes(name))) {
    return { comparable: false, reason: "release-manifest-incomplete" };
  }
  for (const fileName of fileNames) {
    const entry = manifest.files[fileName];
    if (!RELEASE_INTEGRITY_FILES.has(fileName) || !entry || typeof entry !== "object" || !/^[0-9a-f]{64}$/i.test(String(entry.sha256 || "")) || !Number.isInteger(entry.size) || entry.size < 0 || entry.size > RELEASE_INTEGRITY_FILE_MAX_BYTES) {
      return { comparable: false, reason: "release-manifest-invalid" };
    }
    const fileBuffer = readSmallLocalFile(path.join(__dirname, fileName), RELEASE_INTEGRITY_FILE_MAX_BYTES);
    if (!fileBuffer || fileBuffer.length !== entry.size || sha256Hex(fileBuffer) !== String(entry.sha256).toLowerCase()) {
      return { comparable: false, reason: "release-files-modified" };
    }
  }
  return {
    comparable: true,
    source: "official-release",
    baselineTag,
    commit: normalizeCommit(info.commit),
    verification: "release-manifest",
    provenanceLabel: "官方发布包（清单校验通过）",
  };
}

function runGitProbe(args) {
  try {
    const result = spawnSync("git", ["-C", __dirname, ...args], {
      encoding: "utf8",
      timeout: GIT_PROBE_TIMEOUT_MS,
      maxBuffer: 16 * 1024,
      windowsHide: true,
    });
    if (result.error || result.status !== 0) return null;
    return String(result.stdout || "").trim();
  } catch {
    return null;
  }
}

function isOfficialRepositoryRemote(remote) {
  const normalized = String(remote || "").trim().replace(/\/+$/, "").replace(/\.git$/i, "");
  return /(?:^|[/:@])github\.com[:/]aipayim\/codex-proxy$/i.test(normalized);
}

function inspectGitBuild() {
  if (runGitProbe(["rev-parse", "--is-inside-work-tree"]) !== "true") {
    return { comparable: false, reason: "git-unavailable" };
  }
  const remote = runGitProbe(["config", "--get", "remote.origin.url"]);
  if (!isOfficialRepositoryRemote(remote)) return { comparable: false, reason: "git-remote-untrusted" };
  const status = runGitProbe(["status", "--porcelain", "--untracked-files=no"]);
  if (status === null) return { comparable: false, reason: "git-status-unavailable" };
  if (status) return { comparable: false, reason: "git-worktree-modified" };
  const baselineTag = normalizeUpdateBaselineTag(runGitProbe(["describe", "--exact-match", "--tags", "HEAD"]));
  if (!baselineTag) return { comparable: false, reason: "git-not-at-release-tag" };
  return {
    comparable: true,
    source: "git-clean-tag",
    baselineTag,
    commit: normalizeCommit(runGitProbe(["rev-parse", "HEAD"])),
    verification: "git-clean-exact-tag",
    provenanceLabel: "官方 Git 干净正式标签",
  };
}

function inspectSourceBaselineBuild() {
  const buffer = readSmallLocalFile(RELEASE_BASELINE_FILE, RELEASE_BASELINE_MAX_BYTES);
  if (!buffer) return { comparable: false, reason: "source-baseline-missing" };
  const lines = buffer.toString("utf8").split(/\r?\n/);
  let baselineTag = "";
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    baselineTag = normalizeUpdateBaselineTag(line);
    break;
  }
  if (!baselineTag) return { comparable: false, reason: "source-baseline-invalid" };
  return {
    comparable: true,
    source: "source-baseline",
    baselineTag,
    commit: null,
    verification: "source-baseline-file",
    provenanceLabel: "源码基线文件（由发布流程维护）",
  };
}

function inspectLocalBuildProvenance() {
  const releaseBuild = inspectReleaseArtifactBuild();
  if (releaseBuild.comparable) return releaseBuild;
  const gitBuild = inspectGitBuild();
  if (gitBuild.comparable) return gitBuild;
  const baselineBuild = inspectSourceBaselineBuild();
  if (baselineBuild.comparable) return baselineBuild;
  return {
    comparable: false,
    source: "unknown",
    verification: "none",
    provenanceLabel: "本地来源未验证",
    reason: releaseBuild.reason || gitBuild.reason || baselineBuild.reason || "unknown",
  };
}

function refreshLocalBuildProvenance() {
  localBuildProvenance = inspectLocalBuildProvenance();
  return localBuildProvenance;
}

function getCachedLocalBuildProvenance() {
  return localBuildProvenance || refreshLocalBuildProvenance();
}

function getLocalBuildInfo() {
  const manualBaselineTag = normalizeUpdateBaselineTag(config.updateBaselineTag);
  if (manualBaselineTag) {
    return {
      version: manualBaselineTag,
      baselineTag: manualBaselineTag,
      label: `${manualBaselineTag}（定制构建的手动基线）`,
      customized: true,
      comparable: true,
      source: "manual-config",
      verification: "manual",
      provenanceLabel: "定制构建的手动基线",
    };
  }
  const provenance = getCachedLocalBuildProvenance();
  if (provenance.comparable) {
    return {
      version: provenance.baselineTag,
      baselineTag: provenance.baselineTag,
      label: `${provenance.baselineTag}（${provenance.provenanceLabel}）`,
      customized: provenance.source !== "official-release",
      comparable: true,
      source: provenance.source,
      verification: provenance.verification,
      provenanceLabel: provenance.provenanceLabel,
      commit: provenance.commit || null,
    };
  }
  return {
    version: null,
    baselineTag: null,
    label: "本地开发/定制版本（未能验证发布基线）",
    customized: true,
    comparable: false,
    source: "unknown",
    verification: "none",
    provenanceLabel: "本地来源未验证",
    reason: provenance.reason || "unknown",
  };
}

function buildUpdateStatus() {
  const latest = updateCheckState.latest;
  const current = getLocalBuildInfo();
  const comparison = latest && current.comparable ? compareReleaseVersions(latest.tag, current.version) : null;
  return {
    ok: !!latest,
    repository: { slug: UPSTREAM_REPOSITORY, url: UPSTREAM_REPOSITORY_URL },
    current,
    latest: latest ? { ...latest } : null,
    updateAvailable: current.comparable && comparison !== null && comparison > 0,
    comparison: comparison === null ? "unknown" : comparison,
    checkedAt: updateCheckState.checkedAt || null,
    cacheTtlMs: UPDATE_CHECK_TTL_MS,
    lastError: updateCheckState.lastError || null,
    updateMode: "manual-review-required",
  };
}

function fetchLatestRelease() {
  return new Promise((resolve, reject) => {
    const headers = {
      accept: "application/vnd.github+json",
      "user-agent": "codex-proxy-update-check",
    };
    if (updateCheckState.etag) headers["if-none-match"] = updateCheckState.etag;

    let settled = false;
    const finish = (fn, value) => {
      if (settled) return;
      settled = true;
      fn(value);
    };
    const req = https.get(UPSTREAM_LATEST_RELEASE_API, { headers }, (upstreamRes) => {
      const chunks = [];
      let size = 0;
      upstreamRes.on("data", (chunk) => {
        if (settled) return;
        size += chunk.length;
        if (size > UPDATE_MAX_RESPONSE_BYTES) {
          req.destroy(new Error("update response exceeded size limit"));
          return;
        }
        chunks.push(chunk);
      });
      upstreamRes.on("error", (err) => finish(reject, err));
      upstreamRes.on("end", () => {
        if (settled) return;
        if (upstreamRes.statusCode === 304 && updateCheckState.latest) {
          finish(resolve, null);
          return;
        }
        if (upstreamRes.statusCode !== 200) {
          finish(reject, new Error(`GitHub release check returned HTTP ${upstreamRes.statusCode || 0}`));
          return;
        }
        try {
          const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          const tag = typeof parsed.tag_name === "string" ? parsed.tag_name.trim().slice(0, 120) : "";
          if (!tag) throw new Error("GitHub release is missing tag_name");
          const expectedPrefix = `${UPSTREAM_REPOSITORY_URL}/releases/`;
          const releaseUrl = typeof parsed.html_url === "string" && parsed.html_url.startsWith(expectedPrefix)
            ? parsed.html_url
            : `${UPSTREAM_REPOSITORY_URL}/releases/tag/${encodeURIComponent(tag)}`;
          updateCheckState.etag = typeof upstreamRes.headers.etag === "string" ? upstreamRes.headers.etag : updateCheckState.etag;
          finish(resolve, {
            tag,
            name: typeof parsed.name === "string" ? parsed.name.slice(0, 240) : tag,
            publishedAt: typeof parsed.published_at === "string" ? parsed.published_at : null,
            url: releaseUrl,
            notes: typeof parsed.body === "string" ? parsed.body.replace(/\0/g, "").slice(0, UPDATE_MAX_RELEASE_NOTES_CHARS) : "",
          });
        } catch (err) {
          finish(reject, err);
        }
      });
    });
    req.setTimeout(UPDATE_REQUEST_TIMEOUT_MS, () => req.destroy(new Error("GitHub release check timed out")));
    req.on("error", (err) => finish(reject, err));
  });
}

function getUpdateStatus(forceRefresh = false) {
  const now = Date.now();
  if (updateCheckState.inFlight) return updateCheckState.inFlight;
  const cacheFresh = updateCheckState.checkedAt && now - updateCheckState.checkedAt < UPDATE_CHECK_TTL_MS;
  const refreshTooSoon = forceRefresh && updateCheckState.lastNetworkCheckAt && now - updateCheckState.lastNetworkCheckAt < UPDATE_CHECK_MIN_REFRESH_MS;
  if ((cacheFresh && !forceRefresh) || refreshTooSoon) return Promise.resolve(buildUpdateStatus());

  updateCheckState.lastNetworkCheckAt = now;
  updateCheckState.inFlight = fetchLatestRelease()
    .then((latest) => {
      if (latest) updateCheckState.latest = latest;
      updateCheckState.lastError = "";
      updateCheckState.checkedAt = Date.now();
      return buildUpdateStatus();
    })
    .catch((err) => {
      updateCheckState.lastError = String(err && err.message ? err.message : err).slice(0, 240);
      updateCheckState.checkedAt = Date.now();
      return buildUpdateStatus();
    })
    .finally(() => { updateCheckState.inFlight = null; });
  return updateCheckState.inFlight;
}

process.on("uncaughtException", err => {
  console.error("[proxy] UNCAUGHT EXCEPTION:", err.stack || err.message);
  process.exit(1);
});
process.on("unhandledRejection", (reason) => {
  console.error("[proxy] UNHANDLED REJECTION:", reason instanceof Error ? reason.stack : reason);
  process.exit(1);
});
process.on("exit", () => {
  try {
    const pid = parseInt(fs.readFileSync(PID_FILE, "utf8").trim(), 10);
    if (pid === process.pid) fs.unlinkSync(PID_FILE);
  } catch {}
});

// CLI --groups override: --groups "A=3456,B=3457"
let cliGroups = null;
const groupsArgIdx = process.argv.indexOf("--groups");
if (groupsArgIdx >= 0 && groupsArgIdx + 1 < process.argv.length) {
  const groupsStr = process.argv[groupsArgIdx + 1];
  const parsed = {};
  for (const part of groupsStr.split(",")) {
    const [name, portStr] = part.split("=");
    if (!name || !portStr) continue;
    const p = parseInt(portStr.trim());
    if (p >= 1024 && p <= 65535) parsed[name.trim().toUpperCase()] = p;
    else console.warn(`[proxy] Invalid port in --groups: "${portStr.trim()}" for group "${name.trim()}"`);
  }
  if (Object.keys(parsed).length) { cliGroups = parsed; if (!cliGroups["A"]) cliGroups["A"] = 3456; }
}

// Periodic cleanup to prevent memory leaks
setInterval(() => {
  const cutoff = Date.now() - 3600000;
  for (const idx in slidingWindows) {
    slidingWindows[idx] = slidingWindows[idx].filter(e => e.time > cutoff);
  }
  for (const p in pathStats) {
    if (pathStats[p].requests === 0) delete pathStats[p];
  }
  cleanOldLogs();
  if (pruneLogRollups()) scheduleLogSummaryPersist();
  if (compactState()) scheduleStateSave();
  scheduleLogIncidentEvaluation();
}, 600000); // every 10 minutes

// Periodic queue drain. Capacity/cooldown waits are served by re-queueing, but
// nothing short-circuits an already-waiting request until a backoff timer fires.
// A steady drain guarantees max-wait requests are 503'd instead of hanging.
(() => {
  const queueDrainTimer = setInterval(() => { try { processQueue(); } catch (e) {} }, 5000);
  if (queueDrainTimer && typeof queueDrainTimer.unref === "function") queueDrainTimer.unref();
})();

function loadConfig() {
  try {
    const raw = fs.readFileSync(CONFIG_FILE, "utf-8");
    const c = JSON.parse(raw);
    normalizeRuntimeStorageConfig(c);
    normalizeResponsesStreamConfig(c);
    normalizeAutoResumeConfig(c);
    if (c.webhookUrl) config.webhookUrl = c.webhookUrl;
    const defaultPricing = normalizeDefaultPricing(c.prices, c.bytesPerToken);
    config.prices = defaultPricing.prices;
    config.bytesPerToken = defaultPricing.bytesPerToken;
    config.modelPricing = normalizeModelPricing(c.modelPricing);
    if (c.notifications) { config.notifications.sound = c.notifications.sound !== false; config.notifications.desktop = c.notifications.desktop !== false; }
    config.autoRecover = c.autoRecover !== false;
    config.autoRecoverInterval = Math.max(0.5, c.autoRecoverInterval || 1);
    config.autoRecoverCodes = Array.isArray(c.autoRecoverCodes) ? c.autoRecoverCodes : [401,402,403,429,500,502,503,504];
    config.autoRecoverDiscarded = c.autoRecoverDiscarded === true;
    config.autoRecoverDaily = c.autoRecoverDaily === true;
    config.autoRecoverDailyDays = Math.max(1, parseInt(c.autoRecoverDailyDays) || 1);
    config.autoRecoverDailyHour = Math.min(23, Math.max(0, (n=>isNaN(n)?8:n)(parseInt(c.autoRecoverDailyHour))));
    config.autoRecoverDailyMinute = Math.min(59, Math.max(0, (n=>isNaN(n)?0:n)(parseInt(c.autoRecoverDailyMinute))));
    config.autoRecoverPoll = c.autoRecoverPoll === true;
    config.autoRecoverPollInterval = Math.max(1, parseInt(c.autoRecoverPollInterval) || 5);
    config.autoRecoverPollCodes = Array.isArray(c.autoRecoverPollCodes) ? c.autoRecoverPollCodes : [500,502,503,504];
    config.autoRecoverDelays = (Array.isArray(c.autoRecoverDelays) ? c.autoRecoverDelays : [800])
      .map(v => parseInt(v)).filter(v => !isNaN(v) && v >= 100 && v <= 10000).slice(0, 10);
    if (!config.autoRecoverDelays.length) config.autoRecoverDelays = [800];
    config.autoResume = c.autoResume === true;
    config.autoResumeIdleMinutes = c.autoResumeIdleMinutes;
    config.autoResumeDebounceMinutes = c.autoResumeDebounceMinutes;
    config.autoResumeRunnerStallMinutes = c.autoResumeRunnerStallMinutes;
    config.autoResumeRunnerMaxStallRestarts = c.autoResumeRunnerMaxStallRestarts;
    config.autoResumeProjects = normalizeAutoResumeProjects(c.autoResumeProjects);
    config.capacityBackoffSeconds = Math.max(1, parseInt(c.capacityBackoffSeconds) || 60);
    config.capacityMaxWaitSeconds = Math.max(30, parseInt(c.capacityMaxWaitSeconds) || 300);
    config.cmdPath = c.cmdPath || "/mnt/c/Windows/System32/cmd.exe";
    config.weeklySortBy = c.weeklySortBy === "expiry" ? "expiry" : "priority";
    config.roundRobin = c.roundRobin === true;
    config.enableAutoLock = c.enableAutoLock !== false;
    config.lockAfterFailCount = Math.max(1, c.lockAfterFailCount || 3);
    config.lockFailCodes = Array.isArray(c.lockFailCodes) ? c.lockFailCodes : ["401","403"];
    LOG_RETENTION_DAYS = c.logRetentionDays;
    LOG_FILE_ENABLED = c.logFile !== false;
    LOG_DETAIL = c.logDetail === "basic" ? "basic" : "full";
    config.logFile = LOG_FILE_ENABLED;
    config.logRetentionDays = LOG_RETENTION_DAYS;
    config.logDetail = LOG_DETAIL;
    config.logMaxMiB = c.logMaxMiB;
    config.logSegmentMaxMiB = c.logSegmentMaxMiB;
    config.stateHourlyRetentionDays = c.stateHourlyRetentionDays;
    config.stateDailyRetentionDays = c.stateDailyRetentionDays;
    config.stateMaxMiB = c.stateMaxMiB;
    config.proxyLogMaxMiB = c.proxyLogMaxMiB;
    config.proxyLogKeepFiles = c.proxyLogKeepFiles;
    config.codexLogMaintenance = normalizeCodexLogMaintenanceConfig(c.codexLogMaintenance);
    config.taskInsight = normalizeTaskInsightConfig(c.taskInsight);
    config.logIncidents = normalizeLogIncidentConfig(c.logIncidents);
    STREAM_LIFETIME = Math.min(7200000, Math.max(60000, parseInt(c.streamLifetime) || 1800000));
    config.streamLifetime = STREAM_LIFETIME;
    RESPONSES_STREAM_LIFETIME = c.responsesStreamLifetime;
    RESPONSES_IDLE_TIMEOUT = c.responsesIdleTimeout;
    config.responsesStreamLifetime = RESPONSES_STREAM_LIFETIME;
    config.responsesIdleTimeout = RESPONSES_IDLE_TIMEOUT;
    RESPONSES_NO_PROGRESS_TIMEOUT = c.responsesNoProgressTimeout;
    config.responsesNoProgressTimeout = RESPONSES_NO_PROGRESS_TIMEOUT;
    config.adminToken = c.adminToken || "";
    config.updateBaselineTag = normalizeUpdateBaselineTag(c.updateBaselineTag);
    config.rateLimit = c.rateLimit !== false;
    config.maxRequestsPerMin = Math.max(1, parseInt(c.maxRequestsPerMin) || 10);
    config.maxTokensPerMin = Math.max(0, parseInt(c.maxTokensPerMin) || 0);
    config.defaultResetHours = Math.max(1, parseInt(c.defaultResetHours) || 5);
    if (cliGroups) {
      config.groups = JSON.parse(JSON.stringify(cliGroups));
    } else {
      const rawGroups = (c.groups && typeof c.groups === 'object') ? JSON.parse(JSON.stringify(c.groups)) : {A: 3456};
      config.groups = {};
      for (const [k, v] of Object.entries(rawGroups)) config.groups[k.toUpperCase()] = v;
    }
    if (!config.groups["A"]) config.groups["A"] = 3456;
  } catch { /* defaults */ }
  config.logIncidents = normalizeLogIncidentConfig(config.logIncidents);
  config.codexLogMaintenance = normalizeCodexLogMaintenanceConfig(config.codexLogMaintenance);
  normalizeResponsesStreamConfig(config);
  normalizeAutoResumeConfig(config);
  RESPONSES_STREAM_LIFETIME = config.responsesStreamLifetime;
  RESPONSES_IDLE_TIMEOUT = config.responsesIdleTimeout;
  RESPONSES_NO_PROGRESS_TIMEOUT = config.responsesNoProgressTimeout;
  if (autoRecoverTimer) { clearInterval(autoRecoverTimer); autoRecoverTimer = null; }
  if (config.autoRecover) {
    const ms = config.autoRecoverInterval * 3600000;
    autoRecoverNextTime = Date.now() + Math.max(60000, ms);
    autoRecoverTimer = setInterval(() => {
      autoRecoverNextTime = Date.now() + Math.max(60000, config.autoRecoverInterval * 3600000);
      try { autoRecover(); } catch (e) { console.error("[proxy] interval auto-recover error:", e.message); }
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
  if (autoRecoverPollTimer) { clearTimeout(autoRecoverPollTimer); autoRecoverPollTimer = null; }
  autoRecoverPollNextTime = 0;
  if (config.autoRecoverPoll) {
    const pollCodes = config.autoRecoverPollCodes || [];
    for (let i = 0; i < accounts.length; i++) {
      const ks = getKeyState(i);
      if (ks.failCode && pollCodes.includes(ks.failCode)) {
        schedulePollRecover();
        break;
      }
    }
  }
  configureAutoResumeTimer();
  configureCodexLogMaintenanceTimer();
  configureTaskInsightTimers();
}

function clampConfigInteger(value, fallback, min, max) {
  const parsed = parseInt(value, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

function normalizeAutoResumeConfig(value) {
  const configValue = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  configValue.autoResume = configValue.autoResume === true;
  configValue.autoResumeIdleMinutes = clampConfigInteger(configValue.autoResumeIdleMinutes, 10, 1, 999);
  configValue.autoResumeDebounceMinutes = clampConfigInteger(configValue.autoResumeDebounceMinutes, 3, 1, 999);
  configValue.autoResumeRunnerStallMinutes = clampConfigInteger(
    configValue.autoResumeRunnerStallMinutes,
    AUTO_RESUME_RUNNER_STALL_MINUTES_DEFAULT,
    0,
    AUTO_RESUME_RUNNER_STALL_MINUTES_MAX,
  );
  configValue.autoResumeRunnerMaxStallRestarts = clampConfigInteger(
    configValue.autoResumeRunnerMaxStallRestarts,
    AUTO_RESUME_RUNNER_MAX_STALL_RESTARTS_DEFAULT,
    0,
    AUTO_RESUME_RUNNER_MAX_STALL_RESTARTS_MAX,
  );
  return configValue;
}

function normalizeTaskInsightConfig(value) {
  const v = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const signals = v.signals && typeof v.signals === "object" && !Array.isArray(v.signals) ? v.signals : {};
  const distill = v.distill && typeof v.distill === "object" && !Array.isArray(v.distill) ? v.distill : {};
  const retentionDays = clampConfigInteger(v.retentionDays, TASK_INSIGHT_DEFAULT.retentionDays, TASK_INSIGHT_RETENTION_MIN_DAYS, TASK_INSIGHT_RETENTION_MAX_DAYS);
  const engine = TASK_DISTILL_ENGINES.includes(distill.engine) ? distill.engine : "ollama";
  const budgetRaw = Number(distill.dailyBudgetYuan);
  const dailyBudgetYuan = Number.isFinite(budgetRaw) && budgetRaw >= 0 ? budgetRaw : 1;
  return {
    enabled: v.enabled === true,
    signals: {
      instructions: signals.instructions === true,
      tools: signals.tools === true,
      usage: signals.usage === true,
      correlate: signals.correlate === true,
    },
    retentionDays,
    distill: {
      enabled: distill.enabled === true,
      engine,
      model: String(distill.model || "").trim().slice(0, 120),
      baseUrl: String(distill.baseUrl || "").trim().slice(0, 512),
      dailyBudgetYuan,
      report: distill.report === "weekly" ? "weekly" : "daily",
    },
  };
}

function normalizeDefaultPricing(pricesValue, bytesPerTokenValue, strict = false, changed = {}) {
  const prices = pricesValue && typeof pricesValue === "object" && !Array.isArray(pricesValue) ? pricesValue : null;
  const inputPer1M = Number(prices && prices.inputPer1M);
  const outputPer1M = Number(prices && prices.outputPer1M);
  const bytesPerToken = Number(bytesPerTokenValue);
  const pricesValid = prices && Number.isFinite(inputPer1M) && inputPer1M >= 0 && inputPer1M <= MODEL_PRICING_PRICE_MAX &&
    Number.isFinite(outputPer1M) && outputPer1M >= 0 && outputPer1M <= MODEL_PRICING_PRICE_MAX;
  const bytesPerTokenValid = Number.isFinite(bytesPerToken) && bytesPerToken >= MODEL_PRICING_BPT_MIN && bytesPerToken <= MODEL_PRICING_BPT_MAX;
  if (strict && changed.prices && (!prices || typeof prices.inputPer1M !== "number" || typeof prices.outputPer1M !== "number" || !pricesValid)) {
    throw new Error("prices requires finite input/output values between 0 and 1000000");
  }
  if (strict && changed.bytesPerToken && (typeof bytesPerTokenValue !== "number" || !bytesPerTokenValid)) {
    throw new Error("bytesPerToken must be a finite value between 0.1 and 100");
  }
  return {
    prices: {
      inputPer1M: pricesValid ? inputPer1M : 0,
      outputPer1M: pricesValid ? outputPer1M : 0,
    },
    bytesPerToken: bytesPerTokenValid ? bytesPerToken : 3,
  };
}

function normalizeModelPricing(value, strict = false) {
  // The early draft used { rules: [...] }; accept it on read, but persist the
  // compact array form so the dashboard and release config have one schema.
  let rules = value;
  if (rules && typeof rules === "object" && !Array.isArray(rules) && Array.isArray(rules.rules)) rules = rules.rules;
  if (rules == null) return [];
  if (!Array.isArray(rules)) {
    if (strict) throw new Error("modelPricing must be an array of model pricing rules");
    return [];
  }
  if (rules.length > MODEL_PRICING_MAX_RULES) {
    if (strict) throw new Error(`modelPricing supports at most ${MODEL_PRICING_MAX_RULES} rules`);
    rules = rules.slice(0, MODEL_PRICING_MAX_RULES);
  }
  const normalized = [];
  const seen = new Set();
  for (const rule of rules) {
    if (!rule || typeof rule !== "object" || Array.isArray(rule)) {
      if (strict) throw new Error("each modelPricing rule must be an object");
      continue;
    }
    const model = String(rule.model == null ? "" : rule.model).trim();
    const inputPer1M = Number(rule.inputPer1M);
    const outputPer1M = Number(rule.outputPer1M);
    const bytesPerToken = Number(rule.bytesPerToken);
    const hasRequiredFields = ["model", "inputPer1M", "outputPer1M", "bytesPerToken"]
      .every(field => Object.prototype.hasOwnProperty.call(rule, field));
    const strictTypesValid = !strict || (typeof rule.model === "string" &&
      typeof rule.inputPer1M === "number" && typeof rule.outputPer1M === "number" && typeof rule.bytesPerToken === "number");
    const valid = hasRequiredFields && strictTypesValid && model.length > 0 && model.length <= MODEL_PRICING_MODEL_MAX_CHARS &&
      !/[\u0000-\u001f\u007f]/.test(model) &&
      Number.isFinite(inputPer1M) && inputPer1M >= 0 && inputPer1M <= MODEL_PRICING_PRICE_MAX &&
      Number.isFinite(outputPer1M) && outputPer1M >= 0 && outputPer1M <= MODEL_PRICING_PRICE_MAX &&
      Number.isFinite(bytesPerToken) && bytesPerToken >= MODEL_PRICING_BPT_MIN && bytesPerToken <= MODEL_PRICING_BPT_MAX &&
      !seen.has(model);
    if (!valid) {
      if (strict) throw new Error("modelPricing rules require a unique model name, non-negative input/output price, and bytesPerToken between 0.1 and 100");
      continue;
    }
    seen.add(model);
    normalized.push({ model, inputPer1M, outputPer1M, bytesPerToken });
  }
  return normalized;
}

function normalizeRuntimeStorageConfig(value) {
  const configValue = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  configValue.logRetentionDays = clampConfigInteger(configValue.logRetentionDays, 7, 0, 3650);
  configValue.logMaxMiB = clampConfigInteger(configValue.logMaxMiB, DEFAULT_LOG_MAX_MIB, LOG_MAX_MIB_MIN, LOG_MAX_MIB_LIMIT);
  configValue.logSegmentMaxMiB = clampConfigInteger(configValue.logSegmentMaxMiB, DEFAULT_LOG_SEGMENT_MAX_MIB, LOG_SEGMENT_MAX_MIB_MIN, LOG_SEGMENT_MAX_MIB_LIMIT);
  if (configValue.logSegmentMaxMiB > configValue.logMaxMiB) configValue.logSegmentMaxMiB = configValue.logMaxMiB;
  configValue.stateHourlyRetentionDays = clampConfigInteger(configValue.stateHourlyRetentionDays, DEFAULT_STATE_HOURLY_RETENTION_DAYS, STATE_HOURLY_RETENTION_MIN_DAYS, STATE_HOURLY_RETENTION_MAX_DAYS);
  configValue.stateDailyRetentionDays = clampConfigInteger(configValue.stateDailyRetentionDays, DEFAULT_STATE_DAILY_RETENTION_DAYS, STATE_DAILY_RETENTION_MIN_DAYS, STATE_DAILY_RETENTION_MAX_DAYS);
  configValue.stateMaxMiB = clampConfigInteger(configValue.stateMaxMiB, DEFAULT_STATE_MAX_MIB, STATE_MAX_MIB_MIN, STATE_MAX_MIB_LIMIT);
  configValue.proxyLogMaxMiB = clampConfigInteger(configValue.proxyLogMaxMiB, DEFAULT_PROXY_LOG_MAX_MIB, 1, PROXY_LOG_MAX_MIB_LIMIT);
  configValue.proxyLogKeepFiles = clampConfigInteger(configValue.proxyLogKeepFiles, DEFAULT_PROXY_LOG_KEEP_FILES, 1, PROXY_LOG_KEEP_FILES_LIMIT);
  return configValue;
}

function normalizeResponsesTimeout(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  const milliseconds = Math.floor(parsed);
  if (milliseconds === 0) return 0;
  return Math.min(RESPONSES_TIMEOUT_MAX_MS, Math.max(60000, milliseconds));
}

function normalizeResponsesStreamConfig(value) {
  const configValue = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  configValue.responsesStreamLifetime = normalizeResponsesTimeout(configValue.responsesStreamLifetime, 0);
  configValue.responsesIdleTimeout = normalizeResponsesTimeout(configValue.responsesIdleTimeout, RESPONSES_IDLE_TIMEOUT_DEFAULT_MS);
  configValue.responsesNoProgressTimeout = normalizeResponsesTimeout(configValue.responsesNoProgressTimeout, RESPONSES_NO_PROGRESS_DEFAULT_MS);
  if (configValue.responsesNoProgressTimeout === 0) configValue.responsesNoProgressTimeout = RESPONSES_NO_PROGRESS_DEFAULT_MS;
  return configValue;
}

let configSaveQueue = Promise.resolve();

function enqueueConfigSave(work) {
  const run = configSaveQueue.then(work, work);
  // A rejected save must report to its caller without blocking later saves.
  configSaveQueue = run.catch(() => {});
  return run;
}

function writeConfigAtomically(value) {
  const temporaryFile = path.join(__dirname, `.${path.basename(CONFIG_FILE)}.${process.pid}.${crypto.randomUUID()}.tmp`);
  const serialized = `${JSON.stringify(value, null, 2)}\n`;
  let descriptor = null;
  try {
    descriptor = fs.openSync(temporaryFile, "wx", 0o600);
    fs.writeFileSync(descriptor, serialized, "utf8");
    // Some mounted filesystems do not support fsync; rename still prevents a
    // partially written JSON file from replacing the active configuration.
    try { fs.fsyncSync(descriptor); } catch {}
    fs.closeSync(descriptor);
    descriptor = null;
    fs.renameSync(temporaryFile, CONFIG_FILE);
  } finally {
    if (descriptor !== null) {
      try { fs.closeSync(descriptor); } catch {}
    }
    try { fs.unlinkSync(temporaryFile); } catch {}
  }
}

function normalizeCodexLogMaintenancePath(value) {
  const raw = String(value == null ? "" : value).trim();
  if (!raw) return raw;
  const backslash = String.fromCharCode(92);
  const lower = raw.toLowerCase();
  const prefixes = [
    backslash + backslash + "wsl.localhost" + backslash,
    backslash + backslash + "wsl$" + backslash,
  ];
  const prefix = prefixes.find(candidate => lower.startsWith(candidate));
  if (!prefix) return raw;
  const withoutHost = raw.slice(prefix.length);
  const distributionEnd = withoutHost.indexOf(backslash);
  if (distributionEnd <= 0) return raw;
  const innerPath = withoutHost.slice(distributionEnd + 1).split(backslash).join("/").split("/").filter(Boolean).join("/");
  return innerPath ? "/" + innerPath : raw;
}

function normalizeCodexLogMaintenanceConfig(value) {
  const raw = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const rawDbPath = Object.prototype.hasOwnProperty.call(raw, "dbPath")
    ? String(raw.dbPath == null ? "" : raw.dbPath).trim()
    : DEFAULT_CODEX_LOG_MAINTENANCE.dbPath;
  const dbPath = normalizeCodexLogMaintenancePath(rawDbPath || (raw.enabled === true ? "" : DEFAULT_CODEX_LOG_MAINTENANCE.dbPath));
  return {
    enabled: raw.enabled === true,
    dbPath,
    thresholdMiB: clampConfigInteger(raw.thresholdMiB, DEFAULT_CODEX_LOG_MAINTENANCE.thresholdMiB, CODEX_LOG_MAINTENANCE_THRESHOLD_MIN_MIB, CODEX_LOG_MAINTENANCE_THRESHOLD_MAX_MIB),
    retainHours: clampConfigInteger(raw.retainHours, DEFAULT_CODEX_LOG_MAINTENANCE.retainHours, CODEX_LOG_MAINTENANCE_RETAIN_HOURS_MIN, CODEX_LOG_MAINTENANCE_RETAIN_HOURS_MAX),
    checkIntervalMinutes: clampConfigInteger(raw.checkIntervalMinutes, DEFAULT_CODEX_LOG_MAINTENANCE.checkIntervalMinutes, CODEX_LOG_MAINTENANCE_INTERVAL_MINUTES_MIN, CODEX_LOG_MAINTENANCE_INTERVAL_MINUTES_MAX),
  };
}

function formatCodexLogMaintenanceError(result) {
  const code = String(result && result.errorCode || "");
  const messages = {
    missing_path: "请填写 Codex SQLite 数据库路径",
    invalid_path: "数据库路径无效。请填写当前 WSL 用户 ~/.codex 下的 logs*.sqlite 主文件",
    not_found: "数据库文件不存在",
    unavailable: "无法访问数据库文件",
    invalid_sidecar: "数据库或其 SQLite 辅助文件不是常规文件",
    invalid_schema: "该 SQLite 文件不是受支持的 Codex 日志数据库（需要 logs.id 和 Unix 秒 logs.ts）",
    invalid_sqlite: "无法以只读方式安全打开该 SQLite 数据库",
    database_busy: "数据库正忙，请稍后重试",
    invalid_option: "数据库维护参数无效",
    helper_unavailable: "未找到 Python 3 或数据库维护 helper；请安装 Python 3 后重试",
    helper_timeout: "数据库检测/维护超时，未继续占用数据库",
    cleanup_failed: "数据库维护未能安全完成",
    database_active: "Codex 仍在使用中，暂缓清理（请等 60 秒无新请求后再试）",
  };
  const fallback = result && result.error ? String(result.error).slice(0, 240) : "数据库检测失败";
  return messages[code] || fallback;
}

function getCodexLogMaintenanceIdleState(now = Date.now()) {
  const active = getActiveRequestCount();
  const queued = Array.isArray(requestQueue) ? requestQueue.length : 0;
  const lastAgoMs = Math.max(0, now - lastRequestTime);
  return {
    active,
    queued,
    lastAgoMs,
    graceMs: CODEX_LOG_MAINTENANCE_IDLE_GRACE_MS,
    idle: active === 0 && queued === 0 && lastAgoMs >= CODEX_LOG_MAINTENANCE_IDLE_GRACE_MS,
  };
}

function getCodexLogMaintenanceRuntimeStatus() {
  return {
    ...codexLogMaintenanceRuntime,
    inFlight: !!codexLogMaintenanceInFlight,
    idleState: getCodexLogMaintenanceIdleState(),
  };
}

function invokeCodexLogMaintainer(command, maintenanceConfig, options = {}) {
  const cfg = normalizeCodexLogMaintenanceConfig(maintenanceConfig);
  const args = [CODEX_SQLITE_LOG_MAINTAINER_FILE, command, "--path", cfg.dbPath, "--busy-timeout-ms", String(CODEX_LOG_MAINTENANCE_BUSY_TIMEOUT_MS)];
  if (command === "cleanup") {
    args.push(
      "--threshold-mib", String(cfg.thresholdMiB),
      "--retain-hours", String(cfg.retainHours),
      "--batch-rows", String(CODEX_LOG_MAINTENANCE_BATCH_ROWS),
      "--max-batches", String(options.maxBatches && Number.isFinite(Number(options.maxBatches)) ? Number(options.maxBatches) : CODEX_LOG_MAINTENANCE_MAX_BATCHES),
    );
    if (options.vacuum) args.push("--vacuum");
  }
  const timeoutMs = options.timeoutMs && Number.isFinite(Number(options.timeoutMs)) && Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : CODEX_LOG_MAINTAINER_TIMEOUT_MS;
  return new Promise(resolve => {
    let child = null;
    let stdout = "";
    let stderr = "";
    let outputTooLarge = false;
    let timedOut = false;
    let settled = false;
    let timer = null;
    const finish = result => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      resolve(result);
    };
    try {
      child = spawn("python3", args, { stdio: ["ignore", "pipe", "pipe"], windowsHide: true });
    } catch (error) {
      finish({ ok: false, result: "failed", errorCode: "helper_unavailable", error: String(error && error.message || error).slice(0, 240) });
      return;
    }
    const append = (target, chunk) => {
      const value = String(chunk || "");
      if (target === "stdout") stdout += value;
      else stderr += value;
      if (Buffer.byteLength(stdout, "utf8") + Buffer.byteLength(stderr, "utf8") > CODEX_LOG_MAINTAINER_MAX_OUTPUT_BYTES) outputTooLarge = true;
    };
    child.stdout.on("data", chunk => append("stdout", chunk));
    child.stderr.on("data", chunk => append("stderr", chunk));
    child.once("error", error => {
      finish({ ok: false, result: "failed", errorCode: "helper_unavailable", error: String(error && error.message || error).slice(0, 240) });
    });
    child.once("close", code => {
      if (timedOut) {
        finish({ ok: false, result: "failed", errorCode: "helper_timeout", error: "maintenance helper timed out" });
        return;
      }
      if (outputTooLarge) {
        finish({ ok: false, result: "failed", errorCode: "helper_output", error: "maintenance helper returned too much output" });
        return;
      }
      try {
        const payload = JSON.parse(stdout);
        if (!payload || typeof payload !== "object" || Array.isArray(payload)) throw new Error("invalid helper JSON");
        if (code !== 0 && payload.ok !== false) throw new Error("maintenance helper failed");
        finish(payload);
      } catch (error) {
        const detail = stderr.trim() || String(error && error.message || error);
        finish({ ok: false, result: "failed", errorCode: "helper_failed", error: detail.slice(0, 240) });
      }
    });
    timer = setTimeout(() => {
      timedOut = true;
      try { child.kill("SIGTERM"); } catch {}
    }, timeoutMs);
    if (timer && typeof timer.unref === "function") timer.unref();
  });
}

function applyCodexLogMaintenanceResult(result) {
  const runtime = codexLogMaintenanceRuntime;
  runtime.lastCompletedAt = Date.now();
  runtime.lastResult = String(result && result.result || "failed");
  runtime.lastError = result && result.ok === false ? formatCodexLogMaintenanceError(result) : "";
  runtime.phase = result && result.ok === false ? "error" : "idle";
  runtime.vacuumed = !!(result && result.vacuumed === true);
  for (const name of ["databaseBytes", "walBytes", "totalBytes", "totalMiB", "deletedRows", "batches", "physicalBytesBefore", "physicalBytesAfter", "physicalBytesDelta", "vacuumBytesBefore", "vacuumBytesAfter"]) {
    if (result && Number.isFinite(Number(result[name]))) runtime[name] = Number(result[name]);
  }
}

async function runCodexLogMaintenance(reason = "scheduled", opts = {}) {
  const maintenanceConfig = normalizeCodexLogMaintenanceConfig(config.codexLogMaintenance);
  if (!maintenanceConfig.enabled) {
    codexLogMaintenanceRuntime.phase = "disabled";
    codexLogMaintenanceRuntime.nextCheckAt = 0;
    return { ok: false, result: "disabled", errorCode: "disabled", error: "Codex SQLite maintenance is disabled" };
  }
  if (codexLogMaintenanceInFlight) {
    return { ok: false, result: "in_progress", errorCode: "in_progress", error: "Codex SQLite maintenance is already running" };
  }
  if (opts.requireIdle) {
    const idle = getCodexLogMaintenanceIdleState();
    if (!idle.idle) {
      return { ok: false, result: "database_active", errorCode: "database_active", error: "Codex 仍在使用中，暂缓清理", idleState: idle };
    }
  }
  codexLogMaintenanceRuntime.phase = "checking";
  codexLogMaintenanceRuntime.lastCheckAt = Date.now();
  const work = invokeCodexLogMaintainer("cleanup", maintenanceConfig, { timeoutMs: opts.timeoutMs, vacuum: opts.vacuum, maxBatches: opts.maxBatches });
  codexLogMaintenanceInFlight = work;
  try {
    const result = await work;
    applyCodexLogMaintenanceResult(result);
    if (result && result.ok && Number(result.deletedRows) > 0) {
      addEventLog("codex_sqlite_maintenance", 0, `Codex SQLite 日志维护已删除 ${Number(result.deletedRows)} 条过期记录${result.vacuumed === true ? "并 VACUUM 释放空间" : ""}（${reason}）`, "");
    }
    return result;
  } catch (error) {
    const result = { ok: false, result: "failed", errorCode: "helper_failed", error: String(error && error.message || error).slice(0, 240) };
    applyCodexLogMaintenanceResult(result);
    return result;
  } finally {
    if (codexLogMaintenanceInFlight === work) codexLogMaintenanceInFlight = null;
    broadcastStatus();
  }
}

function scheduleCodexLogMaintenance(delayMs) {
  if (!config.codexLogMaintenance || !config.codexLogMaintenance.enabled) return;
  const delay = Math.max(1000, Number(delayMs) || CODEX_LOG_MAINTENANCE_INITIAL_DELAY_MS);
  codexLogMaintenanceRuntime.nextCheckAt = Date.now() + delay;
  codexLogMaintenanceTimer = setTimeout(async () => {
    codexLogMaintenanceTimer = null;
    await runCodexLogMaintenance("scheduled");
    if (config.codexLogMaintenance && config.codexLogMaintenance.enabled) {
      scheduleCodexLogMaintenance(config.codexLogMaintenance.checkIntervalMinutes * 60000);
    }
  }, delay);
  if (codexLogMaintenanceTimer && typeof codexLogMaintenanceTimer.unref === "function") codexLogMaintenanceTimer.unref();
}

function configureCodexLogMaintenanceTimer() {
  if (codexLogMaintenanceTimer) {
    clearTimeout(codexLogMaintenanceTimer);
    codexLogMaintenanceTimer = null;
  }
  const maintenanceConfig = normalizeCodexLogMaintenanceConfig(config.codexLogMaintenance);
  config.codexLogMaintenance = maintenanceConfig;
  if (!maintenanceConfig.enabled) {
    codexLogMaintenanceRuntime.phase = codexLogMaintenanceInFlight ? "checking" : "disabled";
    codexLogMaintenanceRuntime.nextCheckAt = 0;
    return;
  }
  if (!codexLogMaintenanceInFlight) codexLogMaintenanceRuntime.phase = "scheduled";
  scheduleCodexLogMaintenance(CODEX_LOG_MAINTENANCE_INITIAL_DELAY_MS);
}

function normalizePath(p) {
  if (!p) return p;
  let s = String(p);
  s = s.replace(/\\/g, '/');
  s = s.replace(/^([A-Za-z]):\//, (_, d) => '/mnt/' + d.toLowerCase() + '/');
  try { s = require('path').resolve(s); } catch(e) {}
  s = s.replace(/\/+$/, '');
  return s;
}
function normalizeAutoResumeProjects(value) {
  if (!Array.isArray(value)) return [];
  return value.slice(0, 10).map(project => {
    if (!project || typeof project !== "object" || Array.isArray(project)) return null;
    const name = String(project.name || "").trim().slice(0, 80);
    const projectPath = String(project.path || "").trim().slice(0, 2048);
    const cmd = String(project.cmd || "").trim().slice(0, 8192);
    const sessionId = normalizeAutoResumeSessionId(project.sessionId);
    const resumeMode = project.resumeMode === "fixed_session" ? "fixed_session" : "command";
    if (!projectPath && !cmd) return null;
    return {
      name,
      path: projectPath,
      cmd,
      resumeMode,
      ...(sessionId ? { sessionId } : {}),
    };
  }).filter(Boolean);
}
function checkAdminAuth(req) {
  if (!config.adminToken) return true;
  const auth = req.headers.authorization || "";
  if (!auth || auth.length !== config.adminToken.length + 7) return false;
  let diff = 0;
  for (let i = 0; i < config.adminToken.length; i++) {
    diff |= auth.charCodeAt(i + 7) ^ config.adminToken.charCodeAt(i);
  }
  return diff === 0 && auth.startsWith("Bearer ");
}
function normalizeAutoResumeTimestamp(value) {
  const timestamp = Number(value);
  if (!Number.isFinite(timestamp) || timestamp <= 0) return 0;
  return Math.floor(timestamp);
}

function readAutoResumeRuntimeFile() {
  try {
    const raw = fs.readFileSync(AUTO_RESUME_RUNTIME_FILE, "utf8");
    if (Buffer.byteLength(raw, "utf8") > AUTO_RESUME_RUNTIME_FILE_MAX_BYTES) return null;
    const saved = JSON.parse(raw);
    if (!saved || typeof saved !== "object" || Array.isArray(saved)) return null;
    const triggerIdleMinutes = Number(saved.lastTriggerIdleMinutes);
    return {
      lastKeyUseTime: normalizeAutoResumeTimestamp(saved.lastKeyUseTime),
      lastResumeTime: normalizeAutoResumeTimestamp(saved.lastResumeTime),
      lastTriggerIdleMinutes: Number.isFinite(triggerIdleMinutes) && triggerIdleMinutes >= 0
        ? Math.floor(triggerIdleMinutes)
        : null,
    };
  } catch {
    return null;
  }
}

function persistAutoResumeRuntime() {
  const runtime = autoResumeStateReady ? getAutoResumeRuntimeState() : null;
  const payload = {
    schema: 1,
    lastKeyUseTime,
    lastResumeTime,
    lastTriggerIdleMinutes: runtime && Number.isFinite(Number(runtime.lastTriggerIdleMinutes))
      ? Math.max(0, Math.floor(Number(runtime.lastTriggerIdleMinutes)))
      : null,
    updatedAt: Date.now(),
  };
  try {
    fs.writeFileSync(AUTO_RESUME_RUNTIME_FILE, JSON.stringify(payload) + "\n", { mode: 0o600 });
    autoResumeRuntimeWriteErrorLogged = false;
  } catch (error) {
    if (!autoResumeRuntimeWriteErrorLogged) {
      console.error(`[proxy] Failed to persist auto-resume runtime: ${error.message}`);
      autoResumeRuntimeWriteErrorLogged = true;
    }
  }
}

function getAutoResumeRuntimeState() {
  if (!state || typeof state !== "object" || Array.isArray(state)) {
    state = { keys: [], activeKey: null };
  }
  if (!state.autoResume || typeof state.autoResume !== "object" || Array.isArray(state.autoResume)) {
    state.autoResume = {};
  }
  const runtime = state.autoResume;
  runtime.lastKeyUseTime = normalizeAutoResumeTimestamp(runtime.lastKeyUseTime);
  runtime.lastResumeTime = normalizeAutoResumeTimestamp(runtime.lastResumeTime);
  if (!runtime.projects || typeof runtime.projects !== "object" || Array.isArray(runtime.projects)) {
    runtime.projects = {};
  }
  return runtime;
}

function restoreAutoResumeRuntimeState() {
  const runtime = getAutoResumeRuntimeState();
  const durable = readAutoResumeRuntimeFile();
  lastKeyUseTime = Math.max(runtime.lastKeyUseTime, durable ? durable.lastKeyUseTime : 0);
  lastResumeTime = Math.max(runtime.lastResumeTime, durable ? durable.lastResumeTime : 0);
  runtime.lastKeyUseTime = lastKeyUseTime;
  runtime.lastResumeTime = lastResumeTime;
  if (durable && durable.lastTriggerIdleMinutes !== null) {
    runtime.lastTriggerIdleMinutes = durable.lastTriggerIdleMinutes;
  }
  autoResumeStateReady = true;
  configureAutoResumeTimer();
}

function initializeAutoResumeKeyUseTime() {
  if (lastKeyUseTime > 0 || !autoResumeStateReady) return;
  lastKeyUseTime = Date.now();
  const runtime = getAutoResumeRuntimeState();
  runtime.lastKeyUseTime = lastKeyUseTime;
  runtime.initializedAt = lastKeyUseTime;
  persistAutoResumeRuntime();
  saveState(true);
}

function recordKeyUse(idx, at = Date.now()) {
  lastKeyUseTime = normalizeAutoResumeTimestamp(at) || Date.now();
  if (autoResumeStateReady) {
    const runtime = getAutoResumeRuntimeState();
    runtime.lastKeyUseTime = lastKeyUseTime;
    runtime.lastKeyIndex = idx + 1;
    for (let projectIndex = 0; projectIndex < (config.autoResumeProjects || []).length; projectIndex++) {
      const projectId = autoResumeProjectId(config.autoResumeProjects[projectIndex], projectIndex);
      const projectState = runtime.projects[projectId];
      if (!projectState || projectState.idleEpochKeyUseTime === lastKeyUseTime) continue;
      runtime.projects[projectId] = {
        ...projectState,
        idleEpochKeyUseTime: lastKeyUseTime,
        attemptCount: 0,
        lastAttemptAt: 0,
        lastAttemptOutcome: "",
        stallPhase: "",
        stallRunId: null,
        stallTermSentAt: 0,
        stallKillSentAt: 0,
        stallRestartCount: 0,
        stallExhaustedAt: 0,
        stallOwnershipIssue: "",
      };
    }
  }
  // Keep this small heartbeat durable even when the full state write is throttled.
  persistAutoResumeRuntime();
  saveState();
  broadcastStatus();
}

function autoResumeProjectId(proj, index) {
  const base = String((proj && proj.name) || "project").replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 48) || "project";
  return `${base}_${index + 1}`;
}

function autoResumeProjectLabel(proj, index) {
  const name = String((proj && proj.name) || "").trim();
  return name ? name.slice(0, 80) : `项目 ${index + 1}`;
}

function autoResumeProjectFiles(proj, index) {
  const id = autoResumeProjectId(proj, index);
  return {
    id,
    pidFile: `/tmp/codex-resume-${id}.pid`,
    statusFile: `/tmp/codex-resume-${id}.status`,
  };
}

function normalizeAutoResumeSessionId(value) {
  const sessionId = String(value || "").trim();
  if (!sessionId || sessionId.length > 128) return "";
  return /^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(sessionId) ? sessionId : "";
}

function usesFixedAutoResumeSession(proj) {
  if (!proj || proj.resumeMode !== "fixed_session") return false;
  const command = String(proj.cmd || "").trim();
  return Boolean(normalizeAutoResumeSessionId(proj.sessionId) && command.includes("{sessionId}"));
}

function validateAutoResumeProjects(value) {
  if (!Array.isArray(value)) return;
  for (const project of value.slice(0, 10)) {
    if (!project || typeof project !== "object" || Array.isArray(project) || project.resumeMode !== "fixed_session") continue;
    if (!usesFixedAutoResumeSession(project)) {
      throw new Error("fixed-session recovery requires a valid sessionId and {sessionId} command placeholder");
    }
  }
}

function autoResumeCommandHash(command) {
  return crypto.createHash("sha256").update(String(command || ""), "utf8").digest("hex").slice(0, 16);
}

function autoResumeProjectFingerprint(proj) {
  const descriptor = {
    path: normalizePath(proj && proj.path) || "",
    command: String((proj && proj.cmd) || "").trim(),
    resumeMode: proj && proj.resumeMode === "fixed_session" ? "fixed_session" : "command",
    sessionId: normalizeAutoResumeSessionId(proj && proj.sessionId),
  };
  return autoResumeCommandHash(JSON.stringify(descriptor));
}

function shellQuote(value) {
  return `'${String(value || "").replace(/'/g, "'\\''")}'`;
}

function buildAutoResumeCommand(proj) {
  const command = String((proj && proj.cmd) || "").trim();
  // Only the explicit fixed-session mode may expand the placeholder. A
  // command-mode project can safely retain a literal {sessionId} token.
  if (usesFixedAutoResumeSession(proj)) {
    return command.replace(/\{sessionId\}/g, shellQuote(normalizeAutoResumeSessionId(proj.sessionId)));
  }
  return command;
}

function getAutoResumeCommandWarning(proj, command) {
  if (/\bresume\s+--last\b/.test(command) && !usesFixedAutoResumeSession(proj)) {
    return "resume --last selects the most recent session; configure fixed_session with sessionId/{sessionId} for deterministic recovery";
  }
  return "";
}

function autoResumeProjectState(proj, index) {
  const id = autoResumeProjectId(proj, index);
  const runtime = getAutoResumeRuntimeState();
  const previous = runtime.projects[id] || {};
  const epoch = normalizeAutoResumeTimestamp(lastKeyUseTime);
  const fingerprint = autoResumeProjectFingerprint(proj);
  if (previous.idleEpochKeyUseTime === epoch &&
      (!previous.projectFingerprint || previous.projectFingerprint === fingerprint)) {
    return { id, state: previous };
  }
  const next = {
    ...previous,
    idleEpochKeyUseTime: epoch,
    projectFingerprint: fingerprint,
    attemptCount: 0,
    lastAttemptAt: 0,
    lastAttemptOutcome: "",
    stallPhase: "",
    stallRunId: null,
    stallTermSentAt: 0,
    stallKillSentAt: 0,
    stallRestartCount: 0,
    stallExhaustedAt: 0,
    stallOwnershipIssue: "",
  };
  runtime.projects[id] = next;
  if (autoResumeStateReady) saveState(true);
  return { id, state: next };
}

function updateAutoResumeProjectState(id, patch, forceSave) {
  if (!autoResumeStateReady) return;
  const runtime = getAutoResumeRuntimeState();
  runtime.projects[id] = { ...(runtime.projects[id] || {}), ...patch };
  saveState(forceSave === true);
}

function readProcIdentity(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 1) return null;
  try {
    const raw = fs.readFileSync(`/proc/${pid}/stat`, "utf8").trim();
    const end = raw.lastIndexOf(") ");
    if (end < 0) return null;
    const fields = raw.slice(end + 2).trim().split(/\s+/);
    const pgid = Number(fields[2]); // /proc stat field 5 (process group)
    const startTicks = Number(fields[19]); // /proc stat field 22 (start time)
    if (!Number.isSafeInteger(pgid) || pgid <= 1 ||
        !Number.isSafeInteger(startTicks) || startTicks <= 0) return null;
    return { pid, pgid, startTicks };
  } catch {
    return null;
  }
}

function readProcStartTicks(pid) {
  const identity = readProcIdentity(pid);
  return identity ? identity.startTicks : null;
}

function readAutoResumePid(pidFile) {
  try {
    const raw = fs.readFileSync(pidFile, "utf8").trim();
    if (raw.startsWith("{")) {
      const lease = JSON.parse(raw);
      const pid = Number(lease && lease.pid);
      const pgid = Number(lease && lease.pgid);
      const processStartTicks = Number(lease && lease.processStartTicks);
      const runId = normalizeAutoResumeSessionId(lease && lease.runId);
      if (!Number.isSafeInteger(pid) || pid <= 1 || pid === process.pid ||
          !Number.isSafeInteger(pgid) || pgid <= 1 || !runId ||
          !Number.isSafeInteger(processStartTicks) || processStartTicks <= 0) return null;
      return { pid, pgid, processGroup: true, runId, processStartTicks, createdAt: normalizeAutoResumeTimestamp(lease.createdAt), schema: Number(lease.schema) || 1 };
    }
    // Read leases created by older versions, but never signal one: it has no
    // identity/start-time proof and may refer to a reused PID.
    const match = /^(pgid:)?(\d+)$/.exec(raw);
    if (!match) return null;
    const pid = parseInt(match[2], 10);
    if (!Number.isSafeInteger(pid) || pid <= 1 || pid === process.pid) return null;
    return { pid, pgid: pid, processGroup: Boolean(match[1]), legacy: true };
  } catch {
    return null;
  }
}

function isProcessAlive(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 1) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function isProcessGroupAlive(pgid) {
  if (!Number.isSafeInteger(pgid) || pgid <= 1) return false;
  try {
    process.kill(-pgid, 0);
    return true;
  } catch {
    return false;
  }
}

function isOwnedAutoResumeProcess(pidInfo, expectedRunId = "") {
  if (!pidInfo || pidInfo.legacy || !pidInfo.processGroup || !pidInfo.runId || !pidInfo.processStartTicks) return false;
  if (pidInfo.pid !== pidInfo.pgid) return false;
  if (expectedRunId && pidInfo.runId !== expectedRunId) return false;
  if (!isProcessAlive(pidInfo.pid)) return false;
  const identity = readProcIdentity(pidInfo.pid);
  return Boolean(identity && identity.startTicks === pidInfo.processStartTicks && identity.pgid === pidInfo.pgid);
}

function readAutoResumeRunnerStatus(statusFile) {
  try {
    const raw = fs.readFileSync(statusFile, "utf8");
    if (Buffer.byteLength(raw, "utf8") > AUTO_RESUME_STATUS_MAX_BYTES) return null;
    const text = raw.trim();
    if (!text) return null;
    if (text.startsWith("{")) {
      const value = JSON.parse(text);
      const phase = String(value.phase || "");
      const allowed = new Set(["starting", "running", "exited", "failed", "terminated", "cd_failed", "launcher_failed"]);
      if (!allowed.has(phase)) return null;
      const pid = Number(value.pid);
      const updatedAt = normalizeAutoResumeTimestamp(value.updatedAt);
      const exitCode = value.exitCode === null || value.exitCode === undefined || value.exitCode === "" ? null : Number(value.exitCode);
      if (!updatedAt || !Number.isSafeInteger(pid) || pid < 0 ||
          (exitCode !== null && !Number.isSafeInteger(exitCode))) return null;
      return {
        phase,
        pid,
        updatedAt,
        exitCode,
        runId: normalizeAutoResumeSessionId(value.runId),
        startedAt: normalizeAutoResumeTimestamp(value.startedAt),
        signal: String(value.signal || "").slice(0, 32),
        origin: String(value.origin || "runner").slice(0, 64),
        detail: String(value.detail || "").slice(0, 240),
      };
    }
    // Backward-compatible reader for the pre-lease TSV status format.
    const fields = text.split("\t");
    const [phase, rawPid, rawUpdatedAt, rawExitCode, rawLauncherError] = fields;
    const allowed = new Set(["starting", "running", "exited", "failed", "terminated", "cd_failed", "launcher_failed"]);
    if (!allowed.has(phase)) return null;
    const pid = parseInt(rawPid, 10);
    const updatedAt = normalizeAutoResumeTimestamp(rawUpdatedAt);
    const exitCode = rawExitCode === "" || rawExitCode === undefined ? null : parseInt(rawExitCode, 10);
    if (!updatedAt || (rawPid && (!Number.isSafeInteger(pid) || pid < 0))) return null;
    return { phase, pid: Number.isSafeInteger(pid) ? pid : 0, updatedAt, exitCode: Number.isSafeInteger(exitCode) ? exitCode : null, runId: "", startedAt: 0, signal: "", origin: "legacy", detail: rawLauncherError ? String(rawLauncherError).slice(0, 240) : "" };
  } catch {
    return null;
  }
}

function refreshAutoResumeProjectStatus(proj, index) {
  const { id, pidFile, statusFile } = autoResumeProjectFiles(proj, index);
  const status = readAutoResumeRunnerStatus(statusFile);
  if (!status || !autoResumeStateReady) return;
  const runtime = getAutoResumeRuntimeState();
  const previous = runtime.projects[id] || {};
  // Once this proxy has issued a run id, only that exact runner may update
  // its state. Legacy TSV files have no identity proof and must not overwrite
  // a newer launch after restart or upgrade.
  if (previous.runnerRunId && previous.runnerRunId !== status.runId) return;
  if (previous.runnerUpdatedAt === status.updatedAt && previous.phase === status.phase &&
      previous.runnerSignal === status.signal && previous.runnerExitCode === status.exitCode) return;

  const lease = readAutoResumePid(pidFile);
  const processStartTicks = lease && lease.runId === status.runId ? lease.processStartTicks : null;

  updateAutoResumeProjectState(id, {
    phase: status.phase,
    runnerPid: status.pid || null,
    runnerUpdatedAt: status.updatedAt,
    runnerRunId: status.runId || previous.runnerRunId || null,
    runnerStartedAt: status.startedAt || previous.runnerStartedAt || null,
    processStartTicks: processStartTicks || previous.processStartTicks || null,
    runnerExitCode: status.exitCode,
    runnerSignal: status.signal || "",
    runnerOrigin: status.origin || "runner",
    runnerDetail: status.detail || "",
    launcherError: status.origin === "cmd_start" ? status.detail || null : (previous.launcherError || null),
  }, true);

  const label = autoResumeProjectLabel(proj, index);
  if (status.phase === "running") addEventLog("auto_resume_running", 0, `闲置恢复：${label} runner 已启动（CLI 状态待确认）`, "");
  else if (status.phase === "exited") addEventLog("auto_resume_exited", 0, `闲置恢复：${label} 已退出（代码 ${status.exitCode ?? 0}）`, "");
  else if (status.phase === "terminated") addEventLog("auto_resume_terminated", 0, `闲置恢复：${label} 进程终止（信号 ${status.signal || "未知"}，来源 ${status.origin || "未知"}）`, "");
  else if (status.phase === "cd_failed") addEventLog("auto_resume_command_failed", 0, `闲置恢复：${label} 无法进入项目目录`, "");
  else if (status.phase === "failed") addEventLog("auto_resume_command_failed", 0, `闲置恢复：${label} 命令退出失败（代码 ${status.exitCode ?? "未知"}）`, "");
  else if (status.phase === "launcher_failed") addEventLog("auto_resume_launcher_failed", 0, `闲置恢复：${label} 启动器失败${status.detail ? "（"+status.detail+"）" : ""}`, "");
}

function refreshAutoResumeLaunchTimeout(proj, index, now = Date.now()) {
  if (!autoResumeStateReady) return;
  const id = autoResumeProjectId(proj, index);
  const projectState = getAutoResumeRuntimeState().projects[id] || {};
  const launchedAt = normalizeAutoResumeTimestamp(projectState.launchRequestedAt);
  if (projectState.phase !== "launching" || !launchedAt || now - launchedAt < AUTO_RESUME_LAUNCH_TIMEOUT_MS || projectState.launchTimeoutAt) return;
  updateAutoResumeProjectState(id, {
    phase: "launcher_timeout",
    launcherPhase: "timeout",
    launchTimeoutAt: now,
    lastAttemptOutcome: "launcher_timeout",
    runnerDetail: "no runner status arrived before launch timeout",
  }, true);
  addEventLog("auto_resume_launcher_timeout", 0, `闲置恢复：${autoResumeProjectLabel(proj, index)} 启动请求超时，未收到 runner 状态`, "");
}

function autoResumeRunnerStallReferenceTime(projectState) {
  return Math.max(
    normalizeAutoResumeTimestamp(lastKeyUseTime),
    normalizeAutoResumeTimestamp(projectState && projectState.runnerStartedAt),
    normalizeAutoResumeTimestamp(projectState && projectState.launchRequestedAt),
  );
}

function autoResumeStallRestartCount(projectState) {
  const count = Number(projectState && projectState.stallRestartCount);
  return Number.isSafeInteger(count) && count > 0 ? count : 0;
}

function updateAutoResumeLastResumeTime(now, idleMinutes) {
  lastResumeTime = now;
  const runtime = getAutoResumeRuntimeState();
  runtime.lastResumeTime = lastResumeTime;
  runtime.lastTriggerIdleMinutes = Math.round(idleMinutes);
  persistAutoResumeRuntime();
  saveState(true);
  broadcastStatus();
}

function markAutoResumeStallOwnershipUnknown(id, projectState, runnerRunId, reason, label) {
  const previousReason = String(projectState.stallOwnershipIssue || "");
  if (previousReason === reason && projectState.stallRunId === runnerRunId) return;
  updateAutoResumeProjectState(id, {
    phase: "ownership_unknown",
    stallPhase: "ownership_unknown",
    stallRunId: runnerRunId,
    stallOwnershipIssue: reason,
    runnerDetail: "runner lease ownership cannot be verified; no signal sent",
  }, true);
  addEventLog("auto_resume_stall_ownership_unknown", 0, `闲置恢复：${label} runner 归属无法验证，未终止、未重启`, "");
}

function checkAutoResumeRunnerStall(proj, index, id, projectState, now, idleMinutes) {
  const stallMinutes = Number(config.autoResumeRunnerStallMinutes) || 0;
  const maxRestarts = Number(config.autoResumeRunnerMaxStallRestarts) || 0;
  const runnerRunId = normalizeAutoResumeSessionId(projectState.runnerRunId);
  if (stallMinutes <= 0 || maxRestarts <= 0 || !runnerRunId) return false;

  const referenceAt = autoResumeRunnerStallReferenceTime(projectState);
  if (!referenceAt || now - referenceAt < stallMinutes * 60000) return false;

  const label = autoResumeProjectLabel(proj, index);
  const files = autoResumeProjectFiles(proj, index);
  const lease = readAutoResumePid(files.pidFile);
  const matchingLease = lease && lease.runId === runnerRunId;
  const ownedLease = matchingLease && isOwnedAutoResumeProcess(lease, runnerRunId);
  const stallRunId = normalizeAutoResumeSessionId(projectState.stallRunId);
  const stallPhase = String(projectState.stallPhase || "");
  const restartCount = autoResumeStallRestartCount(projectState);

  if (stallRunId === runnerRunId && ["terminating", "force_killing", "termination_failed", "force_kill_failed"].includes(stallPhase)) {
    const trackedPgid = Number(projectState.stallPgid);
    if (!isProcessGroupAlive(trackedPgid)) {
      // The process group that was proven to belong to this runner is gone.
      // Remove only its matching, now-dead lease before launching the single
      // permitted replacement.
      if (matchingLease) {
        try { fs.unlinkSync(files.pidFile); } catch { /* runner may have cleaned it already */ }
      } else if (lease) {
        markAutoResumeStallOwnershipUnknown(id, projectState, runnerRunId, "lease_replaced", label);
        return true;
      }
      const nextRestartCount = restartCount + 1;
      updateAutoResumeProjectState(id, {
        phase: "relaunching",
        runnerPid: null,
        runnerRunId: null,
        runnerStartedAt: null,
        processStartTicks: null,
        stallPhase: "relaunching",
        stallRunId: runnerRunId,
        stallRestartCount: nextRestartCount,
        stallOwnershipIssue: "",
        attemptCount: Math.max(1, Number(projectState.attemptCount) || 0) + 1,
        lastAttemptAt: now,
        lastAttemptOutcome: "stall_relaunching",
      }, true);
      addEventLog("auto_resume_stall_relaunching", 0, `闲置恢复：${label} 已确认停滞 runner 退出，执行本周期唯一一次重启`, "");
      updateAutoResumeLastResumeTime(now, idleMinutes);
      triggerResume(proj, index);
      return true;
    }

    if (!ownedLease) {
      markAutoResumeStallOwnershipUnknown(id, projectState, runnerRunId, "lease_identity_changed", label);
      return true;
    }

    const termSentAt = normalizeAutoResumeTimestamp(projectState.stallTermSentAt);
    if (stallPhase === "terminating" && termSentAt && now - termSentAt >= AUTO_RESUME_STALL_TERM_GRACE_MS && !normalizeAutoResumeTimestamp(projectState.stallKillSentAt)) {
      try {
        process.kill(-lease.pgid, "SIGKILL");
        updateAutoResumeProjectState(id, {
          stallPhase: "force_killing",
          stallKillSentAt: now,
          stallOwnershipIssue: "",
        }, true);
        addEventLog("auto_resume_stall_force_killed", 0, `闲置恢复：${label} 停滞 runner 在 30 秒内未退出，已强制终止已验证进程组`, "");
      } catch (error) {
        updateAutoResumeProjectState(id, {
          stallPhase: "force_kill_failed",
          stallOwnershipIssue: "force_kill_failed",
          runnerDetail: "verified runner force kill could not be sent",
        }, true);
        addEventLog("auto_resume_stall_force_kill_failed", 0, `闲置恢复：${label} 已验证 runner 无法强制终止，未重启`, "");
        console.error(`[proxy] auto-resume force kill failed for ${label}: ${error.message}`);
      }
    }
    return true;
  }

  if (stallRunId === runnerRunId && stallPhase === "relaunching") return true;
  if (restartCount >= maxRestarts) {
    if (!normalizeAutoResumeTimestamp(projectState.stallExhaustedAt)) {
      updateAutoResumeProjectState(id, {
        stallPhase: "restart_exhausted",
        stallRunId: runnerRunId,
        stallExhaustedAt: now,
        lastAttemptOutcome: "stall_restart_exhausted",
      }, true);
      addEventLog("auto_resume_stall_restart_exhausted", 0, `闲置恢复：${label} 已达到本闲置周期的停滞重启上限，未再重启`, "");
    }
    return true;
  }
  if (!ownedLease) {
    if (lease) markAutoResumeStallOwnershipUnknown(id, projectState, runnerRunId, "lease_identity_invalid", label);
    return false;
  }

  // The lease has a random run ID, a stable process start tick, and a
  // dedicated process group. Those proofs are required before signaling.
  updateAutoResumeProjectState(id, {
    phase: "stall_terminating",
    stallPhase: "terminating",
    stallRunId: runnerRunId,
    stallPid: lease.pid,
    stallPgid: lease.pgid,
    stallProcessStartTicks: lease.processStartTicks,
    stallTermSentAt: now,
    stallKillSentAt: 0,
    stallOwnershipIssue: "",
  }, true);
  try {
    process.kill(-lease.pgid, "SIGTERM");
    addEventLog("auto_resume_stall_terminating", 0, `闲置恢复：${label} runner 长时间无进展，正在终止已验证进程组`, "");
  } catch (error) {
    updateAutoResumeProjectState(id, {
      stallPhase: "termination_failed",
      stallOwnershipIssue: "term_failed",
      runnerDetail: "verified runner termination could not be sent",
    }, true);
    addEventLog("auto_resume_stall_termination_failed", 0, `闲置恢复：${label} 已验证 runner 无法终止，未重启`, "");
    console.error(`[proxy] auto-resume stall termination failed for ${label}: ${error.message}`);
  }
  return true;
}

function configureAutoResumeTimer() {
  if (autoResumeTimer) { clearInterval(autoResumeTimer); autoResumeTimer = null; }
  if (!config.autoResume || !autoResumeStateReady) return;
  for (const proj of config.autoResumeProjects || []) {
    if (proj.path) proj.path = normalizePath(proj.path);
  }
  initializeAutoResumeKeyUseTime();
  try {
    checkAutoResume();
  } catch (error) {
    console.error(`[proxy] auto-resume initial check failed: ${error.message}`);
  }
  autoResumeTimer = setInterval(() => {
    try { checkAutoResume(); } catch (error) { console.error(`[proxy] auto-resume check failed: ${error.message}`); }
  }, 30000);
}

function checkAutoResume(now = Date.now()) {
  if (!config.autoResume || !autoResumeStateReady || !config.autoResumeProjects || config.autoResumeProjects.length === 0) return;
  for (let index = 0; index < config.autoResumeProjects.length; index++) {
    refreshAutoResumeProjectStatus(config.autoResumeProjects[index], index);
    refreshAutoResumeLaunchTimeout(config.autoResumeProjects[index], index, now);
  }
  initializeAutoResumeKeyUseTime();
  const idleMinutes = (now - lastKeyUseTime) / 60000;
  if (idleMinutes < config.autoResumeIdleMinutes) return;
  // A long-running request proves that the downstream task is still inside
  // the proxy. Wait for it to finish before opening another Codex terminal;
  // the Key heartbeat remains unchanged and will be evaluated on the next
  // check, so a completed/stalled task still reaches the configured threshold.
  if (getActiveRequestCount() > 0) return;
  let attempted = false;
  for (let index = 0; index < config.autoResumeProjects.length; index++) {
    const proj = config.autoResumeProjects[index];
    if (!proj.path || !proj.cmd) continue;
    const { id, state: projectState } = autoResumeProjectState(proj, index);
    if (checkAutoResumeRunnerStall(proj, index, id, projectState, now, idleMinutes)) continue;
    const sinceAttempt = (now - Number(projectState.lastAttemptAt || 0)) / 60000;
    // One initial launch per continuous idle episode. A subsequent real Key
    // use resets the episode; failed runners are never blindly replayed.
    if (projectState.attemptCount > 0 || sinceAttempt < config.autoResumeDebounceMinutes) continue;
    projectState.attemptCount = 1;
    projectState.lastAttemptAt = now;
    projectState.lastAttemptOutcome = "launching";
    updateAutoResumeProjectState(id, projectState, true);
    triggerResume(proj, index);
    attempted = true;
  }
  if (attempted) {
    updateAutoResumeLastResumeTime(now, idleMinutes);
    console.log("[proxy] autoResume triggered after " + Math.round(idleMinutes) + "min without key use");
    addEventLog("auto_resume_triggered", 0, `闲置恢复触发：Key 已 ${Math.round(idleMinutes)} 分钟未使用（本周期一次初始启动，受管停滞可额外重启一次）`, "");
  }
}

function triggerResume(proj, index) {
  const label = autoResumeProjectLabel(proj, index);
  const files = autoResumeProjectFiles(proj, index);
  const normalizedPath = normalizePath(proj.path);
  const currentState = getAutoResumeRuntimeState().projects[files.id] || {};
  const activeLaunch = autoResumeLaunches.get(files.id);
  if (activeLaunch && activeLaunch.runId === currentState.runnerRunId) return;
  const livePid = readAutoResumePid(files.pidFile);
  if (livePid && isProcessAlive(livePid.pid)) {
    const owned = isOwnedAutoResumeProcess(livePid);
    updateAutoResumeProjectState(files.id, {
      phase: owned ? "running" : "ownership_unknown",
      runnerPid: livePid.pid,
      runnerRunId: livePid.runId || null,
      runnerStartedAt: currentState.runnerStartedAt || currentState.launchRequestedAt || null,
      processStartTicks: livePid.processStartTicks || null,
      runnerDetail: owned ? "managed runner is already alive" : "existing PID lease cannot be verified; no signal sent",
    }, true);
    addEventLog(owned ? "auto_resume_already_running" : "auto_resume_ownership_unknown", 0,
      owned ? `闲置恢复：${label} 已有本功能启动的 runner，跳过重复启动` : `闲置恢复：${label} 发现无法验证归属的进程，未终止、跳过启动`, "");
    return;
  }
  // A dead lease is safe to remove. No signal is ever sent to an unverified
  // or directory-matched process.
  if (livePid) {
    try { fs.unlinkSync(files.pidFile); } catch { /* already removed */ }
  }
  if (currentState.phase === "launching" && (Date.now() - (currentState.launchRequestedAt || 0)) < AUTO_RESUME_LAUNCH_TIMEOUT_MS) return;

  try {
    if (!fs.statSync(normalizedPath).isDirectory()) throw new Error("project directory is unavailable");
    if (!fs.existsSync(RESUME_SCRIPT)) throw new Error("resume launcher is unavailable");
    try { fs.unlinkSync(files.statusFile); } catch { /* stale status is optional */ }

    const startedAt = Date.now();
    const runId = crypto.randomUUID();
    const command = buildAutoResumeCommand(proj);
    const usesFixedSession = usesFixedAutoResumeSession(proj);
    if (proj.resumeMode === "fixed_session" && !usesFixedSession) {
      throw new Error("fixed-session recovery requires a valid sessionId and {sessionId} command placeholder");
    }
    const commandWarning = getAutoResumeCommandWarning(proj, command);
    updateAutoResumeProjectState(files.id, {
      phase: "launching",
      launchRequestedAt: startedAt,
      launchTimeoutAt: null,
      launcherPhase: "pending",
      launcherExitCode: null,
      launcherSignal: null,
      runnerRunId: runId,
      runnerStartedAt: startedAt,
      runnerPid: null,
      processStartTicks: null,
      stallPhase: "",
      stallRunId: null,
      stallTermSentAt: 0,
      stallKillSentAt: 0,
      stallOwnershipIssue: "",
      commandHash: autoResumeCommandHash(command),
      commandWarning,
      runnerDetail: "waiting for runner status",
    }, true);
    addEventLog("auto_resume_launching", 0, `闲置恢复：正在启动 ${label}`, "");
    if (commandWarning) addEventLog("auto_resume_session_ambiguous", 0, `闲置恢复：${label} 使用 resume --last，无法保证恢复到指定会话`, "");
    const child = spawn("/bin/bash", [
      RESUME_SCRIPT,
      normalizedPath,
      command,
      files.pidFile,
      files.statusFile,
      `Codex Resume - ${files.id}`,
      config.cmdPath || "/mnt/c/Windows/System32/cmd.exe",
      runId,
    ], { detached: true, stdio: "ignore" });
    const launchRecord = { child, runId };
    autoResumeLaunches.set(files.id, launchRecord);
    let settled = false;
    const finish = (phase, extra, eventType, message) => {
      if (settled) return;
      settled = true;
      if (autoResumeLaunches.get(files.id) !== launchRecord) return;
      autoResumeLaunches.delete(files.id);
      updateAutoResumeProjectState(files.id, { launcherPhase: phase, launcherUpdatedAt: Date.now(), ...extra }, true);
      addEventLog(eventType, 0, message, "");
      broadcastStatus();
    };
    child.once("error", error => {
      finish("failed", { launcherError: String(error.message || error).slice(0, 160) }, "auto_resume_launcher_failed", `闲置恢复：${label} 启动器失败`);
    });
    child.once("close", (code, signal) => {
      if (code === 0) {
        finish("returned", { launcherExitCode: 0, launcherSignal: null }, "auto_resume_launcher_returned", `闲置恢复：${label} Windows 启动请求已返回，等待 runner/CLI 状态`);
      } else {
        finish("failed", { launcherExitCode: Number.isInteger(code) ? code : null, launcherSignal: signal || null }, "auto_resume_launcher_failed", `闲置恢复：${label} 启动器退出失败`);
      }
    });
    child.unref();
    console.log(`[proxy] autoResume launch requested: ${label}`);
  } catch (error) {
    updateAutoResumeProjectState(files.id, { phase: "launcher_failed", launcherPhase: "failed", launcherUpdatedAt: Date.now(), launcherError: String(error.message || error).slice(0, 160), lastAttemptOutcome: "launcher_failed" }, true);
    addEventLog("auto_resume_launcher_failed", 0, `闲置恢复：${label} 无法启动`, "");
    console.error(`[proxy] autoResume launcher error for ${label}: ${error.message}`);
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
    try {
      autoRecover();
    } catch (e) { console.error("[proxy] daily auto-recover error:", e.message); }
    autoRecoverDailyNextTime = calcNextDailyRun(autoRecoverDailyNextTime, config.autoRecoverDailyDays, config.autoRecoverDailyHour, config.autoRecoverDailyMinute);
    scheduleDailyRecover();
  }, delay);
}
function schedulePollRecover() {
  if (autoRecoverPollTimer) clearTimeout(autoRecoverPollTimer);
  const interval = Math.max(60000, (config.autoRecoverPollInterval || 5) * 60000);
  autoRecoverPollNextTime = Date.now() + interval;
  autoRecoverPollTimer = setTimeout(() => {
    autoRecoverPollTimer = null;
    autoRecoverPollNextTime = 0;
    const codes = config.autoRecoverPollCodes || [];
    let hasMatch = false;
    for (let i = 0; i < accounts.length; i++) {
      const ks = getKeyState(i);
      if (ks.failCode && ks.failReason !== "insufficient_quota" && codes.includes(ks.failCode)) { hasMatch = true; break; }
    }
    if (!hasMatch) {
      console.log(`[proxy] poll-recover: no matching keys, stopped`);
      return;
    }
    console.log(`[proxy] poll-recover: checking ${codes.join(",")} keys...`);
    try { autoRecover(codes); } catch (e) { console.error("[proxy] poll auto-recover error:", e.message); }
    schedulePollRecover();
  }, interval);
}
function autoRecover(optCodes){
  if (!config.autoRecover && !config.autoRecoverDaily && !optCodes) return;
  console.log(`[proxy] auto-recover: fired (daily=${config.autoRecoverDaily}${optCodes ? ', poll' : ''})`);
  const codes = optCodes || config.autoRecoverCodes || [];
  const checkDiscarded = config.autoRecoverDiscarded === true;
  const toCheck = [];
  for (let i = 0; i < accounts.length; i++) {
    const ks = getKeyState(i);
    if (ks.status === "shielded") continue;
    if (ks.failReason === "insufficient_quota") continue;
    if (ks.status === "locked" && !codes.includes(ks.failCode)) continue;
    if (!ks.failCode && ks.status !== "discarded") continue;
    if (ks.status === "discarded" && !checkDiscarded) continue;
    if (ks.failCode && !codes.includes(ks.failCode)) continue;
    toCheck.push(i);
  }
  if (!toCheck.length) { console.log(`[proxy] auto-recover: 0 keys to check`); return; }
  console.log(`[proxy] auto-recover: checking ${toCheck.length} key(s)...`);
  const delays = config.autoRecoverDelays || [800];
  let idx = 0;
  function checkNext(){
    if (idx >= toCheck.length) { console.log(`[proxy] auto-recover: all ${toCheck.length} key(s) done`); return; }
    const i = toCheck[idx++];
    const acct = accounts[i];
    if (!acct) { setTimeout(checkNext, delays[Math.random()*delays.length|0]); return; }
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
            const wasStatus = ks.status || "active";
            ks.failCode = null;
            ks.failTime = null;
            ks.failPeriod = "";
            ks.failCount = 0;
            ks.failReason = null;
            if (ks.status === "discarded" || ks.status === "locked") ks.status = "active";
            allFailedNotified = false;
            saveState();
            broadcastStatus();
            console.log(`[proxy] auto-recover: #${i+1} recovered (was ${wasStatus})`);
            const wasLabel = wasStatus === "discarded" ? "废弃" : wasStatus === "locked" ? "锁定" : "冷却";
            addEventLog("recover", i + 1, `自动恢复成功 (此前状态: ${wasLabel})`, acct.url);
        } else {
          console.log(`[proxy] auto-recover: #${i+1} test returned ${testRes.statusCode}, not recovered`);
        }
        setTimeout(checkNext, delays[Math.random()*delays.length|0]);
      });
    });
    testReq.on("error", () => { setTimeout(checkNext, delays[Math.random()*delays.length|0]); });
    testReq.on("timeout", () => { testReq.destroy(); setTimeout(checkNext, delays[Math.random()*delays.length|0]); });
    testReq.end();
  }
  checkNext();
}

function addLog(entry) {
  requestLog.push(entry);
  if (requestLog.length > MAX_LOG) requestLog.splice(0, requestLog.length - MAX_LOG);
  recordLogSummary(entry);
  if (LOG_FILE_ENABLED) writeLogEntry(entry);
  broadcastLog(entry);
}

function addEventLog(eventType, idx, message, url) {
  const entry = { time: Date.now(), type: "event", eventType, idx, message, url: url || "" };
  requestLog.push(entry);
  if (requestLog.length > MAX_LOG) requestLog.splice(0, requestLog.length - MAX_LOG);
  recordLogSummary(entry);
  if (LOG_FILE_ENABLED) writeLogEntry(entry);
  broadcastLog(entry);
}

function sanitizeUpstreamErrorMessage(value) {
  let message = String(value == null ? "" : value)
    .replace(/<[^>]*>/g, " ")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  // Upstream error bodies are outside our trust boundary. Keep the diagnostic
  // useful without allowing an accidentally echoed credential into local logs.
  message = message
    .replace(/\bBearer\s+[A-Za-z0-9._~+\/=:-]{8,}\b/gi, "Bearer [redacted]")
    .replace(/\b(?:sk|rk)-[A-Za-z0-9_-]{8,}\b/gi, "[redacted-key]")
    .replace(/\b(api[_-]?key|authorization)\s*[:=]\s*[^\s,;]+/gi, "$1=[redacted]");
  return message.slice(0, LOG_ERROR_MESSAGE_MAX_CHARS);
}

function findUpstreamErrorMessage(value, depth = 0) {
  if (depth > 3 || value == null) return "";
  if (typeof value === "string" || typeof value === "number") return String(value);
  if (Array.isArray(value)) {
    for (const item of value) {
      const message = findUpstreamErrorMessage(item, depth + 1);
      if (message) return message;
    }
    return "";
  }
  if (typeof value !== "object") return "";
  for (const key of ["message", "detail", "error_description", "description", "error"]) {
    const message = findUpstreamErrorMessage(value[key], depth + 1);
    if (message) return message;
  }
  return "";
}

function extractUpstreamErrorMessage(payload) {
  let value = payload;
  if (Buffer.isBuffer(value)) value = value.toString("utf8");
  if (typeof value === "string") {
    const raw = value;
    try { value = JSON.parse(raw); } catch { return sanitizeUpstreamErrorMessage(raw); }
  }
  return sanitizeUpstreamErrorMessage(findUpstreamErrorMessage(value));
}

function classifyUpstreamErrorMessage(message) {
  const text = String(message || "");
  if (/capacity|at capacity|overloaded|busy|try a different model|not currently available/i.test(text)) return "model_at_capacity";
  if (/quota|insufficient|billing|billing_hard_limit|_limit_exceeded|usage limit|usage_limit|daily limit|weekly limit|monthly limit/i.test(text)) return "insufficient_quota";
  return "upstream_api_error";
}

function captureUpstreamErrorMessage(apiRes, callback) {
  const chunks = [];
  let capturedBytes = 0;
  let settled = false;
  let timer = null;
  const finish = () => {
    if (settled) return;
    settled = true;
    if (timer) clearTimeout(timer);
    const message = extractUpstreamErrorMessage(chunks.length ? Buffer.concat(chunks) : "");
    if (!apiRes.destroyed) apiRes.destroy();
    callback(message);
  };
  apiRes.on("data", chunk => {
    if (capturedBytes >= UPSTREAM_ERROR_CAPTURE_MAX_BYTES) return;
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    const remaining = UPSTREAM_ERROR_CAPTURE_MAX_BYTES - capturedBytes;
    chunks.push(buffer.subarray(0, remaining));
    capturedBytes += Math.min(buffer.length, remaining);
    if (capturedBytes >= UPSTREAM_ERROR_CAPTURE_MAX_BYTES) finish();
  });
  apiRes.once("end", finish);
  apiRes.once("error", finish);
  apiRes.once("close", finish);
  timer = setTimeout(finish, UPSTREAM_ERROR_CAPTURE_TIMEOUT_MS);
  if (timer.unref) timer.unref();
  apiRes.resume();
}

function addStreamTerminalLog(idx, lifecycle) {
  const outcome = lifecycle.terminalKind || "unknown";
  const reason = lifecycle.terminalReason || "unknown";
  const sawDone = lifecycle.sawDone === true;
  const entry = {
    time: Date.now(),
    type: "event",
    eventType: "stream_terminal",
    idx: idx + 1,
    message: `stream ${outcome}: ${reason}; done=${sawDone ? "yes" : "no"}`,
    streamId: lifecycle.responseId || "",
    streamOutcome: outcome,
    streamReason: reason,
    streamSawDone: sawDone,
    url: (accounts[idx] && accounts[idx].url) || "",
    upstreamErrorReason: outcome === "failed" ? reason : "",
    streamErrorMsg: lifecycle.upstreamErrorMessage || "",
    terminalSource: lifecycle.terminalSource || "upstream_sse",
  };
  requestLog.push(entry);
  if (requestLog.length > MAX_LOG) requestLog.splice(0, requestLog.length - MAX_LOG);
  recordLogSummary(entry);
  if (LOG_FILE_ENABLED) writeLogEntry(entry);
  broadcastLog(entry);
}

function addDownstreamTerminalLog(idx, options = {}) {
  const account = accounts[idx] || {};
  const reason = normalizeStreamTerminalReason(options.reason, "upstream_api_error");
  const errorMessage = sanitizeUpstreamErrorMessage(options.errorMessage || "");
  const status = Number.isInteger(options.status) ? options.status : 502;
  const entry = {
    time: Date.now(),
    type: "event",
    eventType: "downstream_terminal",
    idx: idx + 1,
    group: String(options.group || account.group || "A").toUpperCase(),
    method: options.method || "",
    path: options.path || "",
    reqModel: options.reqModel || null,
    overrideModel: account.model || null,
    status,
    message: `downstream failed: ${reason}; HTTP ${status}`,
    streamOutcome: "failed",
    streamReason: reason,
    streamSawDone: false,
    upstreamErrorReason: reason,
    streamErrorMsg: errorMessage,
    terminalSource: options.source || "upstream_http_error",
    url: account.url || "",
  };
  requestLog.push(entry);
  if (requestLog.length > MAX_LOG) requestLog.splice(0, requestLog.length - MAX_LOG);
  recordLogSummary(entry);
  if (LOG_FILE_ENABLED) writeLogEntry(entry);
  recordStreamOutcome(idx, reason, true);
  broadcastLog(entry);
}

function isRequestLogFailure(entry) {
  return entry.status === 0 || entry.status >= 400 || entry.streamOutcome === "failed";
}

function parseManagedLogFileName(file) {
  const match = LOG_FILE_NAME_RE.exec(String(file || ""));
  if (!match) return null;
  const startedAt = Date.parse(`${match[1]}T00:00:00.000Z`);
  if (!Number.isFinite(startedAt)) return null;
  return {
    name: String(file),
    day: match[1],
    startedAt,
    segment: match[2] ? Number(match[2]) : Number.MAX_SAFE_INTEGER,
  };
}

function logBasePath(day) {
  return path.join(LOG_DIR, `${day}.jsonl`);
}

function nextLogSegmentNumber(day) {
  let next = 1;
  try {
    for (const file of fs.readdirSync(LOG_DIR)) {
      const parsed = parseManagedLogFileName(file);
      if (parsed && parsed.day === day && Number.isFinite(parsed.segment)) next = Math.max(next, parsed.segment + 1);
    }
  } catch {}
  return next;
}

function archiveCurrentLogSegment(day) {
  const source = logBasePath(day);
  try {
    const stat = fs.statSync(source);
    if (!stat.isFile() || stat.size <= 0) return false;
    const destination = path.join(LOG_DIR, `${day}.${String(nextLogSegmentNumber(day)).padStart(6, "0")}.jsonl`);
    fs.renameSync(source, destination);
    return true;
  } catch (error) {
    if (error && error.code !== "ENOENT") console.warn(`[proxy] failed to archive request log: ${error.message}`);
    return false;
  }
}

function openLogStreamForDate(day) {
  if (!LOG_FILE_ENABLED) return false;
  try {
    fs.mkdirSync(LOG_DIR, { recursive: true });
    const base = logBasePath(day);
    let existingBytes = 0;
    try { existingBytes = Math.max(0, fs.statSync(base).size); } catch {}
    const segmentBytes = config.logSegmentMaxMiB * 1024 * 1024;
    if (existingBytes >= segmentBytes && existingBytes > 0) {
      archiveCurrentLogSegment(day);
      existingBytes = 0;
    }
    const stream = fs.createWriteStream(base, { flags: "a", mode: 0o600 });
    stream.on("error", error => {
      console.warn(`[proxy] request log stream error: ${error.message}`);
      if (logStream === stream) {
        logStream = null;
        logDate = null;
        logStreamBytes = 0;
      }
    });
    logStream = stream;
    logDate = day;
    logStreamBytes = existingBytes;
    return true;
  } catch (error) {
    console.warn(`[proxy] failed to open request log: ${error.message}`);
    logStream = null;
    logDate = null;
    logStreamBytes = 0;
    return false;
  }
}

function flushPendingLogLines() {
  while (pendingLogLines.length && !logRotationInFlight) {
    const line = pendingLogLines[0];
    if (!writeLogLine(line)) return;
    pendingLogLines.shift();
  }
}

function rotateLogStream(nextDay, archiveCurrent) {
  if (logRotationInFlight) return;
  logRotationInFlight = true;
  const previousStream = logStream;
  const previousDay = logDate;
  logStream = null;
  logDate = null;
  logStreamBytes = 0;
  const finish = () => {
    try {
      if (!LOG_FILE_ENABLED) {
        pendingLogLines.length = 0;
      } else {
        if (archiveCurrent && previousDay && previousDay === nextDay) archiveCurrentLogSegment(previousDay);
        openLogStreamForDate(nextDay);
      }
    } catch (error) {
      console.warn(`[proxy] failed to rotate request log: ${error.message}`);
    }
    logRotationInFlight = false;
    flushPendingLogLines();
    cleanOldLogs();
  };
  if (previousStream) {
    try { previousStream.end(finish); } catch { finish(); }
  } else {
    finish();
  }
}

function ensureLogStream(day) {
  if (!LOG_FILE_ENABLED || logRotationInFlight) return false;
  if (logDate === day && logStream) return true;
  if (logStream) {
    rotateLogStream(day, false);
    return false;
  }
  return openLogStreamForDate(day);
}

function queueLogLine(line) {
  if (pendingLogLines.length >= MAX_LOG) {
    pendingLogLines.shift();
    const now = Date.now();
    if (now - lastLogWriteDropWarningAt >= 5 * 60 * 1000) {
      lastLogWriteDropWarningAt = now;
      console.warn(`[proxy] request log rotation backlog exceeded ${MAX_LOG}; oldest pending entries were dropped`);
    }
  }
  pendingLogLines.push(line);
}

function writeLogLine(line) {
  if (!LOG_FILE_ENABLED) return false;
  const day = getLogDateKey(Date.now());
  if (!ensureLogStream(day)) return false;
  const lineBytes = Buffer.byteLength(line, "utf8");
  const segmentBytes = config.logSegmentMaxMiB * 1024 * 1024;
  if (logStreamBytes > 0 && logStreamBytes + lineBytes > segmentBytes) {
    rotateLogStream(day, true);
    return false;
  }
  try {
    logStream.write(line);
    logStreamBytes += lineBytes;
    return true;
  } catch (error) {
    console.warn(`[proxy] failed to write request log: ${error.message}`);
    return false;
  }
}

function serializeBoundedLogEntry(entry) {
  let line = JSON.stringify(entry) + "\n";
  if (Buffer.byteLength(line, "utf8") <= LOG_ENTRY_MAX_BYTES) return line;
  const compact = { logTruncated: true };
  for (const field of ["time", "type", "eventType", "idx", "status", "path", "group", "client", "reqModel", "overrideModel", "streamOutcome", "streamReason", "upstreamErrorReason", "terminalSource", "streamId", "message", "streamErrorMsg", "url"]) {
    const value = entry && entry[field];
    if (value === undefined || value === null) continue;
    compact[field] = typeof value === "string" ? value.slice(0, 512) : value;
  }
  line = JSON.stringify(compact) + "\n";
  return Buffer.byteLength(line, "utf8") <= LOG_ENTRY_MAX_BYTES ? line : null;
}

function writeLogEntry(entry) {
  if (!LOG_FILE_ENABLED) return;
  try {
    const line = serializeBoundedLogEntry(entry);
    if (!line) {
      const now = Date.now();
      if (now - lastLogWriteDropWarningAt >= 5 * 60 * 1000) {
        lastLogWriteDropWarningAt = now;
        console.warn(`[proxy] request log entry exceeded ${Math.round(LOG_ENTRY_MAX_BYTES / 1024)} KiB and was dropped`);
      }
      return;
    }
    if (!writeLogLine(line)) queueLogLine(line);
  } catch (error) {
    console.warn(`[proxy] failed to serialize request log: ${error.message}`);
  }
}

function cleanOldLogs() {
  if (logCleanupInFlight || logRotationInFlight) return Promise.resolve(false);
  logCleanupInFlight = true;
  const now = Date.now();
  const activeFile = logDate ? `${logDate}.jsonl` : "";
  return fs.promises.readdir(LOG_DIR, { withFileTypes: true }).then(async entries => {
    const files = [];
    for (const entry of entries) {
      if (!entry.isFile()) continue;
      const parsed = parseManagedLogFileName(entry.name);
      if (!parsed) continue;
      try {
        const stat = await fs.promises.stat(path.join(LOG_DIR, entry.name));
        files.push({ ...parsed, size: Math.max(0, stat.size) });
      } catch {}
    }
    files.sort((left, right) => left.startedAt - right.startedAt || left.segment - right.segment);
    const retained = [];
    let totalBytes = 0;
    for (const file of files) {
      const expired = LOG_RETENTION_DAYS > 0 && now - file.startedAt > LOG_RETENTION_DAYS * 86400000;
      if (expired && file.name !== activeFile) {
        try { await fs.promises.unlink(path.join(LOG_DIR, file.name)); } catch {}
        continue;
      }
      retained.push(file);
      totalBytes += file.size;
    }
    const budgetBytes = Math.max(1, Number(config.logMaxMiB) || DEFAULT_LOG_MAX_MIB) * 1024 * 1024;
    for (const file of retained) {
      if (totalBytes <= budgetBytes) break;
      // Never unlink the stream currently open by proxy.js. Older segments of
      // the same day remain eligible and the compacted summary is preserved.
      if (file.name === activeFile) continue;
      try {
        await fs.promises.unlink(path.join(LOG_DIR, file.name));
        totalBytes -= file.size;
      } catch {}
    }
    if (pruneLogRollups()) scheduleLogSummaryPersist();
  }).catch(() => {}).finally(() => {
    logCleanupInFlight = false;
  });
}

function applyRuntimeStoragePolicy() {
  const now = Date.now();
  // Configuration changes must take effect before the next request arrives:
  // compact state synchronously, then let the file cleanup continue without
  // holding the request handler or closing the proxy listener.
  compactState(now, { force: true });
  saveState(true);

  if (logStream && logDate) {
    let activeBytes = logStreamBytes;
    try { activeBytes = Math.max(activeBytes, fs.statSync(logBasePath(logDate)).size); } catch {}
    if (activeBytes >= config.logSegmentMaxMiB * 1024 * 1024) rotateLogStream(logDate, true);
  }
  cleanOldLogs();
}

function createLogHistogram() {
  return new Array(LOG_LATENCY_BOUNDS.length + 1).fill(0);
}

function normalizeLogHistogram(value) {
  const histogram = createLogHistogram();
  if (!Array.isArray(value)) return histogram;
  for (let i = 0; i < histogram.length; i++) histogram[i] = Math.max(0, Number(value[i]) || 0);
  return histogram;
}

function addLogHistogram(histogram, value) {
  if (!Array.isArray(histogram) || !Number.isFinite(value) || value < 0) return;
  let index = LOG_LATENCY_BOUNDS.findIndex(bound => value <= bound);
  if (index < 0) index = LOG_LATENCY_BOUNDS.length;
  histogram[index] = (histogram[index] || 0) + 1;
}

function estimateLogPercentile(histogram, total, fraction) {
  if (!Array.isArray(histogram) || !total) return 0;
  const target = Math.max(1, Math.ceil(total * fraction));
  let seen = 0;
  for (let i = 0; i < histogram.length; i++) {
    seen += Number(histogram[i]) || 0;
    if (seen >= target) return i < LOG_LATENCY_BOUNDS.length ? LOG_LATENCY_BOUNDS[i] : LOG_LATENCY_BOUNDS[LOG_LATENCY_BOUNDS.length - 1];
  }
  return LOG_LATENCY_BOUNDS[LOG_LATENCY_BOUNDS.length - 1];
}

function createLogMetrics() {
  return {
    requests: 0,
    success: 0,
    error4xx: 0,
    error5xx: 0,
    timeout: 0,
    streamFailed: 0,
    durationSum: 0,
    durationCount: 0,
    ttfbSum: 0,
    ttfbCount: 0,
    durationHistogram: createLogHistogram(),
    ttfbHistogram: createLogHistogram(),
  };
}

function createLogAggregate() {
  return {
    ...createLogMetrics(),
    dimensions: { upstream: {}, model: {}, path: {}, group: {}, key: {} },
    events: {},
  };
}

function normalizeLogMetrics(value) {
  const normalized = createLogMetrics();
  if (!value || typeof value !== "object") return normalized;
  for (const field of ["requests", "success", "error4xx", "error5xx", "timeout", "streamFailed", "durationSum", "durationCount", "ttfbSum", "ttfbCount"]) {
    normalized[field] = Math.max(0, Number(value[field]) || 0);
  }
  normalized.durationHistogram = normalizeLogHistogram(value.durationHistogram);
  normalized.ttfbHistogram = normalizeLogHistogram(value.ttfbHistogram);
  return normalized;
}

function normalizeLogAggregate(value) {
  const normalized = { ...createLogAggregate(), ...normalizeLogMetrics(value) };
  const dimensions = value && value.dimensions && typeof value.dimensions === "object" ? value.dimensions : {};
  for (const type of Object.keys(normalized.dimensions)) {
    normalized.dimensions[type] = {};
    const source = dimensions[type] && typeof dimensions[type] === "object" ? dimensions[type] : {};
    for (const [key, metrics] of Object.entries(source)) {
      if (Object.keys(normalized.dimensions[type]).length >= LOG_DIMENSION_LIMITS[type]) break;
      normalized.dimensions[type][String(key).slice(0, 160)] = normalizeLogMetrics(metrics);
    }
  }
  const events = value && value.events && typeof value.events === "object" ? value.events : {};
  for (const [eventType, count] of Object.entries(events)) normalized.events[String(eventType).slice(0, 160)] = Math.max(0, Number(count) || 0);
  return normalized;
}

function mergeLogMetrics(target, source) {
  if (!source) return target;
  for (const field of ["requests", "success", "error4xx", "error5xx", "timeout", "streamFailed", "durationSum", "durationCount", "ttfbSum", "ttfbCount"]) {
    target[field] += Number(source[field]) || 0;
  }
  for (const histogramField of ["durationHistogram", "ttfbHistogram"]) {
    const sourceHistogram = Array.isArray(source[histogramField]) ? source[histogramField] : [];
    for (let i = 0; i < target[histogramField].length; i++) target[histogramField][i] += Number(sourceHistogram[i]) || 0;
  }
  return target;
}

function mergeLogAggregate(target, source) {
  mergeLogMetrics(target, source);
  if (!source) return target;
  for (const type of Object.keys(target.dimensions)) {
    const sourceMap = source.dimensions && source.dimensions[type] ? source.dimensions[type] : {};
    for (const [key, metrics] of Object.entries(sourceMap)) {
      let targetKey = key;
      if (!target.dimensions[type][targetKey] && Object.keys(target.dimensions[type]).length >= LOG_DIMENSION_LIMITS[type]) targetKey = "(other)";
      if (!target.dimensions[type][targetKey]) target.dimensions[type][targetKey] = createLogMetrics();
      mergeLogMetrics(target.dimensions[type][targetKey], metrics);
    }
  }
  for (const [eventType, count] of Object.entries(source.events || {})) target.events[eventType] = (target.events[eventType] || 0) + (Number(count) || 0);
  return target;
}

function logUpstreamHost(url) {
  try { return new URL(String(url || "")).hostname || "(unknown)"; } catch { return "(unknown)"; }
}

function logDimensionValue(entry, type) {
  if (type === "upstream") return logUpstreamHost(entry.url);
  if (type === "model") return String(entry.overrideModel || entry.reqModel || "(unknown)").slice(0, 120);
  if (type === "path") return String(entry.path || "(unknown)").slice(0, 120);
  if (type === "group") return String(entry.group || "A").toUpperCase().slice(0, 32);
  if (type === "key") return entry.idx ? `#${entry.idx}` : "(unknown)";
  return "(unknown)";
}

function getLogDimensionMetrics(aggregate, entry, type) {
  const map = aggregate.dimensions[type];
  let key = logDimensionValue(entry, type);
  if (!Object.prototype.hasOwnProperty.call(map, key) && Object.keys(map).length >= LOG_DIMENSION_LIMITS[type]) key = "(other)";
  if (!map[key]) map[key] = createLogMetrics();
  return map[key];
}

function addRequestLogMetrics(metrics, entry) {
  metrics.requests++;
  const status = Number(entry.status || 0);
  const failed = status === 0 || status >= 400 || entry.streamOutcome === "failed";
  if (!failed && status >= 200 && status < 300) metrics.success++;
  if (status >= 400 && status < 500) metrics.error4xx++;
  if (status >= 500 && status < 600) metrics.error5xx++;
  if (status === 0) metrics.timeout++;
  if (entry.streamOutcome === "failed") metrics.streamFailed++;
  if (Number.isFinite(entry.duration) && entry.duration >= 0) {
    metrics.durationSum += entry.duration;
    metrics.durationCount++;
    addLogHistogram(metrics.durationHistogram, entry.duration);
  }
  if (Number.isFinite(entry.ttfb) && entry.ttfb >= 0) {
    metrics.ttfbSum += entry.ttfb;
    metrics.ttfbCount++;
    addLogHistogram(metrics.ttfbHistogram, entry.ttfb);
  }
}

function addLogEntryToAggregate(aggregate, entry) {
  if (!entry || typeof entry !== "object") return;
  if (entry.type === "event") {
    const eventType = String(entry.eventType || "event").slice(0, 120);
    aggregate.events[eventType] = (aggregate.events[eventType] || 0) + 1;
    return;
  }
  addRequestLogMetrics(aggregate, entry);
  for (const type of Object.keys(aggregate.dimensions)) addRequestLogMetrics(getLogDimensionMetrics(aggregate, entry, type), entry);
}

function getLogDateKey(time) {
  return new Date(Number.isFinite(time) ? time : Date.now()).toISOString().slice(0, 10);
}

function pruneLogRollups(now = Date.now()) {
  const minuteCutoff = now - LOG_ROLLUP_RETENTION_MS;
  let changed = false;
  for (const [minute] of logMinuteBuckets) {
    if (minute < minuteCutoff) {
      logMinuteBuckets.delete(minute);
      changed = true;
    }
  }
  const dailyRetentionDays = LOG_RETENTION_DAYS > 0 ? LOG_RETENTION_DAYS : 3650;
  const dayCutoff = getLogDateKey(now - dailyRetentionDays * 86400000);
  for (const day of logDailySummaries.keys()) {
    if (day < dayCutoff) {
      logDailySummaries.delete(day);
      changed = true;
    }
  }
  return changed;
}

function recordLogSummary(entry, options = {}) {
  if (!entry || typeof entry !== "object") return;
  if (!logSummaryLoaded) {
    if (logSummaryPendingEntries.length < MAX_LOG) logSummaryPendingEntries.push(entry);
    return;
  }
  const time = Number.isFinite(entry.time) ? entry.time : Date.now();
  const minute = Math.floor(time / 60000) * 60000;
  if (!logMinuteBuckets.has(minute)) logMinuteBuckets.set(minute, createLogAggregate());
  addLogEntryToAggregate(logMinuteBuckets.get(minute), entry);
  const day = getLogDateKey(time);
  if (!logDailySummaries.has(day)) logDailySummaries.set(day, createLogAggregate());
  addLogEntryToAggregate(logDailySummaries.get(day), entry);
  pruneLogRollups(time);
  if (!options.skipPersist) scheduleLogSummaryPersist();
  if (!options.skipIncident) scheduleLogIncidentEvaluation();
}

function logMetricsToStats(metrics, extra = {}) {
  const total = Number(metrics.requests) || 0;
  return {
    total,
    successRate: total ? Math.round(((Number(metrics.success) || 0) / total) * 10000) / 100 : 0,
    avgDuration: metrics.durationCount ? Math.round(metrics.durationSum / metrics.durationCount) : 0,
    p95: estimateLogPercentile(metrics.durationHistogram, metrics.durationCount, 0.95),
    p99: estimateLogPercentile(metrics.durationHistogram, metrics.durationCount, 0.99),
    p95Ttfb: estimateLogPercentile(metrics.ttfbHistogram, metrics.ttfbCount, 0.95),
    error4xx: Number(metrics.error4xx) || 0,
    error5xx: Number(metrics.error5xx) || 0,
    errorTimeout: Number(metrics.timeout) || 0,
    errorStream: Number(metrics.streamFailed) || 0,
    ...extra,
  };
}

function sortLogDimensions(aggregate) {
  const output = {};
  for (const type of Object.keys(aggregate.dimensions)) {
    output[type] = Object.entries(aggregate.dimensions[type])
      .map(([value, metrics]) => ({ value, ...logMetricsToStats(metrics) }))
      .sort((a, b) => (b.error5xx + b.error4xx + b.errorTimeout + b.errorStream) - (a.error5xx + a.error4xx + a.errorTimeout + a.errorStream) || b.total - a.total)
      .slice(0, 12);
  }
  return output;
}

function buildLogRollup(options = {}) {
  const now = Date.now();
  const since = Number.isFinite(options.since) ? options.since : now - LOG_ROLLUP_RETENTION_MS;
  const until = Number.isFinite(options.until) ? options.until : now;
  const aggregate = createLogAggregate();
  const useMinuteBuckets = until - since <= LOG_ROLLUP_RETENTION_MS;
  if (useMinuteBuckets) {
    for (const [minute, bucket] of logMinuteBuckets) {
      if (minute >= since - 60000 && minute <= until) mergeLogAggregate(aggregate, bucket);
    }
  } else {
    const startDay = getLogDateKey(since);
    const endDay = getLogDateKey(until);
    for (const [day, summary] of logDailySummaries) {
      if (day >= startDay && day <= endDay) mergeLogAggregate(aggregate, summary);
    }
  }
  return { aggregate, approximate: !useMinuteBuckets && (since % 86400000 !== 0 || until % 86400000 !== 0) };
}

function listActiveGroupPauses(now = Date.now()) {
  const active = [];
  for (const [group, pause] of groupPauses) {
    if (!pause || pause.expiresAt <= now) {
      groupPauses.delete(group);
      continue;
    }
    active.push({ group, expiresAt: pause.expiresAt, incidentId: pause.incidentId || "", reason: pause.reason || "" });
  }
  return active.sort((a, b) => a.expiresAt - b.expiresAt);
}

function buildLogOverview(options = {}) {
  const rollup = buildLogRollup(options);
  const now = Date.now();
  const timeline = [];
  for (let i = 29; i >= 0; i--) {
    const minute = Math.floor((now - i * 60000) / 60000) * 60000;
    const bucket = logMinuteBuckets.get(minute) || createLogAggregate();
    timeline.push({ time: minute, total: bucket.requests, errors: bucket.error4xx + bucket.error5xx + bucket.timeout + bucket.streamFailed });
  }
  return {
    stats: logMetricsToStats(rollup.aggregate, { approximate: rollup.approximate }),
    dimensions: sortLogDimensions(rollup.aggregate),
    events: rollup.aggregate.events,
    timeline,
    incidents: listLogIncidents(),
    groupPauses: listActiveGroupPauses(),
    summaryReady: logSummaryLoaded,
  };
}

function serializeLogSummary() {
  return {
    schema: LOG_SUMMARY_SCHEMA,
    savedAt: Date.now(),
    minuteBuckets: Array.from(logMinuteBuckets.entries()),
    dailySummaries: Array.from(logDailySummaries.entries()),
  };
}

function serializeBoundedLogSummary() {
  let serialized = JSON.stringify(serializeLogSummary());
  let bytes = Buffer.byteLength(serialized, "utf8");
  if (bytes <= LOG_SUMMARY_MAX_BYTES) return serialized;

  let removed = 0;
  const shrink = (map) => {
    const keys = Array.from(map.keys()).sort((left, right) => left < right ? -1 : left > right ? 1 : 0);
    for (let index = 0; index < keys.length; index++) {
      map.delete(keys[index]);
      removed++;
      if ((index + 1) % LOG_SUMMARY_MEASURE_INTERVAL !== 0 && index !== keys.length - 1) continue;
      serialized = JSON.stringify(serializeLogSummary());
      bytes = Buffer.byteLength(serialized, "utf8");
      if (bytes <= LOG_SUMMARY_MAX_BYTES) return true;
    }
    return false;
  };

  // Minute data only powers the recent operations view. Preserve all raw JSONL
  // logs and daily aggregates before dropping the oldest detailed buckets.
  if (!shrink(logMinuteBuckets)) shrink(logDailySummaries);
  if (removed) {
    const now = Date.now();
    if (now - lastLogSummaryBudgetWarningAt >= 5 * 60 * 1000) {
      lastLogSummaryBudgetWarningAt = now;
      console.warn(`[proxy] log summary exceeded ${(LOG_SUMMARY_MAX_BYTES / (1024 * 1024)).toFixed(0)} MiB; removed ${removed} oldest rollup buckets (remaining ${(bytes / (1024 * 1024)).toFixed(1)} MiB)`);
    }
  }
  return serialized;
}

function scheduleLogSummaryPersist() {
  if (!LOG_FILE_ENABLED || !logSummaryLoaded) return;
  logSummaryDirty = true;
  if (logSummaryPersistTimer) return;
  logSummaryPersistTimer = setTimeout(() => {
    logSummaryPersistTimer = null;
    persistLogSummary();
  }, LOG_SUMMARY_PERSIST_DELAY_MS);
  if (logSummaryPersistTimer.unref) logSummaryPersistTimer.unref();
}

async function persistLogSummary() {
  if (!LOG_FILE_ENABLED || logSummaryPersisting || !logSummaryDirty) return;
  logSummaryPersisting = true;
  logSummaryDirty = false;
  try {
    await fs.promises.mkdir(LOG_DIR, { recursive: true });
    const tempFile = `${LOG_SUMMARY_FILE}.${process.pid}.tmp`;
    const serialized = serializeBoundedLogSummary();
    await fs.promises.writeFile(tempFile, serialized, "utf8");
    await fs.promises.rename(tempFile, LOG_SUMMARY_FILE);
  } catch (error) {
    console.error(`[proxy] failed to persist log summary: ${error.message}`);
  } finally {
    logSummaryPersisting = false;
    if (logSummaryDirty) scheduleLogSummaryPersist();
  }
}

function loadLogSummary() {
  let summaryWasLoaded = false;
  fs.promises.stat(LOG_SUMMARY_FILE).then(stat => {
    if (!stat.isFile() || stat.size > LOG_SUMMARY_MAX_BYTES * 2) {
      if (stat.size > LOG_SUMMARY_MAX_BYTES * 2) console.warn("[proxy] log summary is too large to load; rebuilding bounded rollups in the background");
      return null;
    }
    return fs.promises.readFile(LOG_SUMMARY_FILE, "utf8");
  }).then(raw => {
    if (!raw) return;
    const saved = JSON.parse(raw);
    if (!saved || saved.schema !== LOG_SUMMARY_SCHEMA) return;
    for (const [minute, aggregate] of Array.isArray(saved.minuteBuckets) ? saved.minuteBuckets : []) {
      const numericMinute = Number(minute);
      if (Number.isFinite(numericMinute)) logMinuteBuckets.set(numericMinute, normalizeLogAggregate(aggregate));
    }
    for (const [day, aggregate] of Array.isArray(saved.dailySummaries) ? saved.dailySummaries : []) {
      if (/^\d{4}-\d{2}-\d{2}$/.test(String(day))) logDailySummaries.set(day, normalizeLogAggregate(aggregate));
    }
    summaryWasLoaded = true;
  }).catch(() => {}).finally(() => {
    logSummaryLoaded = true;
    const pending = logSummaryPendingEntries.splice(0);
    for (const entry of pending) recordLogSummary(entry, { skipPersist: true, skipIncident: true });
    const pruned = pruneLogRollups();
    if (pending.length || pruned) scheduleLogSummaryPersist();
    scheduleLogIncidentEvaluation();
    if (!summaryWasLoaded && LOG_FILE_ENABLED) {
      const timer = setTimeout(() => startLogSummaryRebuild(), 2000);
      if (timer.unref) timer.unref();
    }
  });
}

function encodeLogCursor(cursor) {
  if (!cursor) return "";
  return Buffer.from(JSON.stringify(cursor), "utf8").toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function decodeLogCursor(value) {
  if (!value || typeof value !== "string" || value.length > 512) return null;
  try {
    const padded = value.replace(/-/g, "+").replace(/_/g, "/") + "===".slice((value.length + 3) % 4);
    const cursor = JSON.parse(Buffer.from(padded, "base64").toString("utf8"));
    if (!cursor || !parseManagedLogFileName(cursor.file) || !Number.isFinite(Number(cursor.position)) || Number(cursor.position) < 0) return null;
    return { file: cursor.file, position: Math.floor(Number(cursor.position)) };
  } catch {
    return null;
  }
}

function parseLogQuery(url, format) {
  const parseTime = value => {
    if (value == null || String(value).trim() === "") return undefined;
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : undefined;
  };
  const keys = String(url.searchParams.get("key") || "").split(",").map(value => parseInt(value.trim(), 10)).filter(value => Number.isInteger(value) && value > 0).slice(0, 64);
  const exporting = format === "csv" || format === "jsonl";
  const maximum = exporting ? LOG_EXPORT_MAX_LIMIT : LOG_HISTORY_MAX_LIMIT;
  const defaultLimit = exporting ? LOG_EXPORT_MAX_LIMIT : LOG_RECENT_DEFAULT_LIMIT;
  const requestedLimit = parseInt(url.searchParams.get("limit") || String(defaultLimit), 10);
  const limit = Math.max(1, Math.min(Number.isFinite(requestedLimit) ? requestedLimit : defaultLimit, maximum));
  const mode = url.searchParams.get("mode") === "history" ? "history" : "recent";
  const cursorValue = url.searchParams.get("cursor") || "";
  return {
    mode: exporting || cursorValue || url.searchParams.get("since") || url.searchParams.get("until") || keys.length || url.searchParams.get("status") || url.searchParams.get("model") || url.searchParams.get("q") || url.searchParams.get("upstream") || url.searchParams.get("path") || url.searchParams.get("group") ? "history" : mode,
    limit,
    keys,
    status: String(url.searchParams.get("status") || "").trim().slice(0, 12),
    model: String(url.searchParams.get("model") || "").trim().slice(0, 120),
    q: String(url.searchParams.get("q") || "").trim().slice(0, 160),
    upstream: String(url.searchParams.get("upstream") || "").trim().slice(0, 160),
    path: String(url.searchParams.get("path") || "").trim().slice(0, 160),
    group: String(url.searchParams.get("group") || "").trim().toUpperCase().slice(0, 32),
    since: parseTime(url.searchParams.get("since")),
    until: parseTime(url.searchParams.get("until")),
    cursor: cursorValue ? decodeLogCursor(cursorValue) : null,
  };
}

function logEntryMatchesQuery(entry, query) {
  if (!entry || typeof entry !== "object") return false;
  const time = Number(entry.time || 0);
  if (Number.isFinite(query.since) && time < query.since) return false;
  if (Number.isFinite(query.until) && time > query.until) return false;
  if (query.keys.length && !query.keys.includes(Number(entry.idx))) return false;
  const status = Number(entry.status || 0);
  if (query.status) {
    if (/^[1-5]xx$/i.test(query.status)) {
      if (Math.floor(status / 100) !== Number(query.status[0])) return false;
    } else if (String(status) !== query.status) return false;
  }
  if (query.model) {
    const needle = query.model.toLowerCase();
    if (!String(entry.reqModel || "").toLowerCase().includes(needle) && !String(entry.overrideModel || "").toLowerCase().includes(needle)) return false;
  }
  if (query.upstream && !logUpstreamHost(entry.url).toLowerCase().includes(query.upstream.toLowerCase())) return false;
  if (query.path && !String(entry.path || "").toLowerCase().includes(query.path.toLowerCase())) return false;
  if (query.group && String(entry.group || "A").toUpperCase() !== query.group) return false;
  if (query.q) {
    const haystack = [entry.message, entry.url, entry.client, entry.reqModel, entry.overrideModel, entry.method, entry.path, entry.eventType, entry.streamId, entry.streamOutcome, entry.streamReason, entry.streamErrorMsg, entry.upstreamErrorReason, entry.terminalSource].map(value => String(value || "").toLowerCase()).join("\n");
    if (!haystack.includes(query.q.toLowerCase())) return false;
  }
  return true;
}

function summarizeLogEntries(entries) {
  const aggregate = createLogAggregate();
  for (const entry of entries) addLogEntryToAggregate(aggregate, entry);
  return logMetricsToStats(aggregate);
}

function getRecentLogEntries(query) {
  return requestLog.slice().reverse().filter(entry => logEntryMatchesQuery(entry, query)).slice(0, query.limit);
}

function logEntryFingerprint(entry) {
  try {
    return JSON.stringify(entry);
  } catch {
    return "";
  }
}

function mergeRecentLogEntries(memoryEntries, historicalEntries, limit) {
  const memory = Array.isArray(memoryEntries) ? memoryEntries : [];
  const historical = Array.isArray(historicalEntries) ? historicalEntries : [];
  const memoryCounts = new Map();
  for (const entry of memory) {
    const fingerprint = logEntryFingerprint(entry);
    if (!fingerprint) continue;
    memoryCounts.set(fingerprint, (memoryCounts.get(fingerprint) || 0) + 1);
  }
  const merged = memory.slice();
  for (const entry of historical) {
    const fingerprint = logEntryFingerprint(entry);
    const count = fingerprint ? memoryCounts.get(fingerprint) || 0 : 0;
    if (count > 0) {
      memoryCounts.set(fingerprint, count - 1);
      continue;
    }
    merged.push(entry);
  }
  return merged
    .sort((left, right) => Number(right.time || 0) - Number(left.time || 0))
    .slice(0, Math.max(1, Number(limit) || LOG_RECENT_DEFAULT_LIMIT));
}

function startHistoricalLogQuery(query, options = {}) {
  return queueLogWorker({
    kind: "query",
    logDir: LOG_DIR,
    query,
    maxLimit: options.maxLimit || LOG_HISTORY_MAX_LIMIT,
    maxScanBytes: options.maxScanBytes || LOG_QUERY_MAX_SCAN_BYTES,
  }, { timeoutMs: options.timeoutMs || LOG_QUERY_TIMEOUT_MS });
}

function escapeLogCsvCell(value) {
  let text = String(value == null ? "" : value);
  // Keep spreadsheet applications from interpreting upstream-controlled values as formulas.
  if (/^[=+\-@]/.test(text)) text = `'${text}`;
  return `"${text.replace(/"/g, '""')}"`;
}

function formatLogCsv(entries) {
  const header = ["time", "idx", "group", "client", "method", "path", "status", "inputBytes", "outputBytes", "duration", "ttfb", "reqModel", "overrideModel", "url", "type", "eventType", "message", "streamId", "streamOutcome", "streamReason", "streamSawDone", "upstreamErrorReason", "streamErrorMsg", "terminalSource"];
  const rows = entries.map(entry => [
    entry.time,
    entry.idx,
    entry.group,
    entry.client || "",
    entry.method,
    entry.path,
    entry.status || 0,
    entry.inputBytes || 0,
    entry.outputBytes || 0,
    entry.duration || 0,
    entry.ttfb == null ? "" : entry.ttfb,
    entry.reqModel,
    entry.overrideModel,
    entry.url,
    entry.type || "request",
    entry.eventType,
    entry.message,
    entry.streamId,
    entry.streamOutcome,
    entry.streamReason,
    entry.streamSawDone === true ? "true" : "false",
    entry.upstreamErrorReason || "",
    entry.streamErrorMsg || "",
    entry.terminalSource || "",
  ].map(escapeLogCsvCell).join(","));
  return `${header.join(",")}\n${rows.join("\n")}`;
}

function settleLogWorkerJob(job, error, result) {
  if (!job || job.settled) return;
  job.settled = true;
  if (job.timer) clearTimeout(job.timer);
  if (activeLogWorkerJob === job) activeLogWorkerJob = null;
  if (error) job.reject(error); else job.resolve(result);
  pumpLogWorkerQueue();
}

function pumpLogWorkerQueue() {
  if (activeLogWorkerJob) return;
  let job = null;
  while (logWorkerQueue.length && !job) {
    const candidate = logWorkerQueue.shift();
    if (!candidate.cancelled && !candidate.settled) job = candidate;
  }
  if (!job) return;
  activeLogWorkerJob = job;
  try {
    const worker = new Worker(LOG_QUERY_WORKER_FILE, {
      workerData: job.payload,
      resourceLimits: { maxOldGenerationSizeMb: 96 },
    });
    job.worker = worker;
    job.timer = setTimeout(() => {
      worker.terminate();
      settleLogWorkerJob(job, new Error("log worker query timed out"));
    }, job.timeoutMs || LOG_QUERY_TIMEOUT_MS);
    if (job.timer.unref) job.timer.unref();
    worker.once("message", message => {
      if (!message || message.ok !== true) settleLogWorkerJob(job, new Error(message && message.error ? message.error : "log worker failed"));
      else settleLogWorkerJob(job, null, message.result);
      worker.terminate();
    });
    worker.once("error", error => settleLogWorkerJob(job, error));
    worker.once("exit", code => {
      if (!job.settled && code !== 0) settleLogWorkerJob(job, new Error(`log worker exited with code ${code}`));
    });
  } catch (error) {
    settleLogWorkerJob(job, error);
  }
}

function queueLogWorker(payload, options = {}) {
  if (logWorkerQueue.length + (activeLogWorkerJob ? 1 : 0) >= LOG_QUERY_MAX_QUEUE) {
    return { promise: Promise.reject(new Error("log query queue is busy")), cancel() {} };
  }
  let job;
  const promise = new Promise((resolve, reject) => {
    job = { payload, resolve, reject, settled: false, cancelled: false, worker: null, timer: null, timeoutMs: options.timeoutMs || LOG_QUERY_TIMEOUT_MS };
    if (options.priority === "background") logWorkerQueue.push(job); else logWorkerQueue.unshift(job);
    pumpLogWorkerQueue();
  });
  return {
    promise,
    cancel() {
      if (!job || job.settled) return;
      job.cancelled = true;
      if (activeLogWorkerJob === job && job.worker) job.worker.terminate();
      settleLogWorkerJob(job, new Error("log worker job cancelled"));
    },
  };
}

function clampLogIncidentNumber(value, fallback, min, max) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.min(max, Math.max(min, parsed)) : fallback;
}

function normalizeLogIncidentConfig(value) {
  const source = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  return {
    enabled: source.enabled !== false,
    notify: source.notify === true,
    latencyEnabled: source.latencyEnabled === true,
    windowMinutes: clampLogIncidentNumber(source.windowMinutes, 5, 1, 60),
    minRequests: Math.round(clampLogIncidentNumber(source.minRequests, 8, 1, 10000)),
    errorBurst: Math.round(clampLogIncidentNumber(source.errorBurst, 5, 1, 10000)),
    errorRatePercent: clampLogIncidentNumber(source.errorRatePercent, 60, 1, 100),
    streamFailureBurst: Math.round(clampLogIncidentNumber(source.streamFailureBurst, 3, 1, 10000)),
    p95Ms: Math.round(clampLogIncidentNumber(source.p95Ms, 120000, 1000, 1800000)),
    p95TtfbMs: Math.round(clampLogIncidentNumber(source.p95TtfbMs, 20000, 100, 300000)),
    resolveAfterMinutes: clampLogIncidentNumber(source.resolveAfterMinutes, 5, 1, 120),
    defaultSnoozeMinutes: clampLogIncidentNumber(source.defaultSnoozeMinutes, 15, 1, 1440),
  };
}

function serializableLogIncident(incident) {
  return {
    id: incident.id,
    source: "log",
    rule: incident.rule,
    severity: incident.severity,
    status: incident.status,
    title: incident.title,
    scope: incident.scope,
    metrics: incident.metrics,
    startedAt: incident.startedAt,
    lastSeenAt: incident.lastSeenAt,
    updatedAt: incident.updatedAt,
    resolvedAt: incident.resolvedAt || 0,
    acknowledgedAt: incident.acknowledgedAt || 0,
    snoozedUntil: incident.snoozedUntil || 0,
    occurrences: incident.occurrences || 1,
  };
}

function listLogIncidents(limit = 50) {
  const now = Date.now();
  return Array.from(logIncidents.values())
    .filter(incident => incident && typeof incident === "object")
    .map(incident => {
      const serialized = serializableLogIncident(incident);
      if (serialized.status === "snoozed" && serialized.snoozedUntil && serialized.snoozedUntil <= now) serialized.status = "open";
      return serialized;
    })
    .sort((a, b) => {
      const activeA = a.status === "resolved" ? 0 : 1;
      const activeB = b.status === "resolved" ? 0 : 1;
      return activeB - activeA || b.updatedAt - a.updatedAt;
    })
    .slice(0, Math.max(1, Math.min(limit, 200)));
}

function scheduleLogIncidentPersist() {
  if (!logIncidentsLoaded) return;
  logIncidentDirty = true;
  if (logIncidentPersistTimer) return;
  logIncidentPersistTimer = setTimeout(() => {
    logIncidentPersistTimer = null;
    persistLogIncidents();
  }, LOG_INCIDENT_PERSIST_DELAY_MS);
  if (logIncidentPersistTimer.unref) logIncidentPersistTimer.unref();
}

async function persistLogIncidents() {
  if (logIncidentPersisting || !logIncidentDirty) return;
  logIncidentPersisting = true;
  logIncidentDirty = false;
  try {
    await fs.promises.mkdir(LOG_DIR, { recursive: true });
    const incidents = listLogIncidents(200);
    const tempFile = `${LOG_INCIDENT_STATE_FILE}.${process.pid}.tmp`;
    await fs.promises.writeFile(tempFile, JSON.stringify({ schema: 1, savedAt: Date.now(), incidents }), "utf8");
    await fs.promises.rename(tempFile, LOG_INCIDENT_STATE_FILE);
  } catch (error) {
    console.error(`[proxy] failed to persist log incidents: ${error.message}`);
  } finally {
    logIncidentPersisting = false;
    if (logIncidentDirty) scheduleLogIncidentPersist();
  }
}

function loadLogIncidents() {
  fs.promises.readFile(LOG_INCIDENT_STATE_FILE, "utf8").then(raw => {
    const saved = JSON.parse(raw);
    if (!saved || saved.schema !== 1 || !Array.isArray(saved.incidents)) return;
    for (const candidate of saved.incidents.slice(0, 200)) {
      if (!candidate || typeof candidate !== "object" || typeof candidate.id !== "string" || candidate.id.length > 300) continue;
      const incident = serializableLogIncident(candidate);
      incident.status = ["open", "acknowledged", "snoozed", "resolved"].includes(incident.status) ? incident.status : "open";
      incident.scope = incident.scope && typeof incident.scope === "object" ? { type: String(incident.scope.type || "global").slice(0, 32), value: String(incident.scope.value || "").slice(0, 160) } : { type: "global", value: "" };
      logIncidents.set(incident.id, incident);
    }
  }).catch(() => {}).finally(() => {
    logIncidentsLoaded = true;
    scheduleLogIncidentEvaluation();
  });
}

function broadcastIncidentUpdate() {
  if (!wsClients.size) return;
  const message = JSON.stringify({ type: "incidents", data: { incidents: listLogIncidents(), groupPauses: listActiveGroupPauses(), summaryRebuild: logSummaryRebuildState } });
  for (const ws of wsClients) if (ws.readyState === 1) ws.send(message);
}

function incidentFailedCount(metrics) {
  return (Number(metrics.error4xx) || 0) + (Number(metrics.error5xx) || 0) + (Number(metrics.errorTimeout) || 0) + (Number(metrics.errorStream) || 0);
}

function buildLogIncidentConditions(now = Date.now()) {
  const rules = config.logIncidents || normalizeLogIncidentConfig();
  const since = now - rules.windowMinutes * 60000;
  const overview = buildLogOverview({ since, until: now });
  const conditions = [];
  const addCondition = (rule, scope, severity, title, metrics) => {
    conditions.push({ rule, scope, severity, title, metrics, windowMinutes: rules.windowMinutes });
  };
  for (const type of ["upstream", "model", "path", "group"]) {
    for (const metrics of overview.dimensions[type] || []) {
      if (metrics.value === "(other)" || metrics.total < rules.minRequests) continue;
      const failures = incidentFailedCount(metrics);
      const failureRate = metrics.total ? failures / metrics.total * 100 : 0;
      const scope = { type, value: metrics.value };
      if (failures >= rules.errorBurst && failureRate >= rules.errorRatePercent) {
        const severity = failureRate >= 80 || failures >= rules.errorBurst * 2 ? "critical" : "warning";
        addCondition("failure_burst", scope, severity, `${type} 请求失败突增`, { ...metrics, failures, failureRate: Math.round(failureRate * 100) / 100 });
      }
      if (metrics.errorStream >= rules.streamFailureBurst) {
        addCondition("stream_failures", scope, metrics.errorStream >= rules.streamFailureBurst * 2 ? "critical" : "warning", `${type} 流终态失败增多`, { ...metrics, failures, failureRate: Math.round(failureRate * 100) / 100 });
      }
      if (rules.latencyEnabled && (metrics.p95 >= rules.p95Ms || metrics.p95Ttfb >= rules.p95TtfbMs)) {
        addCondition("latency_regression", scope, metrics.p95 >= rules.p95Ms * 2 || metrics.p95Ttfb >= rules.p95TtfbMs * 2 ? "critical" : "warning", `${type} 延迟劣化`, { ...metrics, failures, failureRate: Math.round(failureRate * 100) / 100 });
      }
    }
  }
  return conditions;
}

function createLogIncidentId(rule, scope) {
  return `log:${rule}:${scope.type}:${scope.value}`.slice(0, 300);
}

function emitLogIncidentNotification(incident) {
  if (!config.logIncidents || config.logIncidents.notify !== true) return;
  sendWebhook("log_incident", { incident: serializableLogIncident(incident) });
  broadcastNotification("log_incident", { incident: serializableLogIncident(incident) });
}

function upsertLogIncident(condition, now) {
  const id = createLogIncidentId(condition.rule, condition.scope);
  let incident = logIncidents.get(id);
  const wasResolved = !incident || incident.status === "resolved";
  let stateChanged = wasResolved;
  if (!incident) {
    incident = {
      id,
      source: "log",
      rule: condition.rule,
      severity: condition.severity,
      status: "open",
      title: condition.title,
      scope: condition.scope,
      metrics: condition.metrics,
      startedAt: now,
      lastSeenAt: now,
      updatedAt: now,
      resolvedAt: 0,
      acknowledgedAt: 0,
      snoozedUntil: 0,
      occurrences: 1,
    };
    logIncidents.set(id, incident);
  } else {
    incident.severity = condition.severity;
    incident.title = condition.title;
    incident.scope = condition.scope;
    incident.metrics = condition.metrics;
    incident.lastSeenAt = now;
    incident.updatedAt = now;
    incident.occurrences = (incident.occurrences || 0) + 1;
    if (incident.status === "resolved") {
      incident.status = "open";
      incident.startedAt = now;
      incident.resolvedAt = 0;
      incident.acknowledgedAt = 0;
    }
    if (incident.status === "snoozed" && incident.snoozedUntil <= now) {
      incident.status = "open";
      stateChanged = true;
    }
  }
  if (wasResolved) {
    addEventLog("incident_opened", 0, `日志事件：${incident.title}（${incident.scope.value || "全局"}）`, "");
    emitLogIncidentNotification(incident);
  }
  return { incident, stateChanged };
}

function scheduleLogIncidentEvaluation() {
  if (!logSummaryLoaded || !logIncidentsLoaded || !config.logIncidents || !config.logIncidents.enabled) return;
  if (logIncidentEvaluationTimer) return;
  logIncidentEvaluationTimer = setTimeout(() => {
    logIncidentEvaluationTimer = null;
    evaluateLogIncidents();
  }, LOG_INCIDENT_EVALUATION_DELAY_MS);
  if (logIncidentEvaluationTimer.unref) logIncidentEvaluationTimer.unref();
}

function evaluateLogIncidents() {
  if (!logSummaryLoaded || !logIncidentsLoaded || !config.logIncidents || !config.logIncidents.enabled) return;
  const now = Date.now();
  const seen = new Set();
  let stateChanged = false;
  for (const condition of buildLogIncidentConditions(now)) {
    const result = upsertLogIncident(condition, now);
    seen.add(result.incident.id);
    stateChanged = stateChanged || result.stateChanged;
  }
  const resolveAfter = config.logIncidents.resolveAfterMinutes * 60000;
  for (const incident of logIncidents.values()) {
    if (incident.source !== "log" || incident.status === "resolved" || seen.has(incident.id)) continue;
    if (now - (incident.lastSeenAt || 0) < resolveAfter) continue;
    incident.status = "resolved";
    incident.resolvedAt = now;
    incident.updatedAt = now;
    stateChanged = true;
    addEventLog("incident_resolved", 0, `日志事件已恢复：${incident.title}（${incident.scope.value || "全局"}）`, "");
  }
  while (logIncidents.size > 200) {
    const oldest = Array.from(logIncidents.values()).sort((a, b) => (a.updatedAt || 0) - (b.updatedAt || 0))[0];
    if (!oldest) break;
    logIncidents.delete(oldest.id);
    stateChanged = true;
  }
  if (stateChanged) {
    scheduleLogIncidentPersist();
    broadcastIncidentUpdate();
  }
}

function getGroupPause(group, now = Date.now()) {
  const normalized = String(group || "A").toUpperCase();
  const pause = groupPauses.get(normalized);
  if (!pause) return null;
  if (pause.expiresAt <= now) {
    groupPauses.delete(normalized);
    broadcastIncidentUpdate();
    return null;
  }
  return pause;
}

function isGroupPaused(group) {
  return !!getGroupPause(group);
}

function handleLogIncidentAction(body) {
  const id = String(body && body.id || "");
  const action = String(body && body.action || "");
  const incident = logIncidents.get(id);
  if (!incident) throw new Error("incident not found");
  const now = Date.now();
  if (action === "acknowledge") {
    incident.status = "acknowledged";
    incident.acknowledgedAt = now;
    incident.updatedAt = now;
    addEventLog("incident_acknowledged", 0, `已确认日志事件：${incident.title}`, "");
  } else if (action === "snooze") {
    const minutes = clampLogIncidentNumber(body.minutes, config.logIncidents.defaultSnoozeMinutes, 1, 1440);
    incident.status = "snoozed";
    incident.snoozedUntil = now + minutes * 60000;
    incident.updatedAt = now;
    addEventLog("incident_snoozed", 0, `日志事件已静默 ${minutes} 分钟：${incident.title}`, "");
  } else if (action === "pause_group") {
    const group = String(body.group || (incident.scope && incident.scope.type === "group" ? incident.scope.value : "")).toUpperCase();
    if (!group || !config.groups || !config.groups[group]) throw new Error("a valid group is required");
    const minutes = clampLogIncidentNumber(body.minutes, 5, 1, 60);
    groupPauses.set(group, { expiresAt: now + minutes * 60000, incidentId: incident.id, reason: incident.title });
    addEventLog("incident_group_paused", 0, `日志事件处置：分组 ${group} 已暂停 ${minutes} 分钟（${incident.title}）`, "");
  } else if (action === "resume_group") {
    const group = String(body.group || (incident.scope && incident.scope.type === "group" ? incident.scope.value : "")).toUpperCase();
    if (!group) throw new Error("group is required");
    groupPauses.delete(group);
    addEventLog("incident_group_resumed", 0, `日志事件处置：分组 ${group} 已恢复接入`, "");
  } else {
    throw new Error("unsupported incident action");
  }
  scheduleLogIncidentPersist();
  broadcastIncidentUpdate();
  return { incident: serializableLogIncident(incident), groupPauses: listActiveGroupPauses() };
}

function startLogSummaryRebuild() {
  if (logSummaryRebuildState.phase === "running") return { ok: false, error: "summary rebuild is already running", ...logSummaryRebuildState };
  logSummaryRebuildState = { phase: "running", startedAt: Date.now(), finishedAt: 0, error: "", rebuiltDays: 0 };
  const task = queueLogWorker({ kind: "summary", logDir: LOG_DIR, excludeDays: [getLogDateKey(Date.now())] }, { priority: "background", timeoutMs: 120000 });
  task.promise.then(result => {
    const today = getLogDateKey(Date.now());
    let rebuiltDays = 0;
    for (const [day, aggregate] of Object.entries(result.days || {})) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(day) || day === today) continue;
      logDailySummaries.set(day, normalizeLogAggregate(aggregate));
      rebuiltDays++;
    }
    pruneLogRollups();
    logSummaryRebuildState = { phase: "completed", startedAt: logSummaryRebuildState.startedAt, finishedAt: Date.now(), error: "", rebuiltDays };
    scheduleLogSummaryPersist();
    scheduleLogIncidentEvaluation();
    broadcastIncidentUpdate();
  }).catch(error => {
    logSummaryRebuildState = { phase: "failed", startedAt: logSummaryRebuildState.startedAt, finishedAt: Date.now(), error: String(error.message || error).slice(0, 240), rebuiltDays: 0 };
    broadcastIncidentUpdate();
  });
  broadcastIncidentUpdate();
  return { ok: true, ...logSummaryRebuildState };
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
  if (reset === "hourly" && idx !== undefined) {
    const ks = getKeyState(idx);
    const acct = accounts[idx];
    const act = ks.activatedAt || Date.now();
    const hours = (acct ? acct.resetHours : null) || config.defaultResetHours || 5;
    return String(Math.floor((Date.now() - act) / (hours * 3600000)));
  }
  return reset === "weekly" ? tzWeekPeriod(TZ) : tzDate(TZ);
}
function isConsecutivePeriod(prev, curr, reset) {
  if (reset === "daily") {
    const p = new Date(prev + "T00:00:00+08:00"), c = new Date(curr + "T00:00:00+08:00");
    return (c - p) === 86400000;
  }
  if (reset === "weekly" || reset === "hourly") {
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
function resolveModelPricing(model) {
  const modelName = String(model == null ? "" : model).trim();
  const matched = modelName ? (config.modelPricing || []).find(rule => rule.model === modelName) : null;
  if (matched) return matched;
  return {
    inputPer1M: Number(config.prices && config.prices.inputPer1M) || 0,
    outputPer1M: Number(config.prices && config.prices.outputPer1M) || 0,
    bytesPerToken: Number(config.bytesPerToken) || 3,
  };
}

function estimateCost(model, inputBytes, outputBytes, pricingOverride) {
  const pricing = pricingOverride || resolveModelPricing(model);
  const bpt = pricing.bytesPerToken;
  const inTokens = inputBytes / bpt;
  const outTokens = outputBytes / bpt;
  const cost = (inTokens / 1000000) * pricing.inputPer1M + (outTokens / 1000000) * pricing.outputPer1M;
  return cost;
}

// --- Task Insight (流水任务解析/提炼) ---
function taskInsightActive() {
  return config.taskInsight && config.taskInsight.enabled === true;
}

function taskInsightSignal(name) {
  return taskInsightActive() && config.taskInsight && config.taskInsight.signals && config.taskInsight.signals[name] === true;
}

function taskInsightSessionIdleMs() {
  if (taskInsightSignal("correlate")) return Math.max(1, config.autoResumeIdleMinutes || 10) * 60000;
  return TASK_INSIGHT_SESSION_IDLE_MS;
}

function taskDayKey(ts) {
  const d = new Date(ts);
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${d.getFullYear()}-${m}-${day}`;
}

function taskPersistFile(ts) {
  return path.join(TASK_DIR, taskDayKey(ts) + ".jsonl");
}

function taskInsightProjectHint() {
  if (!taskInsightSignal("correlate")) return null;
  const projects = config.autoResumeProjects || [];
  if (!projects.length) return null;
  let hit = null;
  let count = 0;
  const now = Date.now();
  let runtimeProjects = null;
  try { runtimeProjects = getAutoResumeRuntimeState().projects; } catch (e) {}
  for (let i = 0; i < projects.length; i++) {
    const proj = projects[i];
    if (!proj || (typeof proj === "object" && (proj.disabled === true || proj.enabled === false))) continue;
    let id = null;
    try { id = autoResumeProjectId(proj, i); } catch (e) { continue; }
    const st = (runtimeProjects && runtimeProjects[id]) || {};
    const at = normalizeAutoResumeTimestamp(st.lastAttemptAt);
    if (!at || (now - at) > TASK_INSIGHT_CORRELATE_WINDOW_MS) continue;
    const phase = String(st.phase || "");
    if (["exited", "terminated", "failed", "launcher_failed", "launcher_timeout"].includes(phase)) continue;
    count++;
    hit = { id, name: String(proj.name || proj.path || proj.cmd || id).slice(0, 80) };
  }
  return count === 1 ? hit : null;
}

function taskInsightBaseKey(client, group) {
  return `${group || "A"}|${client || "(未知)"}`;
}

function createTaskSession(now, client, group, hint) {
  return {
    id: "task_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2, 8),
    projectId: hint ? hint.id : null,
    projectName: hint ? hint.name : null,
    client: client || "(未知)",
    group: group || "A",
    start: now,
    end: now,
    requestCount: 0,
    successCount: 0,
    failCount: 0,
    inputTokens: 0,
    outputTokens: 0,
    inputBytes: 0,
    outputBytes: 0,
    cost: 0,
    models: [],
    tools: [],
    files: [],
    instructions: [],
    terminalReasons: [],
    status: "in_progress",
    distill: null,
    requests: [],
  };
}

function taskInsightJoin(now, client, group, hint) {
  const baseKey = taskInsightBaseKey(client, group);
  let session = taskSessions.get(baseKey);
  if (session) {
    if (hint && session.projectId && session.projectId !== hint.id) {
      finalizeTaskSession(session, now);
      session = null;
    } else if (now - session.end >= taskInsightSessionIdleMs()) {
      finalizeTaskSession(session, now);
      session = null;
    }
  }
  if (!session) {
    session = createTaskSession(now, client, group, hint);
    if (hint && !session.projectId) { session.projectId = hint.id; session.projectName = hint.name; }
    taskSessions.set(baseKey, session);
    if (taskSessions.size > TASK_INSIGHT_MAX_MEMORY_SESSIONS) finalizeOldestTaskSessions(now);
  } else {
    session.end = now;
    if (hint && !session.projectId) { session.projectId = hint.id; session.projectName = hint.name; }
  }
  session.requestCount++;
  return session;
}

function finalizeOldestTaskSessions(now) {
  const entries = Array.from(taskSessions.values())
    .filter(s => s.status === "in_progress")
    .sort((a, b) => a.end - b.end);
  const overflow = taskSessions.size - TASK_INSIGHT_MAX_MEMORY_SESSIONS;
  for (let i = 0; i < Math.min(overflow, entries.length); i++) finalizeTaskSession(entries[i], now);
}

function pushUnique(arr, value, max) {
  if (!value || arr.length >= max || arr.includes(value)) return;
  arr.push(value);
}

function taskInsightAddRequestMetrics(session, m) {
  session.end = Math.max(session.end, m.time || Date.now());
  session.inputBytes += m.inputBytes || 0;
  session.outputBytes += m.outputBytes || 0;
  const inTokens = Math.max(0, Number(m.inputTokens) || 0);
  const outTokens = Math.max(0, Number(m.outputTokens) || 0);
  session.inputTokens += inTokens;
  session.outputTokens += outTokens;
  if (m.inputTokens || m.outputTokens) {
    const pricing = resolveModelPricing(m.model || "");
    session.cost += (inTokens / 1000000) * (pricing.inputPer1M || 0) + (outTokens / 1000000) * (pricing.outputPer1M || 0);
  } else {
    session.cost += estimateCost(m.model, m.inputBytes || 0, m.outputBytes || 0, null);
  }
  if (m.success) session.successCount++; else session.failCount++;
  pushUnique(session.models, m.model, TASK_INSIGHT_MODELS_MAX);
  if (Array.isArray(m.tools)) for (const t of m.tools) pushUnique(session.tools, t, TASK_INSIGHT_TOOLS_MAX);
  if (Array.isArray(m.files)) for (const f of m.files) pushUnique(session.files, f, TASK_INSIGHT_FILES_MAX);
  if (m.terminalReason) pushUnique(session.terminalReasons, m.terminalReason, TASK_INSIGHT_REASONS_MAX);
  if (Array.isArray(session.requests) && session.requests.length < TASK_INSIGHT_REQUEST_LOG_MAX) {
    session.requests.push({ t: m.time || Date.now(), status: m.statusCode || 0, model: m.model || "", dur: m.dur || 0 });
  }
}

function finalizeTaskSession(session, now) {
  if (!session || session.status !== "in_progress") return;
  session.end = Math.max(session.end, now || Date.now());
  if (session.requestCount === 0) {
    taskSessions.delete(taskInsightBaseKey(session.client, session.group));
    return;
  }
  if (session.failCount === 0) session.status = "completed";
  else if (session.successCount === 0) session.status = "failed";
  else session.status = "partial";
  taskSessions.delete(taskInsightBaseKey(session.client, session.group));
  try {
    fs.mkdirSync(TASK_DIR, { recursive: true });
    fs.appendFileSync(taskPersistFile(session.start), JSON.stringify(session) + "\n", "utf8");
  } catch (e) {
    console.error(`[proxy] task session persist failed: ${e.message}`);
  }
}

function taskInsightSweep(now = Date.now()) {
  if (!taskInsightActive()) return;
  const idleMs = taskInsightSessionIdleMs();
  const stale = Array.from(taskSessions.values()).filter(s => s.status === "in_progress" && now - s.end >= idleMs);
  for (const session of stale) finalizeTaskSession(session, now);
}

function configureTaskInsightTimers() {
  if (taskInsightSweepTimer) { clearInterval(taskInsightSweepTimer); taskInsightSweepTimer = null; }
  if (taskDistillTimer) { clearInterval(taskDistillTimer); taskDistillTimer = null; }
  if (taskInsightActive()) {
    const sweep = () => { try { taskInsightSweep(); taskInsightPrune(); } catch (e) { console.error(`[proxy] task insight sweep error: ${e.message}`); } };
    sweep();
    taskInsightSweepTimer = setInterval(sweep, TASK_INSIGHT_SWEEP_MS);
  } else {
    const open = Array.from(taskSessions.values()).filter(s => s.status === "in_progress");
    for (const session of open) finalizeTaskSession(session, Date.now());
    taskSessions.clear();
  }
  if (taskInsightActive() && config.taskInsight.distill && config.taskInsight.distill.enabled) {
    taskDistillTimer = setInterval(() => { runTaskDistill().catch(e => console.error(`[proxy] distill interval error: ${e.message}`)); }, TASK_DISTILL_INTERVAL_MS);
    runTaskDistill().catch(e => console.error(`[proxy] distill startup error: ${e.message}`));
  }
}

function taskInsightPrune(now = Date.now()) {
  const retentionMs = Math.max(1, config.taskInsight ? config.taskInsight.retentionDays : 30) * 24 * 60 * 60 * 1000;
  const cutoff = now - retentionMs;
  try {
    if (!fs.existsSync(TASK_DIR)) return;
    for (const f of fs.readdirSync(TASK_DIR)) {
      const m = TASK_FILE_NAME_RE.exec(f);
      if (!m) continue;
      const parsed = Date.parse(m[1] + "T00:00:00.000Z");
      if (Number.isFinite(parsed) && parsed < cutoff) {
        fs.unlinkSync(path.join(TASK_DIR, f));
      }
    }
  } catch (e) {
    console.error(`[proxy] task insight prune error: ${e.message}`);
  }
}

function extractFilePaths(text) {
  const out = [];
  const seen = new Set();
  if (!text) return out;
  const src = String(text).slice(0, TASK_FILE_SCAN_PREFIX_CHARS);
  const re = /["']([A-Za-z0-9_./\\-]{1,200}\.[A-Za-z0-9]{1,12})["']/g;
  let m;
  while ((m = re.exec(src)) && out.length < TASK_INSIGHT_FILES_MAX) {
    const p = m[1];
    if (seen.has(p)) continue;
    seen.add(p);
    out.push(p);
  }
  return out;
}

function taskInsightExtractRequest(parsed) {
  const sig = { instructions: [], tools: [], files: [] };
  if (taskInsightSignal("instructions")) {
    let ins = "";
    if (typeof parsed.instructions === "string") ins = parsed.instructions;
    else if (parsed.instructions && typeof parsed.instructions === "object") ins = JSON.stringify(parsed.instructions);
    if (ins) sig.instructions = [ins.replace(/\s+/g, " ").trim().slice(0, TASK_INSIGHT_INSTRUCTION_MAX_CHARS)];
  }
  if (taskInsightSignal("tools")) {
    for (const t of (parsed.tools || [])) {
      const name = (t && (t.name || (t.function && t.function.name))) || "";
      pushUnique(sig.tools, name, TASK_INSIGHT_TOOLS_MAX);
    }
    for (const msg of (parsed.input || [])) {
      if (!msg || typeof msg !== "object") continue;
      if (msg.type === "function_call" && msg.arguments) {
        const args = typeof msg.arguments === "string" ? msg.arguments : JSON.stringify(msg.arguments || "");
        for (const p of extractFilePaths(args)) pushUnique(sig.files, p, TASK_INSIGHT_FILES_MAX);
      } else if (msg.type === "function_call_output" && msg.output) {
        const txt = typeof msg.output === "string" ? msg.output : JSON.stringify(msg.output || "");
        for (const p of extractFilePaths(txt)) pushUnique(sig.files, p, TASK_INSIGHT_FILES_MAX);
      }
    }
  }
  return sig;
}

function taskInsightPreExtract(parsed) {
  if (!parsed || typeof taskInsightActive !== "function" || !taskInsightActive()) return null;
  try { return taskInsightExtractRequest(parsed); } catch (e) { return null; }
}

// --- Task Insight stream scanning (bounded SSE usage/tool extraction) ---
function taskInsightBuildMetrics(lifecycle, transform, inputBytes, accBytes, dur, statusCode, endedNormally, resolvedModel, terminalReason) {
  const usage = transform ? transform.insightUsage : null;
  const lc = lifecycle || {};
  return {
    success: lifecycle ? (lc.terminalKind === "completed") : endedNormally,
    statusCode,
    dur,
    inputBytes,
    outputBytes: accBytes,
    inputTokens: (usage && usage.input_tokens) || lc.inputTokens || 0,
    outputTokens: (usage && usage.output_tokens) || lc.outputTokens || 0,
    tools: transform ? (transform._taskTools || []) : [],
    files: transform ? (transform._taskFiles || []) : [],
    model: resolvedModel,
    terminalReason,
  };
}

// --- Task Insight storage reads ---
function taskListAll(now = Date.now()) {
  const out = [];
  const retentionMs = Math.max(1, config.taskInsight ? config.taskInsight.retentionDays : 30) * 24 * 60 * 60 * 1000;
  const cutoff = now - retentionMs;
  try {
    if (fs.existsSync(TASK_DIR)) {
      for (const f of fs.readdirSync(TASK_DIR).sort()) {
        const m = TASK_FILE_NAME_RE.exec(f);
        if (!m) continue;
        const dayStart = Date.parse(m[1] + "T00:00:00.000Z");
        if (!Number.isFinite(dayStart) || dayStart < cutoff) continue;
        const full = path.join(TASK_DIR, f);
        const stat = fs.statSync(full);
        if (!stat.isFile() || stat.size > TASK_READ_MAX_BYTES_PER_FILE) continue;
        const data = fs.readFileSync(full, "utf8");
        for (const line of data.split("\n")) {
          const t = line.trim();
          if (!t) continue;
          try { out.push(JSON.parse(t)); } catch (e) {}
        }
      }
    }
  } catch (e) {
    console.error(`[proxy] task read error: ${e.message}`);
  }
  for (const s of taskSessions.values()) {
    if (s.requestCount > 0) out.push(s);
  }
  return out;
}

function taskInsightStatusPayload() {
  const ti = config.taskInsight || TASK_INSIGHT_DEFAULT;
  const distill = ti.distill || {};
  return {
    enabled: ti.enabled === true,
    signals: ti.signals || TASK_INSIGHT_DEFAULT.signals,
    retentionDays: ti.retentionDays,
    distill: {
      enabled: distill.enabled === true,
      engine: distill.engine,
      model: distill.model,
      baseUrl: distill.baseUrl,
      report: distill.report,
      running: taskDistillState.running,
      pending: taskDistillState.pending,
      lastRunAt: taskDistillState.lastRunAt,
      lastError: taskDistillState.lastError,
      budget: {
        day: taskDistillState.day,
        spentYuan: Number(taskDistillState.spentYuan.toFixed(4)),
        limitYuan: Number(distill.dailyBudgetYuan) || 0,
      },
    },
  };
}

// --- Task Insight distillation (阶段二) ---
function taskDistillTodayKey() {
  return taskDayKey(Date.now());
}

function taskDistillLoadBudget() {
  try {
    if (!fs.existsSync(TASK_BUDGET_FILE)) return;
    const saved = JSON.parse(fs.readFileSync(TASK_BUDGET_FILE, "utf8"));
    if (saved && saved.day === taskDistillTodayKey()) {
      taskDistillState.day = saved.day;
      taskDistillState.spentYuan = Number(saved.spentYuan) || 0;
    }
  } catch (e) {}
}

function taskDistillSaveBudget() {
  try {
    fs.mkdirSync(TASK_DIR, { recursive: true });
    fs.writeFileSync(TASK_BUDGET_FILE, JSON.stringify({ day: taskDistillState.day, spentYuan: taskDistillState.spentYuan, updatedAt: Date.now() }) + "\n", { mode: 0o600 });
  } catch (e) {
    console.error(`[proxy] distill budget persist failed: ${e.message}`);
  }
}

function taskDistillBudgetBlocked() {
  if (taskDistillState.day !== taskDistillTodayKey()) {
    taskDistillState.day = taskDistillTodayKey();
    taskDistillState.spentYuan = 0;
    taskDistillSaveBudget();
  }
  const limit = config.taskInsight && config.taskInsight.distill ? Number(config.taskInsight.distill.dailyBudgetYuan) || 0 : 0;
  return limit > 0 && taskDistillState.spentYuan >= limit;
}

function taskSessionDistillSnapshot(session) {
  const lines = [];
  lines.push(`项目: ${session.projectName || "未分类"}`);
  lines.push(`客户端: ${session.client}`);
  lines.push(`时间: ${new Date(session.start).toISOString()} ~ ${new Date(session.end).toISOString()}`);
  lines.push(`状态: ${session.status}，请求 ${session.requestCount}（成功 ${session.successCount} / 失败 ${session.failCount}）`);
  lines.push(`Token: 输入 ${session.inputTokens} / 输出 ${session.outputTokens}，估算费用 ¥${Number(session.cost).toFixed(4)}`);
  lines.push(`模型轨迹: ${(session.models || []).join(" → ") || "-"}`);
  if ((session.tools || []).length) lines.push(`工具: ${session.tools.slice(0, 15).join(", ")}`);
  if ((session.files || []).length) lines.push(`文件: ${session.files.slice(0, 15).join(", ")}`);
  if ((session.terminalReasons || []).length) lines.push(`终止原因: ${session.terminalReasons.slice(0, 10).join(", ")}`);
  if ((session.instructions || []).length) lines.push(`指令(截断): ${session.instructions[0]}`);
  return lines.join("\n").slice(0, TASK_DISTILL_MAX_INPUT_CHARS);
}

function taskDistillParseResult(text) {
  const cleaned = String(text || "").trim();
  const fenced = /```(?:json)?\s*([\s\S]*?)```/i.exec(cleaned);
  const candidate = fenced ? fenced[1] : cleaned;
  const braceStart = candidate.indexOf("{");
  const braceEnd = candidate.lastIndexOf("}");
  if (braceStart >= 0 && braceEnd > braceStart) {
    try {
      return JSON.parse(candidate.slice(braceStart, braceEnd + 1));
    } catch (e) {}
  }
  try {
    return JSON.parse(candidate);
  } catch (e) {}
  return { summary: String(text || "").slice(0, 300) };
}

function taskDistillHttp(engine, baseUrl, model, messages, timeoutMs) {
  return new Promise((resolve, reject) => {
    let url;
    try {
      url = new URL(baseUrl);
    } catch (e) {
      return reject(new Error(`蒸馏接口地址无效: ${baseUrl || "(空)"}`));
    }
    const mod = url.protocol === "http:" ? http : https;
    const body = JSON.stringify({ model, messages, stream: false });
    const options = {
      hostname: url.hostname,
      port: url.port || (url.protocol === "http:" ? 80 : 443),
      path: url.pathname.endsWith("/chat/completions") ? url.pathname : `${url.pathname.replace(/\/+$/, "")}/chat/completions`,
      method: "POST",
      headers: {
        "content-type": "application/json",
        "content-length": Buffer.byteLength(body),
        "user-agent": "codex-proxy-task-insight",
      },
    };
    const req = mod.request(options, res => {
      const chunks = [];
      res.on("data", c => chunks.push(c));
      res.on("end", () => {
        if (res.statusCode !== 200) {
          return reject(new Error(`蒸馏接口返回 ${res.statusCode}: ${Buffer.concat(chunks).toString("utf8").slice(0, 300)}`));
        }
        try {
          const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          const text = parsed && parsed.choices && parsed.choices[0] && parsed.choices[0].message && parsed.choices[0].message.content;
          const usage = (parsed && parsed.usage) || {};
          resolve({ text: String(text || ""), promptTokens: Number(usage.prompt_tokens) || 0, completionTokens: Number(usage.completion_tokens) || 0 });
        } catch (e) {
          reject(new Error(`蒸馏接口响应解析失败: ${e.message}`));
        }
      });
    });
    req.on("error", e => reject(new Error(`蒸馏接口请求失败: ${e.message}`)));
    req.setTimeout(timeoutMs, () => req.destroy(new Error("蒸馏请求超时")));
    req.write(body);
    req.end();
  });
}

function taskDistillProxyHttp(port, model, messages, timeoutMs) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ model, messages, stream: false });
    const options = {
      hostname: "127.0.0.1",
      port,
      path: "/v1/chat/completions",
      method: "POST",
      headers: {
        "content-type": "application/json",
        "content-length": Buffer.byteLength(body),
        "user-agent": "codex-proxy-task-insight",
        "x-task-insight-distill": "1",
      },
    };
    const req = http.request(options, res => {
      const chunks = [];
      res.on("data", c => chunks.push(c));
      res.on("end", () => {
        if (res.statusCode !== 200) {
          return reject(new Error(`代理蒸馏返回 ${res.statusCode}: ${Buffer.concat(chunks).toString("utf8").slice(0, 300)}`));
        }
        try {
          const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          const text = parsed && parsed.choices && parsed.choices[0] && parsed.choices[0].message && parsed.choices[0].message.content;
          const usage = (parsed && parsed.usage) || {};
          resolve({ text: String(text || ""), promptTokens: Number(usage.prompt_tokens) || 0, completionTokens: Number(usage.completion_tokens) || 0 });
        } catch (e) {
          reject(new Error(`代理蒸馏响应解析失败: ${e.message}`));
        }
      });
    });
    req.on("error", e => reject(new Error(`代理蒸馏请求失败: ${e.message}`)));
    req.setTimeout(timeoutMs, () => req.destroy(new Error("代理蒸馏超时")));
    req.write(body);
    req.end();
  });
}

function taskDistillBudgetCharge(promptTokens, completionTokens, model) {
  if (!taskDistillBudgetBlocked()) {
    const pricing = resolveModelPricing(model || "");
    const cost = (promptTokens / 1000000) * (pricing.inputPer1M || 0) + (completionTokens / 1000000) * (pricing.outputPer1M || 0);
    taskDistillState.spentYuan += Math.max(0, cost);
    taskDistillState.day = taskDistillTodayKey();
    taskDistillSaveBudget();
  }
}

async function taskDistillOne(session, distillCfg) {
  const system = "你是流水任务审计助手。根据给出的「任务信号」（不含原始 prompt 原文）输出严格 JSON：{\"summary\":\"一句话中文摘要\",\"decisions\":[\"要点\"],\"risks\":[\"风险/注意\"]}。只依据给定信息；信息不足时 risks 注明“信息不足”；不要编造。";
  const snapshot = taskSessionDistillSnapshot(session);
  const user = `请审计以下流水任务：\n${snapshot}`;
  const messages = [{ role: "system", content: system }, { role: "user", content: user }];
  let result;
  if (distillCfg.engine === "proxy") {
    const port = (config.groups && config.groups.A) || 3456;
    result = await taskDistillProxyHttp(port, distillCfg.model, messages, TASK_DISTILL_TIMEOUT_MS);
  } else {
    result = await taskDistillHttp(distillCfg.engine, distillCfg.baseUrl, distillCfg.model, messages, TASK_DISTILL_TIMEOUT_MS);
  }
  if (!result.text) throw new Error("蒸馏返回空内容");
  const parsed = taskDistillParseResult(result.text);
  return {
    parsed,
    cost: { promptTokens: result.promptTokens, completionTokens: result.completionTokens },
  };
}

async function runTaskDistill(now = Date.now()) {
  const ti = config.taskInsight;
  if (!ti || ti.enabled !== true || !ti.distill || ti.distill.enabled !== true) return { ok: false, reason: "disabled" };
  if (taskDistillState.running) return { ok: false, reason: "busy" };
  if (taskDistillBudgetBlocked()) {
    taskDistillState.lastError = "今日蒸馏预算已用尽";
    return { ok: false, reason: "budget_exhausted" };
  }
  const engine = ti.distill.engine;
  if (engine === "external" && !/^https?:\/\//.test(ti.distill.baseUrl)) return { ok: false, reason: "external_requires_baseUrl" };
  if (engine === "ollama" && !/^https?:\/\//.test(ti.distill.baseUrl)) {
    ti.distill.baseUrl = "http://127.0.0.1:11434/v1";
  }
  if (!ti.distill.model) return { ok: false, reason: "model_required" };

  const cutoff = now - taskInsightSessionIdleMs();
  const candidates = taskListAll(now)
    .filter(s => s.status !== "in_progress" && s.start >= cutoff - 24 * 60 * 60 * 1000 && s.start <= now)
    .filter(s => !s.distill)
    .sort((a, b) => b.start - a.start)
    .slice(0, TASK_DISTILL_BATCH_MAX);
  if (!candidates.length) {
    taskDistillState.lastError = "";
    return { ok: true, distilled: 0 };
  }

  taskDistillState.running = true;
  taskDistillState.pending = candidates.length;
  const results = [];
  try {
    for (const session of candidates) {
      if (taskDistillBudgetBlocked()) {
        taskDistillState.lastError = "今日蒸馏预算已用尽";
        break;
      }
      try {
        const out = await taskDistillOne(session, ti.distill);
        session.distill = {
          summary: String(out.parsed.summary || "").slice(0, 400),
          decisions: Array.isArray(out.parsed.decisions) ? out.parsed.decisions.map(s => String(s).slice(0, 200)).slice(0, 6) : [],
          risks: Array.isArray(out.parsed.risks) ? out.parsed.risks.map(s => String(s).slice(0, 200)).slice(0, 6) : [],
          at: Date.now(),
        };
        taskDistillBudgetCharge(out.cost.promptTokens, out.cost.completionTokens, ti.distill.model);
        results.push(session.id);
        persistTaskSessionUpdate(session);
      } catch (e) {
        taskDistillState.lastError = e.message;
        break;
      }
      taskDistillState.pending--;
    }
  } finally {
    taskDistillState.running = false;
    taskDistillState.pending = 0;
    taskDistillState.lastRunAt = Date.now();
  }
  return { ok: true, distilled: results.length, candidates: candidates.length };
}

function persistTaskSessionUpdate(session) {
  try {
    const full = path.join(TASK_DIR, taskDayKey(session.start) + ".jsonl");
    if (!fs.existsSync(full)) return;
    const lines = fs.readFileSync(full, "utf8").split("\n");
    for (let i = 0; i < lines.length; i++) {
      const t = lines[i].trim();
      if (!t) continue;
      try {
        const parsed = JSON.parse(t);
        if (parsed.id === session.id) {
          lines[i] = JSON.stringify(session);
          fs.writeFileSync(full, lines.join("\n"), "utf8");
          return;
        }
      } catch (e) {}
    }
  } catch (e) {
    console.error(`[proxy] task distill persist failed: ${e.message}`);
  }
}

// --- Task Insight aggregation/report ---
function taskInsightBuildReport(reportMode, now = Date.now()) {
  const sessions = taskListAll(now);
  const grouped = new Map();
  for (const s of sessions) {
    const key = s.projectName || "未分类";
    let g = grouped.get(key);
    if (!g) {
      g = { project: key, sessions: 0, requests: 0, successRequests: 0, failRequests: 0, cost: 0, inputTokens: 0, outputTokens: 0, models: [], tools: [], files: [], reasons: [], completed: 0, failed: 0, partial: 0 };
      grouped.set(key, g);
    }
    g.sessions++;
    g.requests += s.requestCount || 0;
    g.successRequests += s.successCount || 0;
    g.failRequests += s.failCount || 0;
    g.cost += s.cost || 0;
    g.inputTokens += s.inputTokens || 0;
    g.outputTokens += s.outputTokens || 0;
    if (s.status === "completed") g.completed++; else if (s.status === "failed") g.failed++; else if (s.status === "partial") g.partial++;
    for (const m of s.models || []) pushUnique(g.models, m, 12);
    for (const t of s.tools || []) pushUnique(g.tools, t, 12);
    for (const f of s.files || []) pushUnique(g.files, f, 12);
    for (const r of s.terminalReasons || []) pushUnique(g.reasons, r, 8);
  }
  const report = Array.from(grouped.values())
    .map(g => {
      const total = Math.max(1, g.requests);
      return {
        ...g,
        successRate: Math.round((g.successRequests / total) * 100),
        cost: Number(g.cost.toFixed(4)),
      };
    })
    .sort((a, b) => b.cost - a.cost || b.requests - a.requests);
  const scope = reportMode === "weekly" ? "近 7 天" : "今日";
  const total = { sessions: sessions.length, cost: Number(sessions.reduce((s, x) => s + (x.cost || 0), 0).toFixed(4)), requests: sessions.reduce((s, x) => s + (x.requestCount || 0), 0) };
  return { scope, total, projects: report };
}


// --- State ---
function loadState() {
  autoResumeStateReady = false;
  let compacted = false;
  let restoredFromBackup = false;
  try {
    state = JSON.parse(fs.readFileSync(STATE_FILE, "utf-8"));
  } catch (error) {
    try {
      state = JSON.parse(fs.readFileSync(STATE_FILE + ".bak", "utf-8"));
      restoredFromBackup = true;
      console.warn("[proxy] state.json unreadable; restored runtime state from state.json.bak");
    } catch {
      state = { keys: [], activeKey: null };
      console.warn(`[proxy] state.json unreadable; starting with empty runtime state: ${error.message}`);
    }
  }
  if (!state || typeof state !== "object" || Array.isArray(state)) {
    state = { keys: [], activeKey: null };
  }
  if (!Array.isArray(state.keys)) state.keys = [];
  // Key state is index-aligned with keys.json. Dropped trailing Keys must not leave
  // historical state behind forever after an operator removes them.
  if (accounts.length && state.keys.length > accounts.length) state.keys.length = accounts.length;
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
  compacted = compactState(Date.now(), { force: true });
  restoreAutoResumeRuntimeState();
  if (compacted || restoredFromBackup) saveState(true);
}

function pruneLegacyStatePayload() {
  if (!state || typeof state !== "object" || Array.isArray(state)) return false;
  // dailyLog belonged to an early implementation and is no longer read by the
  // proxy. Keeping it would make every state write carry dead historical data.
  if (!Object.prototype.hasOwnProperty.call(state, "dailyLog")) return false;
  delete state.dailyLog;
  return true;
}

function parseStateBucketTime(key, hourly) {
  const value = String(key || "");
  if (hourly) {
    if (!/^\d{4}-\d{2}-\d{2}-\d{2}$/.test(value)) return 0;
    const parsed = Date.parse(`${value.slice(0, 10)}T${value.slice(11, 13)}:00:00.000Z`);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return 0;
  const parsed = Date.parse(`${value}T00:00:00.000Z`);
  return Number.isFinite(parsed) ? parsed : 0;
}

function trimStateBuckets(buckets, cutoff, maxBuckets, hourly) {
  if (!buckets || typeof buckets !== "object" || Array.isArray(buckets)) return false;
  const entries = Object.keys(buckets).map(key => ({ key, time: parseStateBucketTime(key, hourly) }));
  let changed = false;
  for (const entry of entries) {
    if (!entry.time || entry.time < cutoff) {
      delete buckets[entry.key];
      changed = true;
    }
  }
  const remaining = Object.keys(buckets).map(key => ({ key, time: parseStateBucketTime(key, hourly) }))
    .sort((a, b) => a.time - b.time || a.key.localeCompare(b.key));
  const overflow = remaining.length - maxBuckets;
  if (overflow > 0) {
    for (let i = 0; i < overflow; i++) delete buckets[remaining[i].key];
    changed = true;
  }
  return changed;
}

function trimHourlyModelDimensions(models) {
  if (!models || typeof models !== "object" || Array.isArray(models)) return false;
  const keys = Object.keys(models);
  if (keys.length <= STATE_HOURLY_MODEL_LIMIT) return false;
  const ranked = keys
    .map(key => ({ key, requests: Math.max(0, Number(models[key] && models[key].requests) || 0) }))
    .sort((a, b) => b.requests - a.requests || a.key.localeCompare(b.key));
  const keep = new Set(ranked.slice(0, STATE_HOURLY_MODEL_LIMIT - 1).map(item => item.key));
  const other = models[STATE_HOURLY_OTHER_MODEL] || { requests: 0, inputBytes: 0, outputBytes: 0, totalCost: 0 };
  other.totalCost = Number.isFinite(Number(other.totalCost)) ? Number(other.totalCost) : 0;
  for (const key of keys) {
    if (keep.has(key) || key === STATE_HOURLY_OTHER_MODEL) continue;
    const value = models[key] || {};
    other.requests += Math.max(0, Number(value.requests) || 0);
    other.inputBytes += Math.max(0, Number(value.inputBytes) || 0);
    other.outputBytes += Math.max(0, Number(value.outputBytes) || 0);
    other.totalCost += Math.max(0, Number(value.totalCost) || 0);
    delete models[key];
  }
  models[STATE_HOURLY_OTHER_MODEL] = other;
  return true;
}

function trimRateWindow(ks, now = Date.now()) {
  if (!ks || !ks.rateWindow || typeof ks.rateWindow !== "object") return false;
  const requests = Array.isArray(ks.rateWindow.requests) ? ks.rateWindow.requests : [];
  const cutoff = now - RATE_WINDOW_MS;
  const recent = requests.filter(entry => entry && Number.isFinite(Number(entry.time)) && Number(entry.time) >= cutoff);
  const changed = recent.length !== requests.length || !Array.isArray(ks.rateWindow.requests);
  ks.rateWindow.requests = recent;
  if (!Number.isFinite(Number(ks.rateWindow.windowStart)) || Number(ks.rateWindow.windowStart) < cutoff) {
    ks.rateWindow.windowStart = now;
    return true;
  }
  return changed;
}

function pruneAutoResumeProjects() {
  const runtime = state && state.autoResume;
  if (!runtime || typeof runtime !== "object" || Array.isArray(runtime) ||
      !runtime.projects || typeof runtime.projects !== "object" || Array.isArray(runtime.projects)) return false;
  const configured = new Set((config.autoResumeProjects || [])
    .slice(0, 10)
    .map((project, index) => autoResumeProjectId(project, index)));
  let changed = false;
  for (const id of Object.keys(runtime.projects)) {
    if (!configured.has(id)) {
      delete runtime.projects[id];
      changed = true;
    }
  }
  return changed;
}

function compactState(now = Date.now(), options = {}) {
  if (!state || !Array.isArray(state.keys)) return false;
  const prunedLegacyPayload = pruneLegacyStatePayload();
  const prunedAutoResumeProjects = pruneAutoResumeProjects();
  if (!options.force && now - lastStateCompactionAt < STATE_COMPACTION_INTERVAL_MS && stateBucketAddsSinceCompaction < STATE_COMPACTION_BUCKET_ADD_THRESHOLD) return prunedLegacyPayload || prunedAutoResumeProjects;
  const hourlyCutoff = now - config.stateHourlyRetentionDays * 86400000;
  const dailyCutoff = now - config.stateDailyRetentionDays * 86400000;
  const hourlyMaxBuckets = Math.max(24, config.stateHourlyRetentionDays * 24 + 48);
  const dailyMaxBuckets = Math.max(1, config.stateDailyRetentionDays + 2);
  let changed = prunedLegacyPayload || prunedAutoResumeProjects;
  for (const ks of state.keys) {
    if (!ks || typeof ks !== "object") continue;
    if (config.rateLimit) {
      changed = trimRateWindow(ks, now) || changed;
    } else if (Object.prototype.hasOwnProperty.call(ks, "rateWindow")) {
      delete ks.rateWindow;
      changed = true;
    }
    const stats = ks.stats;
    if (!stats || typeof stats !== "object") continue;
    changed = trimStateBuckets(stats.hourly, hourlyCutoff, hourlyMaxBuckets, true) || changed;
    changed = trimStateBuckets(stats.daily, dailyCutoff, dailyMaxBuckets, false) || changed;
    for (const bucket of Object.values(stats.hourly || {})) {
      changed = trimHourlyModelDimensions(bucket && bucket.models) || changed;
    }
  }
  lastStateCompactionAt = now;
  stateBucketAddsSinceCompaction = 0;
  if (changed) stateStatusBucketSelectionCache = new WeakMap();
  return changed;
}

function invalidateStateStatusBucketCache() {
  stateStatusBucketSelectionCache = new WeakMap();
}

function selectRecentStateBuckets(buckets, hourly, limit) {
  if (!buckets || typeof buckets !== "object" || Array.isArray(buckets)) return {};
  const cached = stateStatusBucketSelectionCache.get(buckets);
  if (cached && cached.hourly === hourly && cached.limit === limit) return cached.value;
  const selected = Object.keys(buckets)
    .map(key => ({ key, time: parseStateBucketTime(key, hourly) }))
    .filter(entry => entry.time > 0)
    .sort((left, right) => right.time - left.time || right.key.localeCompare(left.key))
    .slice(0, limit)
    .sort((left, right) => left.time - right.time || left.key.localeCompare(right.key));
  const output = {};
  for (const entry of selected) output[entry.key] = buckets[entry.key];
  stateStatusBucketSelectionCache.set(buckets, { hourly, limit, value: output });
  return output;
}

function collectStateBucketGroups(hourly) {
  const groups = new Map();
  for (const ks of state.keys || []) {
    const stats = ks && ks.stats;
    const buckets = stats && (hourly ? stats.hourly : stats.daily);
    if (!buckets || typeof buckets !== "object" || Array.isArray(buckets)) continue;
    for (const key of Object.keys(buckets)) {
      const time = parseStateBucketTime(key, hourly);
      if (!time) continue;
      if (!groups.has(time)) groups.set(time, []);
      groups.get(time).push({ buckets, key });
    }
  }
  return Array.from(groups.entries())
    .map(([time, entries]) => ({ time, entries }))
    .sort((a, b) => a.time - b.time);
}

function trimStateToByteBudget(serialized, budgetBytes) {
  let output = serialized;
  let bytes = Buffer.byteLength(output, "utf8");
  let changed = false;
  let removed = 0;
  if (bytes <= budgetBytes) return { serialized: output, bytes, changed, removed };

  // Remove complete time buckets across Keys so the most recent aggregate trend
  // remains coherent even when an unusually large deployment reaches the cap.
  for (const hourly of [true, false]) {
    const groups = collectStateBucketGroups(hourly);
    let pendingGroups = 0;
    for (let i = 0; i < groups.length; i++) {
      for (const entry of groups[i].entries) {
        if (Object.prototype.hasOwnProperty.call(entry.buckets, entry.key)) {
          delete entry.buckets[entry.key];
          removed++;
          changed = true;
        }
      }
      pendingGroups++;
      const shouldMeasure = pendingGroups >= (hourly ? 8 : 1) || i === groups.length - 1;
      if (!shouldMeasure) continue;
      pendingGroups = 0;
      output = JSON.stringify(state, null, 2);
      bytes = Buffer.byteLength(output, "utf8");
      if (bytes <= budgetBytes) {
        if (changed) invalidateStateStatusBucketCache();
        return { serialized: output, bytes, changed, removed };
      }
    }
  }
  if (changed) invalidateStateStatusBucketCache();
  return { serialized: output, bytes, changed, removed };
}

function logStateBudgetWarning(bytes, budgetBytes, removed) {
  const now = Date.now();
  if (now - lastStateBudgetWarningAt < 5 * 60 * 1000) return;
  lastStateBudgetWarningAt = now;
  const actualMiB = (bytes / (1024 * 1024)).toFixed(1);
  const budgetMiB = (budgetBytes / (1024 * 1024)).toFixed(1);
  console.warn(`[proxy] state.json exceeded ${budgetMiB} MiB; removed ${removed} oldest statistics buckets (remaining ${actualMiB} MiB)`);
}

let _saveThrottle = 0;
let _savePendingTimer = null;
function saveState(force) {
  const now = Date.now();
  if (!force && now - _saveThrottle < 2000) return false; // at most every 2s
  if (_savePendingTimer !== null) {
    clearTimeout(_savePendingTimer);
    _savePendingTimer = null;
  }
  _saveThrottle = now;
  try {
    compactState(now);
    let serialized = JSON.stringify(state, null, 2);
    const stateBudgetBytes = config.stateMaxMiB * 1024 * 1024;
    if (Buffer.byteLength(serialized, "utf8") > stateBudgetBytes) {
      const result = trimStateToByteBudget(serialized, stateBudgetBytes);
      serialized = result.serialized;
      if (result.changed || result.bytes > stateBudgetBytes) logStateBudgetWarning(result.bytes, stateBudgetBytes, result.removed);
    }
    const tempFile = `${STATE_FILE}.${process.pid}.tmp`;
    fs.writeFileSync(tempFile, serialized, { encoding: "utf8", mode: 0o600 });
    fs.renameSync(tempFile, STATE_FILE);
    return true;
  } catch (e) {
    console.error(`[proxy] Failed to save state: ${e.message}`);
    return false;
  }
}

function scheduleStateSave() {
  if (saveState()) return;
  if (_savePendingTimer !== null) return;
  const delay = Math.max(0, 2000 - (Date.now() - _saveThrottle));
  _savePendingTimer = setTimeout(() => {
    _savePendingTimer = null;
    saveState(true);
  }, delay);
  if (_savePendingTimer.unref) _savePendingTimer.unref();
}
function backupState() {
  const tempFile = `${STATE_FILE}.bak.${process.pid}.tmp`;
  try {
    fs.copyFileSync(STATE_FILE, tempFile);
    fs.renameSync(tempFile, STATE_FILE + ".bak");
  } catch {
    try { fs.rmSync(tempFile, { force: true }); } catch {}
  }
}
setInterval(backupState, 3600000);

const persistedActivatedAt = new Set();

function persistActivatedAt(idx, val) {
  try {
    const raw = JSON.parse(fs.readFileSync(KEYS_FILE, "utf-8"));
    if (raw[idx] && !raw[idx].activatedAt) {
      raw[idx].activatedAt = val;
      fs.writeFileSync(KEYS_FILE, JSON.stringify(raw, null, 2), "utf-8");
    }
  } catch (e) { /* fail silently */ }
}

function getKeyState(idx) {
  while (state.keys.length <= idx) state.keys.push({ failCode: null, failTime: null, failPeriod: null, failCount: 0, status: "active", stats: null, lastStatus: null, lastTime: null, lastModel: null, capUntil: 0 });
  const ks = state.keys[idx];
  if (ks.status === undefined) ks.status = "active";
  if (ks.failPeriod === undefined) ks.failPeriod = null;
  if (ks.capUntil === undefined) ks.capUntil = 0;
  if (ks.failReason === undefined) ks.failReason = null;
  if (ks.activatedAt === undefined) {
    ks.activatedAt = Date.now();
    try {
      const raw = JSON.parse(fs.readFileSync(KEYS_FILE, "utf-8"));
      if (raw[idx] && raw[idx].activatedAt) {
        ks.activatedAt = raw[idx].activatedAt;
        persistedActivatedAt.add(idx);
      }
    } catch (e) { /* 静默失败 */ }
  }
  if (ks.failCount === undefined) ks.failCount = 0;
  if (config.rateLimit && !ks.rateWindow) ks.rateWindow = { requests: [], windowStart: Date.now() };
  if (ks.activatedAt !== undefined && !persistedActivatedAt.has(idx)) {
    persistActivatedAt(idx, ks.activatedAt);
    persistedActivatedAt.add(idx);
  }
  if (!ks.stats) {
    ks.stats = { totalRequests: 0, successRequests: 0, failRequests: 0, inputTokens: 0, outputTokens: 0, inputBytes: 0, outputBytes: 0, lastUsed: null, daily: {}, hourly: {}, totalDuration: 0, totalTtfb: 0, totalCost: 0 };
  } else {
    if (ks.stats.inputBytes === undefined) ks.stats.inputBytes = 0;
    if (ks.stats.outputBytes === undefined) ks.stats.outputBytes = 0;
    if (ks.stats.totalDuration === undefined) ks.stats.totalDuration = 0;
    if (ks.stats.totalTtfb === undefined) ks.stats.totalTtfb = 0;
    ks.stats.totalCost = Number.isFinite(Number(ks.stats.totalCost)) ? Number(ks.stats.totalCost) : 0;
    if (!ks.stats.hourly) ks.stats.hourly = {};
    if (!ks.stats.daily) ks.stats.daily = {};
    if (ks.stats.daily) {
      for (const d of Object.keys(ks.stats.daily)) {
        if (ks.stats.daily[d].inputBytes === undefined) ks.stats.daily[d].inputBytes = 0;
        if (ks.stats.daily[d].outputBytes === undefined) ks.stats.daily[d].outputBytes = 0;
        if (ks.stats.daily[d].totalDuration === undefined) ks.stats.daily[d].totalDuration = 0;
        if (ks.stats.daily[d].totalTtfb === undefined) ks.stats.daily[d].totalTtfb = 0;
        ks.stats.daily[d].totalCost = Number.isFinite(Number(ks.stats.daily[d].totalCost)) ? Number(ks.stats.daily[d].totalCost) : 0;
      }
    }
    if (ks.stats.hourly) {
      for (const h of Object.keys(ks.stats.hourly)) {
        const bucket = ks.stats.hourly[h];
        if (!bucket || typeof bucket !== "object" || Array.isArray(bucket)) continue;
        bucket.totalCost = Number.isFinite(Number(bucket.totalCost)) ? Number(bucket.totalCost) : 0;
        if (!bucket.models || typeof bucket.models !== "object" || Array.isArray(bucket.models)) continue;
        for (const model of Object.keys(bucket.models)) {
          const modelBucket = bucket.models[model];
          if (!modelBucket || typeof modelBucket !== "object" || Array.isArray(modelBucket)) continue;
          modelBucket.totalCost = Number.isFinite(Number(modelBucket.totalCost)) ? Number(modelBucket.totalCost) : 0;
        }
      }
    }
  }
  return ks;
}

function classifyClientApp(ua) {
  if (!ua) return "(未知)";
  const s = String(ua).slice(0, 200);
  const lower = s.toLowerCase();
  const rules = [
    ["codex", "Codex CLI"],
    ["claude", "Claude Code"],
    ["cursor", "Cursor"],
    ["chatbox", "Chatbox"],
    ["cherry studio", "Cherry Studio"],
    ["cherry", "Cherry Studio"],
    ["nextchat", "NextChat"],
    ["chatgpt-next-web", "NextChat"],
    ["lobe", "LobeChat"],
    ["openai-python", "OpenAI SDK (Python)"],
    ["openai/python", "OpenAI SDK (Python)"],
    ["openai/node", "OpenAI SDK (Node)"],
    ["openai-node", "OpenAI SDK (Node)"],
    ["openai/typescript", "OpenAI SDK (TS)"],
    ["openai-typescript", "OpenAI SDK (TS)"],
    ["openai/ts", "OpenAI SDK (TS)"],
    ["openai", "OpenAI SDK"],
    ["vercel-ai", "Vercel AI SDK"],
    ["ai sdk", "Vercel AI SDK"],
    ["python-requests", "Python 脚本"],
    ["curl", "curl"],
    ["wget", "wget"],
    ["postman", "Postman"],
    ["node", "Node.js"],
  ];
  for (const [keyword, label] of rules) {
    if (lower.includes(keyword)) return label;
  }
  return (s.split(" ")[0] || "").slice(0, 40) || "(未知)";
}

function recordRequest(idx, success, inputBytes, outputBytes, duration, ttfb, model, statusCode, client, pricingOverride) {
  const ks = getKeyState(idx);
  const s = ks.stats;
  const cost = estimateCost(model, inputBytes || 0, outputBytes || 0, pricingOverride);
  s.totalRequests++;
  if (success) s.successRequests++; else s.failRequests++;
  s.lastUsed = Date.now();
  if (inputBytes) s.inputBytes += inputBytes;
  if (outputBytes) s.outputBytes += outputBytes;
  s.totalCost = (Number.isFinite(Number(s.totalCost)) ? Number(s.totalCost) : 0) + cost;
  if (duration) {
    s.totalDuration = (s.totalDuration || 0) + duration;
    s.totalTtfb = (s.totalTtfb || 0) + (ttfb || 0);
  }
  const d = today();
  if (!s.daily[d]) {
    s.daily[d] = { requests: 0, inputTokens: 0, outputTokens: 0, inputBytes: 0, outputBytes: 0, totalDuration: 0, totalTtfb: 0, totalCost: 0 };
    stateBucketAddsSinceCompaction++;
    invalidateStateStatusBucketCache();
  }
  s.daily[d].requests++;
  if (inputBytes) s.daily[d].inputBytes += inputBytes;
  if (outputBytes) s.daily[d].outputBytes += outputBytes;
  s.daily[d].totalCost = (Number.isFinite(Number(s.daily[d].totalCost)) ? Number(s.daily[d].totalCost) : 0) + cost;
  if (duration) {
    s.daily[d].totalDuration = (s.daily[d].totalDuration || 0) + duration;
    s.daily[d].totalTtfb = (s.daily[d].totalTtfb || 0) + (ttfb || 0);
  }

  const now = new Date();
  const hk = d + "-" + String(now.getHours()).padStart(2, "0");
  if (!s.hourly[hk]) {
    s.hourly[hk] = { requests: 0, inputBytes: 0, outputBytes: 0, totalCost: 0, totalDuration: 0, totalTtfb: 0, models: {} };
    stateBucketAddsSinceCompaction++;
    invalidateStateStatusBucketCache();
  }
  s.hourly[hk].requests++;
  if (inputBytes) s.hourly[hk].inputBytes += inputBytes;
  if (outputBytes) s.hourly[hk].outputBytes += outputBytes;
  s.hourly[hk].totalCost = (Number.isFinite(Number(s.hourly[hk].totalCost)) ? Number(s.hourly[hk].totalCost) : 0) + cost;
  if (duration) s.hourly[hk].totalDuration = (s.hourly[hk].totalDuration || 0) + duration;
  if (ttfb) s.hourly[hk].totalTtfb = (s.hourly[hk].totalTtfb || 0) + ttfb;
  const _mk = String(model || "(未知)").slice(0, STATE_HOURLY_MODEL_NAME_MAX_CHARS);
  const hourlyModels = s.hourly[hk].models;
  let modelKey = _mk;
  const modelDimensionCount = Object.keys(hourlyModels).length;
  const modelNameLimit = hourlyModels[STATE_HOURLY_OTHER_MODEL] ? STATE_HOURLY_MODEL_LIMIT : STATE_HOURLY_MODEL_LIMIT - 1;
  if (!hourlyModels[modelKey] && modelDimensionCount >= modelNameLimit) modelKey = STATE_HOURLY_OTHER_MODEL;
  if (!hourlyModels[modelKey]) hourlyModels[modelKey] = { requests: 0, inputBytes: 0, outputBytes: 0, totalCost: 0 };
  hourlyModels[modelKey].totalCost = Number.isFinite(Number(hourlyModels[modelKey].totalCost)) ? Number(hourlyModels[modelKey].totalCost) : 0;
  hourlyModels[modelKey].requests++;
  if (inputBytes) hourlyModels[modelKey].inputBytes += inputBytes;
  if (outputBytes) hourlyModels[modelKey].outputBytes += outputBytes;
  hourlyModels[modelKey].totalCost += cost;
  if (statusCode !== undefined) {
    if (!s.hourly[hk].statusCodes) s.hourly[hk].statusCodes = {};
    const scKey = statusCode === 200 ? "ok" : (statusCode >= 400 && statusCode < 500 ? "4xx" : (statusCode >= 500 ? "5xx" : "fail"));
    s.hourly[hk].statusCodes[scKey] = (s.hourly[hk].statusCodes[scKey] || 0) + 1;
    if (!s.daily[d].statusCodes) s.daily[d].statusCodes = {};
    s.daily[d].statusCodes[scKey] = (s.daily[d].statusCodes[scKey] || 0) + 1;
  }
  if (client) {
    if (!s.hourly[hk].clients) s.hourly[hk].clients = {};
    s.hourly[hk].clients[client] = (s.hourly[hk].clients[client] || 0) + 1;
    if (!s.daily[d].clients) s.daily[d].clients = {};
    s.daily[d].clients[client] = (s.daily[d].clients[client] || 0) + 1;
  }

  state.activeKey = idx;
  recordSliding(idx, success, duration);
  if (config.rateLimit) trimRateWindow(ks);
  const rw = ks.rateWindow;
  if (config.rateLimit && rw) {
    const bpt = config.bytesPerToken || 3;
    const tokens = ((inputBytes || 0) + (outputBytes || 0)) / bpt;
    rw.requests.push({ time: Date.now(), tokens: Math.round(tokens) });
  }
  saveState();
  broadcastStatus();
}

function recordStreamOutcome(idx, reason, persist = false) {
  if (!reason) return;
  const s = getKeyState(idx).stats;
  const now = new Date();
  const hk = today() + "-" + String(now.getHours()).padStart(2, "0");
  if (!s.hourly[hk]) {
    s.hourly[hk] = { requests: 0, inputBytes: 0, outputBytes: 0, totalCost: 0, totalDuration: 0, totalTtfb: 0, models: {} };
    stateBucketAddsSinceCompaction++;
    invalidateStateStatusBucketCache();
  }
  if (!s.hourly[hk].streamOutcomes) s.hourly[hk].streamOutcomes = {};
  s.hourly[hk].streamOutcomes[reason] = (s.hourly[hk].streamOutcomes[reason] || 0) + 1;
  if (persist) {
    if (reason === "upstream_done" || reason === "client_disconnect") scheduleStateSave();
    else saveState(true);
    broadcastStatus();
  }
}

function markSuccess(idx) {
  const ks = getKeyState(idx);
  ks.failCode = null;
  ks.failTime = null;
  ks.failCount = 0;
  ks.capUntil = 0;
  ks.failReason = null;
  allFailedNotified = false;
  saveState();
  processQueue();
  broadcastStatus();
}

// Transient capacity pressure (HTTP 429 / model_at_capacity) backs the key off
// for capacityBackoffSeconds WITHOUT recording a hard failure. Unlike
// markFailure it never sets failCode, so reset:"never" keys stay permanently
// usable after the backoff window instead of being cooled down for a whole
// period, and one upstream capacity spike cannot cascade-disable every key.
function markCapacityBackoff(idx) {
  const ks = getKeyState(idx);
  const seconds = Math.max(1, Number(config.capacityBackoffSeconds)) || 60;
  ks.capUntil = Date.now() + seconds * 1000;
  saveState();
  broadcastStatus();
  setTimeout(() => { try { processQueue(); } catch (e) {} }, seconds * 1000 + 100).unref();
}

// A stream can begin with HTTP 200 and later carry an explicit capacity error
// in its SSE terminal event. Treat that the same as a pre-header 429: it is
// transient upstream pressure, not evidence that this Key is invalid.
function markStreamTerminalFailure(idx, lifecycle, clientCancelled) {
  if (clientCancelled) return;
  if (lifecycle && lifecycle.terminalReason === "model_at_capacity") {
    markCapacityBackoff(idx);
    return;
  }
  markFailure(idx, 0);
}

function markFailure(idx, code, reason) {
  const ks = getKeyState(idx);
  const acct = accounts[idx];
  const curr = keyPeriod(acct.reset, idx);

  if (acct.reset !== "never") {
    if (ks.failPeriod && ks.failPeriod !== curr && isConsecutivePeriod(ks.failPeriod, curr, acct.reset)) {
      ks.status = "discarded";
      console.log(`[proxy] #${idx+1} DISCARDED (consecutive ${acct.reset} failure: ${ks.failPeriod} → ${curr})`);
      addEventLog("discard", idx + 1, `连续 ${acct.reset} 周期失败: ${ks.failPeriod} → ${curr}`, acct.url);
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
        addEventLog("lock", idx + 1, `${ks.failCount} 次连续 ${code} 失败自动锁定`, acct.url);
      }
    } else if (ks.failCount) {
      ks.failCount = 0;
    }
  }

  ks.failCode = code;
  if (reason) ks.failReason = reason;
  if (config.autoRecoverPoll && !autoRecoverPollTimer &&
      (config.autoRecoverPollCodes || []).includes(code)) {
    schedulePollRecover();
  }
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
    const ks = getKeyState(i);
    // A key that is only transiently backed off (capUntil, no failCode) is not
    // a hard failure and must not trigger all_keys_failed webhooks.
    if (!ks.failCode) return false;
    if (!inCooldown(i)) return false;
  }
  return true;
}

function inCooldown(idx) {
  const acct = accounts[idx];
  const ks = getKeyState(idx);
  if (ks.status === "discarded" || ks.status === "locked") return true;
  if (ks.capUntil && ks.capUntil > Date.now()) return true;
  if (acct.reset === "never") return !!ks.failCode;
  if (!ks.failCode || !ks.failPeriod) return false;
  const curr = keyPeriod(acct.reset, idx);
  return ks.failPeriod === curr;
}

function parseTz(tz) {
  if (!tz) return 0;
  const m = String(tz).match(/^([+-]?)(\d+(?:\.\d+)?)$/);
  if (!m) return 0;
  return (m[1] === "-" ? -1 : 1) * parseFloat(m[2]);
}
function isInTimeWindow(key) {
  if (!key || !key.timeWindow) return true;
  const { start, end } = key.timeWindow;
  if (start == null || end == null) return true;
  if (start === end) return true;
  const offset = parseTz(key.tz);
  const now = new Date();
  const localHour = (now.getUTCHours() + offset + 24) % 24;
  if (start < end) return localHour >= start && localHour < end;
  return localHour >= start || localHour < end;
}
function timeWindowCountdown(key) {
  if (!key || !key.timeWindow) return null;
  const { start, end } = key.timeWindow;
  if (start == null || end == null) return null;
  if (start === end) return null;
  const offset = parseTz(key.tz);
  const now = new Date();
  const localHour = now.getUTCHours();
  const localMin = now.getUTCMinutes();
  const localSec = now.getUTCSeconds();
  const currentMinutes = localHour * 60 + localMin + localSec / 60;
  const offsetMinutes = offset * 60;
  const localMinutes = ((currentMinutes + offsetMinutes) % 1440 + 1440) % 1440;
  const startMinutes = start * 60;
  const endMinutes = end * 60;
  if (start < end) {
    if (localMinutes >= startMinutes && localMinutes < endMinutes) {
      return { inWindow: true, remaining: Math.floor(endMinutes - localMinutes) };
    }
    const wait = localMinutes < startMinutes ? startMinutes - localMinutes : 1440 - localMinutes + startMinutes;
    return { inWindow: false, remaining: wait };
  } else {
    if (localMinutes >= startMinutes || localMinutes < endMinutes) {
      const remaining = localMinutes >= startMinutes ? (1440 - localMinutes + endMinutes) : (endMinutes - localMinutes);
      return { inWindow: true, remaining: Math.floor(remaining) };
    }
    return { inWindow: false, remaining: Math.floor(startMinutes - localMinutes) };
  }
}

function daysUntilReset(resetDay) {
  if (resetDay == null) return 99;
  const jsDay = new Date().getDay();
  const isoDay = jsDay === 0 ? 7 : jsDay;
  const target = parseInt(resetDay);
  return (target - isoDay + 7) % 7 || 7;
}
function rateLimitAllow(idx) {
  if (!config.rateLimit) return true;
  const ks = getKeyState(idx);
  if (!ks.rateWindow) ks.rateWindow = { requests: [], windowStart: Date.now() };
  const rw = ks.rateWindow;
  const now = Date.now();
  trimRateWindow(ks, now);
  const recent = rw.requests;
  const acct = accounts[idx];
  const maxReqs = (acct && acct.maxReqPerMin) || config.maxRequestsPerMin;
  const maxToks = (acct && acct.maxTokPerMin) || config.maxTokensPerMin;
  if (recent.length >= maxReqs) return false;
  if (maxToks > 0) {
    const tokens = recent.reduce((s, e) => s + (e.tokens || 0), 0);
    if (tokens >= maxToks) return false;
  }
  return true;
}
function pickKey(model, group) {
  group = group || "A";
  function matchesModel(a) {
    if (!model) return true;
    if (!a.models || !a.models.length) return true;
    return a.models.includes(model);
  }
  function matchesGroup(a) {
    return (a.group || "A") === group;
  }
  // Boost: 持续高优
  if (_boostKey >= 0 && _boostKey < accounts.length) {
    if (matchesModel(accounts[_boostKey]) && matchesGroup(accounts[_boostKey]) && accounts[_boostKey].status === "active" && !inCooldown(_boostKey) && rateLimitAllow(_boostKey) && getKeyState(_boostKey).status !== "discarded" && isInTimeWindow(accounts[_boostKey])) {
      return _boostKey;
    }
    // boosted key no longer available, auto-clear
    _boostKey = -1;
    broadcastStatus();
  }
  // Batch Boost
  if (_boostBatch.length && _boostBatchMode === "use") {
    for (const idx of _boostBatch) {
      if (idx >= 0 && idx < accounts.length && matchesModel(accounts[idx]) && matchesGroup(accounts[idx]) && accounts[idx].status === "active" && !inCooldown(idx) && rateLimitAllow(idx) && getKeyState(idx).status !== "discarded" && isInTimeWindow(accounts[idx])) return idx;
    }
  }
  if (_boostBatch.length && _boostBatchMode === "roundrobin") {
    if (_boostBatchCursor >= _boostBatch.length) _boostBatchCursor = 0;
    for (let i = 0; i < _boostBatch.length; i++) {
      const bi = (_boostBatchCursor + i) % _boostBatch.length;
      const idx = _boostBatch[bi];
      if (idx >= 0 && idx < accounts.length && matchesModel(accounts[idx]) && matchesGroup(accounts[idx]) && accounts[idx].status === "active" && !inCooldown(idx) && rateLimitAllow(idx) && getKeyState(idx).status !== "discarded" && isInTimeWindow(accounts[idx])) {
        _boostBatchCursor = (bi + 1) % _boostBatch.length;
        return idx;
      }
    }
  }
  if (_boostBatch.length && _boostBatchMode === "random") {
    if (!_boostBatchPool.length || _boostBatchPoolIdx >= _boostBatchPool.length) {
      _boostBatchPool = [..._boostBatch];
      for (let i = _boostBatchPool.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [_boostBatchPool[i], _boostBatchPool[j]] = [_boostBatchPool[j], _boostBatchPool[i]];
      }
      _boostBatchPoolIdx = 0;
    }
    for (let i = _boostBatchPoolIdx; i < _boostBatchPool.length; i++) {
      const idx = _boostBatchPool[i];
      if (idx >= 0 && idx < accounts.length && matchesModel(accounts[idx]) && matchesGroup(accounts[idx]) && accounts[idx].status === "active" && !inCooldown(idx) && rateLimitAllow(idx) && getKeyState(idx).status !== "discarded" && isInTimeWindow(accounts[idx])) {
        _boostBatchPoolIdx = i + 1;
        return idx;
      }
    }
    _boostBatchPoolIdx = _boostBatchPool.length;
  }

  if (config.roundRobin) {
    const groups = [[], [], []];
    for (let i = 0; i < accounts.length; i++) {
      if (accounts[i].status !== "active") continue;
      if (!matchesModel(accounts[i])) continue;
      if (!matchesGroup(accounts[i])) continue;
      const ks = getKeyState(i);
      if (ks.status === "discarded" || ks.status === "locked") continue;
      if (!isInTimeWindow(accounts[i])) continue;
      groups[PRIORITY[accounts[i].reset] ?? 0].push(i);
    }
    for (const g of groups) g.sort((a, b) => {
      if (config.weeklySortBy === "expiry" && accounts[a].reset === "weekly" && accounts[b].reset === "weekly") {
        return daysUntilReset(accounts[a].resetDay) - daysUntilReset(accounts[b].resetDay);
      }
      return (accounts[b].priority || 0) - (accounts[a].priority || 0) || a - b;
    });
    for (let gi = 0; gi <= 2; gi++) {
      const g = groups[gi];
      if (!g.length) continue;
      if (gi === 1) {
        const sub = {};
        for (const idx of g) {
          const d = accounts[idx].resetDay != null ? String(accounts[idx].resetDay) : 'auto';
          if (!sub[d]) sub[d] = {};
          const p = accounts[idx].priority || 0;
          if (!sub[d][p]) sub[d][p] = [];
          sub[d][p].push(idx);
        }
        const days = Object.keys(sub).sort((a, b) => {
          if (a === 'auto') return 1;
          if (b === 'auto') return -1;
          return daysUntilReset(parseInt(a)) - daysUntilReset(parseInt(b));
        });
        let startIdx = 0;
        if (_weeklyLastDay) {
          const li = days.indexOf(_weeklyLastDay);
          if (li >= 0) startIdx = li;
        }
        for (let s = 0; s < days.length; s++) {
          const di = (startIdx + s) % days.length;
          const prioGroups = sub[days[di]];
          const prios = Object.keys(prioGroups).map(Number).sort((a, b) => b - a);
          for (const p of prios) {
            const pool = prioGroups[p];
            const avail = pool.filter(i => !inCooldown(i) && rateLimitAllow(i));
            if (!avail.length) continue;
            const ck = days[di] + ':' + p;
            if (!_weeklySubCursors[ck]) _weeklySubCursors[ck] = 0;
            if (_weeklySubCursors[ck] >= avail.length) _weeklySubCursors[ck] = 0;
            _weeklyLastDay = days[di];
            return avail[_weeklySubCursors[ck]++];
          }
        }
        continue;
      }
      const avail = g.filter(i => !inCooldown(i) && rateLimitAllow(i));
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
    if (!matchesGroup(accounts[i])) continue;
    const ks = getKeyState(i);
    if (ks.status !== "discarded" && ks.status !== "locked" && isInTimeWindow(accounts[i])) groups[PRIORITY[accounts[i].reset] ?? 0].push(i);
  }
  for (const g of groups) g.sort((a, b) => {
    if (config.weeklySortBy === "expiry" && accounts[a].reset === "weekly" && accounts[b].reset === "weekly") {
      return daysUntilReset(accounts[a].resetDay) - daysUntilReset(accounts[b].resetDay);
    }
    return (accounts[b].priority || 0) - (accounts[a].priority || 0) || a - b;
  });
  for (const g of groups) {
    const a = g.filter(i => !inCooldown(i) && rateLimitAllow(i));
    if (a.length) return a[0];
  }
  for (const g of groups) if (g.length) return g[0];
  return -1;
}

// --- Request Queue ---
function enqueueRequest(method, headers, body, clientRes, pathname, group, extraTransform, failureContext, capacity) {
  if (restartState.phase !== "ready") {
    if (!clientRes.destroyed && !clientRes.headersSent) {
      clientRes.writeHead(503, { "content-type": "application/json", "retry-after": "5" });
      clientRes.end(JSON.stringify({ error: "proxy is restarting" }));
    }
    return;
  }
  group = group || "A";
  requestQueue.push({ method, headers, body, clientRes, pathname, group, time: Date.now(), extraTransform, failureContext: failureContext || null, capacity: capacity === true });
  console.log(`[proxy] Queue depth: ${requestQueue.length}`);
  // Drain promptly so queued requests reach the 503 max-wait path instead of
  // lingering until the next backoff timer or an hourly sweep.
  setImmediate(() => { try { processQueue(); } catch (e) {} });
  clientRes.on("close", () => {
    const i = requestQueue.findIndex(r => r.clientRes === clientRes);
    if (i >= 0) { requestQueue.splice(i, 1); if (!clientRes.destroyed) clientRes.destroy(); }
  });
}

function processQueue() {
  if (restartState.phase !== "ready") {
    rejectQueuedRequestsForRestart();
    return;
  }
  if (queueProcessing) return;
  queueProcessing = true;
    const now = Date.now();
    const batch = [...requestQueue];
    requestQueue = [];
    for (const r of batch) {
      if (restartState.phase !== "ready") {
        if (!r.clientRes.destroyed && !r.clientRes.headersSent) {
          r.clientRes.writeHead(503, { "content-type": "application/json", "retry-after": "5" });
          r.clientRes.end(JSON.stringify({ error: "proxy is restarting" }));
        }
        continue;
      }
      // Requests waiting on transient capacity pressure get a much longer queue
      // window than the normal 30s timeout; otherwise a single upstream spike
      // would 503 every request in the queue.
      const maxWait = r.capacity ? Math.max(1, Number(config.capacityMaxWaitSeconds)) * 1000 : QUEUE_TIMEOUT;
      if (now - r.time > maxWait) {
      if (!r.clientRes.destroyed && !r.clientRes.headersSent) {
        let timedOutModel = null;
        try { timedOutModel = JSON.parse(r.body.toString()).model || null; } catch (e) {}
        if (r.failureContext && Number.isInteger(r.failureContext.idx)) {
          addDownstreamTerminalLog(r.failureContext.idx, {
            reason: r.failureContext.reason || "upstream_idle_timeout",
            errorMessage: r.failureContext.errorMessage || "Request timed out while waiting for an available Key",
            status: 503,
            group: r.group,
            method: r.method,
            path: r.pathname,
            reqModel: timedOutModel,
            source: "queue_timeout",
          });
        }
        r.clientRes.writeHead(503, { "content-type": "application/json" });
        r.clientRes.end(JSON.stringify({ error: "request timeout in queue" }));
      }
      continue;
    }
    let rmodel = null;
    let rforceIdx = -1;
    try {
      const parsed = JSON.parse(r.body.toString());
      rmodel = parsed.model || null;
      if (rmodel) {
        const hm = rmodel.match(/#(\d+)$/);
        if (hm) {
          rforceIdx = parseInt(hm[1], 10) - 1;
          rmodel = rmodel.slice(0, -hm[0].length) || null;
        }
      }
    } catch(e) {}
    if (rforceIdx >= 0) {
      if (rforceIdx >= accounts.length) {
        if (!r.clientRes.destroyed && !r.clientRes.headersSent) {
          r.clientRes.writeHead(400, { "content-type": "application/json" });
          r.clientRes.end(JSON.stringify({ error: `Key #${rforceIdx+1} does not exist` }));
        }
        continue;
      }
      forwardRequest(rforceIdx, r.method, r.headers, r.body, r.clientRes, r.pathname, (result) => {
        if (result.switched) {
          if (!r.clientRes.destroyed && !r.clientRes.headersSent) {
            addDownstreamTerminalLog(result.idx, {
              reason: result.reason || "upstream_api_error",
              errorMessage: result.errorMessage || "",
              status: 502,
              group: r.group,
              method: r.method,
              path: r.pathname,
              reqModel: rmodel,
              source: result.source || "upstream_failure",
            });
            r.clientRes.writeHead(502, { "content-type": "application/json" });
            r.clientRes.end(JSON.stringify({ error: `Key #${rforceIdx+1} failed` }));
          }
        }
      }, r.extraTransform, rmodel);
      continue;
    }
    const idx = pickKey(rmodel, r.group);
    if (idx < 0 || inCooldown(idx)) {
      requestQueue.push(r);
      continue;
    }
    forwardRequest(idx, r.method, r.headers, r.body, r.clientRes, r.pathname, (result) => {
      if (result.switched) {
        r.failureContext = result;
        if (result.capacityRetry) r.capacity = true;
        requestQueue.push(r);
      }
    }, r.extraTransform);
  }
  queueProcessing = false;
}

function loadAccounts() {
  _boostKey = -1; _boostBatch=[]; _boostBatchMode=""; _boostBatchCursor=0; _boostBatchPool=[]; _boostBatchPoolIdx=0; // clear boost on key reload
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
    resetHours: a.resetHours > 0 ? a.resetHours : null,
    maxReqPerMin: a.maxReqPerMin > 0 ? a.maxReqPerMin : null,
    maxTokPerMin: a.maxTokPerMin > 0 ? a.maxTokPerMin : null,
    group: (a.group || "A").toUpperCase(),
    tz: a.tz || undefined,
    timeWindow: a.timeWindow || undefined,
  }));
  if (!accounts.length) { console.error("[proxy] No valid accounts, reverting"); accounts = oldAccounts; return; }
  loadState();

  const groups = [[], [], []];
  for (let i = 0; i < accounts.length; i++) groups[PRIORITY[accounts[i].reset] ?? 0].push(i);
  const resetLabels = { daily: "每日重置", weekly: "每周重置", never: "永不过期", hourly: "每N小时重置" };
  console.log(`[proxy] Loaded ${accounts.length} accounts`);
  for (let gi = 0; gi < groups.length; gi++) {
    for (const i of groups[gi]) {
      const a = accounts[i], m = a.key.match(/^(sk-[^-]+)/);
      const ks = getKeyState(i), tag = a.remark ? ` (${a.remark})` : "";
      const disc = ks.status === "discarded" ? "废弃" : "";
      const user = a.status !== "active" ? a.status : "";
      const st = user ? `✗ ${user}` : (disc ? `✗ ${disc}` : (inCooldown(i) ? `✗ 冷却中 (${ks.failCode || (ks.capUntil && ks.capUntil > Date.now() ? "容量退避" : "")})` : "✓ 可用"));
      const s = ks.stats;
      const t = s ? ` | ${s.totalRequests}次请求 ${fmtBytes(s.inputBytes+s.outputBytes)}` : "";
      console.log(`       ${i+1}. ${m ? m[1] : a.key.slice(0,12)}... → ${a.url} [${resetLabels[a.reset] || a.reset}]${tag} ${st}${t}`);
    }
  }
  broadcastStatus();
}

function makeUsageTransform(idx, inputBytes, reqStart, ttfb, model) {
  let outputBytes = 0;
  const scanUsage = taskInsightSignal("usage");
  const scanTools = taskInsightSignal("tools");
  let scanBuf = "";
  let scanBytes = 0;
  const taskTools = [];
  const taskFiles = [];
  let taskUsage = null;
  const scanLine = (line) => {
    if (!line || !line.startsWith("data:")) return;
    if (line.length > TASK_SSE_LINE_MAX) return;
    const data = line.slice(5).trim();
    if (!data || data === "[DONE]") return;
    try {
      const parsed = JSON.parse(data);
      if (scanUsage) {
        const u = parsed.usage || (parsed.response && parsed.response.usage) || (parsed.message && parsed.message.usage);
        if (u && (u.input_tokens != null || u.prompt_tokens != null || u.output_tokens != null || u.completion_tokens != null)) {
          taskUsage = {
            input_tokens: u.input_tokens != null ? u.input_tokens : (u.prompt_tokens || 0),
            output_tokens: u.output_tokens != null ? u.output_tokens : (u.completion_tokens || 0),
          };
        }
      }
      if (scanTools) {
        if (parsed.type === "response.output_item.done" && parsed.item && parsed.item.type === "function_call" && parsed.item.name) {
          const name = String(parsed.item.name);
          pushUnique(taskTools, name, TASK_INSIGHT_TOOLS_MAX);
          if (typeof parsed.item.arguments === "string") {
            for (const p of extractFilePaths(parsed.item.arguments)) pushUnique(taskFiles, p, TASK_INSIGHT_FILES_MAX);
          }
        }
        if (parsed.type === "content_block_start" && parsed.content_block && parsed.content_block.type === "tool_use" && parsed.content_block.name) {
          const name = String(parsed.content_block.name);
          pushUnique(taskTools, name, TASK_INSIGHT_TOOLS_MAX);
          try {
            for (const p of extractFilePaths(JSON.stringify(parsed.content_block.input || {}))) pushUnique(taskFiles, p, TASK_INSIGHT_FILES_MAX);
          } catch (e) {}
        }
        const delta = parsed.choices && parsed.choices[0] && parsed.choices[0].delta;
        if (delta && Array.isArray(delta.tool_calls)) {
          for (const tc of delta.tool_calls) {
            if (!tc || !tc.function) continue;
            if (tc.function.name) pushUnique(taskTools, String(tc.function.name), TASK_INSIGHT_TOOLS_MAX);
            if (typeof tc.function.arguments === "string") {
              for (const p of extractFilePaths(tc.function.arguments)) pushUnique(taskFiles, p, TASK_INSIGHT_FILES_MAX);
            }
          }
        }
      }
    } catch (e) {}
  };
  const tr = new Transform({
    transform(chunk, encoding, cb) {
      outputBytes += chunk.length;
      tr.accBytes = outputBytes;
      tr.lastActivity = Date.now();
      if (scanUsage || scanTools) {
        scanBuf += chunk.toString();
        scanBytes += chunk.length;
        if (scanBytes > TASK_SSE_SCAN_BUFFER_MAX) {
          scanBuf = "";
          scanBytes = 0;
        }
        let nl;
        while ((nl = scanBuf.indexOf("\n")) >= 0) {
          const line = scanBuf.slice(0, nl);
          scanBuf = scanBuf.slice(nl + 1);
          scanLine(line);
        }
      }
      this.push(chunk);
      cb();
    },
    flush(cb) {
      if (scanBuf && (scanUsage || scanTools)) scanLine(scanBuf);
      cb();
    }
  });
  tr.accBytes = 0;
  tr.lastActivity = Date.now();
  tr._taskTools = taskTools;
  tr._taskFiles = taskFiles;
  Object.defineProperty(tr, "insightUsage", {
    get: () => taskUsage,
    configurable: true,
  });
  return tr;
}

function activeDecr(idx) {
  if (Array.isArray(activeRequests[idx])) {
    activeRequests[idx].shift();
    if (activeRequests[idx].length === 0) delete activeRequests[idx];
  }
}

function forwardRequest(idx, method, headers, body, clientRes, pathname, onDone, extraTransform, cleanModel, taskInsightSink) {
  const lifecycle = extraTransform?._lifecycle || null;
  // Keep the legacy Responses-named settings, but use the long-running coding
  // stream policy for both Responses (Codex) and Messages (Claude Code).
  const codingProtocolStream = lifecycle && (lifecycle.protocol === "responses" || lifecycle.protocol === "messages");
  const streamLifetime = codingProtocolStream ? RESPONSES_STREAM_LIFETIME : STREAM_LIFETIME;
  const upstreamIdleTimeout = codingProtocolStream ? RESPONSES_IDLE_TIMEOUT : TIMEOUT;
  let streamAttached = false;
  let clientCancelled = false;
  let terminateAttachedStream = null;
  if (!Array.isArray(activeRequests[idx])) activeRequests[idx] = [];
  let reqModel = null;
  try { reqModel = JSON.parse(body.toString()).model || null; } catch(e) {}
  const acct = accounts[idx];
  const resolvedModel = acct.model || cleanModel || reqModel || null;
  // Freeze the selected price rule at request start. A config save while a
  // long stream is in flight must apply only to later requests.
  const requestPricing = { ...resolveModelPricing(resolvedModel) };
  activeRequests[idx].push({ start: Date.now(), model: resolvedModel || "?" });
  const reqStart = Date.now();
  let ttfb = null;
  const client = classifyClientApp(headers["user-agent"]);

  const targetUrl = new URL(acct.url);
  const mod = HTTP_MOD[targetUrl.protocol] || https;

  const reqHeaders = { ...headers };
  delete reqHeaders.host;
  delete reqHeaders["content-length"];
  delete reqHeaders.connection;
  // Every transform parses or emits plain SSE. Do not let a downstream client's
  // compression preference turn a byte-preserving terminal guard into a mixed
  // compressed/plain response body.
  if (extraTransform) reqHeaders["accept-encoding"] = "identity";
  reqHeaders["authorization"] = `Bearer ${acct.key}`;

  const options = {
    hostname: targetUrl.hostname,
    port: targetUrl.port || (targetUrl.protocol === "http:" ? 80 : 443),
    path: targetUrl.pathname.replace(/\/+$/, "") + pathname,
    method,
    headers: reqHeaders,
    timeout: upstreamIdleTimeout,
  };

  const logEntry = { time: Date.now(), idx: idx + 1, group: (acct.group || "A").toUpperCase(), method, path: pathname, url: acct.url, client };
  if (lifecycle) logEntry.streamId = lifecycle.responseId;
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
      const statusCode = apiRes.statusCode;
      captureUpstreamErrorMessage(apiRes, errorMessage => {
        const dur = Date.now() - reqStart;
        const reason = errorMessage
          ? classifyUpstreamErrorMessage(errorMessage)
          : (statusCode === 402 ? "insufficient_quota" : "upstream_api_error");
        // Quota-exhausted 429s (usage limits, billing) will not clear within a
        // few seconds: back the key off for its whole period instead of churning
        // every key through capacity retries. Message-classified capacity errors
        // remain transient and use the short capacity backoff.
        const isCapacity = (statusCode === 429 && reason !== "insufficient_quota") || reason === "model_at_capacity";
        activeDecr(idx);
        if (isCapacity) {
          markCapacityBackoff(idx);
        } else {
          markFailure(idx, statusCode, reason);
        }
        recordRequest(idx, false, 0, 0, dur, null, resolvedModel, statusCode, client, requestPricing);
        recordPath(pathname, method, 0, 0, dur);
        Object.assign(logEntry, {
          status: statusCode,
          inputBytes: body ? body.length : 0,
          outputBytes: 0,
          duration: dur,
          ttfb,
          upstreamErrorReason: reason,
          streamErrorMsg: errorMessage,
          terminalSource: "upstream_http_error",
        });
        addLog(logEntry);
        const _ks1=getKeyState(idx);_ks1.lastStatus=statusCode;_ks1.lastTime=Date.now();_ks1.lastModel=logEntry.overrideModel||logEntry.reqModel||null;
        onDone({ switched: true, idx, code: statusCode, reason, errorMessage, source: "upstream_http_error", capacityRetry: isCapacity });
      });
      return;
    }

    const contentEncoding = String(apiRes.headers["content-encoding"] || "").trim().toLowerCase();
    if (extraTransform && contentEncoding && contentEncoding !== "identity") {
      if (onDone.done) { apiRes.destroy(); return; }
      onDone.done = true;
      const dur = Date.now() - reqStart;
      const errorMessage = "Upstream returned a compressed stream despite Accept-Encoding: identity";
      apiRes.destroy();
      activeDecr(idx);
      markFailure(idx, 0);
      recordRequest(idx, false, 0, 0, dur, ttfb, resolvedModel, 0, client, requestPricing);
      recordPath(pathname, method, 0, 0, dur);
      Object.assign(logEntry, {
        status: 0,
        inputBytes: body ? body.length : 0,
        outputBytes: 0,
        duration: dur,
        ttfb,
        upstreamErrorReason: "upstream_api_error",
        streamErrorMsg: errorMessage,
        terminalSource: "upstream_content_encoding",
      });
      addLog(logEntry);
      const _ksEncoding=getKeyState(idx);_ksEncoding.lastStatus=0;_ksEncoding.lastTime=Date.now();_ksEncoding.lastModel=logEntry.overrideModel||logEntry.reqModel||null;
      onDone({ switched: true, idx, reason: "upstream_api_error", error: new Error(errorMessage), errorMessage, source: "upstream_content_encoding" });
      return;
    }

    markSuccess(idx);

    const a = accounts[idx];

    const safeHeaders = { ...apiRes.headers };
    delete safeHeaders["transfer-encoding"];
    if (extraTransform) {
      delete safeHeaders["content-length"];
      delete safeHeaders["content-encoding"];
      delete safeHeaders["content-md5"];
    }
    safeHeaders["x-proxy-account"] = `${idx + 1}/${accounts.length}`;

    if (clientRes.headersSent) { apiRes.destroy(); activeDecr(idx); onDone({ switched: false }); return; }
    clientRes.writeHead(apiRes.statusCode, safeHeaders);
    streamAttached = true;

    const inputBytes = body ? body.length : 0;
    const transform = makeUsageTransform(idx, inputBytes, reqStart, ttfb, resolvedModel);
    let cleaned = false;
    let endedNormally = false;
    let streamTimer = null;
    let progressTimer = null;
    const cleanup = () => {
      if (cleaned) return;
      cleaned = true;
      if (streamTimer) { clearTimeout(streamTimer); streamTimer = null; }
      if (progressTimer) { clearInterval(progressTimer); progressTimer = null; }
      activeDecr(idx);
      const dur = Date.now() - reqStart;
      const accBytes = transform.accBytes || 0;
      recordPath(pathname, method, inputBytes, accBytes, dur);
      Object.assign(logEntry, { status: apiRes.statusCode, inputBytes, outputBytes: accBytes, duration: dur, ttfb });
      if (lifecycle) {
        Object.assign(logEntry, {
          streamId: lifecycle.responseId,
          streamOutcome: lifecycle.terminalKind || "unknown",
          streamReason: lifecycle.terminalReason || "unknown",
          streamSawDone: lifecycle.sawDone === true,
          upstreamErrorReason: lifecycle.terminalKind === "failed" ? (lifecycle.terminalReason || "upstream_api_error") : "",
          streamErrorMsg: lifecycle.upstreamErrorMessage || "",
          terminalSource: lifecycle.terminalSource || "upstream_sse",
        });
      }
      addLog(logEntry);
      const _ks2=getKeyState(idx);_ks2.lastStatus=apiRes.statusCode;_ks2.lastTime=Date.now();_ks2.lastModel=logEntry.overrideModel||logEntry.reqModel||null;
      if (typeof taskInsightSink === "function") {
        try {
          const metrics = taskInsightBuildMetrics(lifecycle, transform, inputBytes, accBytes, dur, apiRes.statusCode, endedNormally, resolvedModel, lifecycle ? lifecycle.terminalReason : "");
          metrics.time = Date.now();
          taskInsightSink(metrics);
        } catch (e) {
          console.error(`[proxy] task insight sink error: ${e.message}`);
        }
      }
      if (!lifecycle) {
        recordRequest(idx, endedNormally, inputBytes, accBytes, dur, ttfb, resolvedModel, apiRes.statusCode, client, requestPricing);
      }
      if (lifecycle && lifecycle.terminalReason) {
        recordStreamOutcome(idx, lifecycle.terminalReason, true);
      }
      broadcastStatus();
    };

    if (lifecycle) {
      lifecycle._metricsCallback = (success) => {
        const dur = Date.now() - reqStart;
        const accBytes = transform.accBytes || 0;
        recordRequest(idx, success, inputBytes, accBytes, dur, ttfb, resolvedModel, success ? (apiRes.statusCode || 200) : 0, client, requestPricing);
        if (!success) markStreamTerminalFailure(idx, lifecycle, clientCancelled);
      };
      lifecycle._onTerminal = completedLifecycle => {
        addStreamTerminalLog(idx, completedLifecycle);
        cleanup();
        // Converted streams have already emitted their protocol terminal. Stop
        // a misbehaving upstream from holding a socket open with later bytes.
        // The native Responses probe keeps a successful upstream tail intact.
        if ((!extraTransform || !extraTransform._nativeResponsesProbe || completedLifecycle.terminalKind !== "completed") && !apiRes.destroyed) {
          apiRes.unpipe(transform);
          apiRes.destroy();
        }
      };
    }

    terminateAttachedStream = reason => {
      if (!lifecycle || clientCancelled || lifecycle.terminalKind) return false;
      lifecycle.emitFailed(reason);
      if (!apiRes.destroyed) apiRes.destroy();
      return true;
    };

    if (lifecycle && extraTransform) {
      extraTransform.once("finish", () => {
        if (!lifecycle.terminalKind && !clientCancelled) lifecycle.emitFailed("upstream_eof_without_done");
      });
    }
    apiRes.on("end", () => {
      endedNormally = true;
      if (!lifecycle) cleanup();
    });
    apiRes.on("close", () => {
      if (!endedNormally && !clientCancelled) {
        if (lifecycle && streamAttached && !lifecycle.terminalKind) {
          lifecycle.emitFailed("upstream_close");
        } else if (!lifecycle) {
          markFailure(idx, 0);
        }
      }
      cleanup();
    });
    apiRes.on("aborted", () => {
      if (!clientCancelled && lifecycle && streamAttached && !lifecycle.terminalKind) {
        lifecycle.emitFailed("upstream_aborted");
      }
      cleanup();
    });
    apiRes.on("error", (err) => {
      console.error(`[proxy] #${idx+1} Stream error: ${err.message}`);
      if (!clientCancelled) {
        if (streamAttached && lifecycle && !lifecycle.terminalKind) {
          lifecycle.emitFailed("upstream_error", err && err.message);
        } else if (!lifecycle) {
          markFailure(idx, 0);
        }
      }
      cleanup();
    });
    clientRes.on("close", () => {
      if (cleaned || clientCancelled || (lifecycle && lifecycle.terminalKind)) return;
      clientCancelled = true;
      if (lifecycle) lifecycle.noteClientCancelled("client_disconnect");
      if (!apiRes.destroyed) apiRes.destroy();
      cleanup();
    });
    if (streamLifetime > 0) {
      streamTimer = setTimeout(() => {
        if (cleaned || clientCancelled || (lifecycle && lifecycle.terminalKind)) return;
        console.error(`[proxy] #${idx+1} Stream lifetime timeout (${streamLifetime / 60000}min)`);
        if (terminateAttachedStream && terminateAttachedStream("stream_lifetime_timeout")) return;
        if (!apiRes.destroyed) apiRes.destroy();
      }, streamLifetime);
    }
    // Long-running coding streams can hang when the upstream stops sending any
    // bytes but never closes the socket. The idle timeout counts from request
    // start, so it cannot catch a stall that begins after a healthy preamble.
    // Track bytes observed since the last real chunk and force a failure
    // terminal when nothing has arrived for the configured window.
    const noProgressMs = (RESPONSES_NO_PROGRESS_TIMEOUT > 0 && codingProtocolStream && lifecycle)
      ? Math.min(RESPONSES_NO_PROGRESS_MAX_MS, RESPONSES_NO_PROGRESS_TIMEOUT)
      : 0;
    if (noProgressMs > 0) {
      progressTimer = setInterval(() => {
        if (cleaned || clientCancelled || (lifecycle && lifecycle.terminalKind)) return;
        const lastActivity = transform.lastActivity || reqStart;
        const idleMs = Date.now() - lastActivity;
        if (idleMs >= noProgressMs) {
          console.error(`[proxy] #${idx+1} No upstream progress for ${Math.round(idleMs / 60000)}min, terminating coding stream`);
          terminateAttachedStream("no_progress_timeout");
        }
      }, Math.min(60000, noProgressMs));
      if (progressTimer.unref) progressTimer.unref();
    }
    if (extraTransform) {
      apiRes.pipe(transform).pipe(extraTransform).pipe(clientRes);
    } else {
      apiRes.pipe(transform).pipe(clientRes);
    }
    onDone({ switched: false, idx });
  });
  proxyReq.setTimeout(upstreamIdleTimeout);
  // The request is now flushed with the selected account's Authorization key.
  proxyReq.once("finish", () => recordKeyUse(idx));

  proxyReq.on("error", (err) => {
    if (streamAttached) {
      if (lifecycle && !clientCancelled && !lifecycle.terminalKind) {
        console.error(`[proxy] #${idx + 1} Stream request error: ${err.message}`);
        if (terminateAttachedStream) terminateAttachedStream("upstream_error");
      }
      return;
    }
    if (onDone.done) return;
    onDone.done = true;
    const dur = Date.now() - reqStart;
    activeDecr(idx);
    console.error(`[proxy] #${idx + 1} Error: ${err.message}`);
    const errorMessage = sanitizeUpstreamErrorMessage(err && err.message);
    markFailure(idx, 0);
    recordRequest(idx, false, 0, 0, dur, null, resolvedModel, 0, client, requestPricing);
    Object.assign(logEntry, { status: 0, inputBytes: body ? body.length : 0, outputBytes: 0, duration: dur, ttfb: null, upstreamErrorReason: "upstream_error", streamErrorMsg: errorMessage, terminalSource: "upstream_transport_error" });
    addLog(logEntry);
    const _ks3=getKeyState(idx);_ks3.lastStatus=0;_ks3.lastTime=Date.now();_ks3.lastModel=logEntry.overrideModel||logEntry.reqModel||null;
    onDone({ switched: true, idx, reason: "upstream_error", error: err, errorMessage, source: "upstream_transport_error" });
  });

  proxyReq.on("timeout", () => {
    if (streamAttached) {
      if (lifecycle && !clientCancelled && !lifecycle.terminalKind) {
        console.error(`[proxy] #${idx+1} Upstream stream idle timeout`);
        if (terminateAttachedStream) terminateAttachedStream("upstream_idle_timeout");
      }
      proxyReq.destroy();
      return;
    }
    if (onDone.done) return;
    onDone.done = true;
    const dur = Date.now() - reqStart;
    activeDecr(idx);
    console.error(`[proxy] #${idx+1} Timeout`);
    proxyReq.destroy();
    markFailure(idx, 0);
    if (!lifecycle) {
      recordRequest(idx, false, 0, 0, dur, null, resolvedModel, 0, client, requestPricing);
    }
    recordPath(pathname, method, 0, 0, dur);
    const errorMessage = "Upstream request timed out";
    Object.assign(logEntry, { status: 0, inputBytes: body ? body.length : 0, outputBytes: 0, duration: dur, ttfb: null, upstreamErrorReason: "upstream_idle_timeout", streamErrorMsg: errorMessage, terminalSource: "upstream_timeout" });
    addLog(logEntry);
    const _ks4=getKeyState(idx);_ks4.lastStatus=0;_ks4.lastTime=Date.now();_ks4.lastModel=logEntry.overrideModel||logEntry.reqModel||null;
    onDone({ switched: true, idx, reason: "upstream_idle_timeout", error: new Error("timeout"), errorMessage, source: "upstream_timeout" });
  });

  if (body) {
    let bodyToWrite = body;
    if (acct.model) {
      try {
        const parsed = JSON.parse(body.toString());
        parsed.model = acct.model;
        bodyToWrite = Buffer.from(JSON.stringify(parsed));
      } catch(e) {}
    } else if (cleanModel) {
      try {
        const parsed = JSON.parse(body.toString());
        parsed.model = cleanModel;
        bodyToWrite = Buffer.from(JSON.stringify(parsed));
      } catch(e) {}
    }
    if (!supportsCacheControl(acct.url)) {
      try {
        const parsed = JSON.parse(bodyToWrite.toString());
        delete parsed.enable_thinking;
        delete parsed.thinking_budget;
        if (Array.isArray(parsed.messages)) {
          for (const msg of parsed.messages) {
            if (Array.isArray(msg.content)) {
              for (const block of msg.content) {
                delete block.cache_control;
              }
            }
          }
        }
        bodyToWrite = Buffer.from(JSON.stringify(parsed));
      } catch(e) {}
    }
    proxyReq.write(bodyToWrite);
  }
  proxyReq.end();
}

function forwardWithPriority(method, headers, body, clientRes, pathname, extraTransform, group, opts) {
  group = group || "A";
  let responded = false;
  const usedKeys = new Set();
  const activeCount = accounts.filter(a => a.status === "active" && (a.group || "A") === group).length;
  let retries = 0;
  const MAX_RETRIES = Math.max(activeCount * 2, 10);
  let model = null;
  let forceIdx = -1;
  let lastFailure = null;
  let parsedBody = null;
  try {
    parsedBody = JSON.parse(body.toString());
    model = parsedBody.model || null;
    if (model) {
      const hashMatch = model.match(/#(\d+)$/);
      if (hashMatch) {
        forceIdx = parseInt(hashMatch[1], 10) - 1;
        model = model.slice(0, -hashMatch[0].length) || null;
      }
    }
  } catch(e) {}
  // Task Insight: session join + request-side signals + per-attempt metrics sink.
  let insightSession = null;
  let insightSink = null;
  if (parsedBody && !(opts && opts.internalDistill) && typeof taskInsightActive === "function" && taskInsightActive()) {
    try {
      const client = classifyClientApp(headers["user-agent"]);
      const hint = taskInsightProjectHint();
      insightSession = taskInsightJoin(Date.now(), client, group, hint);
      const sig = (opts && opts.preExtract) ? opts.preExtract : taskInsightExtractRequest(parsedBody);
      if (sig.instructions.length) for (const i of sig.instructions) pushUnique(insightSession.instructions, i, 4);
      if (sig.tools.length) for (const t of sig.tools) pushUnique(insightSession.tools, t, TASK_INSIGHT_TOOLS_MAX);
      if (sig.files.length) for (const f of sig.files) pushUnique(insightSession.files, f, TASK_INSIGHT_FILES_MAX);
      insightSink = (metrics) => {
        try { taskInsightAddRequestMetrics(insightSession, metrics); } catch (e) { console.error(`[proxy] task insight metrics error: ${e.message}`); }
      };
    } catch (e) {
      console.error(`[proxy] task insight join error: ${e.message}`);
      insightSession = null;
      insightSink = null;
    }
  }
  function reportFinalFailure(failure, fallbackMessage) {
    const canRespond = !clientRes.destroyed && !clientRes.writableEnded;
    if (canRespond && failure && Number.isInteger(failure.idx)) {
      addDownstreamTerminalLog(failure.idx, {
        reason: failure.reason || "upstream_api_error",
        errorMessage: failure.errorMessage || "",
        status: 502,
        group,
        method,
        path: pathname,
        reqModel: model,
        source: failure.source || "upstream_failure",
      });
    }
    if (canRespond && !clientRes.headersSent) {
      clientRes.writeHead(502, { "content-type": "application/json" });
      clientRes.end(JSON.stringify({ error: fallbackMessage || "All keys exhausted" }));
    }
    responded = true;
  }
  function attempt() {
    if (responded) return;
    if (forceIdx >= 0) {
      if (forceIdx >= accounts.length) {
        console.error(`[proxy] #N routing: #${forceIdx+1} does not exist (max ${accounts.length})`);
        if (!clientRes.destroyed && !clientRes.headersSent) {
          clientRes.writeHead(400, { "content-type": "application/json" });
          clientRes.end(JSON.stringify({ error: `Key #${forceIdx+1} does not exist, max key count is ${accounts.length}` }));
        }
        responded = true;
        return;
      }
      const a = accounts[forceIdx];
      const tag = a.remark ? ` (${a.remark})` : "";
      console.log(`[proxy] → #${forceIdx+1}${tag} (direct via #N) ${a.url}`);
      forwardRequest(forceIdx, method, headers, body, clientRes, pathname, (r) => {
        if (r.switched) reportFinalFailure(r, `Key #${forceIdx+1} failed: ${r.code || r.error?.message || "error"}`);
        else responded = true;
      }, extraTransform, model, insightSink);
      return;
    }
    if (retries >= MAX_RETRIES) {
      console.error(`[proxy] Max retries (${MAX_RETRIES}) reached, queueing`);
      enqueueRequest(method, headers, body, clientRes, pathname, group, extraTransform, lastFailure);
      responded = true;
      return;
    }
    retries++;
    const idx = pickKey(model, group);
    if (idx < 0 || (usedKeys.has(idx) && inCooldown(idx))) {
      if (idx < 0) {
        console.log(`[proxy] No available keys, queueing request`);
        enqueueRequest(method, headers, body, clientRes, pathname, group, extraTransform, lastFailure);
        responded = true;
        return;
      }
      console.error(`[proxy] All accounts exhausted`);
      reportFinalFailure(lastFailure, "All keys exhausted");
      return;
    }
    usedKeys.add(idx);
    const a = accounts[idx];
    const tag = a.remark ? ` (${a.remark})` : "";
    console.log(`[proxy] → #${idx + 1}${tag} ${a.url}`);
    forwardRequest(idx, method, headers, body, clientRes, pathname, (r) => {
      if (r.switched) lastFailure = r;
      if (r.capacityRetry) {
        // Transient capacity: requeue the request instead of hot-cycling every
        // key (each of which would just 429 again). It waits in the queue up to
        // capacityMaxWaitSeconds and retries as soon as a key is backed off.
        console.log(`[proxy] #${idx+1} → 429/capacity, queueing for retry`);
        enqueueRequest(method, headers, body, clientRes, pathname, group, extraTransform, r, true);
        responded = true;
        return;
      }
      if (r.switched && usedKeys.size < activeCount) { console.log(`[proxy] #${idx+1} → ${r.code||"err"}, switching...`); return attempt(); }
      if (r.switched) reportFinalFailure(r, "All keys exhausted");
      else responded = true;
    }, extraTransform, model, insightSink);
  }
  attempt();
}

// --- WebSocket ---
function setupWebSocket(server) {
  wss = new WebSocketServer({ server });
  wss.on("connection", (ws, req) => {
    const url = new URL(req.url, "http://localhost");
    const token = url.searchParams.get("token");
    if (config.adminToken && token !== config.adminToken) {
      ws.close(4001, "unauthorized");
      return;
    }
    wsClients.add(ws);
    ws._logSubscribed = false;
    const data = buildStatusData();
    const msg = JSON.stringify({ type: "status", data, boostedIdx: _boostKey >= 0 ? _boostKey + 1 : -1, boostedBatch: _boostBatch.map(i => i + 1), boostedBatchMode: _boostBatchMode, lastRequestTime, lastKeyUseTime, lastResumeTime });
    safeWsSend(ws, msg);
    ws.on("message", raw => {
      try {
        const message = JSON.parse(String(raw));
        if (message && message.type === "log_subscribe") {
          ws._logSubscribed = message.enabled === true;
          safeWsSend(ws, JSON.stringify({ type: "log_subscription", enabled: ws._logSubscribed }));
        }
      } catch { /* ignore malformed dashboard messages */ }
    });
    ws.on("close", () => wsClients.delete(ws));
    ws.on("error", () => wsClients.delete(ws));
  });
  if (!statusHeartbeatTimer) {
    statusHeartbeatTimer = setInterval(() => {
      try {
        if (wsClients.size) doBroadcastStatus();
      } catch (e) { /* never let the heartbeat take down the proxy */ }
    }, STATUS_HEARTBEAT_MS).unref();
  }
}

// Send to a dashboard WebSocket only while it is healthy. A stalled/backgrounded
// browser tab would otherwise buffer unlimited outgoing messages and drive the
// proxy into the 4GB JS heap limit (observed as recurring OOM crashes). Once the
// buffer exceeds STATUS_WS_BUFFER_LIMIT the client is dropped; the dashboard
// reconnects and re-fetches the full status on connect.
function safeWsSend(ws, msg) {
  if (!ws || ws.readyState !== 1) return;
  if (ws.bufferedAmount > STATUS_WS_BUFFER_LIMIT) {
    try { ws.terminate(); } catch { /* ignore */ }
    wsClients.delete(ws);
    return;
  }
  try {
    ws.send(msg);
  } catch { /* ignore send errors on half-closed sockets */ }
}

// Coalesce status broadcasts to at most one per STATUS_BROADCAST_INTERVAL_MS.
// State is always sent as a full snapshot, never as a delta, so merging multiple
// changes into a single broadcast loses nothing. The periodic heartbeat in
// setupWebSocket guarantees every client eventually receives fresh state even if
// an edge case ever swallows a trailing update.
function broadcastStatus() {
  if (statusBroadcastTimer) return;
  statusBroadcastTimer = setTimeout(() => {
    statusBroadcastTimer = null;
    try {
      doBroadcastStatus();
    } catch (e) { /* never let a broadcast failure take down the proxy */ }
  }, STATUS_BROADCAST_INTERVAL_MS);
  if (statusBroadcastTimer.unref) statusBroadcastTimer.unref();
}

function doBroadcastStatus() {
  const data = buildStatusData();
  const msg = JSON.stringify({ type: "status", data, boostedIdx: _boostKey >= 0 ? _boostKey + 1 : -1, boostedBatch: _boostBatch.map(i => i + 1), boostedBatchMode: _boostBatchMode, lastRequestTime, lastKeyUseTime, lastResumeTime });
  lastBroadcast = msg;
  for (const ws of wsClients) {
    safeWsSend(ws, msg);
  }
}

function broadcastNotification(type, data = {}) {
  const msg = JSON.stringify({ type: "notification", notificationType: type, time: new Date().toISOString(), ...data });
  for (const ws of wsClients) {
    safeWsSend(ws, msg);
  }
}

function broadcastLog(entry) {
  if (!wsClients.size) return;
  for (const ws of wsClients) {
    if (ws._logSubscribed === true) safeWsSend(ws, JSON.stringify({ type: "log", data: entry }));
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
      resetHours: a.resetHours || null,
    group: (a.group || "A").toUpperCase(),
      activatedAt: ks.activatedAt || null,
      available: !inCooldown(i),
      status: ks.status || "active",
      failCode: ks.failCode,
      failTime: ks.failTime,
      failPeriod: ks.failPeriod,
      failCount: ks.failCount,
      capUntil: ks.capUntil || 0,
      locked: ks.status === "locked",
      shielded: a.status === "shielded",
      active: (activeRequests[i] || []).length > 0,
      activeRequests: (activeRequests[i] || []).length,
      actives: (activeRequests[i] || []).map(r => ({model: r.model || "?", since: r.start})),
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
      daily: selectRecentStateBuckets(s.daily, false, STATUS_DAILY_BUCKET_LIMIT),
      hourly: selectRecentStateBuckets(s.hourly, true, STATUS_HOURLY_BUCKET_LIMIT),
      timeWindow: a.timeWindow || undefined,
      tz: a.tz || undefined,
      _inTimeWindow: a.timeWindow ? isInTimeWindow(a) : undefined,
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
<title>OpenAPI Multi-Key Proxy</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:clamp(8px,2vw,20px)}
h1{font-size:clamp(16px,3vw,20px);margin-bottom:4px;color:#f1f5f9}
.sub{color:#94a3b8;font-size:clamp(10px,1.5vw,12px);margin-bottom:clamp(8px,2vw,16px);display:flex;align-items:center;justify-content:space-between;gap:12px;min-height:28px}
.sub-ts{flex-shrink:0;white-space:nowrap}
.ticker-wrap{display:flex;align-items:center;gap:8px;min-width:0;flex:1;justify-content:flex-end;overflow:hidden}
.ticker-label{color:#4ade80;font-size:13px;font-weight:700;white-space:nowrap;flex-shrink:0;display:none}
#ticker{display:flex;gap:6px;flex-wrap:nowrap;overflow:hidden;min-width:0}
.ticker-item{display:inline-flex;align-items:center;gap:4px;background:#052e16;color:#4ade80;border:1px solid #16a34a;border-radius:5px;padding:2px 8px;font-size:13px;font-weight:600;white-space:nowrap;flex-shrink:0}
.ticker-overflow{display:inline-flex;align-items:center;background:#1e293b;color:#94a3b8;border:1px solid #475569;border-radius:5px;padding:2px 8px;font-size:12px;font-weight:600;white-space:nowrap;flex-shrink:0}
.ticker-item .t-dur{color:#86efac;font-weight:400;font-size:12px}
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
.bd-hourly{background:#1a3a2e;color:#4ade80}
.bd-group{background:#2d1f3f;color:#fbbf24;border:1px solid #a855f7}
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
.btn:disabled{cursor:not-allowed;opacity:.55}
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
.trend-stack{display:flex;flex-direction:column;align-items:stretch;border-radius:2px 2px 0 0;overflow:hidden}
.trend-stack:hover{filter:brightness(1.2)}
.trend-seg{width:100%;min-height:1px}
.trend-legend{display:flex;flex-wrap:wrap;gap:4px 10px;margin-top:6px;font-size:clamp(9px,1.2vw,11px);color:#94a3b8}
.trend-legend-item{display:inline-flex;align-items:center;gap:4px;white-space:nowrap}
.trend-legend-dot{width:8px;height:8px;border-radius:2px;flex-shrink:0}
.trend-tabs{display:flex;gap:4px;flex-wrap:wrap}
.trend-tab{padding:2px 8px;font-size:clamp(11px,1.2vw,12px);color:#64748b;background:#1e293b;border:1px solid #334155;border-radius:4px;cursor:pointer;user-select:none;white-space:nowrap}
.trend-tab:hover{background:#334155;color:#e2e8f0}
.trend-tab.active{background:#3b82f6;color:#fff;border-color:#3b82f6}
.log-table{width:100%;border-collapse:collapse;font-size:clamp(10px,1.3vw,11px)}
.log-table th,.log-table td{padding:2px 4px;text-align:left;border-bottom:1px solid #334155;white-space:nowrap}
.log-table th{color:#94a3b8;font-weight:600;position:sticky;top:0;background:#1e293b}
.log-status{font-weight:600}
.log-s200{color:#4ade80}
.log-s201{color:#4ade80}
.log-s301{color:#f59e0b}
.log-s400{color:#f59e0b}
.log-s401{color:#f59e0b;background:rgba(245,158,11,0.08)}
.log-s402{color:#f59e0b;background:rgba(245,158,11,0.08)}
.log-s403{color:#f59e0b;background:rgba(245,158,11,0.08)}
.log-s429{color:#f87171;background:rgba(248,113,113,0.08)}
.log-s5xx{color:#ef4444;background:rgba(239,68,68,0.12)}
.log-s0{color:#64748b;background:rgba(100,116,139,0.08)}
.log-row-event{color:#60a5fa;background:rgba(96,165,250,0.06)}
.log-row-conversion{color:#a78bfa;background:rgba(167,139,250,0.06)}
.log-row-recover{color:#4ade80;background:rgba(74,222,128,0.06)}
.log-row-lock{color:#ef4444;background:rgba(239,68,68,0.1)}
.log-row-discard{color:#f97316;background:rgba(249,115,22,0.1)}
.log-stat-card{border:1px solid #334155}
.log-time{color:#64748b;font-size:clamp(9px,1.2vw,10px)}
.log-dur{color:#94a3b8}
/* sparkline */
.log-sparkline-wrap{display:flex;align-items:flex-end;gap:2px;height:28px;padding:2px 0;margin-bottom:4px}
.log-spark-bar{flex:1;min-width:3px;border-radius:1px 1px 0 0;position:relative;background:#334155;cursor:pointer}
.log-spark-bar.ok{background:#3b82f6}
.log-spark-bar.err{background:#ef4444}
.log-spark-bar:hover{opacity:.8}
/* model distribution */
.log-model-row{display:flex;gap:6px;flex-wrap:wrap;font-size:10px;color:#94a3b8;margin-bottom:4px;padding:2px 0}
.log-model-tag{background:#1e3a5f;color:#93c5fd;padding:1px 6px;border-radius:3px;white-space:nowrap}
.log-model-tag .fail{color:#ef4444}
/* error cluster */
.log-error-cluster{cursor:pointer;user-select:none;margin-bottom:4px}
.log-error-cluster .head{font-size:11px;color:#f87171}
.log-error-cluster .body{display:none;flex-wrap:wrap;gap:4px;padding:2px 0;font-size:10px}
.log-error-cluster .body.open{display:flex}
.log-error-code{background:#3b1f1e;color:#f87171;padding:1px 5px;border-radius:3px;white-space:nowrap}
/* expandable row detail */
.log-row-expand{display:none}
.log-row-expand.open{display:table-row}
.log-row-expand td{background:#0f172a;padding:6px 8px;font-size:10px;color:#94a3b8;word-break:break-all;white-space:pre-wrap;max-width:800px;font-family:monospace}
.log-table tr{cursor:pointer}
/* per-key stats popup */
.log-key-popup{position:fixed;background:#1e293b;border:1px solid #3b82f6;border-radius:8px;padding:12px;z-index:200;min-width:280px;max-width:400px;box-shadow:0 4px 20px rgba(0,0,0,.4)}
.log-key-popup .close{float:right;cursor:pointer;color:#94a3b8;font-size:14px}
.log-key-popup .title{font-size:13px;font-weight:700;color:#e2e8f0;margin-bottom:6px}
.log-key-popup .stat-row{display:flex;justify-content:space-between;padding:1px 0;font-size:11px}
.log-key-popup .stat-row .l{color:#94a3b8}
.log-key-popup .stat-row .r{color:#e2e8f0}
.file-banner{display:none;background:#1e3a5f;border:1px solid #3b82f6;border-radius:8px;padding:clamp(8px,1.5vw,10px) clamp(10px,2vw,14px);margin-bottom:clamp(8px,2vw,12px);font-size:clamp(12px,1.8vw,13px);color:#93c5fd;align-items:center;gap:10px}
.file-banner.on{display:flex}
.restart-overlay{display:none;position:fixed;inset:0;z-index:10000;padding:20px;background:rgba(2,6,23,.88);align-items:center;justify-content:center}
.restart-overlay.on{display:flex}
.restart-panel{width:min(420px,100%);background:#1e293b;border:1px solid #3b82f6;border-radius:8px;padding:24px;text-align:center;box-shadow:0 12px 32px rgba(0,0,0,.45)}
.restart-spinner{width:34px;height:34px;margin:0 auto 14px;border:3px solid #475569;border-top-color:#60a5fa;border-radius:50%;animation:restart-spin .8s linear infinite}
.restart-overlay.error .restart-spinner{display:none}
.restart-overlay.complete .restart-spinner{display:none}
.restart-title{color:#f1f5f9;font-size:16px;font-weight:700;margin-bottom:8px}
.restart-detail{color:#94a3b8;font-size:13px;line-height:1.55;min-height:40px}
.restart-elapsed{color:#60a5fa;font-size:12px;margin-top:10px;min-height:18px}
@keyframes restart-spin{to{transform:rotate(360deg)}}
.update-badge{display:none;align-items:center;justify-content:center;min-width:30px;padding:3px 7px;background:#4a2708;border:1px solid #f59e0b;color:#fbbf24;border-radius:4px;cursor:pointer;font-size:14px;line-height:1;animation:update-pulse 1.35s ease-in-out infinite}
.update-badge.on{display:inline-flex}
.update-badge.neutral{display:inline-flex;background:#1e293b;border:1px solid #475569;color:#94a3b8;animation:none}
.update-badge.neutral:hover{background:#334155;color:#e2e8f0}
.update-badge:hover{background:#6b3809;color:#fde68a}
@keyframes update-pulse{0%,100%{box-shadow:0 0 0 0 rgba(245,158,11,0)}50%{box-shadow:0 0 0 5px rgba(245,158,11,.24)}}
.update-notes{margin-top:10px;max-height:280px;overflow:auto;white-space:pre-wrap;word-break:break-word;background:#0f172a;border:1px solid #334155;border-radius:4px;padding:10px;color:#cbd5e1;font:12px/1.55 ui-monospace,SFMono-Regular,Menlo,monospace}
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
<h1>OpenAPI Multi-Key Proxy</h1>
<div style="display:flex;gap:6px;flex-wrap:wrap">
<button class="btn" onclick="openLogs()">📋 日志</button>
<button class="btn" onclick="openTaskInsight()">📊 任务流水</button>
<button class="btn" onclick="openExportCover()">⬇ 导出 CSV</button>
<button class="btn btn-p" onclick="openMgr()">⚙ 管理 Key</button>
<button class="btn btn-s" onclick="openConfig()">⚙ 配置</button>
<button class="update-badge" id="updateBadge" type="button" onclick="openUpdateModal()" title="发现可升级版本，点击查看 Release 说明与安全升级方法" aria-label="发现可升级版本">⬆</button>
</div>
</div>
<div class="sub" id="sub">
  <span class="sub-ts" id="subText">加载中...</span>
  <div class="ticker-wrap">
    <span class="ticker-label" id="tickerLabel">⚡ 并发中：</span>
    <div id="ticker"></div>
  </div>
</div>
<div id="alert" class="alert">⚠️ 所有 Key 均不可用，请求将全部失败！</div>
<div class="top-row" id="summary"></div>
<div class="controls" id="controls">
  <label>排序</label>
  <select id="sortBy"><option value="idx">默认顺序</option><option value="weeklyExpiry">按到期日（最近→最远）</option><option value="activatedAt">首次启用（早→晚）</option><option value="duration">使用时长（长→短）</option><option value="score">健康评分</option><option value="latency">平均延迟</option><option value="rate5m">5分钟成功率</option><option value="group">按分组</option></select>
  <label>筛选</label>
   <select id="filterBy"><option value="all">全部</option><option value="available">可用</option><option value="cooldown">冷却中</option><option value="discarded">废弃</option><option value="locked">🔒 锁死</option><option value="shielded">屏蔽</option></select>
  <label>重置</label>
  <select id="resetFilter"><option value="all">全部</option><option value="daily">每日重置</option><option value="weekly">每周重置</option><option value="hourly">每N小时重置</option><option value="never">永不过期</option></select>
  <span id="weeklyResetDayFilterWrap" style="display:none;align-items:center;gap:4px"><label>周重置日</label><select id="weeklyResetDayFilter"><option value="all">全部</option><option value="1">周一</option><option value="2">周二</option><option value="3">周三</option><option value="4">周四</option><option value="5">周五</option><option value="6">周六</option><option value="7">周日</option><option value="auto">自动</option></select></span>
  <label>分组</label>
  <select id="groupFilter"><option value="all">全部</option></select>
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
  <span style="color:#22c55e;font-size:11px;margin-left:8px;font-weight:500" id="dashResumeStatus"></span>
</div>
<div id="batchBar" style="display:none;margin-bottom:8px;padding:6px 8px;background:#1e293b;border:1px solid #475569;border-radius:6px;gap:6px;flex-wrap:wrap;align-items:center">
  <span style="color:#94a3b8;font-size:12px" id="batchCount">已选 0 个</span>
  <span id="batchModeStatus" style="display:none;color:#facc15;font-size:12px;font-weight:500"></span>
  <button class="btn" style="font-size:11px" onclick="batchActionCards('reset')">🔄 批量重置</button>
  <button class="btn" style="font-size:11px;color:#f87171" onclick="batchActionCards('shield')">🔇 批量屏蔽</button>
  <button class="btn" style="font-size:11px;color:#94a3b8;border-color:#64748b" onclick="selectAllCards()">☐ 全选</button>
  <button class="btn" style="font-size:11px;color:#94a3b8;border-color:#64748b" onclick="deselectAllCards()">☐ 全取消</button>
  <button class="btn" id="batchBoostUseBtn" style="font-size:11px;color:#4ade80;border-color:#22c55e" onclick="batchActionCards('use')">⚡ 优先使用</button>
  <button class="btn" id="batchBoostRRBtn" style="font-size:11px;color:#4ade80;border-color:#22c55e" onclick="batchActionCards('roundrobin')">⭕ 优先轮询</button>
  <button class="btn" id="batchBoostRandBtn" style="font-size:11px;color:#4ade80;border-color:#22c55e" onclick="batchActionCards('random')">🎲 随机轮询</button>
  <button class="btn" id="batchCancelBoostBtn" style="display:none;font-size:11px;color:#f87171" onclick="batchActionCards('cancelboost')">✕ 取消批量优先</button>
</div>
<div id="trend" class="trend-wrap" style="display:none">
<div class="trend-title"><div class="trend-tabs" id="trendTabs"><span class="trend-tab active" data-mode="model" onclick="setTrendMode('model')">📊 模型</span><span class="trend-tab" data-mode="bytes" onclick="setTrendMode('bytes')">📊 流量</span><span class="trend-tab" data-mode="req" onclick="setTrendMode('req')">📈 次数</span><span class="trend-tab" data-mode="health" onclick="setTrendMode('health')">💚 健康</span><span class="trend-tab" data-mode="upstream" onclick="setTrendMode('upstream')">🔺 上游</span><span class="trend-tab" data-mode="downstream" onclick="setTrendMode('downstream')">🔻 下游</span><span class="trend-tab" data-mode="cost" onclick="setTrendMode('cost')">💰 费用</span><span class="trend-tab" data-mode="latency" onclick="setTrendMode('latency')">⏱ 延迟</span></div><span id="trendRangeLabel" style="font-size:10px;color:#64748b">24h</span></div>
<div class="trend-bars" id="trendBars"></div>
<div class="trend-labels" id="trendLabels"></div>
<div id="trendLegend" class="trend-legend"></div>
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
    <option value="duration">启用时长</option>
    <option value="lastFail">最后失败</option>
    <option value="lastResp">最后响应</option>
    <option value="resetDay">周重置日</option>
    <option value="timeIn">时段内</option>
    <option value="timeOut">非时段</option>
  </select>
  <input id="mgrDurationDays" type="number" min="1" style="display:none;width:60px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;font-size:11px" placeholder="≥X天" oninput="renderMgr()" title="筛选启用距今 ≥ X 天的 Key，可与其他条件组合">
  <input id="mgrLastFailDays" type="number" min="1" style="display:none;width:60px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;font-size:11px" placeholder="≥X天" oninput="renderMgr()" title="筛选最后失败距今 ≥ X 天的 Key，可与其他条件组合">
  <select id="mgrResetDayFilter" onchange="renderMgr()" style="display:none;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;font-size:11px" title="筛选指定周重置日的 Key">
    <option value="">全部</option>
    <option value="auto">自动（未设置）</option>
    <option value="1">周一</option>
    <option value="2">周二</option>
    <option value="3">周三</option>
    <option value="4">周四</option>
    <option value="5">周五</option>
    <option value="6">周六</option>
    <option value="7">周日</option>
  </select>
  <select id="mgrSortBy" onchange="mgrSortBy=this.value;renderMgr()" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;font-size:11px">
    <option value="default">默认顺序</option>
    <option value="resetDay">按重置日（周一→周日）</option>
    <option value="activatedAt">首次启用（早→晚）</option>
    <option value="duration">使用时长（长→短）</option>
    <option value="group">按分组</option>
  </select>
  <button class="btn" style="font-size:11px" onclick="selectAllMgr(true)">全选</button>
  <button class="btn" style="font-size:11px" onclick="clearMgrSearch()">取消</button>
  <button class="btn" style="font-size:11px" onclick="batchShieldMgr()">🔇 批量屏蔽</button>
  <button class="btn" style="font-size:11px" onclick="batchResetMgr()">🔄 批量重置</button>
  <button class="btn" style="font-size:11px;color:#f87171" onclick="batchDeleteMgr()">✕ 批量删除</button>
  <button class="btn" style="font-size:11px;color:#f59e0b" onclick="cleanFailedKeys()">🧹 清理失败</button>
  <button class="btn" style="font-size:11px;color:#4ade80" onclick="batchSetTimeWindow()" title="为选中的 Key 设置错峰可用小时段（含时区）；开始==结束=全天可用">⏰ 错峰时段</button>
  <button class="btn" style="font-size:11px;color:#fb923c" onclick="batchClearTimeWindow()" title="清除选中 Key 的错峰时段，恢复全天可用">⏰ 清除时段</button>
  <button class="btn" style="font-size:11px" onclick="openImportMgr()">📋 导入</button>
  <button class="btn" style="font-size:11px" onclick="openExportMgr()">📥 导出</button>
  <button class="btn" style="font-size:11px" onclick="batchTestMgr()">🔍 批量测试</button>
  <button class="btn" style="font-size:11px" onclick="toggleHideShielded()" id="mgrHideBtn">🙉 显示已屏蔽</button>
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
<table class="mtable"><thead id="mgrThead"></thead><tbody id="mgrBody"></tbody></table>
<div class="mfoot">
<button class="btn" onclick="addKeyRow()">+ 添加一行</button>
<div style="flex:1"></div>
<button class="btn" onclick="closeMgr()">取消</button>
<button class="btn btn-p" onclick="saveKeys()">保存</button>
</div>
</div></div>
<div id="importMgrCover" style="display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.6);z-index:10001;align-items:center;justify-content:center">
<div style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:16px;min-width:420px;max-width:90vw">
<div style="font-size:14px;font-weight:600;color:#e2e8f0;margin-bottom:8px">批量导入 Key</div>
<div style="font-size:11px;color:#94a3b8;margin-bottom:8px;line-height:1.6">
每行一个 Key，格式：<code style="background:#0f172a;padding:1px 4px;border-radius:3px">sk-xxx URL [重置类型] [优先级] [分组] [备注]</code><br>
URL 为必填项。重置类型：daily/weekly/never/hourly（或 每日/每周/永久/每N小时）<br>
示例：<code style="background:#0f172a;padding:1px 4px;border-radius:3px">sk-abc123 https://your-api.com weekly 0 A 我的Key</code>
</div>
<textarea id="importMgrTxt" style="width:100%;height:200px;resize:both;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:8px;border-radius:4px;font-family:monospace;font-size:12px;box-sizing:border-box" placeholder="粘贴 Key 数据到此处..."></textarea>
<div style="margin-top:10px;display:flex;gap:8px;justify-content:flex-end">
<button class="btn" onclick="closeImportMgr()">取消</button>
<button class="btn btn-p" onclick="doImportKeys()">导入</button>
</div>
</div>
</div>

<div id="exportMgrCover" style="display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.6);z-index:10001;align-items:center;justify-content:center">
<div style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:16px;min-width:360px;max-width:90vw">
<div style="font-size:14px;font-weight:600;color:#e2e8f0;margin-bottom:8px">导出 CSV（选择字段）</div>
<div id="exportMgrFields" style="display:grid;grid-template-columns:1fr 1fr;gap:4px 12px;font-size:12px;color:#cbd5e1;margin-bottom:12px">
<label><input type="checkbox" checked disabled> Key</label>
<label><input type="checkbox" checked disabled> URL</label>
<label><input type="checkbox" class="exp-f" value="reset"> 重置类型</label>
<label><input type="checkbox" class="exp-f" value="priority"> 优先级</label>
<label><input type="checkbox" class="exp-f" value="group"> 分组</label>
<label><input type="checkbox" class="exp-f" value="remark"> 备注</label>
<label><input type="checkbox" class="exp-f" value="models"> 指定模型</label>
<label><input type="checkbox" class="exp-f" value="model"> 覆盖模型</label>
<label><input type="checkbox" class="exp-f" value="resetDay"> 重置日</label>
<label><input type="checkbox" class="exp-f" value="tz"> 时区</label>
<label><input type="checkbox" class="exp-f" value="timeWindow"> 时段</label>
</div>
<div style="margin-top:10px;display:flex;gap:8px;justify-content:flex-end">
<button class="btn" onclick="closeExportMgr()">取消</button>
<button class="btn btn-p" onclick="doExportCSV()">导出</button>
</div>
</div>
</div>

<div id="exportCover" style="display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.6);z-index:10001;align-items:center;justify-content:center">
<div style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:16px;min-width:380px;max-width:90vw">
<div style="font-size:14px;font-weight:600;color:#e2e8f0;margin-bottom:8px">导出 CSV（选择字段）</div>
<div style="font-size:11px;color:#94a3b8;margin-bottom:8px">导出当前页面可见的所有 Key</div>
<div id="exportCoverFields" style="display:grid;grid-template-columns:1fr 1fr;gap:4px 12px;font-size:12px;color:#cbd5e1;margin-bottom:12px">
<div style="grid-column:1/-1;font-size:11px;color:#64748b;margin-bottom:2px">── 配置字段 ──</div>
<label><input type="checkbox" checked disabled> Key</label>
<label><input type="checkbox" checked disabled> URL</label>
<label><input type="checkbox" class="exp-cfg" value="reset"> 重置类型</label>
<label><input type="checkbox" class="exp-cfg" value="remark"> 备注</label>
<label><input type="checkbox" class="exp-cfg" value="group"> 分组</label>
<label><input type="checkbox" class="exp-cfg" value="priority"> 优先级</label>
<label><input type="checkbox" class="exp-cfg" value="models"> 指定模型</label>
<label><input type="checkbox" class="exp-cfg" value="model"> 覆盖模型</label>
<label><input type="checkbox" class="exp-cfg" value="resetDay"> 重置日</label>
<label><input type="checkbox" class="exp-cfg" value="tz"> 时区</label>
<label><input type="checkbox" class="exp-cfg" value="timeWindow"> 时段</label>
<div style="grid-column:1/-1;font-size:11px;color:#64748b;margin:4px 0 2px">── 统计字段 ──</div>
<label><input type="checkbox" class="exp-st" value="status"> 状态</label>
<label><input type="checkbox" class="exp-st" value="failCode"> 失败码</label>
<label><input type="checkbox" class="exp-st" value="totalRequests"> 请求数</label>
<label><input type="checkbox" class="exp-st" value="successRequests"> 成功数</label>
<label><input type="checkbox" class="exp-st" value="failRequests"> 失败数</label>
<label><input type="checkbox" class="exp-st" value="inputBytes"> 输入字节</label>
<label><input type="checkbox" class="exp-st" value="outputBytes"> 输出字节</label>
<label><input type="checkbox" class="exp-st" value="avgDuration"> 平均耗时</label>
<label><input type="checkbox" class="exp-st" value="healthScore"> 健康分</label>
<label><input type="checkbox" class="exp-st" value="totalCost"> 费用</label>
</div>
<div style="margin-top:10px;display:flex;gap:8px;justify-content:flex-end">
<button class="btn" onclick="closeExportCover()">取消</button>
<button class="btn btn-p" onclick="doFrontendExport()">导出</button>
</div>
</div>
</div>

<div class="modal" id="configModal">
<div class="mcontent">
<div class="mtitle"><span>系统配置</span><button class="btn" onclick="closeConfig()">✕</button></div>
<div style="font-size:11px;color:#94a3b8;margin-bottom:12px" id="configStatus">修改后自动保存</div>
<div id="cfgVersionInfo" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:-4px 0 12px;padding:7px 9px;background:#0f172a;border:1px solid #334155;border-radius:4px;font-size:11px;color:#94a3b8">
  <span>本地构建：<strong id="cfgCurrentVersion" style="color:#e2e8f0">本地开发/定制版本（未能验证发布基线）</strong></span>
  <span id="cfgBuildProvenance" style="color:#64748b">来源：待识别</span>
  <span>最新正式 Release：<strong id="cfgLatestRelease" style="color:#e2e8f0">未知</strong></span>
  <span id="cfgVersionGap" style="color:#94a3b8"></span>
  <a href="https://github.com/aipayim/codex-proxy" target="_blank" rel="noopener noreferrer" title="升级前先备份本机定制代码、配置和状态；在 GitHub 查看 Release 后合并变更，并在维护窗口验证、重启代理。" style="color:#60a5fa">GitHub 升级说明 ↗</a>
  <button class="btn" type="button" onclick="openUpdateModal()" style="font-size:10px;padding:2px 6px">检查更新</button>
  <span id="cfgUpdateStatus" style="color:#64748b"></span>
  <div style="width:100%;display:flex;align-items:center;gap:7px;flex-wrap:wrap;color:#94a3b8">
    <label for="cfgUpdateBaselineTag">定制构建基线 Tag（高级、可选）</label>
    <input id="cfgUpdateBaselineTag" style="width:140px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" placeholder="例如 v1.2.3" title="官方发布包和干净的正式 Git Tag 会自动识别。仅在定制构建需要比较时填写已人工确认的正式 GitHub Release Tag。">
    <span style="color:#64748b">官方发布包自动识别；定制构建留空则只显示 Release，不判断更新。</span>
  </div>
</div>
<div style="display:grid;grid-template-columns:1fr 2fr;gap:10px;font-size:12px">
  <div style="color:#94a3b8;padding:4px 0">💰 默认输入价格（每百万 token）</div>
  <div><input id="cfgPriceIn" type="number" min="0" max="1000000" step="any" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100px" placeholder="0" title="未匹配模型时使用"></div>
  <div style="color:#94a3b8;padding:4px 0">💰 默认输出价格（每百万 token）</div>
  <div><input id="cfgPriceOut" type="number" min="0" max="1000000" step="any" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100px" placeholder="0" title="未匹配模型时使用"></div>
  <div style="color:#94a3b8;padding:4px 0">🔤 默认每 token 字节数</div>
  <div><input id="cfgBpt" type="number" min="0.1" max="100" step="any" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100px" value="3" placeholder="3" title="未匹配模型时使用"></div>
  <div style="color:#94a3b8;padding:4px 0;grid-column:1/-1;border-bottom:1px solid #334155;margin:4px 0">🧮 按模型计费覆盖</div>
  <div style="grid-column:1/-1">
    <div style="font-size:10px;color:#64748b;margin:0 0 6px">按实际最终转发模型名精确匹配；未配置或未知模型使用上方默认规则。费用按传输字节估算，不等同于上游账单 token；新价格仅影响之后的请求。</div>
    <div id="cfgModelPricingArea"></div>
    <button class="btn" type="button" style="font-size:10px;padding:2px 8px;margin-top:6px" onclick="addModelPricingRule()">＋ 添加模型规则</button>
  </div>
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
  <div style="color:#94a3b8;padding:4px 0">⏱ 检测间隔（毫秒）<span style="color:#64748b;font-size:9px">每 Key 间等待，多个值用逗号分隔（最多 10 个），程序随机选取，模拟人工节奏</span></div>
  <div><input id="cfgAutoRecoverDelays" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100%" value="800" placeholder="800,1200,500">
    <div style="color:#64748b;font-size:9px;margin-top:2px">推荐 800,1200,500 等值，范围 100–10000。所有检测模式共用此设置</div>
  </div>
  <div style="color:#94a3b8;padding:4px 0;grid-column:1/-1;border-bottom:1px solid #334155;margin-bottom:4px">⚡ 快速恢复（针对 5xx 等异常）</div>
  <div style="color:#94a3b8;padding:4px 0">启用快速恢复</div>
  <div><label><input type="checkbox" id="cfgAutoRecoverPoll"> 当 Key 出现以下状态码时快速轮询检测</label></div>
  <div style="color:#94a3b8;padding:4px 0">轮询间隔（分钟）</div>
  <div><input id="cfgAutoRecoverPollInterval" type="number" min="1" max="60" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;width:60px" value="5"></div>
  <div style="color:#94a3b8;padding:4px 0">监控的状态码</div>
  <div><input id="cfgAutoRecoverPollCodes" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100%" value="500,502,503,504" placeholder="500,502,503,504"></div>
  <div style="color:#94a3b8;padding:4px 0">🔁 轮询均摊流量</div>
  <div><label><input type="checkbox" id="cfgRoundRobin"> 启用后可用 key 按优先层层轮流使用，而非固定顺序</label></div>
  <div style="color:#94a3b8;padding:4px 0">📅 每周 Key 按到期日排序</div>
  <div><label><input type="checkbox" id="cfgWeeklySortBy"> 每周重置的 Key 按「最先到期先使用」排序（当日最后），无 resetDay 排最后</label></div>
  <div style="color:#94a3b8;padding:4px 0;grid-column:1/-1;border-bottom:1px solid #334155;margin-bottom:4px">🧬 闲置自动恢复（autoResume）</div>
  <div style="color:#94a3b8;padding:4px 0">启用闲置恢复</div>
  <div><label><input type="checkbox" id="cfgAutoResume"> Key 闲置时自动在 Windows 中打开终端运行项目命令</label></div>
  <div style="color:#94a3b8;padding:4px 0">Key 闲置阈值（分钟）</div>
  <div><input id="cfgAutoResumeIdle" type="number" min="1" max="999" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;width:60px" value="10"> 分钟无请求视为空闲</div>
  <div style="color:#94a3b8;padding:4px 0">防抖间隔（分钟）</div>
  <div><input id="cfgAutoResumeDebounce" type="number" min="1" max="999" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;width:60px" value="3"> 兼容间隔（同一闲置周期仅一次）</div>
  <div style="color:#94a3b8;padding:4px 0">runner 停滞宽限（分钟）</div>
  <div><input id="cfgAutoResumeRunnerStall" type="number" min="0" max="1440" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;width:60px" value="20"> 0=关闭；无在途请求且无新 Key 应用时才判定停滞</div>
  <div style="color:#94a3b8;padding:4px 0">停滞重启上限</div>
  <div><input id="cfgAutoResumeRunnerRestarts" type="number" min="0" max="3" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;width:60px" value="1"> 同一 Key 闲置周期；0=关闭（仅管理已验证 runner）</div>
  <div style="color:#94a3b8;padding:4px 0">cmd.exe 路径</div>
  <div><input id="cfgCmdPath" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100%" value="/mnt/c/Windows/System32/cmd.exe"></div>
  <div style="color:#94a3b8;padding:4px 0;grid-column:1/-1;border-bottom:1px solid #334155;margin-bottom:4px">🚦 容量/429 瞬态退避（capacityBackoff）</div>
  <div style="color:#94a3b8;padding:4px 0">瞬态退避时长（秒）</div>
  <div><input id="cfgCapacityBackoffSeconds" type="number" min="1" max="3600" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;width:70px" value="60"> 429/容量错误后该 Key 短暂跳过，不记为整周期故障</div>
  <div style="color:#94a3b8;padding:4px 0">容量等待上限（秒）</div>
  <div><input id="cfgCapacityMaxWaitSeconds" type="number" min="30" max="3600" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;width:70px" value="300"> 仅剩容量退避时，请求在队列中最多等待秒数（默认 30 秒的超时会被此值替换）</div>
  <div style="color:#94a3b8;padding:4px 0;grid-column:1/-1;margin-bottom:4px">项目列表（最多 10 个）<button class="btn" style="font-size:10px;margin-left:6px" onclick="addResumeProject()">+ 添加项目</button></div>
  <div id="cfgResumeProjects" style="grid-column:1/-1"></div>
  <div style="color:#94a3b8;padding:4px 0;grid-column:1/-1;border-bottom:1px solid #334155;margin-bottom:4px">📋 日志配置</div>
  <div style="color:#94a3b8;padding:4px 0">启用文件日志</div>
  <div><label><input type="checkbox" id="cfgLogFile" checked> 保留 <input id="cfgLogRetention" type="number" min="0" max="3650" style="width:54px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 天自动清理（0=关闭按天清理，容量上限仍有效）</label></div>
  <div style="color:#94a3b8;padding:4px 0">日志详情级别</div>
  <div><label><select id="cfgLogDetail" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"><option value="full">完整</option><option value="basic">简洁</option></select> 简洁模式不记录模型名</label></div>
  <div style="color:#94a3b8;padding:4px 0;grid-column:1/-1;border-bottom:1px solid #334155;margin:4px 0">💾 运行时数据上限（保存后立即执行状态压缩和请求日志清理；WSL 控制台日志由 watchdog 轮转器在 10 秒内读取新值）</div>
  <div style="color:#94a3b8;padding:4px 0">请求日志总容量</div>
  <div><input id="cfgLogMaxMiB" type="number" min="16" max="4096" style="width:72px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> MiB（所有 JSONL 分段合计）</div>
  <div style="color:#94a3b8;padding:4px 0">请求日志单段上限</div>
  <div><input id="cfgLogSegmentMaxMiB" type="number" min="1" max="256" style="width:72px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> MiB（超过后同日分段）</div>
  <div style="color:#94a3b8;padding:4px 0">状态小时统计保留</div>
  <div><input id="cfgStateHourlyRetentionDays" type="number" min="31" max="365" style="width:72px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 天</div>
  <div style="color:#94a3b8;padding:4px 0">状态日统计保留</div>
  <div><input id="cfgStateDailyRetentionDays" type="number" min="30" max="3650" style="width:72px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 天</div>
  <div style="color:#94a3b8;padding:4px 0">state.json 容量上限</div>
  <div><input id="cfgStateMaxMiB" type="number" min="4" max="256" style="width:72px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> MiB（超限时删除最旧统计桶）</div>
  <div style="color:#94a3b8;padding:4px 0">WSL proxy.log</div>
  <div><input id="cfgProxyLogMaxMiB" type="number" min="1" max="100" style="width:64px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> MiB / 保留 <input id="cfgProxyLogKeepFiles" type="number" min="1" max="20" style="width:52px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 个归档（systemd 使用 journald）</div>
  <div style="color:#94a3b8;padding:4px 0;grid-column:1/-1;border-bottom:1px solid #334155;margin:4px 0">🗄 Codex SQLite 日志维护</div>
  <div style="color:#94a3b8;padding:4px 0">启用数据库维护</div>
  <div><label><input type="checkbox" id="cfgCodexLogMaintenanceEnabled" onchange="toggleCodexLogMaintenanceControls()"> 达到容量阈值后短批次删除保留期外的 Codex 日志</label></div>
  <div style="color:#94a3b8;padding:4px 0">数据库路径</div>
  <div><input id="cfgCodexLogMaintenancePath" style="width:min(100%,420px);background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" placeholder="/root/.codex/logs_2.sqlite" title="填写 WSL 内部绝对路径，例如 /root/.codex/logs_2.sqlite；也可直接粘贴 WSL 网络路径，检测时会自动转换；不要填写 -wal/-shm 文件。"><button class="btn" id="cfgCodexLogMaintenanceCheckBtn" type="button" style="font-size:10px;padding:2px 7px;margin-left:6px" onclick="checkCodexLogMaintenancePath()">检测路径</button><span id="cfgCodexLogMaintenanceCheck" style="font-size:10px;color:#64748b;margin-left:6px"></span></div>
  <div style="color:#94a3b8;padding:4px 0">触发容量 / 保留时长</div>
  <div><input id="cfgCodexLogMaintenanceThreshold" data-codex-log-maintenance-control type="number" min="64" max="102400" style="width:72px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> MiB / 保留 <input id="cfgCodexLogMaintenanceRetain" data-codex-log-maintenance-control type="number" min="1" max="8760" style="width:64px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 小时</div>
  <div style="color:#94a3b8;padding:4px 0">检查间隔</div>
  <div><input id="cfgCodexLogMaintenanceInterval" data-codex-log-maintenance-control type="number" min="5" max="1440" style="width:64px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 分钟　<button class="btn" id="cfgCodexLogMaintenanceRunBtn" data-codex-log-maintenance-control type="button" style="font-size:10px;padding:2px 7px" onclick="runCodexLogMaintenanceNow()">立即检查</button><button class="btn" id="cfgCodexLogMaintenanceCleanBtn" data-codex-log-maintenance-control type="button" style="font-size:10px;padding:2px 7px;margin-left:6px" onclick="runCodexLogMaintenanceCleanNow()" title="仅在 Codex 空闲（无在途/排队请求且静默 60 秒）时执行；按已保存的触发容量/保留时长删除过期记录并 VACUUM 缩小库文件。需约等于库容量的临时磁盘空间。">立即清理</button></div>
  <div style="color:#94a3b8;padding:4px 0;grid-column:1/-1;border-bottom:1px solid #334155;margin:4px 0">日志事件中心（仅告警和人工处置，不会自动暂停分组、重启代理或修改 Key）</div>
  <div style="color:#94a3b8;padding:4px 0">启用日志事件</div>
  <div><label><input type="checkbox" id="cfgLogIncidentEnabled" checked> 触发失败/流失败规则　<input type="checkbox" id="cfgLogIncidentNotify"> 发送通知</label></div>
  <div style="color:#94a3b8;padding:4px 0">观察窗口 / 最低请求</div>
  <div><input id="cfgLogIncidentWindow" type="number" min="1" max="60" style="width:52px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 分钟 / <input id="cfgLogIncidentMinRequests" type="number" min="1" max="10000" style="width:58px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 次</div>
  <div style="color:#94a3b8;padding:4px 0">失败次数 / 失败率</div>
  <div><input id="cfgLogIncidentErrorBurst" type="number" min="1" max="10000" style="width:58px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 次 / <input id="cfgLogIncidentErrorRate" type="number" min="1" max="100" style="width:52px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> %</div>
  <div style="color:#94a3b8;padding:4px 0">流失败次数 / 默认静默</div>
  <div><input id="cfgLogIncidentStreamBurst" type="number" min="1" max="10000" style="width:58px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 次 / <input id="cfgLogIncidentSnooze" type="number" min="1" max="1440" style="width:58px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 分钟</div>
  <div style="color:#94a3b8;padding:4px 0">恢复判定</div>
  <div><input id="cfgLogIncidentResolve" type="number" min="1" max="120" style="width:58px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 分钟无异常后自动恢复</div>
  <div style="color:#94a3b8;padding:4px 0">延迟告警</div>
  <div><label><input type="checkbox" id="cfgLogIncidentLatency"> 启用 P95　请求 <input id="cfgLogIncidentP95" type="number" min="1000" max="1800000" style="width:76px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> ms　首字节 <input id="cfgLogIncidentP95Ttfb" type="number" min="100" max="300000" style="width:70px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> ms</label></div>
  <div style="color:#94a3b8;padding:4px 0">🔒 连续失败锁死阈值</div>
  <div><input id="cfgLockCount" type="number" min="1" max="20" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:60px" value="3" title="连续 N 次失败后自动锁死该 Key"> 次</div>
  <div style="color:#94a3b8;padding:4px 0">🎯 锁死监控错误码</div>
  <div><input id="cfgLockCodes" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100%" value="401,403" placeholder="401,403" title="只有这些错误码会计入连续失败计数"></div>
  <div style="color:#94a3b8;padding:4px 0">🔒 启用自动锁死</div>
  <div><label><input type="checkbox" id="cfgEnableAutoLock" checked> 开启后连续失败达到阈值将自动锁死 Key</label></div>
  <div style="color:#94a3b8;padding:4px 0;border-bottom:1px solid #334155;margin-bottom:4px;grid-column:1/-1">⏱ 分钟级限速</div>
  <div style="color:#94a3b8;padding:4px 0">每分钟请求上限</div>
  <div><input id="cfgMaxReqPerMin" type="number" min="1" max="1000" style="width:80px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" value="10"></div>
  <div style="color:#94a3b8;padding:4px 0">每分钟 Token 上限 (0=不限)</div>
  <div><input id="cfgMaxTokPerMin" type="number" min="0" max="9999999" style="width:120px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" value="0"></div>
  <div style="color:#94a3b8;padding:4px 0;border-bottom:1px solid #334155;margin-bottom:4px;grid-column:1/-1">⏱ 流超时</div>
  <div style="color:#94a3b8;padding:4px 0">其他协议流最大时长 (ms)</div>
  <div><input id="cfgStreamLifetime" type="number" min="60000" max="7200000" style="width:120px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" value="1800000" title="非 Responses / Messages 编码协议流的绝对超时，防止僵尸连接。默认30分钟"></div>
  <div style="color:#94a3b8;padding:4px 0">Responses / Messages 编码流最大总时长 (ms)</div>
  <div><input id="cfgResponsesStreamLifetime" type="number" min="0" max="86400000" style="width:120px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" value="0" title="0=不设置总时长硬切断；适用于 Codex Responses 与 Claude Messages。非零最少60秒，最多24小时。默认0"></div>
  <div style="color:#94a3b8;padding:4px 0">Responses / Messages 上游空闲超时 (ms)</div>
  <div><input id="cfgResponsesIdleTimeout" type="number" min="0" max="86400000" style="width:120px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" value="5400000" title="0=关闭；适用于 Codex Responses 与 Claude Messages。非零最少60秒，最多24小时。默认90分钟，只在上游完全无数据时触发"></div>
  <div style="color:#94a3b8;padding:4px 0;border-bottom:1px solid #334155;margin-bottom:4px;grid-column:1/-1">🔐 管理认证</div>
  <div style="color:#94a3b8;padding:4px 0">管理 Token（空=不校验）</div>
  <div><input id="cfgAdminToken" style="width:200px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" value="" placeholder="留空=无认证" title="设置后所有管理接口需 Bearer token 认证"></div>
  <div style="color:#94a3b8;padding:4px 0;border-bottom:1px solid #334155;margin-bottom:4px;grid-column:1/-1">🔌 端口分组管理</div>
  <div style="grid-column:1/-1" id="portGroupsArea"></div>
  <div style="color:#94a3b8;padding:4px 0;border-bottom:1px solid #334155;margin-bottom:4px;grid-column:1/-1">🔎 任务洞察（代理流水解析/提炼）</div>
  <div style="color:#94a3b8;padding:4px 0">启用任务洞察</div>
  <div><label><input type="checkbox" id="cfgTaskInsightEnabled"> 记录代理流水任务（默认不落盘原文，仅结构化信号）</label></div>
  <div style="color:#94a3b8;padding:4px 0">采集信号</div>
  <div><label><input type="checkbox" id="cfgTaskInsightInstructions" title="记录截断指令前 200 字"> 截断指令（前 200 字）</label><br><label><input type="checkbox" id="cfgTaskInsightTools" title="仅工具名与文件路径，不存参数全文"> 工具/文件路径</label><br><label><input type="checkbox" id="cfgTaskInsightUsage" title="输入输出 token 与估算费用"> 用量与费用</label><br><label><input type="checkbox" id="cfgTaskInsightCorrelate" title="使用 autoResume 活跃窗口合并同一任务的连续会话"> 关联会话（45 分钟活跃窗口）</label></div>
  <div style="color:#94a3b8;padding:4px 0">保留天数</div>
  <div><input id="cfgTaskInsightRetention" type="number" min="1" max="365" style="width:72px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 天</div>
  <div style="color:#94a3b8;padding:4px 0;border-bottom:1px solid #334155;margin:4px 0;grid-column:1/-1">🤖 LLM 蒸馏摘要（可选，发送时仅含结构化快照，绝不含 Key）</div>
  <div style="color:#94a3b8;padding:4px 0">启用蒸馏</div>
  <div><label><input type="checkbox" id="cfgTaskInsightDistillEnabled"> 定期为已完成任务生成结构化摘要</label></div>
  <div style="color:#94a3b8;padding:4px 0">蒸馏引擎</div>
  <div><select id="cfgTaskInsightDistillEngine" onchange="taskInsightEngineChanged()" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 5px;border-radius:4px">
    <option value="ollama">ollama（本机）</option>
    <option value="proxy">proxy（走代理）</option>
    <option value="external">external（外部 API）</option>
  </select>
  <div id="cfgTaskInsightEngineHint" style="font-size:10px;color:#64748b;margin-top:4px"></div></div>
  <div style="color:#94a3b8;padding:4px 0">模型 / 地址</div>
  <div><input id="cfgTaskInsightDistillModel" style="width:min(100%,280px);background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:3px 5px;border-radius:4px" placeholder="qwen3:4b / gpt-5"><br><input id="cfgTaskInsightDistillBaseUrl" style="width:min(100%,420px);margin-top:4px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:3px 5px;border-radius:4px" placeholder="http://127.0.0.1:11434/v1（仅 ollama 需要）" title="ollama 与 external 引擎使用此地址；proxy 引擎忽略（走代理本身）"></div>
  <div style="color:#94a3b8;padding:4px 0">每日预算 / 报告</div>
  <div><input id="cfgTaskInsightDistillBudget" type="number" min="0" max="10000" step="0.01" style="width:72px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px"> 元（0=不限）　<select id="cfgTaskInsightDistillReport" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 5px;border-radius:4px"><option value="daily">日报</option><option value="weekly">周报</option></select></div>
  <div style="grid-column:1/-1;color:#64748b;font-size:10px" id="cfgTaskInsightDistillStatus">蒸馏: --</div>
</div>
<div style="font-size:11px;color:#64748b;margin-bottom:4px" id="cfgAutoCountdown">⏳ 下次检测（间隔）: --</div>
<div style="font-size:11px;color:#64748b;margin-bottom:4px" id="cfgAutoDailyCountdown">⏳ 下次检测（固定）: --</div>
<div style="font-size:11px;color:#64748b;margin-bottom:4px" id="cfgAutoPollCountdown">⏳ 下次检测（快速）: --</div>
<div style="font-size:11px;color:#22c55e;margin-bottom:8px" id="cfgAutoResumeStatus">🧬 闲置恢复: --</div>
<div style="font-size:11px;color:#64748b;margin-bottom:8px" id="cfgCodexLogMaintenanceRuntime">🗄 Codex SQLite 日志维护: --</div>
  <div class="mfoot"><button class="btn" id="restartProxyBtn" onclick="restartProxy()" style="color:#f87171">🔄 重启代理</button><div style="flex:1"></div><button class="btn btn-p" onclick="saveConfig()">保存</button></div>
</div></div>

<div class="modal" id="taskInsightModal">
<div class="mcontent" style="max-width:960px">
<div class="mtitle"><span>📊 任务流水（代理流水解析/提炼）</span><button class="btn" type="button" onclick="closeTaskInsight()">✕</button></div>
<div id="taskInsightDisabled" style="display:none;padding:14px 12px;background:#1e293b;border:1px solid #475569;border-radius:6px;font-size:12px;color:#cbd5e1;line-height:1.7">
  任务洞察当前未启用，暂无流水记录。<br>
  <button class="btn btn-p" style="margin-top:8px;font-size:11px" onclick="closeTaskInsight();openConfig()">去「配置 → 🔎 任务洞察」开启</button>
</div>
<div id="taskInsightPanel" style="display:none">
  <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:8px;font-size:11px;color:#94a3b8">
    <span id="taskInsightStats" style="color:#e2e8f0"></span>
    <span style="flex:1"></span>
    <label>项目 <select id="taskInsightProjectFilter" onchange="loadTaskInsight()" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 5px;border-radius:4px"><option value="">全部</option><option value="__unclassified__">未分类</option></select></label>
    <label>状态 <select id="taskInsightStatusFilter" onchange="loadTaskInsight()" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 5px;border-radius:4px"><option value="">全部</option><option value="completed">完成</option><option value="failed">失败</option><option value="partial">部分</option></select></label>
    <label>搜索 <input id="taskInsightSearch" onkeydown="if(event.key==='Enter')loadTaskInsight()" placeholder="项目/客户端/工具/文件/模型" style="width:150px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 5px;border-radius:4px"></label>
    <button class="btn" style="font-size:10px;padding:2px 7px" onclick="loadTaskInsight(true)">🔄 刷新</button>
    <button class="btn" style="font-size:10px;padding:2px 7px" onclick="taskInsightExport()">⬇ CSV</button>
    <button class="btn" style="font-size:10px;padding:2px 7px" onclick="taskInsightReport()">📊 报告</button>
    <button class="btn" style="font-size:10px;padding:2px 7px;color:#4ade80" id="taskInsightDistillBtn" onclick="taskInsightDistillNow()">🤖 立即蒸馏</button>
  </div>
  <div style="font-size:11px;color:#64748b;margin-bottom:8px" id="taskInsightHint"></div>
  <div id="taskInsightReportBox" style="display:none;margin-bottom:8px;padding:8px 10px;background:#0f172a;border:1px solid #334155;border-radius:4px;font-size:11px;color:#cbd5e1"></div>
  <div id="taskInsightTable" style="font-size:11px;max-height:62vh;overflow:auto"></div>
</div>
</div></div>


<div class="modal" id="updateModal">
<div class="mcontent" style="max-width:720px">
<div class="mtitle"><span>版本更新</span><button class="btn" type="button" onclick="closeUpdateModal()">✕</button></div>
<div style="display:flex;gap:16px;flex-wrap:wrap;margin:0 0 10px;padding:8px 10px;background:#0f172a;border:1px solid #334155;border-radius:4px;font-size:12px;color:#94a3b8">
  <span>本机版本：<strong id="updateCurrentVersion" style="color:#e2e8f0">…</strong></span>
  <span>最新正式 Release：<strong id="updateLatestVersion" style="color:#e2e8f0">…</strong></span>
  <span id="updateVersionGap" style="color:#94a3b8"></span>
</div>
<div id="updateSummary" style="font-size:13px;color:#cbd5e1;line-height:1.6">正在读取 GitHub Release 信息…</div>
<pre class="update-notes" id="updateReleaseNotes">正在读取更新说明…</pre>
<div id="updateSafety" style="margin-top:10px;padding:9px 10px;background:#3b2f1e;border:1px solid #a16207;border-radius:4px;color:#fde68a;font-size:12px;line-height:1.6"></div>
<div class="mfoot">
  <a class="btn" id="updateReleaseLink" href="https://github.com/aipayim/codex-proxy/releases" target="_blank" rel="noopener noreferrer">在 GitHub 查看 Release ↗</a>
  <button class="btn" id="updateRefreshBtn" type="button" onclick="checkForUpdates(true)">↻ 重新检查</button>
  <button class="btn" id="updateUpgradeBtn" type="button" disabled title="自动覆盖可能丢失本地修改，因此已禁用。">一键升级已禁用</button>
  <button class="btn btn-p" type="button" onclick="closeUpdateModal()">关闭</button>
</div>
</div></div>

<div class="modal" id="logModal">
<div class="mcontent" style="max-width:1100px">
<div class="mtitle"><span>请求日志与运行事件</span><span style="display:flex;gap:6px"><button class="btn" id="logRebuildBtn" onclick="rebuildLogSummary()" title="后台重建历史日汇总，不读取或阻塞代理请求">重建汇总</button><button class="btn" onclick="closeLogs()">✕</button></span></div>
<div id="logStats" style="display:flex;gap:6px;margin-bottom:8px;flex-wrap:wrap">
  <div class="log-stat-card" style="background:#1e293b;border-radius:6px;padding:6px 10px;min-width:60px;text-align:center">
    <div style="font-size:10px;color:#94a3b8">总请求</div>
    <div id="lsTotal" style="font-size:16px;font-weight:700;color:#e2e8f0">-</div>
  </div>
  <div class="log-stat-card" style="background:#1e293b;border-radius:6px;padding:6px 10px;min-width:60px;text-align:center">
    <div style="font-size:10px;color:#94a3b8">成功率</div>
    <div id="lsSuccess" style="font-size:16px;font-weight:700;color:#22c55e">-</div>
  </div>
  <div class="log-stat-card" style="background:#1e293b;border-radius:6px;padding:6px 10px;min-width:60px;text-align:center">
    <div style="font-size:10px;color:#94a3b8">平均耗时</div>
    <div id="lsAvgDur" style="font-size:16px;font-weight:700;color:#e2e8f0">-</div>
  </div>
  <div class="log-stat-card" style="background:#1e293b;border-radius:6px;padding:6px 10px;min-width:60px;text-align:center">
    <div style="font-size:10px;color:#94a3b8">P95</div>
    <div id="lsP95" style="font-size:16px;font-weight:700;color:#f59e0b">-</div>
  </div>
  <div class="log-stat-card" style="background:#1e293b;border-radius:6px;padding:6px 10px;min-width:60px;text-align:center">
    <div style="font-size:10px;color:#94a3b8">P99</div>
    <div id="lsP99" style="font-size:16px;font-weight:700;color:#f97316">-</div>
  </div>
  <div class="log-stat-card" style="background:#1e293b;border-radius:6px;padding:6px 10px;min-width:50px;text-align:center">
    <div style="font-size:10px;color:#94a3b8">4xx</div>
    <div id="ls4xx" style="font-size:16px;font-weight:700;color:#f59e0b">-</div>
  </div>
  <div class="log-stat-card" style="background:#1e293b;border-radius:6px;padding:6px 10px;min-width:50px;text-align:center">
    <div style="font-size:10px;color:#94a3b8">5xx</div>
    <div id="ls5xx" style="font-size:16px;font-weight:700;color:#ef4444">-</div>
  </div>
  <div class="log-stat-card" style="background:#1e293b;border-radius:6px;padding:6px 10px;min-width:50px;text-align:center">
    <div style="font-size:10px;color:#94a3b8">超时</div>
    <div id="lsTimeout" style="font-size:16px;font-weight:700;color:#64748b">-</div>
  </div>
</div>
<div id="logSparklineWrap" class="log-sparkline-wrap" style="margin-bottom:2px" title="最近 30 分钟请求量趋势（蓝色=成功，红色=错误）"></div>
<div id="logModelDist" class="log-model-row"></div>
<div id="logErrorCluster" class="log-error-cluster" onclick="toggleErrorCluster()"><span class="head">⚠ 错误分布</span><div class="body" id="logErrorBody"></div></div>
<div id="logIncidentPanel" style="display:none;border-top:1px solid #334155;margin:6px 0;padding-top:6px">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;font-size:11px"><span style="color:#fbbf24;font-weight:600">运行事件 <span id="logIncidentStatus" style="color:#94a3b8;font-weight:400"></span></span><span id="logIncidentRefreshStatus" aria-live="polite" style="color:#94a3b8;font-size:10px;flex:1;min-width:90px"></span><button class="btn" id="logIncidentRefreshBtn" type="button" style="font-size:10px;padding:2px 7px" onclick="refreshLogOperations({interactive:true})" title="重新读取当前运行事件">刷新事件</button></div>
  <div id="logIncidentList" style="display:grid;gap:4px;margin-top:5px"></div>
  <div id="logGroupPauseList" style="display:flex;flex-wrap:wrap;gap:4px;margin-top:5px"></div>
</div>
<div class="log-filters" style="display:flex;gap:6px;margin-bottom:6px;flex-wrap:wrap;align-items:center">
<input id="logKeyFilter" placeholder="Key #" style="width:50px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
<input id="logStatusFilter" placeholder="状态码" style="width:60px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
<input id="logModelFilter" placeholder="模型" style="width:100px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
<input id="logUpstreamFilter" placeholder="上游域名" style="width:100px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
<input id="logPathFilter" placeholder="路径" style="width:82px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
<input id="logGroupFilter" placeholder="分组" style="width:48px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
<input id="logSearch" placeholder="搜索..." style="width:90px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px" onkeydown="if(event.key==='Enter')reloadLogs()">
<select id="logTimeFilter" onchange="toggleLogCustomRange()" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
<option value="">全部时间</option><option value="5m">最近 5 分钟</option><option value="15m">最近 15 分钟</option><option value="1h">最近 1 小时</option><option value="24h">最近 24 小时</option><option value="7d">最近 7 天</option><option value="30d">最近 30 天</option><option value="custom">自定义范围</option>
</select>
<span id="logCustomRange" style="display:none">
<input type="datetime-local" id="logStartTime" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px;width:160px">
<span style="color:#94a3b8;font-size:11px"> ~ </span>
<input type="datetime-local" id="logEndTime" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px;width:160px">
</span>
<button class="btn" onclick="reloadLogs()" style="font-size:11px;padding:2px 8px">🔍 搜索</button>
<button class="btn" onclick="exportLogs()" style="font-size:11px;padding:2px 8px">⬇ CSV</button>
<span id="logQueryHint" style="color:#64748b;font-size:10px"></span>
</div>
<div style="overflow-x:auto;max-height:500px;overflow-y:auto"><table class="log-table"><thead><tr>
<th style="width:30px;font-size:10px;text-align:center">序</th>
<th onclick="logSortBy('time')" style="cursor:pointer">时间<span id="logSortIcon" style="color:#64748b;font-size:8px;margin-left:2px"></span></th>
<th onclick="logSortBy('idx')" style="cursor:pointer">#<span id="logSortIcon_idx" style="color:#64748b;font-size:8px;margin-left:2px"></span></th>
<th>上游</th><th>方法</th><th onclick="logSortBy('model')" style="cursor:pointer">模型<span id="logSortIcon_model" style="color:#64748b;font-size:8px;margin-left:2px"></span></th>
<th>路径</th><th onclick="logSortBy('status')" style="cursor:pointer">状态<span id="logSortIcon_status" style="color:#64748b;font-size:8px;margin-left:2px"></span></th>
<th>↑B</th><th>↓B</th><th onclick="logSortBy('dur')" style="cursor:pointer">耗时<span id="logSortIcon_dur" style="color:#64748b;font-size:8px;margin-left:2px"></span></th>
<th>首字节</th>
</tr></thead><tbody id="logBody"></tbody></table></div>
<div id="logPagination" style="display:flex;justify-content:center;align-items:center;gap:8px;padding:6px 0;font-size:12px">
  <button class="btn" id="logPrevBtn" type="button" onclick="logPage(-1)" style="font-size:11px;padding:2px 10px" title="浏览历史后可按页返回" disabled>← 上一页</button>
  <span id="logPageInfo" style="color:#94a3b8">最新 50 条</span>
  <button class="btn" id="logBrowseHistoryBtn" type="button" onclick="toggleLogHistory()" style="font-size:11px;padding:2px 8px" title="切换为可分页的历史日志视图">浏览历史</button>
  <button class="btn" id="logNextBtn" type="button" onclick="logPage(1)" style="font-size:11px;padding:2px 10px" title="浏览历史后可翻到更早记录" disabled>下一页 →</button>
  <span style="color:#64748b;font-size:10px;margin-left:8px" id="logRealTimeBadge">● 未订阅</span>
</div>
</div></div>

<div id="logKeyPopup" class="log-key-popup" style="display:none" onclick="event.stopPropagation()">
  <span class="close" onclick="closeLogKeyPopup()">✕</span>
  <div class="title" id="logKeyPopupTitle">Key #- 统计</div>
  <div id="logKeyPopupBody"></div>
</div>

<div class="restart-overlay" id="restartOverlay" role="status" aria-live="polite">
  <div class="restart-panel">
    <div class="restart-spinner" aria-hidden="true"></div>
    <div class="restart-title" id="restartOverlayTitle">正在重启代理</div>
    <div class="restart-detail" id="restartOverlayDetail"></div>
    <div class="restart-elapsed" id="restartOverlayElapsed"></div>
    <div id="restartOverlayActions" style="display:none;justify-content:center;gap:8px;flex-wrap:wrap;margin-top:14px">
      <button class="btn" id="restartCancelBtn" type="button" onclick="cancelPendingRestart()">取消重启</button>
      <button class="btn" id="restartForceBtn" type="button" onclick="forcePendingRestart()" style="display:none;color:#fecaca;border-color:#ef4444">强制重启</button>
    </div>
    <button class="btn" id="restartOverlayDismiss" type="button" style="display:none;margin-top:14px">返回 Dashboard</button>
  </div>
</div>

<script>
const __adminTokenState=(()=>{
  let token=null;
  return {
    get(){return token;},
    set(value){token=typeof value==="string"&&value.trim()?value.trim():null;},
    clear(){token=null;}
  };
})();
try{sessionStorage.removeItem("adminToken");}catch(e){}
const __origFetch=window.fetch;
window.fetch=function(u,o){
  o=o||{};
  const t=__adminTokenState.get();
  if(t){
    let isLocal=false;
    if(typeof u==="string"){
      if(u.startsWith("/__")){isLocal=true;}
      else{try{const url=new URL(u,location.href);if(url.origin===location.origin&&url.pathname.startsWith("/__"))isLocal=true;}catch(e){}}
    }
    if(isLocal){
      const h=o.headers instanceof Headers?o.headers:new Headers(o.headers||{});
      h.set("Authorization","Bearer "+t);
      o.headers=h;
    }
  }
  return __origFetch(u,o);
};
function requireAdminToken(){
  return new Promise((resolve)=>{
    const promptForToken=()=>{
      const d=document.createElement("div");
      d.id="tokenDialog";
      d.style.cssText="position:fixed;inset:0;background:rgba(0,0,0,0.85);display:flex;align-items:center;justify-content:center;z-index:9999";
      d.innerHTML='<div style="background:#1e293b;border:1px solid #475569;border-radius:8px;padding:24px;max-width:360px;text-align:center"><div style="color:#e2e8f0;font-size:16px;margin-bottom:12px">🔐 管理员认证</div><div style="color:#94a3b8;font-size:13px;margin-bottom:16px">请输入管理 Token 以访问 Dashboard</div><input id="tokenInput" type="password" autocomplete="off" style="width:100%;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:8px 12px;border-radius:4px;margin-bottom:12px" placeholder="管理 Token"><div id="tokenErr" style="color:#f87171;font-size:12px;margin-bottom:8px;display:none"></div><button id="tokenConfirmBtn" type="button" style="background:#3b82f6;color:#fff;border:none;padding:8px 24px;border-radius:4px;cursor:pointer">确认</button></div>';
      document.body.appendChild(d);
      const tokenInput=document.getElementById("tokenInput");
      const tokenErr=document.getElementById("tokenErr");
      const submitToken=()=>{
        const v=tokenInput.value.trim();
        if(!v){tokenErr.textContent="请输入 Token";tokenErr.style.display="block";return;}
        __adminTokenState.set(v);
        __origFetch("/__status",{headers:{"Authorization":"Bearer "+v}}).then(r=>{
          if(r.ok){tokenInput.value="";d.remove();resolve(true);}
          else{tokenErr.textContent="Token 错误";tokenErr.style.display="block";__adminTokenState.clear();}
        }).catch(()=>{tokenErr.textContent="连接失败";tokenErr.style.display="block";});
      };
      document.getElementById("tokenConfirmBtn").addEventListener("click",submitToken);
      tokenInput.focus();
      tokenInput.addEventListener("keydown",e=>{if(e.key==="Enter")submitToken();});
    };
    __origFetch("/__auth_check").then(r=>r.json()).then(j=>{
      if(!j.configured){__adminTokenState.clear();resolve(true);return;}
      const saved=__adminTokenState.get();
      if(saved){
        __origFetch("/__status",{headers:{"Authorization":"Bearer "+saved}}).then(r=>{
          if(r.ok){resolve(true);return;}
          __adminTokenState.clear();
          promptForToken();
        }).catch(()=>resolve(false));
        return;
      }
      promptForToken();
    }).catch(()=>resolve(true));
  });
}
const L={"daily":"每日","weekly":"每周","never":"永久","hourly":"每N小时"};
const C={"daily":"bd-daily","weekly":"bd-weekly","never":"bd-never","hourly":"bd-hourly"};
const DAY_CN={"1":"周一","2":"周二","3":"周三","4":"周四","5":"周五","6":"周六","7":"周日"};
function daysUntilResetClient(resetDay) {
  if (resetDay == null) return 99;
  const jsDay = new Date().getDay();
  const isoDay = jsDay === 0 ? 7 : jsDay;
  const target = parseInt(resetDay);
  return (target - isoDay + 7) % 7 || 7;
}
let data=[],curDate="",fullKeys={},filtered=[];
let sortBy="idx",filterBy="all",trendRange="24h",trendMode="model",searchQ="",statusCodeQ="",modelSQ="",groupFilter="all";
let ws=null,wsReconnectTimer=null,pollTimer=null;
let wsFailed=false;
let autoRecoverNextTime=0,autoRecoverDailyNextTime=0,autoRecoverPollNextTime=0;
let lastRequestTime=0,lastKeyUseTime=0,lastResumeTime=0;
let collapsedCards={};
let updateInfo=null;
const UPDATE_UI_RECHECK_MS=30*60*1000;
const UNKNOWN_LOCAL_BUILD_LABEL="本地开发/定制版本（未能验证发布基线）";
boostedBatch=[];boostedBatchMode="";

function formatUpdateCheckTime(value){
  if(!value)return "尚未检查";
  const d=new Date(value);
  if(isNaN(d.getTime()))return "刚刚检查";
  return String(d.getHours()).padStart(2,"0")+":"+String(d.getMinutes()).padStart(2,"0");
}
function formatUpdatePublishedAt(value){
  if(!value)return "";
  const d=new Date(value);
  if(isNaN(d.getTime()))return "";
  return d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0")+"-"+String(d.getDate()).padStart(2,"0")+" "+String(d.getHours()).padStart(2,"0")+":"+String(d.getMinutes()).padStart(2,"0");
}
function formatIdle(ms){
  if(ms<0)ms=0;
  const totalSec=ms/1000;
  const d=Math.floor(totalSec/86400);
  const h=Math.floor((totalSec%86400)/3600);
  const m=Math.floor((totalSec%3600)/60);
  const s=totalSec%60;
  let parts=[];
  if(d>0)parts.push(d+"d");
  if(h>0)parts.push(h+"h");
  if(m>0||d>0||h>0)parts.push(m+"m");
  parts.push(s.toFixed(2)+"s");
  return parts.join(" ");
}
function describeBuildReason(reason){
  const map={
    "release-metadata-missing":"缺少发布元数据（build-info.json/release-manifest.json）",
    "release-metadata-invalid":"发布元数据无效",
    "release-manifest-mismatch":"发布清单摘要不匹配",
    "release-manifest-invalid":"发布清单无效",
    "release-manifest-incomplete":"发布清单不完整",
    "release-files-modified":"本地文件与官方发布清单不一致（可能被修改）",
    "git-unavailable":"无法识别 Git 仓库",
    "git-remote-untrusted":"Git 远程不是官方仓库",
    "git-worktree-modified":"Git 工作树有改动",
    "git-not-at-release-tag":"Git 不在正式 Release Tag",
    "git-status-unavailable":"无法读取 Git 状态",
    "source-baseline-missing":"缺少源码基线文件（release-baseline.txt）",
    "source-baseline-invalid":"源码基线版本号无效",
  };
  return map[reason]||("未知（"+String(reason||"unknown")+"）");
}
function renderUpdateInfo(){
  const current=updateInfo&&updateInfo.current;
  const latest=updateInfo&&updateInfo.latest;
  const currentLabel=(current&&current.label)||UNKNOWN_LOCAL_BUILD_LABEL;
  const currentComparable=!!(current&&current.comparable);
  const provenanceLabel=(current&&current.provenanceLabel)||"本地来源未验证";
  const currentEl=document.getElementById("cfgCurrentVersion");
  if(currentEl)currentEl.textContent=currentLabel;
  const provenanceEl=document.getElementById("cfgBuildProvenance");
  if(provenanceEl)provenanceEl.textContent="来源："+provenanceLabel;
  const latestEl=document.getElementById("cfgLatestRelease");
  if(latestEl)latestEl.textContent=latest?latest.tag:"未知";
  const gapEl=document.getElementById("cfgVersionGap");
  if(gapEl){
    if(latest&&current&&current.comparable){
      if(updateInfo.updateAvailable)gapEl.textContent="（可升级至 "+latest.tag+"）";
      else gapEl.textContent="（已是最新）";
    }else if(latest){
      gapEl.textContent="（无法判断差距）";
    }else{
      gapEl.textContent="";
    }
  }
  const badge=document.getElementById("updateBadge");
  const hasUpdate=!!(updateInfo&&updateInfo.updateAvailable&&latest);
  if(badge){
    badge.classList.remove("on","neutral");
    if(hasUpdate)badge.classList.add("on");
    else if(!currentComparable)badge.classList.add("neutral");
    if(hasUpdate)badge.title="发现 "+latest.tag+"，点击查看 Release 说明与安全升级方法";
    else if(currentComparable)badge.title="当前没有可升级的正式 Release";
    else badge.title="无法判断本机版本基线（"+describeBuildReason(current&&current.reason)+"）。点击查看 GitHub 最新 Release。";
  }
  const status=document.getElementById("cfgUpdateStatus");
  if(status){
    if(!updateInfo)status.textContent="正在检查 GitHub Release…";
    else if(updateInfo.lastError&&!latest)status.textContent="检查失败："+updateInfo.lastError;
    else if(latest&&!currentComparable)status.textContent="基线未知（"+describeBuildReason(current&&current.reason)+"）；仅展示 Release（检查于 "+formatUpdateCheckTime(updateInfo.checkedAt)+"）";
    else if(hasUpdate)status.textContent="发现新版本 "+latest.tag+"（已检查 "+formatUpdateCheckTime(updateInfo.checkedAt)+"）";
    else status.textContent="已确认基线不低于最新 Release（检查于 "+formatUpdateCheckTime(updateInfo.checkedAt)+"）";
  }
  renderUpdateModal();
}
function renderUpdateModal(){
  const modal=document.getElementById("updateModal");
  if(!modal||!modal.classList.contains("on"))return;
  const summary=document.getElementById("updateSummary");
  const notes=document.getElementById("updateReleaseNotes");
  const safety=document.getElementById("updateSafety");
  const link=document.getElementById("updateReleaseLink");
  const currentVersionEl=document.getElementById("updateCurrentVersion");
  const latestVersionEl=document.getElementById("updateLatestVersion");
  const gapEl=document.getElementById("updateVersionGap");
  if(!updateInfo){
    summary.textContent="正在读取 GitHub Release 信息…";
    notes.textContent="正在读取更新说明…";
    safety.textContent="自动覆盖升级不会在后台执行。";
    if(currentVersionEl)currentVersionEl.textContent="…";
    if(latestVersionEl)latestVersionEl.textContent="…";
    if(gapEl)gapEl.textContent="";
    return;
  }
  const currentInfo=updateInfo.current||{};
  const current=currentInfo.label||UNKNOWN_LOCAL_BUILD_LABEL;
  const currentComparable=currentInfo.comparable===true;
  const provenanceLabel=currentInfo.provenanceLabel||"本地来源未验证";
  const latest=updateInfo.latest;
  if(currentVersionEl)currentVersionEl.textContent=current;
  if(latestVersionEl)latestVersionEl.textContent=latest?latest.tag:"—";
  if(gapEl){
    if(latest&&currentComparable)gapEl.textContent=updateInfo.updateAvailable?"（可升级至 "+latest.tag+"）":"（已是最新）";
    else gapEl.textContent="";
  }
  if(latest){
    const published=formatUpdatePublishedAt(latest.publishedAt);
    const releaseMeta=published?" 发布于 "+published+"。":"";
    summary.textContent=!currentComparable
      ? "本地构建来源未验证（"+provenanceLabel+"）。最新正式 Release："+latest.tag+"。该信息仅供查看，不判断本机是否需要升级。"+releaseMeta
      : updateInfo.updateAvailable
        ? "发现可升级版本："+latest.tag+"。当前："+current+"。"+releaseMeta
        : "当前："+current+"；最新正式 Release："+latest.tag+"。"+releaseMeta;
    notes.textContent=latest.notes||"该 Release 未提供文字说明。";
    link.href=latest.url||"https://github.com/aipayim/codex-proxy/releases";
  }else{
    summary.textContent="暂时无法读取 GitHub Release 信息。";
    notes.textContent=updateInfo.lastError||"请稍后重新检查，或直接在 GitHub 查看 Release。";
    link.href="https://github.com/aipayim/codex-proxy/releases";
  }
  safety.textContent="官方发布包会通过随包构建元数据和文件清单自动识别；源码安装会通过 release-baseline.txt 记录的本机版本基线自动识别；官方 Git 工作树只有在干净且正好位于正式 Tag 时才自动识别。当前构建可能包含本地修改，因此自动覆盖式一键升级仍禁用。定制构建如需比较，可在系统配置填写已人工确认的正式 Release Tag；否则只查看 Release。升级前备份当前代理目录及配置/状态，审核变更、执行 node -c proxy.js 后，再于维护窗口重启代理。";
}
async function checkForUpdates(force){
  const refresh=document.getElementById("updateRefreshBtn");
  if(force&&refresh){refresh.disabled=true;refresh.textContent="检查中…";}
  try{
    const r=await fetch("/__update-status"+(force?"?refresh=1":""),{cache:"no-store"});
    const result=await r.json();
    if(!result||typeof result!=="object")throw new Error("更新检查返回格式无效");
    updateInfo=result;
  }catch(e){
    updateInfo={current:{label:UNKNOWN_LOCAL_BUILD_LABEL,comparable:false,provenanceLabel:"本地来源未验证"},lastError:e.message||"更新检查失败"};
  }finally{
    renderUpdateInfo();
    if(force&&refresh){refresh.disabled=false;refresh.textContent="↻ 重新检查";}
  }
}
function startUpdateChecks(){
  checkForUpdates(false);
  if(!window.updateCheckTimer)window.updateCheckTimer=setInterval(function(){checkForUpdates(false);},UPDATE_UI_RECHECK_MS);
}
function openUpdateModal(){
  document.getElementById("updateModal").classList.add("on");
  renderUpdateModal();
  checkForUpdates(false);
}
function closeUpdateModal(){document.getElementById("updateModal").classList.remove("on");}

async function httpLoad(){
  try{
    const r=await fetch("http://localhost:3456/__status");
    if(!r.ok)throw new Error("HTTP "+r.status);
    const j=await r.json();data=j.keys||j;boostedIdx=j.boostedIdx||-1;boostedBatch=j.boostedBatch||[];boostedBatchMode=j.boostedBatchMode||"";if(j.lastRequestTime)lastRequestTime=j.lastRequestTime;if(j.lastKeyUseTime)lastKeyUseTime=j.lastKeyUseTime;if(j.lastResumeTime)lastResumeTime=j.lastResumeTime;render();
  }catch(e){
    if(!wsFailed)document.getElementById("subText").textContent="连接失败，正在重试...";
  }
}

function connectWS(){
  wsFailed=false;
  const t=__adminTokenState.get();
  ws=new WebSocket("ws://localhost:3456"+(t?"?token="+encodeURIComponent(t):""));
  ws.onopen=function(){logSubscriptionEnabled=false;if(document.getElementById("logModal").classList.contains("on"))setLogSubscription(true)};
  ws.onmessage=function(e){
    try{
      const msg=JSON.parse(e.data);
      if(msg.type==="status"){data=msg.data;boostedIdx=msg.boostedIdx||-1;boostedBatch=msg.boostedBatch||[];boostedBatchMode=msg.boostedBatchMode||"";if(msg.lastRequestTime)lastRequestTime=msg.lastRequestTime;if(msg.lastKeyUseTime)lastKeyUseTime=msg.lastKeyUseTime;if(msg.lastResumeTime)lastResumeTime=msg.lastResumeTime;render()}
      if(msg.type==="notification"&&msg.notificationType==="all_keys_failed"){showAlert("所有 Key 均不可用！");playAlert();sendDesktop()}
      if(msg.type==="notification"&&msg.notificationType==="log_incident"){const incident=msg.incident||{};const message="日志事件："+(incident.title||"检测到异常")+(incident.scope&&incident.scope.value?"（"+incident.scope.value+"）":"");showAlert(message);playAlert();sendDesktop(message)}
      if(msg.type==="log"&&document.getElementById("logModal").classList.contains("on"))enqueueLiveLog(msg.data);
      if(msg.type==="incidents")applyLogIncidentData(msg.data||{});
    }catch(e){}
  };
  ws.onclose=function(e){
    wsFailed=true;
    logSubscriptionEnabled=false;
    if(pollTimer)clearInterval(pollTimer);
    if(e&&e.code===4001){
      __adminTokenState.clear();
      if(wsReconnectTimer){clearTimeout(wsReconnectTimer);wsReconnectTimer=null;}
      requireAdminToken().then(ok=>{if(ok)connectWS()});
      return;
    }
    pollTimer=setInterval(httpLoad,5000);
    httpLoad();
    wsReconnectTimer=setTimeout(connectWS,5000);
  };
  ws.onerror=function(){ws.close()};
}

function playAlert(){try{var a=new AudioContext(),o=a.createOscillator(),g=a.createGain();o.type="sine";o.frequency.value=880;g.gain.value=.3;o.connect(g);g.connect(a.destination);o.start();o.stop(a.currentTime+.3)}catch(e){}}
function sendDesktop(message){try{if(config.notifications.desktop!==false&&Notification.permission==="granted")new Notification("Codex Proxy",{body:message||"所有 Key 已不可用！",icon:""})}catch(e){}}

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
    renderModelPricingRules(c.modelPricing);
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
    document.getElementById("cfgAutoRecoverPoll").checked=c.autoRecoverPoll===true;
    document.getElementById("cfgAutoRecoverPollInterval").value=c.autoRecoverPollInterval||5;
    document.getElementById("cfgAutoRecoverPollCodes").value=(c.autoRecoverPollCodes||[500,502,503,504]).join(",");
    document.getElementById("cfgAutoRecoverDelays").value=(Array.isArray(c.autoRecoverDelays)?c.autoRecoverDelays:[800]).join(",");
    document.getElementById("cfgRoundRobin").checked=c.roundRobin===true;
    document.getElementById("cfgWeeklySortBy").checked=c.weeklySortBy==="expiry";
    document.getElementById("cfgLockCount").value=c.lockAfterFailCount||3;
    document.getElementById("cfgLockCodes").value=(c.lockFailCodes||["401","403"]).join(",");
    document.getElementById("cfgLogFile").checked=c.logFile!==false;
    document.getElementById("cfgLogRetention").value=c.logRetentionDays??7;
    document.getElementById("cfgLogDetail").value=c.logDetail||"full";
    document.getElementById("cfgLogMaxMiB").value=c.logMaxMiB??256;
    document.getElementById("cfgLogSegmentMaxMiB").value=c.logSegmentMaxMiB??16;
    document.getElementById("cfgStateHourlyRetentionDays").value=c.stateHourlyRetentionDays??35;
    document.getElementById("cfgStateDailyRetentionDays").value=c.stateDailyRetentionDays??180;
    document.getElementById("cfgStateMaxMiB").value=c.stateMaxMiB??32;
    document.getElementById("cfgProxyLogMaxMiB").value=c.proxyLogMaxMiB??10;
    document.getElementById("cfgProxyLogKeepFiles").value=c.proxyLogKeepFiles??5;
    const codexLogMaintenance=c.codexLogMaintenance||{};
    document.getElementById("cfgCodexLogMaintenanceEnabled").checked=codexLogMaintenance.enabled===true;
    document.getElementById("cfgCodexLogMaintenancePath").value=codexLogMaintenance.dbPath||"/root/.codex/logs_2.sqlite";
    document.getElementById("cfgCodexLogMaintenanceThreshold").value=codexLogMaintenance.thresholdMiB??2048;
    document.getElementById("cfgCodexLogMaintenanceRetain").value=codexLogMaintenance.retainHours??12;
    document.getElementById("cfgCodexLogMaintenanceInterval").value=codexLogMaintenance.checkIntervalMinutes??15;
    toggleCodexLogMaintenanceControls();
    renderCodexLogMaintenanceRuntime(c.codexLogMaintenanceRuntime||{});
    const logIncidents=c.logIncidents||{};
    document.getElementById("cfgLogIncidentEnabled").checked=logIncidents.enabled!==false;
    document.getElementById("cfgLogIncidentNotify").checked=logIncidents.notify===true;
    document.getElementById("cfgLogIncidentWindow").value=logIncidents.windowMinutes||5;
    document.getElementById("cfgLogIncidentMinRequests").value=logIncidents.minRequests||8;
    document.getElementById("cfgLogIncidentErrorBurst").value=logIncidents.errorBurst||5;
    document.getElementById("cfgLogIncidentErrorRate").value=logIncidents.errorRatePercent||60;
    document.getElementById("cfgLogIncidentStreamBurst").value=logIncidents.streamFailureBurst||3;
    document.getElementById("cfgLogIncidentSnooze").value=logIncidents.defaultSnoozeMinutes||15;
    document.getElementById("cfgLogIncidentResolve").value=logIncidents.resolveAfterMinutes||5;
    document.getElementById("cfgLogIncidentLatency").checked=logIncidents.latencyEnabled===true;
    document.getElementById("cfgLogIncidentP95").value=logIncidents.p95Ms||120000;
    document.getElementById("cfgLogIncidentP95Ttfb").value=logIncidents.p95TtfbMs||20000;
    document.getElementById("cfgEnableAutoLock").checked=c.enableAutoLock!==false;
    document.getElementById("cfgMaxReqPerMin").value=c.maxRequestsPerMin||10;
    document.getElementById("cfgMaxTokPerMin").value=c.maxTokensPerMin||0;
    document.getElementById("cfgStreamLifetime").value=c.streamLifetime??1800000;
    document.getElementById("cfgResponsesStreamLifetime").value=c.responsesStreamLifetime??0;
    document.getElementById("cfgResponsesIdleTimeout").value=c.responsesIdleTimeout??5400000;
    document.getElementById("cfgAdminToken").value=c.adminToken||"";
    document.getElementById("cfgUpdateBaselineTag").value=c.updateBaselineTag||"";
    try{
      const sr=await fetch("/__status");
      const sd=await sr.json();
      const gki={};
      (sd.keys||[]).forEach(k=>{const g=k.group||"A";if(!gki[g])gki[g]={count:0,idxs:[]};gki[g].count++;gki[g].idxs.push(k.idx+1)});
      renderPortGroups(c.groups||{A:3456}, c.groupEnabled||{}, gki);
    }catch(e){renderPortGroups(c.groups||{A:3456}, c.groupEnabled||{}, {})}
    testAllPorts(c.groups||{A:3456});
    document.getElementById("cfgAutoResume").checked=c.autoResume===true;
    document.getElementById("cfgAutoResumeIdle").value=c.autoResumeIdleMinutes||10;
    document.getElementById("cfgAutoResumeDebounce").value=c.autoResumeDebounceMinutes||3;
    document.getElementById("cfgAutoResumeRunnerStall").value=c.autoResumeRunnerStallMinutes??20;
    document.getElementById("cfgAutoResumeRunnerRestarts").value=c.autoResumeRunnerMaxStallRestarts??1;
    document.getElementById("cfgCmdPath").value=c.cmdPath||"/mnt/c/Windows/System32/cmd.exe";
    document.getElementById("cfgCapacityBackoffSeconds").value=c.capacityBackoffSeconds||60;
    document.getElementById("cfgCapacityMaxWaitSeconds").value=c.capacityMaxWaitSeconds||300;
    const ti=c.taskInsight||{};
    const tiSignals=ti.signals||{};
    const tiDistill=ti.distill||{};
    document.getElementById("cfgTaskInsightEnabled").checked=ti.enabled===true;
    document.getElementById("cfgTaskInsightInstructions").checked=tiSignals.instructions===true;
    document.getElementById("cfgTaskInsightTools").checked=tiSignals.tools===true;
    document.getElementById("cfgTaskInsightUsage").checked=tiSignals.usage===true;
    document.getElementById("cfgTaskInsightCorrelate").checked=tiSignals.correlate===true;
    document.getElementById("cfgTaskInsightRetention").value=ti.retentionDays||30;
    document.getElementById("cfgTaskInsightDistillEnabled").checked=tiDistill.enabled===true;
    document.getElementById("cfgTaskInsightDistillEngine").value=tiDistill.engine||"ollama";
    document.getElementById("cfgTaskInsightDistillModel").value=tiDistill.model||"";
    document.getElementById("cfgTaskInsightDistillBaseUrl").value=tiDistill.baseUrl||"";
    document.getElementById("cfgTaskInsightDistillBudget").value=tiDistill.dailyBudgetYuan??1;
    document.getElementById("cfgTaskInsightDistillReport").value=tiDistill.report||"daily";
    taskInsightEngineChanged();
    refreshTaskInsightDistillStatus();
    renderResumeProjects(c.autoResumeProjects||[]);
    if(c.autoRecoverNextTime)autoRecoverNextTime=parseInt(c.autoRecoverNextTime);else autoRecoverNextTime=0;
    if(c.autoRecoverDailyNextTime)autoRecoverDailyNextTime=parseInt(c.autoRecoverDailyNextTime);else autoRecoverDailyNextTime=0;
    if(c.autoRecoverPollNextTime)autoRecoverPollNextTime=parseInt(c.autoRecoverPollNextTime);else autoRecoverPollNextTime=0;
    if(c.lastRequestTime)lastRequestTime=parseInt(c.lastRequestTime);else lastRequestTime=Date.now();
    if(c.lastKeyUseTime)lastKeyUseTime=parseInt(c.lastKeyUseTime);else lastKeyUseTime=0;
    if(c.lastResumeTime)lastResumeTime=parseInt(c.lastResumeTime);else lastResumeTime=0;
    if(window.autoCountTimer)clearInterval(window.autoCountTimer);
    window.autoCountTimer=setInterval(updateAutoCountdown,1000);
    updateAutoCountdown();
    if(window.twBadgeTimer)clearInterval(window.twBadgeTimer);
    window.twBadgeTimer=setInterval(updateTimeWindowBadges,60000);
    updateTimeWindowBadges();
  }catch(e){}
}
function normalizeCodexLogMaintenancePathInput(){
  const input=document.getElementById("cfgCodexLogMaintenancePath");
  const raw=(input&&input.value||"").trim();
  const backslash=String.fromCharCode(92);
  const lower=raw.toLowerCase();
  const prefixes=[backslash+backslash+"wsl.localhost"+backslash,backslash+backslash+"wsl$"+backslash];
  const prefix=prefixes.find(candidate=>lower.startsWith(candidate));
  if(!prefix)return raw;
  const withoutHost=raw.slice(prefix.length);
  const distributionEnd=withoutHost.indexOf(backslash);
  if(distributionEnd<=0)return raw;
  const innerPath=withoutHost.slice(distributionEnd+1).split(backslash).join("/").split("/").filter(Boolean).join("/");
  const normalized=innerPath?"/"+innerPath:raw;
  if(input&&normalized!==raw)input.value=normalized;
  return normalized;
}
function codexLogMaintenanceConfigFromForm(){
  return {
    enabled:document.getElementById("cfgCodexLogMaintenanceEnabled").checked,
    dbPath:normalizeCodexLogMaintenancePathInput(),
    thresholdMiB:configInteger("cfgCodexLogMaintenanceThreshold",2048),
    retainHours:configInteger("cfgCodexLogMaintenanceRetain",12),
    checkIntervalMinutes:configInteger("cfgCodexLogMaintenanceInterval",15)
  };
}
function toggleCodexLogMaintenanceControls(){
  const enabled=!!document.getElementById("cfgCodexLogMaintenanceEnabled")?.checked;
  document.querySelectorAll("[data-codex-log-maintenance-control]").forEach(el=>{el.disabled=!enabled;});
  if(!enabled)renderCodexLogMaintenanceRuntime({phase:"disabled"});
}
function codexLogMaintenanceBytes(value){
  const bytes=Math.max(0,Number(value)||0);
  if(bytes>=1024*1024*1024)return (bytes/(1024*1024*1024)).toFixed(2)+" GiB";
  if(bytes>=1024*1024)return (bytes/(1024*1024)).toFixed(1)+" MiB";
  if(bytes>=1024)return (bytes/1024).toFixed(1)+" KiB";
  return String(Math.round(bytes))+" B";
}
function setCodexLogMaintenanceCheck(text,color){
  const el=document.getElementById("cfgCodexLogMaintenanceCheck");
  if(!el)return;
  el.textContent=text||"";
  el.style.color=color||"#64748b";
}
function taskInsightEngineChanged(){
  const el=document.getElementById("cfgTaskInsightEngineHint");
  if(!el)return;
  const engine=(document.getElementById("cfgTaskInsightDistillEngine").value||"").trim();
  const hints={
    ollama:"数据不出本机、无外部费用；需本机运行 ollama 并已拉取所用模型（默认地址 http://127.0.0.1:11434/v1）。",
    proxy:"蒸馏请求经代理转发，token 计入代理统计/成本与限速，Key 不外泄；模型名需与代理内可用模型一致。",
    external:"直接调用外部 API（需在地址中携带可用的 API Key），不经代理，保密自担。"
  };
  el.textContent=hints[engine]||"";
}
function renderTaskInsightDistillStatus(runtime){
  const el=document.getElementById("cfgTaskInsightDistillStatus");
  if(!el)return;
  const enabled=!!document.getElementById("cfgTaskInsightDistillEnabled")?.checked;
  const state=runtime&&typeof runtime==="object"?runtime:{};
  if(!enabled){el.textContent="蒸馏: 未启用";el.style.color="#64748b";return;}
  let text="蒸馏: ";
  let color="#94a3b8";
  if(state.running){text+="正在运行…";color="#fbbf24";}
  else if(state.lastError){text+="上次失败: "+String(state.lastError).slice(0,120);color="#f87171";}
  else if(state.pending>0){text+="待处理 "+state.pending+" 个任务";color="#fbbf24";}
  else if(state.lastRunAt){text+="上次运行 "+new Date(state.lastRunAt).toLocaleString();color="#94a3b8";}
  else{text+="等待运行";color="#94a3b8";}
  const budget=state.budget||{};
  if(budget.limitYuan>0)text+="；今日预算 ¥"+Number(budget.spentYuan||0).toFixed(4)+" / ¥"+Number(budget.limitYuan);
  el.textContent=text;
  el.style.color=color;
}
function renderCodexLogMaintenanceRuntime(runtime){
  const el=document.getElementById("cfgCodexLogMaintenanceRuntime");
  if(!el)return;
  const enabled=!!document.getElementById("cfgCodexLogMaintenanceEnabled")?.checked;
  if(!enabled){el.textContent="🗄 Codex SQLite 日志维护: 未启用";el.style.color="#64748b";return;}
  const state=runtime&&typeof runtime==="object"?runtime:{};
  const total=codexLogMaintenanceBytes(state.totalBytes||0);
  let text="🗄 Codex SQLite 日志维护: ";
  let color="#94a3b8";
  if(state.inFlight||state.phase==="checking"){text+="正在检查数据库…";color="#fbbf24";}
  else if(state.phase==="error"){text+=(state.lastError||"上次检查失败");color="#f87171";}
  else if(state.lastResult==="cleaned"){text+="已删除 "+(state.deletedRows||0)+" 条过期记录"+(state.vacuumed?"并 VACUUM 释放 "+codexLogMaintenanceBytes((state.vacuumBytesBefore||0)-(state.vacuumBytesAfter||0)):"")+"；当前 "+total;color="#4ade80";}
  else if(state.lastResult==="retention_satisfied"){text+="已满足保留期；当前 "+total+"（物理空间将在 SQLite 后续复用）";color="#94a3b8";}
  else if(state.lastResult==="skipped_busy"){text+="数据库忙，已跳过并等待下次检查；当前 "+total;color="#fbbf24";}
  else if(state.lastResult==="below_threshold"){text+="当前 "+total+"，未达到触发阈值";color="#94a3b8";}
  else if(state.nextCheckAt&&state.nextCheckAt>Date.now()){text+="已计划检查；当前 "+total;color="#94a3b8";}
  else{text+="等待首次检查";color="#94a3b8";}
  const idleState=state.idleState;
  if(idleState){text+="　"+(idleState.idle?"空闲，可立即清理":"使用中（在途 "+(idleState.active||0)+" / 排队 "+(idleState.queued||0)+"），暂不可清理");}
  el.textContent=text;
  el.style.color=color;
}
async function refreshTaskInsightDistillStatus(){
  const el=document.getElementById("cfgTaskInsightDistillStatus");
  if(!el)return;
  try{
    const r=await fetch("/__task-insight-status");
    const j=await r.json();
    if(j&&j.ok){renderTaskInsightDistillStatus(j.distill||{});return;}
  }catch(e){}
  el.textContent="蒸馏: 状态不可用";
  el.style.color="#64748b";
}
async function checkCodexLogMaintenancePath(){
  const button=document.getElementById("cfgCodexLogMaintenanceCheckBtn");
  if(button)button.disabled=true;
  setCodexLogMaintenanceCheck("检测中…","#fbbf24");
  try{
    const r=await fetch("/__codex-log-maintenance/check",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({codexLogMaintenance:codexLogMaintenanceConfigFromForm()})});
    const j=await r.json();
    if(!r.ok||!j.ok){setCodexLogMaintenanceCheck("✕ "+(j.error||"路径或数据库无效"),"#f87171");return false;}
    const check=j.check||{};
    setCodexLogMaintenanceCheck("✓ 有效：主库 "+codexLogMaintenanceBytes(check.databaseBytes)+"，WAL "+codexLogMaintenanceBytes(check.walBytes),"#4ade80");
    return true;
  }catch(e){setCodexLogMaintenanceCheck("✕ 检测失败: "+e.message,"#f87171");return false;}
  finally{if(button)button.disabled=false;}
}
async function runCodexLogMaintenanceNow(){
  if(!document.getElementById("cfgCodexLogMaintenanceEnabled").checked){setCodexLogMaintenanceCheck("请先启用并保存配置","#fbbf24");return;}
  const button=document.getElementById("cfgCodexLogMaintenanceRunBtn");
  if(button)button.disabled=true;
  setCodexLogMaintenanceCheck("正在检查已保存的数据库配置…","#fbbf24");
  try{
    const r=await fetch("/__codex-log-maintenance/run",{method:"POST"});
    const j=await r.json();
    renderCodexLogMaintenanceRuntime(j.runtime||{});
    if(!r.ok||!j.ok){setCodexLogMaintenanceCheck("✕ "+(j.error||"检查失败"),"#f87171");return;}
    const check=j.result||{};
    const threshold=Number(document.getElementById("cfgCodexLogMaintenanceThreshold").value||0);
    const total=codexLogMaintenanceBytes(check.totalBytes||0);
    setCodexLogMaintenanceCheck("✓ 有效：主库 "+codexLogMaintenanceBytes(check.databaseBytes)+"，WAL "+codexLogMaintenanceBytes(check.walBytes)+"，当前 "+total+(threshold>0&&(check.totalBytes||0)<threshold*1024*1024?"（未达 "+threshold+" MiB 触发阈值）":""),"#4ade80");
  }catch(e){setCodexLogMaintenanceCheck("✕ 检查失败: "+e.message,"#f87171");}
  finally{toggleCodexLogMaintenanceControls();}
}
async function runCodexLogMaintenanceCleanNow(){
  if(!document.getElementById("cfgCodexLogMaintenanceEnabled").checked){setCodexLogMaintenanceCheck("请先启用并保存配置","#fbbf24");return;}
  const runBtn=document.getElementById("cfgCodexLogMaintenanceRunBtn");
  const cleanBtn=document.getElementById("cfgCodexLogMaintenanceCleanBtn");
  if(runBtn)runBtn.disabled=true;
  if(cleanBtn)cleanBtn.disabled=true;
  setCodexLogMaintenanceCheck("正在等待 Codex 空闲并清理数据库…","#fbbf24");
  try{
    const r=await fetch("/__codex-log-maintenance/clean",{method:"POST"});
    const j=await r.json();
    renderCodexLogMaintenanceRuntime(j.runtime||{});
    const result=j.result||{};
    if(result.result==="database_active"){
      const idle=result.idleState||{};
      setCodexLogMaintenanceCheck("✕ Codex 仍在使用中（在途 "+(idle.active||0)+" / 排队 "+(idle.queued||0)+"，距上次请求 "+Math.round((idle.lastAgoMs||0)/1000)+" 秒）；静默 60 秒后再试","#fbbf24");
      return;
    }
    if(!r.ok||!j.ok){setCodexLogMaintenanceCheck("✕ "+(j.error||"清理失败"),"#f87171");return;}
    if(result.result==="skipped_busy")setCodexLogMaintenanceCheck("数据库忙，已跳过；请稍后重试","#fbbf24");
    else if(result.result==="below_threshold")setCodexLogMaintenanceCheck("当前容量未达到阈值，未删除记录","#94a3b8");
    else if(result.result==="retention_satisfied")setCodexLogMaintenanceCheck("没有超过保留期的记录，无需清理","#94a3b8");
    else{
      let text="✓ 已删除 "+(result.deletedRows||0)+" 条过期记录";
      if(result.vacuumed)text+="；VACUUM 释放 "+codexLogMaintenanceBytes((result.vacuumBytesBefore||0)-(result.vacuumBytesAfter||0))+"（文件 "+codexLogMaintenanceBytes(result.physicalBytesBefore||0)+" → "+codexLogMaintenanceBytes(result.physicalBytesAfter||0)+"）";
      else text+="；当前 "+codexLogMaintenanceBytes(result.totalBytes||0);
      setCodexLogMaintenanceCheck(text,"#4ade80");
    }
  }catch(e){setCodexLogMaintenanceCheck("✕ 清理失败: "+e.message,"#f87171");}
  finally{toggleCodexLogMaintenanceControls();}
}
function renderPortGroups(groups, groupEnabled, groupKeyInfo){
  const area=document.getElementById("portGroupsArea");
  if(!area)return;
  const g=groups||{A:3456};
  const enabled=groupEnabled||{};
  const gki=groupKeyInfo||{};
  const names=Object.keys(g).sort();
  let html='<div style="font-size:11px;display:flex;flex-direction:column;gap:4px;white-space:nowrap">';
  for(const n of names){
    const port=g[n];
    const isA=n==="A";
    const isOn=enabled[n]!==false;
    const ki=gki[n];
    let keyInfo='';
    if(ki){
      const show=ki.idxs.slice(0,10);
      const more=ki.idxs.length>10?'...':'';
      keyInfo='<span style="color:#64748b;font-size:10px;margin-left:8px">🔑 '+ki.count+'个 | #'+show.join(',#')+more+'</span>';
    }
    html+='<div style="display:flex;gap:8px;align-items:center;padding:2px 0;white-space:nowrap">'+
      '<span style="width:30px;font-weight:600;color:'+(isA?"#60a5fa":"#e2e8f0")+'">'+n+'</span>'+
      '<span style="color:#94a3b8">端口 '+port+'</span>'+
      '<span class="portStatus" data-group="'+n+'" style="font-size:10px;min-width:14px;display:inline-block;text-align:center">⏳</span>'+
      (isA?'<span style="color:#60a5fa;font-size:10px">(默认/始终运行)</span>':'');
    if(!isA){
      html+='<button class="btn" style="font-size:10px;padding:1px 6px;color:'+(isOn?'#f87171':'#4ade80')+'" onclick="toggleGroup(\\''+n+'\\','+String(!isOn)+',this)">'+(isOn?'🔴 禁用':'🟢 启用')+'</button>'+
        '<button class="btn" style="font-size:10px;padding:1px 6px;color:#f87171" onclick="removePortGroup(\\''+n+'\\')">删除</button>';
    }
    html+=keyInfo+'</div>';
  }
  html+='<div style="display:flex;gap:6px;align-items:center;margin-top:4px;padding-top:4px;border-top:1px solid #334155">'+
    '<input id="newGroupName" placeholder="组名" style="width:40px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;text-transform:uppercase">'+
    '<input id="newGroupPort" type="number" placeholder="端口" min="1024" max="65535" style="width:70px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px">'+
    '<button class="btn" style="font-size:10px;padding:1px 6px;color:#4ade80" onclick="addPortGroup()">添加</button>'+
    '</div></div>';
  area.style.paddingLeft="12px";
  area.innerHTML=html;
}
function testAllPorts(groups){
  const g=groups||{A:3456};
  for(const [name, port] of Object.entries(g)){
    const el=document.querySelector('.portStatus[data-group="'+name+'"]');
    if(!el)continue;
    el.textContent="⏳";
    const ctrl=new AbortController();
    const tid=setTimeout(()=>ctrl.abort(),3000);
    fetch("/__test_port?port="+port,{signal:ctrl.signal})
      .then(r=>r.json()).then(d=>{clearTimeout(tid);el.textContent=d.running?"🟢":"🔴"})
      .catch(()=>{clearTimeout(tid);el.textContent="🔴"});
  }
}
function toggleGroup(name, enable, btn){
  fetch("http://localhost:3456/__config",{method:"PUT",headers:{"content-type":"application/json"},body:JSON.stringify({_groupAction:"toggleGroup",_groupName:name,_groupEnabled:enable})})
    .then(r=>r.json()).then(j=>{if(j.ok)loadConfigUI();else alert("操作失败: "+j.error)}).catch(e=>alert("操作失败: "+e.message));
}
function removePortGroup(name){
  if(name==="A")return;
  if(!confirm("确定删除分组 "+name+" ？"))return;
  fetch("http://localhost:3456/__config",{method:"PUT",headers:{"content-type":"application/json"},body:JSON.stringify({_groupAction:"removeGroup",_groupName:name})})
    .then(r=>r.json()).then(j=>{if(j.ok){loadConfigUI()}else{alert("删除失败: "+j.error)}}).catch(e=>alert("删除失败: "+e.message));
}
function addPortGroup(){
  const name=document.getElementById("newGroupName").value.trim().toUpperCase();
  const port=parseInt(document.getElementById("newGroupPort").value);
  if(!name||!port){alert("请输入组名和端口号");return}
  fetch("http://localhost:3456/__config",{method:"PUT",headers:{"content-type":"application/json"},body:JSON.stringify({_groupAction:"addGroup",_groupName:name,_groupPort:port})})
    .then(r=>r.json()).then(j=>{if(j.ok){document.getElementById("newGroupName").value="";document.getElementById("newGroupPort").value="";loadConfigUI()}else{alert("添加失败: "+j.error)}}).catch(e=>alert("添加失败: "+e.message));
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
  const pollEl=document.getElementById("cfgAutoPollCountdown");
  if(pollEl){
    if(!autoRecoverPollNextTime||autoRecoverPollNextTime<=Date.now()){pollEl.textContent="⏳ 下次检测（快速）: --";}
    else{const diff=Math.ceil((autoRecoverPollNextTime-Date.now())/1000);const m=Math.floor(diff/60),s=diff%60;pollEl.textContent="⏳ 下次检测（快速）: "+m+"m "+String(s).padStart(2,"0")+"s";}
  }
  const resumeEl=document.getElementById("cfgAutoResumeStatus");
  if(resumeEl){
    if(typeof lastKeyUseTime==='number'&&lastKeyUseTime>0){
      const idleMs=Date.now()-(window._idleFrom||lastKeyUseTime);
      const sinceResume=typeof lastResumeTime==='number'&&lastResumeTime>0?Math.round((Date.now()-lastResumeTime)/60000):null;
      resumeEl.textContent="🧬 闲置恢复: Key 闲置 "+formatIdle(idleMs)+(sinceResume!==null?"，上次触发 "+sinceResume+"m 前":"，等待阈值");
    }else{
      resumeEl.textContent="🧬 闲置恢复: 等待中";
    }
  }
}
function updateTimeWindowBadges(){
  document.querySelectorAll('[data-tw]').forEach(el=>{
    const idx=parseInt(el.dataset.tw);
    const k=data&&data[idx];
    if(!k||!k.timeWindow)return;
    const tz=k.tz||"+0";
    const m=String(tz).match(/^([+-]?)(\d+(?:\.\d+)?)$/);
    const offset=m?(m[1]==="-"?-1:1)*parseFloat(m[2]):0;
    const now=new Date();
    const lh=(now.getUTCHours()+offset+24)%24;
    const lm=now.getUTCMinutes();
    const start=k.timeWindow.start,end=k.timeWindow.end;
    const inWin=start<end?(lh>=start&&lh<end):(lh>=start||lh<end);
    let remaining;
    if(start<end){
      remaining=inWin?((end-lh)*60-lm):((lh<start?(start-lh)*60-lm:(24-lh+start)*60-lm));
    }else{
      if(lh>=start)remaining=(24-lh+end)*60-lm;
      else if(lh<end)remaining=(end-lh)*60-lm;
      else remaining=(start-lh)*60-lm;
    }
    const h=Math.floor(remaining/60),m2=remaining%60;
    const timeStr=start+':00-'+end+':00 (UTC'+tz+')';
    if(inWin){el.title='时段: '+timeStr+' | 剩余 '+h+'小时'+m2+'分钟';el.style.background='#1a3a2e';el.style.color='#4ade80';el.style.borderColor='#22c55e';}
    else{el.title='时段: '+timeStr+' | 距可用 '+h+'小时'+m2+'分钟';el.style.background='#3b2a1a';el.style.color='#fb923c';el.style.borderColor='#f97316';}
  });
}

function renderResumeProjects(projects){
  const container=document.getElementById("cfgResumeProjects");
  if(!container)return;
  let html='<div style="display:flex;flex-direction:column;gap:6px">';
  const list=projects&&projects.length?projects:[{name:"",path:"",cmd:""}];
  for(let i=0;i<list.length&&i<10;i++){
    const p=list[i];
    const mode=p.resumeMode==="fixed_session"?"fixed_session":"command";
    html+='<div class="resume-proj-row" style="display:flex;gap:4px;align-items:center;flex-wrap:wrap;padding:4px;background:#1e293b;border:1px solid #334155;border-radius:4px">'+
      '<input placeholder="项目名" class="rp-name" value="'+esc(p.name||'')+'" style="width:80px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">'+
      '<input placeholder="WSL 路径 /mnt/d/..." class="rp-path" value="'+esc(p.path||'')+'" style="flex:1;min-width:120px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">'+
      '<input placeholder="命令 codex ..." class="rp-cmd" value="'+esc(p.cmd||'')+'" style="flex:1;min-width:100px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">'+
      '<select class="rp-mode" title="固定会话模式会将命令中的 {sessionId} 替换为下方会话 ID" style="width:74px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 3px;border-radius:4px;font-size:10px"><option value="command"'+(mode==="command"?" selected":"")+'>命令</option><option value="fixed_session"'+(mode==="fixed_session"?" selected":"")+'>固定会话</option></select>'+
      '<input placeholder="会话 ID" class="rp-session" value="'+esc(p.sessionId||'')+'" style="width:130px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px" title="固定会话模式需要合法 Codex 会话 ID，且命令中必须含 {sessionId}">'+
      '<button class="btn" style="font-size:10px;color:#ef4444;padding:0 4px" onclick="removeResumeProject(this)">✕</button></div>';
  }
  html+='</div>';
  container.innerHTML=html;
}
function addResumeProject(){
  const container=document.getElementById("cfgResumeProjects");
  if(!container)return;
  const rows=container.querySelectorAll(".resume-proj-row");
  if(rows.length>=10)return;
  const div=document.createElement("div");
  div.className="resume-proj-row";
  div.style.cssText="display:flex;gap:4px;align-items:center;flex-wrap:wrap;padding:4px;background:#1e293b;border:1px solid #334155;border-radius:4px";
  div.innerHTML='<input placeholder="项目名" class="rp-name" style="width:80px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">'+
    '<input placeholder="WSL 路径 /mnt/d/..." class="rp-path" style="flex:1;min-width:120px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">'+
    '<input placeholder="命令 codex ..." class="rp-cmd" style="flex:1;min-width:100px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">'+
    '<select class="rp-mode" title="固定会话模式会将命令中的 {sessionId} 替换为下方会话 ID" style="width:74px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 3px;border-radius:4px;font-size:10px"><option value="command">命令</option><option value="fixed_session">固定会话</option></select>'+
    '<input placeholder="会话 ID" class="rp-session" style="width:130px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px" title="固定会话模式需要合法 Codex 会话 ID，且命令中必须含 {sessionId}">'+
    '<button class="btn" style="font-size:10px;color:#ef4444;padding:0 4px" onclick="removeResumeProject(this)">✕</button>';
  container.querySelector("div").appendChild(div);
}
function collectResumeProjects(){
  const rows=document.querySelectorAll("#cfgResumeProjects .resume-proj-row");
  const projects=[];
  for(const row of rows){
    const name=row.querySelector(".rp-name").value.trim();
    const path=row.querySelector(".rp-path").value.trim();
    const cmd=row.querySelector(".rp-cmd").value.trim();
    const resumeMode=(row.querySelector(".rp-mode")||{}).value||"command";
    const sessionId=(row.querySelector(".rp-session")||{}).value||"";
    if(path&&cmd)projects.push({name:name||path.split("/").pop(),path,cmd,resumeMode,sessionId:sessionId.trim()});
  }
  return projects;
}
function removeResumeProject(btn){
  const row=btn.closest(".resume-proj-row");
  if(row){
    const container=document.getElementById("cfgResumeProjects");
    if(container&&container.querySelectorAll(".resume-proj-row").length<=1)return;
    row.remove();
  }
}

function setTrendMode(mode){
  trendMode=mode;
  document.querySelectorAll("#trendTabs .trend-tab").forEach(function(el){
    el.classList.toggle("active",el.getAttribute("data-mode")===mode);
  });
  renderTrend();
}
function renderTrend(){
  const now=new Date();
  const hours=trendRange==="7d"?168:(trendRange==="30d"?720:24);
  const hMap={};
  for(let i=hours-1;i>=0;i--){
    const d=new Date(now-i*3600000);
    const y=d.getFullYear(),m=String(d.getMonth()+1).padStart(2,"0"),dd=String(d.getDate()).padStart(2,"0"),hh=String(d.getHours()).padStart(2,"0");
    hMap[y+"-"+m+"-"+dd+"-"+hh]={bytes:0,input:0,output:0,req:0,totalCost:0,totalDuration:0,keys:{},models:{},status:{},urls:{},urlsKeys:{},clients:{}};
  }
  const allModels={};
  const allUrls={};
  const allClients={};
  for(const a of data){
    if(!a.hourly)continue;
    const ai=a.idx;
    let uKey="(未知)";
    if(a.url){
      try{ uKey=new URL(a.url).hostname; }catch(e){ uKey=a.url; }
    }
    for(const [hk,v] of Object.entries(a.hourly)){
      if(hMap[hk]===undefined)continue;
      const h=hMap[hk];
      const ib=v.inputBytes||0,ob=v.outputBytes||0;
      h.input+=ib;h.output+=ob;h.bytes+=ib+ob;h.req+=v.requests||0;
      h.totalCost+=Number(v.totalCost)||0;h.totalDuration+=v.totalDuration||0;
      if(!h.keys[ai])h.keys[ai]={bytes:0,req:0};
      h.keys[ai].bytes+=ib+ob;
      h.keys[ai].req+=v.requests||0;
      h.urls[uKey]=(h.urls[uKey]||0)+(v.requests||0);
      if(!h.urlsKeys[uKey])h.urlsKeys[uKey]={};
      h.urlsKeys[uKey][ai]=(h.urlsKeys[uKey][ai]||0)+(v.requests||0);
      allUrls[uKey]=(allUrls[uKey]||0)+(v.requests||0);
      if(v.models){
        for(const [mk,mv] of Object.entries(v.models)){
          if(!h.models[mk])h.models[mk]={requests:0,inputBytes:0,outputBytes:0,totalCost:0};
          h.models[mk].requests+=mv.requests||0;
          h.models[mk].inputBytes+=mv.inputBytes||0;
          h.models[mk].outputBytes+=mv.outputBytes||0;
          h.models[mk].totalCost+=Number(mv.totalCost)||0;
          if(!allModels[mk])allModels[mk]=0;
          allModels[mk]+=mv.requests||0;
        }
      }
      if(v.statusCodes){
        for(const [scKey,scVal] of Object.entries(v.statusCodes)){
          if(!h.status[scKey])h.status[scKey]=0;
          h.status[scKey]+=scVal;
        }
      }
      if(v.clients){
        for(const [ck,cv] of Object.entries(v.clients)){
          if(!h.clients[ck])h.clients[ck]=0;
          h.clients[ck]+=cv;
          if(!allClients[ck])allClients[ck]=0;
          allClients[ck]+=cv;
        }
      }
    }
  }
  const modelColors=["#3b82f6","#f59e0b","#10b981","#ef4444","#8b5cf6","#ec4899","#06b6d4","#f97316"];
  const sortedModels=Object.keys(allModels).sort((a,b)=>allModels[b]-allModels[a]);
  const topModels=sortedModels.slice(0,8);
  const modelColorMap={};
  topModels.forEach((m,i)=>{modelColorMap[m]=modelColors[i%modelColors.length];});
  if(sortedModels.length>8){
    modelColorMap["(其他)"]="#6b7280";
  }
  const keys=Object.keys(hMap);
  let vals,max;
  if(trendMode==="model"){
    vals=keys.map(k=>{let s=0;for(const mk of topModels)s+=hMap[k].models[mk]?.requests||0;if(sortedModels.length>8){for(const [mk,mv] of Object.entries(hMap[k].models)){if(!topModels.includes(mk))s+=mv.requests||0;}}return s;});
    max=Math.max(...vals,1);
  }else if(trendMode==="health"){
    vals=keys.map(k=>{const s=hMap[k].status;return(s.ok||0)+(s["4xx"]||0)+(s["5xx"]||0)+(s.fail||0);});
    max=Math.max(...vals,1);
  }else if(trendMode==="upstream"){
    vals=keys.map(k=>{const u=hMap[k].urls||{};return Object.values(u).reduce((s,n)=>s+n,0);});
    max=Math.max(...vals,1);
  }else if(trendMode==="downstream"){
    vals=keys.map(k=>{const c=hMap[k].clients||{};return Object.values(c).reduce((s,n)=>s+n,0);});
    max=Math.max(...vals,1);
  }else if(trendMode==="latency"){
    vals=keys.map(k=>{const h=hMap[k];return h.req>0?Math.round(h.totalDuration/h.req):0;});
    max=Math.max(...vals,1);
  }else{
    vals=keys.map(k=>hMap[k][trendMode]);
    max=Math.max(...vals,1);
  }
  const bars=document.getElementById("trendBars");
  const labels=document.getElementById("trendLabels");
  if(trendMode==="model"){
    bars.innerHTML=keys.map((k,i)=>{
      const h=hMap[k];
      const total=h.req||0;
      const lines=[];
      const mmdd=k.slice(0,10),hh=k.slice(11);
      lines.push(mmdd+" "+hh+":00~"+String(Number(hh)+1).padStart(2,"0")+":00");
      lines.push("合计: "+h.req+"次");
      const segments=[];
      for(const mk of topModels){
        const mv=h.models[mk]||{requests:0};
        if(mv.requests>0){
          const pct=mv.requests/total*100;
          const clr=modelColorMap[mk];
          segments.push('<div class="trend-seg" style="height:'+pct+'%;background:'+clr+'"></div>');
          lines.push("  "+mk+": "+mv.requests+"次");
        }
      }
      if(sortedModels.length>8){
        let otherReq=0;
        for(const [mk,mv] of Object.entries(h.models)){
          if(!topModels.includes(mk))otherReq+=mv.requests||0;
        }
        if(otherReq>0){
          const pct=otherReq/total*100;
          segments.push('<div class="trend-seg" style="height:'+pct+'%;background:#6b7280"></div>');
          lines.push("  (其他): "+otherReq+"次");
        }
      }
      const barH=Math.max(2,vals[i]/max*80);
      return '<div class="trend-bar trend-stack" style="height:'+barH+'px" title="'+esc(lines.join("\\n")).replace(/\\n/g,"&#10;")+'">'+segments.join("")+'</div>';
    }).join("");
    const legendModels=topModels.slice();
    if(sortedModels.length>8){
      let otherTotal=0;
      for(const mk of sortedModels){if(!topModels.includes(mk))otherTotal+=allModels[mk]||0;}
      allModels["(其他)"]=otherTotal;
      legendModels.push("(其他)");
    }
    const legendHtml=legendModels.map(mk=>'<span class="trend-legend-item"><span class="trend-legend-dot" style="background:'+modelColorMap[mk]+'"></span>'+esc(mk)+' ('+allModels[mk]+')</span>').join("");
    const legendEl=document.getElementById("trendLegend");
    if(legendEl)legendEl.innerHTML=legendHtml;
  }else if(trendMode==="health"){
    bars.innerHTML=keys.map((k,i)=>{
      const h=hMap[k];
      const st=h.status||{};
      const ok=st.ok||0,c4xx=st["4xx"]||0,c5xx=st["5xx"]||0,fail=st.fail||0;
      const total=ok+c4xx+c5xx+fail;
      const lines=[];
      const mmdd=k.slice(0,10),hh=k.slice(11);
      lines.push(mmdd+" "+hh+":00~"+String(Number(hh)+1).padStart(2,"0")+":00");
      lines.push("合计: "+total+"次");
      if(ok)lines.push("  200: "+ok+"次");
      if(c4xx)lines.push("  4xx: "+c4xx+"次");
      if(c5xx)lines.push("  5xx: "+c5xx+"次");
      if(fail)lines.push("  失败: "+fail+"次");
      const pct=s=>total?s/total*100:0;
      const segments=[];
      if(ok)segments.push('<div class="trend-seg" style="height:'+pct(ok)+'%;background:#22c55e"></div>');
      if(c4xx)segments.push('<div class="trend-seg" style="height:'+pct(c4xx)+'%;background:#eab308"></div>');
      if(c5xx)segments.push('<div class="trend-seg" style="height:'+pct(c5xx)+'%;background:#ef4444"></div>');
      if(fail)segments.push('<div class="trend-seg" style="height:'+pct(fail)+'%;background:#6b7280"></div>');
      const barH=Math.max(2,vals[i]/max*80);
      return '<div class="trend-bar trend-stack" style="height:'+barH+'px" title="'+esc(lines.join("\\n")).replace(/\\n/g,"&#10;")+'">'+segments.join("")+'</div>';
    }).join("");
    const legendEl=document.getElementById("trendLegend");
    if(legendEl)legendEl.innerHTML=
      '<span class="trend-legend-item"><span class="trend-legend-dot" style="background:#22c55e"></span>200</span>'+
      '<span class="trend-legend-item"><span class="trend-legend-dot" style="background:#eab308"></span>4xx</span>'+
      '<span class="trend-legend-item"><span class="trend-legend-dot" style="background:#ef4444"></span>5xx</span>'+
      '<span class="trend-legend-item"><span class="trend-legend-dot" style="background:#6b7280"></span>失败</span>';
  }else if(trendMode==="cost"){
    bars.innerHTML=keys.map((k,i)=>{
      const h=hMap[k];
      const lines=[];
      const mmdd=k.slice(0,10),hh=k.slice(11);
      lines.push(mmdd+" "+hh+":00~"+String(Number(hh)+1).padStart(2,"0")+":00");
      lines.push("费用: $"+h.totalCost.toFixed(6));
      const modelCosts=Object.entries(h.models||{}).filter(([,mv])=>(Number(mv.totalCost)||0)>0).sort((a,b)=>(Number(b[1].totalCost)||0)-(Number(a[1].totalCost)||0));
      for(const [mk,mv] of modelCosts.slice(0,8))lines.push("  "+mk+": $"+(Number(mv.totalCost)||0).toFixed(6));
      if(modelCosts.length>8)lines.push("  其他模型: $"+modelCosts.slice(8).reduce((sum,[,mv])=>sum+(Number(mv.totalCost)||0),0).toFixed(6));
      return '<div class="trend-bar" style="height:'+Math.max(2,vals[i]/max*80)+'px" title="'+esc(lines.join("\\n")).replace(/\\n/g,"&#10;")+'"></div>';
    }).join("");
    const legendEl=document.getElementById("trendLegend");
    if(legendEl)legendEl.innerHTML="";
  }else if(trendMode==="latency"){
    bars.innerHTML=keys.map((k,i)=>{
      const h=hMap[k];
      const avg=h.req>0?Math.round(h.totalDuration/h.req):0;
      const lines=[];
      const mmdd=k.slice(0,10),hh=k.slice(11);
      lines.push(mmdd+" "+hh+":00~"+String(Number(hh)+1).padStart(2,"0")+":00");
      lines.push("平均延迟: "+fmtDur(avg));
      if(h.req)lines.push("请求数: "+h.req+"次");
      return '<div class="trend-bar" style="height:'+Math.max(2,vals[i]/max*80)+'px" title="'+esc(lines.join("\\n")).replace(/\\n/g,"&#10;")+'"></div>';
    }).join("");
    const legendEl=document.getElementById("trendLegend");
    if(legendEl)legendEl.innerHTML="";
  }else if(trendMode==="upstream"){
    const sortedUrls=Object.keys(allUrls).sort((a,b)=>allUrls[b]-allUrls[a]);
    const topUrls=sortedUrls.slice(0,8);
    const urlColorMap={};
    topUrls.forEach((u,i)=>{urlColorMap[u]=modelColors[i%modelColors.length];});
    if(sortedUrls.length>8)urlColorMap["(其他)"]="#6b7280";
    bars.innerHTML=keys.map((k,i)=>{
      const h=hMap[k];
      const total=h.req||0;
      const lines=[];
      const mmdd=k.slice(0,10),hh=k.slice(11);
      lines.push(mmdd+" "+hh+":00~"+String(Number(hh)+1).padStart(2,"0")+":00");
      lines.push("合计: "+total+"次");
      const segments=[];
      for(const u of topUrls){
        const uv=h.urls[u]||0;
        if(uv>0){
          const pct=total?uv/total*100:0;
          segments.push('<div class="trend-seg" style="height:'+pct+'%;background:'+urlColorMap[u]+'"></div>');
          const uk=Object.keys(h.urlsKeys[u]||{});
          lines.push("  "+u+" (#"+uk.join(",#")+"): "+uv+"次");
        }
      }
      if(sortedUrls.length>8){
        let otherTotal=0;
        for(const [uu,uuVal] of Object.entries(h.urls)){
          if(!topUrls.includes(uu))otherTotal+=uuVal;
        }
        if(otherTotal>0){
          const pct=total?otherTotal/total*100:0;
          segments.push('<div class="trend-seg" style="height:'+pct+'%;background:#6b7280"></div>');
          lines.push("  (其他): "+otherTotal+"次");
        }
      }
      const barH=Math.max(2,vals[i]/max*80);
      return '<div class="trend-bar trend-stack" style="height:'+barH+'px" title="'+esc(lines.join("\\n")).replace(/\\n/g,"&#10;")+'">'+segments.join("")+'</div>';
    }).join("");
    const legendUrls=topUrls.slice();
    if(sortedUrls.length>8){
      let otherTotal=0;
      for(const u of sortedUrls){if(!topUrls.includes(u))otherTotal+=allUrls[u]||0;}
      allUrls["(其他)"]=otherTotal;
      legendUrls.push("(其他)");
    }
    const legendEl=document.getElementById("trendLegend");
    if(legendEl)legendEl.innerHTML=legendUrls.map(u=>'<span class="trend-legend-item"><span class="trend-legend-dot" style="background:'+urlColorMap[u]+'"></span>'+esc(u)+' ('+allUrls[u]+')</span>').join("");
  }else if(trendMode==="downstream"){
    const sortedClients=Object.keys(allClients).sort((a,b)=>allClients[b]-allClients[a]);
    const topClients=sortedClients.slice(0,8);
    const clientColorMap={};
    topClients.forEach((c,i)=>{clientColorMap[c]=modelColors[i%modelColors.length];});
    if(sortedClients.length>8)clientColorMap["(其他)"]="#6b7280";
    bars.innerHTML=keys.map((k,i)=>{
      const h=hMap[k];
      const total=h.req||0;
      const lines=[];
      const mmdd=k.slice(0,10),hh=k.slice(11);
      lines.push(mmdd+" "+hh+":00~"+String(Number(hh)+1).padStart(2,"0")+":00");
      lines.push("合计: "+total+"次");
      const segments=[];
      for(const c of topClients){
        const cv=h.clients[c]||0;
        if(cv>0){
          const pct=total?cv/total*100:0;
          segments.push('<div class="trend-seg" style="height:'+pct+'%;background:'+clientColorMap[c]+'"></div>');
          lines.push("  "+c+": "+cv+"次");
        }
      }
      if(sortedClients.length>8){
        let otherTotal=0;
        for(const [cc,ccVal] of Object.entries(h.clients)){
          if(!topClients.includes(cc))otherTotal+=ccVal;
        }
        if(otherTotal>0){
          const pct=total?otherTotal/total*100:0;
          segments.push('<div class="trend-seg" style="height:'+pct+'%;background:#6b7280"></div>');
          lines.push("  (其他): "+otherTotal+"次");
        }
      }
      const barH=Math.max(2,vals[i]/max*80);
      return '<div class="trend-bar trend-stack" style="height:'+barH+'px" title="'+esc(lines.join("\\n")).replace(/\\n/g,"&#10;")+'">'+segments.join("")+'</div>';
    }).join("");
    const legendClients=topClients.slice();
    if(sortedClients.length>8){
      let otherTotal=0;
      for(const c of sortedClients){if(!topClients.includes(c))otherTotal+=allClients[c]||0;}
      allClients["(其他)"]=otherTotal;
      legendClients.push("(其他)");
    }
    const legendEl=document.getElementById("trendLegend");
    if(legendEl)legendEl.innerHTML=legendClients.map(c=>'<span class="trend-legend-item"><span class="trend-legend-dot" style="background:'+clientColorMap[c]+'"></span>'+esc(c)+' ('+allClients[c]+')</span>').join("");
  }else{
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
      return '<div class="trend-bar" style="height:'+Math.max(2,vals[i]/max*80)+'px" title="'+esc(lines.join("\\n")).replace(/\\n/g,"&#10;")+'"></div>';
    }).join("");
    const legendEl=document.getElementById("trendLegend");
    if(legendEl)legendEl.innerHTML="";
  }
  const labelStep=trendRange==="30d"?24:(trendRange==="7d"?12:1);
  labels.innerHTML=keys.map((k,i)=>{
    const hh=k.slice(-2);
    const mmdd=k.slice(5,10);
    let text=trendRange==="30d"?mmdd:(trendRange==="7d"?mmdd+" "+hh+":00":hh+":00");
    const vis=i%labelStep===0;
    return '<div class="trend-label" style="'+(vis?"":"visibility:hidden;font-size:0")+'">'+text+'</div>';
  }).join("");
  document.getElementById("trend").style.display="block";
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
  const totalCost=data.reduce((s,x)=>s+(Number(x.totalCost)||0),0);
  const q="http://localhost:3456/";

  document.getElementById("subText").textContent="最后更新: "+new Date().toLocaleString("zh-CN")+" | 实时推送";

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
  // Populate group filter options
  const groupsSet={};
  data.forEach(x=>{const g=x.group||"A";groupsSet[g]=true});
  const groupSel=document.getElementById("groupFilter");
  const curVal=groupSel.value;
  const knownGroups=Object.keys(groupsSet).sort();
  groupSel.innerHTML='<option value="all">全部</option>'+knownGroups.map(g=>'<option value="'+g+'"'+(curVal===g?' selected':'')+'>'+g+'组</option>').join("");
  filtered=data;
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
  const weeklyResetDayFilter=document.getElementById("weeklyResetDayFilter");
  const weeklyResetDayFilterWrap=document.getElementById("weeklyResetDayFilterWrap");
  const showWeeklyResetDayFilter=resetType==="weekly";
  weeklyResetDayFilterWrap.style.display=showWeeklyResetDayFilter?"inline-flex":"none";
  if(!showWeeklyResetDayFilter)weeklyResetDayFilter.value="all";
  if(showWeeklyResetDayFilter&&weeklyResetDayFilter.value!=="all"){
    const selectedResetDay=weeklyResetDayFilter.value;
    filtered=filtered.filter(x=>{
      const resetDay=x.resetDay==null||x.resetDay===""?"auto":String(x.resetDay);
      return resetDay===selectedResetDay;
    });
  }
  groupFilter=document.getElementById("groupFilter").value;
  if(groupFilter!=="all")filtered=filtered.filter(x=>x.group===groupFilter);
  document.getElementById("filterCount").textContent="显示 "+filtered.length+" / "+data.length+" 个";
  const shieldedCount=data.filter(x=>x.shielded).length;
  if(shieldedCount>0)document.getElementById("filterCount").textContent+="，屏蔽 "+shieldedCount+" 个";
  const dashResume=document.getElementById("dashResumeStatus");
  if(dashResume&&typeof lastKeyUseTime==='number'&&lastKeyUseTime>0){
    const sinceResume=typeof lastResumeTime==='number'&&lastResumeTime>0?Math.round((Date.now()-lastResumeTime)/60000):null;
    if(sinceResume!==null)dashResume.textContent="🧬Key闲置 "+formatIdle(now-(window._idleFrom||lastKeyUseTime))+"/恢复"+sinceResume+"m前";
    else dashResume.textContent="🧬Key闲置 "+formatIdle(now-(window._idleFrom||lastKeyUseTime));
  }
  const actKeys=data.filter(x=>x.active);
  if(!window._tickerInit){
    window._tickerInit=true;
    setInterval(()=>{
      document.querySelectorAll("#ticker .ticker-item").forEach(el=>{
        const since=+el.dataset.since;if(!since)return;
        const sec=Math.max(0,Math.round((Date.now()-since)/1000));
        const dur=sec>=60?Math.floor(sec/60)+"m"+(sec%60)+"s":sec+"s";
        const d=el.querySelector(".t-dur");if(d)d.textContent=dur;
      });
    },1000);
  }
  if(!window._idleTicker){
    window._idleTicker=true;
    let wasActive=false;
    setInterval(()=>{
      const de=document.getElementById("dashResumeStatus");
      const re=document.getElementById("cfgAutoResumeStatus");
      if(!de&&!re)return;
      const anyActive=data&&data.some(k=>(k.actives||[]).length>0);
      if(!anyActive&&wasActive){
        wasActive=false;
        window._idleFrom=Date.now();
      }else if(anyActive){
        wasActive=true;
      }
      if(typeof lastKeyUseTime==='number'&&lastKeyUseTime>0&&!anyActive){
        const idleMs=Date.now()-(window._idleFrom||lastKeyUseTime);
        const idleStr=formatIdle(idleMs);
        const sinceResume=typeof lastResumeTime==='number'&&lastResumeTime>0?Math.round((Date.now()-lastResumeTime)/60000):null;
        if(de)de.textContent="🧬Key闲置 "+idleStr+(sinceResume!==null?"/恢复"+sinceResume+"m前":"");
        if(re)re.textContent="🧬 闲置恢复: Key 闲置 "+idleStr+(sinceResume!==null?"，上次触发 "+sinceResume+"m 前":"，等待阈值");
      }else if(anyActive){
        if(de)de.textContent="🧬Key闲置 0.00s";
        if(re)re.textContent="🧬 闲置恢复: Key 使用中";
      }else{
        if(de)de.textContent="🧬Key闲置 --";
        if(re)re.textContent="🧬 闲置恢复: 等待中";
      }
    },100);
  }
  const curSet=new Set();
  actKeys.forEach(k=>{(k.actives||[]).forEach(r=>{curSet.add(k.idx+"#"+r.since);});});
  const tickerEl=document.getElementById("ticker");
  const tickerLabel=document.getElementById("tickerLabel");
  [...tickerEl.children].forEach(el=>{if(!curSet.has(el.dataset.key))el.remove();});
  actKeys.forEach(k=>{
    (k.actives||[]).forEach(r=>{
      const tk=k.idx+"#"+r.since;
      if(tickerEl.querySelector('[data-key="'+tk+'"]'))return;
      const sec=Math.max(0,Math.round((Date.now()-r.since)/1000));
      const dur=sec>=60?Math.floor(sec/60)+"m"+(sec%60)+"s":sec+"s";
      const el=document.createElement("span");
      el.className="ticker-item";
      el.dataset.key=tk;
      el.dataset.since=r.since;
      el.innerHTML="#"+k.idx+" "+esc(r.model)+' <span class="t-dur">'+dur+"</span>";
      tickerEl.appendChild(el);
    });
  });
  tickerLabel.style.display=curSet.size>0?"inline":"none";
  const existingOverflow=tickerEl.querySelector(".ticker-overflow");
  if(existingOverflow)existingOverflow.remove();
  if(tickerEl.scrollWidth>tickerEl.clientWidth){
    while(tickerEl.children.length>1&&tickerEl.scrollWidth>tickerEl.clientWidth){
      tickerEl.removeChild(tickerEl.lastChild);
    }
    let hiddenCount=curSet.size-tickerEl.children.length;
    if(hiddenCount>0){
      const badge=document.createElement("span");
      badge.className="ticker-overflow";
      badge.textContent="+"+hiddenCount;
      badge.title=hiddenCount+" 个并发请求未显示";
      tickerEl.prepend(badge);
      if(tickerEl.scrollWidth>tickerEl.clientWidth&&tickerEl.children.length>2){
        tickerEl.removeChild(tickerEl.children[1]);
        hiddenCount++;
        badge.textContent="+"+hiddenCount;
        badge.title=hiddenCount+" 个并发请求未显示";
      }
    }
  }
  if(sortBy==="score")filtered.sort((a,b)=>(b.healthScore||0)-(a.healthScore||0));
  else if(sortBy==="latency")filtered.sort((a,b)=>(a.avgDuration||0)-(b.avgDuration||0));
  else if(sortBy==="rate5m"){
    filtered.sort((a,b)=>{
      const ra=a.sliding5mRate!==null?a.sliding5mRate:-1;
      const rb=b.sliding5mRate!==null?b.sliding5mRate:-1;
      return rb-ra;
    });
  }else if(sortBy==="weeklyExpiry"){
    filtered.sort((a,b)=>{
      const da=a.reset==="weekly"?daysUntilResetClient(a.resetDay):99;
      const db=b.reset==="weekly"?daysUntilResetClient(b.resetDay):99;
      return da-db;
    });
  }else if(sortBy==="activatedAt"){
    filtered.sort((a,b)=>(a.activatedAt||0)-(b.activatedAt||0));
  }else if(sortBy==="duration"){
    filtered.sort((a,b)=>(b.activatedAt||0)-(a.activatedAt||0));
  }else if(sortBy==="group"){
    filtered.sort((a,b)=>(a.group||"A").localeCompare(b.group||"A"));
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
      else if(r==="hourly"){cd="本时段已用完，下一时段重置"}
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
      '<span class="badge '+C[a.reset]+'">'+(a.reset==="weekly"?("每周-"+(DAY_CN[a.resetDay]||"自动")):(a.reset==="hourly"?("每"+(a.resetHours||5)+"小时"):L[a.reset]))+'</span>'+
      (a.group&&a.group!=="A"?' <span class="badge bd-group">'+a.group+'组</span>':'')+
      (isActive?' <span class="badge bd-active">'+a.activeRequests+'并发</span>':'')+
      (isDiscard?' <span class="badge" style="background:#3b1f1e;color:#f87171;border:1px solid #ef4444">已废弃</span>':'')+
      (isBoosted?' <span class="badge" style="background:#1a3a2e;color:#4ade80;border:1px solid #22c55e">⚡ 已优先</span>':'')+
      (boostedBatch.includes(a.idx)?((a.group||"A")!=="A"?' <span class="badge" style="background:#1a3a2e;color:#ef4444;border:1px solid #ef4444;text-decoration:line-through" title="此 Key 属于 '+esc(a.group||"A")+' 组，不参与当前端口轮询">⚡ '+({"use":"队列","roundrobin":"轮询","random":"🎲 随机"}[boostedBatchMode]||"轮询")+'</span>':' <span class="badge" style="background:#1a3a2e;color:#facc15;border:1px solid #eab308">⚡ '+({"use":"队列","roundrobin":"轮询","random":"🎲 随机"}[boostedBatchMode]||"轮询")+'</span>'):'')+
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
      (a.failTime?'<div class="row"><span class="label">最后失败</span><span class="val" style="color:#f87171">'+fmtTimeAgo(Date.now()-a.failTime)+'前</span></div>':"")+
      (cd?'<div class="row"><span class="label">冷却剩余</span><span class="val cooldown">'+cd+'</span></div>':"")+
      '<div class="row"><div class="label">请求</div><div class="val">'+req+'次 (成功'+suc+' 失败'+(req-suc)+')</div></div>'+
      '<div class="row"><div class="label">流量</div><div class="val">↑'+fmtBytes(ib)+' / ↓'+fmtBytes(ob)+'</div></div>'+
      (cost?'<div class="row"><div class="label">预估费用</div><div class="val">'+cost+'</div></div>':"")+
      (avgD?'<div class="row"><div class="label">平均延迟</div><div class="val">'+avgD+'</div></div>':"")+
      (avgT?'<div class="row"><div class="label">平均首字节</div><div class="val">'+avgT+'</div></div>':"")+
      '<div class="row" style="border-top:1px solid #334155;padding-top:4px;margin-top:4px"><div class="label">P50 / P95 / P99</div><div class="val">'+p50+' / '+p95+' / '+p99+'</div></div>'+
      '<div class="row"><div class="label">滑动成功率</div><div class="val">5分钟: '+r5+' | 1小时: '+r1+'</div></div>'+
      (a.activatedAt?'<div class="row" style="border-top:1px solid #334155;padding-top:4px;margin-top:4px"><span class="label">首次启用</span><span class="val">'+fmtDate(a.activatedAt)+'</span></div>':'')+
      (a.activatedAt?'<div class="row"><span class="label">启用至今</span><span class="val">'+fmtDuration(Date.now()-a.activatedAt)+'</span></div>':'');

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
      (a.timeWindow?'<span style="margin-left:6px;font-size:10px;color:'+(a._inTimeWindow?'#4ade80':'#fb923c')+'">🕐 '+(a._inTimeWindow?'时段内':'非时段')+' | '+a.timeWindow.start+':00-'+a.timeWindow.end+':00 (UTC'+esc(a.tz||'+0')+')</span>':'')+
      '<span style="display:flex;gap:3px;align-items:center">'+
      (!isDiscard?'<span class="btn-act" onclick="cardShield('+a.idx+')" title="屏蔽此 Key（不再参与调度）">🔇</span>':'')+
      (isDiscard?'<span class="btn-act" onclick="cardReset('+a.idx+')" title="重置此 Key">🔄</span>':'')+
      (!isDiscard&&a.failCode?'<span class="btn-act" onclick="cardReset('+a.idx+')" title="重置冷却">🔄</span>':'')+
      (a.available&&!isDiscard?'<span class="btn-act'+(isBoosted?' boost-on':'')+'" onclick="boostKey('+a.idx+')" title="'+(isBoosted?'点击取消优先':'下一个请求优先使用此 Key')+'">'+(isBoosted?'✅':'⚡')+'</span>':'')+
      '<span class="btn-act" onclick="cardTest('+a.idx+')" title="测试连通性">🔍</span>'+
      '</span></div></div>';
  }
  const checkedIdxs=[...document.querySelectorAll("#grid .card-cb:checked")].map(c=>parseInt(c.dataset.idx));
  document.getElementById("grid").innerHTML=html;
  checkedIdxs.forEach(i=>{const cb=document.querySelector('#grid .card-cb[data-idx="'+i+'"]');if(cb)cb.checked=true});
  updateBatchBar();
  for(const idx in collapsedCards){const b=document.getElementById("body-"+idx);if(b)b.classList.toggle("collapsed",collapsedCards[idx])}
  updateTimeWindowBadges();
}

function toggleCollapse(idx){
  const body=document.getElementById("body-"+idx);
  if(body){body.classList.toggle("collapsed");collapsedCards[idx]=body.classList.contains("collapsed")}
}

function todayStr(){return new Date().toISOString().slice(0,10)}
function fmtBytes(n){if(!n)return"0B";if(n>=1048576)return(n/1048576).toFixed(1)+"MB";if(n>=1024)return(n/1024).toFixed(1)+"KB";return n+"B"}
function fmtDur(ms){if(ms>=1000)return(ms/1000).toFixed(2)+"s";return ms+"ms"}
function fmtDate(ts){const d=new Date(ts);return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0')+' '+String(d.getHours()).padStart(2,'0')+':'+String(d.getMinutes()).padStart(2,'0')}
function fmtDuration(ms){if(ms<=0)return'刚刚';const s=Math.floor(ms/1000),m=Math.floor(s/60),h=Math.floor(m/60),d=Math.floor(h/24);return d>0?d+'d '+(h%24)+'h':h>0?h+'h '+(m%60)+'m':m>0?m+'m '+(s%60)+'s':s+'s'}
function fmtTimeAgo(ms){if(ms<=0)return'刚刚';const s=Math.floor(ms/1000),m=Math.floor(s/60),h=Math.floor(m/60),d=Math.floor(h/24);return d>0?d+'天'+(h%24)+'小时'+(m%60)+'分钟'+(s%60)+'秒':h>0?h+'小时'+(m%60)+'分钟'+(s%60)+'秒':m>0?m+'分钟'+(s%60)+'秒':s+'秒'}
function maskKey(k){return k&&k.length>12?k.slice(0,6)+'...'+k.slice(-4):(k||'')}
function esc(s){return String(s).replace(/&/g,"&amp;").replace(/"/g,"&quot;").replace(/</g,"&lt;").replace(/>/g,"&gt;")}
const FAIL_MEAN={"401":"API Key 无效或已过期","402":"额度不足，账号已欠费","403":"权限不足，Key 无访问权限","429":"请求过频繁，触发了速率限制","500":"上游服务器内部错误","502":"上游网关错误","503":"服务暂时不可用","504":"上游超时"};
function toggleAllCollapse(){
  const all=document.querySelectorAll("#grid .cbody");
  if(!all.length)return;
  const isCollapsed=all[0].classList.contains("collapsed");
  all.forEach(b=>{b.classList.toggle("collapsed",!isCollapsed);const idx=b.id.replace("body-","");collapsedCards[idx]=!isCollapsed});
}
function showAlert(txt){document.getElementById("subText").textContent=txt}
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
      if(j.ok)alert("Key #"+idx+" 测试成功！"+(j.modelCount?" 可用模型("+j.modelCount+"个): "+j.model:"")+(j.duration?" 耗时: "+j.duration+"ms":""));
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
requireAdminToken().then(ok=>{if(ok){loadKeys();connectWS();startUpdateChecks();if(Notification.permission==="default")Notification.requestPermission();}});
setTimeout(function(){if(!data.length)httpLoad()},3000);

document.getElementById("sortBy").addEventListener("change",function(){sortBy=this.value;render()});
document.getElementById("filterBy").addEventListener("change",function(){filterBy=this.value;render()});
document.getElementById("resetFilter").addEventListener("change",function(){if(this.value!=="weekly")document.getElementById("weeklyResetDayFilter").value="all";render()});
document.getElementById("weeklyResetDayFilter").addEventListener("change",function(){render()});
document.getElementById("trendRange").addEventListener("change",function(){trendRange=this.value;render()});
document.getElementById("searchBox").addEventListener("input",function(){searchQ=this.value;render()});
document.getElementById("statusCodeBox").addEventListener("input",function(){statusCodeQ=this.value.trim();render()});
document.getElementById("modelSearchBox").addEventListener("input",function(){modelSQ=this.value.trim();render()});
document.getElementById("groupFilter").addEventListener("change",function(){groupFilter=this.value;render()});
document.getElementById("restartOverlayDismiss").addEventListener("click",dismissRestartOverlay);

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
  mgrDirty=false;mgrGroupCodeFilter=null;
  renderMgr();
  document.getElementById("mgrModal").classList.add("on");
}
function closeMgr(){if(mgrDirty&&!confirm('有未保存的变更，确定退出？\\n点击「确定」不保存退出，点击「取消」返回编辑'))return;document.getElementById("mgrModal").classList.remove("on")}
function toggleRemarkMode(){
  const rows=document.getElementById("mgrBody").children;
  for(let i=0;i<rows.length;i++){
    const r=rows[i];if(r.tagName!=="TR")continue;
    const sidx=parseInt(r.querySelector(".mgr-cb")?.value||"-1");
    if(sidx<0||sidx>=mgrKeys.length)continue;
    const el=r.querySelector(".kremark");
    if(el&&el.tagName==="INPUT")mgrKeys[sidx].remark=el.value.trim();
  }
  mgrRemarkMode=mgrRemarkMode==="remark"?"activated":"remark";
  renderMgr();
}
let mgrSearchCache=[],dragIdx=-1,grpCache=null,mgrSortBy="default",mgrRemarkMode="remark";
let mgrCollapsed={},mgrCollapsedExpandedAll=true,mgrHideShielded=true;
let mgrViewMode="default",mgrSortDir="asc",mgrDirty=false,mgrGroupCodeFilter=null;
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
function toggleHideShielded(){
  mgrHideShielded=!mgrHideShielded;
  document.getElementById("mgrHideBtn").textContent=mgrHideShielded?"🙉 显示已屏蔽":"🙈 隐藏已屏蔽";
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
  const durationDays=parseInt(document.getElementById("mgrDurationDays")?.value)||0;
  const durationInput=document.getElementById("mgrDurationDays");
  if(durationInput)durationInput.style.display=statusFilter==="duration"?"inline-block":"none";
  const resetDayFilter=document.getElementById("mgrResetDayFilter");
  const resetDayVal=resetDayFilter?resetDayFilter.value:"";
  if(resetDayFilter)resetDayFilter.style.display=statusFilter==="resetDay"?"inline-block":"none";
  const lastFailDays=parseInt(document.getElementById("mgrLastFailDays")?.value)||0;
  const lastFailInput=document.getElementById("mgrLastFailDays");
  if(lastFailInput)lastFailInput.style.display=statusFilter==="lastFail"?"inline-block":"none";
  mgrViewMode=statusFilter==="lastResp"?"lastResp":"default";
  const sortEl=document.getElementById("mgrSortBy");
  if(sortEl)sortEl.style.display=mgrViewMode==="lastResp"?"none":"inline-block";
  const thead=document.getElementById("mgrThead");
  if(mgrViewMode==="lastResp"){
    thead.innerHTML='<tr><th style="width:24px"><input type="checkbox" id="mgrSelectAll" onchange="selectAllMgr(this.checked)"></th>'+
      '<th style="width:30px">#</th><th style="min-width:140px">Key</th><th style="">URL</th><th style="width:40px">分组</th>'+
      '<th style="white-space:nowrap;cursor:pointer" onclick="toggleMgrSort(\\'lastStatus\\')">状态码<span id="mgrSortIcon_lastStatus"> '+((mgrSortBy==="lastStatus"?(mgrSortDir==="desc"?"▼":"▲"):"⇅"))+'</span></th>'+
      '<th style="white-space:nowrap;cursor:pointer" onclick="toggleMgrSort(\\'lastTime\\')">最后响应<span id="mgrSortIcon_lastTime"> '+((mgrSortBy==="lastTime"?(mgrSortDir==="desc"?"▼":"▲"):"⇅"))+'</span></th>'+
      '<th style="white-space:nowrap;cursor:pointer" onclick="toggleMgrSort(\\'lastModel\\')">响应模型<span id="mgrSortIcon_lastModel"> '+((mgrSortBy==="lastModel"?(mgrSortDir==="desc"?"▼":"▲"):"⇅"))+'</span></th>'+
      '<th style="width:100px"></th></tr>';
  }else{
    thead.innerHTML='<tr><th style="width:24px"><input type="checkbox" id="mgrSelectAll" onchange="selectAllMgr(this.checked)"></th>'+
      '<th style="width:30px">#</th><th style="min-width:140px">Key</th><th style="">URL</th><th style="width:40px">分组</th>'+
      '<th style="width:50px">状态码</th><th style="width:130px">重置</th><th style="width:50px">优先</th>'+
      '<th style="width:80px">指定模型</th><th style="width:80px">覆盖模型</th>'+
      '<th style="max-width:80px;white-space:nowrap">备注 <span onclick="toggleRemarkMode()" style="cursor:pointer;font-size:9px;color:#94a3b8;user-select:none" title="点击切换显示模式">🔄</span></th>'+
      '<th style="width:80px"></th></tr>';
  }
  const tbody=document.getElementById("mgrBody");
  tbody.innerHTML="";
  const filtered=[];const grp={};
  for(let i=0;i<mgrKeys.length;i++){
    const k=mgrKeys[i];
    if(q&&!(k.remark||"").toLowerCase().includes(q)&&!(k.url||"").toLowerCase().includes(q)&&!(k.key||"").toLowerCase().includes(q)&&!String(k._failCode||"").includes(q))continue;
    if(codeFilter){const ec=k._failCode!=null?String(k._failCode):(k._lastStatus!=null?String(k._lastStatus):"");if(ec!==codeFilter)continue;}
    if(mf&&!((k.models||k._models||[])||[]).some(m=>m.toLowerCase().includes(mf)))continue;
    if(statusFilter==="available"&&k._available!==true)continue;
    if(statusFilter==="cooldown"&&k._available!==false)continue;
    if(statusFilter==="discarded"&&k.status!=="discarded")continue;
    if(statusFilter==="locked"&&k._locked!==true)continue;
    if(statusFilter==="shielded"&&k.status!=="shielded")continue;
    if(statusFilter==="duration"&&durationDays>0){const cutoff=Date.now()-durationDays*86400000;if(!k._activatedAt||k._activatedAt>cutoff)continue;}
    if(statusFilter==="lastFail"&&lastFailDays>0){const cutoff=Date.now()-lastFailDays*86400000;if(!k._failTime||k._failTime>cutoff)continue;}
    if(statusFilter==="resetDay"&&resetDayVal!==""){if(resetDayVal==="auto"){if(k.resetDay!=null)continue;}else{if(String(k.resetDay||"")!==resetDayVal)continue;}}
    if(statusFilter==="timeIn"&&k._inTimeWindow!==true)continue;
    if(statusFilter==="timeOut"&&k._inTimeWindow!==false)continue;
    if(mgrHideShielded&&k.status==="shielded")continue;
    if(mgrGroupCodeFilter){
      let _kg;
      if(statusFilter==="resetDay"){const _dm={1:"\\u5468\\u4e00",2:"\\u5468\\u4e8c",3:"\\u5468\\u4e09",4:"\\u5468\\u56db",5:"\\u5468\\u4e94",6:"\\u5468\\u516d",7:"\\u5468\\u65e5"};_kg=k.resetDay!=null?(_dm[k.resetDay]||"\\u672a\\u77e5"):"\\u81ea\\u52a8\\uff08\\u672a\\u8bbe\\u7f6e\\uff09";}
      else{_kg=(k.remark||"").split(/[，,\\s]/)[0]||(k.url||"").replace(/https?:\\/\\//,"").slice(0,16)||"\\u672a\\u5206\\u7c7b";}
      const _ec=k._failCode!=null?String(k._failCode):(k._lastStatus!=null?String(k._lastStatus):"");
      if(_kg!==mgrGroupCodeFilter.group||_ec!==String(mgrGroupCodeFilter.code))continue;
    }
    filtered.push(i);
    let g;
    if(statusFilter==="resetDay"){
      const dayMap={1:"\\u5468\\u4e00",2:"\\u5468\\u4e8c",3:"\\u5468\\u4e09",4:"\\u5468\\u56db",5:"\\u5468\\u4e94",6:"\\u5468\\u516d",7:"\\u5468\\u65e5"};
      g=k.resetDay!=null?(dayMap[k.resetDay]||"\\u672a\\u77e5"):"\\u81ea\\u52a8\\uff08\\u672a\\u8bbe\\u7f6e\\uff09";
    }else{
      g=(k.remark||"").split(/[，,\s]/)[0]||(k.url||"").replace(/https?:\\/\\//,"").slice(0,16)||"\\u672a\\u5206\\u7c7b";
    }
    if(!grp[g])grp[g]=[];
    grp[g].push(i);
  }
  grpCache=grp;
  mgrSearchCache=filtered;
  const shieldedCount=mgrKeys.filter(k=>k.status==="shielded").length;
  document.getElementById("mgrCount").textContent="共 "+mgrKeys.length+" 个，已屏蔽 "+shieldedCount+" 个"+(filtered.length<mgrKeys.length?"，筛选后 "+filtered.length+" 个":"")+(mgrHideShielded?"（已屏蔽已隐藏）":"");
  if(mgrSortBy==="resetDay"){
    Object.keys(grp).forEach(g=>{
      grp[g].sort((a,b)=>{
        const ka=mgrKeys[a],kb=mgrKeys[b];
        const da=ka.reset==="weekly"?(parseInt(ka.resetDay)||99):99;
        const db=kb.reset==="weekly"?(parseInt(kb.resetDay)||99):99;
        return da-db;
      });
    });
  }else if(mgrSortBy==="activatedAt"){
    Object.keys(grp).forEach(g=>{grp[g].sort((a,b)=>(mgrKeys[a]._activatedAt||0)-(mgrKeys[b]._activatedAt||0))});
  }else if(mgrSortBy==="duration"){
    Object.keys(grp).forEach(g=>{grp[g].sort((a,b)=>(mgrKeys[b]._activatedAt||0)-(mgrKeys[a]._activatedAt||0))});
  }else if(mgrSortBy==="group"){
    Object.keys(grp).forEach(g=>{grp[g].sort((a,b)=>(mgrKeys[a].group||"A").localeCompare(mgrKeys[b].group||"A"))});
  }else if(mgrSortBy==="lastStatus"){
    const dir=mgrSortDir==="desc"?-1:1;
    Object.keys(grp).forEach(g=>{grp[g].sort((a,b)=>dir*((mgrKeys[a]._lastStatus??-1)-(mgrKeys[b]._lastStatus??-1)))});
  }else if(mgrSortBy==="lastTime"){
    const dir=mgrSortDir==="desc"?-1:1;
    Object.keys(grp).forEach(g=>{grp[g].sort((a,b)=>dir*((mgrKeys[b]._lastTime||0)-(mgrKeys[a]._lastTime||0)))});
  }else if(mgrSortBy==="lastModel"){
    const dir=mgrSortDir==="desc"?-1:1;
    Object.keys(grp).forEach(g=>{grp[g].sort((a,b)=>dir*((mgrKeys[a]._lastModel||"").localeCompare(mgrKeys[b]._lastModel||"")))});
  }
  const groups=Object.keys(grp);
  if(statusFilter==="resetDay"){
    const dayOrder={"周一":1,"周二":2,"周三":3,"周四":4,"周五":5,"周六":6,"周日":7,"未知":8,"自动（未设置）":9};
    groups.sort((a,b)=>(dayOrder[a]??99)-(dayOrder[b]??99));
  }
  for(let gi=0;gi<groups.length;gi++){
    const g=groups[gi],items=grp[g];
    const collapsed=mgrCollapsed[g]===true;
    const codeCounts={};
    items.forEach(idx=>{const k=mgrKeys[idx];const c=k._failCode!=null?k._failCode:(k._lastStatus!=null?k._lastStatus:null);if(c!=null)codeCounts[c]=(codeCounts[c]||0)+1;});
    const codeBadges=Object.entries(codeCounts).sort((a,b)=>b[1]-a[1]).map(([code,cnt])=>{
      const color=code>=200&&code<300?"#4ade80":code===429?"#fbbf24":code>=400&&code<500?"#fb923c":code>=500?"#f87171":code===0?"#f87171":"#94a3b8";
      const isActive=mgrGroupCodeFilter&&mgrGroupCodeFilter.group===g&&String(mgrGroupCodeFilter.code)===String(code);
      const border=isActive?'border-color:'+color:'border-color:#475569';
      const cancelBtn=isActive?'<span onclick="event.stopPropagation();mgrGroupCodeFilter=null;renderMgr();" style="cursor:pointer;color:#94a3b8;margin-left:2px" title="\\u53d6\\u6d88\\u7b5b\\u9009">\\u2715</span>':'';
      return'<span onclick="event.stopPropagation();mgrGroupCodeFilter={group:\\''+esc(g).replace(/'/g,"\\\\'")+'\\',code:'+code+'};renderMgr();" style="cursor:pointer;background:#1e293b;color:'+color+';'+border+';border-radius:3px;padding:1px 4px;font-size:10px;margin-left:4px">'+esc(String(code))+'\\u00d7'+cnt+cancelBtn+'</span>';
    }).join('');
    const hdr=document.createElement("tr");
    hdr.style.background="#1e293b";hdr.style.cursor="pointer";
    hdr.onclick=function(){toggleGroup(g)};
    const colspan=mgrViewMode==="lastResp"?9:12;
    hdr.innerHTML='<td colspan="'+colspan+'" style="padding:6px 8px;font-size:11px;font-weight:600;border-bottom:1px solid #334155;user-select:none">'+
      (collapsed?'▶':'▼')+' '+esc(g)+' ('+items.length+')'+codeBadges+'</td>';
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
      function mgrStatusBadge(code){if(code==null||code==="")return'<span style="color:#64748b">-</span>';const c=code>=200&&code<300?"#4ade80":code===429?"#fbbf24":code>=400&&code<500?"#fb923c":code>=500?"#f87171":code===0?"#f87171":"#94a3b8";return'<span style="color:'+c+';font-weight:500">'+code+'</span>';}
      const fcBadge=fc?'<span class="badge" style="background:#1e293b;color:'+(fc===429||fc==="429"?"#fbbf24":fc===401||fc==="401"?"#fb923c":fc===403||fc==="403"?"#f87171":"#94a3b8")+';border:1px solid #475569">'+fc+'</span>':(!sh&&k._available?'<span class="badge" style="background:#1e293b;color:#4ade80;border:1px solid #475569">200</span>':'');
      const actionsTd='<td style="display:flex;gap:4px;align-items:center;white-space:nowrap">'+
          '<span class="del" onclick="testKey('+i+')" title="#'+(i+1)+' 测试连通性">🔍</span>'+
          '<span class="del" onclick="resetKeyStatus('+i+')" title="#'+(i+1)+' 重置状态（清除冷却/废弃/锁死）">🔄</span>'+
          '<span class="del" onclick="toggleShield('+i+')" title="#'+(i+1)+' '+(sh?'恢复使用':'屏蔽')+'">'+(sh?'🔄':'🔇')+'</span>'+
          (lk?'<span class="del" onclick="unlockKey('+i+')" title="#'+(i+1)+' 解锁 Key" style="color:#a78bfa">🔓</span>':'')+
          '<span class="del" onclick="delKeyRow('+i+')" title="#'+(i+1)+' 删除">✕</span></td>';
      if(mgrViewMode==="lastResp"){
        const ls=k._lastStatus!=null?k._lastStatus:(k._failCode!=null?k._failCode:null);
        const lt=k._lastTime||k._failTime||null;
        const lm=k._lastModel||"";
        tr.innerHTML='<td><input type="checkbox" class="mgr-cb" value="'+i+'"></td>'+
          '<td>'+(i+1)+'</td>'+
          '<td style="display:flex;align-items:center;gap:4px"'+(k.timeWindow?' title="错峰时段: '+k.timeWindow.start+':00-'+k.timeWindow.end+':00 (UTC'+(k.tz||'+0')+') | '+(k._inTimeWindow?'时段内':'非时段')+'"':'')+'><input class="kkey" value="'+esc(k.key||"")+'" placeholder="sk-..." style="flex:1">'+badges+'</td>'+
          '<td><input class="kurl" value="'+esc(k.url||"")+'" placeholder="https://..." style="width:100%"></td>'+
          '<td><input class="kgroup" value="'+esc(k.group||"A")+'" placeholder="组名" style="width:36px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;text-align:center" title="所属分组，如 A/B/C"></td>'+
          '<td style="text-align:center;white-space:nowrap">'+mgrStatusBadge(ls)+'</td>'+
          '<td style="font-size:10px;white-space:nowrap">'+(lt?fmtTimeAgo(Date.now()-lt)+'前':'<span style="color:#64748b">未使用</span>')+'</td>'+
          '<td style="font-size:10px;white-space:nowrap;max-width:120px;overflow:hidden;text-overflow:ellipsis" title="'+esc(lm)+'">'+(lm?esc(lm):'<span style="color:#64748b">-</span>')+'</td>'+
          actionsTd;
      }else{
        tr.innerHTML='<td><input type="checkbox" class="mgr-cb" value="'+i+'"></td>'+
          '<td>'+(i+1)+'</td>'+
          '<td style="display:flex;align-items:center;gap:4px"'+(k.timeWindow?' title="错峰时段: '+k.timeWindow.start+':00-'+k.timeWindow.end+':00 (UTC'+(k.tz||'+0')+') | '+(k._inTimeWindow?'时段内':'非时段')+'"':'')+'><input class="kkey" value="'+esc(k.key||"")+'" placeholder="sk-..." style="flex:1">'+badges+'</td>'+
          '<td><input class="kurl" value="'+esc(k.url||"")+'" placeholder="https://..." style="width:100%"></td>'+
          '<td><input class="kgroup" value="'+esc(k.group||"A")+'" placeholder="组名" style="width:36px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;text-align:center" title="所属分组，如 A/B/C"></td>'+
          '<td style="text-align:center">'+fcBadge+'</td>'+
          '<td style="display:flex;gap:4px;align-items:center">'+
          '<select class="kreset" onchange="var d=this.parentNode.querySelector(\\'.kresetday\\');var h=this.parentNode.querySelector(\\'.kresethours\\');d&&(d.style.display=this.value===\\'weekly\\'?\\'inline-block\\':\\'none\\');h&&(h.style.display=this.value===\\'hourly\\'?\\'inline-block\\':\\'none\\')"><option value="daily"'+(k.reset==="daily"?" selected":"")+'>每日</option><option value="weekly"'+(k.reset==="weekly"?" selected":"")+'>每周</option><option value="hourly"'+(k.reset==="hourly"?" selected":"")+'>每N小时</option><option value="never"'+(k.reset==="never"?" selected":"")+'>永久</option></select>'+
          '<input class="kresethours" type="number" min="1" max="168" style="display:'+(k.reset==="hourly"?"inline-block":"none")+';width:40px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px" value="'+(k.resetHours||"")+'" placeholder="h">'+
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
          '<td>'+(mgrRemarkMode==="activated"&&k._activatedAt?'<span class="kremark" style="font-size:10px;color:#94a3b8;cursor:default"'+(k.remark?' title="'+esc(k.remark)+'"':'')+'>'+fmtDate(k._activatedAt)+' / '+fmtDuration(Date.now()-k._activatedAt)+'</span>':'<input class="kremark" value="'+esc(k.remark||"")+'" placeholder="备注" style="width:100%"'+(k._activatedAt?' title="首次启用: '+fmtDate(k._activatedAt)+' | 启用至今: '+fmtDuration(Date.now()-k._activatedAt)+'"':'')+'>')+'</td>'+
          actionsTd;
      }
      tbody.appendChild(tr);
    }
  }
  document.getElementById("mgrSelectAll").checked=false;
  if(mgrGroupCodeFilter){setTimeout(()=>{document.querySelectorAll("#mgrBody .mgr-cb").forEach(c=>c.checked=true);const sa=document.getElementById("mgrSelectAll");if(sa)sa.checked=true;},0);}
}
function toggleMgrSort(field){
  if(mgrSortBy===field){mgrSortDir=mgrSortDir==="asc"?"desc":"asc";}
  else{mgrSortBy=field;mgrSortDir="asc";}
  renderMgr();
}
function cleanFailedKeys(){
  const days=prompt("清理条件：最后响应距今 ≥ ? 天","7");
  if(days===null)return;
  const d=parseInt(days)||0;
  if(d<=0){alert("请输入正整数天数");return}
  const cutoff=Date.now()-d*86400000;
  let count=0;
  document.querySelectorAll("#mgrBody .mgr-cb").forEach(cb=>{
    const i=parseInt(cb.value);
    const k=mgrKeys[i];
    if(!k)return;
    const badStatus=k._lastStatus!=null&&(k._lastStatus>=400||k._lastStatus===0);
    const oldTime=k._lastTime&&k._lastTime<cutoff;
    const neverUsed=!k._lastTime;
    if(badStatus&&(oldTime||neverUsed)){cb.checked=true;count++;}
    else cb.checked=false;
  });
  alert("已选中 "+count+" 个符合条件的 Key\\n（最后响应≥"+d+"天 且 状态码≥400 或 网络错误）\\n\\n可使用「批量屏蔽」处理");
}
function addKeyRow(){mgrKeys.push({key:"",url:"",reset:"weekly",remark:"",priority:0,models:[],model:null,resetDay:void 0,resetHours:void 0,group:"A"});mgrDirty=true;renderMgr()}
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
  setTimeout(function(){var a=collectMgr();if(a.length)fetch("http://localhost:3456/__keys",{method:"PUT",headers:{"content-type":"application/json"},body:JSON.stringify(a,null,2)}).then(r=>r.json()).then(j=>{if(j.ok){mgrDirty=false;loadKeys()}})},100);
}
function clearMgrSearch(){
  document.getElementById("mgrSearch").value="";
  document.getElementById("mgrCodeFilter").value="";
  document.getElementById("mgrModelFilter").value="";
  document.getElementById("mgrStatusFilter").value="";
  const durEl=document.getElementById("mgrDurationDays");if(durEl)durEl.value="";
  const lfEl=document.getElementById("mgrLastFailDays");if(lfEl)lfEl.value="";
  const rdEl=document.getElementById("mgrResetDayFilter");if(rdEl)rdEl.value="";
  mgrSortBy="default";mgrSortDir="asc";mgrGroupCodeFilter=null;
  document.getElementById("mgrSortBy").value="default";
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
  mgrDirty=true;
  renderMgr();
}
function batchResetMgr(){
  const sel=getSelectedMgr();
  if(!sel.length){alert("请先勾选要重置的 Key");return}
  sel.forEach(i=>{
    mgrKeys[i].status="active";
    fetch("http://localhost:3456/__reset-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx:i+1})}).catch(()=>{});
  });
  mgrDirty=true;
  renderMgr();
}
function batchDeleteMgr(){
  const sel=getSelectedMgr();
  if(!sel.length){alert("请先勾选要删除的 Key");return}
  if(!confirm('确定要删除选中的 '+sel.length+' 个 Key？\\n删除后不再显示和调用，可在 keys.json 中恢复。'))return;
  sel.forEach(i=>{mgrKeys[i].status="deleted"});
  renderMgr();
  setTimeout(function(){var a=collectMgr();if(a.length)fetch("http://localhost:3456/__keys",{method:"PUT",headers:{"content-type":"application/json"},body:JSON.stringify(a,null,2)}).then(r=>r.json()).then(j=>{if(j.ok){mgrDirty=false;loadKeys()}})},100);
}
function batchSetTimeWindow(){
  const sel=getSelectedMgr();
  if(!sel.length){alert("请先勾选要设置错峰时段的 Key");return}
  const tzOpts=Array.from({length:25},(_, i)=>i-12).map(v=>'<option value="'+(v>=0?"+":"")+v+'">UTC'+(v>=0?"+":"")+v+'</option>').join('');
  const hourOpts=Array.from({length:24},(_, i)=>'<option value="'+i+'">'+(i<10?'0':'')+i+':00</option>').join('');
  const html='<div style="padding:12px;background:#1e293b;border-radius:8px;min-width:300px">'+
    '<div style="margin-bottom:8px;color:#e2e8f0;font-size:13px;font-weight:500">设置选中 '+sel.length+' 个 Key 的错峰时段</div>'+
    '<div style="margin-bottom:10px;font-size:11px;color:#94a3b8;line-height:1.7;padding:8px 10px;background:#0f172a;border:1px solid #334155;border-radius:4px">设定后仅在该时段内参与调度（按所选时区，24 小时制）。<br>开始&lt;结束=同天时段（如 08-17）；开始&gt;结束=跨夜时段（如 22-08）；开始==结束=全天可用。</div>'+
    '<div style="display:grid;grid-template-columns:auto 1fr;gap:8px;font-size:12px;align-items:center">'+
    '<label style="color:#94a3b8">时区</label><select id="twTz" onchange="updateTwPreview()" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px;border-radius:4px">'+tzOpts+'</select>'+
    '<label style="color:#94a3b8">开始</label><select id="twStart" onchange="updateTwPreview()" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px;border-radius:4px">'+hourOpts+'</select>'+
    '<label style="color:#94a3b8">结束</label><select id="twEnd" onchange="updateTwPreview()" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px;border-radius:4px">'+hourOpts+'</select>'+
    '</div>'+
    '<div id="twPreview" style="margin-top:10px;padding:8px 10px;background:#0f172a;border:1px solid #334155;border-radius:4px;font-size:12px;color:#cbd5e1;line-height:1.7"></div>'+
    '<div style="margin-top:12px;display:flex;gap:8px;justify-content:flex-end">'+
    '<button class="btn" onclick="this.closest(\\'.modal-cover\\').remove()">取消</button>'+
    '<button class="btn btn-p" onclick="confirmSetTimeWindow()">确认设置</button>'+
    '</div></div>';
  const cover=document.createElement('div');
  cover.className='modal-cover';
  cover.style.cssText='position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.6);display:flex;align-items:center;justify-content:center;z-index:10000';
  cover.innerHTML=html;
  cover.addEventListener('click',e=>{if(e.target===cover)cover.remove()});
  document.body.appendChild(cover);
  document.getElementById('twTz').value='+0';
  updateTwPreview();
}
function updateTwPreview(){
  const tzEl=document.getElementById('twTz'),startEl=document.getElementById('twStart'),endEl=document.getElementById('twEnd');
  const pre=document.getElementById('twPreview');
  if(!tzEl||!startEl||!endEl||!pre)return;
  const tz=tzEl.value,start=parseInt(startEl.value),end=parseInt(endEl.value);
  const windowDesc=start===end
    ? '全时段（无限制，等效清除）'
    : (start<end?start+':00-'+end+':00（同天窗口）':start+':00-'+end+':00（跨夜窗口）');
  const inWin=isInTimeWindow({timeWindow:{start,end},tz});
  pre.innerHTML='时段：<b style="color:#e2e8f0">'+windowDesc+'</b>（'+tz+'，24 小时制）<br>当前：<b style="color:'+(inWin?'#4ade80':'#fb923c')+'">'+(inWin?'时段内 ✔（可参与调度）':'非时段 ✖（不参与调度）')+'</b>';
}
function confirmSetTimeWindow(){
  const sel=getSelectedMgr();
  const tz=document.getElementById('twTz').value;
  const start=parseInt(document.getElementById('twStart').value);
  const end=parseInt(document.getElementById('twEnd').value);
  sel.forEach(i=>{
    mgrKeys[i].tz=tz;
    mgrKeys[i].timeWindow={start,end};
    fetch("http://localhost:3456/__patch-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx:i+1,tz,timeWindow:{start,end}})}).catch(()=>{});
  });
  document.querySelector('.modal-cover')?.remove();
  mgrDirty=true;
  renderMgr();
  setTimeout(loadKeys,200);
}
function batchClearTimeWindow(){
  const sel=getSelectedMgr();
  if(!sel.length){alert("请先勾选要清除时段的 Key");return}
  if(!confirm('确定清除选中的 '+sel.length+' 个 Key 的时段设置？'))return;
  sel.forEach(i=>{
    delete mgrKeys[i].tz;
    delete mgrKeys[i].timeWindow;
    fetch("http://localhost:3456/__patch-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx:i+1,tz:null,timeWindow:null})}).catch(()=>{});
  });
  mgrDirty=true;
  renderMgr();
  setTimeout(loadKeys,200);
}
function collectMgr(){
  const result=mgrKeys.map(k=>({key:k.key,url:k.url,reset:k.reset,remark:k.remark||"",priority:k.priority||0,models:k.models||[],model:k.model||null,group:k.group||"A",resetDay:k.resetDay||void 0,resetHours:k.resetHours>0?k.resetHours:void 0,activatedAt:k.activatedAt||void 0,status:k.status&&k.status!=="active"?k.status:void 0,tz:k.tz||void 0,timeWindow:k.timeWindow||void 0}));
  const rows=document.getElementById("mgrBody").children;
  for(let i=0;i<rows.length;i++){
    const r=rows[i];
    if(r.tagName!=="TR")continue;
    const sidx=parseInt(r.querySelector(".mgr-cb")?.value||"-1");
    if(sidx<0||sidx>=result.length)continue;
    const key=r.querySelector(".kkey")?.value.trim();
    if(!key)continue;
    result[sidx].key=key;
    const urlEl=r.querySelector(".kurl");
    result[sidx].url=urlEl?urlEl.value.trim():result[sidx].url;
    const resetEl=r.querySelector(".kreset");
    result[sidx].reset=resetEl?resetEl.value:result[sidx].reset;
    const resetDayEl=r.querySelector(".kresetday");
    result[sidx].resetDay=resetDayEl?resetDayEl.value||void 0:void 0;
    const resetHoursEl=r.querySelector(".kresethours");
    result[sidx].resetHours=resetHoursEl?parseInt(resetHoursEl.value)||void 0:void 0;
    const prioEl=r.querySelector(".kprio");
    result[sidx].priority=prioEl?parseInt(prioEl.value)||0:result[sidx].priority;
    const modelsEl=r.querySelector(".kmodels");
    const raw=modelsEl?modelsEl.value.trim():"";
    result[sidx].models=raw?raw.split(",").map(s=>s.trim()).filter(Boolean):[];
    const groupEl=r.querySelector(".kgroup");
    result[sidx].group=groupEl?groupEl.value.trim().toUpperCase()||"A":result[sidx].group||"A";
    const modelEl=r.querySelector(".kmodel");
    result[sidx].model=modelEl?modelEl.value.trim()||null:result[sidx].model;
    const remEl=r.querySelector(".kremark");result[sidx].remark=remEl&&remEl.tagName==="INPUT"?remEl.value.trim():(mgrKeys[sidx].remark||"");
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
    mgrDirty=false;
    closeMgr();loadKeys();
  }catch(e){alert("保存失败: "+e.message)}
}
function openImportMgr(){document.getElementById("importMgrTxt").value="";document.getElementById("importMgrCover").style.display="flex"}
function closeImportMgr(){document.getElementById("importMgrCover").style.display="none"}
function doImportKeys(){
  const txt=document.getElementById("importMgrTxt").value;
  if(!txt.trim()){alert("请粘贴 Key 数据");return}
  const lines=txt.trim().split("\\n").filter(l=>l.trim());
  let added=0,skipped=0;
  for(const line of lines){
    const parts=line.trim().split(/\\s+/);
    if(!parts[0]||!parts[0].startsWith("sk-")){skipped++;continue}
    const key=parts[0];
    if(!parts[1]||!parts[1].startsWith("http")){skipped++;continue}
    const url=parts[1];
    const resetMap={"daily":"daily","weekly":"weekly","never":"never","hourly":"hourly","每日":"daily","每周":"weekly","永久":"never","每N小时":"hourly"};
    const reset=resetMap[parts[2]]||"weekly";
    const priority=parseInt(parts[3])||0;
    const group=(parts[4]||"A").toUpperCase();
    const remark=parts.slice(5).join(" ")||"";
    mgrKeys.push({key,url,reset,remark,priority,group,models:[],model:null,resetDay:void 0,resetHours:void 0});
    added++;
  }
  if(added){mgrDirty=true;renderMgr()}
  closeImportMgr();
  alert("成功添加 "+added+" 个 Key"+(skipped>0?"，"+skipped+" 行被跳过（格式错误或缺少 URL）":""));
}
function openExportMgr(){document.getElementById("exportMgrCover").style.display="flex"}
function closeExportMgr(){document.getElementById("exportMgrCover").style.display="none"}
function doExportCSV(){
  const fields=["key","url"];
  document.querySelectorAll(".exp-f:checked").forEach(el=>fields.push(el.value));
  const esc=v=>'"'+String(v==null?"":v).replace(/"/g,'""')+'"';
  const headers=["key","url"];
  const labels={"key":"Key","url":"URL","reset":"reset","priority":"priority","group":"group","remark":"remark","models":"models","model":"model","resetDay":"resetDay","tz":"tz","timeWindow":"timeWindow"};
  const sel=getSelectedMgr();
  if(!sel.length){alert("请先勾选要导出的 Key");return}
  const rows=sel.map(i=>mgrKeys[i]).filter(k=>k&&k.key).map(k=>{
    return fields.map(f=>{
      if(f==="timeWindow")return k.timeWindow?(k.timeWindow.start+":"+k.timeWindow.end):"";
      if(f==="models")return(k.models||[]).join(";");
      return k[f]||"";
    }).map(esc).join(",");
  });
  const csv="\uFEFF"+fields.map(f=>labels[f]||f).join(",")+"\\n"+rows.join("\\n");
  const blob=new Blob([csv],{type:"text/csv;charset=utf-8"});
  const a=document.createElement("a");
  const d=new Date();
  a.download="keys_export_"+d.getFullYear()+String(d.getMonth()+1).padStart(2,"0")+String(d.getDate()).padStart(2,"0")+".csv";
  a.href=URL.createObjectURL(blob);
  a.click();
  URL.revokeObjectURL(a.href);
  closeExportMgr();
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
    if(j.ok){alert("Key #"+(i+1)+" 测试成功！"+(j.modelCount?" 可用模型("+j.modelCount+"个): "+j.model:"")+(j.duration?" 耗时: "+j.duration+"ms":""))}
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
         const dur=j.duration||0;let durc="#22c55e";if(dur>=3000)durc="#ef4444";else if(dur>=1000)durc="#eab308";line.textContent="✅ #"+(i+1)+" 成功"+(j.modelCount?" 可用模型("+j.modelCount+"个): "+j.model:"")+" ("+dur+"ms)";line.style.color=durc;
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
function openExportCover(){document.getElementById("exportCover").style.display="flex"}
function closeExportCover(){document.getElementById("exportCover").style.display="none"}
function doFrontendExport(){
  const fields=["key","url"];
  document.querySelectorAll(".exp-cfg:checked,.exp-st:checked").forEach(el=>fields.push(el.value));
  if(!filtered.length){alert("当前没有可见的 Key 可导出");return}
  const esc=v=>'"'+String(v==null?"":v).replace(/"/g,'""')+'"';
  const labels={key:"Key",url:"URL",reset:"reset",remark:"remark",group:"group",priority:"priority",models:"指定模型",model:"覆盖模型",resetDay:"resetDay",tz:"tz",timeWindow:"时段",status:"状态",failCode:"失败码",totalRequests:"请求数",successRequests:"成功数",failRequests:"失败数",inputBytes:"输入字节",outputBytes:"输出字节",avgDuration:"平均耗时",healthScore:"健康分",totalCost:"费用"};
  const rows=filtered.map(k=>{
    return fields.map(f=>{
      if(f==="timeWindow")return k.timeWindow?(k.timeWindow.start+":"+k.timeWindow.end):"";
      if(f==="models")return(k.models||[]).join(";");
      return k[f]==null?"":k[f];
    }).map(esc).join(",");
  });
  const csv="\uFEFF"+fields.map(f=>labels[f]||f).join(",")+"\\n"+rows.join("\\n");
  const blob=new Blob([csv],{type:"text/csv;charset=utf-8"});
  const a=document.createElement("a");
  const d=new Date();
  a.download="openapi-export-"+d.getFullYear()+String(d.getMonth()+1).padStart(2,"0")+String(d.getDate()).padStart(2,"0")+".csv";
  a.href=URL.createObjectURL(blob);
  a.click();
  URL.revokeObjectURL(a.href);
  closeExportCover();
}
// --- Log panel state ---
let logCurrentPage = 1;
let logAllEntries = [];
let logLastStats = null;
let logSortField = "time";
let logSortAsc = false;

function logRequestFailed(e){
  return !!e && (e.status === 0 || e.status >= 400 || e.streamOutcome === "failed");
}
function closeLogKeyPopup(){
  document.getElementById("logKeyPopup").style.display="none";
}
async function openLogKeyPopup(idx, event){
  event.stopPropagation();
  const popup = document.getElementById("logKeyPopup");
  const entries = logAllEntries.filter(e => e.idx === parseInt(idx));
  const reqs = entries.filter(e => e.type !== "event");
  const events = entries.filter(e => e.type === "event");
  const total = reqs.length;
  const success = reqs.filter(e => e.status >= 200 && e.status < 300 && !logRequestFailed(e)).length;
  const err4xx = reqs.filter(e => e.status >= 400 && e.status < 500).length;
  const err5xx = reqs.filter(e => e.status >= 500).length;
  const timeout = reqs.filter(e => e.status === 0 || e.status == null).length;
  const streamFailed = reqs.filter(e => e.streamOutcome === "failed").length;
  const durs = reqs.filter(e => e.duration != null).map(e => e.duration).sort((a,b)=>a-b);
  const avgDur = durs.length ? Math.round(durs.reduce((a,b)=>a+b,0)/durs.length) : 0;
  const p95 = durs.length ? durs[Math.floor(durs.length*0.95)]||durs[durs.length-1] : 0;
  // Models used
  const models = {};
  reqs.forEach(e => { const m=e.overrideModel||e.reqModel||"(未知)"; models[m]=(models[m]||0)+1; });
  const modelStr = Object.keys(models).sort((a,b)=>models[b]-models[a]).map(m => esc(m)+"("+models[m]+")").join(", ");
  const eventsStr = events.map(e => new Date(e.time).toTimeString().slice(0,5)+" "+e.eventType+": "+(e.message||"")).join("\\n");
  document.getElementById("logKeyPopupTitle").textContent = "Key #"+idx+" 统计";
  document.getElementById("logKeyPopupBody").innerHTML = 
    '<div class="stat-row"><span class="l">请求数</span><span class="r">'+total+'</span></div>'
    + '<div class="stat-row"><span class="l">成功</span><span class="r" style="color:#22c55e">'+success+' ('+(total?Math.round(success/total*100):0)+'%)</span></div>'
    + '<div class="stat-row"><span class="l">4xx</span><span class="r" style="color:'+(err4xx?'#f59e0b':'#94a3b8')+'">'+err4xx+'</span></div>'
    + '<div class="stat-row"><span class="l">5xx</span><span class="r" style="color:'+(err5xx?'#ef4444':'#94a3b8')+'">'+err5xx+'</span></div>'
    + '<div class="stat-row"><span class="l">超时</span><span class="r" style="color:'+(timeout?'#64748b':'#94a3b8')+'">'+timeout+'</span></div>'
    + '<div class="stat-row"><span class="l">流失败</span><span class="r" style="color:'+(streamFailed?'#ef4444':'#94a3b8')+'">'+streamFailed+'</span></div>'
    + '<div class="stat-row"><span class="l">平均耗时</span><span class="r">'+fmtDur(avgDur)+'</span></div>'
    + '<div class="stat-row"><span class="l">P95</span><span class="r">'+fmtDur(p95)+'</span></div>'
    + '<div style="margin-top:6px;padding-top:6px;border-top:1px solid #334155;font-size:10px;color:#94a3b8">📊 模型: '+modelStr+'</div>'
    + (events.length ? '<div style="margin-top:4px;padding-top:4px;border-top:1px solid #334155;font-size:10px;color:#60a5fa;white-space:pre-wrap">📌 事件:\\n'+esc(eventsStr)+'</div>' : '');
  // Position popup near the click
  popup.style.display="block";
  popup.style.left=Math.min(event.clientX, window.innerWidth-420)+"px";
  popup.style.top=Math.min(event.clientY, window.innerHeight-350)+"px";
}
// Click outside to close popup
document.addEventListener("click", function(e){
  const popup = document.getElementById("logKeyPopup");
  if (popup && popup.style.display === "block" && !popup.contains(e.target)) {
    popup.style.display = "none";
  }
});

// The log viewer keeps only the visible page in the DOM. Historical scans run in
// the server worker; the dashboard never asks the main proxy process to count a file.
const LOG_FAST_PAGE_SIZE = 50;
const LOG_HISTORY_PAGE_SIZE = 100;
let logCursorStack = [""];
let logHasMore = false;
let logQueryMode = "recent";
let logQueryTruncated = false;
let logRecentSource = "memory";
let logRecentHistoryChecked = false;
let logRecentHistoryUnavailable = false;
let logRecentHistoryFilesAvailable = 0;
let logRecentHistoryFilesScanned = 0;
let logHistoryFilesAvailable = 0;
let logHistoryFilesScanned = 0;
let logForceHistory = false;
let logOverview = null;
let logAbortController = null;
let logRequestSerial = 0;
let logLivePending = [];
let logLiveFlushTimer = null;
let logOverviewRefreshTimer = null;
let logOperations = { incidents: [], groupPauses: [], summaryRebuild: { phase: "idle" } };
let logSubscriptionEnabled = false;
let logIncidentRefreshInFlight = false;
let logIncidentRefreshStatus = "";

function isLogModalOpen(){
  const modal=document.getElementById("logModal");
  return !!modal&&modal.classList.contains("on");
}

function setLogSubscription(enabled){
  const next=enabled===true;
  if(ws&&ws.readyState===1&&logSubscriptionEnabled!==next){
    try{ws.send(JSON.stringify({type:"log_subscribe",enabled:next}));}catch(e){}
  }
  logSubscriptionEnabled=next;
  const badge=document.getElementById("logRealTimeBadge");
  if(!badge)return;
  if(!isLogModalOpen()||enabled!==true){badge.textContent="● 未订阅";badge.style.color="#64748b";return;}
  badge.textContent=logQueryMode==="recent"?"● 实时订阅":"● 实时已订阅（历史筛选）";
  badge.style.color="#4ade80";
}

function buildLogFilterQuery(){
  const p=new URLSearchParams();
  const add=(id,name)=>{const el=document.getElementById(id);const value=el&&el.value?el.value.trim():"";if(value)p.set(name,value);};
  add("logKeyFilter","key");
  add("logStatusFilter","status");
  add("logModelFilter","model");
  add("logUpstreamFilter","upstream");
  add("logPathFilter","path");
  add("logGroupFilter","group");
  add("logSearch","q");
  const time=document.getElementById("logTimeFilter")?.value;
  if(time==="custom"){
    const start=document.getElementById("logStartTime")?.value;
    const end=document.getElementById("logEndTime")?.value;
    const since=start?new Date(start).getTime():NaN;
    const until=end?new Date(end).getTime():NaN;
    if(Number.isFinite(since))p.set("since",String(since));
    if(Number.isFinite(until))p.set("until",String(until));
  }else if(time){
    const ranges={"5m":3e5,"15m":9e5,"1h":36e5,"24h":864e5,"7d":6048e5,"30d":2592e6};
    if(ranges[time])p.set("since",String(Date.now()-ranges[time]));
  }
  return p;
}

function logQueryHasFilters(query){
  return ["key","status","model","upstream","path","group","q","since","until"].some(name=>query.has(name));
}

function closeLogs(){
  const modal=document.getElementById("logModal");
  if(modal)modal.classList.remove("on");
  if(logAbortController){logAbortController.abort();logAbortController=null;}
  if(logLiveFlushTimer){clearTimeout(logLiveFlushTimer);logLiveFlushTimer=null;}
  if(logOverviewRefreshTimer){clearTimeout(logOverviewRefreshTimer);logOverviewRefreshTimer=null;}
  logLivePending=[];
  setLogSubscription(false);
}

async function openLogs(){
  document.getElementById("logModal").classList.add("on");
  logForceHistory=false;
  logCursorStack=[""];
  logCurrentPage=1;
  logHasMore=false;
  logQueryTruncated=false;
  logIncidentRefreshStatus="";
  setLogSubscription(true);
  refreshLogOperations();
  await loadLogs({reset:true});
}

async function loadLogs(options){
  const opts=options&&typeof options==="object"?options:{};
  const query=buildLogFilterQuery();
  const requestedMode=(logForceHistory||logQueryHasFilters(query))?"history":"recent";
  if(opts.reset||requestedMode!==logQueryMode){
    logCursorStack=[""];
    logCurrentPage=1;
    logHasMore=false;
    logQueryTruncated=false;
  }
  const pageIndex=requestedMode==="history"?Math.max(0,Number.isInteger(opts.pageIndex)?opts.pageIndex:logCurrentPage-1):0;
  if(requestedMode==="history"&&pageIndex>0&&!logCursorStack[pageIndex])return;
  query.set("mode",requestedMode);
  query.set("limit",String(requestedMode==="history"?LOG_HISTORY_PAGE_SIZE:LOG_FAST_PAGE_SIZE));
  if(requestedMode==="history"&&logCursorStack[pageIndex])query.set("cursor",logCursorStack[pageIndex]);
  if(logAbortController)logAbortController.abort();
  const controller=new AbortController();
  logAbortController=controller;
  const serial=++logRequestSerial;
  const hint=document.getElementById("logQueryHint");
  if(hint)hint.textContent=requestedMode==="history"?"正在读取历史日志...":"正在读取最新日志...";
  try{
    const response=await fetch("/__logs?"+query.toString(),{cache:"no-store",signal:controller.signal});
    const payload=await response.json();
    if(serial!==logRequestSerial||controller.signal.aborted)return;
    if(!response.ok)throw new Error(payload&&payload.error?payload.error:"日志查询失败");
    logAllEntries=Array.isArray(payload.entries)?payload.entries:[];
    logLastStats=payload.stats||null;
    logOverview=payload.overview||logOverview;
    logQueryMode=payload.mode||requestedMode;
    logQueryTruncated=payload.truncated===true;
    if(logQueryMode==="recent"){
      logRecentSource=payload.source==="history"?"history":"memory";
      logRecentHistoryChecked=payload.historyChecked===true;
      logRecentHistoryUnavailable=payload.historyUnavailable===true;
      logRecentHistoryFilesAvailable=Math.max(0,Math.floor(Number(payload.historyFilesAvailable)||0));
      logRecentHistoryFilesScanned=Math.max(0,Math.floor(Number(payload.historyFilesScanned)||0));
    }else{
      logHistoryFilesAvailable=Math.max(0,Math.floor(Number(payload.filesAvailable)||0));
      logHistoryFilesScanned=Math.max(0,Math.floor(Number(payload.filesScanned)||0));
    }
    if(logQueryMode==="history"){
      logCurrentPage=pageIndex+1;
      logHasMore=payload.hasMore===true&&!!payload.nextCursor;
      if(logHasMore)logCursorStack[pageIndex+1]=payload.nextCursor;
      else logCursorStack=logCursorStack.slice(0,pageIndex+1);
    }else{
      logCurrentPage=1;
      logHasMore=false;
      logCursorStack=[""];
    }
    if(payload.overview)applyLogIncidentData({incidents:payload.overview.incidents||[],groupPauses:payload.overview.groupPauses||[]});
    renderLogs();
  }catch(error){
    if(error&&error.name==="AbortError")return;
    if(serial!==logRequestSerial)return;
    if(hint)hint.textContent="日志查询失败："+(error&&error.message?error.message:"未知错误");
  }finally{
    if(logAbortController===controller)logAbortController=null;
  }
}

function reloadLogs(){loadLogs({reset:true});}

function toggleLogHistory(){
  if(logQueryHasFilters(buildLogFilterQuery()))return;
  logForceHistory=!logForceHistory;
  loadLogs({reset:true});
}

function logPage(delta){
  if(logQueryMode!=="history")return;
  const currentIndex=logCurrentPage-1;
  if(delta<0&&currentIndex>0)loadLogs({pageIndex:currentIndex-1});
  if(delta>0&&logHasMore)loadLogs({pageIndex:currentIndex+1});
}

function renderLogStats(){
  const stats=(logOverview&&logOverview.stats)||logLastStats;
  if(!stats)return;
  const set=(id,value)=>{const el=document.getElementById(id);if(el)el.textContent=value;};
  set("lsTotal",stats.total||0);
  set("lsSuccess",stats.total>0?stats.successRate+"%":"-");
  const success=document.getElementById("lsSuccess");
  if(success)success.style.color=stats.successRate>=95?"#22c55e":stats.successRate>=80?"#f59e0b":"#ef4444";
  set("lsAvgDur",stats.avgDuration?fmtDur(stats.avgDuration):"-");
  set("lsP95",stats.p95?fmtDur(stats.p95):"-");
  set("lsP99",stats.p99?fmtDur(stats.p99):"-");
  set("ls4xx",stats.error4xx||0);
  set("ls5xx",stats.error5xx||0);
  set("lsTimeout",stats.errorTimeout||0);
  const four=document.getElementById("ls4xx");
  const five=document.getElementById("ls5xx");
  if(four)four.style.color=stats.error4xx>0?"#f59e0b":"#94a3b8";
  if(five)five.style.color=stats.error5xx>0?"#ef4444":"#94a3b8";
}

function renderLogs(){
  const tbody=document.getElementById("logBody");
  if(!tbody)return;
  tbody.innerHTML=logAllEntries.length
    ?logAllEntries.map((entry,index)=>'<tr class="'+logRowClass(entry)+'" data-log-index="'+index+'" onclick="toggleLogDetail(this,'+index+')">'+makeLogRow(entry,index+1)+"</tr>").join("")
    :'<tr><td colspan="12" style="text-align:center;color:#64748b;padding:20px">暂无记录</td></tr>';
  const previous=document.getElementById("logPrevBtn");
  const next=document.getElementById("logNextBtn");
  const hasLogFilters=logQueryHasFilters(buildLogFilterQuery());
  if(previous){previous.disabled=logQueryMode!=="history"||logCurrentPage<=1;previous.title=logQueryMode!=="history"?"默认视图仅显示最新记录；点击“浏览历史”后可分页":"已是最早可返回的历史页";}
  if(next){next.disabled=logQueryMode!=="history"||!logHasMore;next.title=logQueryMode!=="history"?"默认视图仅显示最新记录；点击“浏览历史”后可分页":logHasMore?"查看更早的历史记录":"没有更早的历史记录";}
  const browseHistory=document.getElementById("logBrowseHistoryBtn");
  if(browseHistory){
    browseHistory.style.display=hasLogFilters?"none":"";
    browseHistory.textContent=logQueryMode==="history"&&logForceHistory?"最新视图":"浏览历史";
    browseHistory.title=logQueryMode==="history"&&logForceHistory?"返回仅显示最新记录的即时视图":"切换为可分页的历史日志视图";
  }
  const pageInfo=document.getElementById("logPageInfo");
  if(pageInfo)pageInfo.textContent=logQueryMode==="history"?"历史第 "+logCurrentPage+" 页":"最新 "+LOG_FAST_PAGE_SIZE+" 条"+(logRecentSource==="history"?"（历史尾部）":"");
  const hint=document.getElementById("logQueryHint");
  const historyFileHint=(available,scanned)=>available>0?"发现 "+available+" 个保存日志文件，已扫描 "+Math.min(available,scanned)+" 个":"未发现符合格式的保存日志文件";
  if(hint)hint.textContent=logQueryMode==="history"?(logQueryTruncated?"历史查询已达到本页扫描边界":"历史查询 · "+historyFileHint(logHistoryFilesAvailable,logHistoryFilesScanned)+" · 游标翻页"):(logRecentSource==="history"?"已载入保存日志尾部（"+historyFileHint(logRecentHistoryFilesAvailable,logRecentHistoryFilesScanned)+"）· 实时订阅":logRecentHistoryUnavailable?"保存日志暂不可读取 · 显示内存记录 · 实时订阅":logRecentHistoryChecked?"已检查保存日志（"+historyFileHint(logRecentHistoryFilesAvailable,logRecentHistoryFilesScanned)+"），未找到可读记录 · 实时订阅":"内存尾部 · 实时订阅");
  setLogSubscription(isLogModalOpen());
  renderLogStats();
  renderLogSparkline();
  renderLogModelDist();
  renderLogErrorCluster();
  renderLogOperations();
}

function renderLogSparkline(){
  const wrap=document.getElementById("logSparklineWrap");
  if(!wrap)return;
  const timeline=logOverview&&Array.isArray(logOverview.timeline)?logOverview.timeline:[];
  const max=Math.max(1,...timeline.map(item=>Number(item.total)||0));
  wrap.innerHTML=timeline.map(item=>{
    const total=Number(item.total)||0;
    const errors=Number(item.errors)||0;
    const height=Math.max(1,Math.round(total/max*24));
    const color=errors>0?"#ef4444":total>0?"#3b82f6":"#334155";
    const tip=new Date(item.time).toTimeString().slice(0,5)+" - 请求:"+total+" 错误:"+errors;
    return '<div class="log-spark-bar" style="height:'+height+'px;background:'+color+'" title="'+esc(tip)+'"></div>';
  }).join("");
}

function renderLogModelDist(){
  const target=document.getElementById("logModelDist");
  if(!target)return;
  const models=logOverview&&logOverview.dimensions&&Array.isArray(logOverview.dimensions.model)?logOverview.dimensions.model:[];
  target.innerHTML=models.length?'<span style="color:#64748b;margin-right:4px">模型:</span>'+models.slice(0,8).map(item=>{
    const failures=(item.error4xx||0)+(item.error5xx||0)+(item.errorTimeout||0)+(item.errorStream||0);
    return '<span class="log-model-tag">'+esc(item.value||"(未知)")+" ("+(item.total||0)+")"+(failures?' <span class="fail">x'+failures+"</span>":"")+' <span style="color:#64748b">'+(item.avgDuration?fmtDur(item.avgDuration):"-")+"</span></span>";
  }).join(""):"";
}

function renderLogErrorCluster(){
  const body=document.getElementById("logErrorBody");
  const panel=document.getElementById("logErrorCluster");
  if(!body||!panel)return;
  const stats=(logOverview&&logOverview.stats)||logLastStats||{};
  const groups=[["4xx",stats.error4xx],["5xx",stats.error5xx],["超时",stats.errorTimeout],["流中断",stats.errorStream]].filter(item=>Number(item[1])>0);
  const head=panel.querySelector(".head");
  const total=groups.reduce((sum,item)=>sum+(Number(item[1])||0),0);
  if(head)head.textContent="错误分布 ("+total+" 次)";
  body.innerHTML=groups.length?groups.map(item=>'<span class="log-error-code">'+item[0]+" x"+item[1]+"</span>").join(""):'<span style="color:#22c55e">无错误</span>';
}

function toggleErrorCluster(){
  const body=document.getElementById("logErrorBody");
  if(body)body.classList.toggle("open");
}

function logSortBy(field){
  if(logSortField===field)logSortAsc=!logSortAsc;
  else{logSortField=field;logSortAsc=field!=="time";}
  ["","_idx","_model","_status","_dur"].forEach(suffix=>{
    const icon=document.getElementById("logSortIcon"+suffix);
    if(icon)icon.textContent=(field===(suffix.replace("_","")||"time"))?(logSortAsc?"▲":"▼"):"";
  });
  logAllEntries.sort((a,b)=>{
    let left,right;
    if(field==="idx"){left=a.idx||0;right=b.idx||0;}
    else if(field==="model"){left=a.overrideModel||a.reqModel||"";right=b.overrideModel||b.reqModel||"";return logSortAsc?left.localeCompare(right):right.localeCompare(left);}
    else if(field==="status"){left=a.status||0;right=b.status||0;}
    else if(field==="dur"){left=a.duration||0;right=b.duration||0;}
    else{left=a.time||0;right=b.time||0;}
    return logSortAsc?left-right:right-left;
  });
  renderLogs();
}

function logDetailText(entry){
  if(entry.type==="event"){
    const lines=["事件: "+(entry.eventType||""),"消息: "+(entry.message||""),"URL: "+(entry.url||"")];
    if(entry.eventType==="stream_terminal"||entry.eventType==="downstream_terminal")lines.push("流 ID: "+(entry.streamId||""),"结果: "+(entry.streamOutcome||""),"原因: "+(entry.streamReason||""),"收到 [DONE]: "+(entry.streamSawDone===true?"是":"否"),"错误信息: "+(entry.streamErrorMsg||"无"),"来源: "+(entry.terminalSource||""));
    return lines.join("\\n");
  }
  return [
    "时间: "+new Date(entry.time).toISOString(),"Key #: "+(entry.idx||""),"分组: "+(entry.group||"A"),"客户端: "+(entry.client||""),"方法: "+(entry.method||""),"路径: "+(entry.path||""),"上游 URL: "+(entry.url||""),"模型: "+(entry.reqModel||""),"覆盖模型: "+(entry.overrideModel||""),"状态码: "+(entry.status||0),"上行: "+fmtBytes(entry.inputBytes||0),"下行: "+fmtBytes(entry.outputBytes||0),"耗时: "+fmtDur(entry.duration||0),"首字节: "+(entry.ttfb?fmtDur(entry.ttfb):"-"),"流 ID: "+(entry.streamId||""),"流结果: "+(entry.streamOutcome||""),"流终态原因: "+(entry.streamReason||""),"上游错误分类: "+(entry.upstreamErrorReason||""),"错误信息: "+(entry.streamErrorMsg||"无"),"错误来源: "+(entry.terminalSource||""),"收到 [DONE]: "+(entry.streamSawDone===true?"是":"否")
  ].join("\\n");
}

function toggleLogDetail(row,index){
  const existing=row.nextElementSibling;
  if(existing&&existing.classList.contains("log-row-expand")){existing.remove();return;}
  document.querySelectorAll("#logBody .log-row-expand").forEach(item=>item.remove());
  const entry=logAllEntries[index];
  if(!entry)return;
  const detail=document.createElement("tr");
  detail.className="log-row-expand open";
  const cell=document.createElement("td");
  cell.colSpan=12;
  cell.textContent=logDetailText(entry);
  detail.appendChild(cell);
  row.after(detail);
}

function enqueueLiveLog(entry){
  if(!entry||!isLogModalOpen())return;
  logLivePending.push(entry);
  if(logLiveFlushTimer)return;
  logLiveFlushTimer=setTimeout(()=>{
    logLiveFlushTimer=null;
    const batch=logLivePending.splice(0).sort((a,b)=>(b.time||0)-(a.time||0));
    if(!batch.length||!isLogModalOpen())return;
    if(logQueryMode==="recent"){
      logAllEntries=batch.concat(logAllEntries).slice(0,LOG_FAST_PAGE_SIZE);
      renderLogs();
    }
    scheduleLogOverviewRefresh();
  },350);
}

function scheduleLogOverviewRefresh(){
  if(logOverviewRefreshTimer||!isLogModalOpen())return;
  logOverviewRefreshTimer=setTimeout(async()=>{
    logOverviewRefreshTimer=null;
    const filters=buildLogFilterQuery();
    const params=new URLSearchParams();
    if(filters.has("since"))params.set("since",filters.get("since"));
    if(filters.has("until"))params.set("until",filters.get("until"));
    try{
      const response=await fetch("/__log-overview"+(params.toString()?"?"+params.toString():""),{cache:"no-store"});
      const payload=await response.json();
      if(!response.ok||!isLogModalOpen())return;
      logOverview=payload.overview||logOverview;
      applyLogIncidentData({incidents:(payload.overview&&payload.overview.incidents)||[],groupPauses:(payload.overview&&payload.overview.groupPauses)||[],summaryRebuild:payload.summaryRebuild});
      renderLogStats();renderLogSparkline();renderLogModelDist();renderLogErrorCluster();
    }catch(e){}
  },1800);
}

function applyLogIncidentData(payload){
  logOperations={
    incidents:Array.isArray(payload.incidents)?payload.incidents:logOperations.incidents,
    groupPauses:Array.isArray(payload.groupPauses)?payload.groupPauses:logOperations.groupPauses,
    summaryRebuild:payload.summaryRebuild||logOperations.summaryRebuild||{phase:"idle"}
  };
  if(isLogModalOpen())renderLogOperations();
}

function renderLogOperations(){
  const panel=document.getElementById("logIncidentPanel");
  const list=document.getElementById("logIncidentList");
  const pauses=document.getElementById("logGroupPauseList");
  const status=document.getElementById("logIncidentStatus");
  const refreshStatus=document.getElementById("logIncidentRefreshStatus");
  const rebuild=document.getElementById("logRebuildBtn");
  const refresh=document.getElementById("logIncidentRefreshBtn");
  if(!panel||!list||!pauses)return;
  panel.style.display="block";
  const state=logOperations.summaryRebuild||{};
  if(status){
    status.style.color="#94a3b8";
    if(state.phase==="running")status.textContent="汇总重建中";
    else if(state.phase==="failed"){status.textContent="汇总重建失败";status.style.color="#f87171";}
    else if(state.phase==="completed")status.textContent="已重建 "+(state.rebuiltDays||0)+" 天";
    else{status.textContent=logIncidentRefreshStatus;if(logIncidentRefreshStatus.startsWith("刷新失败"))status.style.color="#f87171";}
  }
  if(rebuild){rebuild.disabled=state.phase==="running";rebuild.textContent=state.phase==="running"?"重建中...":"重建汇总";}
  if(refresh){refresh.disabled=logIncidentRefreshInFlight;refresh.textContent=logIncidentRefreshInFlight?"刷新中...":"刷新事件";}
  if(refreshStatus){
    refreshStatus.textContent=logIncidentRefreshStatus;
    refreshStatus.style.color=logIncidentRefreshInFlight?"#60a5fa":logIncidentRefreshStatus.startsWith("刷新失败")?"#f87171":logIncidentRefreshStatus?"#4ade80":"#94a3b8";
  }
  list.textContent="";
  const incidents=(logOperations.incidents||[]).slice(0,12);
  if(!incidents.length){
    const empty=document.createElement("div");
    empty.style.cssText="color:#94a3b8;font-size:11px;padding:3px 0";
    empty.textContent="当前没有运行事件";
    list.appendChild(empty);
  }
  for(const incident of incidents){
    const row=document.createElement("div");
    row.style.cssText="display:flex;gap:7px;align-items:center;justify-content:space-between;flex-wrap:wrap;border-left:3px solid "+(incident.severity==="critical"?"#ef4444":"#f59e0b")+";background:#0f172a;padding:5px 7px;font-size:11px";
    const text=document.createElement("span");
    const scope=incident.scope&&incident.scope.value?" · "+incident.scope.value:"";
    const metrics=incident.metrics||{};
    const failureInfo=metrics.failures!=null?" · 失败 "+metrics.failures+"/"+(metrics.total||0):"";
    text.textContent=(incident.status==="resolved"?"已恢复":"["+(incident.status||"open")+"]")+" "+(incident.title||"日志事件")+scope+failureInfo;
    text.style.color=incident.status==="resolved"?"#94a3b8":"#e2e8f0";
    row.appendChild(text);
    const actions=document.createElement("span");
    actions.style.cssText="display:flex;gap:4px;align-items:center";
    if(incident.status!=="resolved"){
      const ack=document.createElement("button");ack.className="btn";ack.style.cssText="font-size:10px;padding:2px 6px";ack.textContent="确认";ack.onclick=()=>runLogIncidentAction(incident.id,"acknowledge");actions.appendChild(ack);
      const snooze=document.createElement("button");snooze.className="btn";snooze.style.cssText="font-size:10px;padding:2px 6px";snooze.textContent="静默";snooze.onclick=()=>runLogIncidentAction(incident.id,"snooze");actions.appendChild(snooze);
      if(incident.scope&&incident.scope.type==="group"){
        const pause=document.createElement("button");pause.className="btn btn-d";pause.style.cssText="font-size:10px;padding:2px 6px";pause.textContent="暂停分组";pause.onclick=()=>runLogIncidentAction(incident.id,"pause_group",{group:incident.scope.value,minutes:5});actions.appendChild(pause);
      }
    }
    row.appendChild(actions);
    list.appendChild(row);
  }
  pauses.textContent="";
  for(const pause of logOperations.groupPauses||[]){
    const wrap=document.createElement("span");
    wrap.style.cssText="display:inline-flex;gap:5px;align-items:center;border:1px solid #ef4444;background:#3b1f1e;color:#fecaca;padding:3px 5px;font-size:10px";
    const seconds=Math.max(0,Math.ceil((Number(pause.expiresAt)-Date.now())/1000));
    const text=document.createElement("span");text.textContent="分组 "+pause.group+" 已暂停 "+(seconds<60?seconds+" 秒":Math.ceil(seconds/60)+" 分钟");wrap.appendChild(text);
    const resume=document.createElement("button");resume.className="btn";resume.style.cssText="font-size:10px;padding:1px 5px";resume.textContent="恢复";resume.onclick=()=>runLogIncidentAction(pause.incidentId,"resume_group",{group:pause.group});wrap.appendChild(resume);
    pauses.appendChild(wrap);
  }
}

async function refreshLogOperations(options){
  const opts=options&&typeof options==="object"?options:{};
  if(logIncidentRefreshInFlight)return;
  logIncidentRefreshInFlight=true;
  if(opts.interactive)logIncidentRefreshStatus="正在刷新事件...";
  if(isLogModalOpen())renderLogOperations();
  try{
    const response=await fetch("/__incidents",{cache:"no-store"});
    const payload=await response.json();
    if(!response.ok)throw new Error(payload&&payload.error?payload.error:"刷新失败");
    logIncidentRefreshStatus="已刷新 "+new Date().toTimeString().slice(0,8);
    applyLogIncidentData(payload||{});
  }catch(e){
    logIncidentRefreshStatus="刷新失败";
  }finally{
    logIncidentRefreshInFlight=false;
    if(isLogModalOpen())renderLogOperations();
  }
}

async function runLogIncidentAction(id,action,extra){
  if(action==="pause_group"&&!confirm("暂停该分组会立即拒绝新的代理请求，确认继续？"))return;
  try{
    const response=await fetch("/__incident-action",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(Object.assign({id,action},extra||{}))});
    const payload=await response.json();
    if(!response.ok)throw new Error(payload&&payload.error?payload.error:"处置失败");
    await refreshLogOperations();
  }catch(error){alert("日志事件处置失败："+(error&&error.message?error.message:"未知错误"));}
}

async function rebuildLogSummary(){
  try{
    const response=await fetch("/__logs/rebuild-summary",{method:"POST",cache:"no-store"});
    const payload=await response.json();
    if(!response.ok)throw new Error(payload&&payload.error?payload.error:"无法开始重建");
    applyLogIncidentData({summaryRebuild:payload});
  }catch(error){alert("日志汇总重建失败："+(error&&error.message?error.message:"未知错误"));}
}

function logRowClass(e){
  if (e.type === "event") {
    if (e.eventType === "conversion") return "log-row-conversion";
    if (e.eventType === "recover") return "log-row-recover";
    if (e.eventType === "lock") return "log-row-lock";
    if (e.eventType === "discard") return "log-row-discard";
    return "log-row-event";
  }
  const st = e.status || 0;
  if (e.streamOutcome === "failed") return "log-row-" + (document.getElementById("logBody").children.length % 2 === 0 ? "even" : "odd");
  if (st >= 500) return "log-row-" + (document.getElementById("logBody").children.length % 2 === 0 ? "even" : "odd");
  return "";
}
function makeLogRow(e, seq){
  if (e.type === "event") return makeEventRow(e, seq);
  const s=e.status||0;
  const streamFailed=e.streamOutcome === "failed";
  const sc="log-s"+(s>=500 || streamFailed?"5xx":s);
  const tm=new Date(e.time);
  const now=new Date();
  const isToday=tm.getFullYear()===now.getFullYear()&&tm.getMonth()===now.getMonth()&&tm.getDate()===now.getDate();
  const ts=(isToday?"":String(tm.getMonth()+1).padStart(2,"0")+"-"+String(tm.getDate()).padStart(2,"0")+" ")+String(tm.getHours()).padStart(2,"0")+":"+String(tm.getMinutes()).padStart(2,"0")+":"+String(tm.getSeconds()).padStart(2,"0");
  const mdl=e.overrideModel||e.reqModel||"";
  const urlShort = e.url ? e.url.replace(/^https?:\\/\\//, "").split("/")[0] : "";
  let icon = "";
  if (e.conversion) icon = ' <span title="协议转换" style="color:#a78bfa">🔄</span>';
  else if (streamFailed) icon = ' <span title="流式终态失败: '+esc(e.streamReason||"")+'" style="color:#ef4444">✕</span>';
  else if (s >= 500) icon = ' <span style="color:#ef4444">✕</span>';
  else if (s >= 400) icon = ' <span style="color:#f59e0b">⚠</span>';
  return '<td class="log-seq" style="text-align:center;color:#64748b;font-size:10px">'+(seq != null ? seq : "")+'</td><td class="log-time">'+ts+'</td><td>#'+(e.idx||"")+'</td>'
    +'<td style="max-width:100px;overflow:hidden;text-overflow:ellipsis;color:#64748b;font-size:10px" title="'+esc(e.url||"")+'">'+esc(urlShort)+'</td>'
    +'<td>'+e.method+'</td>'
    +'<td style="max-width:80px;overflow:hidden;text-overflow:ellipsis" title="'+esc(mdl)+'">'+esc(mdl)+icon+'</td>'
    +'<td style="max-width:150px;overflow:hidden;text-overflow:ellipsis">'+esc(e.path)+'</td>'
    +'<td class="log-status '+sc+'">'+s+'</td>'
    +'<td>'+fmtBytes(e.inputBytes||0)+'</td><td>'+fmtBytes(e.outputBytes||0)+'</td>'
    +'<td class="log-dur">'+fmtDur(e.duration||0)+'</td>'
    +'<td class="log-dur">'+(e.ttfb?fmtDur(e.ttfb):"-")+'</td>';
}
function makeEventRow(e, seq){
  const tm=new Date(e.time);
  const now=new Date();
  const isToday=tm.getFullYear()===now.getFullYear()&&tm.getMonth()===now.getMonth()&&tm.getDate()===now.getDate();
  const ts=(isToday?"":String(tm.getMonth()+1).padStart(2,"0")+"-"+String(tm.getDate()).padStart(2,"0")+" ")+String(tm.getHours()).padStart(2,"0")+":"+String(tm.getMinutes()).padStart(2,"0")+":"+String(tm.getSeconds()).padStart(2,"0");
  let label="", detail="";
  switch(e.eventType){
    case "conversion":
      let dir="";
      if(e.message && e.message.indexOf("Responses→Chat")>=0) dir='<span style="font-weight:700;color:#a78bfa">R→C</span>';
      else if(e.message && e.message.indexOf("Messages→Chat")>=0) dir='<span style="font-weight:700;color:#a78bfa">M→C</span>';
      else if(e.message && e.message.indexOf("Chat→Messages")>=0) dir='<span style="font-weight:700;color:#a78bfa">C→M</span>';
      label="🔄 转换 "+dir; detail=e.message||""; break;
    case "recover": label="✅ 自动恢复"; detail=e.message||""; break;
    case "lock": label="🔒 自动锁死"; detail=e.message||""; break;
    case "discard": label="🗑 废弃"; detail=e.message||""; break;
    case "stream_terminal":
      if (e.streamOutcome === "completed") label="✅ 流完成";
      else if (e.streamOutcome === "cancelled") label="⏹ 客户端断开";
      else label="⛔ 流失败";
      detail=e.message||"";
      if (e.streamErrorMsg) detail=e.message+" | "+e.streamErrorMsg;
      break;
    case "downstream_terminal":
      label="🔻 下游失败";
      detail=e.message||"";
      if (e.streamErrorMsg) detail=e.message+" | "+e.streamErrorMsg;
      break;
    default: label="📌 "+(e.eventType||"事件"); detail=e.message||"";
  }
  const urlShort = e.url ? e.url.replace(/^https?:\\/\\//, "").split("/")[0] : "";
  return '<td class="log-seq" style="text-align:center;color:#64748b;font-size:10px"></td><td class="log-time">'+ts+'</td><td>#'+(e.idx||"")+'</td>'
    +'<td style="color:#60a5fa;font-size:10px">'+esc(urlShort)+'</td>'
    +'<td colspan="8" style="color:inherit">'+esc(label)+' <span style="color:#94a3b8;font-size:10px">'+esc(detail)+'</span></td>';
}
function toggleLogCustomRange(){
  document.getElementById("logCustomRange").style.display=
    document.getElementById("logTimeFilter").value==="custom"?"inline":"none";
}
function exportLogs(){
  const q=buildLogFilterQuery();
  q.set("mode","history");
  q.set("limit","2000");
  q.set("format","csv");
  window.open("http://localhost:3456/__logs?"+q.toString());
}

function openConfig(){renderUpdateInfo();loadConfigUI();document.getElementById("configModal").classList.add("on")}
function closeConfig(){document.getElementById("configModal").classList.remove("on")}
function openTaskInsight(){document.getElementById("taskInsightModal").classList.add("on");loadTaskInsight()}
function closeTaskInsight(){document.getElementById("taskInsightModal").classList.remove("on")}
function escTaskText(v){
  return String(v==null?"":v).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#39;");
}
function fmtTaskDuration(ms){
  const s=Math.max(0,Math.round((ms||0)/1000));
  if(s<60)return s+"s";
  if(s<3600)return Math.floor(s/60)+"m"+(s%60?(" "+s%60+"s"):"");
  return Math.floor(s/3600)+"h"+(Math.floor(s%3600/60)?(" "+Math.floor(s%3600/60)+"m"):"");
}
function taskInsightBadge(status){
  const map={completed:["完成","#22c55e"],failed:["失败","#f87171"],partial:["部分","#fbbf24"]};
  const [t,c]=map[status]||[status||"未知","#94a3b8"];
  return '<span style="color:'+c+'">'+t+'</span>';
}
async function loadTaskInsight(refresh){
  const disabledEl=document.getElementById("taskInsightDisabled");
  const panelEl=document.getElementById("taskInsightPanel");
  if(!disabledEl||!panelEl)return;
  try{
    const st=await fetch("/__task-insight-status");
    const sj=await st.json();
    if(!sj.ok||!sj.enabled){
      panelEl.style.display="none";
      disabledEl.style.display="block";
      return;
    }
    disabledEl.style.display="none";
    panelEl.style.display="block";
    const d=sj.distill||{};
    const budget=d.budget||{};
    const hint=document.getElementById("taskInsightHint");
    if(hint){
      let parts=["仅结构化信号，不含 Key 与完整原文"];
      if(d.enabled){
        parts.push("蒸馏引擎: "+(d.engine||"-")+(d.running?"（运行中）":""));
        if(budget.limitYuan>0)parts.push("今日预算 ¥"+Number(budget.spentYuan||0).toFixed(2)+"/¥"+Number(budget.limitYuan));
      }else parts.push("LLM 蒸馏未启用（可在配置中开启）");
      hint.textContent=parts.join(" · ");
    }
    const distillBtn=document.getElementById("taskInsightDistillBtn");
    if(distillBtn){
      if(!d.enabled){distillBtn.style.display="none";}
      else{distillBtn.style.display="inline-block";distillBtn.disabled=!!d.running;distillBtn.textContent=d.running?"蒸馏中…":"🤖 立即蒸馏";}
    }
    const q=new URLSearchParams();
    const proj=document.getElementById("taskInsightProjectFilter").value;
    if(proj)q.set("project",proj);
    const status=document.getElementById("taskInsightStatusFilter").value;
    if(status)q.set("status",status);
    const search=document.getElementById("taskInsightSearch").value.trim();
    if(search)q.set("q",search);
    if(!refresh)q.set("limit","100");
    const r=await fetch("/__tasks?"+q.toString());
    const j=await r.json();
    if(!j.ok)throw new Error(j.error||"load failed");
    const stats=document.getElementById("taskInsightStats");
    if(stats){
      const s=j.status||{};
      stats.textContent="共 "+j.total+" 个会话 · 启用状态: "+(s.enabled?"开启":"关闭")+(s.signals?(" · 信号 "+Object.keys(s.signals||{}).filter(k=>s.signals[k]).join(",")||"无"):"");
    }
    const table=document.getElementById("taskInsightTable");
    if(j.tasks&&j.tasks.length){
      table.innerHTML='<table style="width:100%;border-collapse:collapse;min-width:760px"><thead><tr style="text-align:left;color:#64748b;border-bottom:1px solid #334155">'
        +'<th style="padding:4px 6px">时间</th><th style="padding:4px 6px">项目</th><th style="padding:4px 6px">客户端</th><th style="padding:4px 6px">状态</th><th style="padding:4px 6px">请求</th>'
        +'<th style="padding:4px 6px">Token(入/出)</th><th style="padding:4px 6px">费用</th><th style="padding:4px 6px">模型</th><th style="padding:4px 6px">工具/文件</th><th style="padding:4px 6px">摘要</th></tr></thead><tbody>'
        +j.tasks.map(t=>{
          const tools=[...(t.tools||[]).slice(0,6)];
          const files=[...(t.files||[]).slice(0,3)];
          const toolsHtml=tools.length?escTaskText(tools.join(", ")):"";
          const filesHtml=files.length?escTaskText(files.join(", ")):"";
          const chain=(t.models||[]).slice(0,4).join(" → ");
          const summary=t.distill&&t.distill.summary?escTaskText(String(t.distill.summary).slice(0,120)):"";
          const instr=(t.instructions||[]).slice(0,2).map(i=>escTaskText(String(i).slice(0,120))).join("<br>");
          return '<tr style="border-bottom:1px solid #1e293b;vertical-align:top">'
            +'<td style="padding:4px 6px;color:#94a3b8;white-space:nowrap">'+new Date(t.start).toLocaleString()+'<br><span style="color:#64748b">'+fmtTaskDuration(t.end-t.start)+'</span></td>'
            +'<td style="padding:4px 6px;color:#60a5fa">'+escTaskText(t.projectName||"未分类")+'</td>'
            +'<td style="padding:4px 6px;color:#94a3b8">'+escTaskText(t.client)+'</td>'
            +'<td style="padding:4px 6px">'+taskInsightBadge(t.status)+'</td>'
            +'<td style="padding:4px 6px">'+escTaskText(t.requestCount)+'（成功 '+escTaskText(t.successCount)+'）</td>'
            +'<td style="padding:4px 6px;color:#94a3b8">'+escTaskText(t.inputTokens)+' / '+escTaskText(t.outputTokens)+'</td>'
            +'<td style="padding:4px 6px;color:#fbbf24">¥'+Number(t.cost||0).toFixed(4)+'</td>'
            +'<td style="padding:4px 6px;color:#94a3b8">'+escTaskText(chain)+'</td>'
            +'<td style="padding:4px 6px;color:#94a3b8">'+toolsHtml+(toolsHtml&&filesHtml?"<br>":"")+filesHtml+'</td>'
            +'<td style="padding:4px 6px;max-width:280px">'+(summary||instr)+'</td>'
            +'</tr>';
        }).join("")+'</tbody></table>';
    }else{
      table.innerHTML='<div style="padding:24px;text-align:center;color:#64748b;font-size:12px">暂无匹配的会话记录</div>';
    }
  }catch(e){
    const table=document.getElementById("taskInsightTable");
    if(table)table.innerHTML='<div style="padding:24px;text-align:center;color:#f87171;font-size:12px">加载失败: '+escTaskText(e.message||e)+'</div>';
  }
}
function taskInsightExport(){
  const q=new URLSearchParams();
  const proj=document.getElementById("taskInsightProjectFilter").value;
  if(proj)q.set("project",proj);
  const status=document.getElementById("taskInsightStatusFilter").value;
  if(status)q.set("status",status);
  window.open("http://localhost:3456/__tasks/export?"+q.toString());
}
async function taskInsightReport(){
  const box=document.getElementById("taskInsightReportBox");
  if(!box)return;
  box.style.display="block";
  box.textContent="正在生成报告…";
  try{
    const r=await fetch("/__tasks/report");
    const j=await r.json();
    if(!j.ok)throw new Error(j.error||"report failed");
    const projects=(j.projects||[]).slice(0,12);
    let html='<strong style="color:#e2e8f0">'+escTaskText(j.scope||"")+'报告</strong> · 会话 '+j.total.sessions+' · 请求 '+j.total.requests+' · 费用 ¥'+Number(j.total.cost).toFixed(4);
    if(projects.length){
      html+='<table style="width:100%;border-collapse:collapse;margin-top:6px"><thead><tr style="text-align:left;color:#64748b;border-bottom:1px solid #334155"><th style="padding:3px 6px">项目</th><th style="padding:3px 6px">会话</th><th style="padding:3px 6px">请求</th><th style="padding:3px 6px">成功率</th><th style="padding:3px 6px">费用</th><th style="padding:3px 6px">模型</th></tr></thead><tbody>'
        +projects.map(g=>'<tr style="border-bottom:1px solid #1e293b"><td style="padding:3px 6px;color:#60a5fa">'+escTaskText(g.project)+'</td><td style="padding:3px 6px">'+g.sessions+'</td><td style="padding:3px 6px">'+g.requests+'</td><td style="padding:3px 6px;color:'+(g.successRate>=80?"#22c55e":g.successRate>=50?"#fbbf24":"#f87171")+'">'+g.successRate+'%</td><td style="padding:3px 6px;color:#fbbf24">¥'+Number(g.cost).toFixed(4)+'</td><td style="padding:3px 6px;color:#94a3b8">'+escTaskText((g.models||[]).slice(0,3).join(", "))+'</td></tr>').join("")
        +'</tbody></table>';
    }
    box.innerHTML=html;
  }catch(e){
    box.textContent="报告失败: "+escTaskText(e.message||e);
  }
}
async function taskInsightDistillNow(){
  const btn=document.getElementById("taskInsightDistillBtn");
  if(btn)btn.disabled=true;
  try{
    const r=await fetch("/__tasks/distill-now",{method:"POST"});
    const j=await r.json();
    alert((j.ok?"已开始蒸馏":"蒸馏失败")+": "+(j.error||"已提交，运行后可在任务详情查看摘要"));
    if(j.ok)loadTaskInsight(true);
  }catch(e){alert("蒸馏请求失败: "+e.message);}
  finally{if(btn)btn.disabled=false;}
}
function configInteger(id,fallback){const value=parseInt(document.getElementById(id).value,10);return Number.isFinite(value)?value:fallback}
function renderModelPricingRules(modelPricing){
  const area=document.getElementById("cfgModelPricingArea");
  if(!area)return;
  const rules=Array.isArray(modelPricing)?modelPricing:(modelPricing&&Array.isArray(modelPricing.rules)?modelPricing.rules:[]);
  area.innerHTML='<div id="cfgModelPricingRows" style="display:flex;flex-direction:column;gap:4px"></div><div id="cfgModelPricingEmpty" style="font-size:10px;color:#64748b;padding:4px 0">暂无模型覆盖规则</div>';
  rules.slice(0,50).forEach(rule=>addModelPricingRule(rule));
  updateModelPricingEmptyState();
}
function modelPricingField(value){return value===undefined||value===null||value===""?"":String(value)}
function addModelPricingRule(rule){
  const rows=document.getElementById("cfgModelPricingRows");
  if(!rows||rows.children.length>=50)return;
  const row=document.createElement("div");
  row.className="cfg-model-pricing-row";
  row.style.cssText="display:grid;grid-template-columns:minmax(110px,1.5fr) repeat(3,minmax(78px,1fr)) 28px;gap:4px;align-items:center";
  row.innerHTML='<input class="cfg-model-pricing-model" placeholder="模型名，例如 gpt-5" maxlength="80" style="min-width:0;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:3px 5px;border-radius:4px;font-size:11px">'+
    '<input class="cfg-model-pricing-input" type="number" min="0" max="1000000" step="any" placeholder="输入 / 1M" title="输入价格（每百万 token）" style="min-width:0;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:3px 5px;border-radius:4px;font-size:11px">'+
    '<input class="cfg-model-pricing-output" type="number" min="0" max="1000000" step="any" placeholder="输出 / 1M" title="输出价格（每百万 token）" style="min-width:0;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:3px 5px;border-radius:4px;font-size:11px">'+
    '<input class="cfg-model-pricing-bpt" type="number" min="0.1" max="100" step="any" placeholder="字节/token" title="每 token 字节数" style="min-width:0;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:3px 5px;border-radius:4px;font-size:11px">'+
    '<button class="btn" type="button" title="删除模型规则" style="padding:1px 5px;color:#f87171" onclick="removeModelPricingRule(this)">✕</button>';
  const value=rule&&typeof rule==="object"?rule:{};
  row.querySelector(".cfg-model-pricing-model").value=modelPricingField(value.model);
  row.querySelector(".cfg-model-pricing-input").value=modelPricingField(value.inputPer1M);
  row.querySelector(".cfg-model-pricing-output").value=modelPricingField(value.outputPer1M);
  row.querySelector(".cfg-model-pricing-bpt").value=modelPricingField(value.bytesPerToken);
  rows.appendChild(row);
  updateModelPricingEmptyState();
}
function removeModelPricingRule(button){
  const row=button&&button.closest(".cfg-model-pricing-row");
  if(row)row.remove();
  updateModelPricingEmptyState();
}
function updateModelPricingEmptyState(){
  const rows=document.getElementById("cfgModelPricingRows");
  const empty=document.getElementById("cfgModelPricingEmpty");
  if(empty)empty.style.display=rows&&rows.children.length?"none":"block";
}
function readModelPricingNumber(value,label,minimum,maximum){
  if(value==="")throw new Error(label+"不能为空");
  const number=Number(value);
  if(!Number.isFinite(number)||number<minimum||number>maximum)throw new Error(label+"必须在 "+minimum+"–"+maximum+" 之间");
  return number;
}
function readDefaultPricingNumber(id,label,fallback,minimum,maximum){
  const value=document.getElementById(id).value.trim();
  return value===""?fallback:readModelPricingNumber(value,label,minimum,maximum);
}
function collectDefaultPricingRule(){
  return {
    prices:{
      inputPer1M:readDefaultPricingNumber("cfgPriceIn","默认输入价格",0,0,1000000),
      outputPer1M:readDefaultPricingNumber("cfgPriceOut","默认输出价格",0,0,1000000)
    },
    bytesPerToken:readDefaultPricingNumber("cfgBpt","默认每 token 字节数",3,0.1,100)
  };
}
function collectModelPricingRules(){
  const result=[];
  const seen=new Set();
  const rows=document.querySelectorAll("#cfgModelPricingRows .cfg-model-pricing-row");
  for(const row of rows){
    const model=row.querySelector(".cfg-model-pricing-model").value.trim();
    const inputRaw=row.querySelector(".cfg-model-pricing-input").value.trim();
    const outputRaw=row.querySelector(".cfg-model-pricing-output").value.trim();
    const bptRaw=row.querySelector(".cfg-model-pricing-bpt").value.trim();
    if(!model&&!inputRaw&&!outputRaw&&!bptRaw)continue;
    if(!model)throw new Error("模型规则缺少模型名");
    if(model.length>80||/[\\u0000-\\u001f\\u007f]/.test(model))throw new Error("模型名无效: "+model);
    if(seen.has(model))throw new Error("模型规则重复: "+model);
    seen.add(model);
    result.push({
      model,
      inputPer1M:readModelPricingNumber(inputRaw,"模型 "+model+" 的输入价格",0,1000000),
      outputPer1M:readModelPricingNumber(outputRaw,"模型 "+model+" 的输出价格",0,1000000),
      bytesPerToken:readModelPricingNumber(bptRaw,"模型 "+model+" 的每 token 字节数",0.1,100)
    });
  }
  return result;
}
async function saveConfig(){
  let modelPricing,defaultPricing;
  try{modelPricing=collectModelPricingRules();defaultPricing=collectDefaultPricingRule();}
  catch(e){document.getElementById("configStatus").textContent="保存失败: "+e.message;return;}
  const c={
    prices:defaultPricing.prices,
    bytesPerToken:defaultPricing.bytesPerToken,
    modelPricing,
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
    autoRecoverPoll:document.getElementById("cfgAutoRecoverPoll").checked,
    autoRecoverPollInterval:parseInt(document.getElementById("cfgAutoRecoverPollInterval").value)||5,
    autoRecoverPollCodes:(document.getElementById("cfgAutoRecoverPollCodes").value||"").split(",").map(s=>parseInt(s.trim())).filter(n=>!isNaN(n)),
    autoRecoverDelays:(document.getElementById("cfgAutoRecoverDelays").value||"").split(",").map(s=>parseInt(s.trim())).filter(n=>!isNaN(n)&&n>=100&&n<=10000).slice(0,10),
    roundRobin:document.getElementById("cfgRoundRobin").checked,
    weeklySortBy:document.getElementById("cfgWeeklySortBy").checked?"expiry":"priority",
    enableAutoLock:document.getElementById("cfgEnableAutoLock").checked,
    maxRequestsPerMin:parseInt(document.getElementById("cfgMaxReqPerMin").value)||10,
    maxTokensPerMin:parseInt(document.getElementById("cfgMaxTokPerMin").value)||0,
    streamLifetime:configInteger("cfgStreamLifetime",1800000),
    responsesStreamLifetime:configInteger("cfgResponsesStreamLifetime",0),
    responsesIdleTimeout:configInteger("cfgResponsesIdleTimeout",5400000),
    adminToken:document.getElementById("cfgAdminToken").value.trim(),
    updateBaselineTag:document.getElementById("cfgUpdateBaselineTag").value.trim(),
    lockAfterFailCount:parseInt(document.getElementById("cfgLockCount").value)||3,
    lockFailCodes:(document.getElementById("cfgLockCodes").value||"").split(",").map(s=>s.trim()).filter(s=>s),
    logFile:document.getElementById("cfgLogFile").checked,
    logRetentionDays:configInteger("cfgLogRetention",7),
    logDetail:document.getElementById("cfgLogDetail").value,
    logMaxMiB:configInteger("cfgLogMaxMiB",256),
    logSegmentMaxMiB:configInteger("cfgLogSegmentMaxMiB",16),
    stateHourlyRetentionDays:configInteger("cfgStateHourlyRetentionDays",35),
    stateDailyRetentionDays:configInteger("cfgStateDailyRetentionDays",180),
    stateMaxMiB:configInteger("cfgStateMaxMiB",32),
    proxyLogMaxMiB:configInteger("cfgProxyLogMaxMiB",10),
    proxyLogKeepFiles:configInteger("cfgProxyLogKeepFiles",5),
    codexLogMaintenance:codexLogMaintenanceConfigFromForm(),
    logIncidents:{
      enabled:document.getElementById("cfgLogIncidentEnabled").checked,
      notify:document.getElementById("cfgLogIncidentNotify").checked,
      windowMinutes:parseInt(document.getElementById("cfgLogIncidentWindow").value)||5,
      minRequests:parseInt(document.getElementById("cfgLogIncidentMinRequests").value)||8,
      errorBurst:parseInt(document.getElementById("cfgLogIncidentErrorBurst").value)||5,
      errorRatePercent:parseFloat(document.getElementById("cfgLogIncidentErrorRate").value)||60,
      streamFailureBurst:parseInt(document.getElementById("cfgLogIncidentStreamBurst").value)||3,
      defaultSnoozeMinutes:parseInt(document.getElementById("cfgLogIncidentSnooze").value)||15,
      resolveAfterMinutes:parseInt(document.getElementById("cfgLogIncidentResolve").value)||5,
      latencyEnabled:document.getElementById("cfgLogIncidentLatency").checked,
      p95Ms:parseInt(document.getElementById("cfgLogIncidentP95").value)||120000,
      p95TtfbMs:parseInt(document.getElementById("cfgLogIncidentP95Ttfb").value)||20000
    },
    autoResume:document.getElementById("cfgAutoResume").checked,
    autoResumeIdleMinutes:parseInt(document.getElementById("cfgAutoResumeIdle").value)||10,
    autoResumeDebounceMinutes:parseInt(document.getElementById("cfgAutoResumeDebounce").value)||3,
    autoResumeRunnerStallMinutes:configInteger("cfgAutoResumeRunnerStall",20),
    autoResumeRunnerMaxStallRestarts:configInteger("cfgAutoResumeRunnerRestarts",1),
    cmdPath:document.getElementById("cfgCmdPath").value.trim()||"/mnt/c/Windows/System32/cmd.exe",
    capacityBackoffSeconds:parseInt(document.getElementById("cfgCapacityBackoffSeconds").value)||60,
    capacityMaxWaitSeconds:parseInt(document.getElementById("cfgCapacityMaxWaitSeconds").value)||300,
    taskInsight:{
      enabled:document.getElementById("cfgTaskInsightEnabled").checked,
      signals:{
        instructions:document.getElementById("cfgTaskInsightInstructions").checked,
        tools:document.getElementById("cfgTaskInsightTools").checked,
        usage:document.getElementById("cfgTaskInsightUsage").checked,
        correlate:document.getElementById("cfgTaskInsightCorrelate").checked
      },
      retentionDays:configInteger("cfgTaskInsightRetention",30),
      distill:{
        enabled:document.getElementById("cfgTaskInsightDistillEnabled").checked,
        engine:document.getElementById("cfgTaskInsightDistillEngine").value,
        model:document.getElementById("cfgTaskInsightDistillModel").value.trim(),
        baseUrl:document.getElementById("cfgTaskInsightDistillBaseUrl").value.trim(),
        dailyBudgetYuan:parseFloat(document.getElementById("cfgTaskInsightDistillBudget").value)||0,
        report:document.getElementById("cfgTaskInsightDistillReport").value
      }
    },
    autoResumeProjects:collectResumeProjects()
  };
  const invalidResumeProject=c.autoResumeProjects.find(p=>p.resumeMode==="fixed_session"&&(!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(p.sessionId||"")||!String(p.cmd||"").includes("{sessionId}")));
  if(invalidResumeProject){
    document.getElementById("configStatus").textContent="保存失败: 固定会话模式需要合法会话 ID，且启动命令必须包含 {sessionId}";
    return;
  }
  if(c.codexLogMaintenance.enabled&&!(await checkCodexLogMaintenancePath())){
    document.getElementById("configStatus").textContent="保存失败: Codex SQLite 数据库路径或结构未通过检测";
    return;
  }
  try{
    const r=await fetch("http://localhost:3456/__config",{method:"PUT",headers:{"content-type":"application/json"},body:JSON.stringify(c)});
    const j=await r.json();
    if(j.ok){closeConfig();checkForUpdates(false)}else{document.getElementById("configStatus").textContent="保存失败: "+(j.error||"未知错误")};
  }catch(e){document.getElementById("configStatus").textContent="保存失败: "+e.message}
}
let restartPollTimer=null,restartElapsedTimer=null,restartPollingActive=false,restartOldInstanceId="",restartStartedAt=0,restartLastStatus=null,restartActionInFlight=false;
function restartElapsedText(){
  const seconds=Math.max(0,Math.floor((Date.now()-restartStartedAt)/1000));
  return seconds<60?seconds+" 秒":Math.floor(seconds/60)+" 分 "+(seconds%60)+" 秒";
}
function restartWaitText(ms){
  const seconds=Math.max(1,Math.ceil((Number(ms)||0)/1000));
  return seconds<60?seconds+" 秒":Math.floor(seconds/60)+" 分 "+(seconds%60)+" 秒";
}
function refreshRestartElapsed(){
  const elapsed=document.getElementById("restartOverlayElapsed");
  if(elapsed)elapsed.textContent=restartStartedAt?"已等待 "+restartElapsedText():"";
}
function startRestartElapsedTimer(){
  if(restartElapsedTimer)clearInterval(restartElapsedTimer);
  refreshRestartElapsed();
  restartElapsedTimer=setInterval(refreshRestartElapsed,1000);
}
function stopRestartElapsedTimer(){
  if(restartElapsedTimer){clearInterval(restartElapsedTimer);restartElapsedTimer=null;}
}
async function fetchRestartStatus(){
  if(typeof AbortController==="undefined")return fetch("/__restart-status",{cache:"no-store"});
  const controller=new AbortController();
  const timeout=setTimeout(function(){controller.abort();},3500);
  try{return await fetch("/__restart-status",{cache:"no-store",signal:controller.signal});}
  finally{clearTimeout(timeout);}
}
function setRestartOverlayActions(status){
  const actions=document.getElementById("restartOverlayActions");
  const cancel=document.getElementById("restartCancelBtn");
  const force=document.getElementById("restartForceBtn");
  if(!actions||!cancel||!force)return;
  const canCancel=!!(restartPollingActive&&status&&status.canCancel);
  const canForce=!!(canCancel&&status.canForce);
  actions.style.display=canCancel?"flex":"none";
  cancel.style.display=canCancel?"inline-block":"none";
  cancel.disabled=!canCancel||restartActionInFlight;
  force.style.display=canForce?"inline-block":"none";
  force.disabled=!canForce||restartActionInFlight;
}
function restartDrainDetail(status){
  let detail="仍有 "+(status.activeRequests||0)+" 个请求正在完成"+((status.queuedRequests||0)>0?"，另有 "+(status.queuedRequests||0)+" 个排队请求。":"。");
  if((status.cancelledQueuedRequests||0)>0)detail+=" 本次已拒绝 "+status.cancelledQueuedRequests+" 个排队请求。";
  if(status.canForce)detail+=" 已等待至少 30 秒，现在可以强制重启。";
  else if(status.forceAvailableInMs>0)detail+=" "+restartWaitText(status.forceAvailableInMs)+"后可选择强制重启。";
  return detail;
}
function updateRestartOverlay(title,detail,isError,isComplete){
  const overlay=document.getElementById("restartOverlay");
  overlay.classList.add("on");
  overlay.classList.toggle("error",!!isError);
  overlay.classList.toggle("complete",!!isComplete);
  document.getElementById("restartOverlayTitle").textContent=title;
  document.getElementById("restartOverlayDetail").textContent=detail;
  refreshRestartElapsed();
  document.getElementById("restartOverlayDismiss").style.display=(isError||isComplete)?"inline-block":"none";
}
function dismissRestartOverlay(){
  restartPollingActive=false;
  if(restartPollTimer){clearTimeout(restartPollTimer);restartPollTimer=null;}
  stopRestartElapsedTimer();
  restartStartedAt=0;
  restartLastStatus=null;
  restartActionInFlight=false;
  setRestartOverlayActions(null);
  document.getElementById("restartOverlay").classList.remove("on","error","complete");
  const btn=document.getElementById("restartProxyBtn");
  if(btn){btn.disabled=false;btn.textContent="🔄 重启代理";}
}
function scheduleRestartStatusPoll(delay){
  if(!restartPollingActive)return;
  if(restartPollTimer)clearTimeout(restartPollTimer);
  restartPollTimer=setTimeout(pollRestartStatus,delay);
}
async function pollRestartStatus(){
  if(!restartPollingActive)return;
  let title="正在等待新的代理实例",detail="旧代理已停止，正在等待 watchdog 启动新实例。";
  try{
    const r=await fetchRestartStatus();
    if(!restartPollingActive)return;
    if(r.status===401)throw new Error("管理认证失效，请重新打开 Dashboard 后重试");
    if(!r.ok)throw new Error("HTTP "+r.status);
    const status=await r.json();
    if(!restartPollingActive)return;
    restartLastStatus=status;
    if(Number.isFinite(status.restartStartedAt)&&status.restartStartedAt>0)restartStartedAt=status.restartStartedAt;
    if(restartOldInstanceId&&status.instanceId!==restartOldInstanceId&&status.phase==="ready"){
      restartPollTimer=null;
      restartPollingActive=false;
      stopRestartElapsedTimer();
      setRestartOverlayActions(null);
      updateRestartOverlay("新代理实例已就绪","正在重新载入 Dashboard…",false);
      setTimeout(function(){location.reload();},350);
      return;
    }
    if(restartOldInstanceId&&status.instanceId===restartOldInstanceId&&status.phase==="ready"){
      restartPollTimer=null;
      restartPollingActive=false;
      stopRestartElapsedTimer();
      restartStartedAt=0;
      setRestartOverlayActions(null);
      updateRestartOverlay("重启已取消","代理继续运行，新的请求已恢复接入。",false,true);
      const btn=document.getElementById("restartProxyBtn");
      if(btn){btn.disabled=false;btn.textContent="🔄 重启代理";}
      return;
    }
    setRestartOverlayActions(status);
    if(status.phase==="draining"){
      title="正在排空进行中的请求";
      detail=restartDrainDetail(status);
    }else{
      title="正在等待旧代理退出";
      detail="重启请求已提交，正在准备切换到新实例。";
    }
  }catch(e){
    if(!restartPollingActive)return;
    if(e.message.indexOf("管理认证失效")>=0){
      restartPollingActive=false;
      stopRestartElapsedTimer();
      setRestartOverlayActions(null);
      updateRestartOverlay("无法确认重启状态",e.message,true);
      return;
    }
  }
  if(!restartPollingActive)return;
  if(Date.now()-restartStartedAt>=120000)detail+=" 已等待较久，请检查 watchdog 日志。";
  updateRestartOverlay(title,detail,false);
  scheduleRestartStatusPoll(1000);
}
async function cancelPendingRestart(){
  if(!restartPollingActive||restartActionInFlight)return;
  if(!confirm("取消这次安全重启？\\n代理会立刻恢复接受新请求；已经被拒绝的排队请求不会恢复。"))return;
  restartActionInFlight=true;
  setRestartOverlayActions(restartLastStatus);
  updateRestartOverlay("正在取消重启","正在恢复代理的正常接入…",false);
  try{
    const r=await fetch("/__restart/cancel",{method:"POST",cache:"no-store"});
    let result={};
    try{result=await r.json();}catch(e){}
    restartLastStatus=result;
    restartActionInFlight=false;
    if(!r.ok||!result.ok){
      setRestartOverlayActions(result);
      updateRestartOverlay("未能取消重启",(result.error||"取消请求被拒绝（HTTP "+r.status+"）")+"；仍会继续监控重启状态。",true);
      scheduleRestartStatusPoll(700);
      return;
    }
    restartPollingActive=false;
    if(restartPollTimer){clearTimeout(restartPollTimer);restartPollTimer=null;}
    stopRestartElapsedTimer();
    restartStartedAt=0;
    setRestartOverlayActions(null);
    const cancelled=result.cancelledQueuedRequests||0;
    updateRestartOverlay("重启已取消","代理继续运行，新的请求已恢复接入。"+(cancelled?" 已拒绝的 "+cancelled+" 个排队请求不会恢复。":""),false,true);
    const btn=document.getElementById("restartProxyBtn");
    if(btn){btn.disabled=false;btn.textContent="🔄 重启代理";}
  }catch(e){
    restartActionInFlight=false;
    setRestartOverlayActions(null);
    updateRestartOverlay("未能取消重启",(e&&e.message)||"取消请求连接失败；仍会继续监控重启状态。",true);
    scheduleRestartStatusPoll(700);
  }
}
async function forcePendingRestart(){
  if(!restartPollingActive||restartActionInFlight)return;
  let status;
  try{
    const statusRes=await fetchRestartStatus();
    if(!statusRes.ok)throw new Error("无法读取重启状态（HTTP "+statusRes.status+"）");
    status=await statusRes.json();
  }catch(e){
    updateRestartOverlay("无法确认强制重启条件",(e&&e.message)||"请稍后重试。",true);
    scheduleRestartStatusPoll(700);
    return;
  }
  restartLastStatus=status;
  if(status.phase!=="draining"){
    setRestartOverlayActions(status);
    updateRestartOverlay("已无法强制重启","代理已不处于可强制重启的排空阶段。",true);
    scheduleRestartStatusPoll(700);
    return;
  }
  if(!status.canForce){
    setRestartOverlayActions(status);
    updateRestartOverlay("仍在安全排空",restartWaitText(status.forceAvailableInMs)+"后才能强制重启。",false);
    scheduleRestartStatusPoll(700);
    return;
  }
  const active=status.activeRequests||0;
  if(!confirm("强制重启会立即断开仍在执行的 "+active+" 个请求。\\nCodex CLI 任务可能部分执行或报错，未完成工作需要人工确认。\\n\\n确定强制重启？"))return;
  restartActionInFlight=true;
  setRestartOverlayActions(status);
  updateRestartOverlay("正在强制重启","将中断 "+active+" 个仍在执行的请求，并等待 watchdog 拉起新实例。",false);
  let forceRequestAttempted=false;
  try{
    forceRequestAttempted=true;
    const r=await fetch("/__restart/force",{method:"POST",cache:"no-store"});
    let result={};
    try{result=await r.json();}catch(e){}
    restartLastStatus=result;
    restartActionInFlight=false;
    if(!r.ok||!result.ok){
      setRestartOverlayActions(result);
      const retry=result.retryAfterMs?" 还需等待 "+restartWaitText(result.retryAfterMs)+"。":"";
      updateRestartOverlay("强制重启尚未执行",(result.error||"强制重启请求被拒绝（HTTP "+r.status+"）")+retry,false);
      scheduleRestartStatusPoll(700);
      return;
    }
    setRestartOverlayActions(null);
    updateRestartOverlay("正在强制重启","已确认中断 "+(result.interruptedActiveRequests||0)+" 个在途请求，正在等待新实例就绪。",false);
    scheduleRestartStatusPoll(500);
  }catch(e){
    restartActionInFlight=false;
    setRestartOverlayActions(null);
    if(forceRequestAttempted&&restartOldInstanceId){
      updateRestartOverlay("正在确认强制重启状态","请求连接在切换期间中断，正在等待新实例恢复。",false);
      scheduleRestartStatusPoll(700);
      return;
    }
    updateRestartOverlay("未能提交强制重启",(e&&e.message)||"请检查代理状态后重试。",true);
    scheduleRestartStatusPoll(700);
  }
}
async function restartProxy(){
  if(!confirm("确定要重启代理进程？\\n新的 API 请求会暂时暂停，进行中的请求会先排空。"))return;
  const btn=document.getElementById("restartProxyBtn");
  if(btn){btn.disabled=true;btn.textContent="正在重启…";}
  restartPollingActive=true;restartStartedAt=Date.now();restartOldInstanceId="";restartLastStatus=null;restartActionInFlight=false;
  startRestartElapsedTimer();
  setRestartOverlayActions(null);
  updateRestartOverlay("正在确认代理状态","正在提交安全重启请求…",false);
  let restartRequestAttempted=false,restartRequestRejected=false;
  try{
    const beforeRes=await fetchRestartStatus();
    if(!beforeRes.ok)throw new Error("无法读取当前代理状态（HTTP "+beforeRes.status+"）");
    const before=await beforeRes.json();
    restartOldInstanceId=before.instanceId||"";
    if(Number.isFinite(before.restartStartedAt)&&before.restartStartedAt>0)restartStartedAt=before.restartStartedAt;
    if(before.phase!=="ready"){
      restartLastStatus=before;
      setRestartOverlayActions(before);
      updateRestartOverlay("正在监控已有重启",before.phase==="draining"?restartDrainDetail(before):"旧代理正在退出，等待 watchdog 拉起新实例。",false);
      scheduleRestartStatusPoll(700);
      return;
    }
    restartRequestAttempted=true;
    const r=await fetch("/__restart",{method:"POST",cache:"no-store"});
    if(!r.ok){
      let result={};
      try{result=await r.json();}catch(e){}
      if(result.phase==="draining"){
        restartLastStatus=result;
        setRestartOverlayActions(result);
        updateRestartOverlay("正在监控已有重启",restartDrainDetail(result),false);
        scheduleRestartStatusPoll(700);
        return;
      }
      restartRequestRejected=true;
      throw new Error(result.error||"重启请求被拒绝（HTTP "+r.status+"）");
    }
    const result=await r.json();
    if(!result.ok){
      restartRequestRejected=true;
      throw new Error(result.error||"重启请求被拒绝（HTTP "+r.status+"）");
    }
    restartOldInstanceId=result.instanceId||restartOldInstanceId;
    restartLastStatus=result;
    if(Number.isFinite(result.restartStartedAt)&&result.restartStartedAt>0)restartStartedAt=result.restartStartedAt;
    const active=result.activeRequests||0;
    const queued=result.cancelledQueuedRequests||0;
    updateRestartOverlay("重启请求已提交",active?"正在等待 "+active+" 个进行中的请求完成。":"没有进行中的请求，正在停止旧代理实例。",false);
    if(queued>0)document.getElementById("restartOverlayDetail").textContent+=" 已取消 "+queued+" 个排队请求。";
    setRestartOverlayActions(result);
    scheduleRestartStatusPoll(750);
  }catch(e){
    if(restartRequestAttempted&&!restartRequestRejected&&restartOldInstanceId){
      updateRestartOverlay("正在确认重启状态","请求连接在重启期间中断，正在等待新实例恢复。",false);
      scheduleRestartStatusPoll(1000);
      return;
    }
    restartPollingActive=false;
    stopRestartElapsedTimer();
    setRestartOverlayActions(null);
    updateRestartOverlay("未能提交重启请求",e.message||"请检查代理状态后重试。",true);
    if(btn){btn.disabled=false;btn.textContent="🔄 重启代理";}
  }
}
function batchActionCards(action){
  if(action==="cancelboost"){
    fetch("http://localhost:3456/__boost-batch",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({mode:""})}).then(()=>setTimeout(loadKeys,100)).catch(()=>{});
    return;
  }
  const cbs=document.querySelectorAll("#grid .card-cb:checked");
  const sel=[...cbs].map(c=>parseInt(c.dataset.idx)).filter(i=>i>0);
  if(!sel.length){alert("请先勾选要操作的 Key");return}
  if(action==="shield"){
    if(!confirm("确定屏蔽选中的 "+sel.length+" 个 Key？"))return;
    sel.forEach(i=>fetch("http://localhost:3456/__patch-key-status",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx:i,status:"shielded"})}).catch(()=>{}));
  }else if(action==="reset"){
    sel.forEach(i=>fetch("http://localhost:3456/__reset-key",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx:i})}).catch(()=>{}));
  }else if(action==="use"||action==="roundrobin"||action==="random"){
    fetch("http://localhost:3456/__boost-batch",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({mode:action,idxs:sel})}).then(()=>setTimeout(loadKeys,100)).catch(()=>{});
  }
}
function cardShield(idx){
  if(!confirm("确定屏蔽 Key #"+idx+"？屏蔽后不再参与调度，可在管理弹窗恢复。"))return;
  fetch("http://localhost:3456/__patch-key-status",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({idx,status:"shielded"})}).then(()=>setTimeout(loadKeys,200)).catch(()=>{});
}
function selectAllCards(){
  document.querySelectorAll("#grid .card-cb").forEach(cb => cb.checked = true);
  updateBatchBar();
}
function deselectAllCards(){
  document.querySelectorAll("#grid .card-cb").forEach(cb => cb.checked = false);
  updateBatchBar();
}
function updateBatchBar(){
  const cbs=document.querySelectorAll("#grid .card-cb:checked");
  const bar=document.getElementById("batchBar");
  const cnt=document.getElementById("batchCount");
  const modeStatus=document.getElementById("batchModeStatus");
  const useBtn=document.getElementById("batchBoostUseBtn");
  const rrBtn=document.getElementById("batchBoostRRBtn");
  const randBtn=document.getElementById("batchBoostRandBtn");
  const cancelBtn=document.getElementById("batchCancelBoostBtn");
  if(!bar||!cnt)return;
  if(boostedBatchMode){
    bar.style.display="flex";cnt.style.display="none";
    if(modeStatus){modeStatus.style.display="inline";modeStatus.textContent="⏳ 批量优先 ("+({"use":"队列","roundrobin":"轮询","random":"🎲 随机"}[boostedBatchMode]||"")+") 中"}
    if(useBtn)useBtn.style.display="none";
    if(rrBtn)rrBtn.style.display="none";
    if(randBtn)randBtn.style.display="none";
    if(cancelBtn)cancelBtn.style.display="inline";
  }else if(cbs.length){
    bar.style.display="flex";cnt.style.display="inline";cnt.textContent="已选 "+cbs.length+" 个";
    if(modeStatus)modeStatus.style.display="none";
    if(useBtn)useBtn.style.display="inline";
    if(rrBtn)rrBtn.style.display="inline";
    if(randBtn)randBtn.style.display="inline";
    if(cancelBtn)cancelBtn.style.display="none";
  }else{
    bar.style.display="none";
  }
}
</script>
</body>
</html>`;
}

// --- Protocol converter functions ---
// Direction A: Responses ↔ Chat (Codex CLI ↔ non-OpenAI upstreams)
function responsesToChatRequest(upstreamUrl, body) {
  const chatBody = { model: body.model, messages: [], stream: body.stream, max_tokens: body.max_output_tokens };
  if (body.instructions) chatBody.messages.push({ role: "system", content: body.instructions });
  if (typeof body.input === "string") {
    chatBody.messages.push({ role: "user", content: body.input });
  } else if (Array.isArray(body.input)) {
    for (const m of body.input) {
      if (m.type === "function_call") {
        chatBody.messages.push({
          role: "assistant",
          content: null,
          tool_calls: [{ id: m.call_id, type: "function", function: { name: m.name, arguments: m.arguments || "" } }]
        });
      } else if (m.type === "function_call_output") {
        chatBody.messages.push({
          role: "tool",
          tool_call_id: m.call_id,
          content: typeof m.output === "string" ? m.output : JSON.stringify(m.output || "")
        });
      } else {
        const role = m.role === "developer" ? "system" : m.role || "user";
        let content = "";
        if (typeof m.content === "string") content = m.content;
        else if (Array.isArray(m.content)) content = m.content.map(c => c.text || "").join("\n");
        else if (typeof m.content === "object" && m.content) content = m.content.text || JSON.stringify(m.content);
        chatBody.messages.push({ role, content });
      }
    }
  }
  if (body.tools) chatBody.tools = body.tools.map(t => {
    if (t.type === "function") return { type: "function", function: { name: t.name, description: t.description, parameters: t.input_schema || t.parameters } };
    return t;
  });
  if (body.tool_choice) chatBody.tool_choice = body.tool_choice;
  if (body.temperature !== undefined) chatBody.temperature = body.temperature;
  if (body.top_p !== undefined) chatBody.top_p = body.top_p;
  if (body.stop) chatBody.stop = body.stop;
  if (body.metadata) chatBody.metadata = body.metadata;
  return chatBody;
}
const STREAM_TERMINAL_REASONS = new Set([
  "upstream_done",
  "upstream_eof_without_done",
  "upstream_eof_without_completed",
  "upstream_close",
  "upstream_aborted",
  "upstream_incomplete",
  "upstream_error",
  "upstream_idle_timeout",
  "stream_lifetime_timeout",
  "no_progress_timeout",
  "client_disconnect",
  "model_at_capacity",
  "insufficient_quota",
  "upstream_api_error",
]);

function normalizeStreamTerminalReason(reason, fallback) {
  return STREAM_TERMINAL_REASONS.has(reason) ? reason : fallback;
}

function createResponsesLifecycle(model) {
  return {
    protocol: "responses",
    responseId: "resp_" + crypto.randomUUID(),
    created: Math.floor(Date.now() / 1000),
    model: model || "",
    fullContent: "",
    inputTokens: 0,
    outputTokens: 0,
    terminalKind: null,
    terminalReason: null,
    sawDone: false,
    upstreamErrorMessage: "",
    _started: false,
    _transform: null,
    _metricsCallback: null,
    _onTerminal: null,
    _terminalNotified: false,
    _pushEvent(event, payload) {
      if (!this._transform || this._transform.destroyed || this._transform.readableEnded) return;
      this._transform.push(`event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`);
    },
    _endReadable() {
      if (this._transform && !this._transform.destroyed && !this._transform.readableEnded) {
        this._transform.push(null);
      }
    },
    _recordMetrics(success) {
      if (typeof this._metricsCallback === "function") this._metricsCallback(success);
    },
    _notifyTerminal() {
      if (this._terminalNotified) return;
      this._terminalNotified = true;
      if (typeof this._onTerminal === "function") this._onTerminal(this);
    },
    emitStart() {
      if (this._started) return;
      this._started = true;
      const response = { id: this.responseId, object: "response", created_at: this.created, model: this.model, status: "in_progress", output: [] };
      this._pushEvent("response.created", { type: "response.created", response });
      this._pushEvent("response.in_progress", { type: "response.in_progress", response });
    },
    emitCompleted(reason) {
      if (this.terminalKind) return false;
      if (!this._started) this.emitStart();
      this.terminalKind = "completed";
      this.terminalReason = normalizeStreamTerminalReason(reason, "upstream_done");
      this._pushEvent("response.output_text.done", { type: "response.output_text.done", delta: "" });
      this._pushEvent("response.completed", {
        type: "response.completed",
        response: {
          id: this.responseId,
          object: "response",
          created_at: this.created,
          model: this.model,
          status: "completed",
          output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text: this.fullContent }] }],
          usage: { input_tokens: this.inputTokens, output_tokens: this.outputTokens, total_tokens: this.inputTokens + this.outputTokens },
        },
      });
      if (this._transform && !this._transform.destroyed && !this._transform.readableEnded) {
        this._transform.push("data: [DONE]\n\n");
      }
      this._endReadable();
      this._recordMetrics(true);
      this._notifyTerminal();
      return true;
    },
    emitFailed(reason) {
      if (this.terminalKind) return false;
      if (!this._started) this.emitStart();
      this.terminalKind = "failed";
      this.terminalReason = normalizeStreamTerminalReason(reason, "upstream_error");
      this._pushEvent("response.failed", {
        type: "response.failed",
        response: { id: this.responseId, object: "response", created_at: this.created, model: this.model, status: "failed" },
      });
      this._endReadable();
      this._recordMetrics(false);
      this._notifyTerminal();
      return true;
    },
    noteClientCancelled(reason) {
      if (this.terminalKind) return false;
      this.terminalKind = "cancelled";
      this.terminalReason = normalizeStreamTerminalReason(reason, "client_disconnect");
      this._notifyTerminal();
      return true;
    },
  };
}

// Observes a native Responses SSE stream without rewriting normal upstream bytes.
// The only synthetic output is a terminal response.failed when the upstream ends
// without the response.completed event required by Codex CLI.
function createNativeResponsesTerminalProbe(model) {
  const decoder = new StringDecoder("utf8");
  let lineBuffer = "";
  let eventName = "";
  let dataLines = [];

  const lifecycle = {
    protocol: "responses",
    responseId: "resp_" + crypto.randomUUID(),
    created: Math.floor(Date.now() / 1000),
    model: String(model || "").slice(0, 160),
    fullContent: "",
    inputTokens: 0,
    outputTokens: 0,
    terminalKind: null,
    terminalReason: null,
    terminalSource: "native_responses_sse",
    sawDone: false,
    upstreamErrorMessage: "",
    _transform: null,
    _metricsCallback: null,
    _onTerminal: null,
    _terminalNotified: false,
    _syntheticTerminal: false,
    _pushEvent(event, payload) {
      if (!this._transform || this._transform.destroyed || this._transform.readableEnded) return false;
      this._transform.push(`event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`);
      return true;
    },
    _endReadable() {
      if (this._transform && !this._transform.destroyed && !this._transform.readableEnded) {
        this._transform.push(null);
      }
    },
    _recordMetrics(success) {
      if (typeof this._metricsCallback === "function") this._metricsCallback(success);
    },
    _notifyTerminal() {
      if (this._terminalNotified) return;
      this._terminalNotified = true;
      if (typeof this._onTerminal === "function") this._onTerminal(this);
    },
    _captureResponse(response) {
      if (!response || typeof response !== "object") return;
      if (typeof response.id === "string" && response.id.trim()) this.responseId = response.id.trim().slice(0, 200);
      if (typeof response.model === "string" && response.model.trim()) this.model = response.model.trim().slice(0, 160);
      const usage = response.usage && typeof response.usage === "object" ? response.usage : null;
      if (!usage) return;
      const input = Number(usage.input_tokens != null ? usage.input_tokens : usage.prompt_tokens);
      const output = Number(usage.output_tokens != null ? usage.output_tokens : usage.completion_tokens);
      if (Number.isFinite(input) && input >= 0) this.inputTokens = input;
      if (Number.isFinite(output) && output >= 0) this.outputTokens = output;
    },
    _setTerminal(kind, reason, message, source) {
      if (this.terminalKind) return false;
      this.terminalKind = kind;
      this.terminalReason = normalizeStreamTerminalReason(reason, kind === "completed" ? "upstream_done" : "upstream_api_error");
      this.terminalSource = source || "native_responses_sse";
      if (message) this.upstreamErrorMessage = sanitizeUpstreamErrorMessage(message);
      this._recordMetrics(kind === "completed");
      this._notifyTerminal();
      return true;
    },
    markCompleted() {
      return this._setTerminal("completed", "upstream_done", "", "native_responses_sse");
    },
    markFailed(reason, message) {
      return this._setTerminal("failed", reason, message, "native_responses_sse");
    },
    emitFailed(reason, message) {
      if (this.terminalKind) return false;
      const fallbackMessages = {
        upstream_eof_without_completed: "Upstream response stream ended before response.completed",
        upstream_close: "Upstream response stream closed before response.completed",
        upstream_aborted: "Upstream response stream was aborted before response.completed",
        upstream_error: "Upstream response stream failed before response.completed",
        upstream_idle_timeout: "Upstream response stream was idle before response.completed",
        stream_lifetime_timeout: "Upstream response stream exceeded its configured maximum duration",
        no_progress_timeout: "Upstream response stream made no progress before timing out",
      };
      const safeMessage = sanitizeUpstreamErrorMessage(message || fallbackMessages[reason] || "Upstream response stream failed before response.completed");
      if (!this._setTerminal("failed", reason, safeMessage, "native_responses_terminal_guard")) return false;
      this._syntheticTerminal = true;
      this._pushEvent("response.failed", {
        type: "response.failed",
        response: {
          id: this.responseId,
          object: "response",
          created_at: this.created,
          model: this.model,
          status: "failed",
          error: { code: this.terminalReason, message: safeMessage },
        },
      });
      this._endReadable();
      return true;
    },
    noteClientCancelled(reason) {
      if (this.terminalKind) return false;
      this.terminalKind = "cancelled";
      this.terminalReason = normalizeStreamTerminalReason(reason, "client_disconnect");
      this.terminalSource = "downstream_client";
      this._notifyTerminal();
      return true;
    },
  };

  const resetEvent = () => {
    eventName = "";
    dataLines = [];
  };

  const processEvent = () => {
    if (!eventName && !dataLines.length) return;
    const data = dataLines.join("\n");
    const declaredEvent = eventName.trim();
    resetEvent();
    if (data === "[DONE]") {
      lifecycle.sawDone = true;
      return;
    }
    let payload = null;
    if (data) {
      try { payload = JSON.parse(data); } catch (e) { return; }
    }
    const payloadType = payload && typeof payload.type === "string" ? payload.type : "";
    const type = declaredEvent || payloadType;
    if (payload && payload.response) lifecycle._captureResponse(payload.response);
    if (type === "response.completed") {
      lifecycle.markCompleted();
      return;
    }
    if (type === "response.failed") {
      const message = extractUpstreamErrorMessage(payload && (payload.error || (payload.response && payload.response.error) || payload));
      lifecycle.markFailed(message ? classifyUpstreamErrorMessage(message) : "upstream_api_error", message);
      return;
    }
    if (type === "response.incomplete") {
      const detail = payload && payload.response && payload.response.incomplete_details && payload.response.incomplete_details.reason;
      const message = detail ? `Upstream response is incomplete: ${detail}` : "Upstream response is incomplete";
      lifecycle.markFailed("upstream_incomplete", message);
      return;
    }
    if (type === "error" || (payload && (payload.error || payload.object === "error"))) {
      const message = extractUpstreamErrorMessage(payload) || "Upstream Responses SSE error";
      lifecycle.emitFailed(classifyUpstreamErrorMessage(message), message);
    }
  };

  const processLine = rawLine => {
    const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
    if (!line) {
      processEvent();
      return;
    }
    if (line.startsWith(":")) return;
    const colon = line.indexOf(":");
    const field = colon >= 0 ? line.slice(0, colon) : line;
    let value = colon >= 0 ? line.slice(colon + 1) : "";
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "event") eventName = value;
    else if (field === "data") dataLines.push(value);
  };

  const processText = text => {
    if (!text) return;
    lineBuffer += text;
    let newline;
    while ((newline = lineBuffer.indexOf("\n")) >= 0) {
      const line = lineBuffer.slice(0, newline);
      lineBuffer = lineBuffer.slice(newline + 1);
      processLine(line);
    }
  };

  const probe = new Transform({
    transform(chunk, encoding, cb) {
      if (lifecycle._syntheticTerminal) { cb(); return; }
      const raw = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding);
      this.push(raw);
      processText(decoder.write(raw));
      cb();
    },
    flush(cb) {
      if (!lifecycle._syntheticTerminal) {
        processText(decoder.end());
        if (lineBuffer) {
          processLine(lineBuffer);
          lineBuffer = "";
        }
        processEvent();
        if (!lifecycle.terminalKind) lifecycle.emitFailed("upstream_eof_without_completed");
      }
      cb();
    },
  });
  lifecycle._transform = probe;
  probe._lifecycle = lifecycle;
  probe._nativeResponsesProbe = true;
  return probe;
}

function createMessagesLifecycle(model) {
  return {
    protocol: "messages",
    responseId: "msg_" + crypto.randomUUID(),
    created: Math.floor(Date.now() / 1000),
    model: String(model || "").slice(0, 160),
    fullContent: "",
    inputTokens: 0,
    outputTokens: 0,
    terminalKind: null,
    terminalReason: null,
    terminalSource: "chat_to_messages_sse",
    sawDone: false,
    upstreamErrorMessage: "",
    _transform: null,
    _metricsCallback: null,
    _onTerminal: null,
    _terminalNotified: false,
    _syntheticTerminal: false,
    _pushEvent(event, payload) {
      if (!this._transform || this._transform.destroyed || this._transform.readableEnded) return false;
      this._transform.push(`event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`);
      return true;
    },
    _endReadable() {
      if (this._transform && !this._transform.destroyed && !this._transform.readableEnded) {
        this._transform.push(null);
      }
    },
    _recordMetrics(success) {
      if (typeof this._metricsCallback === "function") this._metricsCallback(success);
    },
    _notifyTerminal() {
      if (this._terminalNotified) return;
      this._terminalNotified = true;
      if (typeof this._onTerminal === "function") this._onTerminal(this);
    },
    emitCompleted() {
      if (this.terminalKind) return false;
      this.terminalKind = "completed";
      this.terminalReason = "upstream_done";
      this.terminalSource = "chat_to_messages_sse";
      this.sawDone = true;
      this._endReadable();
      this._recordMetrics(true);
      this._notifyTerminal();
      return true;
    },
    emitFailed(reason, message) {
      if (this.terminalKind) return false;
      const fallbackMessages = {
        upstream_eof_without_done: "Upstream chat stream ended before its [DONE] terminal",
        upstream_close: "Upstream chat stream closed before its [DONE] terminal",
        upstream_aborted: "Upstream chat stream was aborted before its [DONE] terminal",
        upstream_error: "Upstream chat stream failed before its [DONE] terminal",
        upstream_idle_timeout: "Upstream chat stream was idle before its [DONE] terminal",
        stream_lifetime_timeout: "Upstream chat stream exceeded its configured maximum duration",
        no_progress_timeout: "Upstream chat stream made no progress before timing out",
      };
      const safeMessage = sanitizeUpstreamErrorMessage(message || fallbackMessages[reason] || "Upstream chat stream failed");
      this.terminalKind = "failed";
      this.terminalReason = normalizeStreamTerminalReason(reason, "upstream_api_error");
      this.terminalSource = "chat_to_messages_terminal_guard";
      this.upstreamErrorMessage = safeMessage;
      this._syntheticTerminal = true;
      this._pushEvent("error", {
        type: "error",
        error: { type: "api_error", message: safeMessage },
      });
      this._endReadable();
      this._recordMetrics(false);
      this._notifyTerminal();
      return true;
    },
    noteClientCancelled(reason) {
      if (this.terminalKind) return false;
      this.terminalKind = "cancelled";
      this.terminalReason = normalizeStreamTerminalReason(reason, "client_disconnect");
      this.terminalSource = "downstream_client";
      this._notifyTerminal();
      return true;
    },
  };
}

// Lifecycle for an Anthropic Messages upstream converted to an OpenAI Chat
// stream. It is deliberately separate from the Messages lifecycle above: the
// failure event must use the protocol expected by the downstream Chat client.
function createChatLifecycle(model) {
  return {
    protocol: "chat",
    responseId: "chatcmpl_" + crypto.randomUUID(),
    created: Math.floor(Date.now() / 1000),
    model: String(model || "").slice(0, 160),
    fullContent: "",
    inputTokens: 0,
    outputTokens: 0,
    terminalKind: null,
    terminalReason: null,
    terminalSource: "messages_to_chat_sse",
    sawDone: false,
    upstreamErrorMessage: "",
    _transform: null,
    _metricsCallback: null,
    _onTerminal: null,
    _terminalNotified: false,
    _syntheticTerminal: false,
    _pushData(payload) {
      if (!this._transform || this._transform.destroyed || this._transform.readableEnded) return false;
      this._transform.push(`data: ${JSON.stringify(payload)}\n\n`);
      return true;
    },
    _endReadable() {
      if (this._transform && !this._transform.destroyed && !this._transform.readableEnded) {
        this._transform.push(null);
      }
    },
    _recordMetrics(success) {
      if (typeof this._metricsCallback === "function") this._metricsCallback(success);
    },
    _notifyTerminal() {
      if (this._terminalNotified) return;
      this._terminalNotified = true;
      if (typeof this._onTerminal === "function") this._onTerminal(this);
    },
    emitCompleted() {
      if (this.terminalKind) return false;
      this.terminalKind = "completed";
      this.terminalReason = "upstream_done";
      this.terminalSource = "messages_to_chat_sse";
      this.sawDone = true;
      this._endReadable();
      this._recordMetrics(true);
      this._notifyTerminal();
      return true;
    },
    emitFailed(reason, message) {
      if (this.terminalKind) return false;
      const fallbackMessages = {
        upstream_eof_without_done: "Upstream Messages stream ended before message_stop",
        upstream_close: "Upstream Messages stream closed before message_stop",
        upstream_aborted: "Upstream Messages stream was aborted before message_stop",
        upstream_error: "Upstream Messages stream failed before message_stop",
        upstream_idle_timeout: "Upstream Messages stream was idle before message_stop",
        stream_lifetime_timeout: "Upstream Messages stream exceeded its configured maximum duration",
      };
      const safeMessage = sanitizeUpstreamErrorMessage(message || fallbackMessages[reason] || "Upstream Messages stream failed");
      this.terminalKind = "failed";
      this.terminalReason = normalizeStreamTerminalReason(reason, "upstream_api_error");
      this.terminalSource = "messages_to_chat_terminal_guard";
      this.upstreamErrorMessage = safeMessage;
      this._syntheticTerminal = true;
      this._pushData({ error: { message: safeMessage, type: "api_error", code: this.terminalReason } });
      this._endReadable();
      this._recordMetrics(false);
      this._notifyTerminal();
      return true;
    },
    noteClientCancelled(reason) {
      if (this.terminalKind) return false;
      this.terminalKind = "cancelled";
      this.terminalReason = normalizeStreamTerminalReason(reason, "client_disconnect");
      this.terminalSource = "downstream_client";
      this._notifyTerminal();
      return true;
    },
  };
}

function createChatToResponsesStream(lifecycle) {
  const Transform = require("stream").Transform;
  let buffer = "";
  const seenToolCalls = new Set();
  const toolCallArgs = {};
  const toolCallIds = {};

  const finishToolCalls = output => {
    for (const callIdx of seenToolCalls) {
      const callId = toolCallIds[callIdx];
      if (callId && toolCallArgs[callIdx] !== undefined) {
        output.push(`event: response.function_call_arguments.done\ndata: ${JSON.stringify({type:"response.function_call_arguments.done",arguments:toolCallArgs[callIdx],item_id:callId,output_index:callIdx})}\n\n`);
        output.push(`event: response.output_item.done\ndata: ${JSON.stringify({type:"response.output_item.done",output_index:callIdx,item:{type:"function_call",id:callId,name:"",arguments:toolCallArgs[callIdx],status:"completed"}})}\n\n`);
        delete toolCallArgs[callIdx];
      }
    }
  };

  const processLine = (line, output) => {
    if (lifecycle.terminalKind || lifecycle.sawDone || !line.startsWith("data:")) return false;
    const data = line.slice(5).trim();
    if (!data) return false;
    if (data === "[DONE]") {
      lifecycle.sawDone = true;
      return true;
    }
    let parsed;
    try { parsed = JSON.parse(data); } catch(e) { return false; }
    if (parsed && (parsed.error || parsed.object === "error" || parsed.type === "error")) {
      const errMsg = extractUpstreamErrorMessage(parsed) || "upstream error";
      lifecycle.upstreamErrorMessage = errMsg;
      lifecycle.emitFailed(classifyUpstreamErrorMessage(errMsg));
      return true;
    }
    if (parsed.object !== "chat.completion.chunk") return false;

    lifecycle.model = parsed.model || lifecycle.model;
    if (!lifecycle._started) lifecycle.emitStart();
    const delta = parsed.choices?.[0]?.delta;
    if (delta?.reasoning_content) {
      lifecycle.fullContent += delta.reasoning_content;
      output.push(`event: response.output_text.delta\ndata: ${JSON.stringify({type:"response.output_text.delta",delta:delta.reasoning_content})}\n\n`);
    }
    if (delta?.content) {
      lifecycle.fullContent += delta.content;
      output.push(`event: response.output_text.delta\ndata: ${JSON.stringify({type:"response.output_text.delta",delta:delta.content})}\n\n`);
    }
    if (delta?.tool_calls) {
      for (const tc of delta.tool_calls) {
        const callIdx = tc.index || 0;
        if (!seenToolCalls.has(callIdx)) {
          seenToolCalls.add(callIdx);
          toolCallArgs[callIdx] = "";
          toolCallIds[callIdx] = tc.id || ("call_" + callIdx + "_" + Date.now().toString(36));
          const fnName = tc.function?.name || "";
          output.push(`event: response.output_item.added\ndata: ${JSON.stringify({type:"response.output_item.added",output_index:callIdx,item:{type:"function_call",id:toolCallIds[callIdx],name:fnName,arguments:"",status:"in_progress"}})}\n\n`);
          output.push(`event: response.function_call_arguments.starting\ndata: ${JSON.stringify({type:"response.function_call_arguments.starting",item_id:toolCallIds[callIdx],output_index:callIdx})}\n\n`);
        }
        const args = tc.function?.arguments || "";
        toolCallArgs[callIdx] += args;
        output.push(`event: response.function_call_arguments.delta\ndata: ${JSON.stringify({type:"response.function_call_arguments.delta",delta:args,item_id:toolCallIds[callIdx],output_index:callIdx})}\n\n`);
      }
    }
    if (parsed.choices?.[0]?.finish_reason === "tool_calls") finishToolCalls(output);
    if (parsed.usage) {
      lifecycle.inputTokens = parsed.usage.prompt_tokens || 0;
      lifecycle.outputTokens = parsed.usage.completion_tokens || 0;
    }
    return false;
  };

  return new Transform({
    readableObjectMode: false, writableObjectMode: false,
    transform(chunk, encoding, cb) {
      if (lifecycle.terminalKind || lifecycle.sawDone) { cb(); return; }
      buffer += chunk.toString();
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";
      for (const line of lines) {
        if (processLine(line, this)) break;
      }
      cb();
    },
    flush(cb) {
      if (!lifecycle.terminalKind && buffer) processLine(buffer, this);
      buffer = "";
      if (!lifecycle.terminalKind) {
        finishToolCalls(this);
        if (lifecycle.sawDone) lifecycle.emitCompleted("upstream_done");
        else lifecycle.emitFailed("upstream_eof_without_done");
      }
      cb();
    }
  });
}
function chatToResponsesResponse(upstreamUrl, chatBody) {
  const choice = chatBody.choices?.[0];
  const message = choice?.message || {};
  const text = message.reasoning_content ? (message.reasoning_content + "\n\n" + (message.content || "")) : (message.content || "");
  return {
    id: "resp_" + Date.now(),
    object: "response",
    created_at: Math.floor(Date.now() / 1000),
    model: chatBody.model || "",
    output: [{
      type: "message",
      role: "assistant",
      content: [{ type: "output_text", text }]
    }],
    usage: {
      input_tokens: chatBody.usage?.prompt_tokens || 0,
      output_tokens: chatBody.usage?.completion_tokens || 0,
      output_characters: text.length,
      input_characters: 0
    }
  };
}
// Direction B: Messages → Chat (Claude Code CLI → non-Anthropic upstreams)
function messagesToChatRequest(upstreamUrl, body) {
  const chatBody = { model: body.model, messages: [], stream: body.stream, max_tokens: body.max_tokens };
  if (body.system) {
    if (typeof body.system === "string") {
      chatBody.messages.push({ role: "system", content: body.system });
    } else if (Array.isArray(body.system)) {
      const blocks = body.system.map(s => {
        if (typeof s === "string") return { type: "text", text: s };
        const block = { type: "text", text: s.text || "" };
        if (s.cache_control) block.cache_control = s.cache_control;
        return block;
      });
      chatBody.messages.push({ role: "system", content: blocks.length === 1 && !blocks[0].cache_control ? blocks[0].text : blocks });
    } else {
      chatBody.messages.push({ role: "system", content: String(body.system) });
    }
  }
  if (Array.isArray(body.messages)) {
    for (const m of body.messages) {
      let content = "";
      if (typeof m.content === "string") {
        content = m.content;
      } else if (Array.isArray(m.content)) {
        const blocks = [];
        for (const c of m.content) {
          if (c.type === "text") {
            const block = { type: "text", text: c.text || "" };
            if (c.cache_control) block.cache_control = c.cache_control;
            blocks.push(block);
          } else if (c.type === "image" || c.type === "image_url") {
            blocks.push({ type: "image_url", image_url: { url: c.source?.data || c.image_url?.url || "" } });
          } else if (c.type === "thinking") {
            // Embed thinking text; forwardRequest will handle per-provider
            blocks.push({ type: "text", text: `【thinking】${c.thinking || ""}【/thinking】` });
          } else if (c.type === "tool_use" || c.type === "tool_result") {
            // handled separately via tool_calls/tool messages
          }
        }
        if (blocks.length === 0) {
          content = "";
        } else if (blocks.length === 1 && !blocks[0].cache_control) {
          content = blocks[0].text;
        } else {
          content = blocks;
        }
      }
      chatBody.messages.push({ role: m.role === "assistant" ? "assistant" : "user", content });
    }
  }
  if (body.tools) {
    chatBody.tools = body.tools.map(t => ({
      type: "function",
      function: { name: t.name, description: t.description || "", parameters: t.input_schema || {} }
    }));
  }
  if (body.tool_choice) chatBody.tool_choice = body.tool_choice.type || "auto";
  if (body.metadata) chatBody.metadata = body.metadata;
  if (body.stop_sequences) chatBody.stop = body.stop_sequences;
  if (body.temperature !== undefined) chatBody.temperature = body.temperature;
  if (body.top_p !== undefined) chatBody.top_p = body.top_p;
  // preserve thinking as enable_thinking for providers that support it
  if (body.thinking && body.thinking.type === "enabled") {
    chatBody.enable_thinking = true;
    if (body.thinking.budget_tokens) chatBody.thinking_budget = body.thinking.budget_tokens;
  }
  return chatBody;
}
function createChatToMessagesStream(lifecycle) {
  const Transform = require("stream").Transform;
  lifecycle = lifecycle || createMessagesLifecycle("");
  const decoder = new StringDecoder("utf8");
  let lineBuffer = "";
  let stopReason = "end_turn";
  let messageStarted = false;
  let nextBlockIndex = 0;
  let textBlock = null;
  let thinkingBlock = null;
  let activeContentBlock = null;
  const toolBlocks = new Map();
  const openedBlocks = [];

  const pushEvent = (output, event, payload) => {
    output.push(`event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`);
  };

  const ensureMessageStarted = output => {
    if (messageStarted) return;
    messageStarted = true;
    pushEvent(output, "message_start", {
      type: "message_start",
      message: {
        id: lifecycle.responseId,
        type: "message",
        role: "assistant",
        content: [],
        model: lifecycle.model,
        stop_reason: null,
        usage: { input_tokens: lifecycle.inputTokens, output_tokens: lifecycle.outputTokens },
      },
    });
  };

  const startBlock = (output, kind, contentBlock) => {
    ensureMessageStarted(output);
    const block = { kind, index: nextBlockIndex++, stopped: false };
    openedBlocks.push(block);
    pushEvent(output, "content_block_start", {
      type: "content_block_start",
      index: block.index,
      content_block: contentBlock,
    });
    return block;
  };

  const stopBlock = (output, block) => {
    if (!block || block.stopped) return;
    block.stopped = true;
    if (activeContentBlock === block) activeContentBlock = null;
    pushEvent(output, "content_block_stop", { type: "content_block_stop", index: block.index });
  };

  const ensureTextStarted = output => {
    if (!textBlock || textBlock.stopped) {
      stopBlock(output, activeContentBlock);
      textBlock = startBlock(output, "text", { type: "text", text: "" });
    }
    activeContentBlock = textBlock;
    return textBlock;
  };

  const ensureThinkingStarted = output => {
    if (!thinkingBlock || thinkingBlock.stopped) {
      stopBlock(output, activeContentBlock);
      thinkingBlock = startBlock(output, "thinking", { type: "thinking", thinking: "" });
    }
    activeContentBlock = thinkingBlock;
    return thinkingBlock;
  };

  const ensureToolStarted = (output, call) => {
    const callIndex = Number.isInteger(call && call.index) ? call.index : 0;
    let block = toolBlocks.get(callIndex);
    if (block && !block.stopped) return block;
    const name = call && call.function && call.function.name;
    if (!name) return null;
    stopBlock(output, activeContentBlock);
    block = startBlock(output, "tool", {
      type: "tool_use",
      id: call.id || `toolu_${Date.now().toString(36)}_${callIndex}`,
      name,
      input: {},
    });
    toolBlocks.set(callIndex, block);
    return block;
  };

  const closeOpenedBlocks = output => {
    for (const block of openedBlocks) stopBlock(output, block);
  };

  const closeToolBlocks = output => {
    for (const block of toolBlocks.values()) stopBlock(output, block);
  };

  const emitCompleted = output => {
    if (lifecycle.terminalKind) return false;
    lifecycle.sawDone = true;
    ensureMessageStarted(output);
    closeOpenedBlocks(output);
    pushEvent(output, "message_delta", {
      type: "message_delta",
      delta: { stop_reason: stopReason, stop_sequence: null },
      usage: { output_tokens: lifecycle.outputTokens },
    });
    pushEvent(output, "message_stop", { type: "message_stop" });
    lifecycle.emitCompleted();
    return true;
  };

  const processLine = (rawLine, output) => {
    if (lifecycle.terminalKind || lifecycle.sawDone) return true;
    const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
    if (!line.startsWith("data:")) return false;
    const data = line.slice(5).trim();
    if (!data) return false;
    if (data === "[DONE]") return emitCompleted(output);

    let parsed;
    try { parsed = JSON.parse(data); } catch (e) { return false; }
    if (parsed && (parsed.error || parsed.object === "error" || parsed.type === "error")) {
      const message = extractUpstreamErrorMessage(parsed) || "Upstream chat SSE error";
      lifecycle.emitFailed(classifyUpstreamErrorMessage(message), message);
      return true;
    }
    if (!parsed || parsed.object !== "chat.completion.chunk") return false;

    lifecycle.model = parsed.model || lifecycle.model;
    if (parsed.usage) {
      lifecycle.inputTokens = parsed.usage.prompt_tokens || lifecycle.inputTokens;
      lifecycle.outputTokens = parsed.usage.completion_tokens || lifecycle.outputTokens;
    }
    const choice = parsed.choices && parsed.choices[0];
    const delta = choice && choice.delta;
    const finishReason = choice && choice.finish_reason;
    if (finishReason === "stop") stopReason = "end_turn";
    else if (finishReason === "length") stopReason = "max_tokens";
    else if (finishReason === "tool_calls") stopReason = "tool_use";
    else if (finishReason) stopReason = finishReason;

    if (delta && delta.reasoning_content) {
      const block = ensureThinkingStarted(output);
      lifecycle.fullContent += delta.reasoning_content;
      pushEvent(output, "content_block_delta", {
        type: "content_block_delta",
        index: block.index,
        delta: { type: "thinking_delta", thinking: delta.reasoning_content },
      });
    }
    if (delta && delta.content) {
      const block = ensureTextStarted(output);
      lifecycle.fullContent += delta.content;
      pushEvent(output, "content_block_delta", {
        type: "content_block_delta",
        index: block.index,
        delta: { type: "text_delta", text: delta.content },
      });
    }
    if (delta && Array.isArray(delta.tool_calls)) {
      for (const call of delta.tool_calls) {
        const block = ensureToolStarted(output, call);
        const args = call && call.function && call.function.arguments;
        if (block && args) {
          pushEvent(output, "content_block_delta", {
            type: "content_block_delta",
            index: block.index,
            delta: { type: "input_json_delta", partial_json: args },
          });
        }
      }
    }
    if (finishReason === "tool_calls") closeToolBlocks(output);
    return false;
  };

  const transform = new Transform({
    readableObjectMode: false,
    writableObjectMode: false,
    transform(chunk, encoding, cb) {
      if (lifecycle.terminalKind || lifecycle.sawDone) { cb(); return; }
      lineBuffer += decoder.write(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding));
      let newline;
      while ((newline = lineBuffer.indexOf("\n")) >= 0) {
        const line = lineBuffer.slice(0, newline);
        lineBuffer = lineBuffer.slice(newline + 1);
        if (processLine(line, this)) {
          lineBuffer = "";
          break;
        }
      }
      cb();
    },
    flush(cb) {
      if (!lifecycle.terminalKind) {
        lineBuffer += decoder.end();
        if (lineBuffer) processLine(lineBuffer, this);
        lineBuffer = "";
      }
      if (!lifecycle.terminalKind) lifecycle.emitFailed("upstream_eof_without_done");
      cb();
    },
  });
  lifecycle._transform = transform;
  transform._lifecycle = lifecycle;
  return transform;
}
function chatToMessagesResponse(upstreamUrl, chatBody) {
  const choice = chatBody.choices?.[0];
  const message = choice?.message || {};
  const usage = chatBody.usage || {};
  const content = [{ type: "text", text: message.content || "" }];
  if (message.reasoning_content) {
    content.unshift({ type: "thinking", thinking: message.reasoning_content });
  }
  return {
    id: "msg_" + Date.now(),
    type: "message",
    role: "assistant",
    content,
    model: chatBody.model || "",
    stop_reason: choice?.finish_reason === "stop" ? "end_turn" : choice?.finish_reason || "end_turn",
    stop_sequence: null,
    usage: { input_tokens: usage.prompt_tokens || 0, output_tokens: usage.completion_tokens || 0 }
  };
}
// Direction C: Chat → Messages (Chat clients → Anthropic upstream)
function chatToMessagesRequest(upstreamUrl, body) {
  const msgsBody = { model: body.model, messages: [], max_tokens: body.max_tokens || 4096, stream: body.stream };
  if (Array.isArray(body.messages)) {
    const systemParts = [];
    for (const m of body.messages) {
      if (m.role === "system") { systemParts.push(m.content); continue; }
      let content = typeof m.content === "string" ? m.content : Array.isArray(m.content) ? m.content.map(c => c.text || "").join("\n") : String(m.content);
      if (m.role === "assistant" && Array.isArray(m.tool_calls)) {
        const blocks = [{ type: "text", text: content }];
        for (const tc of m.tool_calls) {
          blocks.push({ type: "tool_use", id: tc.id, name: tc.function.name, input: JSON.parse(tc.function.arguments || "{}") });
        }
        msgsBody.messages.push({ role: "assistant", content: blocks });
      } else if (m.role === "tool") {
        msgsBody.messages.push({ role: "user", content: [{ type: "tool_result", tool_use_id: m.tool_call_id, content: content }] });
      } else {
        msgsBody.messages.push({ role: m.role || "user", content });
      }
    }
    if (systemParts.length) msgsBody.system = systemParts.join("\n");
  }
  if (body.tools) {
    msgsBody.tools = body.tools.map(t => ({
      name: t.function?.name || t.name || "",
      description: t.function?.description || t.description || "",
      input_schema: t.function?.parameters || t.parameters || {}
    }));
  }
  if (body.tool_choice) {
    msgsBody.tool_choice = typeof body.tool_choice === "string" ? { type: body.tool_choice } : body.tool_choice;
  }
  if (body.stop) msgsBody.stop_sequences = Array.isArray(body.stop) ? body.stop : [body.stop];
  if (body.temperature !== undefined) msgsBody.temperature = body.temperature;
  if (body.top_p !== undefined) msgsBody.top_p = body.top_p;
  if (body.max_tokens) msgsBody.max_tokens = body.max_tokens;
  return msgsBody;
}
function createMessagesToChatStream(lifecycle) {
  const Transform = require("stream").Transform;
  lifecycle = lifecycle || createChatLifecycle("");
  const decoder = new StringDecoder("utf8");
  let lineBuffer = "";
  let nextToolCallIndex = 0;
  const toolCalls = new Map();

  const emitChatChunk = (output, choice, usage) => {
    output.push(`data: ${JSON.stringify({
      choices: [choice],
      ...(usage ? { usage } : {}),
      object: "chat.completion.chunk",
      model: lifecycle.model,
    })}\n\n`);
  };

  const emitCompleted = output => {
    if (lifecycle.terminalKind) return false;
    lifecycle.sawDone = true;
    output.push("data: [DONE]\n\n");
    lifecycle.emitCompleted();
    return true;
  };

  const upstreamBlockKey = value => Number.isInteger(value) ? value : String(value == null ? "" : value);
  const ensureToolCall = (output, upstreamIndex, contentBlock) => {
    const key = upstreamBlockKey(upstreamIndex);
    let call = toolCalls.get(key);
    if (!call) {
      const block = contentBlock || {};
      call = {
        index: nextToolCallIndex++,
        id: block.id || `call_${Date.now().toString(36)}_${nextToolCallIndex}`,
        name: block.name || "unknown_tool",
        emittedStart: false,
      };
      toolCalls.set(key, call);
    }
    if (!call.emittedStart) {
      call.emittedStart = true;
      const input = contentBlock && contentBlock.input && typeof contentBlock.input === "object" && Object.keys(contentBlock.input).length
        ? JSON.stringify(contentBlock.input)
        : "";
      emitChatChunk(output, {
        index: 0,
        delta: {
          tool_calls: [{
            index: call.index,
            id: call.id,
            type: "function",
            function: { name: call.name, arguments: input },
          }],
        },
        finish_reason: null,
      });
    }
    return call;
  };

  const processLine = (rawLine, output) => {
    if (lifecycle.terminalKind || lifecycle.sawDone) return true;
    const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
    if (!line.startsWith("data:")) return false;
    const data = line.slice(5).trim();
    if (!data) return false;
    let parsed;
    try { parsed = JSON.parse(data); } catch (e) { return false; }
    if (parsed && (parsed.error || parsed.type === "error" || parsed.object === "error")) {
      const message = extractUpstreamErrorMessage(parsed) || "Upstream Messages SSE error";
      lifecycle.emitFailed(classifyUpstreamErrorMessage(message), message);
      return true;
    }
    if (!parsed || typeof parsed.type !== "string") return false;

    if (parsed.type === "message_start") {
      lifecycle.model = parsed.message && parsed.message.model || lifecycle.model;
      lifecycle.inputTokens = parsed.message && parsed.message.usage && parsed.message.usage.input_tokens || lifecycle.inputTokens;
    } else if (parsed.type === "content_block_start" && parsed.content_block && parsed.content_block.type === "tool_use") {
      ensureToolCall(output, parsed.index, parsed.content_block);
    } else if (parsed.type === "content_block_delta" && parsed.delta && parsed.delta.type === "text_delta") {
      const text = parsed.delta.text || "";
      lifecycle.fullContent += text;
      emitChatChunk(output, { index: 0, delta: { role: "assistant", content: text }, finish_reason: null });
    } else if (parsed.type === "content_block_delta" && parsed.delta && parsed.delta.type === "thinking_delta") {
      const thinking = parsed.delta.thinking || "";
      lifecycle.fullContent += thinking;
      emitChatChunk(output, { index: 0, delta: { role: "assistant", reasoning_content: thinking }, finish_reason: null });
    } else if (parsed.type === "content_block_delta" && parsed.delta && parsed.delta.type === "input_json_delta") {
      const call = ensureToolCall(output, parsed.index, null);
      emitChatChunk(output, {
        index: 0,
        delta: { tool_calls: [{ index: call.index, function: { arguments: parsed.delta.partial_json || "" } }] },
        finish_reason: null,
      });
    } else if (parsed.type === "message_delta") {
      lifecycle.outputTokens = parsed.usage && parsed.usage.output_tokens || lifecycle.outputTokens;
      const finishMap = { end_turn: "stop", max_tokens: "length", tool_use: "tool_calls" };
      const finishReason = finishMap[parsed.delta && parsed.delta.stop_reason] || parsed.delta && parsed.delta.stop_reason || "stop";
      emitChatChunk(output, {
        index: 0,
        delta: {},
        finish_reason: finishReason,
      }, {
        prompt_tokens: lifecycle.inputTokens,
        completion_tokens: lifecycle.outputTokens,
        total_tokens: lifecycle.inputTokens + lifecycle.outputTokens,
      });
    } else if (parsed.type === "message_stop") {
      return emitCompleted(output);
    }
    return false;
  };

  const transform = new Transform({
    readableObjectMode: false,
    writableObjectMode: false,
    transform(chunk, encoding, cb) {
      if (lifecycle.terminalKind || lifecycle.sawDone) { cb(); return; }
      lineBuffer += decoder.write(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding));
      let newline;
      while ((newline = lineBuffer.indexOf("\n")) >= 0) {
        const line = lineBuffer.slice(0, newline);
        lineBuffer = lineBuffer.slice(newline + 1);
        if (processLine(line, this)) {
          lineBuffer = "";
          break;
        }
      }
      cb();
    },
    flush(cb) {
      if (!lifecycle.terminalKind) {
        lineBuffer += decoder.end();
        if (lineBuffer) processLine(lineBuffer, this);
        lineBuffer = "";
      }
      if (!lifecycle.terminalKind) lifecycle.emitFailed("upstream_eof_without_done");
      cb();
    },
  });
  lifecycle._transform = transform;
  transform._lifecycle = lifecycle;
  return transform;
}
function messagesToChatResponse(upstreamUrl, msgsBody) {
  const content = msgsBody.content || [];
  const textBlocks = content.filter(c => c.type === "text");
  const text = textBlocks.map(t => t.text || "").join("");
  const usage = msgsBody.usage || {};
  const finishMap = { end_turn: "stop", max_tokens: "length", tool_use: "tool_calls" };
  return {
    id: "chatcmpl-" + Date.now(),
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model: msgsBody.model || "",
    choices: [{
      index: 0,
      message: { role: "assistant", content: text },
      finish_reason: finishMap[msgsBody.stop_reason] || msgsBody.stop_reason || "stop"
    }],
    usage: { prompt_tokens: usage.input_tokens || 0, completion_tokens: usage.output_tokens || 0, total_tokens: (usage.input_tokens || 0) + (usage.output_tokens || 0) }
  };
}
// --- End protocol converter functions ---

// --- Mixed account forwarder for /v1/chat/completions (Anthropic → non-Anthropic fallback) ---
const FALLBACK_CORS = { "access-control-allow-origin": "*", "content-type": "application/json; charset=utf-8" };
function forwardChatCompletions(method, chatHeaders, chatBody, msgsHeaders, msgsBody, clientRes, group) {
  group = group || "A";
  let responded = false;
  const respond = (code, data) => { if (!responded && !clientRes.destroyed && !clientRes.headersSent) { responded = true; clientRes.writeHead(code, { "content-type": "application/json" }); clientRes.end(JSON.stringify(data)); } };

  // Phase 1: Anthropic accounts with Messages body
  const anthroAccounts = [];
  for (let i = 0; i < accounts.length; i++) {
    if (accounts[i].status === "active" && !inCooldown(i) && isMessagesNative(accounts[i].url) && (accounts[i].group || "A") === group) {
      const ks = getKeyState(i);
      if (ks.status !== "discarded" && ks.status !== "locked") anthroAccounts.push(i);
    }
  }
  let attempt1 = 0;
  const tryAnthropic = () => {
    if (responded || attempt1 >= anthroAccounts.length) { if (!responded) tryChat(); return; }
    const idx = anthroAccounts[attempt1++];
    forwardRequest(idx, method, msgsHeaders, msgsBody, clientRes, "/v1/messages", (r) => {
      if (responded) return;
      if (r.switched) { console.log(`[proxy] #${idx+1} → anthropic ${r.code||"err"}`); return tryAnthropic(); }
      responded = true;
    }, createMessagesToChatStream());
  };

  // Phase 2: non-Anthropic accounts with original Chat body
  const chatAccounts = [];
  for (let i = 0; i < accounts.length; i++) {
    if (accounts[i].status === "active" && !inCooldown(i) && !isMessagesNative(accounts[i].url) && (accounts[i].group || "A") === group) {
      const ks = getKeyState(i);
      if (ks.status !== "discarded" && ks.status !== "locked") chatAccounts.push(i);
    }
  }
  let attempt2 = 0;
  const tryChat = () => {
    if (responded || attempt2 >= chatAccounts.length) { respond(502, { error: "All keys exhausted" }); return; }
    const idx = chatAccounts[attempt2++];
    forwardRequest(idx, method, chatHeaders, chatBody, clientRes, "/v1/chat/completions", (r) => {
      if (responded) return;
      if (r.switched) { console.log(`[proxy] #${idx+1} → chat ${r.code||"err"}`); return tryChat(); }
      responded = true;
    });
  };

  if (anthroAccounts.length) tryAnthropic(); else tryChat();
}
// --- End Mixed account forwarder ---

// --- HTTP Server ---
function createGroupServer(groupName, port) {
  const server = http.createServer((req, res) => {
  const pathname = (req.url || "/").split("?")[0];
  if (groupName !== "A" && (pathname.startsWith("/__") || pathname === "/" || pathname === "/dashboard" || pathname === "/metrics")) {
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "Admin panel only on primary port (A)" }));
    return;
  }
  if (!pathname.startsWith("/__") && pathname !== "/" && pathname !== "/dashboard" && pathname !== "/metrics") {
    lastRequestTime = Date.now();
    const pause = getGroupPause(groupName);
    if (pause) {
      const retryAfter = Math.max(1, Math.ceil((pause.expiresAt - Date.now()) / 1000));
      res.writeHead(503, { "content-type": "application/json", "retry-after": String(retryAfter) });
      res.end(JSON.stringify({ error: `group ${groupName} is temporarily paused by an incident action`, retryAfter, resumeAt: pause.expiresAt }));
      return;
    }
  }

  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "access-control-allow-origin": "null",
      "access-control-allow-methods": "GET, PUT, POST, OPTIONS",
      "access-control-allow-headers": "content-type, authorization",
      "access-control-max-age": "86400",
    });
    res.end();
    return;
  }

  if (req.method === "GET" && pathname === "/favicon.ico") {
    res.writeHead(204); res.end();
    return;
  }
  if (req.method === "GET" && (pathname === "/" || pathname === "/dashboard")) {
    res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    res.end(getDashboardHTML());
    return;
  }

  const cors = { "content-type": "application/json; charset=utf-8" };

  if ((pathname.startsWith("/__") || pathname === "/metrics") && pathname !== "/__auth_check" && !checkAdminAuth(req)) {
    res.writeHead(401, cors);
    res.end(JSON.stringify({ error: "unauthorized" }));
    return;
  }

  if (req.method === "GET" && pathname === "/__auth_check") {
    res.writeHead(200, cors);
    res.end(JSON.stringify({ configured: !!(config.adminToken) }));
    return;
  }

  if (req.method === "GET" && pathname === "/__restart-status") {
    res.writeHead(200, { ...cors, "cache-control": "no-store" });
    res.end(JSON.stringify({ ok: true, ...buildRestartStatus() }));
    return;
  }

  if (req.method === "GET" && pathname === "/__update-status") {
    const requestUrl = new URL(req.url, "http://localhost");
    const forceRefresh = requestUrl.searchParams.get("refresh") === "1";
    getUpdateStatus(forceRefresh).then((status) => {
      if (res.destroyed) return;
      res.writeHead(status.ok ? 200 : 502, { ...cors, "cache-control": "no-store" });
      res.end(JSON.stringify(status));
    }).catch((err) => {
      if (res.destroyed) return;
      res.writeHead(502, { ...cors, "cache-control": "no-store" });
      res.end(JSON.stringify({ ok: false, error: String(err && err.message ? err.message : err).slice(0, 240) }));
    });
    return;
  }

  if (req.method === "GET" && pathname === "/__task-insight-status") {
    res.writeHead(200, { ...cors, "cache-control": "no-store" });
    res.end(JSON.stringify({ ok: true, ...taskInsightStatusPayload() }));
    return;
  }

  if (req.method === "POST" && pathname === "/__tasks/distill-now") {
    runTaskDistill().then(r => {
      if (res.destroyed) return;
      res.writeHead(200, cors);
      res.end(JSON.stringify({ ok: true, ...r }));
    }).catch(e => {
      if (res.destroyed) return;
      res.writeHead(500, cors);
      res.end(JSON.stringify({ ok: false, error: String(e.message || e).slice(0, 240) }));
    });
    return;
  }

  if (req.method === "GET" && (pathname === "/__tasks" || pathname === "/__tasks/export" || pathname === "/__tasks/report")) {
    const requestUrl = new URL(req.url, "http://localhost");
    const now = Date.now();
    const parseMs = (v, fallback) => {
      if (!v) return fallback;
      const n = Date.parse(String(v));
      return Number.isFinite(n) ? n : fallback;
    };
    const from = parseMs(requestUrl.searchParams.get("from"), 0);
    const to = parseMs(requestUrl.searchParams.get("to"), now);
    const project = (requestUrl.searchParams.get("project") || "").trim();
    const status = (requestUrl.searchParams.get("status") || "").trim();
    const model = (requestUrl.searchParams.get("model") || "").trim();
    const q = (requestUrl.searchParams.get("q") || "").trim().toLowerCase();
    const limit = Math.min(500, Math.max(1, parseInt(requestUrl.searchParams.get("limit"), 10) || 200));
    let sessions = taskListAll(now).filter(s => {
      if (s.start > to || s.end < from) return false;
      if (status && s.status !== status) return false;
      if (project) {
        if (project === "__unclassified__") { if (s.projectName) return false; }
        else if (s.projectName !== project) return false;
      }
      if (model && !(s.models || []).includes(model)) return false;
      if (q) {
        const hay = [s.projectName || "", s.client || "", s.instructions || [], s.tools || [], s.files || [], s.models || []].map(v => Array.isArray(v) ? v.join(" ") : String(v)).join(" ").toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    }).sort((a, b) => b.start - a.start);
    if (pathname === "/__tasks/report") {
      const report = taskInsightBuildReport((requestUrl.searchParams.get("mode") || "").trim() || (config.taskInsight && config.taskInsight.distill && config.taskInsight.distill.report) || "daily", now);
      res.writeHead(200, cors);
      res.end(JSON.stringify({ ok: true, ...report, status: taskInsightStatusPayload() }));
      return;
    }
    if (pathname === "/__tasks/export") {
      const rows = sessions.slice(0, 2000);
      const esc = v => {
        const s = Array.isArray(v) ? v.join("; ") : String(v == null ? "" : v);
        return '"' + s.replace(/"/g, '""') + '"';
      };
      const header = ["任务ID", "项目", "客户端", "分组", "开始", "结束", "状态", "请求数", "成功", "失败", "输入token", "输出token", "费用", "模型轨迹", "工具", "文件", "指令(截断)", "摘要"];
      const lines = [header.join(",")];
      for (const s of rows) {
        lines.push([
          esc(s.id), esc(s.projectName || "未分类"), esc(s.client), esc(s.group),
          esc(new Date(s.start).toISOString()), esc(new Date(s.end).toISOString()), esc(s.status),
          esc(s.requestCount), esc(s.successCount), esc(s.failCount),
          esc(s.inputTokens), esc(s.outputTokens), esc(Number(s.cost).toFixed(4)),
          esc((s.models || []).join(" → ")), esc((s.tools || []).join(", ")), esc((s.files || []).join(", ")),
          esc((s.instructions || []).join(" | ")),
          esc(s.distill ? s.distill.summary : ""),
        ].join(","));
      }
      res.writeHead(200, { ...cors, "content-type": "text/csv; charset=utf-8", "content-disposition": "attachment; filename=task-insight.csv", "cache-control": "no-store" });
      res.end("\uFEFF" + lines.join("\n"));
      return;
    }
    sessions = sessions.slice(0, limit);
    res.writeHead(200, { ...cors, "cache-control": "no-store" });
    res.end(JSON.stringify({ ok: true, tasks: sessions, total: sessions.length, status: taskInsightStatusPayload() }));
    return;
  }

  if (restartState.phase !== "ready" && !["/__restart", "/__restart/cancel", "/__restart/force"].includes(pathname)) {
    res.writeHead(503, { ...cors, "retry-after": "5" });
    res.end(JSON.stringify({ error: "proxy is restarting", ...buildRestartStatus() }));
    return;
  }

  if (req.method === "GET" && pathname === "/__status") {
    res.writeHead(200, cors);
    const data = buildStatusData();
    res.end(JSON.stringify({ keys: data, boostedIdx: _boostKey >= 0 ? _boostKey + 1 : -1, boostedBatch: _boostBatch.map(i => i + 1), boostedBatchMode: _boostBatchMode, lastRequestTime, lastKeyUseTime, lastResumeTime }, null, 2));
    return;
  }

  if (req.method === "GET" && pathname === "/__test_port") {
    const u = new URL(req.url, "http://localhost");
    const port = parseInt(u.searchParams.get("port"));
    let running = false;
    if (port) {
      running = Object.values(servers).some(srv => { const a = srv.address(); return a && a.port === port; });
    }
    res.writeHead(200, cors);
    res.end(JSON.stringify({ ok: true, running }));
    return;
  }

  if (pathname === "/__codex-log-maintenance/check") {
    if (req.method !== "POST") {
      res.writeHead(405, cors);
      res.end(JSON.stringify({ error: "method not allowed" }));
      return;
    }
    const bodyChunks = [];
    let bodyBytes = 0;
    let tooLarge = false;
    req.on("data", chunk => {
      bodyBytes += chunk.length;
      if (bodyBytes > 32 * 1024) { tooLarge = true; return; }
      bodyChunks.push(chunk);
    });
    req.on("end", async () => {
      try {
        if (tooLarge) throw new Error("request body is too large");
        const payload = JSON.parse(Buffer.concat(bodyChunks).toString("utf8"));
        if (!payload || typeof payload !== "object" || Array.isArray(payload)) throw new Error("a configuration object is required");
        const candidateInput = Object.prototype.hasOwnProperty.call(payload, "codexLogMaintenance") ? payload.codexLogMaintenance : payload;
        if (!candidateInput || typeof candidateInput !== "object" || Array.isArray(candidateInput) || !Object.prototype.hasOwnProperty.call(candidateInput, "dbPath") || !String(candidateInput.dbPath == null ? "" : candidateInput.dbPath).trim()) {
          throw new Error("database path is required");
        }
        const candidate = normalizeCodexLogMaintenanceConfig(candidateInput);
        const check = await invokeCodexLogMaintainer("check", candidate);
        if (!check.ok) {
          res.writeHead(400, cors);
          res.end(JSON.stringify({ ok: false, error: formatCodexLogMaintenanceError(check), errorCode: check.errorCode || "invalid" }));
          return;
        }
        res.writeHead(200, cors);
        res.end(JSON.stringify({ ok: true, check }));
      } catch (error) {
        res.writeHead(400, cors);
        res.end(JSON.stringify({ ok: false, error: String(error && error.message || error).slice(0, 240) }));
      }
    });
    return;
  }

  if (pathname === "/__codex-log-maintenance/run") {
    if (req.method !== "POST") {
      res.writeHead(405, cors);
      res.end(JSON.stringify({ error: "method not allowed" }));
      return;
    }
    if (!config.codexLogMaintenance || !config.codexLogMaintenance.enabled) {
      res.writeHead(409, cors);
      res.end(JSON.stringify({ ok: false, error: "请先启用并保存 Codex SQLite 日志维护配置" }));
      return;
    }
    const checkCfg = normalizeCodexLogMaintenanceConfig(config.codexLogMaintenance);
    invokeCodexLogMaintainer("check", checkCfg).then(check => {
      if (res.destroyed) return;
      if (!check.ok) {
        res.writeHead(400, cors);
        res.end(JSON.stringify({ ok: false, error: formatCodexLogMaintenanceError(check), errorCode: check.errorCode || "invalid", runtime: getCodexLogMaintenanceRuntimeStatus() }));
        return;
      }
      applyCodexLogMaintenanceResult(check);
      res.writeHead(200, cors);
      res.end(JSON.stringify({ ok: true, result: check, runtime: getCodexLogMaintenanceRuntimeStatus() }));
    }).catch(error => {
      if (res.destroyed) return;
      res.writeHead(503, cors);
      res.end(JSON.stringify({ ok: false, error: String(error && error.message || error).slice(0, 240), runtime: getCodexLogMaintenanceRuntimeStatus() }));
    });
    return;
  }

  if (pathname === "/__codex-log-maintenance/clean") {
    if (req.method !== "POST") {
      res.writeHead(405, cors);
      res.end(JSON.stringify({ error: "method not allowed" }));
      return;
    }
    if (!config.codexLogMaintenance || !config.codexLogMaintenance.enabled) {
      res.writeHead(409, cors);
      res.end(JSON.stringify({ ok: false, error: "请先启用并保存 Codex SQLite 日志维护配置", runtime: getCodexLogMaintenanceRuntimeStatus() }));
      return;
    }
    runCodexLogMaintenance("manual_clean", { requireIdle: true, vacuum: true, timeoutMs: CODEX_LOG_MAINTENANCE_CLEAN_TIMEOUT_MS, maxBatches: CODEX_LOG_MAINTENANCE_CLEAN_MAX_BATCHES }).then(result => {
      if (res.destroyed) return;
      const refused = result && (result.result === "in_progress" || result.result === "database_active");
      const statusCode = refused ? 409 : result && result.ok ? 200 : 503;
      res.writeHead(statusCode, cors);
      res.end(JSON.stringify({ ok: !!(result && result.ok), result, runtime: getCodexLogMaintenanceRuntimeStatus(), error: result && result.ok ? "" : formatCodexLogMaintenanceError(result) }));
    }).catch(error => {
      if (res.destroyed) return;
      res.writeHead(503, cors);
      res.end(JSON.stringify({ ok: false, error: String(error && error.message || error).slice(0, 240), runtime: getCodexLogMaintenanceRuntimeStatus() }));
    });
    return;
  }

  if (pathname === "/__config") {
    if (req.method === "GET") {
      res.writeHead(200, cors);
      res.end(JSON.stringify({ ...config, autoRecoverNextTime, autoRecoverDailyNextTime, autoRecoverPollNextTime, lastRequestTime, lastKeyUseTime, lastResumeTime, codexLogMaintenanceRuntime: getCodexLogMaintenanceRuntimeStatus() }, null, 2));
      return;
    }
    if (req.method === "PUT") {
      const bodyChunks = [];
      req.on("data", c => bodyChunks.push(c));
      req.on("end", async () => {
        const body = Buffer.concat(bodyChunks).toString('utf-8');
        try {
          const c = JSON.parse(body);
          if (!c || typeof c !== "object" || Array.isArray(c)) throw new Error("configuration object is required");
          if (Object.prototype.hasOwnProperty.call(c, "autoResumeProjects")) {
            validateAutoResumeProjects(c.autoResumeProjects);
          }
          await enqueueConfigSave(async () => {
            if (Object.prototype.hasOwnProperty.call(c, "updateBaselineTag")) {
              const rawBaselineTag = c.updateBaselineTag == null ? "" : String(c.updateBaselineTag).trim();
              const normalizedBaselineTag = normalizeUpdateBaselineTag(rawBaselineTag);
              if (rawBaselineTag && !normalizedBaselineTag) {
                throw new Error("updateBaselineTag must be a stable Release tag such as v2.29.1");
              }
              c.updateBaselineTag = normalizedBaselineTag;
            }
            const cur = JSON.parse(fs.readFileSync(CONFIG_FILE, "utf-8"));
            const oldGroups = (cur.groups && typeof cur.groups === 'object') ? JSON.parse(JSON.stringify(cur.groups)) : {A: 3456};
            Object.assign(cur, c);
            normalizeRuntimeStorageConfig(cur);
            normalizeResponsesStreamConfig(cur);
            normalizeAutoResumeConfig(cur);
            const changesDefaultPrices = Object.prototype.hasOwnProperty.call(c, "prices");
            const changesDefaultBytesPerToken = Object.prototype.hasOwnProperty.call(c, "bytesPerToken");
            if (changesDefaultPrices || changesDefaultBytesPerToken) {
              const defaultPricing = normalizeDefaultPricing(cur.prices, cur.bytesPerToken, true, {
                prices: changesDefaultPrices,
                bytesPerToken: changesDefaultBytesPerToken,
              });
              cur.prices = defaultPricing.prices;
              cur.bytesPerToken = defaultPricing.bytesPerToken;
            }
            if (Object.prototype.hasOwnProperty.call(c, "modelPricing")) {
              cur.modelPricing = normalizeModelPricing(cur.modelPricing, true);
            }
            if (Object.prototype.hasOwnProperty.call(c, "taskInsight")) {
              cur.taskInsight = normalizeTaskInsightConfig(c.taskInsight);
            }
            if (Object.prototype.hasOwnProperty.call(c, "codexLogMaintenance")) {
              cur.codexLogMaintenance = normalizeCodexLogMaintenanceConfig(cur.codexLogMaintenance);
              if (cur.codexLogMaintenance.enabled) {
                const check = await invokeCodexLogMaintainer("check", cur.codexLogMaintenance);
                if (!check.ok) throw new Error(formatCodexLogMaintenanceError(check));
              }
            }
            // Handle group actions
            if (c._groupAction) {
              if (c._groupAction === "addGroup" && c._groupName && c._groupPort) {
                const gName = c._groupName.toUpperCase();
                const gPort = parseInt(c._groupPort);
                if (!gPort || gPort < 1024 || gPort > 65535) throw new Error("Port must be 1024-65535");
                if (cur.groups && Object.values(cur.groups).includes(gPort)) throw new Error("Port already in use by another group");
                cur.groups = cur.groups || {};
                cur.groups[gName] = gPort;
                c._groupName = gName; c._groupPort = gPort;
                delete c._groupAction;
              } else if (c._groupAction === "removeGroup" && c._groupName) {
                if (c._groupName === "A") throw new Error("Cannot remove group A");
                if (cur.groups) { delete cur.groups[c._groupName]; }
                stopGroup(c._groupName);
                delete c._groupAction; delete c._groupName;
              } else if (c._groupAction === "setGroupPort" && c._groupName && c._groupPort) {
                const gName = c._groupName.toUpperCase();
                const gPort = parseInt(c._groupPort);
                if (!gPort || gPort < 1024 || gPort > 65535) throw new Error("Port must be 1024-65535");
                const portUsed = Object.entries(cur.groups||{}).some(([n,p]) => p === gPort && n !== gName);
                if (portUsed) throw new Error("Port already in use by another group");
                cur.groups = cur.groups || {};
                cur.groups[gName] = gPort;
                if (servers[gName]) { stopGroup(gName); }
                c._groupName = gName; c._groupPort = gPort;
                delete c._groupAction;
              } else if (c._groupAction === "toggleGroup" && c._groupName) {
                if (c._groupName === "A") throw new Error("Cannot disable group A");
                const port = cur.groups && cur.groups[c._groupName];
                if (!port) throw new Error("Group not found: "+c._groupName);
                cur.groupEnabled = cur.groupEnabled || {};
                if (c._groupEnabled === false) {
                  stopGroup(c._groupName);
                  cur.groupEnabled[c._groupName] = false;
                } else {
                  startGroup(c._groupName, port);
                  cur.groupEnabled[c._groupName] = true;
                }
                delete c._groupAction; delete c._groupName; delete c._groupEnabled;
              }
            }
            // Clean up group action metadata so it doesn't pollute config.json
            delete cur._groupAction; delete cur._groupName; delete cur._groupPort; delete cur._groupEnabled;
            writeConfigAtomically(cur);
            const savedNextTime = autoRecoverNextTime;
            const savedDailyNextTime = autoRecoverDailyNextTime;
            const savedInterval = config.autoRecoverInterval;
            const savedDailyDays = config.autoRecoverDailyDays;
            const savedDailyHour = config.autoRecoverDailyHour;
            const savedDailyMin = config.autoRecoverDailyMinute;
            loadConfig();
            applyRuntimeStoragePolicy();
            // Sync group servers with new config
            const newGroups = config.groups || {A: 3456};
            // Start new groups
            for (const [name, port] of Object.entries(newGroups)) {
              if (!oldGroups[name] && name !== "A") {
                startGroup(name, port).catch(e => console.error(`[proxy] Failed to start group ${name}: ${e.message}`));
              }
            }
            // Stop removed groups
            for (const name of Object.keys(oldGroups)) {
              if (!newGroups[name] && name !== "A") {
                stopGroup(name);
              }
            }
            // Restart groups with changed port
            for (const [name, port] of Object.entries(newGroups)) {
              if (oldGroups[name] && oldGroups[name] !== port && name !== "A") {
                stopGroup(name);
                startGroup(name, port).catch(e => console.error(`[proxy] Failed to restart group ${name}: ${e.message}`));
              }
            }
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
            } else if (config.autoRecoverDaily && savedDailyNextTime > 0 && savedDailyNextTime <= Date.now() &&
                savedDailyDays === config.autoRecoverDailyDays &&
                savedDailyHour === config.autoRecoverDailyHour &&
                savedDailyMin === config.autoRecoverDailyMinute) {
              if (autoRecoverDailyTimer) clearTimeout(autoRecoverDailyTimer);
              autoRecover();
              autoRecoverDailyNextTime = calcNextDailyRun(Date.now(), savedDailyDays, savedDailyHour, savedDailyMin);
              scheduleDailyRecover();
            }
          });
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
          const isAnthropic = targetUrl.hostname === "api.anthropic.com";
          const opts = {
            hostname: targetUrl.hostname,
            port: targetUrl.port || (targetUrl.protocol === "http:" ? 80 : 443),
            path: isAnthropic ? "/v1/messages" : "/v1/models",
            method: isAnthropic ? "POST" : "GET",
            headers: isAnthropic
              ? { authorization: "Bearer " + key, "content-type": "application/json", "anthropic-version": "2023-06-01" }
              : { authorization: "Bearer " + key, "content-type": "application/json" },
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
                let modelCount = 0;
                try {
                  if (isAnthropic) {
                    const j = JSON.parse(data);
                    model = j.model || "claude (unknown)";
                    modelCount = 1;
                  } else {
                    const j = JSON.parse(data);
                    if (j.data && j.data.length) {
                      model = j.data.map(m => m.id).join(", ");
                      modelCount = j.data.length;
                    }
                  }
                } catch (e) {}
                res.writeHead(200, cors);
                res.end(JSON.stringify({ ok: true, status: testRes.statusCode, duration: dur, model, modelCount }));
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
          testReq.end(isAnthropic ? JSON.stringify({ model: "claude-sonnet-4-20250514", max_tokens: 1, messages: [{ role: "user", content: "hi" }] }) : undefined);
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
        if (ks && ks.failTime) raw[i]._failTime = ks.failTime;
        if (ks && ks.lastStatus != null) raw[i]._lastStatus = ks.lastStatus;
        if (ks && ks.lastTime) raw[i]._lastTime = ks.lastTime;
        if (ks && ks.lastModel) raw[i]._lastModel = ks.lastModel;
        if (ks) {
          const act = raw[i].activatedAt || ks.activatedAt || null;
          raw[i].activatedAt = raw[i]._activatedAt = act;
        }
        if (i < accounts.length && accounts[i]) { raw[i]._available = !inCooldown(i); if (accounts[i].models && accounts[i].models.length) raw[i]._models = accounts[i].models; }
        if (raw[i].timeWindow) raw[i]._inTimeWindow = isInTimeWindow(raw[i]);
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
          const parsed = JSON.parse(body);
          // Handle batch group update
          if (parsed && parsed._batchGroup) {
            const { keys: targetKeys, group: rawGroup } = parsed._batchGroup;
            const targetGroup = rawGroup.toUpperCase();
            if (!Array.isArray(targetKeys) || !targetGroup) throw new Error("batchGroup needs keys[] and group");
            const current = JSON.parse(fs.readFileSync(KEYS_FILE, "utf-8"));
            let count = 0;
            for (const k of current) {
              if (targetKeys.includes(k.key)) { k.group = targetGroup; count++; }
            }
            if (!count) throw new Error("No matching keys found");
            const raw = JSON.stringify(current, null, 2);
            fs.writeFileSync(KEYS_FILE, raw, "utf-8");
            loadAccounts();
            broadcastStatus();
            res.writeHead(200, cors);
            res.end(JSON.stringify({ ok: true, count, message: `${count} keys moved to group ${targetGroup}` }));
            return;
          }
          const arr = parsed;
          if (!Array.isArray(arr)) throw new Error("must be an array");
          for (const k of arr) {
            if (!k.key || !k.url) throw new Error("each entry needs key + url");
            if (!k.key.startsWith("sk-")) throw new Error("key must start with sk-");
          }
          const raw = JSON.stringify(arr, null, 2);
          fs.writeFileSync(KEYS_FILE, raw, "utf-8");
          loadAccounts();
          for (let i = 0; i < accounts.length; i++) {
            const ks = getKeyState(i);
            if (ks && ks.failCode && ks.failPeriod) {
              const curr = keyPeriod(accounts[i].reset, i);
              if (ks.failPeriod !== curr) {
                delete ks.failCode;
                delete ks.failTime;
                delete ks.failPeriod;
                delete ks.failCount;
              }
            }
          }
          saveState();
          broadcastStatus();
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
          ks.failReason = null;
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
            ks.failReason = null;
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

  if (pathname === "/__log-overview") {
    if (req.method !== "GET") {
      res.writeHead(405, cors);
      res.end(JSON.stringify({ error: "method not allowed" }));
      return;
    }
    const url = new URL(req.url, "http://localhost");
    const query = parseLogQuery(url);
    const overview = buildLogOverview({ since: query.since, until: query.until });
    res.writeHead(200, { ...cors, "cache-control": "no-store" });
    res.end(JSON.stringify({ ok: true, overview, summaryRebuild: logSummaryRebuildState }));
    return;
  }

  if (pathname === "/__incidents") {
    if (req.method !== "GET") {
      res.writeHead(405, cors);
      res.end(JSON.stringify({ error: "method not allowed" }));
      return;
    }
    res.writeHead(200, { ...cors, "cache-control": "no-store" });
    res.end(JSON.stringify({
      ok: true,
      incidents: listLogIncidents(),
      groupPauses: listActiveGroupPauses(),
      summaryReady: logSummaryLoaded,
      summaryRebuild: logSummaryRebuildState,
    }));
    return;
  }

  if (pathname === "/__incident-action") {
    if (req.method !== "POST") {
      res.writeHead(405, cors);
      res.end(JSON.stringify({ error: "method not allowed" }));
      return;
    }
    const bodyChunks = [];
    let bodyLength = 0;
    let bodyTooLarge = false;
    req.on("data", chunk => {
      bodyLength += chunk.length;
      if (bodyLength > 64 * 1024) bodyTooLarge = true;
      else bodyChunks.push(chunk);
    });
    req.on("end", () => {
      try {
        if (bodyTooLarge) throw new Error("request body is too large");
        const body = JSON.parse(Buffer.concat(bodyChunks).toString("utf8"));
        const result = handleLogIncidentAction(body);
        res.writeHead(200, { ...cors, "cache-control": "no-store" });
        res.end(JSON.stringify({ ok: true, ...result }));
      } catch (error) {
        res.writeHead(400, cors);
        res.end(JSON.stringify({ error: String(error.message || error).slice(0, 240) }));
      }
    });
    return;
  }

  if (pathname === "/__logs/rebuild-summary") {
    if (req.method !== "POST") {
      res.writeHead(405, cors);
      res.end(JSON.stringify({ error: "method not allowed" }));
      return;
    }
    const result = startLogSummaryRebuild();
    res.writeHead(result.ok ? 202 : 409, { ...cors, "cache-control": "no-store" });
    res.end(JSON.stringify(result));
    return;
  }

  if (pathname === "/__logs") {
    if (req.method !== "GET") {
      res.writeHead(405, cors);
      res.end(JSON.stringify({ error: "method not allowed" }));
      return;
    }
    const url = new URL(req.url, "http://localhost");
    const format = url.searchParams.get("format") || "";
    if (format && format !== "csv") {
      res.writeHead(400, cors);
      res.end(JSON.stringify({ error: "unsupported log format" }));
      return;
    }
    const query = parseLogQuery(url, format);
    const overview = () => buildLogOverview({ since: query.since, until: query.until });
    if (query.mode === "recent") {
      const memoryEntries = getRecentLogEntries(query);
      const sendRecentResponse = (entries, source, history = {}) => {
        res.writeHead(200, { ...cors, "cache-control": "no-store" });
        res.end(JSON.stringify({
          entries,
          stats: summarizeLogEntries(entries),
          overview: overview(),
          mode: "recent",
          source,
          historyChecked: history.checked === true,
          historyUnavailable: history.unavailable === true,
          historyFilesAvailable: Math.max(0, Math.floor(Number(history.filesAvailable) || 0)),
          historyFilesScanned: Math.max(0, Math.floor(Number(history.filesScanned) || 0)),
          hasMore: false,
          nextCursor: "",
          truncated: false,
        }));
      };
      if (memoryEntries.length >= query.limit) {
        sendRecentResponse(memoryEntries, "memory");
        return;
      }

      // Saved logs remain queryable even if writing new file logs is disabled.
      // Read only a bounded tail in the Worker so opening the log panel never
      // synchronously scans files in the proxy process.
      const fallbackQuery = { ...query, mode: "history", cursor: null, limit: LOG_HISTORY_MAX_LIMIT };
      const task = startHistoricalLogQuery(fallbackQuery, { maxLimit: LOG_HISTORY_MAX_LIMIT });
      let detached = false;
      const cancelIfDetached = () => {
        if (!res.writableEnded) {
          detached = true;
          task.cancel();
        }
      };
      req.once("aborted", cancelIfDetached);
      res.once("close", cancelIfDetached);
      const clearRequestListeners = () => {
        req.removeListener("aborted", cancelIfDetached);
        res.removeListener("close", cancelIfDetached);
      };
      task.promise.then(result => {
        clearRequestListeners();
        if (detached || res.destroyed || res.writableEnded) return;
        const historicalEntries = Array.isArray(result.entries) ? result.entries : [];
        const entries = mergeRecentLogEntries(memoryEntries, historicalEntries, query.limit);
        const source = entries.length > memoryEntries.length ? "history" : "memory";
        sendRecentResponse(entries, source, {
          checked: true,
          filesAvailable: result.filesAvailable,
          filesScanned: result.filesScanned,
        });
      }).catch(() => {
        clearRequestListeners();
        if (detached || res.destroyed || res.writableEnded) return;
        // History lookup is an enhancement. A busy or unavailable Worker must
        // not make the basic in-memory log view fail.
        sendRecentResponse(memoryEntries, "memory", { unavailable: true });
      });
      return;
    }

    const task = startHistoricalLogQuery(query, { maxLimit: format === "csv" ? LOG_EXPORT_MAX_LIMIT : LOG_HISTORY_MAX_LIMIT });
    let detached = false;
    const cancelIfDetached = () => {
      if (!res.writableEnded) {
        detached = true;
        task.cancel();
      }
    };
    req.once("aborted", cancelIfDetached);
    res.once("close", cancelIfDetached);
    const clearRequestListeners = () => {
      req.removeListener("aborted", cancelIfDetached);
      res.removeListener("close", cancelIfDetached);
    };
    task.promise.then(result => {
      clearRequestListeners();
      if (detached || res.destroyed) return;
      const entries = Array.isArray(result.entries) ? result.entries : [];
      if (format === "csv") {
        res.writeHead(200, { ...cors, "content-type": "text/csv; charset=utf-8", "content-disposition": "attachment; filename=proxy-logs.csv", "cache-control": "no-store" });
        res.end(formatLogCsv(entries));
        return;
      }
      res.writeHead(200, { ...cors, "cache-control": "no-store" });
      res.end(JSON.stringify({
        entries,
        stats: logMetricsToStats(result.stats || createLogAggregate()),
        overview: overview(),
        mode: "history",
        hasMore: result.hasMore === true,
        nextCursor: encodeLogCursor(result.nextCursor),
        truncated: result.truncated === true,
        scannedBytes: Number(result.scannedBytes) || 0,
        filesAvailable: Math.max(0, Math.floor(Number(result.filesAvailable) || 0)),
        filesScanned: Math.max(0, Math.floor(Number(result.filesScanned) || 0)),
      }));
    }).catch(error => {
      clearRequestListeners();
      if (detached || res.destroyed) return;
      const message = String(error && error.message ? error.message : error).slice(0, 240);
      const status = /timed out/i.test(message) ? 504 : /busy/i.test(message) ? 503 : 500;
      res.writeHead(status, { ...cors, "cache-control": "no-store" });
      res.end(JSON.stringify({ error: message }));
    });
    return;
  }

  if (pathname === "/__export-logs") {
    if (req.method !== "GET") {
      res.writeHead(405, cors);
      res.end(JSON.stringify({ error: "method not allowed" }));
      return;
    }
    const url = new URL(req.url, "http://localhost");
    const format = url.searchParams.get("format") || "csv";
    if (!["csv", "jsonl"].includes(format)) {
      res.writeHead(400, cors);
      res.end(JSON.stringify({ error: "unsupported export format" }));
      return;
    }
    const query = parseLogQuery(url, format);
    const date = String(url.searchParams.get("date") || "");
    if (date) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
        res.writeHead(400, cors);
        res.end(JSON.stringify({ error: "date must be YYYY-MM-DD" }));
        return;
      }
      const start = Date.parse(`${date}T00:00:00.000Z`);
      if (!Number.isFinite(start)) {
        res.writeHead(400, cors);
        res.end(JSON.stringify({ error: "invalid date" }));
        return;
      }
      query.since = start;
      query.until = start + 86400000 - 1;
    }
    query.mode = "history";
    query.limit = Math.min(LOG_EXPORT_MAX_LIMIT, query.limit || LOG_EXPORT_MAX_LIMIT);
    const task = startHistoricalLogQuery(query, { maxLimit: LOG_EXPORT_MAX_LIMIT });
    let detached = false;
    const cancelIfDetached = () => {
      if (!res.writableEnded) {
        detached = true;
        task.cancel();
      }
    };
    req.once("aborted", cancelIfDetached);
    res.once("close", cancelIfDetached);
    const clearRequestListeners = () => {
      req.removeListener("aborted", cancelIfDetached);
      res.removeListener("close", cancelIfDetached);
    };
    task.promise.then(result => {
      clearRequestListeners();
      if (detached || res.destroyed) return;
      const entries = Array.isArray(result.entries) ? result.entries : [];
      if (format === "jsonl") {
        res.writeHead(200, { ...cors, "content-type": "application/x-ndjson; charset=utf-8", "content-disposition": "attachment; filename=proxy-logs.jsonl", "cache-control": "no-store" });
        res.end(entries.map(entry => JSON.stringify(entry)).join("\n"));
        return;
      }
      res.writeHead(200, { ...cors, "content-type": "text/csv; charset=utf-8", "content-disposition": "attachment; filename=proxy-logs.csv", "cache-control": "no-store" });
      res.end(formatLogCsv(entries));
    }).catch(error => {
      clearRequestListeners();
      if (detached || res.destroyed) return;
      const message = String(error && error.message ? error.message : error).slice(0, 240);
      const status = /timed out/i.test(message) ? 504 : /busy/i.test(message) ? 503 : 500;
      res.writeHead(status, { ...cors, "cache-control": "no-store" });
      res.end(JSON.stringify({ error: message }));
    });
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
  if (pathname === "/__patch-key") {
    if (req.method === "POST") {
      const bodyChunks = [];
      req.on("data", c => bodyChunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(bodyChunks).toString('utf-8');
        try {
          const { idx, tz, timeWindow } = JSON.parse(body);
          if (typeof idx !== "number") throw new Error("idx required");
          const raw = JSON.parse(fs.readFileSync(KEYS_FILE, "utf-8"));
          const ai = idx - 1;
          if (ai < 0 || ai >= raw.length) throw new Error("invalid idx");
          if (tz !== undefined) {
            if (tz === null || tz === "") { delete raw[ai].tz; }
            else { raw[ai].tz = String(tz); }
          }
          if (timeWindow !== undefined) {
            if (timeWindow === null) { delete raw[ai].timeWindow; }
            else {
              const s = parseInt(timeWindow.start), e = parseInt(timeWindow.end);
              if (isNaN(s) || isNaN(e) || s < 0 || s > 23 || e < 0 || e > 23) throw new Error("invalid timeWindow start/end (must be 0-23)");
              if (s === 0 && e === 0) { delete raw[ai].timeWindow; }
              else { raw[ai].timeWindow = { start: s, end: e }; }
            }
          }
          fs.writeFileSync(KEYS_FILE, JSON.stringify(raw, null, 2), "utf-8");
          loadAccounts();
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

  if (pathname === "/__boost-batch") {
    if (req.method === "POST") {
      const bodyChunks = [];
      req.on("data", c => bodyChunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(bodyChunks).toString('utf-8');
        try {
          const { mode, idxs } = JSON.parse(body);
          if (!mode) {
            _boostBatch = []; _boostBatchMode = ""; _boostBatchCursor = 0; _boostBatchPool = []; _boostBatchPoolIdx = 0;
          } else {
            if (mode !== "use" && mode !== "roundrobin" && mode !== "random") throw new Error("mode must be 'use', 'roundrobin', 'random', or empty");
            if (!Array.isArray(idxs) || !idxs.length) throw new Error("idxs required");
            const arr = [...new Set(idxs)].map(i => { const n = parseInt(i); if (isNaN(n) || n < 1) throw new Error("invalid idx"); return n - 1; }).sort((a, b) => a - b);
            _boostBatch = arr; _boostBatchMode = mode; _boostBatchCursor = 0; _boostBatchPool = []; _boostBatchPoolIdx = 0;
            _boostKey = -1; // mutually exclusive with single boost
            console.log(`[proxy] batch boost set: mode=${mode} idxs=[${arr.map(i=>i+1).join(",")}]`);
          }
          broadcastStatus();
          res.writeHead(200, cors);
          res.end(JSON.stringify({ ok: true, batch: _boostBatch.map(i => i + 1), mode: _boostBatchMode }));
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

  if (pathname === "/__restart/cancel") {
    if (req.method === "POST") {
      const result = cancelRestartDrain();
      res.writeHead(result.ok ? 200 : 409, { ...cors, "cache-control": "no-store" });
      res.end(JSON.stringify(result));
      return;
    }
    res.writeHead(405, cors);
    res.end(JSON.stringify({ error: "method not allowed" }));
    return;
  }

  if (pathname === "/__restart/force") {
    if (req.method === "POST") {
      const result = requestForcedRestart();
      if (!result.ok) {
        res.writeHead(409, { ...cors, "cache-control": "no-store" });
        res.end(JSON.stringify(result));
        return;
      }
      res.writeHead(202, { ...cors, "cache-control": "no-store" });
      exitForForcedRestartAfterResponse(res);
      res.end(JSON.stringify(result));
      return;
    }
    res.writeHead(405, cors);
    res.end(JSON.stringify({ error: "method not allowed" }));
    return;
  }

  if (pathname === "/__restart") {
    if (req.method === "POST") {
      const result = beginRestartDrain();
      res.writeHead(result.ok ? 202 : 409, { ...cors, "cache-control": "no-store" });
      res.end(JSON.stringify(result));
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

  // --- Protocol conversion routes ---
  if (pathname === "/v1/responses" && req.method === "POST") {
    const chunks = [];
    req.on("data", c => chunks.push(c));
    req.on("end", () => {
      try {
        const body = Buffer.concat(chunks);
        let reqBody;
        try { reqBody = JSON.parse(body); } catch (e) { res.writeHead(400, cors); res.end(JSON.stringify({ error: "invalid JSON" })); return; }
        const chatBody = responsesToChatRequest("", reqBody);
        chatBody.stream = true;
        addEventLog("conversion", 0, `Responses→Chat 转换: ${reqBody.model || "?"} → ${chatBody.model}`, "");
        const lifecycle = createResponsesLifecycle(chatBody.model || "");
        const transform = createChatToResponsesStream(lifecycle);
        lifecycle._transform = transform;
        transform._lifecycle = lifecycle;
        forwardWithPriority(req.method, req.headers, Buffer.from(JSON.stringify(chatBody)), res, "/v1/chat/completions", transform, groupName, { preExtract: taskInsightPreExtract(reqBody) });
      } catch (e) { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); }
    });
    req.on("error", e => { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); });
    return;
  }

  if (pathname === "/v1/messages" && req.method === "POST") {
    const chunks = [];
    req.on("data", c => chunks.push(c));
    req.on("end", () => {
      try {
        const body = Buffer.concat(chunks);
        let reqBody;
        try { reqBody = JSON.parse(body); } catch (e) { res.writeHead(400, cors); res.end(JSON.stringify({ error: "invalid JSON" })); return; }
        const chatBody = messagesToChatRequest("", reqBody);
        chatBody.stream = true;
        addEventLog("conversion", 0, `Messages→Chat 转换: ${reqBody.model || "?"}`, "");
        const lifecycle = createMessagesLifecycle(reqBody.model || chatBody.model || "");
        const transform = createChatToMessagesStream(lifecycle);
        forwardWithPriority(req.method, req.headers, Buffer.from(JSON.stringify(chatBody)), res, "/v1/chat/completions", transform, groupName, { preExtract: taskInsightPreExtract(reqBody) });
      } catch (e) { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); }
    });
    req.on("error", e => { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); });
    return;
  }

  if (pathname === "/v1/chat/completions" && req.method === "POST") {
    const hasAnthropic = accounts.some(a => (a.group || "A") === groupName && isMessagesNative(a.url));
    if (req.headers["x-task-insight-distill"] === "1" && config.taskInsight && config.taskInsight.distill && config.taskInsight.distill.engine === "proxy" && groupName === "A") {
      const chunks = [];
      req.on("data", c => chunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(chunks);
        const headers = { ...req.headers };
        delete headers["x-task-insight-distill"];
        forwardWithPriority(req.method, headers, body, res, pathname, null, groupName, { internalDistill: true });
      });
      req.on("error", e => { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); });
      return;
    }
    if (!hasAnthropic) {
      // No Anthropic upstreams — fall through to default handler
    } else {
      const chunks = [];
      req.on("data", c => chunks.push(c));
      req.on("end", () => {
        try {
          const body = Buffer.concat(chunks);
          let reqBody;
          try { reqBody = JSON.parse(body); } catch (e) { res.writeHead(400, cors); res.end(JSON.stringify({ error: "invalid JSON" })); return; }
          const origBody = Buffer.from(JSON.stringify({ ...reqBody, stream: true }));
          const msgsBody = chatToMessagesRequest("", reqBody);
          msgsBody.stream = true;
          const msgsHeaders = { ...req.headers, "anthropic-version": "2023-06-01" };
          addEventLog("conversion", 0, `Chat→Messages 转换: ${reqBody.model || "?"}`, "");
          forwardChatCompletions(req.method, req.headers, origBody, msgsHeaders, Buffer.from(JSON.stringify(msgsBody)), res, groupName);
        } catch (e) { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); }
      });
      req.on("error", e => { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); });
      return;
    }
  }
  // --- End protocol conversion routes ---

  const chunks = [];
  req.on("data", (c) => chunks.push(c));
  req.on("end", () => {
    const body = chunks.length ? Buffer.concat(chunks) : null;
    if (!req.headers["authorization"]) {
      res.writeHead(400, cors);
      res.end(JSON.stringify({ error: "Missing Authorization header" }));
      return;
    }
    console.log(`[proxy] ${req.method} ${pathname} (group ${groupName})`);
    let nativeResponsesProbe = null;
    if (pathname === "/responses" && req.method === "POST" && body) {
      try {
        const requestBody = JSON.parse(body.toString("utf8"));
        if (requestBody && requestBody.stream === true) {
          nativeResponsesProbe = createNativeResponsesTerminalProbe(requestBody.model || "");
        }
      } catch (e) {
        // Leave malformed or non-JSON requests on the existing passthrough path.
      }
    }
    forwardWithPriority(req.method, req.headers, body, res, pathname, nativeResponsesProbe, groupName);
  });
  req.on("error", (e) => { console.error(`[proxy] ${e.message}`); if (!res.destroyed) res.end(); });
});

  return server;
}

function startGroup(name, port) {
  if (servers[name]) { console.log(`[proxy] Group ${name} already running`); return Promise.resolve(servers[name]); }
  const srv = createGroupServer(name, port);
  servers[name] = srv;
  return new Promise((resolve, reject) => {
    srv.on("error", (e) => {
      console.error(`[proxy] Group ${name} error: ${e.message}`);
      delete servers[name];
      reject(e);
    });
    srv.listen(port, "localhost", () => {
      console.log(`[proxy] Group ${name} listening on http://localhost:${port}`);
      if (name === "A") {
        fs.writeFileSync(PID_FILE, String(process.pid));
        setupWebSocket(srv);
        const n = (() => { try { return JSON.parse(fs.readFileSync(KEYS_FILE, "utf-8")).length; } catch { return 0; } })();
        console.log(`
╔══════════════════════════════════════════════╗
║    Codex Multi-Key Proxy v3                 ║
║──────────────────────────────────────────────║
║  Listen:  http://localhost:${port} (Group A)          ║
║  Accounts: ${n}  │  Dashboard: /                   ║
║──────────────────────────────────────────────║
║  WebSocket push, sliding window (5m/1h),     ║
║  P50/P95/P99, path stats, cost estimate,     ║
║  request queue, webhook, Prometheus /metrics  ║
║  Multi-port key-group routing                ║
╚══════════════════════════════════════════════╝`);
      }
      resolve(srv);
    });
  });
}

function stopGroup(name) {
  if (!servers[name]) { console.log(`[proxy] Group ${name} not running`); return; }
  try { servers[name].close(); } catch {}
  delete servers[name];
  console.log(`[proxy] Group ${name} stopped`);
}

function initServers() {
  const groups = config.groups || { A: 3456 };
  loadAccounts();
  const promises = [];
  for (const [name, port] of Object.entries(groups)) {
    if (name === "A") {
      promises.push(startGroup(name, port || 3456).catch(e => {
        console.error(`[proxy] Fatal: Failed to start group A: ${e.message}`);
        process.exit(1);
      }));
    } else if (config.groupEnabled === undefined || config.groupEnabled[name] !== false) {
      promises.push(startGroup(name, port).catch(e => console.error(`[proxy] Failed to start group ${name}: ${e.message}`)));
    }
  }
  return Promise.allSettled(promises).then(() => {
    broadcastStatus();
  });
}

// Graceful shutdown
function shutdown(signal) {
  console.log(`[proxy] ${signal} received, shutting down...`);
  for (const srv of Object.values(servers)) {
    try {
      srv.close();
      srv.unref();
    } catch {}
  }
  const drainStart = Date.now();
  const maxDrainMs = 10000;
  const drainCheck = () => {
    const total = Object.values(activeRequests).reduce((s, a) => s + (Array.isArray(a) ? a.length : 0), 0);
    if (total === 0 || Date.now() - drainStart >= maxDrainMs) {
      try {
        const pid = parseInt(fs.readFileSync(PID_FILE, "utf8").trim(), 10);
        if (pid === process.pid) fs.unlinkSync(PID_FILE);
      } catch {}
      process.exit(0);
    } else {
      setTimeout(drainCheck, 500);
    }
  };
  setTimeout(drainCheck, 500).unref();
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

// Start servers
loadConfig();
refreshLocalBuildProvenance();
cleanOldLogs();
loadLogSummary();
loadLogIncidents();
initServers();
