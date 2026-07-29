const crypto = require("crypto");
const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");
const { Transform } = require("stream");
const { spawn, spawnSync } = require("child_process");
const { WebSocketServer } = require("ws");

const PORT = 3456;
const servers = {};
const KEYS_FILE = path.join(__dirname, "keys.json");
const STATE_FILE = path.join(__dirname, "state.json");
const CONFIG_FILE = path.join(__dirname, "config.json");
const RESUME_SCRIPT = path.join(__dirname, "resume-codex.sh");
const AUTO_RESUME_RUNTIME_FILE = path.join(__dirname, ".auto-resume-runtime.json");
const WATCHDOG_RELOAD_FILE = path.join(__dirname, ".watchdog-reload");
const TIMEOUT = 1500000;
const PRIORITY = { daily: 0, weekly: 1, never: 2, hourly: 0 };
const HTTP_MOD = { "http:": http, "https:": https };
const TZ = "Asia/Shanghai";
const MAX_LOG = 2000;
const QUEUE_TIMEOUT = 30000;
const RESTART_FORCE_MIN_WAIT_MS = 30000;
let STREAM_LIFETIME = 1800000;
const LOG_DIR = path.join(__dirname, "logs");
const PID_FILE = path.join(__dirname, "proxy.pid");
const UPSTREAM_REPOSITORY = "aipayim/codex-proxy";
const UPSTREAM_REPOSITORY_URL = "https://github.com/aipayim/codex-proxy";
const UPSTREAM_LATEST_RELEASE_API = "https://api.github.com/repos/aipayim/codex-proxy/releases/latest";
const BUILD_INFO_FILE = path.join(__dirname, "build-info.json");
const RELEASE_MANIFEST_FILE = path.join(__dirname, "release-manifest.json");
const BUILD_INFO_MAX_BYTES = 16 * 1024;
const RELEASE_MANIFEST_MAX_BYTES = 64 * 1024;
const RELEASE_INTEGRITY_FILE_MAX_BYTES = 8 * 1024 * 1024;
const AUTO_RESUME_RUNTIME_FILE_MAX_BYTES = 8 * 1024;
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
]);
const REQUIRED_RELEASE_INTEGRITY_FILES = new Set(["proxy.js", "package.json"]);
const UPDATE_CHECK_TTL_MS = 6 * 60 * 60 * 1000;
const UPDATE_CHECK_MIN_REFRESH_MS = 60 * 1000;
const UPDATE_REQUEST_TIMEOUT_MS = 8000;
const UPDATE_MAX_RESPONSE_BYTES = 64 * 1024;
const UPDATE_MAX_RELEASE_NOTES_CHARS = 24000;
let LOG_RETENTION_DAYS = 7;
let LOG_FILE_ENABLED = true;
let LOG_DETAIL = "full";
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

let accounts = [];
let state = { keys: [], activeKey: null, dailyLog: {} };
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
let config = { webhookUrl: "", prices: { inputPer1M: 0, outputPer1M: 0 }, bytesPerToken: 3, notifications: { sound: true, desktop: true }, roundRobin: false, rateLimit: true, maxRequestsPerMin: 10, maxTokensPerMin: 0, defaultResetHours: 5, updateBaselineTag: "" };
let wss = null;
const wsClients = new Set();
let lastBroadcast = "{}";
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
let autoResumeTimer = null;
let logWrittenCount = 0;
const _logCache = { key: "", result: null, time: 0, ttl: 2000 };
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

function inspectLocalBuildProvenance() {
  const releaseBuild = inspectReleaseArtifactBuild();
  if (releaseBuild.comparable) return releaseBuild;
  const gitBuild = inspectGitBuild();
  if (gitBuild.comparable) return gitBuild;
  return {
    comparable: false,
    source: "unknown",
    verification: "none",
    provenanceLabel: "本地来源未验证",
    reason: releaseBuild.reason || gitBuild.reason || "unknown",
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
  // Trim stale queue entries
  const qcut = Date.now() - 30000;
  requestQueue = requestQueue.filter(r => r.time > qcut && !r.clientRes.destroyed);
  cleanOldLogs();
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
    config.autoRecoverPoll = c.autoRecoverPoll === true;
    config.autoRecoverPollInterval = Math.max(1, parseInt(c.autoRecoverPollInterval) || 5);
    config.autoRecoverPollCodes = Array.isArray(c.autoRecoverPollCodes) ? c.autoRecoverPollCodes : [500,502,503,504];
    config.autoRecoverDelays = (Array.isArray(c.autoRecoverDelays) ? c.autoRecoverDelays : [800])
      .map(v => parseInt(v)).filter(v => !isNaN(v) && v >= 100 && v <= 10000).slice(0, 10);
    if (!config.autoRecoverDelays.length) config.autoRecoverDelays = [800];
    config.autoResume = c.autoResume === true;
    config.autoResumeIdleMinutes = Math.max(1, parseInt(c.autoResumeIdleMinutes) || 10);
    config.autoResumeDebounceMinutes = Math.max(1, parseInt(c.autoResumeDebounceMinutes) || 3);
    config.autoResumeProjects = Array.isArray(c.autoResumeProjects) ? c.autoResumeProjects.slice(0, 10) : [];
    config.cmdPath = c.cmdPath || "/mnt/c/Windows/System32/cmd.exe";
    config.weeklySortBy = c.weeklySortBy === "expiry" ? "expiry" : "priority";
    config.roundRobin = c.roundRobin === true;
    config.enableAutoLock = c.enableAutoLock !== false;
    config.lockAfterFailCount = Math.max(1, c.lockAfterFailCount || 3);
    config.lockFailCodes = Array.isArray(c.lockFailCodes) ? c.lockFailCodes : ["401","403"];
    LOG_RETENTION_DAYS = c.logRetentionDays != null ? c.logRetentionDays : 7;
    LOG_FILE_ENABLED = c.logFile !== false;
    LOG_DETAIL = c.logDetail === "basic" ? "basic" : "full";
    config.logFile = LOG_FILE_ENABLED;
    config.logRetentionDays = LOG_RETENTION_DAYS;
    config.logDetail = LOG_DETAIL;
    STREAM_LIFETIME = Math.min(7200000, Math.max(60000, parseInt(c.streamLifetime) || 1800000));
    config.streamLifetime = STREAM_LIFETIME;
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
    state = { keys: [], activeKey: null, dailyLog: {} };
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

function updateAutoResumeProjectState(id, patch, forceSave) {
  if (!autoResumeStateReady) return;
  const runtime = getAutoResumeRuntimeState();
  runtime.projects[id] = { ...(runtime.projects[id] || {}), ...patch };
  saveState(forceSave === true);
}

function readAutoResumePid(pidFile) {
  try {
    const raw = fs.readFileSync(pidFile, "utf8").trim();
    const match = /^(pgid:)?(\d+)$/.exec(raw);
    if (!match) return null;
    const pid = parseInt(match[2], 10);
    if (!Number.isSafeInteger(pid) || pid <= 1 || pid === process.pid) return null;
    return { pid, processGroup: Boolean(match[1]) };
  } catch {
    return null;
  }
}

function stopAutoResumeProcess(pidInfo) {
  if (!pidInfo) return false;
  if (pidInfo.processGroup) {
    try {
      process.kill(-pidInfo.pid, "SIGTERM");
      return true;
    } catch { /* fall through to the leader process */ }
  }
  try {
    process.kill(pidInfo.pid, "SIGTERM");
    return true;
  } catch {
    return false;
  }
}

function readAutoResumeRunnerStatus(statusFile) {
  try {
    const fields = fs.readFileSync(statusFile, "utf8").trim().split("\t");
    const [phase, rawPid, rawUpdatedAt, rawExitCode] = fields;
    const allowed = new Set(["starting", "running", "exited", "failed", "terminated", "cd_failed", "launcher_failed"]);
    if (!allowed.has(phase)) return null;
    const pid = parseInt(rawPid, 10);
    const updatedAt = normalizeAutoResumeTimestamp(rawUpdatedAt);
    const exitCode = rawExitCode === "" || rawExitCode === undefined ? null : parseInt(rawExitCode, 10);
    if (!updatedAt || (rawPid && (!Number.isSafeInteger(pid) || pid < 0))) return null;
    return { phase, pid: Number.isSafeInteger(pid) ? pid : 0, updatedAt, exitCode: Number.isSafeInteger(exitCode) ? exitCode : null };
  } catch {
    return null;
  }
}

function refreshAutoResumeProjectStatus(proj, index) {
  const { id, statusFile } = autoResumeProjectFiles(proj, index);
  const status = readAutoResumeRunnerStatus(statusFile);
  if (!status || !autoResumeStateReady) return;
  const runtime = getAutoResumeRuntimeState();
  const previous = runtime.projects[id] || {};
  if (previous.runnerUpdatedAt === status.updatedAt && previous.phase === status.phase) return;

  updateAutoResumeProjectState(id, {
    phase: status.phase,
    runnerPid: status.pid || null,
    runnerUpdatedAt: status.updatedAt,
    exitCode: status.exitCode,
  }, true);

  const label = autoResumeProjectLabel(proj, index);
  if (status.phase === "running") addEventLog("auto_resume_running", 0, `闲置恢复：${label} 已开始执行`, "");
  else if (status.phase === "exited") addEventLog("auto_resume_exited", 0, `闲置恢复：${label} 已退出（代码 ${status.exitCode ?? 0}）`, "");
  else if (status.phase === "terminated") addEventLog("auto_resume_terminated", 0, `闲置恢复：${label} 已被下一次恢复终止`, "");
  else if (status.phase === "cd_failed") addEventLog("auto_resume_command_failed", 0, `闲置恢复：${label} 无法进入项目目录`, "");
  else if (status.phase === "failed") addEventLog("auto_resume_command_failed", 0, `闲置恢复：${label} 命令退出失败（代码 ${status.exitCode ?? "未知"}）`, "");
  else if (status.phase === "launcher_failed") addEventLog("auto_resume_launcher_failed", 0, `闲置恢复：${label} 启动器退出失败`, "");
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
  }
  initializeAutoResumeKeyUseTime();
  const idleMinutes = (now - lastKeyUseTime) / 60000;
  if (idleMinutes < config.autoResumeIdleMinutes) return;
  const sinceLastResume = (now - lastResumeTime) / 60000;
  if (sinceLastResume < config.autoResumeDebounceMinutes) return;

  lastResumeTime = now;
  const runtime = getAutoResumeRuntimeState();
  runtime.lastResumeTime = lastResumeTime;
  runtime.lastTriggerIdleMinutes = Math.round(idleMinutes);
  persistAutoResumeRuntime();
  saveState(true);
  broadcastStatus();
  console.log("[proxy] autoResume triggered after " + Math.round(idleMinutes) + "min without key use");
  addEventLog("auto_resume_triggered", 0, `闲置恢复触发：Key 已 ${Math.round(idleMinutes)} 分钟未使用`, "");
  for (let index = 0; index < config.autoResumeProjects.length; index++) {
    const proj = config.autoResumeProjects[index];
    if (proj.path && proj.cmd) triggerResume(proj, index);
  }
}

function triggerResume(proj, index) {
  const label = autoResumeProjectLabel(proj, index);
  const files = autoResumeProjectFiles(proj, index);
  const normalizedPath = normalizePath(proj.path);
  if (autoResumeLaunches.has(files.id)) return;

  try {
    if (!fs.statSync(normalizedPath).isDirectory()) throw new Error("project directory is unavailable");
    if (!fs.existsSync(RESUME_SCRIPT)) throw new Error("resume launcher is unavailable");

    const oldPid = readAutoResumePid(files.pidFile);
    if (oldPid) {
      const stopped = stopAutoResumeProcess(oldPid);
      addEventLog("auto_resume_replacing", 0, `闲置恢复：${label}${stopped ? " 正在停止上一次命令" : " 发现上一次命令已结束"}`, "");
    }
    try { fs.unlinkSync(files.statusFile); } catch { /* stale status is optional */ }

    const startedAt = Date.now();
    updateAutoResumeProjectState(files.id, { phase: "launching", launchRequestedAt: startedAt, launcherExitCode: null }, true);
    addEventLog("auto_resume_launching", 0, `闲置恢复：正在启动 ${label}`, "");
    const child = spawn("/bin/bash", [
      RESUME_SCRIPT,
      normalizedPath,
      String(proj.cmd),
      files.pidFile,
      files.statusFile,
      `Codex Resume - ${files.id}`,
      config.cmdPath || "/mnt/c/Windows/System32/cmd.exe",
    ], { detached: true, stdio: "ignore" });
    autoResumeLaunches.set(files.id, child);
    let settled = false;
    const finish = (phase, extra, eventType, message) => {
      if (settled) return;
      settled = true;
      autoResumeLaunches.delete(files.id);
      updateAutoResumeProjectState(files.id, { phase, launcherUpdatedAt: Date.now(), ...extra }, true);
      addEventLog(eventType, 0, message, "");
      broadcastStatus();
    };
    child.once("error", error => {
      finish("launcher_failed", { launcherError: String(error.message || error).slice(0, 160) }, "auto_resume_launcher_failed", `闲置恢复：${label} 启动器失败`);
    });
    child.once("close", (code, signal) => {
      if (code === 0) {
        finish("launcher_accepted", { launcherExitCode: 0, launcherSignal: null }, "auto_resume_launcher_accepted", `闲置恢复：${label} 启动器已接受`);
      } else {
        finish("launcher_failed", { launcherExitCode: Number.isInteger(code) ? code : null, launcherSignal: signal || null }, "auto_resume_launcher_failed", `闲置恢复：${label} 启动器退出失败`);
      }
    });
    child.unref();
    console.log(`[proxy] autoResume launch requested: ${label}`);
  } catch (error) {
    updateAutoResumeProjectState(files.id, { phase: "launcher_failed", launcherUpdatedAt: Date.now(), launcherError: String(error.message || error).slice(0, 160) }, true);
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
      if (ks.failCode && codes.includes(ks.failCode)) { hasMatch = true; break; }
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
  if (LOG_FILE_ENABLED) writeLogEntry(entry);
  broadcastLog(entry);
}

function addEventLog(eventType, idx, message, url) {
  const entry = { time: Date.now(), type: "event", eventType, idx, message, url: url || "" };
  requestLog.push(entry);
  if (requestLog.length > MAX_LOG) requestLog.splice(0, requestLog.length - MAX_LOG);
  if (LOG_FILE_ENABLED) writeLogEntry(entry);
  broadcastLog(entry);
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
  };
  requestLog.push(entry);
  if (requestLog.length > MAX_LOG) requestLog.splice(0, requestLog.length - MAX_LOG);
  if (LOG_FILE_ENABLED) writeLogEntry(entry);
  broadcastLog(entry);
}

function isRequestLogFailure(entry) {
  return entry.status === 0 || entry.status >= 400 || entry.streamOutcome === "failed";
}

function requestLogDedupeKey(entry) {
  return [
    entry.time || 0,
    entry.type || "request",
    entry.eventType || "",
    entry.idx || 0,
    entry.path || "",
    entry.status ?? "",
    entry.streamId || "",
    entry.streamOutcome || "",
    entry.streamReason || "",
    entry.duration || 0,
    entry.message || "",
  ].join("|");
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
    logWrittenCount++;
  } catch(e) { /* fail silently */ }
}

function cleanOldLogs() {
  try {
    if (!fs.existsSync(LOG_DIR)) { logWrittenCount = 0; return; }
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
    // Initialize logWrittenCount by counting remaining files
    logWrittenCount = 0;
    const remaining = fs.readdirSync(LOG_DIR).filter(f => f.endsWith(".jsonl"));
    for (const f of remaining) {
      const content = fs.readFileSync(path.join(LOG_DIR, f), "utf-8");
      logWrittenCount += content.split("\n").filter(l => l.trim()).length;
    }
  } catch(e) { /* fail silently */ }
}

function countAllEntries(since, until) {
  if (since || until) {
    let total = 0;
    try {
      if (!fs.existsSync(LOG_DIR)) return requestLog.length;
      const files = fs.readdirSync(LOG_DIR).filter(f => f.endsWith(".jsonl"));
      const sTime = since ? parseInt(since, 10) : 0;
      const uTime = until ? parseInt(until, 10) : 9e15;
      for (const f of files) {
        const dateStr = f.replace(".jsonl", "");
        const fd = new Date(dateStr + "T00:00:00");
        const fdEnd = fd.getTime() + 86400000;
        if (fdEnd < sTime || fd.getTime() > uTime) continue;
        const filePath = path.join(LOG_DIR, f);
        const content = fs.readFileSync(filePath, "utf-8");
        const lines = content.split("\n").filter(l => l.trim());
        total += lines.length;
      }
    } catch (e) { /* fail silently */ }
    return total + requestLog.length;
  }
  return logWrittenCount + requestLog.length;
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
function estimateCost(inputBytes, outputBytes) {
  const bpt = config.bytesPerToken || 3;
  const inTokens = inputBytes / bpt;
  const outTokens = outputBytes / bpt;
  const cost = (inTokens / 1000000) * (config.prices.inputPer1M || 0) + (outTokens / 1000000) * (config.prices.outputPer1M || 0);
  return cost;
}

// --- State ---
function loadState() {
  autoResumeStateReady = false;
  try {
    state = JSON.parse(fs.readFileSync(STATE_FILE, "utf-8"));
  } catch {
    state = { keys: [], activeKey: null, dailyLog: {} };
  }
  if (!state || typeof state !== "object" || Array.isArray(state)) {
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
  restoreAutoResumeRuntimeState();
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
  while (state.keys.length <= idx) state.keys.push({ failCode: null, failTime: null, failPeriod: null, failCount: 0, status: "active", stats: null, lastStatus: null, lastTime: null, lastModel: null });
  const ks = state.keys[idx];
  if (ks.status === undefined) ks.status = "active";
  if (ks.failPeriod === undefined) ks.failPeriod = null;
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
  if (!ks.rateWindow) ks.rateWindow = { requests: [], windowStart: Date.now() };
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

function recordRequest(idx, success, inputBytes, outputBytes, duration, ttfb, model, statusCode) {
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
  if (!s.hourly[hk]) s.hourly[hk] = { requests: 0, inputBytes: 0, outputBytes: 0, models: {} };
  s.hourly[hk].requests++;
  if (inputBytes) s.hourly[hk].inputBytes += inputBytes;
  if (outputBytes) s.hourly[hk].outputBytes += outputBytes;
  const _mk = model || "(未知)";
  if (!s.hourly[hk].models[_mk]) s.hourly[hk].models[_mk] = { requests: 0, inputBytes: 0, outputBytes: 0 };
  s.hourly[hk].models[_mk].requests++;
  if (inputBytes) s.hourly[hk].models[_mk].inputBytes += inputBytes;
  if (outputBytes) s.hourly[hk].models[_mk].outputBytes += outputBytes;
  if (statusCode !== undefined) {
    if (!s.hourly[hk].statusCodes) s.hourly[hk].statusCodes = {};
    const scKey = statusCode === 200 ? "ok" : (statusCode >= 400 && statusCode < 500 ? "4xx" : (statusCode >= 500 ? "5xx" : "fail"));
    s.hourly[hk].statusCodes[scKey] = (s.hourly[hk].statusCodes[scKey] || 0) + 1;
    if (!s.daily[d].statusCodes) s.daily[d].statusCodes = {};
    s.daily[d].statusCodes[scKey] = (s.daily[d].statusCodes[scKey] || 0) + 1;
  }

  state.activeKey = idx;
  recordSliding(idx, success, duration);
  const rw = ks.rateWindow;
  if (rw) {
    const bpt = config.bytesPerToken || 3;
    const tokens = ((inputBytes || 0) + (outputBytes || 0)) / bpt;
    rw.requests.push({ time: Date.now(), tokens: Math.round(tokens) });
  }
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
  const rw = ks.rateWindow || { requests: [], windowStart: Date.now() };
  const now = Date.now();
  if (now - rw.windowStart > 60000) {
    rw.requests = [];
    rw.windowStart = now;
  }
  const recent = rw.requests.filter(e => now - e.time <= 60000);
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
function enqueueRequest(method, headers, body, clientRes, pathname, group, extraTransform) {
  if (restartState.phase !== "ready") {
    if (!clientRes.destroyed && !clientRes.headersSent) {
      clientRes.writeHead(503, { "content-type": "application/json", "retry-after": "5" });
      clientRes.end(JSON.stringify({ error: "proxy is restarting" }));
    }
    return;
  }
  group = group || "A";
  requestQueue.push({ method, headers, body, clientRes, pathname, group, time: Date.now(), extraTransform });
  console.log(`[proxy] Queue depth: ${requestQueue.length}`);
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
    if (now - r.time > QUEUE_TIMEOUT) {
      if (!r.clientRes.destroyed && !r.clientRes.headersSent) {
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
      const st = user ? `✗ ${user}` : (disc ? `✗ ${disc}` : (inCooldown(i) ? `✗ 冷却中 (${ks.failCode})` : "✓ 可用"));
      const s = ks.stats;
      const t = s ? ` | ${s.totalRequests}次请求 ${fmtBytes(s.inputBytes+s.outputBytes)}` : "";
      console.log(`       ${i+1}. ${m ? m[1] : a.key.slice(0,12)}... → ${a.url} [${resetLabels[a.reset] || a.reset}]${tag} ${st}${t}`);
    }
  }
  broadcastStatus();
}

function makeUsageTransform(idx, inputBytes, reqStart, ttfb, model) {
  let outputBytes = 0;
  const tr = new Transform({
    transform(chunk, encoding, cb) {
      outputBytes += chunk.length;
      tr.accBytes = outputBytes;
      this.push(chunk);
      cb();
    },
    flush(cb) {
      cb();
    }
  });
  tr.accBytes = 0;
  return tr;
}

function activeDecr(idx) {
  if (Array.isArray(activeRequests[idx])) {
    activeRequests[idx].shift();
    if (activeRequests[idx].length === 0) delete activeRequests[idx];
  }
}

function forwardRequest(idx, method, headers, body, clientRes, pathname, onDone, extraTransform, cleanModel) {
  const lifecycle = extraTransform?._lifecycle || null;
  let streamAttached = false;
  let clientCancelled = false;
  let terminateAttachedStream = null;
  if (!Array.isArray(activeRequests[idx])) activeRequests[idx] = [];
  let reqModel = null;
  try { reqModel = JSON.parse(body.toString()).model || null; } catch(e) {}
  const acct = accounts[idx];
  const resolvedModel = acct.model || cleanModel || reqModel || null;
  activeRequests[idx].push({ start: Date.now(), model: resolvedModel || "?" });
  const reqStart = Date.now();
  let ttfb = null;

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
      const dur = Date.now() - reqStart;
      activeDecr(idx);
      markFailure(idx, apiRes.statusCode);
      recordRequest(idx, false, 0, 0, dur, null, resolvedModel, apiRes.statusCode);
      recordPath(pathname, method, 0, 0, dur);
      Object.assign(logEntry, { status: apiRes.statusCode, inputBytes: body ? body.length : 0, outputBytes: 0, duration: dur, ttfb: null });
      addLog(logEntry);
      const _ks1=getKeyState(idx);_ks1.lastStatus=apiRes.statusCode;_ks1.lastTime=Date.now();_ks1.lastModel=logEntry.overrideModel||logEntry.reqModel||null;
      apiRes.destroy();
      onDone({ switched: true, code: apiRes.statusCode });
      return;
    }

    markSuccess(idx);

    const a = accounts[idx];

    const safeHeaders = { ...apiRes.headers };
    delete safeHeaders["transfer-encoding"];
    safeHeaders["x-proxy-account"] = `${idx + 1}/${accounts.length}`;

    if (clientRes.headersSent) { apiRes.destroy(); activeDecr(idx); onDone({ switched: false }); return; }
    clientRes.writeHead(apiRes.statusCode, safeHeaders);
    streamAttached = true;

    const inputBytes = body ? body.length : 0;
    const transform = makeUsageTransform(idx, inputBytes, reqStart, ttfb, resolvedModel);
    let cleaned = false;
    let endedNormally = false;
    let streamTimer = null;
    const cleanup = () => {
      if (cleaned) return;
      cleaned = true;
      if (streamTimer) { clearTimeout(streamTimer); streamTimer = null; }
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
        });
      }
      addLog(logEntry);
      const _ks2=getKeyState(idx);_ks2.lastStatus=apiRes.statusCode;_ks2.lastTime=Date.now();_ks2.lastModel=logEntry.overrideModel||logEntry.reqModel||null;
      if (!lifecycle) {
        recordRequest(idx, endedNormally, inputBytes, accBytes, dur, ttfb, resolvedModel, apiRes.statusCode);
      }
      broadcastStatus();
    };

    if (lifecycle) {
      lifecycle._metricsCallback = (success) => {
        const dur = Date.now() - reqStart;
        const accBytes = transform.accBytes || 0;
        recordRequest(idx, success, inputBytes, accBytes, dur, ttfb, resolvedModel, success ? (apiRes.statusCode || 200) : 0);
        if (!success && !clientCancelled) markFailure(idx, 0);
      };
      lifecycle._onTerminal = completedLifecycle => {
        addStreamTerminalLog(idx, completedLifecycle);
        cleanup();
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
    apiRes.on("error", (err) => {
      console.error(`[proxy] #${idx+1} Stream error: ${err.message}`);
      if (!clientCancelled) {
        if (streamAttached && lifecycle && !lifecycle.terminalKind) {
          lifecycle.emitFailed("upstream_error");
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
    streamTimer = setTimeout(() => {
      if (cleaned || clientCancelled || (lifecycle && lifecycle.terminalKind)) return;
      console.error(`[proxy] #${idx+1} Stream lifetime timeout (${STREAM_LIFETIME/60000}min)`);
      if (terminateAttachedStream && terminateAttachedStream("stream_lifetime_timeout")) return;
      if (!apiRes.destroyed) apiRes.destroy();
    }, STREAM_LIFETIME);
    if (extraTransform) {
      apiRes.pipe(transform).pipe(extraTransform).pipe(clientRes);
    } else {
      apiRes.pipe(transform).pipe(clientRes);
    }
    onDone({ switched: false });
  });
  proxyReq.setTimeout(TIMEOUT);
  // The request is now flushed with the selected account's Authorization key.
  proxyReq.once("finish", () => recordKeyUse(idx));

  proxyReq.on("error", (err) => {
    if (streamAttached && lifecycle && !clientCancelled && !lifecycle.terminalKind) {
      console.error(`[proxy] #${idx + 1} Stream request error: ${err.message}`);
      if (terminateAttachedStream) terminateAttachedStream("upstream_error");
      return;
    }
    if (onDone.done) return;
    onDone.done = true;
    const dur = Date.now() - reqStart;
    activeDecr(idx);
    console.error(`[proxy] #${idx + 1} Error: ${err.message}`);
    markFailure(idx, 0);
    recordRequest(idx, false, 0, 0, dur, null, resolvedModel, 0);
    Object.assign(logEntry, { status: 0, inputBytes: body ? body.length : 0, outputBytes: 0, duration: dur, ttfb: null });
    addLog(logEntry);
    const _ks3=getKeyState(idx);_ks3.lastStatus=0;_ks3.lastTime=Date.now();_ks3.lastModel=logEntry.overrideModel||logEntry.reqModel||null;
    onDone({ switched: true, error: err });
  });

  proxyReq.on("timeout", () => {
    if (streamAttached && lifecycle && !clientCancelled && !lifecycle.terminalKind) {
      console.error(`[proxy] #${idx+1} Upstream stream idle timeout`);
      if (terminateAttachedStream) terminateAttachedStream("upstream_idle_timeout");
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
      recordRequest(idx, false, 0, 0, dur, null, resolvedModel, 0);
    }
    recordPath(pathname, method, 0, 0, dur);
    Object.assign(logEntry, { status: 0, inputBytes: body ? body.length : 0, outputBytes: 0, duration: dur, ttfb: null });
    addLog(logEntry);
    const _ks4=getKeyState(idx);_ks4.lastStatus=0;_ks4.lastTime=Date.now();_ks4.lastModel=logEntry.overrideModel||logEntry.reqModel||null;
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

function forwardWithPriority(method, headers, body, clientRes, pathname, extraTransform, group) {
  group = group || "A";
  let responded = false;
  const usedKeys = new Set();
  const activeCount = accounts.filter(a => a.status === "active" && (a.group || "A") === group).length;
  let retries = 0;
  const MAX_RETRIES = Math.max(activeCount * 2, 10);
  let model = null;
  let forceIdx = -1;
  try {
    const parsed = JSON.parse(body.toString());
    model = parsed.model || null;
    if (model) {
      const hashMatch = model.match(/#(\d+)$/);
      if (hashMatch) {
        forceIdx = parseInt(hashMatch[1], 10) - 1;
        model = model.slice(0, -hashMatch[0].length) || null;
      }
    }
  } catch(e) {}
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
        if (r.switched && !clientRes.destroyed && !clientRes.headersSent) {
          clientRes.writeHead(502, { "content-type": "application/json" });
          clientRes.end(JSON.stringify({ error: `Key #${forceIdx+1} failed: ${r.code || r.error?.message || "error"}` }));
        }
        responded = true;
      }, extraTransform, model);
      return;
    }
    if (retries >= MAX_RETRIES) {
      console.error(`[proxy] Max retries (${MAX_RETRIES}) reached, queueing`);
      enqueueRequest(method, headers, body, clientRes, pathname, group, extraTransform);
      responded = true;
      return;
    }
    retries++;
    const idx = pickKey(model, group);
    if (idx < 0 || (usedKeys.has(idx) && inCooldown(idx))) {
      if (idx < 0) {
        console.log(`[proxy] No available keys, queueing request`);
        enqueueRequest(method, headers, body, clientRes, pathname, group, extraTransform);
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
    }, extraTransform);
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
    const data = buildStatusData();
    const msg = JSON.stringify({ type: "status", data, boostedIdx: _boostKey >= 0 ? _boostKey + 1 : -1, boostedBatch: _boostBatch.map(i => i + 1), boostedBatchMode: _boostBatchMode, lastRequestTime, lastKeyUseTime, lastResumeTime });
    ws.send(msg);
    ws.on("close", () => wsClients.delete(ws));
    ws.on("error", () => wsClients.delete(ws));
  });
}

function broadcastStatus() {
  const data = buildStatusData();
  const msg = JSON.stringify({ type: "status", data, boostedIdx: _boostKey >= 0 ? _boostKey + 1 : -1, boostedBatch: _boostBatch.map(i => i + 1), boostedBatchMode: _boostBatchMode, lastRequestTime, lastKeyUseTime, lastResumeTime });
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
      resetHours: a.resetHours || null,
    group: (a.group || "A").toUpperCase(),
      activatedAt: ks.activatedAt || null,
      available: !inCooldown(i),
      status: ks.status || "active",
      failCode: ks.failCode,
      failTime: ks.failTime,
      failPeriod: ks.failPeriod,
      failCount: ks.failCount,
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
      daily: s.daily || {},
      hourly: s.hourly || {},
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
<title>OpenAPI 多 Key 代理监控</title>
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
.btn:disabled{cursor:wait;opacity:.65}
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
<h1>OpenAPI 多 Key 代理监控</h1>
<div style="display:flex;gap:6px;flex-wrap:wrap">
<button class="btn" onclick="openLogs()">📋 日志</button>
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
<div class="trend-title"><span id="trendModeLabel" style="cursor:pointer;user-select:none" onclick="toggleTrendMode()">📊 模型趋势</span><span id="trendRangeLabel" style="font-size:10px;color:#64748b">24h</span></div>
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
  <button class="btn" style="font-size:11px;color:#4ade80" onclick="batchSetTimeWindow()">⏰ 设置时段</button>
  <button class="btn" style="font-size:11px;color:#fb923c" onclick="batchClearTimeWindow()">⏰ 清除时段</button>
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
  <div><input id="cfgAutoResumeDebounce" type="number" min="1" max="999" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;width:60px" value="3"> 两次触发最小间隔</div>
  <div style="color:#94a3b8;padding:4px 0">cmd.exe 路径</div>
  <div><input id="cfgCmdPath" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px;width:100%" value="/mnt/c/Windows/System32/cmd.exe"></div>
  <div style="color:#94a3b8;padding:4px 0;grid-column:1/-1;margin-bottom:4px">项目列表（最多 10 个）<button class="btn" style="font-size:10px;margin-left:6px" onclick="addResumeProject()">+ 添加项目</button></div>
  <div id="cfgResumeProjects" style="grid-column:1/-1"></div>
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
  <div style="color:#94a3b8;padding:4px 0;border-bottom:1px solid #334155;margin-bottom:4px;grid-column:1/-1">⏱ 分钟级限速</div>
  <div style="color:#94a3b8;padding:4px 0">每分钟请求上限</div>
  <div><input id="cfgMaxReqPerMin" type="number" min="1" max="1000" style="width:80px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" value="10"></div>
  <div style="color:#94a3b8;padding:4px 0">每分钟 Token 上限 (0=不限)</div>
  <div><input id="cfgMaxTokPerMin" type="number" min="0" max="9999999" style="width:120px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" value="0"></div>
  <div style="color:#94a3b8;padding:4px 0;border-bottom:1px solid #334155;margin-bottom:4px;grid-column:1/-1">⏱ 流超时</div>
  <div style="color:#94a3b8;padding:4px 0">响应流最大时长 (ms)</div>
  <div><input id="cfgStreamLifetime" type="number" min="60000" max="7200000" style="width:120px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" value="1800000" title="响应流绝对超时，防止僵尸连接。默认30分钟"></div>
  <div style="color:#94a3b8;padding:4px 0;border-bottom:1px solid #334155;margin-bottom:4px;grid-column:1/-1">🔐 管理认证</div>
  <div style="color:#94a3b8;padding:4px 0">管理 Token（空=不校验）</div>
  <div><input id="cfgAdminToken" style="width:200px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px 6px;border-radius:4px" value="" placeholder="留空=无认证" title="设置后所有管理接口需 Bearer token 认证"></div>
  <div style="color:#94a3b8;padding:4px 0;border-bottom:1px solid #334155;margin-bottom:4px;grid-column:1/-1">🔌 端口分组管理</div>
  <div style="grid-column:1/-1" id="portGroupsArea"></div>
</div>
<div style="font-size:11px;color:#64748b;margin-bottom:4px" id="cfgAutoCountdown">⏳ 下次检测（间隔）: --</div>
<div style="font-size:11px;color:#64748b;margin-bottom:4px" id="cfgAutoDailyCountdown">⏳ 下次检测（固定）: --</div>
<div style="font-size:11px;color:#64748b;margin-bottom:4px" id="cfgAutoPollCountdown">⏳ 下次检测（快速）: --</div>
<div style="font-size:11px;color:#22c55e;margin-bottom:8px" id="cfgAutoResumeStatus">🧬 闲置恢复: --</div>
<div class="mfoot"><button class="btn" id="restartProxyBtn" onclick="restartProxy()" style="color:#f87171">🔄 重启代理</button><div style="flex:1"></div><button class="btn btn-p" onclick="saveConfig()">保存</button></div>
</div></div>

<div class="modal" id="updateModal">
<div class="mcontent" style="max-width:720px">
<div class="mtitle"><span>版本更新</span><button class="btn" type="button" onclick="closeUpdateModal()">✕</button></div>
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
<div class="mtitle"><span>实时请求日志</span><button class="btn" onclick="closeLogs()">✕</button></div>
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
<div class="log-filters" style="display:flex;gap:6px;margin-bottom:6px;flex-wrap:wrap;align-items:center">
<input id="logKeyFilter" placeholder="Key #" style="width:50px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
<input id="logStatusFilter" placeholder="状态码" style="width:60px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
<input id="logModelFilter" placeholder="模型" style="width:100px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">
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
  <button class="btn" id="logPrevBtn" onclick="logPage(-1)" style="font-size:11px;padding:2px 10px" disabled>← 上一页</button>
  <span id="logPageInfo" style="color:#94a3b8">第 1 / 1 页</span>
  <button class="btn" id="logNextBtn" onclick="logPage(1)" style="font-size:11px;padding:2px 10px" disabled>下一页 →</button>
  <span style="color:#64748b;font-size:10px;margin-left:8px" id="logRealTimeBadge">● 实时</span>
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
  const badge=document.getElementById("updateBadge");
  const hasUpdate=!!(updateInfo&&updateInfo.updateAvailable&&latest);
  if(badge){
    badge.classList.toggle("on",hasUpdate);
    badge.title=hasUpdate?"发现 "+latest.tag+"，点击查看 Release 说明与安全升级方法":(currentComparable?"当前没有可升级的正式 Release":"本地来源未验证；仅展示 GitHub Release 信息");
  }
  const status=document.getElementById("cfgUpdateStatus");
  if(status){
    if(!updateInfo)status.textContent="正在检查 GitHub Release…";
    else if(updateInfo.lastError&&!latest)status.textContent="检查失败："+updateInfo.lastError;
    else if(latest&&!currentComparable)status.textContent="本地来源未验证；仅展示 Release（检查于 "+formatUpdateCheckTime(updateInfo.checkedAt)+"）";
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
  if(!updateInfo){
    summary.textContent="正在读取 GitHub Release 信息…";
    notes.textContent="正在读取更新说明…";
    safety.textContent="自动覆盖升级不会在后台执行。";
    return;
  }
  const currentInfo=updateInfo.current||{};
  const current=currentInfo.label||UNKNOWN_LOCAL_BUILD_LABEL;
  const currentComparable=currentInfo.comparable===true;
  const provenanceLabel=currentInfo.provenanceLabel||"本地来源未验证";
  const latest=updateInfo.latest;
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
  safety.textContent="官方发布包会通过随包构建元数据和文件清单自动识别；官方 Git 工作树只有在干净且正好位于正式 Tag 时才自动识别。当前构建可能包含本地修改，因此自动覆盖式一键升级仍禁用。定制构建如需比较，可在系统配置填写已人工确认的正式 Release Tag；否则只查看 Release。升级前备份当前代理目录及配置/状态，审核变更、执行 node -c proxy.js 后，再于维护窗口重启代理。";
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
  ws.onmessage=function(e){
    try{
      const msg=JSON.parse(e.data);
      if(msg.type==="status"){data=msg.data;boostedIdx=msg.boostedIdx||-1;boostedBatch=msg.boostedBatch||[];boostedBatchMode=msg.boostedBatchMode||"";if(msg.lastRequestTime)lastRequestTime=msg.lastRequestTime;if(msg.lastKeyUseTime)lastKeyUseTime=msg.lastKeyUseTime;if(msg.lastResumeTime)lastResumeTime=msg.lastResumeTime;render()}
      if(msg.type==="notification"&&msg.notificationType==="all_keys_failed"){showAlert("所有 Key 均不可用！");playAlert();sendDesktop()}
      if(msg.type==="log"&&document.getElementById("logModal").classList.contains("on")){
        logAllEntries.push(msg.data);
        _logSparkVersion++;
        if(logAllEntries.length>LOG_PAGE_SIZE*3)logAllEntries.splice(0,logAllEntries.length-LOG_PAGE_SIZE*3);
        if(logLastStats)logLastStats.total=(logLastStats.total||0)+1;
        logTotalPages=Math.max(1,Math.ceil((logLastStats?.totalAll||logLastStats?.total||logAllEntries.length)/LOG_PAGE_SIZE));
        if(logCurrentPage===1){
          const tbody=document.getElementById("logBody");
          if(tbody){
            const tr=document.createElement("tr");
            tr.className=logRowClass(msg.data);
            tr.onclick=function(){toggleLogDetail(this,logAllEntries.length-1)};
            tr.innerHTML=makeLogRow(msg.data, logAllEntries.length);
            tbody.insertBefore(tr,tbody.firstChild);
            const expandRow=document.createElement("tr");
            expandRow.className="log-row-expand";
            expandRow.id="logDetail_"+(logAllEntries.length-1);
            expandRow.innerHTML='<td colspan="12"><span id="logDetailContent_'+(logAllEntries.length-1)+'"></span></td>';
            tbody.insertBefore(expandRow,tr.nextSibling);
            if(tbody.children.length>LOG_PAGE_SIZE*2+2)while(tbody.children.length>LOG_PAGE_SIZE*2+2)tbody.removeChild(tbody.lastChild);
          }
        }
        // Debounce chart re-renders
        if(_logRenderTimer)clearTimeout(_logRenderTimer);
        _logRenderTimer=setTimeout(function(){
          _logRenderTimer=null;
          renderLogSparkline();
          renderLogModelDist();
          renderLogErrorCluster();
        },500);
      }
    }catch(e){}
  };
  ws.onclose=function(e){
    wsFailed=true;
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
    document.getElementById("cfgAutoRecoverPoll").checked=c.autoRecoverPoll===true;
    document.getElementById("cfgAutoRecoverPollInterval").value=c.autoRecoverPollInterval||5;
    document.getElementById("cfgAutoRecoverPollCodes").value=(c.autoRecoverPollCodes||[500,502,503,504]).join(",");
    document.getElementById("cfgAutoRecoverDelays").value=(Array.isArray(c.autoRecoverDelays)?c.autoRecoverDelays:[800]).join(",");
    document.getElementById("cfgRoundRobin").checked=c.roundRobin===true;
    document.getElementById("cfgWeeklySortBy").checked=c.weeklySortBy==="expiry";
    document.getElementById("cfgLockCount").value=c.lockAfterFailCount||3;
    document.getElementById("cfgLockCodes").value=(c.lockFailCodes||["401","403"]).join(",");
    document.getElementById("cfgLogFile").checked=c.logFile!==false;
    document.getElementById("cfgLogRetention").value=c.logRetentionDays||7;
    document.getElementById("cfgLogDetail").value=c.logDetail||"full";
    document.getElementById("cfgEnableAutoLock").checked=c.enableAutoLock!==false;
    document.getElementById("cfgMaxReqPerMin").value=c.maxRequestsPerMin||10;
    document.getElementById("cfgMaxTokPerMin").value=c.maxTokensPerMin||0;
    document.getElementById("cfgStreamLifetime").value=c.streamLifetime||1800000;
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
    document.getElementById("cfgCmdPath").value=c.cmdPath||"/mnt/c/Windows/System32/cmd.exe";
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
      const idleMin=Math.round((Date.now()-lastKeyUseTime)/60000);
      const sinceResume=typeof lastResumeTime==='number'&&lastResumeTime>0?Math.round((Date.now()-lastResumeTime)/60000):null;
      resumeEl.textContent="🧬 闲置恢复: Key 闲置 "+idleMin+"m"+(sinceResume!==null?"，上次触发 "+sinceResume+"m 前":"，等待阈值");
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
    html+='<div class="resume-proj-row" style="display:flex;gap:4px;align-items:center;flex-wrap:wrap;padding:4px;background:#1e293b;border:1px solid #334155;border-radius:4px">'+
      '<input placeholder="项目名" class="rp-name" value="'+esc(p.name||'')+'" style="width:80px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">'+
      '<input placeholder="WSL 路径 /mnt/d/..." class="rp-path" value="'+esc(p.path||'')+'" style="flex:1;min-width:120px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">'+
      '<input placeholder="命令 codex ..." class="rp-cmd" value="'+esc(p.cmd||'')+'" style="flex:1;min-width:100px;background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:2px 4px;border-radius:4px;font-size:11px">'+
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
    if(path&&cmd)projects.push({name:name||path.split("/").pop(),path,cmd});
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

function toggleTrendMode(){
  trendMode=trendMode==="bytes"?"req":(trendMode==="req"?"model":(trendMode==="model"?"health":"bytes"));
  const labels={bytes:"📊 流量趋势",req:"📈 次数趋势",model:"📊 模型趋势",health:"💚 健康趋势"};
  document.getElementById("trendModeLabel").textContent=labels[trendMode]||"📊 流量趋势";
  renderTrend();
}
function renderTrend(){
  const now=new Date();
  const hours=trendRange==="7d"?168:(trendRange==="30d"?720:24);
  const hMap={};
  for(let i=hours-1;i>=0;i--){
    const d=new Date(now-i*3600000);
    const y=d.getFullYear(),m=String(d.getMonth()+1).padStart(2,"0"),dd=String(d.getDate()).padStart(2,"0"),hh=String(d.getHours()).padStart(2,"0");
    hMap[y+"-"+m+"-"+dd+"-"+hh]={bytes:0,input:0,output:0,req:0,keys:{},models:{},status:{}};
  }
  const allModels={};
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
      if(v.models){
        for(const [mk,mv] of Object.entries(v.models)){
          if(!h.models[mk])h.models[mk]={requests:0,inputBytes:0,outputBytes:0};
          h.models[mk].requests+=mv.requests||0;
          h.models[mk].inputBytes+=mv.inputBytes||0;
          h.models[mk].outputBytes+=mv.outputBytes||0;
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
      return '<div class="trend-bar" style="height:'+Math.max(2,hMap[k][trendMode]/max*80)+'px" title="'+esc(lines.join("\\n")).replace(/\\n/g,"&#10;")+'"></div>';
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
    const idleMin=Math.round((Date.now()-lastKeyUseTime)/60000);
    const sinceResume=typeof lastResumeTime==='number'&&lastResumeTime>0?Math.round((Date.now()-lastResumeTime)/60000):null;
    if(sinceResume!==null)dashResume.textContent="🧬Key闲置"+idleMin+"m/恢复"+sinceResume+"m前";
    else dashResume.textContent="🧬Key闲置"+idleMin+"m";
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
  if(!sel.length){alert("请先勾选要设置时段的 Key");return}
  const tzOpts=Array.from({length:25},(_, i)=>i-12).map(v=>'<option value="'+(v>=0?"+":"")+v+'">UTC'+(v>=0?"+":"")+v+'</option>').join('');
  const hourOpts=Array.from({length:24},(_, i)=>'<option value="'+i+'">'+(i<10?'0':'')+i+':00</option>').join('');
  const html='<div style="padding:12px;background:#1e293b;border-radius:8px;min-width:280px">'+
    '<div style="margin-bottom:8px;color:#e2e8f0;font-size:13px;font-weight:500">设置选中 '+sel.length+' 个 Key 的时段</div>'+
    '<div style="display:grid;grid-template-columns:auto 1fr;gap:8px;font-size:12px;align-items:center">'+
    '<label style="color:#94a3b8">时区</label><select id="twTz" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px;border-radius:4px">'+tzOpts+'</select>'+
    '<label style="color:#94a3b8">开始</label><select id="twStart" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px;border-radius:4px">'+hourOpts+'</select>'+
    '<label style="color:#94a3b8">结束</label><select id="twEnd" style="background:#0f172a;border:1px solid #475569;color:#e2e8f0;padding:4px;border-radius:4px">'+hourOpts+'</select>'+
    '</div>'+
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
let logTotalPages = 1;
let logAllEntries = [];
let logLastStats = null;
const LOG_PAGE_SIZE = 200;
let _logRenderTimer = null;
let _logSparkVersion = 0;
let _logSparkCache = { version: 0, bars: null };

function logRequestFailed(e){
  return !!e && (e.status === 0 || e.status >= 400 || e.streamOutcome === "failed");
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
  const qs=document.getElementById("logSearch")?.value.trim();
  if(qs)p.set("q",qs);
  const t=document.getElementById("logTimeFilter")?.value;
  if(t==="custom"){
    const st=document.getElementById("logStartTime")?.value;
    const et=document.getElementById("logEndTime")?.value;
    if(st)p.set("since",new Date(st).getTime());
    if(et)p.set("until",new Date(et).getTime());
  }else if(t){
    const ms={"5m":3e5,"15m":9e5,"1h":36e5,"24h":864e5,"7d":6048e5,"30d":2592e6}[t];
    if(ms)p.set("since",Date.now()-ms);
  }
  p.set("limit",String(LOG_PAGE_SIZE));
  return p;
}
async function openLogs(){
  document.getElementById("logModal").classList.add("on");
  await loadLogs(1);
}
async function loadLogs(page){
  try{
    const qp=buildLogFilterQuery();
    const pg=page||logCurrentPage;
    qp.set("offset",String(Math.max(0,(pg-1)*LOG_PAGE_SIZE)));
    const r=await fetch("http://localhost:3456/__logs?"+qp.toString());
    if(!r.ok)return;
    const resp=await r.json();
    logAllEntries = resp.entries || resp;
    _logSparkVersion++;
    logLastStats = resp.stats || null;
    logTotalPages = Math.max(1, Math.ceil((logLastStats?.totalAll || logLastStats?.total || logAllEntries.length) / LOG_PAGE_SIZE));
    logCurrentPage = pg;
    if (logCurrentPage > logTotalPages) logCurrentPage = logTotalPages;
    renderLogs();
  }catch(e){}
}
function reloadLogs(){ loadLogs(1); }
function logPage(delta){
  const np = Math.max(1, Math.min(logTotalPages, logCurrentPage + delta));
  if (np !== logCurrentPage) loadLogs(np);
}
function renderLogs(){
  const tbody=document.getElementById("logBody");
  const startIdx = (logCurrentPage - 1) * LOG_PAGE_SIZE;
  const pageEntries = logAllEntries;
  tbody.innerHTML = pageEntries.length
    ? pageEntries.map((e,i) => '<tr class="'+logRowClass(e)+'" onclick="toggleLogDetail(this,'+(startIdx+i)+')">'+makeLogRow(e, startIdx+i+1)+'</tr>'
      + '<tr class="log-row-expand" id="logDetail_'+(startIdx+i)+'"><td colspan="12"><span id="logDetailContent_'+(startIdx+i)+'"></span></td></tr>').join("")
    : '<tr><td colspan="12" style="text-align:center;color:#64748b;padding:20px">暂无记录</td></tr>';
  // Pagination
  document.getElementById("logPrevBtn").disabled = logCurrentPage <= 1;
  document.getElementById("logNextBtn").disabled = logCurrentPage >= logTotalPages;
  document.getElementById("logPageInfo").textContent = "\u7b2c "+logCurrentPage+" / "+logTotalPages+" \u9875";
  // Stats
  if (logLastStats) {
    const s = logLastStats;
    document.getElementById("lsTotal").textContent = s.total;
    document.getElementById("lsSuccess").textContent = s.total > 0 ? s.successRate + "%" : "-";
    document.getElementById("lsSuccess").style.color = s.successRate >= 95 ? "#22c55e" : s.successRate >= 80 ? "#f59e0b" : "#ef4444";
    document.getElementById("lsAvgDur").textContent = s.avgDuration ? fmtDur(s.avgDuration) : "-";
    document.getElementById("lsP95").textContent = s.p95 ? fmtDur(s.p95) : "-";
    document.getElementById("lsP99").textContent = s.p99 ? fmtDur(s.p99) : "-";
    document.getElementById("ls4xx").textContent = s.error4xx || "0";
    document.getElementById("ls4xx").style.color = s.error4xx > 0 ? "#f59e0b" : "#94a3b8";
    document.getElementById("ls5xx").textContent = s.error5xx || "0";
    document.getElementById("ls5xx").style.color = s.error5xx > 0 ? "#ef4444" : "#94a3b8";
    document.getElementById("lsTimeout").textContent = s.errorTimeout || "0";
  }
  renderLogSparkline();
  renderLogModelDist();
  renderLogErrorCluster();
}
function renderLogSparkline(){
  // Cache: only recompute if data changed
  if (_logSparkCache.version === _logSparkVersion && _logSparkCache.bars) {
    document.getElementById("logSparklineWrap").innerHTML = _logSparkCache.bars;
    return;
  }
  const wrap = document.getElementById("logSparklineWrap");
  const now = Date.now();
  const bars = [];
  for (let i = 29; i >= 0; i--) {
    const start = now - (i + 1) * 60000;
    const end = now - i * 60000;
    const inMin = logAllEntries.filter(e => e.time >= start && e.time < end && e.type !== "event");
    const total = inMin.length;
    const errs = inMin.filter(logRequestFailed).length;
    bars.push({ total, errs, start });
  }
  const maxVal = Math.max(1, ...bars.map(b => b.total));
  const html = bars.map(b => {
    const h = Math.round(b.total / maxVal * 24);
    const cls = b.errs > 0 ? "log-spark-bar err" : (b.total > 0 ? "log-spark-bar ok" : "log-spark-bar");
    const tip = new Date(b.start).toTimeString().slice(0,5)+" - 请求:"+b.total+" 错误:"+b.errs;
    return '<div class="'+cls+'" style="height:'+Math.max(h,1)+'px;background:'+(b.errs>0?'#ef4444':b.total>0?'#3b82f6':'#334155')+'" title="'+tip+'"></div>';
  }).join("");
  _logSparkCache.version = _logSparkVersion;
  _logSparkCache.bars = html;
  wrap.innerHTML = html;
}
function renderLogModelDist(){
  const el = document.getElementById("logModelDist");
  const map = {};
  const reqs = logAllEntries.filter(e => e.type !== "event");
  for (const e of reqs) {
    const m = e.overrideModel || e.reqModel || "(未知)";
    if (!map[m]) map[m] = { total: 0, fail: 0, dur: 0 };
    map[m].total++;
    if (logRequestFailed(e)) map[m].fail++;
    if (e.duration) map[m].dur += e.duration;
  }
  const sorted = Object.keys(map).sort((a,b) => map[b].total - map[a].total).slice(0,8);
  el.innerHTML = sorted.length ? '<span style="color:#64748b;margin-right:4px">📊 模型:</span>'
    + sorted.map(m => {
      const s = map[m];
      const avg = s.total > 0 ? Math.round(s.dur / s.total) : 0;
      const failPct = s.total > 0 ? Math.round(s.fail / s.total * 100) : 0;
      return '<span class="log-model-tag">'+esc(m)+' ('+s.total+')'
        + (s.fail > 0 ? ' <span class="fail">✕'+s.fail+'</span>' : '')
        + ' <span style="color:#64748b">'+fmtDur(avg)+'</span></span>';
    }).join("") : '';
}
function renderLogErrorCluster(){
  const body = document.getElementById("logErrorBody");
  const map = {};
  const reqs = logAllEntries.filter(e => e.type !== "event");
  for (const e of reqs) {
    const st = e.status || 0;
    if (logRequestFailed(e)) {
      const key = e.streamOutcome === "failed" ? "流中断" : (st === 0 ? "超时" : String(st));
      map[key] = (map[key] || 0) + 1;
    }
  }
  const sorted = Object.keys(map).sort((a,b) => map[b] - map[a]);
  const el = document.getElementById("logErrorCluster");
  const head = el.querySelector(".head");
  const totalErr = sorted.reduce((s,k) => s+map[k], 0);
  head.textContent = "⚠ 错误分布 ("+totalErr+" 次)";
  body.innerHTML = sorted.length
    ? sorted.map(k => '<span class="log-error-code">'+k+' x'+map[k]+'</span>').join("")
    : '<span style="color:#22c55e">无错误</span>';
}
function toggleErrorCluster(){
  document.getElementById("logErrorBody").classList.toggle("open");
}
let logSortField = "time";
let logSortAsc = false;
function logSortBy(field){
  if (logSortField === field) logSortAsc = !logSortAsc;
  else { logSortField = field; logSortAsc = field === "time" ? false : true; }
  // Update sort icons
  ["","_idx","_model","_status","_dur"].forEach(sfx => {
    const el = document.getElementById("logSortIcon"+sfx);
    if (el) el.textContent = (field === (sfx.replace("_","") || "time")) ? (logSortAsc ? "▲" : "▼") : "";
  });
  logAllEntries.sort((a,b) => {
    if (a.type === "event" && b.type !== "event") return 1;
    if (a.type !== "event" && b.type === "event") return -1;
    let va, vb;
    switch(field){
      case "idx": va=a.idx||0; vb=b.idx||0; break;
      case "model": va=(a.overrideModel||a.reqModel||""); vb=(b.overrideModel||b.reqModel||""); return logSortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
      case "status": va=a.status||0; vb=b.status||0; break;
      case "dur": va=a.duration||0; vb=b.duration||0; break;
      default: va=a.time||0; vb=b.time||0;
    }
    return logSortAsc ? va - vb : vb - va;
  });
  _logSparkVersion++;
  renderLogs();
}
function toggleLogDetail(tr, idx){
  const expandRow = document.getElementById("logDetail_"+idx);
  if (!expandRow) return;
  const isOpen = expandRow.classList.contains("open");
  // Close all other details
  document.querySelectorAll(".log-row-expand.open").forEach(el => el.classList.remove("open"));
  if (isOpen) return;
  expandRow.classList.add("open");
  const e = logAllEntries[idx];
  if (!e) return;
  const content = document.getElementById("logDetailContent_"+idx);
  if (e.type === "event") {
    const lines = ["事件: "+(e.eventType||""), "消息: "+(e.message||""), "URL: "+(e.url||"")];
    if (e.eventType === "stream_terminal") {
      lines.push("流 ID: "+(e.streamId||""));
      lines.push("结果: "+(e.streamOutcome||""));
      lines.push("原因: "+(e.streamReason||""));
      lines.push("收到 [DONE]: "+(e.streamSawDone === true ? "是" : "否"));
    }
    content.textContent = lines.join("\\n");
  } else {
    const lines = [
      "时间: "+new Date(e.time).toISOString(),
      "Key #: "+(e.idx||""),
      "方法: "+e.method,
      "路径: "+e.path,
      "上游URL: "+(e.url||""),
      "模型: "+(e.reqModel||""),
      "覆盖模型: "+(e.overrideModel||""),
      "状态码: "+e.status,
      "上行: "+fmtBytes(e.inputBytes||0),
      "下行: "+fmtBytes(e.outputBytes||0),
      "耗时: "+fmtDur(e.duration||0),
      "首字节: "+(e.ttfb?fmtDur(e.ttfb):"-"),
      "协议转换: "+(e.conversion?"是":"否"),
      "流 ID: "+(e.streamId||""),
      "流结果: "+(e.streamOutcome||""),
      "流终态原因: "+(e.streamReason||""),
      "收到 [DONE]: "+(e.streamSawDone === true ? "是" : "否"),
    ];
    content.textContent = lines.join("\\n");
  }
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
  window.open("http://localhost:3456/__logs?"+q.toString()+"&format=csv");
}

function openConfig(){renderUpdateInfo();loadConfigUI();document.getElementById("configModal").classList.add("on")}
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
    autoRecoverPoll:document.getElementById("cfgAutoRecoverPoll").checked,
    autoRecoverPollInterval:parseInt(document.getElementById("cfgAutoRecoverPollInterval").value)||5,
    autoRecoverPollCodes:(document.getElementById("cfgAutoRecoverPollCodes").value||"").split(",").map(s=>parseInt(s.trim())).filter(n=>!isNaN(n)),
    autoRecoverDelays:(document.getElementById("cfgAutoRecoverDelays").value||"").split(",").map(s=>parseInt(s.trim())).filter(n=>!isNaN(n)&&n>=100&&n<=10000).slice(0,10),
    roundRobin:document.getElementById("cfgRoundRobin").checked,
    weeklySortBy:document.getElementById("cfgWeeklySortBy").checked?"expiry":"priority",
    enableAutoLock:document.getElementById("cfgEnableAutoLock").checked,
    maxRequestsPerMin:parseInt(document.getElementById("cfgMaxReqPerMin").value)||10,
    maxTokensPerMin:parseInt(document.getElementById("cfgMaxTokPerMin").value)||0,
    streamLifetime:parseInt(document.getElementById("cfgStreamLifetime").value)||1800000,
    adminToken:document.getElementById("cfgAdminToken").value.trim(),
    updateBaselineTag:document.getElementById("cfgUpdateBaselineTag").value.trim(),
    lockAfterFailCount:parseInt(document.getElementById("cfgLockCount").value)||3,
    lockFailCodes:(document.getElementById("cfgLockCodes").value||"").split(",").map(s=>s.trim()).filter(s=>s),
    logFile:document.getElementById("cfgLogFile").checked,
    logRetentionDays:parseInt(document.getElementById("cfgLogRetention").value)||7,
    logDetail:document.getElementById("cfgLogDetail").value,
    autoResume:document.getElementById("cfgAutoResume").checked,
    autoResumeIdleMinutes:parseInt(document.getElementById("cfgAutoResumeIdle").value)||10,
    autoResumeDebounceMinutes:parseInt(document.getElementById("cfgAutoResumeDebounce").value)||3,
    cmdPath:document.getElementById("cfgCmdPath").value.trim()||"/mnt/c/Windows/System32/cmd.exe",
    autoResumeProjects:collectResumeProjects()
  };
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
  "upstream_close",
  "upstream_error",
  "upstream_idle_timeout",
  "stream_lifetime_timeout",
  "client_disconnect",
]);

function normalizeStreamTerminalReason(reason, fallback) {
  return STREAM_TERMINAL_REASONS.has(reason) ? reason : fallback;
}

function createResponsesLifecycle(model) {
  return {
    responseId: "resp_" + crypto.randomUUID(),
    created: Math.floor(Date.now() / 1000),
    model: model || "",
    fullContent: "",
    inputTokens: 0,
    outputTokens: 0,
    terminalKind: null,
    terminalReason: null,
    sawDone: false,
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
function createChatToMessagesStream(upstreamUrl) {
  const Transform = require("stream").Transform;
  let buffer = "";
  let thinkingContent = "";
  let textContent = "";
  let model = "";
  let inputTokens = 0, outputTokens = 0;
  let stopReason = "end_turn";
  let hasStarted = false;
  let thinkingStarted = false;
  const TEXT_IDX = 0, THINKING_IDX = 1, TOOL_IDX = 2;
  return new Transform({
    readableObjectMode: false, writableObjectMode: false,
    transform(chunk, encoding, cb) {
      buffer += chunk.toString();
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";
      for (const line of lines) {
        if (!line.startsWith("data: ")) continue;
        const data = line.slice(6).trim();
        if (data === "[DONE]") {
          if (!hasStarted) {
            this.push(`event: message_start\ndata: {"type":"message_start","message":{"id":"msg_${Date.now()}","type":"message","role":"assistant","content":[],"model":${JSON.stringify(model)},"stop_reason":"${stopReason}","usage":{"input_tokens":${inputTokens},"output_tokens":${outputTokens}}}}\n\n`);
          }
          if (thinkingStarted) {
            this.push(`event: content_block_stop\ndata: {"type":"content_block_stop","index":${THINKING_IDX}}\n\n`);
          }
          this.push(`event: content_block_stop\ndata: {"type":"content_block_stop","index":${TEXT_IDX}}\n\n`);
          this.push(`event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"${stopReason}","stop_sequence":null},"usage":{"output_tokens":${outputTokens}}}\n\n`);
          this.push(`event: message_stop\ndata: {"type":"message_stop"}\n\n`);
          return cb();
        }
        let parsed;
        try { parsed = JSON.parse(data); } catch(e) { continue; }
        if (parsed.object === "chat.completion.chunk") {
          model = parsed.model || model;
          const delta = parsed.choices?.[0]?.delta;
          if (parsed.usage) {
            inputTokens = parsed.usage.prompt_tokens || 0;
            outputTokens = parsed.usage.completion_tokens || 0;
          }
          const finishReason = parsed.choices?.[0]?.finish_reason;
          if (finishReason === "stop") stopReason = "end_turn";
          else if (finishReason === "length") stopReason = "max_tokens";
          else if (finishReason === "tool_calls") stopReason = "tool_use";
          else if (finishReason) stopReason = finishReason;

          if (delta?.reasoning_content) {
            if (!hasStarted) {
              hasStarted = true;
              this.push(`event: message_start\ndata: {"type":"message_start","message":{"id":"msg_${Date.now()}","type":"message","role":"assistant","content":[{"type":"text","text":""}],"model":${JSON.stringify(model)},"stop_reason":null,"usage":{"input_tokens":${inputTokens},"output_tokens":${outputTokens}}}}\n\n`);
            }
            if (!thinkingStarted) {
              thinkingStarted = true;
              this.push(`event: content_block_start\ndata: {"type":"content_block_start","index":${THINKING_IDX},"content_block":{"type":"thinking","thinking":""}}\n\n`);
            }
            thinkingContent += delta.reasoning_content;
            this.push(`event: content_block_delta\ndata: {"type":"content_block_delta","index":${THINKING_IDX},"delta":{"type":"thinking_delta","thinking":${JSON.stringify(delta.reasoning_content)}}}\n\n`);
          }
          if (delta?.content) {
            if (!hasStarted) {
              hasStarted = true;
              this.push(`event: message_start\ndata: {"type":"message_start","message":{"id":"msg_${Date.now()}","type":"message","role":"assistant","content":[{"type":"text","text":""}],"model":${JSON.stringify(model)},"stop_reason":null,"usage":{"input_tokens":${inputTokens},"output_tokens":${outputTokens}}}}\n\n`);
              this.push(`event: content_block_start\ndata: {"type":"content_block_start","index":${TEXT_IDX},"content_block":{"type":"text","text":""}}\n\n`);
            }
            textContent += delta.content;
            this.push(`event: content_block_delta\ndata: {"type":"content_block_delta","index":${TEXT_IDX},"delta":{"type":"text_delta","text":${JSON.stringify(delta.content)}}}\n\n`);
          }
          if (delta?.tool_calls) {
            for (const tc of delta.tool_calls) {
              if (tc.function?.name) {
                this.push(`event: content_block_start\ndata: {"type":"content_block_start","index":${TOOL_IDX},"content_block":{"type":"tool_use","id":"${tc.id || "toolu_"+Date.now()}","name":"${tc.function.name}","input":{}}}\n\n`);
              }
              if (tc.function?.arguments) {
                this.push(`event: content_block_delta\ndata: {"type":"content_block_delta","index":${TOOL_IDX},"delta":{"type":"input_json_delta","partial_json":${JSON.stringify(tc.function.arguments)}}}\n\n`);
              }
              if (tc.function?.name) {
                this.push(`event: content_block_stop\ndata: {"type":"content_block_stop","index":${TOOL_IDX}}\n\n`);
              }
            }
          }
        }
      }
      cb();
    },
    flush(cb) {
      if (textContent || thinkingContent) {
        if (thinkingStarted) {
          this.push(`event: content_block_stop\ndata: {"type":"content_block_stop","index":${THINKING_IDX}}\n\n`);
        }
        this.push(`event: content_block_stop\ndata: {"type":"content_block_stop","index":${TEXT_IDX}}\n\n`);
        this.push(`event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"${stopReason}","stop_sequence":null},"usage":{"output_tokens":${outputTokens}}}\n\n`);
        this.push(`event: message_stop\ndata: {"type":"message_stop"}\n\n`);
      }
      cb();
    }
  });
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
function createMessagesToChatStream(upstreamUrl) {
  const Transform = require("stream").Transform;
  let buffer = "";
  let fullContent = "";
  let model = "";
  let inputTokens = 0, outputTokens = 0;
  return new Transform({
    readableObjectMode: false, writableObjectMode: false,
    transform(chunk, encoding, cb) {
      buffer += chunk.toString();
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";
      for (const line of lines) {
        if (!line.startsWith("data: ")) continue;
        const data = line.slice(6).trim();
        if (!data) continue;
        let parsed;
        try { parsed = JSON.parse(data); } catch(e) { continue; }
        if (parsed.type === "message_start") {
          model = parsed.message?.model || model;
          inputTokens = parsed.message?.usage?.input_tokens || 0;
        } else if (parsed.type === "content_block_delta" && parsed.delta?.type === "text_delta") {
          fullContent += parsed.delta.text;
          this.push(`data: {"choices":[{"index":0,"delta":{"role":"assistant","content":${JSON.stringify(parsed.delta.text)}},"finish_reason":null}],"object":"chat.completion.chunk","model":${JSON.stringify(model)}}\n\n`);
        } else if (parsed.type === "content_block_delta" && parsed.delta?.type === "input_json_delta") {
          this.push(`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":${JSON.stringify(parsed.delta.partial_json)}}}]},"finish_reason":null}],"object":"chat.completion.chunk","model":${JSON.stringify(model)}}\n\n`);
        } else if (parsed.type === "message_delta") {
          outputTokens = parsed.usage?.output_tokens || outputTokens;
          const finishMap = { end_turn: "stop", max_tokens: "length", tool_use: "tool_calls" };
          const fr = finishMap[parsed.delta?.stop_reason] || parsed.delta?.stop_reason || "stop";
          this.push(`data: {"choices":[{"index":0,"delta":{},"finish_reason":"${fr}"}],"usage":{"prompt_tokens":${inputTokens},"completion_tokens":${outputTokens},"total_tokens":${inputTokens + outputTokens}},"object":"chat.completion.chunk","model":${JSON.stringify(model)}}\n\n`);
        } else if (parsed.type === "message_stop") {
          this.push("data: [DONE]\n\n");
        }
      }
      cb();
    },
    flush(cb) {
      if (fullContent) {
        this.push("data: [DONE]\n\n");
      }
      cb();
    }
  });
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

  if (pathname === "/__config") {
    if (req.method === "GET") {
      res.writeHead(200, cors);
      res.end(JSON.stringify({ ...config, autoRecoverNextTime, autoRecoverDailyNextTime, autoRecoverPollNextTime, lastRequestTime, lastKeyUseTime, lastResumeTime }, null, 2));
      return;
    }
    if (req.method === "PUT") {
      const bodyChunks = [];
      req.on("data", c => bodyChunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(bodyChunks).toString('utf-8');
        try {
          const c = JSON.parse(body);
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
          fs.writeFileSync(CONFIG_FILE, JSON.stringify(cur, null, 2));
          const savedNextTime = autoRecoverNextTime;
          const savedDailyNextTime = autoRecoverDailyNextTime;
          const savedInterval = config.autoRecoverInterval;
          const savedDailyDays = config.autoRecoverDailyDays;
          const savedDailyHour = config.autoRecoverDailyHour;
          const savedDailyMin = config.autoRecoverDailyMinute;
          loadConfig();
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
      const q = u.searchParams.get("q") || "";
      const offset = parseInt(u.searchParams.get("offset") || "0", 10);
      const format = u.searchParams.get("format");

      // Cache check for repeated requests
      const cacheKey = req.url;
      if (Date.now() - _logCache.time < _logCache.ttl && _logCache.key === cacheKey && _logCache.result) {
        res.writeHead(200, cors);
        res.end(_logCache.result);
        return;
      }

      let entries;
      if (since || until) {
        const sTime = since ? parseInt(since, 10) : 0;
        const uTime = until ? parseInt(until, 10) : 9e15;
        const maxNeeded = (limit + offset) || 500;
        const fileEntries = [];
        try {
          if (fs.existsSync(LOG_DIR)) {
            let files = fs.readdirSync(LOG_DIR).filter(f => f.endsWith(".jsonl"));
            files.sort().reverse();
            for (const f of files) {
              if (fileEntries.length >= maxNeeded) break;
              const dateStr = f.replace(".jsonl", "");
              const fd = new Date(dateStr + "T00:00:00");
              const fdEnd = fd.getTime() + 86400000;
              if (fdEnd < sTime || fd.getTime() > uTime) continue;
              const filePath = path.join(LOG_DIR, f);
              const stat = fs.statSync(filePath);
              if (stat.size === 0) continue;
              const fdFile = fs.openSync(filePath, "r");
              let pos = stat.size;
              let leftover = "";
              const chunkSize = 32768;
              while (fileEntries.length < maxNeeded && pos > 0) {
                const readLen = Math.min(chunkSize, pos);
                pos -= readLen;
                const buf = Buffer.alloc(readLen);
                fs.readSync(fdFile, buf, 0, readLen, pos);
                const chunk = buf.toString("utf-8");
                const data = chunk + leftover;
                const parts = data.split("\n");
                leftover = parts[0];
                for (let i = parts.length - 1; i > 0 && fileEntries.length < maxNeeded; i--) {
                  const line = parts[i].trim();
                  if (!line) continue;
                  try {
                    const e = JSON.parse(line);
                    if (e.time >= sTime && e.time <= uTime) fileEntries.push(e);
                  } catch (e2) {}
                }
              }
              fs.closeSync(fdFile);
              if (fileEntries.length < maxNeeded && leftover.trim()) {
                try {
                  const e = JSON.parse(leftover.trim());
                  if (e.time >= sTime && e.time <= uTime) fileEntries.push(e);
                } catch (e2) {}
              }
            }
          }
        } catch (e) { /* fail silently */ }
        const seen = new Set();
        const merged = [];
        for (const e of fileEntries) {
          const k = requestLogDedupeKey(e);
          if (!seen.has(k)) { seen.add(k); merged.push(e); }
        }
        for (const e of requestLog) {
          if (e.time >= sTime && e.time <= uTime) {
            const k = requestLogDedupeKey(e);
            if (!seen.has(k)) { seen.add(k); merged.push(e); }
          }
        }
        entries = merged;
      } else {
        // No time filter: read from newest files backwards + merge with requestLog
        const maxNeeded = (limit + offset) || 500;
        const fileEntries = [];
        try {
          if (fs.existsSync(LOG_DIR)) {
            let files = fs.readdirSync(LOG_DIR).filter(f => f.endsWith(".jsonl"));
            files.sort().reverse();
            for (const f of files) {
              if (fileEntries.length >= maxNeeded) break;
              const filePath = path.join(LOG_DIR, f);
              const stat = fs.statSync(filePath);
              if (stat.size === 0) continue;
              const fdFile = fs.openSync(filePath, "r");
              let pos = stat.size;
              let leftover = "";
              const chunkSize = 32768;
              while (fileEntries.length < maxNeeded && pos > 0) {
                const readLen = Math.min(chunkSize, pos);
                pos -= readLen;
                const buf = Buffer.alloc(readLen);
                fs.readSync(fdFile, buf, 0, readLen, pos);
                const chunk = buf.toString("utf-8");
                const data = chunk + leftover;
                const parts = data.split("\n");
                leftover = parts[0];
                for (let i = parts.length - 1; i > 0 && fileEntries.length < maxNeeded; i--) {
                  const line = parts[i].trim();
                  if (!line) continue;
                  try { fileEntries.push(JSON.parse(line)); } catch (e2) {}
                }
              }
              fs.closeSync(fdFile);
              if (fileEntries.length < maxNeeded && leftover.trim()) {
                try { fileEntries.push(JSON.parse(leftover.trim())); } catch (e2) {}
              }
            }
          }
        } catch (e) { /* fail silently */ }
        const seen = new Set();
        const merged = [];
        for (const e of fileEntries) {
          const k = requestLogDedupeKey(e);
          if (!seen.has(k)) { seen.add(k); merged.push(e); }
        }
        for (const e of requestLog) {
          const k = requestLogDedupeKey(e);
          if (!seen.has(k)) { seen.add(k); merged.push(e); }
        }
        entries = merged;
        entries.sort((a, b) => b.time - a.time);
      }

      // Apply filters
      if (key) { const keys = key.split(",").map(s => parseInt(s.trim(), 10)).filter(n => !isNaN(n)); if (keys.length) entries = entries.filter(e => keys.includes(e.idx)); }
      if (status) { entries = entries.filter(e => { const s = e.status || 0; if (status.endsWith("xx")) { const prefix = parseInt(status, 10); return !isNaN(prefix) && Math.floor(s / 100) === prefix; } return String(s) === status; }); }
      if (model) { const ml = model.toLowerCase(); entries = entries.filter(e => (e.reqModel||"").toLowerCase().includes(ml) || (e.overrideModel||"").toLowerCase().includes(ml)); }
      if (q) { const ql = q.toLowerCase(); entries = entries.filter(e => (e.message||"").toLowerCase().includes(ql) || (e.url||"").toLowerCase().includes(ql) || (e.reqModel||"").toLowerCase().includes(ql) || (e.overrideModel||"").toLowerCase().includes(ql) || (e.method||"").toLowerCase().includes(ql) || (e.path||"").toLowerCase().includes(ql) || (e.eventType||"").toLowerCase().includes(ql) || (e.streamId||"").toLowerCase().includes(ql) || (e.streamOutcome||"").toLowerCase().includes(ql) || (e.streamReason||"").toLowerCase().includes(ql)); }
      if (since || until || q) entries.sort((a, b) => b.time - a.time);

      // Compute stats
      const stats = { total: 0, successRate: 0, p95: 0, p99: 0, avgDuration: 0, error4xx: 0, error5xx: 0, errorTimeout: 0, errorStream: 0 };
      stats.total = entries.length;
      stats.totalAll = countAllEntries(since, until);
      if (offset === 0) {
        const durs = entries.filter(e => e.duration != null && e.type !== "event").map(e => e.duration).sort((a, b) => a - b);
        if (durs.length) {
          stats.avgDuration = Math.round(durs.reduce((a, b) => a + b, 0) / durs.length);
          stats.p95 = durs[Math.floor(durs.length * 0.95)] || durs[durs.length - 1];
          stats.p99 = durs[Math.floor(durs.length * 0.99)] || durs[durs.length - 1];
        }
        const reqs = entries.filter(e => e.type !== "event");
        const success = reqs.filter(e => e.status >= 200 && e.status < 300 && !isRequestLogFailure(e)).length;
        stats.successRate = reqs.length ? Math.round(success / reqs.length * 10000) / 100 : 0;
        stats.error4xx = reqs.filter(e => e.status >= 400 && e.status < 500).length;
        stats.error5xx = reqs.filter(e => e.status >= 500 && e.status < 600).length;
        stats.errorTimeout = reqs.filter(e => e.status === 0 || e.status == null).length;
        stats.errorStream = reqs.filter(e => e.streamOutcome === "failed").length;
      }
      entries = entries.slice(offset, offset + limit);
      if (format === "csv") {
        const header = "time,idx,method,path,status,inputBytes,outputBytes,duration,ttfb,reqModel,overrideModel,url,type,eventType,message,streamId,streamOutcome,streamReason,streamSawDone";
        const escCsv = v => `"${String(v).replace(/"/g, '""')}"`;
        const rows = entries.map(e => [e.time, e.idx, e.method, e.path, e.status||0, e.inputBytes||0, e.outputBytes||0, e.duration||0, e.ttfb||"", escCsv(e.reqModel||""), escCsv(e.overrideModel||""), escCsv(e.url||""), e.type||"request", e.eventType||"", escCsv(e.message||""), escCsv(e.streamId||""), e.streamOutcome||"", e.streamReason||"", e.streamSawDone === true ? "true" : "false"].join(","));
        res.writeHead(200, { ...cors, "content-type": "text/csv", "content-disposition": "attachment; filename=proxy-logs.csv" });
        res.end(header + "\n" + rows.join("\n"));
        return;
      }
      const body = JSON.stringify({ entries, stats });
      _logCache.key = cacheKey;
      _logCache.result = body;
      _logCache.time = Date.now();
      res.writeHead(200, cors);
      res.end(body);
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
      const header = "time,idx,method,path,status,inputBytes,outputBytes,duration,ttfb,reqModel,overrideModel,streamId,streamOutcome,streamReason,streamSawDone";
      const esc = v => `"${String(v).replace(/"/g, '""')}"`;
      const rows = entries.map(e => [e.time, e.idx, e.method, e.path, e.status||0, e.inputBytes||0, e.outputBytes||0, e.duration||0, e.ttfb||"", esc(e.reqModel||""), esc(e.overrideModel||""), esc(e.streamId||""), e.streamOutcome||"", e.streamReason||"", e.streamSawDone === true ? "true" : "false"].join(","));
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
        forwardWithPriority(req.method, req.headers, Buffer.from(JSON.stringify(chatBody)), res, "/v1/chat/completions", transform, groupName);
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
        forwardWithPriority(req.method, req.headers, Buffer.from(JSON.stringify(chatBody)), res, "/v1/chat/completions", createChatToMessagesStream(), groupName);
      } catch (e) { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); }
    });
    req.on("error", e => { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); });
    return;
  }

  if (pathname === "/v1/chat/completions" && req.method === "POST") {
    const hasAnthropic = accounts.some(a => (a.group || "A") === groupName && isMessagesNative(a.url));
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
    forwardWithPriority(req.method, req.headers, body, res, pathname, null, groupName);
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
initServers();
