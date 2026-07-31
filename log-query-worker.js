"use strict";

const fs = require("fs");
const path = require("path");
const { parentPort, workerData } = require("worker_threads");

const LOG_FILE_RE = /^(\d{4}-\d{2}-\d{2})(?:\.(\d{1,6}))?\.jsonl$/;
const CHUNK_SIZE = 64 * 1024;
const MAX_DIMENSION_VALUES = { upstream: 32, model: 48, path: 32, group: 16, key: 64 };
const LATENCY_BOUNDS = [50, 100, 250, 500, 1000, 2500, 5000, 10000, 20000, 30000, 60000, 120000, 300000, 900000, 1800000];

function emptyHistogram() {
  return new Array(LATENCY_BOUNDS.length + 1).fill(0);
}

function addHistogram(histogram, value) {
  if (!Array.isArray(histogram) || !Number.isFinite(value) || value < 0) return;
  let index = LATENCY_BOUNDS.findIndex(bound => value <= bound);
  if (index < 0) index = LATENCY_BOUNDS.length;
  histogram[index] = (histogram[index] || 0) + 1;
}

function createAggregate() {
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
    durationHistogram: emptyHistogram(),
    ttfbHistogram: emptyHistogram(),
    dimensions: { upstream: {}, model: {}, path: {}, group: {}, key: {} },
    events: {},
  };
}

function safeHost(value) {
  try { return new URL(String(value || "")).hostname || "(unknown)"; } catch { return "(unknown)"; }
}

function dimensionValue(entry, type) {
  if (type === "upstream") return safeHost(entry.url);
  if (type === "model") return String(entry.overrideModel || entry.reqModel || "(unknown)").slice(0, 120);
  if (type === "path") return String(entry.path || "(unknown)").slice(0, 120);
  if (type === "group") return String(entry.group || "A").toUpperCase().slice(0, 32);
  if (type === "key") return entry.idx ? `#${entry.idx}` : "(unknown)";
  return "(unknown)";
}

function addDimension(aggregate, type, entry) {
  const map = aggregate.dimensions[type];
  let key = dimensionValue(entry, type);
  if (!Object.prototype.hasOwnProperty.call(map, key) && Object.keys(map).length >= MAX_DIMENSION_VALUES[type]) key = "(other)";
  if (!map[key]) {
    map[key] = {
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
      durationHistogram: emptyHistogram(),
      ttfbHistogram: emptyHistogram(),
    };
  }
  return map[key];
}

function addRequestMetrics(target, entry) {
  target.requests++;
  const status = Number(entry.status || 0);
  const failed = status === 0 || status >= 400 || entry.streamOutcome === "failed";
  if (!failed && status >= 200 && status < 300) target.success++;
  if (status >= 400 && status < 500) target.error4xx++;
  if (status >= 500 && status < 600) target.error5xx++;
  if (status === 0) target.timeout++;
  if (entry.streamOutcome === "failed") target.streamFailed++;
  if (Number.isFinite(entry.duration) && entry.duration >= 0) {
    target.durationSum += entry.duration;
    target.durationCount++;
    addHistogram(target.durationHistogram, entry.duration);
  }
  if (Number.isFinite(entry.ttfb) && entry.ttfb >= 0) {
    target.ttfbSum += entry.ttfb;
    target.ttfbCount++;
    addHistogram(target.ttfbHistogram, entry.ttfb);
  }
}

function addEntryToAggregate(aggregate, entry) {
  if (!entry || typeof entry !== "object") return;
  if (entry.type === "event") {
    const eventType = String(entry.eventType || "event").slice(0, 120);
    aggregate.events[eventType] = (aggregate.events[eventType] || 0) + 1;
    return;
  }
  addRequestMetrics(aggregate, entry);
  for (const type of Object.keys(aggregate.dimensions)) addRequestMetrics(addDimension(aggregate, type, entry), entry);
}

function parseLine(buffer) {
  const trimmed = buffer.toString("utf8").replace(/\r$/, "").trim();
  if (!trimmed) return null;
  try { return JSON.parse(trimmed); } catch { return null; }
}

function parseLogFileName(name) {
  const match = LOG_FILE_RE.exec(String(name || ""));
  if (!match) return null;
  const startedAt = Date.parse(`${match[1]}T00:00:00.000Z`);
  if (!Number.isFinite(startedAt)) return null;
  return {
    name: String(name),
    day: match[1],
    startedAt,
    segment: match[2] ? Number(match[2]) : Number.MAX_SAFE_INTEGER,
  };
}

function listLogFiles(logDir, query) {
  let files = [];
  try {
    files = fs.readdirSync(logDir)
      .map(parseLogFileName)
      .filter(Boolean)
      .sort((left, right) => right.startedAt - left.startedAt || right.segment - left.segment || right.name.localeCompare(left.name));
  } catch {
    return [];
  }
  const since = Number.isFinite(query.since) ? query.since : 0;
  const until = Number.isFinite(query.until) ? query.until : Number.MAX_SAFE_INTEGER;
  return files.filter(file => file.startedAt <= until && file.startedAt + 86400000 >= since);
}

function statusMatches(value, filter) {
  if (!filter) return true;
  const status = Number(value || 0);
  if (/^[1-5]xx$/i.test(filter)) return Math.floor(status / 100) === Number(filter[0]);
  return String(status) === String(filter);
}

function matchesQuery(entry, query) {
  if (!entry || typeof entry !== "object") return false;
  const time = Number(entry.time || 0);
  if (Number.isFinite(query.since) && time < query.since) return false;
  if (Number.isFinite(query.until) && time > query.until) return false;
  if (Array.isArray(query.keys) && query.keys.length && !query.keys.includes(Number(entry.idx))) return false;
  if (!statusMatches(entry.status, query.status)) return false;
  if (query.model) {
    const needle = query.model.toLowerCase();
    if (!String(entry.reqModel || "").toLowerCase().includes(needle) && !String(entry.overrideModel || "").toLowerCase().includes(needle)) return false;
  }
  if (query.upstream && !safeHost(entry.url).toLowerCase().includes(query.upstream.toLowerCase())) return false;
  if (query.path && !String(entry.path || "").toLowerCase().includes(query.path.toLowerCase())) return false;
  if (query.group && String(entry.group || "A").toUpperCase() !== query.group.toUpperCase()) return false;
  if (query.q) {
    const needle = query.q.toLowerCase();
    const haystack = [entry.message, entry.url, entry.reqModel, entry.overrideModel, entry.method, entry.path, entry.eventType, entry.streamId, entry.streamOutcome, entry.streamReason, entry.streamErrorMsg, entry.upstreamErrorReason, entry.terminalSource]
      .map(value => String(value || "").toLowerCase()).join("\n");
    if (!haystack.includes(needle)) return false;
  }
  return true;
}

function scanFileReverse(filePath, startPosition, consume, budget) {
  const stat = fs.statSync(filePath);
  if (!stat.isFile() || stat.size === 0) return { exhausted: true, nextPosition: 0, limited: false };
  const fd = fs.openSync(filePath, "r");
  let position = Math.max(0, Math.min(stat.size, Number.isFinite(startPosition) ? startPosition : stat.size));
  let carry = Buffer.alloc(0);
  try {
    while (position > 0) {
      if (budget.bytes >= budget.maxBytes) return { exhausted: false, nextPosition: position, limited: true };
      const chunkStart = Math.max(0, position - CHUNK_SIZE);
      const readLength = position - chunkStart;
      const chunk = Buffer.allocUnsafe(readLength);
      fs.readSync(fd, chunk, 0, readLength, chunkStart);
      budget.bytes += readLength;
      const data = carry.length ? Buffer.concat([chunk, carry]) : chunk;
      let lineEnd = data.length;
      for (let index = data.length - 1; index >= 0; index--) {
        if (data[index] !== 10) continue;
        const entry = parseLine(data.subarray(index + 1, lineEnd));
        if (entry && consume(entry, chunkStart + index)) return { exhausted: false, nextPosition: chunkStart + index, limited: false };
        lineEnd = index;
      }
      carry = data.subarray(0, lineEnd);
      position = chunkStart;
    }
    const entry = parseLine(carry);
    if (entry && consume(entry, 0)) return { exhausted: false, nextPosition: 0, limited: false };
    return { exhausted: true, nextPosition: 0, limited: false };
  } finally {
    fs.closeSync(fd);
  }
}

function scanFileForward(filePath, consume) {
  const stat = fs.statSync(filePath);
  if (!stat.isFile() || stat.size === 0) return;
  const fd = fs.openSync(filePath, "r");
  let position = 0;
  let carry = Buffer.alloc(0);
  try {
    while (position < stat.size) {
      const readLength = Math.min(CHUNK_SIZE, stat.size - position);
      const chunk = Buffer.allocUnsafe(readLength);
      fs.readSync(fd, chunk, 0, readLength, position);
      position += readLength;
      const data = carry.length ? Buffer.concat([carry, chunk]) : chunk;
      let lineStart = 0;
      for (let index = 0; index < data.length; index++) {
        if (data[index] !== 10) continue;
        const entry = parseLine(data.subarray(lineStart, index));
        if (entry) consume(entry);
        lineStart = index + 1;
      }
      carry = data.subarray(lineStart);
    }
    const entry = parseLine(carry);
    if (entry) consume(entry);
  } finally {
    fs.closeSync(fd);
  }
}

function pageStats(entries) {
  const aggregate = createAggregate();
  for (const entry of entries) addEntryToAggregate(aggregate, entry);
  return aggregate;
}

function runQuery(data) {
  const query = data.query || {};
  const files = listLogFiles(data.logDir, query);
  const entries = [];
  const limit = Math.max(1, Math.min(Number(query.limit) || 50, Number(data.maxLimit) || 1000));
  const budget = { bytes: 0, maxBytes: Math.max(CHUNK_SIZE, Number(data.maxScanBytes) || 64 * 1024 * 1024) };
  let fileIndex = 0;
  if (query.cursor && typeof query.cursor.file === "string") {
    const cursorIndex = files.findIndex(file => file.name === query.cursor.file);
    if (cursorIndex < 0) return { entries: [], nextCursor: null, hasMore: false, truncated: false, scannedBytes: 0, filesAvailable: files.length, filesScanned: 0, stats: pageStats([]) };
    fileIndex = cursorIndex;
  }
  const firstFileIndex = fileIndex;
  let nextCursor = null;
  let truncated = false;
  let filesScanned = 0;
  for (; fileIndex < files.length; fileIndex++) {
    const file = files[fileIndex];
    const filePath = path.join(data.logDir, file.name);
    const start = query.cursor && fileIndex === firstFileIndex ? query.cursor.position : undefined;
    let result;
    filesScanned++;
    try {
      result = scanFileReverse(filePath, start, (entry, resumePosition) => {
        if (!matchesQuery(entry, query)) return false;
        entries.push(entry);
        if (entries.length < limit) return false;
        nextCursor = { file: file.name, position: resumePosition };
        return true;
      }, budget);
    } catch {
      continue;
    }
    if (result.limited) {
      nextCursor = { file: file.name, position: result.nextPosition };
      truncated = true;
      break;
    }
    if (!result.exhausted) {
      // A page can end on the first line of a file. Advance directly to the
      // next older file so the caller does not receive a misleading empty page.
      if (result.nextPosition <= 0) {
        const nextFile = files[fileIndex + 1];
        nextCursor = nextFile ? { file: nextFile.name, position: Number.MAX_SAFE_INTEGER } : null;
      }
      break;
    }
  }
  return {
    entries,
    nextCursor,
    hasMore: !!nextCursor,
    truncated,
    scannedBytes: budget.bytes,
    filesAvailable: files.length,
    filesScanned,
    stats: pageStats(entries),
  };
}

function runSummary(data) {
  const files = listLogFiles(data.logDir, {});
  const excludedDays = new Set(Array.isArray(data.excludeDays) ? data.excludeDays.map(value => String(value)) : []);
  const days = {};
  for (const file of files) {
    if (excludedDays.has(file.day)) continue;
    if (!days[file.day]) days[file.day] = createAggregate();
    try { scanFileForward(path.join(data.logDir, file.name), entry => addEntryToAggregate(days[file.day], entry)); } catch { continue; }
  }
  return { days };
}

try {
  const result = workerData && workerData.kind === "summary" ? runSummary(workerData) : runQuery(workerData || {});
  parentPort.postMessage({ ok: true, result });
} catch (error) {
  parentPort.postMessage({ ok: false, error: String(error && error.message ? error.message : error) });
}
