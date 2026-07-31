#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const logFile = process.argv[2] ? path.resolve(process.argv[2]) : "";
const configFile = process.argv[3] ? path.resolve(process.argv[3]) : "";
const DEFAULT_MAX_MIB = 10;
const DEFAULT_KEEP_FILES = 5;
const MAX_MIB_LIMIT = 100;
const KEEP_FILES_LIMIT = 20;
const CONFIG_MAX_BYTES = 256 * 1024;

if (!logFile) {
  process.stderr.write("[proxy-log-rotator] missing log file path\n");
  process.exit(2);
}

let maxBytes = DEFAULT_MAX_MIB * 1024 * 1024;
let keepFiles = DEFAULT_KEEP_FILES;
let outputDescriptor = null;
let outputBytes = 0;
let closed = false;
let configTimer = null;

function boundedInteger(value, fallback, min, max) {
  const parsed = parseInt(value, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

function refreshConfig() {
  let config = null;
  try {
    const raw = fs.readFileSync(configFile, "utf8");
    if (Buffer.byteLength(raw, "utf8") <= CONFIG_MAX_BYTES) config = JSON.parse(raw);
  } catch {}
  maxBytes = boundedInteger(config && config.proxyLogMaxMiB, DEFAULT_MAX_MIB, 1, MAX_MIB_LIMIT) * 1024 * 1024;
  keepFiles = boundedInteger(config && config.proxyLogKeepFiles, DEFAULT_KEEP_FILES, 1, KEEP_FILES_LIMIT);
}

function currentLogBytes() {
  try { return Math.max(0, fs.statSync(logFile).size); } catch { return 0; }
}

function report(error) {
  const message = String(error && error.message ? error.message : error || "unknown error").slice(0, 240);
  process.stderr.write(`[proxy-log-rotator] ${message}\n`);
}

function closeOutput() {
  if (outputDescriptor === null) return;
  try { fs.closeSync(outputDescriptor); } catch {}
  outputDescriptor = null;
}

function openOutput() {
  try {
    fs.mkdirSync(path.dirname(logFile), { recursive: true, mode: 0o700 });
    outputDescriptor = fs.openSync(logFile, "a", 0o600);
    outputBytes = currentLogBytes();
    return true;
  } catch (error) {
    outputDescriptor = null;
    report(error);
    return false;
  }
}

function trimArchives() {
  let names = [];
  try { names = fs.readdirSync(path.dirname(logFile)); } catch { return; }
  const prefix = `${path.basename(logFile)}.`;
  for (const name of names) {
    if (!name.startsWith(prefix)) continue;
    const suffix = name.slice(prefix.length);
    if (!/^\d+$/.test(suffix) || Number(suffix) <= keepFiles) continue;
    try { fs.rmSync(path.join(path.dirname(logFile), name), { force: true }); } catch (error) { report(error); }
  }
}

function shiftArchives(offset = 1) {
  const shift = Math.max(1, Math.min(keepFiles, Math.floor(offset) || 1));
  for (let index = keepFiles; index >= 1; index--) {
    const source = `${logFile}.${index}`;
    if (!fs.existsSync(source)) continue;
    const destinationIndex = index + shift;
    if (destinationIndex > keepFiles) {
      fs.rmSync(source, { force: true });
      continue;
    }
    const destination = `${logFile}.${destinationIndex}`;
    fs.rmSync(destination, { force: true });
    fs.renameSync(source, destination);
  }
  trimArchives();
}

function readRange(fileDescriptor, start, length) {
  const buffer = Buffer.allocUnsafe(length);
  let offset = 0;
  while (offset < length) {
    const read = fs.readSync(fileDescriptor, buffer, offset, length - offset, start + offset);
    if (!read) break;
    offset += read;
  }
  return buffer.subarray(0, offset);
}

function migrateExistingLogIfNeeded() {
  const sourceBytes = currentLogBytes();
  outputBytes = sourceBytes;
  if (sourceBytes < maxBytes) return true;
  const tempFiles = [];
  let sourceDescriptor = null;
  try {
    // A pre-existing proxy.log can originate from an older direct-append
    // watchdog. Keep its most recent bounded segments, not only one tail.
    const chunkCount = Math.min(keepFiles, Math.ceil(sourceBytes / maxBytes));
    sourceDescriptor = fs.openSync(logFile, "r");
    for (let archiveIndex = 1; archiveIndex <= chunkCount; archiveIndex++) {
      const end = Math.max(0, sourceBytes - (archiveIndex - 1) * maxBytes);
      const start = Math.max(0, end - maxBytes);
      const chunk = readRange(sourceDescriptor, start, end - start);
      if (!chunk.length) continue;
      const tempFile = `${logFile}.rotator.${process.pid}.${archiveIndex}.tmp`;
      fs.writeFileSync(tempFile, chunk, { mode: 0o600 });
      tempFiles.push({ archiveIndex, tempFile });
    }
    if (!tempFiles.length) return true;
    fs.truncateSync(logFile, 0);
    shiftArchives(tempFiles.length);
    for (const { archiveIndex, tempFile } of tempFiles) {
      const destination = `${logFile}.${archiveIndex}`;
      fs.rmSync(destination, { force: true });
      fs.renameSync(tempFile, destination);
    }
    outputBytes = currentLogBytes();
    return true;
  } catch (error) {
    report(error);
    return false;
  } finally {
    try { if (sourceDescriptor !== null) fs.closeSync(sourceDescriptor); } catch {}
    for (const { tempFile } of tempFiles) {
      try { fs.rmSync(tempFile, { force: true }); } catch {}
    }
  }
}

function rotateActiveLog() {
  closeOutput();
  const activeBytes = currentLogBytes();
  let rotated = true;
  try {
    if (activeBytes >= maxBytes) {
      rotated = migrateExistingLogIfNeeded();
    } else if (activeBytes > 0) {
      shiftArchives(1);
      fs.rmSync(`${logFile}.1`, { force: true });
      fs.renameSync(logFile, `${logFile}.1`);
      outputBytes = 0;
    }
  } catch (error) {
    report(error);
    rotated = false;
  }
  const reopened = openOutput();
  return rotated && reopened;
}

function writeFully(buffer, offset, length) {
  let written = 0;
  while (written < length) {
    const count = fs.writeSync(outputDescriptor, buffer, offset + written, length - written);
    if (!count) throw new Error("zero-byte log write");
    written += count;
  }
}

function writeChunk(chunk) {
  if (closed || !chunk || !chunk.length) return;
  if (outputDescriptor === null && !openOutput()) return;
  let offset = 0;
  while (offset < chunk.length) {
    if (outputBytes >= maxBytes && !rotateActiveLog()) {
      // Disk/rename failures must not make the supervised proxy block forever.
      // Continue appending to the reopened file and report the failure above.
      try {
        writeFully(chunk, offset, chunk.length - offset);
        outputBytes += chunk.length - offset;
      } catch (error) {
        report(error);
      }
      return;
    }
    const available = Math.max(1, maxBytes - outputBytes);
    const length = Math.min(available, chunk.length - offset);
    try {
      writeFully(chunk, offset, length);
    } catch (error) {
      report(error);
      return;
    }
    outputBytes += length;
    offset += length;
    if (outputBytes >= maxBytes && offset < chunk.length && !rotateActiveLog()) return;
  }
}

function finish() {
  if (closed) return;
  closed = true;
  if (configTimer) clearInterval(configTimer);
  closeOutput();
}

refreshConfig();
try { fs.mkdirSync(path.dirname(logFile), { recursive: true, mode: 0o700 }); } catch (error) { report(error); }
migrateExistingLogIfNeeded();
openOutput();

process.stdin.on("data", writeChunk);
process.stdin.on("end", finish);
process.stdin.on("error", error => {
  report(error);
  finish();
});
// A process-substitution pipe can be connected just after this process starts.
// Keep stdin referenced until EOF so Node cannot exit before its first chunk.
process.stdin.resume();
process.once("SIGTERM", finish);
process.once("SIGINT", finish);

configTimer = setInterval(() => {
  if (closed) return;
  refreshConfig();
  trimArchives();
  outputBytes = Math.max(outputBytes, currentLogBytes());
  if (outputBytes >= maxBytes) rotateActiveLog();
}, 10000);
// Keep the rotator alive while the supervised proxy is still connected. The
// timer is cleared by finish() after stdin EOF, so standalone/rotate-only use
// still exits promptly.
