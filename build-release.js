#!/usr/bin/env node
"use strict";

const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");

const SOURCE_DIR = __dirname;
const DEFAULT_OUTPUT_DIR = path.join(SOURCE_DIR, "dist");
const RELEASE_CONTENT_FILES = [
  "proxy.js",
  "package.json",
  "package-lock.json",
  "README.md",
  "dashboard.html",
  "install.sh",
  "start-proxy.sh",
  "watchdog.sh",
  "resume-codex.sh",
  "edit-keys.sh",
  "codex-proxy.service",
];
const RELEASE_INTEGRITY_FILES = [
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
];
const REQUIRED_INTEGRITY_FILES = ["proxy.js", "package.json"];

function fail(message) {
  throw new Error(message);
}

function normalizeTag(value) {
  const match = /^v?(\d+)\.(\d+)\.(\d+)$/i.exec(String(value || "").trim());
  return match ? `v${match[1]}.${match[2]}.${match[3]}` : "";
}

function normalizeCommit(value) {
  const commit = String(value || "").trim();
  return /^[0-9a-f]{7,64}$/i.test(commit) ? commit.toLowerCase() : null;
}

function sha256(buffer) {
  return crypto.createHash("sha256").update(buffer).digest("hex");
}

function parseArgs(argv) {
  const options = { tag: "", out: DEFAULT_OUTPUT_DIR, commit: null };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--tag" || arg === "--out" || arg === "--commit") {
      const value = argv[++i];
      if (!value) fail(`Missing value for ${arg}`);
      options[arg.slice(2)] = value;
    } else if (arg === "--help" || arg === "-h") {
      console.log("Usage: node build-release.js --tag vX.Y.Z [--out ./dist] [--commit <sha>]");
      process.exit(0);
    } else {
      fail(`Unknown option: ${arg}`);
    }
  }
  const tag = normalizeTag(options.tag || process.env.GITHUB_REF_NAME);
  if (!tag) fail("A stable release tag is required, for example --tag v1.2.3");
  const outputDir = path.resolve(options.out || DEFAULT_OUTPUT_DIR);
  return { tag, outputDir, commit: normalizeCommit(options.commit || process.env.GITHUB_SHA) };
}

function getGitCommit() {
  try {
    const result = spawnSync("git", ["-C", SOURCE_DIR, "rev-parse", "HEAD"], {
      encoding: "utf8",
      timeout: 1000,
      maxBuffer: 4096,
      windowsHide: true,
    });
    return !result.error && result.status === 0 ? normalizeCommit(result.stdout) : null;
  } catch {
    return null;
  }
}

function copyRegularFile(relativePath, destinationDir) {
  const sourcePath = path.resolve(SOURCE_DIR, relativePath);
  if (!sourcePath.startsWith(SOURCE_DIR + path.sep)) fail(`Invalid release file path: ${relativePath}`);
  if (!fs.existsSync(sourcePath)) return false;
  const stat = fs.lstatSync(sourcePath);
  if (!stat.isFile() || stat.isSymbolicLink()) fail(`Release file must be a regular file: ${relativePath}`);
  const destinationPath = path.join(destinationDir, relativePath);
  fs.mkdirSync(path.dirname(destinationPath), { recursive: true });
  fs.copyFileSync(sourcePath, destinationPath);
  fs.chmodSync(destinationPath, stat.mode & 0o777);
  return true;
}

function writeJson(filePath, value) {
  const data = Buffer.from(`${JSON.stringify(value, null, 2)}\n`, "utf8");
  fs.writeFileSync(filePath, data, { mode: 0o644 });
  return data;
}

function setReleasePackageVersion(releaseDir, tag) {
  const version = tag.slice(1);
  const packagePath = path.join(releaseDir, "package.json");
  const packageJson = JSON.parse(fs.readFileSync(packagePath, "utf8"));
  if (!packageJson || typeof packageJson !== "object" || Array.isArray(packageJson)) {
    fail("package.json must contain an object");
  }
  packageJson.version = version;
  // Release assets are runtime-only and do not include the source test/build tooling.
  delete packageJson.scripts;
  writeJson(packagePath, packageJson);

  const lockPath = path.join(releaseDir, "package-lock.json");
  if (!fs.existsSync(lockPath)) return;
  const lockJson = JSON.parse(fs.readFileSync(lockPath, "utf8"));
  if (!lockJson || typeof lockJson !== "object" || Array.isArray(lockJson)) {
    fail("package-lock.json must contain an object");
  }
  lockJson.version = version;
  if (lockJson.packages && lockJson.packages[""] && typeof lockJson.packages[""] === "object") {
    lockJson.packages[""].version = version;
  }
  writeJson(lockPath, lockJson);
}

function buildRelease(options) {
  const releaseDir = path.join(options.outputDir, `codex-proxy-${options.tag}`);
  if (fs.existsSync(releaseDir)) fail(`Release output already exists: ${releaseDir}`);
  fs.mkdirSync(releaseDir, { recursive: true, mode: 0o755 });

  try {
    const copied = new Set();
    for (const relativePath of RELEASE_CONTENT_FILES) {
      if (copyRegularFile(relativePath, releaseDir)) copied.add(relativePath);
    }
    for (const required of REQUIRED_INTEGRITY_FILES) {
      if (!copied.has(required)) fail(`Required release file is missing: ${required}`);
    }
    setReleasePackageVersion(releaseDir, options.tag);

    const files = {};
    for (const relativePath of RELEASE_INTEGRITY_FILES) {
      if (!copied.has(relativePath)) continue;
      const filePath = path.join(releaseDir, relativePath);
      const buffer = fs.readFileSync(filePath);
      files[relativePath] = { sha256: sha256(buffer), size: buffer.length };
    }
    const commit = options.commit || getGitCommit();
    const manifest = {
      schema: 1,
      releaseTag: options.tag,
      commit,
      files,
    };
    const manifestBuffer = writeJson(path.join(releaseDir, "release-manifest.json"), manifest);
    const buildInfo = {
      schema: 1,
      channel: "official-release",
      releaseTag: options.tag,
      commit,
      createdAt: new Date().toISOString(),
      manifestFile: "release-manifest.json",
      manifestSha256: sha256(manifestBuffer),
    };
    writeJson(path.join(releaseDir, "build-info.json"), buildInfo);
    return { releaseDir, buildInfo, manifest };
  } catch (error) {
    fs.rmSync(releaseDir, { recursive: true, force: true });
    throw error;
  }
}

function main() {
  const options = parseArgs(process.argv.slice(2));
  const result = buildRelease(options);
  console.log(`Release package created: ${result.releaseDir}`);
  console.log(`Release tag: ${result.buildInfo.releaseTag}`);
  console.log(`Integrity files: ${Object.keys(result.manifest.files).length}`);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(`[release-build] ${error.message}`);
    process.exitCode = 1;
  }
}

module.exports = { buildRelease, normalizeTag, parseArgs, RELEASE_CONTENT_FILES, RELEASE_INTEGRITY_FILES };
