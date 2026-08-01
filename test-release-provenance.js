#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawnSync } = require("child_process");
const vm = require("vm");
const { buildRelease } = require("./build-release");

function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, encoding: "utf8", timeout: 5000 });
  if (result.error || result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed: ${(result.stderr || result.error?.message || "").trim()}`);
  }
  return String(result.stdout || "").trim();
}

function loadProxyHarness(proxyDir) {
  const source = fs.readFileSync(path.join(proxyDir, "proxy.js"), "utf8");
  const cutoff = source.lastIndexOf("// Start servers");
  if (cutoff < 0) throw new Error("proxy startup marker not found");
  const sandbox = {
    require,
    console,
    Buffer,
    URL,
    __dirname: proxyDir,
    __filename: path.join(proxyDir, "proxy.js"),
    setInterval: () => 0,
    clearInterval: () => {},
    setTimeout: () => 0,
    clearTimeout: () => {},
    process: { pid: 999999, argv: [], on: () => {}, exit: () => {} },
  };
  sandbox.globalThis = sandbox;
  const harness = ";globalThis.__releaseTest={buildUpdateStatus,getLocalBuildInfo,getDashboardHTML,setBaseline:v=>config.updateBaselineTag=v,setLatest:v=>updateCheckState.latest=v};";
  vm.runInNewContext(source.slice(0, cutoff) + harness, sandbox, { filename: "proxy.js", timeout: 5000 });
  return sandbox.__releaseTest;
}

function setLatest(harness, tag = "v1.2.4") {
  harness.setLatest({
    tag,
    name: tag,
    publishedAt: null,
    url: `https://github.com/aipayim/codex-proxy/releases/tag/${tag}`,
    notes: "",
  });
}

function copyTree(sourceDir, destinationDir) {
  fs.mkdirSync(destinationDir, { recursive: true });
  for (const entry of fs.readdirSync(sourceDir, { withFileTypes: true })) {
    const sourcePath = path.join(sourceDir, entry.name);
    const destinationPath = path.join(destinationDir, entry.name);
    if (entry.isDirectory()) {
      copyTree(sourcePath, destinationPath);
    } else if (entry.isFile()) {
      fs.copyFileSync(sourcePath, destinationPath);
      fs.chmodSync(destinationPath, fs.statSync(sourcePath).mode & 0o777);
    } else {
      throw new Error(`unexpected release entry: ${entry.name}`);
    }
  }
}

function testDashboardAdminTokenMemory(html) {
  assert.match(html, /const __adminTokenState=\(\(\)=>\{/);
  assert.match(html, /sessionStorage\.removeItem\("adminToken"\)/);
  assert.doesNotMatch(html, /sessionStorage\.(?:getItem|setItem)\("adminToken"\)/);
  assert.match(html, /window\.fetch=function\(u,o\)\{[\s\S]*?const t=__adminTokenState\.get\(\);/);
  assert.match(html, /function connectWS\(\)\{[\s\S]*?const t=__adminTokenState\.get\(\);/);
  assert.match(html, /e&&e\.code===4001[\s\S]*?__adminTokenState\.clear\(\)/);
}

function testReleaseArtifact(releaseDir) {
  const buildInfo = JSON.parse(fs.readFileSync(path.join(releaseDir, "build-info.json"), "utf8"));
  const manifest = JSON.parse(fs.readFileSync(path.join(releaseDir, "release-manifest.json"), "utf8"));
  assert.strictEqual(buildInfo.releaseTag, "v1.2.3");
  assert.strictEqual(buildInfo.channel, "official-release");
  const packageJson = JSON.parse(fs.readFileSync(path.join(releaseDir, "package.json"), "utf8"));
  assert.strictEqual(packageJson.version, "1.2.3");
  assert.ok(!Object.prototype.hasOwnProperty.call(packageJson, "scripts"));
  const packageLock = JSON.parse(fs.readFileSync(path.join(releaseDir, "package-lock.json"), "utf8"));
  assert.strictEqual(packageLock.version, "1.2.3");
  assert.strictEqual(packageLock.packages[""].version, "1.2.3");
  assert.ok(!fs.existsSync(path.join(releaseDir, "build-release.js")));
  const sourceTestFiles = fs.readdirSync(__dirname).filter((name) => /^test-.*\.js$/.test(name));
  assert.ok(sourceTestFiles.length > 0);
  for (const testFile of sourceTestFiles) {
    assert.ok(!fs.existsSync(path.join(releaseDir, testFile)), `release must not contain ${testFile}`);
    assert.ok(!Object.prototype.hasOwnProperty.call(manifest.files, testFile), `manifest must not contain ${testFile}`);
  }
  assert.ok(!fs.existsSync(path.join(releaseDir, "scripts")));
  assert.ok(manifest.files["proxy.js"]);
  assert.ok(fs.existsSync(path.join(releaseDir, "log-query-worker.js")));
  assert.ok(manifest.files["log-query-worker.js"]);
  assert.ok(fs.existsSync(path.join(releaseDir, "proxy-log-rotator.js")));
  assert.ok(manifest.files["proxy-log-rotator.js"]);
  assert.ok(fs.existsSync(path.join(releaseDir, "codex-sqlite-log-maintainer.py")));
  assert.ok(manifest.files["codex-sqlite-log-maintainer.py"]);
  assert.ok(fs.existsSync(path.join(releaseDir, "release-baseline.txt")));
  assert.strictEqual(fs.readFileSync(path.join(releaseDir, "release-baseline.txt"), "utf8"), "v1.2.3\n");
  for (const privateFile of ["config.json", "keys.json", "state.json", "proxy.log", "proxy.pid"]) {
    assert.ok(!fs.existsSync(path.join(releaseDir, privateFile)), `release must not contain ${privateFile}`);
  }

  const harness = loadProxyHarness(releaseDir);
  setLatest(harness);
  const status = harness.buildUpdateStatus();
  assert.strictEqual(status.current.source, "official-release");
  assert.strictEqual(status.current.version, "v1.2.3");
  assert.strictEqual(status.current.comparable, true);
  assert.strictEqual(status.updateAvailable, true);
  const html = harness.getDashboardHTML();
  for (const match of html.matchAll(/<script>([\s\S]*?)<\/script>/g)) new Function(match[1]);
  testDashboardAdminTokenMemory(html);
  const fallbackHtml = fs.readFileSync(path.join(releaseDir, "dashboard.html"), "utf8");
  assert.match(fallbackHtml, /restartCancelBtn/);
  assert.match(fallbackHtml, /restartForceBtn/);
  for (const match of fallbackHtml.matchAll(/<script>([\s\S]*?)<\/script>/g)) new Function(match[1]);
  const sourceRoot = __dirname.replace(/\\/g, "/");
  assert.ok(!html.includes(sourceRoot));

  fs.appendFileSync(path.join(releaseDir, "proxy.js"), "\n// provenance test mutation\n");
  const modified = loadProxyHarness(releaseDir);
  setLatest(modified);
  const modifiedStatus = modified.buildUpdateStatus();
  assert.strictEqual(modifiedStatus.current.source, "source-baseline");
  assert.strictEqual(modifiedStatus.current.version, "v1.2.3");
  assert.strictEqual(modifiedStatus.current.comparable, true);
  assert.strictEqual(modifiedStatus.updateAvailable, true);

  modified.setBaseline("v1.2.3");
  const manualStatus = modified.buildUpdateStatus();
  assert.strictEqual(manualStatus.current.source, "manual-config");
  assert.strictEqual(manualStatus.updateAvailable, true);
}

function testGitFallback(releaseDir, tempDir) {
  const gitAvailable = spawnSync("git", ["--version"], { encoding: "utf8" });
  if (gitAvailable.error || gitAvailable.status !== 0) {
    console.log("git provenance fallback: SKIPPED (git unavailable)");
    return;
  }
  const gitDir = path.join(tempDir, "git-checkout");
  copyTree(releaseDir, gitDir);
  fs.rmSync(path.join(gitDir, "build-info.json"));
  fs.rmSync(path.join(gitDir, "release-manifest.json"));
  run("git", ["init"], gitDir);
  run("git", ["config", "user.email", "release-test@example.invalid"], gitDir);
  run("git", ["config", "user.name", "Release Test"], gitDir);
  run("git", ["add", "-A"], gitDir);
  run("git", ["commit", "-m", "release test"], gitDir);
  run("git", ["remote", "add", "origin", "https://github.com/aipayim/codex-proxy.git"], gitDir);
  run("git", ["tag", "v1.2.3"], gitDir);

  const harness = loadProxyHarness(gitDir);
  setLatest(harness);
  const status = harness.buildUpdateStatus();
  assert.strictEqual(status.current.source, "git-clean-tag");
  assert.strictEqual(status.updateAvailable, true);

  fs.appendFileSync(path.join(gitDir, "proxy.js"), "\n// dirty worktree test\n");
  const dirty = loadProxyHarness(gitDir);
  setLatest(dirty);
  const dirtyStatus = dirty.buildUpdateStatus();
  assert.strictEqual(dirtyStatus.current.source, "source-baseline");
  assert.strictEqual(dirtyStatus.current.version, "v1.2.3");
  assert.strictEqual(dirtyStatus.current.comparable, true);
}

function testSourceBaseline(releaseDir, tempDir) {
  const baselineDir = path.join(tempDir, "source-baseline");
  copyTree(releaseDir, baselineDir);
  fs.rmSync(path.join(baselineDir, "build-info.json"));
  fs.rmSync(path.join(baselineDir, "release-manifest.json"));

  const harness = loadProxyHarness(baselineDir);
  setLatest(harness);
  const status = harness.buildUpdateStatus();
  assert.strictEqual(status.current.source, "source-baseline");
  assert.strictEqual(status.current.version, "v1.2.3");
  assert.strictEqual(status.current.comparable, true);
  assert.strictEqual(status.updateAvailable, true);

  setLatest(harness, "v1.2.3");
  const upToDate = harness.buildUpdateStatus();
  assert.strictEqual(upToDate.updateAvailable, false);

  const missingDir = path.join(tempDir, "source-no-baseline");
  copyTree(releaseDir, missingDir);
  fs.rmSync(path.join(missingDir, "build-info.json"));
  fs.rmSync(path.join(missingDir, "release-manifest.json"));
  fs.rmSync(path.join(missingDir, "release-baseline.txt"));
  const missing = loadProxyHarness(missingDir);
  setLatest(missing);
  assert.strictEqual(missing.buildUpdateStatus().current.comparable, false);

  const invalidDir = path.join(tempDir, "source-invalid-baseline");
  copyTree(releaseDir, invalidDir);
  fs.rmSync(path.join(invalidDir, "build-info.json"));
  fs.rmSync(path.join(invalidDir, "release-manifest.json"));
  fs.writeFileSync(path.join(invalidDir, "release-baseline.txt"), "not-a-version\n", "utf8");
  const invalid = loadProxyHarness(invalidDir);
  setLatest(invalid);
  assert.strictEqual(invalid.buildUpdateStatus().current.comparable, false);
}

function main() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "codex-proxy-release-test-"));
  try {
    const result = buildRelease({
      tag: "v1.2.3",
      outputDir: path.join(tempDir, "dist"),
      commit: "0123456789abcdef0123456789abcdef01234567",
    });
    testReleaseArtifact(result.releaseDir);
    testGitFallback(result.releaseDir, tempDir);
    testSourceBaseline(result.releaseDir, tempDir);
    console.log("release metadata and provenance: PASS");
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

try {
  main();
} catch (error) {
  console.error(`[release-test] ${error.stack || error.message}`);
  process.exitCode = 1;
}
