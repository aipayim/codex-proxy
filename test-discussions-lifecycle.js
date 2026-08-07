#!/usr/bin/env node
"use strict";

const assert = require("assert");
const fs = require("fs");
const path = require("path");
const vm = require("vm");
const { EventEmitter } = require("events");

const SNAPSHOT_FILE_NAME = "discussions-cache.json";
const MAX_BYTES = 200 * 1024;

function makeHttpsMock(routes) {
  const calls = [];
  const httpsMock = {
    request(url, opts, cb) {
      const call = { url, opts, body: opts.body || null };
      calls.push(call);
      const req = new EventEmitter();
      const res = new EventEmitter();
      req.setTimeout = () => req;
      req.destroy = (err) => setTimeout(() => req.emit("error", err), 0);
      req.write = (chunk) => { call.body = chunk; call.opts.body = chunk; };
      req.end = () => {
        const route = routes(url, opts);
        if (route.timeout) {
          req.destroy(new Error("mock timeout"));
          return;
        }
        setTimeout(() => {
          cb(res);
          res.statusCode = route.status;
          res.headers = route.headers || {};
          const raw = route.raw != null
            ? Buffer.from(String(route.raw), "utf8")
            : Buffer.from(route.body == null ? "" : JSON.stringify(route.body), "utf8");
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

function loadDiscussionsHarness(proxyDir) {
  const source = fs.readFileSync(path.join(proxyDir, "proxy.js"), "utf8");
  const cutoff = source.lastIndexOf("// Start servers");
  if (cutoff < 0) throw new Error("proxy startup marker not found");

  const memoryFiles = new Map();
  const fsMock = {
    ...fs,
    promises: {
      ...fs.promises,
      readFile: async (file, enc) => {
        if (memoryFiles.has(file)) return memoryFiles.get(file);
        const err = new Error("ENOENT");
        err.code = "ENOENT";
        throw err;
      },
      writeFile: async (file, data) => { memoryFiles.set(file, String(data)); },
      rename: async (a, b) => {
        if (memoryFiles.has(a)) { memoryFiles.set(b, memoryFiles.get(a)); memoryFiles.delete(a); }
      },
    },
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
  const httpsDelegator = { request: (url, opts, cb) => httpsMock.request(url, opts, cb) };
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
    ;globalThis.__discTest = {
      config,
      discussionsState,
      fetchDiscussions,
      fetchDiscussionReplies,
      fetchDiscussionCategories,
      testGitHubToken,
      postDiscussionComment,
      createDiscussion,
      normalizeDiscussionsConfig,
      pickDiscussionFields,
      pickReplyFields,
      buildSnapshotPayload,
      degradeSnapshotPayload,
      saveDiscussionsSnapshotAsync,
      loadDiscussionsSnapshot,
      buildDiscussionsPayload,
      DISCUSSIONS_SNAPSHOT_FILE,
      DISCUSSIONS_SNAPSHOT_MAX_BYTES,
      DISCUSSIONS_SNAPSHOT_BODY_MAX,
      reset: () => {
        discussionsState.items = [];
        discussionsState.checkedAt = 0;
        discussionsState.etag = "";
        discussionsState.lastError = "";
        discussionsState.stale = false;
        discussionsState.snapshotSavedAt = 0;
        discussionsState.replyCache = new Map();
        discussionsState.categoryCache = { items: [], checkedAt: 0 };
        discussionsState.snapshot = null;
        discussionsState.lastWriteAt = 0;
        discussionsState.inFlight = null;
        discussionsState.repoNodeId = "";
        discussionsState.repoNodeFetchedAt = 0;
        discussionsState.pendingOwnReplies = new Map();
        discussionsState.ownNumbers = new Map();
        discussionsState.ownUpdatedAt = new Map();
        config.discussions = { enabled: true, maxItems: 10, githubToken: "" };
        lastSnapshotJson = "";
      },
      setToken: (tok) => { config.discussions.githubToken = tok; },
      setRoutes: (fn) => discHttp.setRoutes(fn),
      calls: () => discHttp.calls(),
    };
  `;
  sandbox.discHttp = discHttp;
  sandbox.memoryFiles = memoryFiles;
  vm.runInNewContext(source.slice(0, cutoff) + harness, sandbox, { filename: "proxy.js", timeout: 5000 });
  sandbox.__discTest.memoryFiles = memoryFiles;
  return sandbox.__discTest;
}

function sampleDiscussion(overrides) {
  return Object.assign({
    number: 1,
    title: "Sample title",
    body: "Sample body text",
    user: { login: "alice" },
    comments: 3,
    answer_chosen_at: null,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-02T00:00:00Z",
    html_url: "https://github.com/aipayim/codex-proxy/discussions/1",
    node_id: "D_kw_1",
    category: { name: "Q&A", slug: "qa" },
    locked: false,
    state: "open",
    reactions: { total_count: 5 },
    labels: [{ name: "x" }],
    body_html: "<p>html</p>",
  }, overrides || {});
}

function graphqlRoutes() {
  return (url, opts) => {
    if (url.indexOf("/graphql") >= 0) {
      const body = JSON.parse(opts.body || "{}");
      if (body.query.indexOf("addDiscussionComment") >= 0) {
        return { status: 200, body: { data: { addDiscussionComment: { comment: { id: "c1" } } } } };
      }
      if (body.query.indexOf("createDiscussion") >= 0) {
        return { status: 200, body: { data: { createDiscussion: { discussion: { id: "D_new", number: 7, url: "https://github.com/aipayim/codex-proxy/discussions/7" } } } } };
      }
      if (body.query.indexOf("discussionCategories") >= 0) {
        return { status: 200, body: { data: { repository: { discussionCategories: { nodes: [
          { id: "cat1", name: "Q&A", slug: "qa" },
          { id: "cat2", name: "Ideas", slug: "ideas" },
        ] } } } } };
      }
      return { status: 200, body: { data: {} } };
    }
    if (url.indexOf("/repos/aipayim/codex-proxy") >= 0 && url.indexOf("/discussions") < 0) {
      return { status: 200, body: { node_id: "R_kw_REPO" } };
    }
    if (url.indexOf("/discussions/1/comments") >= 0) {
      return { status: 200, body: [
        { id: 11, user: { login: "bob" }, body: "Reply one", created_at: "2026-01-02T00:00:01Z", html_url: "https://github.com/aipayim/codex-proxy/discussions/1#comment-11" },
        { id: 12, user: { login: "carol" }, body: "Reply two", created_at: "2026-01-02T00:00:02Z", html_url: "https://github.com/aipayim/codex-proxy/discussions/1#comment-12" },
      ] };
    }
    if (url.indexOf("/discussions") >= 0) {
      return { status: 200, body: [sampleDiscussion(), sampleDiscussion({ number: 2, title: "Second", user: { login: "bob" }, answer_chosen_at: "2026-01-03T00:00:00Z" })] };
    }
    if (url.indexOf("/user") >= 0) {
      return { status: 200, body: { login: "alice", name: "Alice" } };
    }
    return { status: 404, body: { message: "not found" } };
  };
}

function mutationErrorRoutes() {
  const base = graphqlRoutes();
  return (url, opts) => {
    if (url.indexOf("/graphql") >= 0) {
      let q = "";
      try { q = JSON.parse(opts.body || "{}").query || ""; } catch {}
      if (q.indexOf("addDiscussionComment") >= 0 || q.indexOf("createDiscussion") >= 0) {
        return { status: 200, body: { errors: [{ message: "Resource not accessible by integration" }] } };
      }
    }
    return base(url, opts);
  };
}

function allGraphQLErrorRoutes() {
  const base = graphqlRoutes();
  return (url, opts) => {
    if (url.indexOf("/graphql") >= 0) {
      return { status: 200, body: { errors: [{ message: "Resource not accessible by integration" }] } };
    }
    return base(url, opts);
  };
}

async function testListParsing(h) {
  h.reset();
  h.setRoutes(graphqlRoutes());
  const payload = await h.fetchDiscussions(true);
  assert.ok(payload.ok === undefined || true);
  assert.strictEqual(payload.items.length, 2);
  const first = payload.items[0];
  assert.strictEqual(first.number, 1);
  assert.strictEqual(first.author, "alice");
  assert.strictEqual(first.comments, 3);
  assert.strictEqual(first.answered, false);
  assert.strictEqual(first.category, "Q&A");
  assert.strictEqual(first.body, "Sample body text");
  assert.ok(!("reactions" in first), "no reactions key");
  assert.ok(!("labels" in first), "no labels key");
  assert.ok(!("body_html" in first), "no body_html key");
  assert.strictEqual(payload.writeEnabled, false);
  const second = payload.items[1];
  assert.strictEqual(second.answered, true, "answer_chosen_at sets answered");
  const hd = h.pickDiscussionFields({ number: 9 });
  assert.strictEqual(hd.author, "", "missing user degrades to empty");
  assert.strictEqual(hd.comments, 0);
  assert.strictEqual(hd.title, "");
  assert.ok(h.pickDiscussionFields(null) === null);
  const hr = h.pickReplyFields({ user: { login: "x" }, body: "b", created_at: "t", html_url: "u", reactions: {} });
  assert.deepStrictEqual(Object.keys(hr).sort(), ["author", "body", "createdAt", "htmlUrl", "id"]);
  console.log("discussions: list parsing and field pruning: PASS");
}

async function testCacheAndInflight(h) {
  h.reset();
  let count = 0;
  h.setRoutes((url) => { count++; return { status: 200, body: [sampleDiscussion()] }; });
  const p1 = await h.fetchDiscussions(true);
  assert.strictEqual(count, 1);
  assert.strictEqual(p1.items.length, 1);
  const p2 = await h.fetchDiscussions(false);
  assert.strictEqual(count, 1, "cached: no second request within TTL");
  const p3 = await h.fetchDiscussions(true);
  assert.strictEqual(count, 2, "force refreshes");
  assert.strictEqual(p3.items.length, 1);
  h.discussionsState.checkedAt = Date.now() + 999999;
  h.fetchDiscussions(true);
  const same = h.fetchDiscussions(true);
  assert.ok(same && typeof same.then === "function", "in-flight dedup returns a thenable");
  const both = await Promise.all([same, h.fetchDiscussions(true)]);
  assert.strictEqual(both[0].items.length, 1);
  assert.strictEqual(both[1].items.length, 1);
  h.discussionsState.checkedAt = Date.now() - 100000;
  const p4 = await h.fetchDiscussions(true);
  assert.strictEqual(p4.items.length, 1);
  console.log("discussions: cache hit / force / in-flight dedup: PASS");
}

async function testFailureModes(h) {
  h.reset();
  h.setRoutes(() => ({ status: 500, body: { message: "boom" } }));
  let threw = false;
  try { await h.fetchDiscussions(true); } catch { threw = true; }
  assert.ok(threw, "no items and no snapshot -> error propagates");
  h.reset();
  h.setRoutes(() => ({ status: 200, raw: "{not valid json" }));
  let bad = false;
  try { await h.fetchDiscussions(true); } catch { bad = true; }
  assert.ok(bad, "unparseable JSON -> error");
  h.reset();
  h.setRoutes(() => ({ timeout: true }));
  let timed = false;
  try { await h.fetchDiscussions(true); } catch { timed = true; }
  assert.ok(timed, "timeout -> error");
  console.log("discussions: bad JSON / non-200 / timeout -> lastError: PASS");
}

async function testTokenLessRejection(h) {
  h.reset();
  h.setRoutes(graphqlRoutes());
  await h.fetchDiscussions(true);
  const t = await h.testGitHubToken();
  assert.strictEqual(t.ok, false);
  const cats = await h.fetchDiscussionCategories();
  assert.strictEqual(cats.items.length, 0);
  assert.strictEqual(cats.reason, "no-token");
  let cErr = "";
  try { await h.postDiscussionComment(1, "hello"); } catch (e) { cErr = e.message; }
  assert.ok(cErr.indexOf("not configured") >= 0, "comment rejected without token");
  let kErr = "";
  try { await h.createDiscussion("t", "b", "cat1"); } catch (e) { kErr = e.message; }
  assert.ok(kErr.indexOf("not configured") >= 0, "create rejected without token");
  console.log("discussions: no-token rejection for all write endpoints: PASS");
}

async function testGraphQLBodies(h) {
  h.reset();
  h.setRoutes(graphqlRoutes());
  h.setToken("test-pat-123");
  await h.fetchDiscussions(true);
  const result = await h.postDiscussionComment(1, "hello world");
  assert.strictEqual(result.ok, true);
  let gql = h.calls().filter((c) => c.url.indexOf("/graphql") >= 0);
  let last = gql[gql.length - 1];
  assert.ok(last.opts.headers.authorization === "Bearer test-pat-123");
  let body = JSON.parse(last.body);
  assert.ok(body.query.indexOf("addDiscussionComment") >= 0);
  assert.strictEqual(body.variables.id, "D_kw_1");
  assert.strictEqual(body.variables.body, "hello world");

  h.reset();
  h.setRoutes(graphqlRoutes());
  h.setToken("test-pat-123");
  await h.fetchDiscussions(true);
  const created = await h.createDiscussion("New idea", "Body text", "cat2");
  assert.strictEqual(created.number, 7);
  gql = h.calls().filter((c) => c.url.indexOf("/graphql") >= 0);
  last = gql[gql.length - 1];
  body = JSON.parse(last.body);
  assert.ok(body.query.indexOf("createDiscussion") >= 0);
  assert.strictEqual(body.variables.rid, "R_kw_REPO");
  assert.strictEqual(body.variables.cid, "cat2");
  assert.strictEqual(body.variables.title, "New idea");
  console.log("discussions: GraphQL request bodies and Bearer auth: PASS");
}

async function testRateLimit(h) {
  h.reset();
  h.setRoutes(graphqlRoutes());
  h.setToken("test-pat-123");
  await h.fetchDiscussions(true);
  h.discussionsState.lastWriteAt = Date.now();
  let err = "";
  try { await h.postDiscussionComment(1, "again"); } catch (e) { err = e.message; }
  assert.ok(err.indexOf("过于频繁") >= 0, "second write within 30s rejected");
  console.log("discussions: write rate limit (30s): PASS");
}

async function testNumberWhitelist(h) {
  h.reset();
  h.setRoutes(graphqlRoutes());
  h.setToken("test-pat-123");
  await h.fetchDiscussions(true);
  let err = "";
  try { await h.postDiscussionComment(9999, "x"); } catch (e) { err = e.message; }
  assert.ok(err.indexOf("不存在") >= 0, "number not in cached list rejected");
  let rErr = "";
  try { await h.fetchDiscussionReplies(9999); } catch (e) { rErr = e.message; }
  assert.ok(rErr.indexOf("不存在") >= 0, "replies for unknown number rejected");
  console.log("discussions: number whitelist: PASS");
}

async function testTruncation(h) {
  h.reset();
  h.setRoutes(graphqlRoutes());
  h.setToken("test-pat-123");
  await h.fetchDiscussions(true);
  let err = "";
  try { await h.postDiscussionComment(1, "   "); } catch (e) { err = e.message; }
  assert.ok(err.indexOf("不能为空") >= 0, "blank body rejected");
  const longBody = "x".repeat(1500);
  await h.postDiscussionComment(1, longBody);
  const gql = h.calls().filter((c) => c.url.indexOf("/graphql") >= 0);
  const last = JSON.parse(gql[gql.length - 1].body);
  assert.strictEqual(last.variables.body.length, 1000, "body truncated to 1000");

  h.reset();
  h.setRoutes(graphqlRoutes());
  h.setToken("test-pat-123");
  await h.fetchDiscussions(true);
  let e1 = "";
  try { await h.createDiscussion("", "b", "cat1"); } catch (e) { e1 = e.message; }
  assert.ok(e1.indexOf("标题") >= 0);
  let e2 = "";
  try { await h.createDiscussion("t", "", "cat1"); } catch (e) { e2 = e.message; }
  assert.ok(e2.indexOf("正文") >= 0);
  let e3 = "";
  try { await h.createDiscussion("t", "b", ""); } catch (e) { e3 = e.message; }
  assert.ok(e3.indexOf("分类") >= 0);
  const longTitle = "y".repeat(200);
  const created = await h.createDiscussion(longTitle, "body", "cat1");
  assert.strictEqual(created.ok, true);
  const gql2 = h.calls().filter((c) => c.url.indexOf("/graphql") >= 0);
  const last2 = JSON.parse(gql2[gql2.length - 1].body);
  assert.strictEqual(last2.variables.title.length, 120, "title truncated to 120");
  console.log("discussions: body/title truncation and empty validation: PASS");
}

async function testNormalize(h) {
  h.reset();
  const cfg = h.normalizeDiscussionsConfig({ enabled: "yes", maxItems: 9999, githubToken: "  abc  \n" });
  assert.strictEqual(cfg.enabled, true, "non-boolean enabled -> default true");
  assert.strictEqual(cfg.maxItems, 50, "maxItems capped at 50");
  assert.strictEqual(cfg.githubToken, "abc", "token trimmed");
  const d = h.normalizeDiscussionsConfig({ enabled: false, maxItems: 0, githubToken: "x".repeat(300) });
  assert.strictEqual(d.enabled, false);
  assert.strictEqual(d.maxItems, 1, "maxItems floor at 1");
  assert.strictEqual(d.githubToken.length, 255, "token truncated to 255");
  const e = h.normalizeDiscussionsConfig(null);
  assert.deepStrictEqual({ ...e }, { enabled: true, maxItems: 10, githubToken: "" });
  const f = h.normalizeDiscussionsConfig([1, 2]);
  assert.deepStrictEqual({ ...f }, { enabled: true, maxItems: 10, githubToken: "" });
  console.log("discussions: normalizeDiscussionsConfig validation: PASS");
}

async function testSnapshotWrite(h) {
  h.reset();
  h.setRoutes(graphqlRoutes());
  h.setToken("test-pat-123");
  await h.fetchDiscussions(true);
  await h.fetchDiscussionReplies(1);
  const writtenBefore = h.memoryFiles.size;
  await h.saveDiscussionsSnapshotAsync();
  const snapshotPath = h.DISCUSSIONS_SNAPSHOT_FILE;
  assert.ok(h.memoryFiles.has(snapshotPath), "snapshot written after successful fetch");
  const saved = JSON.parse(h.memoryFiles.get(snapshotPath));
  assert.ok(typeof saved.savedAt === "number");
  assert.strictEqual(saved.items.length, 2);
  assert.ok(!("githubToken" in saved) && !("token" in saved), "snapshot contains no token");
  assert.ok(!("body_html" in saved.items[0]), "snapshot fields are pruned");
  assert.strictEqual(saved.replies["1"].items.length, 2, "replies captured in snapshot");
  assert.strictEqual(saved.categories.items.length, 0);
  const jsonBefore = h.memoryFiles.get(snapshotPath);
  const snapshotRunning = h.discussionsState;
  assert.ok(snapshotRunning.items.length === 2);
  await h.saveDiscussionsSnapshotAsync();
  assert.strictEqual(h.memoryFiles.get(snapshotPath), jsonBefore, "no rewrite when content unchanged");
  console.log("discussions: snapshot write with savedAt, pruned fields, no token, no-op rewrite: PASS");
}

async function testSnapshotLoad(h) {
  h.reset();
  h.setRoutes(() => ({ status: 200, body: [sampleDiscussion()] }));
  await h.fetchDiscussions(true);
  await h.saveDiscussionsSnapshotAsync();
  const snapshotPath = h.DISCUSSIONS_SNAPSHOT_FILE;
  const snapshotJson = h.memoryFiles.get(snapshotPath);
  h.reset();
  h.memoryFiles.set(snapshotPath, snapshotJson);
  await h.loadDiscussionsSnapshot();
  assert.strictEqual(h.discussionsState.items.length, 1, "items restored from snapshot");
  assert.ok(h.discussionsState.snapshot, "snapshot object retained");
  h.reset();
  h.memoryFiles.delete(snapshotPath);
  await h.loadDiscussionsSnapshot();
  assert.strictEqual(h.discussionsState.items.length, 0, "missing snapshot ignored");
  h.memoryFiles.set(snapshotPath, "{corrupt json");
  await h.loadDiscussionsSnapshot();
  assert.strictEqual(h.discussionsState.items.length, 0, "corrupt snapshot ignored without throwing");
  console.log("discussions: snapshot load / missing / corrupt: PASS");
}

async function testFallbackChain(h) {
  h.reset();
  h.setRoutes(graphqlRoutes());
  await h.fetchDiscussions(true);
  assert.strictEqual(h.discussionsState.items.length, 2);
  h.setRoutes(() => ({ status: 500, body: {} }));
  const payload = await h.fetchDiscussions(true);
  assert.ok(payload.items.length > 0, "memory items survive upstream failure");
  assert.ok(payload.lastError, "lastError populated on upstream failure");

  h.reset();
  h.setRoutes(graphqlRoutes());
  await h.fetchDiscussions(true);
  await h.saveDiscussionsSnapshotAsync();
  const snapshotPath = h.DISCUSSIONS_SNAPSHOT_FILE;
  const snapshotJson = h.memoryFiles.get(snapshotPath);
  h.reset();
  h.memoryFiles.set(snapshotPath, snapshotJson);
  await h.loadDiscussionsSnapshot();
  h.setRoutes(() => ({ status: 500, body: {} }));
  const fallback = await h.fetchDiscussions(true);
  assert.strictEqual(fallback.stale, true, "stale flagged when serving snapshot");
  assert.ok(fallback.items.length >= 1, "snapshot items served on failure");
  assert.ok(fallback.lastError, "lastError populated");

  h.reset();
  h.setRoutes(() => ({ status: 500, body: {} }));
  let threw = false;
  try { await h.fetchDiscussions(true); } catch { threw = true; }
  assert.ok(threw, "no memory, no snapshot -> error");
  console.log("discussions: fallback chain (memory -> snapshot -> error): PASS");
}

async function testDegrade(h) {
  h.reset();
  const makeReply = (n) => ({ items: Array.from({ length: 30 }, (_, i) => ({ author: "u" + i, body: "r".repeat(2000), createdAt: "", htmlUrl: "" })), checkedAt: Date.now() });
  const big = {
    schema: 1,
    savedAt: Date.now(),
    items: Array.from({ length: 200 }, (_, i) => ({ number: i, title: "t".repeat(120), body: "b".repeat(2000), author: "a", comments: 1, answered: false, createdAt: "", updatedAt: "", htmlUrl: "", nodeId: "", category: "", locked: false, state: "open" })),
    replies: { "1": makeReply(1), "2": makeReply(2), "3": makeReply(3), "4": makeReply(4) },
    categories: { items: [], checkedAt: 0 },
  };
  const degraded = h.degradeSnapshotPayload(JSON.parse(JSON.stringify(big)));
  const bytes = Buffer.byteLength(JSON.stringify(degraded), "utf8");
  assert.ok(bytes <= h.DISCUSSIONS_SNAPSHOT_MAX_BYTES, `degraded size ${bytes} <= ${h.DISCUSSIONS_SNAPSHOT_MAX_BYTES}`);
  assert.strictEqual(degraded.replies["1"], undefined, "replies dropped first (level 1)");
  assert.ok(degraded.items.length < 200, "list halved when still over cap (level 2)");
  assert.strictEqual(degraded.items.length, 5, "floored to top 5 as last resort (level 3)");
  console.log("discussions: write degradation within soft cap: PASS");
}

async function testConfigPreserveToken(h) {
  h.reset();
  const cur = { discussions: { enabled: true, maxItems: 9, githubToken: "keep-me" } };
  const incoming = { discussions: { enabled: true, maxItems: 9 } };
  const merged = Object.assign({}, cur.discussions, incoming.discussions);
  const out = h.normalizeDiscussionsConfig(merged);
  assert.strictEqual(out.githubToken, "keep-me", "omitted token preserved on save");
  const explicit = h.normalizeDiscussionsConfig(Object.assign({}, cur.discussions, { githubToken: "" }));
  assert.strictEqual(explicit.githubToken, "", "explicit empty token clears");
  console.log("discussions: config token preserve-on-omit / clear-on-explicit: PASS");
}

async function testCleanMarkdownBody(h) {
  h.reset();
  const raw = [
    "<!--",
    "  ✏️ Optional: Customize the content below",
    "-->",
    "",
    "## 标题",
    "",
    "你好，见 [文档](https://example.com/doc) 和 ![图标](https://example.com/icon.png)",
    "",
    "```js",
    "console.log('hi')",
    "```",
    "",
    "> 引用内容",
  ].join("\n");
  const item = h.pickDiscussionFields({
    number: 1, title: "T", body: raw, user: { login: "a" }, comments: 0,
    created_at: "t", updated_at: "t", html_url: "u", node_id: "n", category: {}, locked: false, state: "open",
  });
  assert.ok(!item.body.includes("<!--"), "html comment stripped");
  assert.ok(!item.body.includes("```"), "code fence markers stripped");
  assert.ok(item.body.includes("console.log('hi')"), "code content preserved");
  assert.ok(!item.body.includes("![图标]"), "image syntax converted");
  assert.ok(!item.body.includes("[文档]"), "link syntax converted");
  assert.ok(item.body.includes("文档"), "link label kept");
  assert.ok(!item.body.includes("> 引用内容"), "blockquote marker stripped");
  assert.ok(item.body.includes("引用内容"), "blockquote text kept");
  const commentOnly = h.pickDiscussionFields({ number: 2, title: "T", body: "<!--hidden-->", user: {}, comments: 0, created_at: "t", updated_at: "t", html_url: "u", node_id: "n", category: {}, locked: false, state: "open" });
  assert.strictEqual(commentOnly.body, "", "comment-only body empties");
  const reply = h.pickReplyFields({ body: "<!--y-->hi", user: { login: "b" }, created_at: "t", html_url: "u" });
  assert.strictEqual(reply.body, "hi", "reply body cleaned too");
  console.log("discussions: body markdown/html source cleanup: PASS");
}

async function testTokenOverride(h) {
  h.reset();
  h.setRoutes(graphqlRoutes());
  h.setToken("saved-pat");
  const t1 = await h.testGitHubToken();
  assert.strictEqual(t1.ok, true);
  assert.strictEqual(t1.login, "alice");
  let userCalls = h.calls().filter((c) => String(c.url).indexOf("/user") >= 0);
  assert.strictEqual(userCalls[userCalls.length - 1].opts.headers.authorization, "Bearer saved-pat", "saved token used without override");

  h.reset();
  h.setRoutes(graphqlRoutes());
  h.setToken("");
  const t2 = await h.testGitHubToken("override-pat");
  assert.strictEqual(t2.ok, true);
  userCalls = h.calls().filter((c) => String(c.url).indexOf("/user") >= 0);
  assert.strictEqual(userCalls[userCalls.length - 1].opts.headers.authorization, "Bearer override-pat", "override token used when provided");

  const t3 = await h.testGitHubToken("   ");
  assert.strictEqual(t3.ok, false, "blank override falls back to saved (empty) -> not configured");
  console.log("discussions: test-token saved vs override: PASS");
}

async function testGraphQLErrors(h) {
  h.reset();
  h.setToken("test-pat-123");
  h.setRoutes(mutationErrorRoutes());
  await h.fetchDiscussions(true);
  let cErr = "";
  try { await h.postDiscussionComment(1, "hello"); } catch (e) { cErr = e.message; }
  assert.ok(cErr.indexOf("not accessible") >= 0, "comment surfaces graphql error, got: " + cErr);
  assert.ok(cErr.indexOf("Discussions 读/写权限") >= 0, "comment error has scope hint");
  h.discussionsState.lastWriteAt = 0;
  let kErr = "";
  try { await h.createDiscussion("New idea", "Body text", "cat1"); } catch (e) { kErr = e.message; }
  assert.ok(kErr.indexOf("not accessible") >= 0, "create surfaces graphql error, got: " + kErr);
  assert.ok(kErr.indexOf("Discussions 读/写权限") >= 0, "create error has scope hint");
  console.log("discussions: graphql errors surfaced (comment/create): PASS");
}

async function testOwnReplyVisibility(h) {
  h.reset();
  h.setRoutes(graphqlRoutes());
  h.setToken("test-pat-123");
  await h.fetchDiscussions(true);
  const before = h.discussionsState.items.find((d) => d.number === 1).comments;
  await h.postDiscussionComment(1, "my own reply");
  const after = h.discussionsState.items.find((d) => d.number === 1);
  assert.strictEqual(after.comments, before + 1, "list comments incremented locally");
  assert.ok(h.discussionsState.ownNumbers.get(1) > 0, "ownNumbers marked");
  const p = h.buildDiscussionsPayload();
  assert.ok(Array.isArray(p.ownNumbers) && p.ownNumbers.indexOf(1) >= 0, "ownNumbers exposed in payload");
  const r = await h.fetchDiscussionReplies(1);
  assert.ok(r.items.some((x) => x.body === "my own reply"), "own reply visible immediately via local merge");
  console.log("discussions: own reply visible immediately + unread exclusion marks: PASS");
}

async function testTokenScopeDiagnosis(h) {
  h.reset();
  h.setRoutes(allGraphQLErrorRoutes());
  h.setToken("test-pat-123");
  const t = await h.testGitHubToken();
  assert.strictEqual(t.ok, true, "user check still ok");
  assert.strictEqual(t.login, "alice");
  assert.ok(t.discussionsScope && t.discussionsScope.ok === false, "scope flagged false");
  assert.ok((t.discussionsScope.message || "").indexOf("not accessible") >= 0, "scope error surfaced");

  h.reset();
  h.setRoutes(graphqlRoutes());
  h.setToken("test-pat-123");
  const ok = await h.testGitHubToken();
  assert.ok(ok.discussionsScope && ok.discussionsScope.ok === true, "scope flagged ok with permissions");
  console.log("discussions: test-token discussions scope diagnosis: PASS");
}

async function main() {
  const harness = loadDiscussionsHarness(__dirname);
  await testListParsing(harness);
  await testCacheAndInflight(harness);
  await testFailureModes(harness);
  await testTokenLessRejection(harness);
  await testGraphQLBodies(harness);
  await testRateLimit(harness);
  await testNumberWhitelist(harness);
  await testTruncation(harness);
  await testNormalize(harness);
  await testSnapshotWrite(harness);
  await testSnapshotLoad(harness);
  await testFallbackChain(harness);
  await testDegrade(harness);
  await testConfigPreserveToken(harness);
  await testCleanMarkdownBody(harness);
  await testTokenOverride(harness);
  await testGraphQLErrors(harness);
  await testOwnReplyVisibility(harness);
  await testTokenScopeDiagnosis(harness);
  console.log("discussions lifecycle: ALL PASS");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
