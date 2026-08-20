# OpenAPI Multi-Key Proxy

> Official site: [OpenAPI.im](https://openapi.im) · Author Twitter: [@C_2049s](https://twitter.com/C_2049s)

[中文完整版 (README_CN.md)](README_CN.md) · [简洁版 (README.md)](README.md)

## Core Capabilities

**Three-way protocol conversion**: This proxy automatically converts bidirectionally between the OpenAI Responses API, the Anthropic Messages API, and the OpenAI Chat Completions API, forming a 3×3 full protocol adaptation matrix. Any downstream client (Codex CLI, Claude Code CLI, Chat apps) can connect to any upstream (OpenAI, Anthropic, DeepSeek, Kimi, Qwen, Gemini, Grok, and any Chat-compatible relay) with zero configuration — same-protocol requests pass through, cross-protocol requests are converted.

**Model-aware adaptation**: Above the protocol layer, the proxy probes each upstream's actually available model list at runtime and caches it (10-minute TTL), automatically translating between the model names a downstream client sends and the names an upstream recognizes. When Claude Code sends `claude-*` and the upstream is a gpt relay → it maps to the nearest available gpt model automatically; when Codex/other terminals send `gpt-*` and the upstream is a claude relay → it maps to a claude model. Priority: the Key's `model` override field > upstream capability adaptation > passthrough as-is. When the upstream probe fails it passes through conservatively without guessing, so other clients are unaffected.

**Beyond cc-switch**:
- **A runtime proxy, not a config manager** — cc-switch is a local config switcher; this proxy is a network-layer transparent proxy that works without modifying client configuration
- **Three-way full protocol conversion** — cc-switch only supports Anthropic→OpenAI one-way; this proxy supports Responses↔Chat↔Messages mutual conversion + multi-protocol multi-pool fallback with mixed accounts
- **Intelligent multi-Key scheduling** — automatic routing based on health score, cooldown status, sliding success rate, and latency percentiles, not simple round-robin or manual selection
- **System-level resilience** — automatic lock-out, automatic recovery, discard detection, queue buffering, concurrency control — no manual intervention needed
- **Complete monitoring dashboard** — real-time dashboard, per-Key stats, traffic trends, request logs (with sparkline / error clustering / model distribution), Prometheus metrics, webhook/desktop notifications

**Provider-aware intelligent conversion**: Protocol differences between API providers are adapted automatically:
- **Alibaba Cloud Bailian (DashScope)** OpenAI-compatible endpoints support full `cache_control` passthrough (cache markers in the Messages protocol are preserved into the Chat protocol automatically), no manual configuration needed
- Bailian DeepSeek / QwQ and other deep-thinking models' `reasoning_content` field is mapped automatically to `thinking` / `thinking_delta` content blocks in Messages ↔ Chat conversion
- `reasoning_content` → `thinking_delta` in streaming, and `message.reasoning_content` → `thinking` content block in non-streaming responses, both directions
- `thinking` / `enable_thinking` parameters are auto-enabled on Bailian upstreams and stripped for incompatible standard Chat upstreams
- Provider identification is extensible: adding a new provider only requires extending the `CACHE_CONTROL_COMPATIBLE_HOSTS` list

## Directory Structure

```
codex-proxy/
├── proxy.js              # Core proxy server (includes the embedded full monitoring dashboard)
├── keys.json             # API Key list (each key can independently configure url/reset/status/remark)
├── config.json           # System config (prices, webhookUrl, notifications, groups, log, discussions, etc.)
├── state.json            # Auto-generated, persisted statistics and cooldown/discard state
├── state.json.bak        # Hourly automatic backup
├── dashboard.html        # Standalone dashboard file (shows a bootstrap hint when opened via file://)
├── discussions-cache.json # Auto-generated GitHub conversation-flow local snapshot (latest 10 + replies + categories, 200KB soft cap, safe to delete)
├── codex-proxy.service   # systemd service template (contains {{PROXY_DIR}} placeholder)
├── install.sh            # One-click install script (auto-reads its own path, replaces {{PROXY_DIR}})
├── edit-keys.sh          # CLI helper to quickly edit keys.json
├── package.json          # npm deps (ws only)
├── build-release.js      # Generates release assets with provenance and integrity metadata
├── log-query-worker.js   # Historical JSONL query and aggregation worker (non-blocking)
├── proxy-log-rotator.js  # WSL watchdog console log segment rotator
├── test-release-provenance.js # Release provenance regression test
├── test-stream-lifecycle.js # Responses / Messages stream terminal-state regression test
├── test-protocol-matrix.js # 3×3 protocol adaptation matrix (converters + stream conversion + protocol probing + buildForwardPlan) regression test
├── test-restart-lifecycle.js # Restart drain, cancel, and force-restart regression test
├── test-auto-resume-lifecycle.js # Key heartbeat idle-resume and watchdog reload regression test
├── test-resume-runner-lifecycle.js # Idle-resume runner lease, exit, and signal regression test
├── test-log-operations-lifecycle.js # Log cursor, aggregation and event disposition regression test
├── test-runtime-storage-lifecycle.js # state/JSONL/proxy.log capacity governance regression test
├── test-discussions-lifecycle.js # GitHub conversation flow (list/cache/snapshot/degradation/fallback chain/write debounce/token retention) regression test
├── logs/                 # Auto-generated per-day segmented JSONL logs and local aggregation/event sidecars
├── watchdog.sh           # Process watchdog script (for WSL without systemd), checks every 10s and auto-restarts on crash
├── start-proxy.sh        # One-click start watchdog + proxy (replaces systemctl start)
├── resume-codex.sh       # autoResume helper: creates a Windows-visible terminal via cmd.exe to run wsl commands
├── proxy.pid             # Auto-generated, records the proxy process PID (watchdog relies on it to detect liveness)
├── README.md             # English landing page
├── README_CN.md          # Full Chinese documentation
└── README_EN.md          # This file (full English documentation)
```

## New-Machine Deployment Checklist

This checklist lists every file and configuration needed for a full deployment; an AI agent can follow it step by step:

### Source Repository and Release Assets

The GitHub source repository keeps the build tooling and all regression tests, so release maintainers can run `npm test` before building. The GitHub Release asset for regular users is a runtime-only package: it does not include `build-release.js` or any `test-*.js`, and its `package.json` does not provide `npm test` or `npm run build:release`. Therefore, testing and building must be done in the source repository, then the standalone generated Release directory is uploaded.

### Files to Copy from Source

| File | Description | Install method |
|------|-------------|----------------|
| `build-release.js` | Release asset generator | Release maintainers; alongside `proxy.js` |
| `test-release-provenance.js` | Release provenance regression test | Release maintainers; alongside `proxy.js` |
| `test-stream-lifecycle.js` | Responses / Messages stream terminal-state regression test | Release maintainers; alongside `proxy.js` |
| `test-protocol-matrix.js` | 3×3 protocol adaptation matrix (converters / stream conversion / non-streaming / protocol probing / buildForwardPlan) regression test | Release maintainers; alongside `proxy.js` |
| `test-restart-lifecycle.js` | Restart lifecycle regression test | Release maintainers; alongside `proxy.js` |
| `test-auto-resume-lifecycle.js` | Key heartbeat idle-resume and watchdog reload regression test | Release maintainers; alongside `proxy.js` |
| `test-resume-runner-lifecycle.js` | Idle-resume runner lease, exit and signal regression test | Release maintainers; alongside `proxy.js` |
| `test-log-operations-lifecycle.js` | Log query, aggregation and event disposition regression test | Release maintainers; alongside `proxy.js` |
| `test-runtime-storage-lifecycle.js` | State, request-log and console-log capacity governance regression test | Release maintainers; alongside `proxy.js` |
| `test-config-persistence-lifecycle.js` | Config save/load persistence regression test | Release maintainers; alongside `proxy.js` |
| `test-restart-lifecycle.js` | Restart lifecycle regression test | Release maintainers; alongside `proxy.js` |
| `test-task-insight.js` | Task Insight session/distill regression test | Release maintainers; alongside `proxy.js` |
| `test-discussions-lifecycle.js` | GitHub conversation-flow regression test | Release maintainers; alongside `proxy.js` |
| `test-model-pricing.js` | Model-level pricing regression test | Release maintainers; alongside `proxy.js` |
| `test-model-adaptation.js` | Model adaptation (capability probing / nearest mapping / any-model fallback) regression test | Release maintainers; alongside `proxy.js` |
| `test-codex-log-maintenance.js` | Codex SQLite log maintenance regression test | Release maintainers; alongside `proxy.js` |
| `test-capacity-backoff-lifecycle.js` | Capacity/429 transient backoff regression test | Release maintainers; alongside `proxy.js` |
| `test-auto-resume-lifecycle.js` | autoResume lifecycle regression test | Release maintainers; alongside `proxy.js` |

### System Files (auto-generated by install.sh)

| File | Description | Install method |
|------|-------------|----------------|
| `config.json` | System config (prices, webhookUrl, notifications, groups, log, discussions, etc.) | Auto-created if missing |
| `keys.json` | API Key list | Auto-created if missing |
| `state.json` | Runtime statistics/cooldown state | Auto-created; deleted freely, re-created |
| `state.json.bak` | Hourly backup of state | Auto-maintained |
| `discussions-cache.json` | GitHub conversation-flow snapshot | Auto-created; safe to delete |
| `logs/` | Per-day JSONL request logs | Auto-created |
| `proxy.pid` | Proxy PID file | Auto-created on start |

### Config Initialization (install.sh auto-creates defaults)

- `config.json` — default empty config; the dashboard "System Config" writes it
- `keys.json` — the installer prompts for an API key, or you can edit it manually

### Dependency Check

- **Node.js ≥ 16** is required; the installer refuses to continue if the version is insufficient
- **Python 3** with the standard library `sqlite3` is optional, only needed for Codex SQLite log maintenance

### Optional: Multi-Port Group Helper Scripts

See the "Port Groups" section below for `codex-ask-b` / `codex-ask-c` helper scripts.

## AI Agent One-Click Installation

### 1. Environment Preparation

```bash
# Check Node.js version (needs ≥ 16)
node --version

# Clone or copy this project to any directory
# cd /path/to/codex-proxy
```

### 2. Install Dependencies + Configure Keys

```bash
# Install npm dependencies (ws only)
npm install

# Edit keys.json and fill in your API keys
#   key:     API key, must start with sk-
#   url:     relay address (http/https, each key may differ)
#   reset:   quota reset period daily / weekly / never
#   remark:  note (optional)
#   status:  optional field, active / shielded / deleted
#   priority: optional field, integer (default 0); higher = higher scheduling priority (within the same reset type)
#   models:   optional field, array, the model names this Key is allowed to handle.
#             Unset or empty matches all models (wildcard). When set, only matching model requests route to this Key.
#   model:    optional field, string. When non-empty, forces body.model to be replaced with this value when forwarding upstream.
#             Independent from models: models controls routing admission, model controls the actual model used upstream.
#   resetDay: optional field, 1-7 (1=Monday…7=Sunday). Only effective for weekly, sets which weekday at 00:00 resets.
#             Unset aligns automatically to the Key's first activation day.
#   resetHours: optional field, 1-168. Only effective for hourly, sets the reset period in hours (default 5).
#   activatedAt: auto-generated, the millisecond timestamp of first activation. Not lost after deleting state.json; preserved automatically when saving from the edit panel.
#   maxReqPerMin: optional field, overrides the global maxRequestsPerMin per-Key request cap.
#   maxTokPerMin: optional field, overrides the global maxTokensPerMin per-Key token cap.
#   group:    optional field, string, the group name (default "A"). Keys in different groups are scheduled independently by proxy servers on different ports.
#   tz:       optional field, timezone offset string like "+8"
#   timeWindow: optional field, {"start":22,"end":8}, the hour window this Key may be scheduled
#
# Example:
# [
#   {"key": "sk-xxx...", "url": "https://api.openai.com/v1",   "reset": "weekly", "remark": "Primary Key"},
#   {"key": "sk-yyy...", "url": "https://api.provider.com/v1", "reset": "daily",  "remark": "Backup quota card", "models": ["gpt-5.5", "gpt-5.4-mini"]},
#   {"key": "sk-zzz...", "url": "http://proxy.example.com:8080", "reset": "never",  "remark": "One-time", "status": "shielded"}
#   {"key": "sk-tw1...", "url": "https://api.example.com/v1", "reset": "daily",  "remark": "Off-peak Key", "tz": "+8", "timeWindow": {"start": 22, "end": 8}}
# ]
```

### 3. One-Click Install (Recommended)

```bash
bash install.sh
```

The installer automatically: installs npm dependencies → creates `config.json`/`state.json`/`keys.json` if missing → checks Node.js ≥ 16 → installs the service (systemd service or WSL watchdog boot script) → creates a `codex` wrapper script → prints a summary.

### 4. Configure Codex CLI to Use the Local Proxy

```bash
# Create or edit ~/.codex/config.toml
# Replace <PROXY_URL> with the local proxy address, e.g. http://localhost:3456/v1
```

### 5. Verification

```bash
# Start the proxy
bash start-proxy.sh

# Check status
curl http://localhost:3456/__status

# Open the dashboard
# Browser: http://localhost:3456/

# Use codex (the wrapper script automatically ensures the proxy is running)
codex "Your task"
```

## Key Scheduling Order

### Default Mode (`roundRobin: false`)

- Keys are ordered by: reset type group (`daily` first, then `weekly`, then `never`) → priority (descending) → index (ascending); in the `weekly` group, keys near their reset day are used first (see below)
- Each request picks the first available (non-cooling) key in order
- After a request, the key stays at its position (no rotation)

### Round-Robin Mode (`roundRobin: true`)

- Keys are evenly spread: each request advances to the next key in order, skipping cooling/discarded keys
- Used to smooth out upstream rate-limit pressure across keys

### Weekly Keys Sorted by Expiry Day

- Weekly keys are ordered by their `resetDay` (Monday first … Sunday last) within the weekly group, so keys whose quota resets sooner are used first
- `weeklySortBy` config: `"priority"` (priority+index) or `"expiry"` (nearest-expiry first)

## Model Routing

### Configuration

- `models` field on a key (array): routing admission — the list of model names this key accepts. Unset/empty = wildcard (accepts all)
- `model` field on a key (string): forces `body.model` to be replaced with this value when forwarding upstream
- Both are independent: `models` controls admission, `model` controls the actual upstream model

### Routing Logic

1. Client request carries a `model` name
2. Among all non-cooling keys in the group, filter by `models` admission
3. Order the filtered keys by scheduling rules; pick the first
4. If the picked key has a `model` override, replace the request's model before forwarding

### Model Override

```
# A key's configuration
#   models: ["gpt-5.5"]      → only accepts gpt-5.5 requests
#   model:  "gpt-5.6-sol"    → but forwards as gpt-5.6-sol upstream
#
# CLI sends model=gpt-5.5 → pickKey matches → forwards with gpt-5.6-sol
```

**Claude Code connecting to a gpt relay (zero config)**: put the gpt relay's keys into any group (e.g., group A port 3456) and point Claude Code at that port. The proxy returns a `claude-*` tier list for `GET /v1/models` automatically (`claude-opus-4-5` / `claude-sonnet-4-5` / `claude-haiku-4-5`, or passthroughs the relay's real claude list when a claude-capable upstream exists in the group), and maps to a gpt model on forwarding per the rules above. If you do not like the mapped result, set a `model` field on that key to force an override.

### Off-Peak Time Window

An off-peak window limits the hours a key participates in scheduling; keys outside their window are excluded. Typical use: set some keys to a night window (22:00-08:00) to avoid daytime peaks, or rotate multiple key groups across time windows to flatten upstream pressure.

Add `timeWindow` and `tz` fields to a key in `keys.json`:

```json
{"key": "sk-xxx...", "url": "https://...", "reset": "daily", "tz": "+8", "timeWindow": {"start": 22, "end": 8}}
```

| Field | Description |
|---|---|
| `tz` | Timezone offset, string, e.g. `"+8"`, `"-5"`, default `"+0"` (UTC) |
| `timeWindow` | `{"start": 22, "end": 8}`, hour-level (0-23), optional; unset = all hours |

**Window rules**:
- `start < end`: same-day window (e.g., `08:00-17:00`)
- `start > end`: overnight window (e.g., `22:00-08:00`)
- `start == end`: all hours (no limit)

**Combining with existing features**:
- Round-robin/random/batch priority: only keys inside their window participate
- cooldown/rate limit: stack on top
- Weekly reset/priority groups: unaffected

**Dashboard**:
- The card's bottom status bar shows the off-peak window (green=inside, orange=outside)
- Admin dialog: filter "In window" / "Out of window", batch "⏰ Set window" / "⏰ Clear window". The "Set window" dialog live-previews the window shape (same-day/overnight/all-hours) and the current in/out status

### Key Admin Dialog

The "Manage Keys" dialog provides two model inputs per row:
- **Models** (`models`, comma-separated): routing admission filter
- **Override model** (`model`, single value): force-replace on forwarding

Changes take effect immediately after saving, no restart needed.

## Automatic Failed-Key Switching

`forwardRequest()` switches to the next key automatically in these cases:

| Upstream status code | Handling |
|---|---|
| 401 Unauthorized | `markFailure` → switch |
| 402 Payment Required | `markFailure` → switch |
| 403 Forbidden | `markFailure` → switch |
| 429 with error body classified as quota/usage exhausted (`insufficient_quota`) | `markFailure` → cooldown for this period, switch (quota exhaustion won't recover in seconds) |
| 429 otherwise (pure transient capacity/rate) or explicit `model_at_capacity` | `markCapacityBackoff` → temporary skip; requeue to wait while no response has been submitted downstream yet |
| 5xx Server Error (not capacity) | `markFailure` → switch |
| Connection timeout / DNS error / TLS error | `markFailure` → switch |
| Stream transfer interrupted | `markFailure` → switch |
| 2xx / 3xx success | `markSuccess` → response returned along the original path |
| Other 4xx | passthrough to Codex (no switch) |

Capacity exhaustion may be returned either as HTTP `429`/5xx, or inside an already-established HTTP 200 SSE stream as `response.failed`, `error`, or protocol-equivalent events. **Transient** capacity pressure (`model_at_capacity` or `429` not classified as quota) enters a short `capUntil` backoff: no `failCode` is written, no cross-period discard. **Quota/usage exhaustion** (error body contains `quota`, `billing`, `usage limit`, `*_limit_exceeded`, `weekly/monthly/daily limit`, e.g. `WEEKLY_LIMIT_EXCEEDED` / `MONTHLY_LIMIT_EXCEEDED`) is classified as `insufficient_quota`, goes through `markFailure` into current-period cooldown, avoiding dragging every key into a futile capacity-retry storm. Pure transient rate-limit text (e.g. `rate limit exceeded`, without the quota keywords above) is still handled as transient capacity — backoff only, no cooldown. Streams already written downstream are never replayed to another key, to avoid duplicated text or tool calls; the proxy keeps the protocol-failure terminal state, and the next request uses the recovered pool.

If all keys fail, it returns `502 {"error": "All keys exhausted"}` (an older bug where it hung without responding after all keys failed is fixed).

## Cooldown, Discard, and Auto Lock

- A key returning 401/402/403, quota-class 429, non-capacity 5xx, or connection/stream transfer failure → `failCode` + `failPeriod` written to `state.json` → `inCooldown()` returns true during the period → no longer selected by `pickKey()`
- Transient capacity pressure (HTTP 429 not classified as quota, or explicit `model_at_capacity`, including SSE stream terminal state) only writes a short `capUntil` backoff — not a cross-period failure or discard basis
- The same key failing **two consecutive periods** (day/week) → auto-marked `status: "discarded"` → skipped permanently (until manually reset)
- A `reset: "never"` key is cooled permanently after one failure
- **Auto lock** (`enableAutoLock: true`): for error codes in `lockFailCodes` (default 401,403), after `lockAfterFailCount` consecutive failures (default 3) the key is auto-marked `status: "locked"` → skipped permanently (until manually unlocked)
  - The lock counter only counts consecutive same-code failures within the same period; a successful request or a different error code resets the counter
  - Lock state is written to `state.json`, not `keys.json` (so the dashboard save won't overwrite it)
  - Admin dialog shows a 🔒 lock badge + 🔓 unlock button

## Resetting Key State

Each row in the dashboard "Manage Keys" modal provides a 🔄 button that calls `POST /__reset-key`.

Backend handling:
- Clears `failCode` / `failTime` / `failPeriod`
- If the key was auto-marked `discarded` or `locked`, restores it to `active`
- Saves `state.json` + broadcasts WebSocket updates
- Does not restart the proxy; the next poll of this key attempts it normally

Use it to quickly recover cooldown/discarded/locked keys after top-up.

### Batch Test Result Reset

After a batch test, the "Reset All Keys' Status Codes" button calls `POST /__apply-test-result` and syncs per the **actual test result** of each key:

| Test result | Action |
|---|---|
| 200 success | clears `failCode` (equivalent to resetting cooldown; usable next time) |
| 429/401/403 and other failure codes | calls `markFailure()` writing the correct `failCode`; key enters cooldown |
| network error (no status code) | skipped; key state unchanged |

Unlike the old "reset everything" behavior, the new version keeps the real cooldown state of failed keys, avoiding mistakenly releasing rate-limited keys.

## Manage Dialog

From `http://localhost:3456/` click the "Manage Keys" button to open the manage dialog, which provides:

- **View**: full key list (including shielded/soft-deleted), masked display of ID, status, remark, address
- **Add/Delete**: add new keys, soft delete (`status="deleted"` kept in JSON)
- **Shield/Unshield**: 🔇 shield (excluded from scheduling), 🔓 unshield
- **Reset cooldown**: 🔄 clears cooldown/discard state
- **Search/filter**: real-time search by ID/remark/address
- **Remark toggle**: click the **Remark 🔄** header to toggle between "edit remark" and "show first activation + duration"; auto-saves the current edit before switching
- **Sort**: dropdown for "default order" / "by reset day (Mon→Sun)" / "first activation (early→late)" / "usage duration (long→short)" / "by group"
- **Status-code filter**: type `401` etc. to filter keys by a specific failure code or last-response status code (failed keys match failCode, available keys match lastStatus)
- **Status filter**: dropdown for All/Available/Cooling/Discarded/Locked/Shielded/Activation duration/Last failure/Last response/Weekly reset day
- **Activation-duration filter**: after choosing "activation duration" a "X days" input appears, filtering keys activated ≥ X days ago. Combinable with the status-code filter (e.g. `429` + `30` days = keys with failCode 429 activated over 30 days ago)
- **Last-failure filter**: after choosing "last failure" a "X days" input appears, filtering keys whose last failure was ≥ X days ago (e.g. last failure `30` days = failed 30 days ago and not recovered)
- **Last-response view**: choosing "last response" switches the table to a compact view (9 columns) showing every key's last-request status code (colored: 200=green/429=yellow/4xx=orange/5xx=red/network error=red/null=gray), last response time (XdXh), and response model. Falls back to `failCode` when `lastStatus` is empty (so every key shows a status). Available keys also show a green 200 badge in the default view. Columns are sortable (asc/desc). Combine with "🧹 Clean failures" to batch-shield long-failing keys with one click
- **Weekly reset-day filter**: after choosing "weekly reset day" a day dropdown appears (All/Auto/Mon-Sun), filtering keys with the given reset day. Combinable with other conditions (e.g. `401` + `Monday` = failCode 401 with reset day Monday)
- **Off-peak window filter**: dropdown "In window" / "Out of window" filters keys currently inside/outside their window
- **Off-peak window settings**: check keys then click "⏰ Set window" to open a form setting timezone (UTC-12~UTC+12) and window (start/end hour); the dialog live-previews the window shape (same-day/overnight/all-hours) and current in/out status. "⏰ Clear window" removes the window for selected keys (equivalent to all-hours)
- **Off-peak hover**: hovering a key row with a window set shows the full window info (timezone + range + current status), keys without a window show nothing
- **Hide shielded**: the 🙈 button hides all "shielded" keys at once (default on), convenient for batch operations on non-shielded keys. State persists across modal opens. Click 🙉 again to restore
- **Count display**: live `total X, filtered Y`, plus a separate shielded count
- **Auto-group**: auto-groups/folds by remark prefix (the first segment split by Chinese/English comma or space)
- **Collapse/expand all**: the 📂 button folds or expands all groups
- **Drag-sort**: drag rows to reorder, auto-saved to keys.json
- **Priority setting**: each row has a "priority" number input; higher = more scheduling priority (within same reset-type group)
- **Select-all + batch ops**: batch reset / batch shield / batch delete / 🧹 clean failures (auto-checks long-failing keys then batch-shields)
- **Batch import**: the 📋 button opens a multi-line textarea, one key per line, format `sk-xxx URL [reset type] [priority] [group] [remark]`; URL is required, duplicate keys allowed
- **Single key test**: 🔍 calls `GET /v1/models` to test connectivity, returns model names + latency
- **Batch test**: check multiple keys → 🔍 batch test → the dashboard shows each key's result live (success/fail) → tested-ok keys can be reset and reused with one click
- **Override model**: each row's "override model" input force-replaces `body.model` on forward. Works independently from "models" (routing admission)
- **Group management**: each row's "group" input sets the key's group (default A); supports batch migration to a target group
- **Reset all keys' status codes**: after batch testing, syncs state per each key's actual result (200 success → clear cooldown; 429 failure → write 429 and enter cooldown) rather than resetting everything to available
- **CSV export (in dialog)**: 📥 check keys → 📥 export → choose fields (Key/URL required; reset type/priority/group/remark/models/override model/reset day/timezone/window optional), generates a BOM-prefixed CSV file
- **Unsaved-change reminder**: prompts to save when closing the dialog after edits, avoiding accidental data loss
- **Group status-code stats**: each group header shows the key count + per-status-code counts (e.g. `▼ remark (12) 200×8 429×3`), colors matching the table; clicking a status badge filters and auto-checks all keys in that group with that code (badge border changes + cancel button appears); filtering "weekly reset day" auto-groups by Mon-Sun

## Monitoring Dashboard

`http://localhost:3456/` embeds a full monitoring dashboard:

### Interface Language

The Dashboard supports Chinese and English. Click the language button in the top-right corner to switch; the choice is stored in the current browser's `localStorage` and reused on later visits. Chinese is the default. Language switching only affects the current browser interface and does not require a proxy restart.

If you modify Dashboard or i18n code in `proxy.js`, restart the running proxy process and refresh the browser. A running Node.js process does not automatically load source changes.

### Top Summary
Available/total, cooling, 🔒 locked count, concurrent requests, total traffic, total requests, health score, estimated cost

### Sort/Filter/Search/Batch
- Sort: default / by expiry (nearest→farthest) / first activation (early→late) / usage duration (long→short) / health score / avg latency / 5-minute success rate / by group
- Filter: all / available / cooling / discarded / 🔒 locked / shielded / activation duration / weekly reset day (combinable with the status-code filter)
- Reset filter: daily / weekly / every N hours / never expires (combinable with status filter); choosing "weekly" allows further filtering by weekly reset day: all / Mon-Sun / auto (no `resetDay`)
- Status-code filter: type `401` etc. to filter keys by a specific failure code or last-response status code
- Search: ID / remark / address
- Live filtered count: `showing X / Y`
- Batch: check cards → batch action bar appears (with "select all" / "clear all") / batch reset / batch shield / ⚡ priority use (use the checked keys one by one, restore normal after) / ⭕ priority round-robin (round-robin among checked keys, restore normal when all cooled) / 🎲 random round-robin (Fisher-Yates shuffle, each key exactly once per round then reshuffle)
- **CSV export (main dashboard)**: ⬇ export CSV → field-selection window → default exports all keys currently visible on the page (after search/filter/group conditions), fields selectable (Key/URL required; config fields: reset type/remark/group/priority/models/override model/reset day/timezone/window; stats fields: status/failCode/requests/success/fail/input-output bytes/avg duration/health score/cost), generates a BOM-prefixed `openapi-export-YYYYMMDD.csv`

**Batch clean expired keys example**: status code `429` → status filter "activation duration" → `30` → "select all" → "batch shield" → save

### Trend Charts
24h / 7d / 30d switch, hourly bars, X-axis label density auto-adapts. Shielded keys' traffic is also included in the trend charts. Click a tab to switch trend mode, 8 modes total:

| Label | Description |
|---|---|
| 📊 Models | Stacked bar chart, per-model request share colored by model, top 8 models shown, the rest grouped into "Other", default view; legend uses the `requested model (actual model)` composite label (e.g. `claude-fable-5 (gpt-5.6-sol)`), single name when not adapted |
| 📊 Traffic | traffic trend in bytes, hover for up/down bytes and per-key detail |
| 📈 Count | request-count trend |
| 💚 Health | stacked bar chart colored by 200 (green)/4xx (yellow)/5xx (red)/failed (gray), reflecting upstream HTTP status distribution |
| 🔺 Upstream | stacked bar chart grouped by key's upstream domain, hover for domain + #Key list + counts, top 8 domains, rest into "Other" |
| 🔻 Downstream | stacked bar chart grouped by downstream client app (request User-Agent), hover for app + counts, top 8 apps, rest into "Other"; stream terminal details live in the log/CSV stream fields |
| 💰 Cost | hourly estimated cost (USD), hover for exact value |
| ⏱ Latency | hourly average latency, hover for value + request count |

### Key Cards
Masked display (click to toggle plaintext), reset-type badge (weekly keys additionally show the specific weekday/auto), concurrency badge, batch-priority badge, health-score progress bar, collapse button, cooldown countdown, stat metrics (requests/traffic/latency/P50-P95-P99/sliding success rate/cost), first-activation time + elapsed, daily/hourly detail, failure-code hover Chinese meaning, last-failure time, active-key glow highlight, locked-key purple mark, bottom status bar off-peak window (green=in window, orange=out of window)

### Status Bar Quick Actions
- 🔍 test connectivity
- 🔄 reset cooldown/discarded/locked
- 🔇 shield this key

### One-Click Collapse
The 📂 button collapses/expands all cards

### Log Viewer
Opening the log does not read the entire log file synchronously. The default "live view" returns the latest 50 records from memory first; if the proxy just restarted and memory records are insufficient, `log-query-worker.js` asynchronously reads back up to 100 saved log records, deduplicates them with memory records, and shows the latest 50. So after a restart the log still shows the saved history tail, labeled "history tail". This read does not depend on the "log file" switch: even if new files are no longer written, old JSONL can still be queried and exported if present. The dashboard also shows discovered/scanned historical file counts; when the Worker is unavailable it explicitly notes that only current memory records are shown. The table only creates currently visible rows; WebSocket is subscribed only while the log window is open, with live records batch-refreshed every 350ms.

The default log page shows the latest 50 records; click "Browse history" to switch to a paginated history view, using previous/next pages with cursors, and return to the latest view anytime. Filtering, specifying a time range, or exporting also enters history mode. Leaving the time range empty queries all available time, not treated as Unix time 0. Historical JSONL is scanned backwards in chunks by `log-query-worker.js`; the main proxy does not read whole files or count total lines; each page is up to 100 records via cursor pagination, with a 64MB/30-second boundary guard. Closing the window or starting a new query cancels the old query.

Stat cards, the latest-30-minutes trend, model distribution, and error distribution come from server-side minute/day aggregations, not the current table page. Aggregations live in `logs/.log-summary.json`; when first missing, the background Worker rebuilds them automatically, or you can click "Rebuild summary". Day aggregations crossing natural day boundaries are marked approximate and never block proxy requests.

Filtering supports key, status code (incl. `4xx`/`5xx`), model, upstream domain, path, group, full-text, and time range. The table keeps requests and runtime events; clicking a row expands the full stream terminal, URL, duration, and bytes detail. CSV/JSONL history export also goes through the Worker, up to 2000 records.

The log window has a built-in "runtime events" center. It monitors upstream, model, path, and group for failure bursts, stream-terminal failures, and optional P95 latency degradation; you can acknowledge, silence, or enable desktop/webhook notifications. "Refresh events" shows refreshing/success-time/failure status. For group events you can manually pause a group for 1-60 minutes; during the pause, new proxy requests to that group return `503`, and it can be resumed anytime. This never auto-modifies keys, never auto-restarts the proxy, and pause state is not preserved across proxy restarts.

`GET /__logs` returns `{ entries, stats, overview, mode, hasMore, nextCursor, truncated }`: `recent` without filters, padding with the saved log tail when memory is insufficient; `history` with filters/cursor/time range. The `recent` response additionally carries `source`, `historyChecked`, `historyUnavailable`, `historyFilesAvailable` and `historyFilesScanned`; the `history` response carries `filesAvailable` and `filesScanned`. These fields explain whether saved history is included, whether the tail check finished, whether the Worker is temporarily unavailable, and the count of matching log files found/actually scanned. `overview` is the server-side aggregation; `truncated=true` means the page hit the scan boundary — narrow the filter or continue with time/dimension filters.

### Runtime Data Governance

Runtime files no longer grow unboundedly. Governance runs inside the proxy process without closing listening ports, restarting the watchdog, or pausing in-flight transfers:

| File | Default policy | Notes |
|---|---|---|
| `state.json` | hourly buckets 35 days, daily buckets 180 days, file cap 32 MiB | expired buckets and the oldest complete time buckets are trimmed on overflow; saves use temp-file atomic replace, with an hourly `state.json.bak`. Backups are only for corruption recovery, not trend stats. |
| `logs/YYYY-MM-DD*.jsonl` | keep 7 days by day, 256 MiB total across segments, 16 MiB per segment | same-day overflow produces numbered segments; day-based and capacity-based cleanup both apply. `logRetentionDays=0` only disables day-based deletion; the capacity cap still applies. A single record over 512 KiB is truncated to bounded diagnostic fields or dropped. |
| `logs/.log-summary.json` | 8 MiB | keeps only recent aggregation buckets; it is a stats acceleration cache, rebuildable by the Worker when missing, not raw logs. |
| WSL `proxy.log` | current segment 10 MiB, 5 archives kept | `proxy-log-rotator.js` writes via a pipe and rotates into `proxy.log.1` etc., config re-read every 10s. Old oversized single files are migrated to a bounded tail on first start. systemd deployments use journald, not this file. |
| Codex `logs*.sqlite` (optional) | off by default; when 2048 MiB reached, keep the latest 12 hours | only the main `.sqlite` under the current proxy user's `~/.codex/` is allowed. Checks the main + `-wal` combined size; at most 5 short 1000-row transactions per round, yielding for 1s when the DB is busy and retrying next cycle. |

Clicking "System Config" save immediately runs one state compaction and one request-log cleanup; the new WSL console log capacity value takes effect within ~10 seconds. The rotator is a required runtime file for the WSL watchdog — without it the watchdog refuses to start the proxy unboundedly rather than appending to a growing `proxy.log`.

These operations are not equivalent to restarting the proxy. If the running instance has not yet loaded this source change, it must be manually restarted in a suitable maintenance window to use the new code; a restart itself may interrupt active streams and cannot be used to safely "pause and resume" arbitrary Codex CLI tasks.

#### Codex SQLite Log Maintenance

After enabling it in "System Config", the proxy first detects the DB path, regular file attributes, and the `logs(id, ts)` structure; if detection fails the enabled config cannot be saved. Example path field: `/root/.codex/logs_2.sqlite`. Pasting a Windows Explorer path like `\\wsl.localhost\Ubuntu\root\.codex\logs_2.sqlite` or `\\wsl$\Ubuntu\root\.codex\logs_2.sqlite` is converted to `/root/.codex/logs_2.sqlite` by the dashboard and server before validation; other Windows paths, symlinks, `-wal`/`-shm` auxiliary files, and files outside `~/.codex/` are rejected.

After reaching the configured "main + WAL" capacity threshold, the background standalone process deletes only records whose `ts` is older than the retention period. It relies on Python 3's standard-library `sqlite3`, requiring no new npm native dependencies; saving with Python 3 missing fails explicitly. If the DB is busy when saving the enabled config, the save is rejected with a retry hint to avoid applying an unverified config; an already-saved background task hitting a busy state shows "DB busy, skipped" and waits for the next cycle without forcing a write lock. It never restarts/pauses the proxy, watchdog, or a running Codex CLI; "Check now" and "Clean now" only use already-saved and validated config.

"Check now" is a read-only check: returns main/WAL capacity, current total, and whether the trigger threshold is reached; deletes nothing. "Clean now" actually deletes, then cleans by trigger capacity/retention and runs `VACUUM` to physically shrink the DB file; for safety it only runs when Codex is idle (in-flight = 0, queue = 0, and ≥ 60s since the last request), otherwise it refuses with "Codex is still in use". `VACUUM` needs temporary disk space roughly equal to the DB size, takes time depending on size, and only triggers when records were actually deleted.

Since SQLite usually leaves pages for reuse by later writes after deleting rows and the physical file does not necessarily shrink immediately, the scheduled background maintenance **does not proactively run `VACUUM`, checkpoint, or delete WAL/SHM files**; when physical compaction is really needed, wait for the Codex CLI to stop and stay silent for 60s then use "Clean now", or handle it in a manual maintenance window.

### Key Management
Add/edit/delete, shield/unshield, soft delete (`status="deleted"` kept in JSON), reset cooldown, set weekly reset day (Mon-Sun or auto), search/group/drag-sort, select-all batch ops, batch CSV import, single-key connectivity test
### System Config

Webhook URL, price params, desktop notification/sound switches, 🔄 auto-recover cooldown keys (interval/fixed/fast three independent modes), failure-code list, discard detection toggle, 🔁 round-robin, 📅 weekly keys sorted by expiry day, 🧬 autoResume idle auto-resume, project list (name/WSL path/start command/resumeMode/sessionId dynamic add-remove), cmd.exe path, 🔒 auto-lock threshold and monitor codes, log file/retention days/detail level, runtime file capacity/retention policy (JSONL, `state.json`, WSL `proxy.log`), 🗄 Codex SQLite log maintenance (switch, path detection, capacity threshold, retention, cycle and immediate check), log incident rules (fail/stream-fail/optional latency thresholds and default silence time), ⏱ other-protocol stream max duration (default 30 min) and Responses/Messages coding-stream total duration (default unlimited)/upstream idle timeout (default 90 min)/no-progress watchdog (default 15 min), 🔐 admin token (optional; admin endpoints require Bearer auth, dashboard prompts for input), 🔌 port group management (dynamic add/remove/modify ports), 🔄 restart-proxy button (fullscreen progress, drain, cancel, force confirm after 30s, watchdog relaunch and new-instance readiness), ⬆ GitHub Release update check (official release package / clean official tag / source baseline file auto-identification, custom builds can set a manual baseline), 💬 GitHub interaction (conversation flow: enable switch, display count, optional GitHub token)

> **GitHub interaction (conversation flow) config fields** (`discussions` section of `config.json`, maintained in dashboard "System Config → 💬 GitHub interaction"):
> - `enabled` (bool): master switch for the conversation-flow panel, default `false`
> - `maxItems` (1~50, default 10): list item count
> - `githubToken` (optional): GitHub token, needs Discussions read/write permission on the target repo (fine-grained PAT: Repository permissions → Discussions = Read and write; classic token: check the `repo` scope). Stored only in local `config.json`, masked in the dashboard, never leaked in plaintext (`GET /__config` returns `hasDiscussionsToken`); omitting the key on save keeps the old value, passing an explicit empty string clears it
> - Without a token the dashboard is read-only (`writeEnabled=false`), all write endpoints return 401 `missing_github_token`
> - This section only affects the conversation flow, not the proxy forwarding main path

## API Endpoints

> All `/__*` admin endpoints and `/metrics` require `Authorization: Bearer <token>` when an admin token is set (constant-time comparison, empty token rejected). WebSocket connections must carry `?token=<token>` in the URL. The Dashboard prompts for the token on first open; the token is kept only in the current page's memory, re-entered after refresh/close. First load after upgrade clears old token from the previous browser session storage. Without a token, the Dashboard works normally.

| Endpoint | Method | Description |
|---|---|---|
| `/` or `/dashboard` | GET | dashboard HTML |
| `/__status` | GET | JSON status (full metrics for all keys) |
| `/__keys` | GET | reads keys.json (enriched `_locked`/`_failCode`/`_failTime`/`_activatedAt`/`_available`/`_lastStatus`/`_lastTime`/`_lastModel`/`_inTimeWindow` fields) |
| `/__keys` | PUT | writes keys.json (auto-reload; auto-clears stale failCode caused by reset/resetDay changes) |
| `/__config` | GET | reads config.json |
| `/__config` | PUT | writes config.json (auto-reload) |
| `/__codex-log-maintenance/check` | POST | detects candidate Codex SQLite path and `logs(id, ts)` structure; does not write config or delete logs. Body `{"codexLogMaintenance":{"dbPath":"/root/.codex/logs_2.sqlite"}}` |
| `/__codex-log-maintenance/run` | POST | read-only immediate check of the saved-and-enabled Codex SQLite maintenance config (capacity/validity), deletes nothing; does not accept arbitrary paths, does not restart proxy or Codex CLI |
| `/__codex-log-maintenance/clean` | POST | immediate cleanup: only when Codex is idle (in-flight/queue 0 and silent 60s) deletes stale records per trigger capacity/retention and `VACUUM`s to shrink the file; 409 `database_active` when busy |
| `/__reset-key` | POST | resets a key's cooldown/discard state (`{"idx": 1}`) |
| `/__apply-test-result` | POST | applies batch test results: `{"idx":1, "failCode":429}` → markFailure; `failCode=null/200` → clear cooldown (`{"idx":1, "failCode":null}`) |
| `/__test-key` | POST | single-key connectivity test (`{"key":"sk-...","url":"https://..."}`), returns `model` (comma-separated available model list) and `modelCount` |
| `/__patch-key-status` | POST | modifies key status (`{"idx":1,"status":"shielded"}`) |
| `/__patch-key` | POST | modifies key config (`{"idx":1,"tz":"+8","timeWindow":{"start":22,"end":8}}`); `timeWindow:null` clears the window |
| `/__boost-batch` | POST | batch priority: `{"mode":"use","idxs":[1,3,5]}` (use one by one) or `{"mode":"roundrobin","idxs":[1,3,5]}` (round-robin) or `{"mode":"random","idxs":[1,3,5]}` (random) or `{"mode":""}` (cancel) |
| `/__restart` | POST | returns `202` then enters restart drain: rejects queued requests, pauses new API requests, waits for in-flight requests to finish, then exits and is relaunched by the watchdog |
| `/__restart-status` | GET | restart progress: returns instance ID, start time, phase (`ready`/`draining`/`stopping`), in-flight and queued request counts, plus `restartId`, `canCancel`, `canForce`, `forceAvailableInMs`; polled by the Dashboard, no sensitive info |
| `/__restart/cancel` | POST | only cancelable while `draining`; restores new-request admission but does not requeue already-rejected `503` requests; `200` on success, `409` in other phases |
| `/__restart/force` | POST | force-exit only after `draining` for ≥ 30s; interrupts active streams and in-flight requests, `202` on success, `409` in other phases or before the wait elapses |
| `/__update-status` | GET | queries the latest official GitHub Release of `aipayim/codex-proxy`; server caches 1h, supports `?refresh=1` for a manual recheck (≥ 60s apart). Compares official release package, clean official Git tag, or source baseline file (`release-baseline.txt`); custom builds only compare when a valid manual baseline tag is configured; never downloads or executes remote code |
| `/__auth_check` | GET | checks whether an admin token is configured, returns `{configured: true/false}` (never exposes the actual token) |
| `/__config` `_groupAction` | PUT | port group management: `{"_groupAction":"addGroup","_groupName":"B","_groupPort":3457}` or `"removeGroup"`/`"setGroupPort"`/`{"_groupAction":"toggleGroup","_groupName":"B","_groupEnabled":false}` |
| `/__test_port?port=3457` | GET | checks whether a group port is running (queries the in-memory `servers` registry) |
| `/__keys` `_batchGroup` | PUT | batch group migration: `{"_batchGroup":"B", …full keys array…}` |
| `/__logs` | GET | log query. No filter defaults to the latest 50 records, padded with saved history tail by the Worker when memory is insufficient; filters, time range, or `cursor` enter Worker history mode. Supports `key/status/model/upstream/path/group/q/since/until/limit/cursor`, status supports `4xx`/`5xx` wildcards; returns a cursor instead of `offset`/total rows |
| `/__export-logs` | GET | Worker history export (`?date=2026-07-10&key=11&status=502&model=gpt-5.6-sol&format=csv` or `jsonl`), up to 2000 records |
| `/__log-overview` | GET | server-side minute/day aggregation, supports `since/until`, returns trends, dimension distributions, stats, and current runtime events |
| `/__incidents` | GET | log events, temporary group pauses, and aggregation rebuild status |
| `/__incident-action` | POST | manual log-event disposition: `acknowledge`, `snooze`, `pause_group`, `resume_group`; pause only allowed for valid groups, max 60 minutes |
| `/__logs/rebuild-summary` | POST | rebuilds historical day aggregation in the background Worker; returns `202`, non-blocking |
| `/__export` | GET | CSV export of statistics report |
| `/__pathstats` | GET | per-path/model request distribution |
| `/metrics` | GET | Prometheus-format metrics |
| `/responses` | POST | native Responses passthrough; on `stream:true` preserves upstream bytes and checks `response.completed`, re-sending one `response.failed` if the upstream ends early |
| `/v1/responses` | POST | **full protocol adaptation**: accepts Responses API requests; native Responses upstream → passthrough, otherwise auto-converts to Chat/Messages format and forwards, converting the response stream (or non-stream) back to Responses |
| `/v1/messages` | POST | **full protocol adaptation**: accepts Messages API requests; native Messages upstream → passthrough, otherwise auto-converts to Chat/Responses format and forwards, converting the response back to Messages; only sends `message_stop` after the upstream clearly reaches a terminal state |
| `/v1/chat/completions` | POST | **full protocol adaptation**: accepts Chat Completions requests; native Chat upstream → passthrough, otherwise auto-converts to Messages/Responses format and forwards, converting the response back to Chat |
| `ws://localhost:3456/?token=<token>` | WS | WebSocket real-time push (carry token param in URL when an admin token is set) |
| `/__discussions` | GET | GitHub conversation flow: latest `maxItems` Discussions list (read-only, anonymous OK); `?number=n` includes that thread's replies. Server 60s cache + snapshot fallback; failures return `lastError` plus cached/snapshot data, not affecting the proxy main flow |
| `/__discussions/categories` | GET | thread categories (GraphQL, needs configured token; empty without one) |
| `/__discussions/test-token` | POST | tests connectivity with the saved GitHub token (`GET /user` + Discussions read-only probe), returns `{ok, login, discussionsScope}`; **only validates token validity and read permission — write permission for publishing/replying is determined by actual publishing**. Pass `{"token":"..."}` for a one-time test of a new value (not saved) |
| `/__discussions/comment` | POST | publishes a public comment on a thread: `{"number": 1, "body": "..."}`; needs a configured token, 30s debounce, body max 1000 chars |
| `/__discussions/create` | POST | creates a new thread: `{"title": "...", "body": "...", "category": "<category id>"}`; needs a configured token, title max 120, body max 1000 |

> **Conversation flow (GitHub interaction)**: the Dashboard "💬 Conversation flow" panel shows recent activity of the `aipayim/codex-proxy` repo. Its positioning is "recent-activity reminder + light reading + quick participation", **not a full Discussions client** — deep browsing/history search links out to the GitHub official pages (each entry has "Open on GitHub ↗"). Without a configured GitHub token it is view-only; with one you can comment, reply, and create new threads directly from the panel (content is **publicly published** to GitHub Discussions). The token is stored only in local `config.json`, masked in the dashboard; reads go through a 60s cache well within anonymous quotas; unread reminders poll at a low 60s frequency (**your own comments/threads are not counted as unread**). `discussions-cache.json` is a local on-disk snapshot (latest 10 + expanded replies + categories, 200KB soft cap), **runtime data, safe to delete**, auto-rebuilt after restart or network recovery. **Permission notes**: "Test connectivity" only validates token validity and read permission — GitHub provides no read-only way to predict write permission; if publishing/replying reports `Resource not accessible by personal access token`, go to GitHub and set the token's Discussions permission to Read and write (or add the `repo` scope to a classic token). **Security note**: commenting/creating threads are write endpoints; if you expose the admin port to a LAN/public network, you must also set an "admin token".

### Protocol Conversion Details

The protocol conversion layer lets any downstream client connect to any upstream model, forming a 3×3 full protocol adaptation matrix:

| Downstream client | Request path | Conversion direction | Supported upstreams |
|---|---|---|---|
| Codex CLI | `/responses` | native Responses passthrough + terminal protection | native Responses upstream |
| Codex CLI | `/v1/responses` | Responses → Chat / Messages → Responses | any Chat / Messages / Responses upstream |
| Claude Code CLI | `/v1/messages` | Messages → Chat / Responses → Messages | any Chat / Messages / Responses upstream |
| Chat client | `/v1/chat/completions` | Chat → Messages / Responses → Chat | any Chat / Messages / Responses upstream |

- Upstream protocol auto-detection: static judgment from `url` in `keys.json` (`api.openai.com`/`api.ofox.ai` = native Responses, `api.anthropic.com` = native Messages, everything else = generic Chat), plus dynamic probing for unknown relays (`/v1/models` available → Chat; otherwise probe `/v1/messages`, then `/v1/responses`). Dynamic results are written to a dedicated protocol cache (24h TTL) decoupled from the model-list cache (success 10min / failure 30s): even if a relay's `/v1/models` probe fails, already-identified Messages/Responses protocols are not wrongly downgraded back to Chat. A background task re-probes upstreams every 10 minutes deduplicated by URL (multiple keys in the same pool trigger one probe); new/changed keys take effect without restart
- The `/v1/chat/completions` multi-protocol pool judgment also uses dynamic protocol: if any account in the group (static whitelist or dynamic probe) is identified as a Messages/Responses upstream, Chat→Messages / Chat→Responses conversion fallback is enabled automatically, no need to mark it explicitly on `url`
- Same-protocol upstreams pass through first (byte passthrough / terminal protection); conversion only happens cross-protocol to avoid needless rewriting; protocol pool order: downstream Chat → `chat, messages, responses`; downstream Messages → `messages, chat, responses`; downstream Responses → `responses, chat, messages`
- Any key failure auto-switches to the next protocol pool's downstream key retry; `#N` forced key, boost/round-robin, cooldown and capacity requeue still work
- Conversion supports **streaming and non-streaming**: cross-protocol streaming forces `stream:true`, responses rewritten through SSE converters; non-streaming requests go through one-shot JSON response converters
- Conversion fields: text, system/instructions, tool definitions and call chains (`tool_use`↔`function_call`), thinking content (`thinking`↔`reasoning`↔`reasoning_content`), images, usage stats

#### Responses / Messages Stream Terminal States and Coding-CLI Disconnect Debugging

For `Responses → Chat → Responses` conversion, the proxy only emits downstream `response.completed` and the final `[DONE]` after the upstream SSE explicitly sends `[DONE]`. Even when the upstream puts `data: [DONE]` at the connection end without a trailing newline, it is recognized as a normal completion.

Native `/responses` streams are never converted or rewritten. The proxy parses SSE frames side-channel and only treats an upstream `response.completed` as success; `[DONE]` itself does not equal Responses success. If the upstream EOFs before `response.completed`, closes, is aborted, has an SSE error, idles out, or hits the configured total duration, the proxy keeps the raw bytes received and re-sends one `response.failed` — never fakes `response.completed` or `[DONE]`. When the upstream already sent `response.failed` or `response.incomplete`, no duplicate failure event is injected.

The `/v1/messages` Chat→Messages conversion likewise does not disguise truncation as completion: only after the upstream Chat SSE explicitly sends `[DONE]` does the proxy output one `message_delta` and one Anthropic `message_stop`. On EOF, close/abort, SSE error, idle timeout, or total-duration arrival, received deltas are kept then only one Anthropic `event: error` is appended — no `message_stop`. The reverse Messages→Chat conversion also outputs a single Chat `data: [DONE]` only after a real `message_stop`; abnormal termination returns an OpenAI-compatible SSE error, never a fake `[DONE]`.

System config separates normal protocol streams from coding-protocol streams: `streamLifetime` remains the max duration for other protocol streams (default 30 min); for backward compatibility the config keys are still named `responsesStreamLifetime` / `responsesIdleTimeout`, but now apply to both Codex Responses and Claude Messages. The former is the max total duration (default `0`, i.e. no hard cut), the latter is the idle timeout with no upstream data at all (default `5400000` ms = 90 minutes, `0` disables). Non-zero values are at least 60000 ms, at most 24 hours. Long tasks are not interrupted by the normal 30-minute total duration, while optional idle-connection protection stays available.

Coding-protocol streams have a third independent protection: **the no-progress watchdog** (`responsesNoProgressTimeout`, default `900000` ms = 15 min, range 1 min–24 h). The idle timeout counts from request start and cannot catch long streams that stall after a healthy preamble; the watchdog tracks the time since the latest real upstream SSE data — if a coding stream gets no bytes for longer than that, the proxy force-writes a failure terminal (`response.failed` / Anthropic `event: error`) and destroys the upstream connection, reporting an explicit error to the CLI instead of letting an accepted-but-never-returning connection hang forever. `responsesStreamLifetime=0` is unaffected.

Every converted request and protected native `/responses` request records `streamOutcome`, `streamReason`, `stopReason`, `streamSawDone`, `streamId`, `streamErrorMsg`, and `terminalSource` in the normal request log, and additionally writes a `stream_terminal` event carrying the upstream address. `stopReason` is the protocol-level terminal reason (the downstream `stop_reason` equivalent): Chat upstream `finish_reason: length` or Responses upstream `status: incomplete` (`incomplete_details.reason = max_output_tokens`) is recorded as `max_tokens`, meaning the response was truncated by the model's output limit — the log table shows an amber "truncated" badge next to the model name, the detail view and CSV add a "terminal reason" column, and full-text search covers the field; normal endings are `end_turn`/`tool_use` etc. Searchable terminal reasons: `upstream_done`, `upstream_eof_without_done`, `upstream_eof_without_completed`, `upstream_close`, `upstream_aborted`, `upstream_incomplete`, `upstream_error`, `upstream_idle_timeout`, `stream_lifetime_timeout`, `no_progress_timeout`, `client_disconnect`, `model_at_capacity`, `insufficient_quota`, `upstream_api_error`. HTTP status codes still represent transport-layer responses; HTTP `200` with `streamOutcome=failed` counts as a failure for success rate, model errors, and error distribution.

Error objects returned by the upstream inside an SSE stream or a non-2xx HTTP error body (e.g. `Selected model is at capacity. Please try a different model.`, `You exceeded your current quota`, `WEEKLY_LIMIT_EXCEEDED` / `MONTHLY_LIMIT_EXCEEDED`) are no longer silently dropped: the proxy records the length-limited and desensitized error message into `streamErrorMsg` and classifies it as `model_at_capacity` / `insufficient_quota` / `upstream_api_error` by content. SSE errors emit `response.failed` to Codex CLI and Anthropic `event: error` to Claude Code; if all available keys fail and the final response is `502`, the proxy additionally writes a `downstream_terminal` event. Auto-switching to another key that succeeds does not produce that event or count as a downstream failure. Logs, CSV, and full-text history search expose the error classification, message, and source; the "🔻 Downstream" trend chart now groups request counts by downstream application (request User-Agent), no longer showing stream terminal states — see the log/CSV "stream result / stream terminal reason / terminal reason" fields for terminal details. The proxy can only record HTTP/SSE content that actually passes through it; it cannot read Codex CLI prompts that never went through the proxy.

If you see Codex CLI's `stream disconnected before completion`, first check the workspace and logs for partial file modifications, command execution, or tool-call results; do not blindly replay the whole coding task. Such changes take effect after a proxy restart, so wait for all in-flight CLI tasks to finish, then restart in a maintenance window.

#### Mixed-Account Fallback (Chat Client → Anthropic)

The `/v1/chat/completions` route auto-enables multi-protocol pool fallback when Messages/Responses accounts exist in the group (static whitelist or dynamic probe; see "Upstream protocol auto-detection"): first try same-protocol Chat upstreams (byte passthrough), if the whole Chat pool fails convert to Messages format and try Anthropic upstreams, then convert to Responses format and try native Responses upstreams; only when every key in a protocol pool fails does it move to the next pool.

Different-protocol (Chat / Messages / Responses) upstreams can coexist freely, no extra configuration needed.

#### Responses→Chat Supported Fields

`/v1/responses` → Chat conversion supports these parameter mappings:

| Responses field | Chat field | Notes |
|---|---|---|
| `model` | `model` | model name |
| `input` | `messages` | supports string / array formats |
| `instructions` | `system message` | converted to system role |
| `max_output_tokens` | `max_tokens` | max output tokens |
| `temperature` | `temperature` | sampling temperature |
| `top_p` | `top_p` | nucleus sampling |
| `stop` | `stop` | stop sequences |
| `tools` | `tools` | tool definitions |
| `tool_choice` | `tool_choice` | tool choice policy |
| `metadata` | `metadata` | custom metadata |

Not supported (dropped): `include`, `previous_response_id`, `store`

### /__status Field Reference

| Field | Type | Description |
|---|---|---|
| `idx` | int | key index |
| `key` | string | masked display (first 6 + ... + last 4) |
| `url` | string | relay address |
| `reset` | string | daily / weekly / never / hourly |
| `remark` | string | remark |
| `available` | bool | currently available (`!inCooldown()`) |
| `status` | string | active / discarded / locked |
| `failCode` | int/null | last failure code |
| `failTime` | int/null | last failure timestamp |
| `failPeriod` | string/null | failed-period identifier |
| `failReason` | string/null | failure classification reason (`insufficient_quota` / `model_at_capacity` / `upstream_api_error`). Recorded for quota-class failures so auto-recovery can skip quota-exhausted keys |
| `failCount` | int | consecutive-failure count (only for codes in lockFailCodes) |
| `locked` | bool | auto-locked or not |
| `active` | bool | has in-flight request |
| `activeRequests` | int | current concurrent request count |
| `actives` | array | per-active-request detail: `{model, since}` |
| `healthScore` | int | 0-100 |
| `avgDuration` | int | average latency (ms) |
| `avgTtfb` | int | average time-to-first-byte (ms) |
| `p50` / `p95` / `p99` | int/null | latency percentiles (ms) |
| `sliding5mRate` | float/null | 5-minute sliding success rate |
| `sliding1hRate` | float/null | 1-hour sliding success rate |
| `totalCost` | float | cumulative estimated cost (USD) |
| `totalRequests` / `successRequests` / `failRequests` | int | request counts |
| `inputBytes` / `outputBytes` | int | cumulative traffic |
| `lastUsed` | int/null | last-use timestamp |
| `lastStatus` | int/null | last-request upstream status code (200/429/5xx etc.), written only after a request completes |
| `lastTime` | int/null | last-request completion timestamp (ms) |
| `lastModel` | string/null | last-request model name |
| `timeWindow` | object/null | off-peak window `{"start":22,"end":8}`, hour-level, `start==end` = all hours |
| `tz` | string/null | timezone offset, e.g. `"+8"`, `"-5"`, default `"+0"` |
| `inTimeWindow` | bool/null | currently inside the window (only returned by `/__keys` GET) |
| `daily` | object | per-day stats `{"YYYY-MM-DD": {...}}` |
| `hourly` | object | per-hour stats `{"YYYY-MM-DD-HH": {...}}` |
| `boostedBatch` | array | key indices in a batch priority (always 1-based) |
| `boostedBatchMode` | string | batch priority mode: `"use"` / `"roundrobin"` / `"random"` / `""` |

### /metrics Prometheus

| Metric | Type | Labels | Description |
|---|---|---|---|
| `codex_proxy_accounts_total` | gauge | — | total keys |
| `codex_proxy_keys_active` | gauge | — | currently available keys |
| `codex_proxy_queue_depth` | gauge | — | request queue depth |
| `codex_proxy_key_requests_total` | counter | key, url | cumulative request count |
| `codex_proxy_key_bytes_total` | counter | key, type(input/output) | cumulative bytes |
| `codex_proxy_key_health_score` | gauge | key | health score |
| `codex_proxy_request_queue_max_wait_seconds` | gauge | — | queue timeout setting |

### WebSocket Protocol

After connecting, pushed automatically:

```json
{"type": "status", "data": [...], "boostedIdx": -1, "boostedBatch": [], "boostedBatchMode": ""}
{"type": "notification", "notificationType": "all_keys_failed", "time": "..."}
```

`status` pushes are full snapshots and **throttled/coalesced**: at most 1 broadcast per second in any window, multiple state changes merged into one push (full not incremental, no info loss); a 10-second heartbeat provides eventual consistency as a fallback. When WebSocket connection fails, the frontend automatically degrades to HTTP polling (every 5 seconds).

To keep slow/background-tab dashboard clients from unbounded buffering that could OOM the proxy process, a WebSocket connection whose send buffer exceeds 4 MiB is actively disconnected; the frontend auto-reconnects (continuing via HTTP polling during the gap) and receives a full snapshot immediately after reconnecting.

## Failure Code Meanings

Hovering a failure code on a dashboard card shows its Chinese meaning:

| Status code | Meaning |
|---|---|
| 401 | API Key invalid or expired |
| 402 | Insufficient quota, account in arrears |
| 403 | Insufficient permission, key has no access |
| 429 | Too many requests, rate limit triggered |
| 500 | Upstream server internal error |
| 502 | Upstream gateway error |
| 503 | Service temporarily unavailable |
| 504 | Upstream timeout |

## Restarting the Proxy

Two ways:

1. **Dashboard**: Config dialog → 🔄 restart proxy → confirm (recommended)
2. **CLI**: `kill $(cat proxy.pid)` (maintenance window only; the watchdog relaunches within 10 seconds)

After dashboard confirmation a fullscreen progress appears: submitting the restart request, draining in-flight requests, waiting for the old instance to exit and the watchdog to relaunch, detecting new-instance readiness. Once the new instance's ID changes and its state is `ready`, the Dashboard auto-reloads — no more blank waiting without feedback. Both the embedded Dashboard and the standalone backup dashboard can choose "Cancel restart" during `draining`.

After `POST /__restart` returns `202`, the old instance enters `draining`: queued requests receive `503`, new API requests temporarily receive `503` with `Retry-After: 5`, and in-flight requests finish. The old instance keeps restart control and progress-polling endpoints, writes the watchdog reload marker after draining, then exits; the watchdog re-reads its own script via `exec` on seeing the marker, detects the port, and relaunches the process. There is always exactly one watchdog lock holder, avoiding concurrent state conflicts. Let clients handle this brief unavailability per their own retry policy.

On first upgrade from an old watchdog that does not support the marker, the old script cannot recognize the new reload request. In a maintenance window with no active CLI tasks, manually stop the confirmed old watchdog PID, then run `bash start-proxy.sh --boot` to start the new script, which takes over the still-running proxy. Then perform one normal safe restart from the dashboard so the new `proxy.js` loads. After that, the dashboard's "Restart proxy" performs the watchdog's `exec` reload at the same time and never spawns a second watchdog that competes for the lock.

If a restart was accidentally triggered while tasks are running, call `POST /__restart/cancel` or click "Cancel restart" while the old instance is still `draining`. The proxy immediately restores new-request admission, but already-rejected queued requests cannot be restored — clients should retry themselves.

"Force restart" is only shown after draining for over 30 seconds, and requires a second confirmation. `POST /__restart/force` makes the old instance exit immediately, so it disconnects SSE/streaming responses and other in-flight connections; Codex CLI tasks may partially execute or report `stream disconnected before completion`. This operation is never performed automatically — only use it when you are sure interrupting current work is acceptable.

## Version Check and Upgrade

The Dashboard checks [official GitHub Releases](https://github.com/aipayim/codex-proxy/releases) once on open, with the frontend rechecking every 30 minutes; the server uses ETag and a 1-hour cache. Release metadata is always viewable, but the version is compared and the blinking ⬆ indicator shown at the right of the top "Config" only after the local build baseline is determined.

The proxy identifies the **build provenance** and never guesses a "dev machine" from directory name, disk, hostname, or Windows/WSL environment, nor hard-codes local paths or Release versions into the source. The local determination runs once at startup, not on the proxy request hot path:

| Priority | Local state | Compare behavior |
|---|---|---|
| 1 | `updateBaselineTag` filled in System Config | used as the explicit baseline for custom builds |
| 2 | `build-info.json` + `release-manifest.json` in the official release asset pass validation | release tag used automatically |
| 3 | official GitHub remote, clean working tree, `HEAD` exactly at a stable Release tag | that Git tag used automatically |
| 4 | `release-baseline.txt` in the source install dir records a valid baseline (cloned/copied source repo or source ZIP) | version number from the file used automatically |
| 5 | dev branch, local modifications, unknown remote, missing Git/release metadata/baseline file, or manifest validation failure | baseline unknown; only shows Release, no update prompt |

`release-baseline.txt` is a version-baseline file maintained in sync with the source repo and Release assets (single line `vX.Y.Z`), so source installs without build metadata can still show the local version and judge upgradability. `updateBaselineTag` is an advanced customization option, format `vX.Y.Z` or `X.Y.Z`, e.g. `v1.2.3`. Regular users do not need to fill it in: unmodified official release assets auto-identify; leaving it empty is the correct default for custom builds. The update indicator is only shown when GitHub's latest official Release is higher than the determined baseline.

### Generating Official Release Assets

Do not treat a GitHub source snapshot directly as an auto-identifiable release package. Release maintainers should run tests in the source to be released first, then generate a standalone asset directory:

```bash
npm test
npm run build:release -- --tag v1.2.3 --out ./dist
```

The builder generates `dist/codex-proxy-v1.2.3/`, containing:

- `build-info.json`: the Release tag, commit, and manifest digest written by the build process;
- `release-manifest.json`: a SHA-256 manifest of the proxy code, startup scripts, and package metadata;
- the runtime whitelisted files.

The publisher syncs the output asset's `package.json` version to the Release tag and removes the source-only `scripts` (including `npm test` and `npm run build:release`), but does not modify the dev source. It does not copy `build-release.js`, any `test-*.js`, `config.json`, keys, state, logs, PID, or local paths. Archive this directory as the GitHub Release asset for regular users. Installed user configuration is not in the manifest; but if the proxy code or protected scripts change, manifest validation fails and falls back to "unknown provenance", avoiding false update prompts.

The manifest is used for local integrity and version-provenance determination; it never executes remote code and never enables overwrite-style upgrades. Regardless of provenance, "one-click upgrade" stays disabled to avoid overwriting local code, config, or runtime state. The safe upgrade flow: back up the current proxy directory and config/state, review the Release, manually merge desired changes, run `node -c proxy.js`, then restart the proxy in a maintenance window.

## config.json System Config

```json
{
  "webhookUrl": "https://hooks.example.com/webhook?key=your_key_here",
  "prices": { "inputPer1M": 5, "outputPer1M": 15 },
  "bytesPerToken": 3,
  "modelPricing": [
    { "model": "gpt-5.6-sol", "inputPer1M": 5, "outputPer1M": 15, "bytesPerToken": 3 },
    { "model": "gpt-5.6-luna", "inputPer1M": 1, "outputPer1M": 3, "bytesPerToken": 4 }
  ],
  "notifications": { "sound": true, "desktop": true },
  "autoRecover": true,
  "autoRecoverInterval": 1,
  "autoRecoverCodes": [401,402,403,429,500,502,503,504],
  "autoRecoverDiscarded": false,
  "autoRecoverDaily": false,
  "autoRecoverDailyDays": 1,
  "autoRecoverDailyHour": 8,
  "autoRecoverDailyMinute": 0,
  "autoRecoverPoll": false,
  "autoRecoverPollInterval": 5,
  "autoRecoverPollCodes": [500, 502, 503, 504],
  "autoRecoverDelays": [800],
  "rateLimit": true,
  "maxRequestsPerMin": 10,
  "maxTokensPerMin": 0,
  "defaultResetHours": 5,
  "autoResume": false,
  "autoResumeIdleMinutes": 10,
  "autoResumeDebounceMinutes": 3,
  "autoResumeRunnerStallMinutes": 20,
  "autoResumeRunnerMaxStallRestarts": 1,
  "autoResumeProjects": [],
  "cmdPath": "/mnt/c/Windows/System32/cmd.exe",
  "weeklySortBy": "priority",
  "roundRobin": false,
  "enableAutoLock": true,
  "lockAfterFailCount": 3,
  "lockFailCodes": ["401","403"],
  "logFile": true,
  "logRetentionDays": 7,
  "logMaxMiB": 256,
  "logSegmentMaxMiB": 16,
  "stateHourlyRetentionDays": 35,
  "stateDailyRetentionDays": 180,
  "stateMaxMiB": 32,
  "proxyLogMaxMiB": 10,
  "proxyLogKeepFiles": 5,
  "codexLogMaintenance": {
    "enabled": false,
    "dbPath": "/root/.codex/logs_2.sqlite",
    "thresholdMiB": 2048,
    "retainHours": 12,
    "checkIntervalMinutes": 15
  },
  "logDetail": "full",
  "logIncidents": {
    "enabled": true,
    "notify": false,
    "latencyEnabled": false,
    "windowMinutes": 5,
    "minRequests": 8,
    "errorBurst": 5,
    "errorRatePercent": 60,
    "streamFailureBurst": 3,
    "p95Ms": 120000,
    "p95TtfbMs": 20000,
    "resolveAfterMinutes": 5,
    "defaultSnoozeMinutes": 15
  },
  "updateBaselineTag": "",
  "groups": {
    "A": 3456,
    "B": 3457,
    "C": 3458
  }
}
```

| Field | Description |
|---|---|
| `webhookUrl` | POST JSON alert when all keys fail (compatible with WeCom/DingTalk/Telegram) |
| `prices.inputPer1M` | default input price ($/million tokens), used when no `modelPricing` rule matches |
| `prices.outputPer1M` | default output price ($/million tokens), used when no `modelPricing` rule matches |
| `bytesPerToken` | default approximate bytes per token (default 3; Chinese ~1.5-2, English ~4), used when no `modelPricing` rule matches |
| `modelPricing` | optional per-model pricing rule array, default `[]`; a matching rule overrides the three global price/estimate params above, non-matches still use the global values |
| `notifications.sound` | whether to play a sound alert |
| `notifications.desktop` | whether to send desktop notifications |
| `autoRecover` | whether to auto-recover cooled keys |
| `autoRecoverInterval` | probe interval (hours, min 0.5) |
| `autoRecoverCodes` | failure-code array to detect, e.g. `[401,429,500]` |
| `autoRecoverDiscarded` | also detect keys in `discarded` state |
| `autoRecoverDaily` | enable fixed-time detection (true/false, default false) |
| `autoRecoverDailyDays` | detect every N days (default 1) |
| `autoRecoverDailyHour` | detect hour (0-23, default 8) |
| `autoRecoverDailyMinute` | detect minute (0-59, default 0) |
| `autoRecoverPoll` | enable fast recovery (true/false, default false). When a key gets a matching failure code, starts short-interval polling detection, stops when all are recovered |
| `autoRecoverPollInterval` | poll interval (minutes, default 5, min 1) |
| `autoRecoverPollCodes` | triggering failure-code array, e.g. `[500,502,503,504]`. Any matching code activates fast polling |
| `autoRecoverDelays` | detection-interval array (ms), default `[800]`. Shared by all modes; after each key test a random value is picked as the wait before the next key. Max 10, range 100–10000. Recommended `[800,1200,500]` to mimic human rhythm and lower batch risk-control odds |

> **Quota keys are excluded from auto-recovery**: keys classified as `insufficient_quota` (quota/usage exhausted, e.g. `WEEKLY_LIMIT_EXCEEDED`) are skipped by auto-recovery (interval/fixed/fast), because `/v1/models` probing still returns 200 for quota-exhausted keys and cannot distinguish quota state. Such keys can only recover naturally via periodic rollover or a real successful request (`markSuccess`), or by manually "reset cooldown".
| `rateLimit` | enable per-minute rate limiting (true/false, default true) |
| `maxRequestsPerMin` | per-key max requests per minute (default 10). Overridable per key in keys.json (`maxReqPerMin`) |
| `maxTokensPerMin` | per-key max tokens per minute (default 0=unlimited). Overridable per key in keys.json (`maxTokPerMin`) |
| `defaultResetHours` | default reset period for `hourly` type (hours, default 5). Overridable per key in keys.json (`resetHours`) |
| `autoResume` | enable idle auto-resume (true/false, default false) |
| `autoResumeIdleMinutes` | idle threshold (minutes, default 10) |
| `autoResumeDebounceMinutes` | legacy-config-compat field (minutes, default 3); only one initial launch per key idle period |
| `autoResumeRunnerStallMinutes` | validated-resume-runner stall grace (minutes, default 20, `0` off, range 0–1440). Only applies when there are no in-flight requests and no key has been applied since runner start or the last actual key application |
| `autoResumeRunnerMaxStallRestarts` | allowed validated-runner stall restarts per key idle period (default 1, `0` off, range 0–3). Never blindly replays ordinary failed runners |
| `autoResumeProjects` | project list array, max 10. Each item has `name`/`path`/`cmd`; `resumeMode` optional `"command"` (default, raw command like `resume --last`) or `"fixed_session"`; `fixed_session` requires `sessionId`, and `cmd` must contain the `{sessionId}` placeholder (replaced with that session ID on launch). `resume --last` resumes the "most recent" session, which may kick/restore the wrong session when multiple Codex instances run concurrently; use `fixed_session` for deterministic resume |
| `cmdPath` | cmd.exe path (default `/mnt/c/Windows/System32/cmd.exe`) |
| `weeklySortBy` | weekly-group sort: `"priority"` (priority+index) or `"expiry"` (nearest-expiry first) |
| `roundRobin` | enable round-robin balancing mode (see "Key Scheduling Order") |
| `enableAutoLock` | enable auto lock (true/false, default true) |
| `lockAfterFailCount` | auto-lock after N consecutive failures (default 3) |
| `lockFailCodes` | only these error codes count toward consecutive-failure counting (default `["401","403"]`) |
| `logFile` | enable file logging (true/false, default true). When off, only an in-memory cache of 2000 records is kept, no new logs written; existing JSONL can still be queried and exported in the log viewer |
| `logRetentionDays` | log file retention days (default 7). 0 disables auto-cleanup |
| `logMaxMiB` | request JSONL total capacity cap (default 256 MiB, range 16–4096). Deletes oldest segments, independent of day retention |
| `logSegmentMaxMiB` | single request JSONL segment cap (default 16 MiB, range 1–256, cannot exceed `logMaxMiB`). Numbered segments after exceeding in a day |
| `stateHourlyRetentionDays` | `state.json` hourly stats retention days (default 35, range 31–365) |
| `stateDailyRetentionDays` | `state.json` daily stats retention days (default 180, range 30–3650) |
| `stateMaxMiB` | `state.json` capacity cap (default 32 MiB, range 4–256). On overflow deletes oldest stats by complete time bucket via temp-file atomic replace |
| `proxyLogMaxMiB` | WSL watchdog current `proxy.log` segment cap (default 10 MiB, range 1–100) |
| `proxyLogKeepFiles` | WSL watchdog retained `proxy.log.N` archive count (default 5, range 1–20); total ≈ current segment + these archives |
| `codexLogMaintenance.enabled` | enable Codex SQLite log maintenance (default `false`). Must pass path and SQLite structure detection to save enabled |
| `codexLogMaintenance.dbPath` | main `logs*.sqlite` under the current proxy user's `~/.codex/`, e.g. `/root/.codex/logs_2.sqlite`. Supports auto-conversion from `\\wsl.localhost\distro\...` / `\\wsl$\distro\...`; rejects ordinary Windows paths, symlinks, `-wal`, or `-shm` |
| `codexLogMaintenance.thresholdMiB` | maintenance starts when main + WAL combined reaches this size, default 2048 MiB, range 64–102400 |
| `codexLogMaintenance.retainHours` | only deletes `logs.ts` records older than this, default 12h, range 1–8760 |
| `codexLogMaintenance.checkIntervalMinutes` | background check cycle, default 15 min, range 5–1440. At most 5 × 1000-row short transactions per round; skips when SQLite is busy and waits for the next cycle |
| `logDetail` | log detail level: `"full"` (full, includes model name) or `"basic"` (terse, no model name) |
| `logIncidents` | log incident rules object. Configures whether to enable/notify, observation window, minimum request count, failure count/rate, stream-failure count, optional P95 request/TTFB thresholds, auto-resolve time, and default silence minutes; default only alerts, never auto-pauses a group, restarts, or mutates keys |
| `updateBaselineTag` | advanced option. Manually confirmed upstream stable Release tag for custom builds, format `vX.Y.Z` or `X.Y.Z`. Official release assets, clean official Git tags, and the source baseline file (`release-baseline.txt`) are auto-identified and need no value; custom builds with unknown provenance left empty only show Release info without update comparison |
| `groups` | port group mapping, e.g. `{"A": 3456, "B": 3457}`. Group A always runs and cannot be deleted; B/C/D etc. managed dynamically via the dashboard |
| `groupEnabled` | group switch states, e.g. `{"B": true, "C": false}`. Disabled groups do not start their port after restart. All enabled by default |
| `taskInsight` | Task Insight config object. Off by default; fields below in the "Task Insight" section |

## Task Insight (Agent pipeline parsing/distillation)

Optional feature: aggregates AI-coding requests passing through the proxy into "task sessions" by client+group, automatically recording instructions, tool calls, involved files, usage and cost, and can call an LLM to generate structured summaries (decisions/risks). Off by default; all signals must be enabled explicitly.

### Enabling and Signals

Add `taskInsight` to `config.json`:

```jsonc
{
  "taskInsight": {
    "enabled": true,
    "signals": { "instructions": true, "tools": true, "usage": true, "correlate": true },
    "retentionDays": 30,
    "distill": {
      "enabled": true,
      "engine": "ollama",        // ollama | proxy | external
      "model": "qwen2.5:7b",
      "baseUrl": "http://127.0.0.1:11434/v1",
      "dailyBudgetYuan": 1,
      "report": "daily"          // daily | weekly
    }
  }
}
```

| Field | Description |
|---|---|
| `enabled` | master switch, default `false` |
| `signals.instructions` | records instructions in requests (first 200 chars only, whitespace-normalized) |
| `signals.tools` | records tool names (e.g. `read_file`) and file/directory paths (regex-extracted, no full args stored) |
| `signals.usage` | records real token usage (extracted from upstream SSE, not byte estimates) and estimated cost |
| `signals.correlate` | session correlation: anchors to the unique autoResume-active project in a 45-minute window; idle timeout for the same client auto-closes a new session |
| `retentionDays` | session-file retention days (range 1–3650, default 30); expired files under `tasks/` auto-deleted |

### Distillation (LLM Summary)

Generates a structured `{summary, decisions, risks}` digest for finished sessions, guarded by a daily budget.

| Field | Description |
|---|---|
| `distill.enabled` | enable distillation |
| `distill.engine` | `ollama`=local Ollama (data never leaves the machine); `proxy`=via this proxy (counts proxy tokens within the daily budget, no key leakage); `external`=direct third-party API (privacy is your own responsibility) |
| `distill.model` | summary model name |
| `distill.baseUrl` | OpenAI-compatible endpoint. Required for `external`; for `ollama` empty defaults to `http://127.0.0.1:11434/v1` |
| `distill.dailyBudgetYuan` | daily distillation cost cap (default 1), estimated from upstream usage, stops when exceeded |
| `distill.report` | `daily`/`weekly` report period |

### Data and Privacy

- Sessions persist in `tasks/YYYY-MM-DD.jsonl` (per session-start day), stored only locally.
- Instructions truncate to the first 200 chars; tools store only names, files only regex-extracted paths; no full tool args or raw request text stored.
- Distillation only sends a structured snapshot (≤6000 chars), never any API key.
- Sessions are written to disk on idle close or feature disable; only the latest 2000 in-memory sessions are kept at runtime.

### Related Endpoints

| Endpoint | Description |
|---|---|
| `GET /__task-insight-status` | current Task Insight status (switch, signals, distill state and budget) |
| `GET /__tasks?from&to&project&status&model&q&limit` | query sessions (newest first by default, `limit` ≤500) |
| `GET /__tasks/export` | export CSV (BOM-prefixed, ≤2000 rows) |
| `GET /__tasks/report?mode=daily|weekly` | per-project aggregation report |
| `POST /__tasks/distill-now` | manually trigger distillation |

The dashboard toolbar "📋 Task flow" views/exports sessions; the System Config dialog switches engine and budget.

This proxy can listen on multiple ports within a single process. The Key pools on different ports are fully independent, enabling layered isolation across models.

### Grouping Mechanism

- Every Key has a `group` field (default `"A"`) marking its group
- `groups` in `config.json` defines each group's listening port; Keys match a port through their `group` field
- Group A's proxy always runs on the configured port (default 3456) and cannot be deleted. Other groups are added/removed dynamically via the System Config dialog
- On CLI startup you can override port allocation with `--groups "A=3456,B=3457"`; after restart the config.json allocation is restored

### Port Routing

| Port | Group | Default use | Note |
|---|---|---|---|
| 3456 | A | daily tasks | always runs, default group, all Keys without a `group` belong here |
| 3457 | B | complex tasks (Sol, etc.) | optional, added via System Config |
| 3458 | C | simple tasks (Luna, etc.) | optional, added via System Config |
| more | D~Z | custom | dynamically added, custom port |

### Use Cases

Assign Keys of different models to different groups; the codex CLI picks a model by pointing at different ports:

```bash
# Daily tasks (port A, default 3456) — standard models like Tree
codex "write a Python script"

# Complex tasks (port B, 3457) — configure base_url as http://localhost:3457 in codex CLI
# The matching Key's model field can be set to an advanced model like "gpt-5.6-sol"

# Simple tasks (port C, 3458) — quick Q&A using low-cost models
```

### Dashboard

- **Key management**: each row shows a "Group" input to set a Key's group
- **Card badge**: non-A group Keys show a group-letter badge in the top-right corner
- **Group filter**: the top dropdown filters Key cards by group
- **Batch migrate**: check multiple Keys → batch toolbar "Migrate to group" → pick target group
- **System Config**: the port-group management area adds/removes groups dynamically; changing a port syncs to the server automatically
- **Port detection**: each group port auto-shows 🟢 (running)/🔴 (not running), probed when the config dialog opens
- **Enable/disable**: non-A groups can toggle their port via 🔴 disable / 🟢 enable; state saves to `groupEnabled` in `config.json` and persists across restarts

### Routing Logic

1. Request arrives on port P → look up `groups` in `config.json` for group name G
2. Pick a Key in group G's pool using the usual priority/round-robin/cooldown logic
3. If group G's server is not running → 502 "Group X server not running"
4. Key pools are fully isolated across groups and do not affect each other

> **Scheduling-isolation note**: each group runs the complete, independent `pickKey()` scheduling logic — the same two-level priority (reset → priority), round-robin (`roundRobin`), cooldown checks, model routing and all other rules. Whatever scheduling rule group A uses, groups B/C/D use exactly the same rule — only the Key pools differ.
>
> There is no "finish round-robinning group A, then group B" or cross-group mixed scheduling.
> Whatever port a request hits, it is scheduled independently within that port's group Key pool, unaware of the others.

### Codex CLI Integration and Task Routing

The core value of multi-port grouping: **auto-pick the right model based on task complexity**. Full guide on combining it with the Codex CLI:

#### Port Group Definitions

| Port | Group | Model | Role | Key capacity | Suitable for |
|---|---|---|---|---|---|
| 3456 | A (main) | default | daily tasks | 249 (high) | code editing, tests, docs, formatting, refactoring |
| 3457 | B (advanced) | gpt-5.6-sol | complex tasks | 2 (low) | architecture design, hard debugging, three-strike fallback |
| 3458 | C (basic) | gpt-5.6-luna | simplest tasks | 1 (very low) | simple queries, format checks, small edits |

#### Three-Strike Fallback Mechanism

When the Codex CLI hits a complex problem, this strategy applies:

```
Attempt 1-2 → Group A (port 3456, default model)
     ↓ fails
3rd failure → call codex-ask-b (Group B, gpt-5.6-sol)
     ↓ get a plan
Review plan → apply code changes → continue
     ↓ if B also fails
Report to user → manual intervention
```

**Fallback triggers** (any of the following counts as one failure):
- Compile/test failure
- Runtime panic or error
- Plan rejected by the user
- Same problem unresolved for over 30 minutes

#### How to Invoke

**Group A (default)**: the Codex CLI uses it automatically on startup; nothing extra needed.

**Group B**: call via the `codex-ask-b` script (install once):

```bash
# Install script (one-time)
curl -o /usr/local/bin/codex-ask-b https://raw.githubusercontent.com/.../codex-ask-b
chmod +x /usr/local/bin/codex-ask-b

# Usage
codex-ask-b "
【TASK】${current task description}
【FAILURE CONTEXT】${errors/reasons from the first 3 attempts}
【CODE LOCATION】${relevant file paths}
【ATTEMPTED】${methods already tried}
Please give a complete solution (with code).
"
```

**Group C**: call via the `codex-ask-c` script (install once):

```bash
# Install script (one-time)
curl -o /usr/local/bin/codex-ask-c https://raw.githubusercontent.com/.../codex-ask-c
chmod +x /usr/local/bin/codex-ask-c

# Usage (simplest tasks: format checks, simple queries, small edits)
codex-ask-c "check the formatting of this function"
codex-ask-c "quick query: what is 1+1"
```

#### Sending to a Specific Key (`#N` Syntax)

Appending `#N` (N = Key number) to the model name **routes directly to that Key**, bypassing `pickKey()` scheduling. Useful for testing a specific Key, debugging a problem Key, or manually controlling request distribution.

**Syntax**: `model#N`, where N is the 1-based index in keys.json.

```
Model name examples:
  o3#87       → uses Key #87 (group B), sends model name o3 upstream
  gpt-4o#6    → uses Key #6 (group C), sends model name gpt-4o upstream
  o3#253      → uses Key #253 (group A)
```

**Codex CLI configuration**:

```toml
# ~/.codex/config.toml
# without #N: automatic scheduling
model = "o3"

# with #N: pinned to a specific Key
model = "o3#87"
```

**Claude Code CLI configuration**:

```json
// ~/.claude/settings.json
{
  "model": "o3#87",
  "apiBaseUrl": "http://localhost:3456"
}
```

**Behavior notes**:
- If Key #N does not exist → returns `400 Key #N does not exist`
- If Key #N's URL is unreachable → returns a normal 502 error
- If the Key has a `model` override field (`acct.model`) → the override model wins, not the model name before `#N`
- If the Key has no override → the `#N` suffix is stripped from the model name and the clean model name is sent upstream

#### Group C Upstream API Compatibility

Some upstream APIs have a malformed non-streaming response: the `message.content` field is missing yet `completion_tokens` is reported. This only affects non-streaming requests; streaming works fine.

**Solution**: `codex-ask-c` uses streaming (`stream: true`) by default, extracts `delta.content` from SSE events, and assembles a standard response format.

**Debug steps if you hit a similar problem**:
1. Test the upstream API's streaming response with `curl`
2. Check whether `message.content` exists in the non-streaming response
3. If streaming works but non-streaming is broken, use `stream: true` in your script and assemble the response yourself

#### AGENTS.md Configuration Example

Add the following to your project's `AGENTS.md` so AI agents know how to use the different ports:

```markdown
## Port Group Routing and Three-Strike Fallback Rules

### Port Group Overview
| Port | Group | Model | Role | Key capacity |
|------|-------|-------|------|--------------|
| 3456 | A (main) | default | daily tasks | 249 (high) |
| 3457 | B (advanced) | gpt-5.6-sol | complex tasks | 2 (low) |
| 3458 | C (basic) | gpt-5.6-luna | simplest tasks | 1 (very low) |

### Three-Strike Fallback Flow
1. Attempts 1-2: try normally with Group A (port 3456)
2. 3rd failure: call `codex-ask-b` to get a Group B model solution
3. If B also fails: report the error to the user

### How to Invoke
- Group A: default, used automatically by the Codex CLI
- Group B: `codex-ask-b "prompt"`
- Group C: `codex-ask-c "prompt"`

### When to Use Group B vs Group C
| Criterion | Use Group B | Use Group C |
|-----------|-------------|-------------|
| Task complexity | complex (architecture, hard debugging, algorithms) | simple (format checks, simple queries, small edits) |
| Trigger | three-strike fallback (Group A fails 3 times) or explicit need for an advanced model | the task is simply simple |
| Model ability | gpt-5.6-sol (strong reasoning) | gpt-5.6-luna (basic) |
```

#### Notes

- **No nested Codex CLI**: an active Codex CLI session cannot start another Codex CLI (it will hang)
- **Group B keys are limited**: only 2 keys, avoid frequent calls
- **Group C model is basic**: only for the simplest tasks, not for complex work or fallback
- **Ports are independent**: each port's Key pool is fully isolated; A's 429s don't affect B/C
- **Script install**: `codex-ask-b` and `codex-ask-c` must be manually installed to `/usr/local/bin/`, see the install instructions above

## Auto-Recovery of Cooled Keys

A background timer probes cooled Keys with `GET /v1/models` for connectivity; on success it automatically clears the cooldown/discarded state.

### Behavior

- Skips failure codes not in the `autoRecoverCodes` list
- The `discarded` state is only checked when `autoRecoverDiscarded=true`
- Successful probe (200 OK) → auto-clears `failCode`/`failTime`/`failPeriod`; if `discarded`, restores to `active`
- Logs `[proxy] auto-recover: #N recovered`
- Config changes take effect immediately, no restart needed (timers auto-reset)
- Batch probing runs serially with the intervals in `autoRecoverDelays`; after each Key test a random interval is picked before the next Key (default 800ms), avoiding simultaneous batch requests that could trip upstream risk controls

### Three Modes

| Mode | Note |
|---|---|
| **Periodic detect & recover** (interval mode) | detects every N hours (default 1 hour), based on `setInterval` |
| **Fixed-time detect** (calendar mode) | detects at a set HH:MM every N days (default daily 08:00), based on a `setTimeout` chain |
| **Fast recovery** (event-driven mode) | when a Key gets a code in `autoRecoverPollCodes` (default 500/502/503/504), polls at short intervals (default 5 min) until all recovered, then auto-stops; re-activates automatically on the next such code, based on a `setTimeout` chain + `markFailure` event hook |

The three modes can be enabled/disabled independently or together. When all are on, the dashboard shows three independent countdowns. The failure-code and discarded config are shared by all three modes.

## Idle Auto-Resume (autoResume)

When the last actual Key application exceeds the threshold, it automatically reopens the project terminal window in a visible Windows terminal (for WSL2 environments, ensuring the codex CLI runs in a visible window).

### How It Works

1. The proxy only records `lastKeyUseTime` when the selected upstream Key was actually written into a real forwarded request. Ordinary downstream connections, admin endpoints, dashboard/poll requests, and a long stream that never applies the Key again do not reset this timer
2. The runtime heartbeat is kept in `.auto-resume-runtime.json` and mirrored to `state.json`; so the last Key-application time survives throttled full-state writes, safe restarts, or old state files being overwritten. On first run with no historical heartbeat, a baseline is established at proxy startup so unknown history is not misread as failure
3. Detection runs once at startup and after saving config, then every 30 seconds while idle; when idle exceeds `autoResumeIdleMinutes` (default 10), a new Key idle period begins
4. While requests are still in flight, opening new Codex terminals is deferred; after the request ends the same Key heartbeat continues to drive the decision, so healthy long streams are not mistaken for dead ones, and no `/goal` dependency is used
5. Each project runs its initial launch only once per idle period; the next initial attempt happens only after the proxy actually applies a Key again, or the user edits that project's path/command/session config. Ordinary failed runners are not replayed infinitely on the debounce interval
6. `checkAutoResume()` walks the `autoResumeProjects` list and calls `triggerResume()` for each; a new visible Windows cmd window is started via `cmd.exe /c start` → runs wsl.exe → bash → executes the project command
7. Each runner writes an atomic JSON lease to `/tmp/codex-resume-<project>.pid` containing a random run ID, PGID, and Linux process start time; as long as the lease remains verifiable, duplicate launches are skipped
8. By default there is one controlled stall restart: if an already-launched managed runner has no new Key application within `autoResumeRunnerStallMinutes` (default 20) and the proxy has no in-flight requests, the proxy re-verifies the random run ID, PID start tick, independent PGID, and the actual `/proc` process group; only when all match does it send `SIGTERM` to the negative PGID, then `SIGKILL` if still alive after 30 s. Only after confirming that process group exited does it launch one extra time in this idle period
9. The proxy never scans, terminates, or SIGKILLs arbitrary external `codex` processes by project directory. `cmd.exe start` returning success only means Windows accepted the launch request; only the runner state file indicates it started, exited, or received a signal

### Config Fields

```json
{
  "autoResume": true,
  "autoResumeIdleMinutes": 10,
  "autoResumeDebounceMinutes": 3,
  "autoResumeRunnerStallMinutes": 20,
  "autoResumeRunnerMaxStallRestarts": 1,
  "autoResumeProjects": [
    {"name": "project-a", "path": "/mnt/d/projects/project-a", "cmd": "codex chat"},
    {"name": "project-b", "path": "/mnt/d/projects/project-b", "cmd": "./run.sh"},
    {"name": "project-c", "path": "/mnt/d/projects/project-c", "resumeMode": "fixed_session", "sessionId": "<codex-session-id>", "cmd": "codex resume {sessionId} 'continue'"}
  ],
  "cmdPath": "/mnt/c/Windows/System32/cmd.exe"
}
```

| Field | Note |
|---|---|
| `autoResume` | enable idle auto-resume (true/false) |
| `autoResumeIdleMinutes` | Key-application threshold (minutes, default 10). Triggers when the last real Key application exceeds this |
| `autoResumeDebounceMinutes` | legacy-config-compat field (minutes, default 3). Only one initial launch per Key idle period |
| `autoResumeRunnerStallMinutes` | managed-runner stall grace (minutes, default 20, `0` off). Controlled takeover only starts when the proxy has no in-flight requests and there has been no Key application since runner start or the last real Key application |
| `autoResumeRunnerMaxStallRestarts` | controlled stall restarts allowed per Key idle period (default 1, `0` off, range 0–3). Stops relaunching once the cap is reached |
| `autoResumeProjects` | project list, max 10. Each has `name` (display name), `path` (WSL path, supports `E:\xxx` auto-conversion), `cmd` (command to run); optional `resumeMode` and `sessionId` |
| `resumeMode` | optional per-project mode: default `command` runs `cmd` verbatim; `fixed_session` requires `cmd` to contain the `{sessionId}` placeholder |
| `sessionId` | Codex session ID for `fixed_session`. On launch `{sessionId}` in the command is safely replaced, avoiding `resume --last` picking an uncertain branch/sub-session |
| `cmdPath` | cmd.exe path (default `/mnt/c/Windows/System32/cmd.exe`) |

### Panel Status

- **Config dialog**: shows a live row `🧬 Idle resume: Key idle ${formatIdle}, last triggered Ym ago` (second-level two-decimal, cascading d/h/m/s)
- **Dashboard**: toolbar-right shows `🧬Key idle ${formatIdle}/resume Ym ago` (same cascade format), auto-zeroing to `0.00s` while there are active requests
- **Concurrent-Key ticker**: inline to the right of the timestamp row shows `⚡ Concurrent: ` + a badge per active request (Key number, model name, elapsed time, e.g. `#3 gpt-5.6-sol 12s`), elapsed time live-updating every second; badges go left-to-right, overflow badges auto-collapse with a `+N` (gray) hint in front; when there is no concurrency the whole ticker hides and takes no space

### Path Self-Check

The `path` field supports these formats, auto-normalized on config save:
- WSL Linux path: `/mnt/<drive>/path/to/project` → unchanged
- Windows path: `D:\path\to\project` → `/mnt/d/path/to/project`
- Mixed path: auto-converted to a WSL absolute path

### Dependent Script

`resume-codex.sh` lives in `codex-proxy/` and opens a visible terminal window via `cmd.exe /c start` + `wsl.exe`. It uses an atomic JSON state file recording `starting`, `running`, `exited`, `failed`, `terminated`; `HUP`/`INT`/`TERM` received by the runner itself and by the command process group are labeled with their source and signal. The state file is a diagnostic record and contains no API Keys or command bodies.

### Notes

- **WSL2 only**: depends on `cmd.exe` and `wsl.exe` to create a visible Windows terminal; useless in a pure-Linux environment
- **Path must exist**: `path` must be `cd`-able inside WSL
- **Terminal boundary**: `codex resume` is still a TUI; the Windows/WSL/PTY chain may show control characters; this does not mean the task resumed. A log line "runner started" only means the command process was created; a new Key application is what shows observed proxy activity
- **Session selection**: `resume --last` is not deterministic when parallel project branches or subagents exist. For reliable resumption use `fixed_session`, the session ID, and `{sessionId}`; update that ID manually after switching sessions
- **PID lease**: stored at `/tmp/codex-resume-*.pid`, only manages processes created by this feature whose run ID, start time, and independent process group are verifiable. It never cleans up manual Codex instances in the same directory
- **Controlled stall recovery**: by default, when a verified runner has no new Key application for 20 straight minutes and there are no in-flight proxy requests, its independent process group is first terminated gently, forced only if still alive after 30 s, and after confirmed exit only one extra restart happens. Set `autoResumeRunnerStallMinutes: 0` or `autoResumeRunnerMaxStallRestarts: 0` to disable this protection
- **Single initial attempt**: ordinary launches or failed runners are not replayed infinitely within one Key idle period; only the strict controlled stall recovery above may restart once more

#### How Fast Recovery Works

1. A Key gets a failure code like 500/502/503/504 → `markFailure()` sees it is in `autoRecoverPollCodes` and no timer is running → starts `schedulePollRecover()`
2. Every `autoRecoverPollInterval` minutes (default 5): check whether any Key still holds a matching failure code → if yes, call `GET /v1/models` to test connectivity, auto-clearing cooldown on success; if no, stop (no leftover timer)
3. After stopping, if a Key again gets a matching failure code → re-activates (event-driven, no polling dependency)

## Cost Estimation

The global `prices` and `bytesPerToken` are fallback values for legacy-config compatibility. If different models have different input/output prices or token densities, add rules in `modelPricing`; each rule must include `model`, `inputPer1M`, `outputPer1M`, and `bytesPerToken`:

```json
{
  "modelPricing": [
    { "model": "gpt-5.6-sol", "inputPer1M": 5, "outputPer1M": 15, "bytesPerToken": 3 },
    { "model": "gpt-5.6-luna", "inputPer1M": 1, "outputPer1M": 3, "bytesPerToken": 4 }
  ]
}
```

- Default is `[]`, so with no config the behavior is identical to old versions.
- `model` matches the final forwarded-and-recorded model name exactly (leading/trailing whitespace in saved and request names is ignored); different case, aliases, prefixes, or substring relations never match. For example `gpt-5.6-sol` does not match `gpt-5.6-sol-preview`; a miss falls back to the global `prices` / `bytesPerToken`.
- One model can have only one rule, max 50 rules; model names up to 80 chars. Input/output unit prices must be 0–1,000,000, `bytesPerToken` must be 0.1–100.
- Unit prices are still "USD / million tokens". A rule's `bytesPerToken` only controls that model's cost byte→token estimation, not exact upstream usage; per-minute token rate limits (`maxTokensPerMin` / `maxTokPerMin`) still use the global `bytesPerToken`.

```
tokens ≈ bytes / bytesPerToken
cost = (inputTokens / 1_000_000) × inputPer1M + (outputTokens / 1_000_000) × outputPer1M
```

> ⚠️ Exact token tracking is unavailable (upstream relays do not return a `usage` field); this is a byte→token estimate. Each request freezes the price rule matched at the moment forwarding begins; rules saved later only affect new requests and never rewrite in-flight long streams or already-written `state.json` totals, hour/day trends, or historical logs. So the same historical window may contain estimates from different price rules in effect at the time.

## Webhook Alert Format

```json
{
  "event": "all_keys_failed",
  "time": "2026-06-17T12:00:00.000Z",
  "accounts": 5,
  "proxy": { "accounts": 5, "queueDepth": 3 }
}
```

## Request Queue

When all available Keys are cooling down, new requests enter a buffer:
- Queued requests are processed automatically when a Key recovers
- Max wait 30 seconds → times out with `503`
- Client disconnects → automatically removed

## Process Watchdog

### systemd Environments (Linux server)

```bash
# Install
bash install.sh

# Manage
systemctl status codex-proxy    # status
journalctl -u codex-proxy -f    # live logs
systemctl restart codex-proxy   # restart
systemctl stop codex-proxy      # stop
systemctl disable codex-proxy   # disable auto-start
```

The `codex-proxy.service` template ships with `Restart=always` + `RestartSec=5`; systemd relaunches the process automatically after a crash.

### WSL2 Environments (no systemd)

WSL2 does not use systemd by default; the built-in watchdog script provides equivalent protection:

```bash
# One-click start watchdog + proxy
bash start-proxy.sh

# Check status
curl http://localhost:3456/__status

# Stop the proxy (maintenance window only; the watchdog relaunches within 10 seconds)
kill $(cat proxy.pid)

# Fully stop watchdog + proxy (maintenance window only)
pkill -f watchdog.sh
```

#### How It Works

| Component | Role |
|---|---|
| `watchdog.sh` | every 10 s checks proxy liveness: flock single-instance lock → checks `proxy.pid` + port binding + command-line verification. The script locates the proxy from its own directory, no fixed machine paths. If the process disappears it relaunches; if a non-proxy process holds the port it only alerts, never kills. A dashboard safe-restart asks the watchdog to reload its own script via `exec`, then launches the proxy — no second watchdog is created. proxy.js writes the PID file itself after successfully listening on the A port |
| `start-proxy.sh` | starts the watchdog in the foreground (manual). `bash start-proxy.sh --boot` starts it in the background (WSL boot) |
| `proxy.pid` | `proxy.js` writes `process.pid` on start, cleans it up on exit |
| `/etc/wsl.conf` | already has `[boot] command = /usr/local/bin/codex-watchdog.sh`, loading the watchdog automatically when Windows boots WSL |

#### Auto-Start on Boot

`/etc/wsl.conf` is configured:

```ini
[boot]
command = /usr/local/bin/codex-watchdog.sh
```

`codex-watchdog.sh` runs in order: fix opencode network routing → `start-proxy.sh --boot` → watchdog stays resident in the background.

**To make it take effect**: run `wsl --shutdown` once in Windows PowerShell then reopen a WSL terminal, or restart Windows.

#### Resource Usage

The watchdog is blocked in `sleep 10` 99.9% of the time, **0 CPU usage**, about **500KB** memory.

## How install.sh Works

`install.sh` is a full-environment bootstrap script that auto-detects the environment (WSL2 / Linux systemd) and performs the matching actions:

### Execution Flow

| Step | Action | Note |
|---|---|---|
| 1 | `npm install` | installs the `ws` dependency |
| 2 | create `config.json` / `state.json` / `keys.json` | only when missing (never overwrites existing config) |
| 3 | check Node.js ≥ 16, hint about Python 3 SQLite support | exit if Node too old; Python 3 is only optional for Codex SQLite log maintenance, its absence doesn't block install |
| 4 | install system service | **WSL2**: creates the watchdog bootstrap script + `/etc/wsl.conf`; **systemd**: installs the systemd service and `enable` `start` |
| 5 | create `codex` wrapper script | auto-detects `CODEX_BIN`, writes `~/bin/codex`; ensures `~/bin` is added to `~/.bashrc` PATH; ensures the login shell loads `.bashrc` |
| 6 | print summary + next steps | shows environment info, file locations, next-step hints |

### Environment Variable Overrides

```bash
WRAPPER_DIR=/custom/bin CODEX_BIN=/opt/codex/bin/codex.js bash install.sh
```

| Variable | Default | Note |
|---|---|---|
| `WRAPPER_DIR` | `$HOME/bin` | wrapper script output directory |
| `CODEX_BIN` | auto-detected | real codex CLI path. Auto-searches common locations like `/usr/lib/node_modules/@openai/codex/bin/codex.js`, `/usr/local/bin/codex`, `/opt/codex/bin/codex`, `$HOME/codex/bin/codex` |

### WSL2 Auto-Install Contents

| File | Path | Note |
|---|---|---|
| watchdog bootstrap | `/usr/local/bin/codex-watchdog.sh` | auto-starts the watchdog when WSL boots (invoked by `/etc/wsl.conf`) |
| WSL config | `/etc/wsl.conf` | appends `[boot] command` pointing to the bootstrap script above |
| wrapper script | `$WRAPPER_DIR/codex` | running `codex` automatically: checks proxy liveness → starts watchdog if down → `exec`s the real codex |
| PATH config | `~/.bashrc` / `~/.profile` | ensures `$HOME/bin` is on PATH and the login shell loads `.bashrc` |

## FAQ

**Q: After starting, `http://localhost:3456/` doesn't respond?**
A: Is the proxy running? Check `ps aux | grep proxy.js`. If not running, `node proxy.js &`.

**Q: The proxy crashed. Will it restart automatically?**
A: The watchdog process protection is configured. The watchdog checks the proxy every 10 seconds and relaunches it with `nohup node proxy.js &` on crash. When you type `codex` in a terminal, the wrapper script also auto-detects and starts the watchdog.
Run `ps aux | grep watchdog` to confirm the watchdog is online.

**Q: The dashboard won't open in WSL2?**
A: Use `localhost`, not `127.0.0.1`. In WSL2, `127.0.0.1` points to Windows' own loopback.

**Q: Double-clicking dashboard.html won't connect?**
A: The standalone panel needs the proxy running. Use `http://localhost:3456/` for the full feature set.

**Q: How do I shield a Key without deleting it?**
A: Click 🔇 in the admin UI, or set `"status": "shielded"` in keys.json.

**Q: How do I restore a deleted Key?**
A: Frontend deletion is a soft delete (sets `status="deleted"`); the Key is still in keys.json. Edit keys.json to remove the `status` field or set it back to `"active"`.

**Q: A Key got auto-locked. How do I recover it?**
A: Find the 🔒 locked Key in the admin dialog and click the 🔓 unlock button, or call `POST /__reset-key {"idx": N}` manually. You can also disable auto-lock (uncheck "Enable auto lock") or adjust the `lockAfterFailCount` threshold in config.

**Q: Cost estimates are off?**
A: First adjust the global `bytesPerToken` and `prices`; different models have different token densities, so you can use `modelPricing` to set a per-model unit price and `bytesPerToken` by exact name. This is still a byte estimate, and price changes don't recompute historical costs.

**Q: The dashboard shows "Loading"?**
A: Check the browser console (F12) for JS errors. Open `http://localhost:3456/` rather than `file://`. If the proxy just restarted, wait a few seconds and refresh.

**Q: How do I use batch operations on the dashboard?**
A: Check the card's top-left checkbox → the top action bar appears → click batch reset or batch shield.

**Q: How do I bulk-import Keys in the admin dialog?**
A: Click the 📋 button → paste one `sk-xxx url period remark` per line → confirm.

**Q: The "Restart proxy" button in the config dialog won't click?**
A: New versions show a restart progress bar after clicking and auto-refresh once the new instance is ready. If the running `proxy.js` predates this update, restart it manually once in a maintenance window; the interaction works after the new version loads.

**Q: Why do I need to re-enter the admin Token after refreshing the Dashboard?**
A: The admin Token is kept only in the current page's memory, not in browser persistence or session storage. Re-authenticate after refreshing, closing the page, or WebSocket auth failure.

**Q: Why is there no update indicator, or is "one-click upgrade" disabled?**
A: Unmodified official Release assets auto-identify their version; source installs (cloned/copied source repo or source ZIP) auto-identify via the version recorded in `release-baseline.txt`; clean official Git tags also auto-identify. The indicator shows when GitHub has a higher official Release. Dev branches, local modifications, or copies of unknown provenance show no indicator by default and only display the Release; you can leave the field empty, or fill in "Custom build baseline tag (advanced)" when confident. Click "Check for updates" in System Config to re-check; the dialog and version box show your local version and the gap to the latest official Release. To avoid overwriting local modifications, the panel only offers Release info and safe upgrade steps, never auto-replaces source.

## Contributor Acknowledgments

This section collects reviewed public contributions, issue reports, and design suggestions that were adopted; future contributors will be added continuously based on their actual contributions.

| Contributor | Contribution |
|---|---|
| [@anupamme](https://github.com/anupamme) | proposed the security improvement that the admin Token should not be stored in browser session storage (PR [#1](https://github.com/aipayim/codex-proxy/pull/1)). Current version implements the in-memory adaptation on the latest code and completes the WebSocket auth path. |

## Changelog

- **2026-08-12 LAN access mode**: added network mode setting (localhost / LAN) and LAN API key protection, allowing other devices on the same LAN to use this proxy for AI requests. Dashboard settings panel includes mode selection and key input; saving automatically restarts all port groups to apply the new bind address. LAN clients authenticate via Bearer token constant-time comparison. Local requests always bypass the LAN key. Synchronized Chinese and English i18n keys (`cfg.networkMode`, `cfg.networkLocalhost`, `cfg.networkLan`, `cfg.lanApiKey`, `cfg.lanApiKeyPh`, `cfg.lanApiKeyTitle`).

- **2026-08-12 Update-dialog security and backup guidance**: show the full safe-upgrade steps only when GitHub has a newer Release; when the local version is current or provenance is unknown, show only the single disabled-upgrade warning. The backup step explicitly includes `keys.json`, which may contain API Keys and upstream URLs and must never be uploaded to GitHub, Release assets, or any public location. Synchronized the Chinese and English translation keys.

- **2026-08-12 Update-dialog safety conditions and Release-note rendering**: corrected the previous condition that showed the full five-step safety guide only for detected updates or unknown provenance. After Release information is fetched successfully, the dialog now shows the complete five-step manual upgrade process and the GitHub upgrade-guide link whether the local version is current or behind; only a failed Release lookup falls back to the one-line warning. Release notes now use restricted Markdown rendering with HTML escaping and only `http`/`https` links allowed, supporting headings, unordered lists, and inline code without allowing Release content to inject HTML or scripts. Long notes remain scrollable.

- **2026-08-12 Full update-dialog safety guide**: when GitHub has a newer Release, the dialog now lists backup, Release review, manual merge, `node -c proxy.js`, and maintenance-window restart steps, with a GitHub upgrade-guide link. Current or incomparable local versions retain the one-line disabled-upgrade warning. Added the matching `upd.safetyGuideTitle`, `upd.safetyStep1`–`upd.safetyStep5`, and `upd.safetyUnverified` translations. This changes Dashboard presentation only; update-check backend behavior is unchanged.

- **2026-08-12 Dashboard localization copy fix**: restored localized Chinese/English bindings for batch Key actions, duration displays, Task Insight, database-maintenance status, group controls, resume-project controls, and collapse actions. This is a compatibility-preserving UI copy and display-logic fix; it prevents portions of the Dashboard from staying in English or losing translations after switching languages.

- **2026-08-12 README release status and Release-link fix**: corrected the Markdown line breaks in the v2.55.0 release description, marked v2.55.0 as released throughout the public documentation, and replaced the broken `build-release.js` link because that source-only file is not shipped in regular-user Release assets.

- **2026-08-11 Dashboard bilingual interface and language switching**: added Chinese/English Dashboard resources and a top-right language control. The current browser stores the choice in `localStorage`; Chinese remains the default, switching affects only that browser and does not require a proxy restart. After changing Dashboard or i18n source code, restart the running Node.js process and refresh the page.

- **2026-08-10 Stop-reason tracking and cost-trend fix**: added end-to-end tracking of `stopReason` (protocol-level terminal reason) — Chat upstream `finish_reason: length` and Responses upstream `status: incomplete` (`incomplete_details.reason = max_output_tokens`) are unified as `max_tokens`, meaning the response was truncated by the model's output limit. Log request entries and the `stream_terminal` event record this field; the log table shows an amber "truncated" badge next to the model name, the detail view shows "Terminal reason", CSV gains a `stopReason` column, and full-text search covers the field. Previously the responses→messages conversion treated truncation as `end_turn`, losing information; now distinguished. Also fixed the "💰 Cost" trend chart showing no bars: `vals` wrongly read a nonexistent `cost` field (the real field is `totalCost`), producing all `undefined`, `max` stuck at 1, and the whole trend container hidden by `display:none`; and the `Math.max(...vals, 1)` floor of `$1` compressed hourly costs (usually far below $1) into a 2px sliver. The cost mode now reads the real `totalCost`, bases the scale on the actual max (falls back only when all-zero), and bars render with normal proportions.

- **2026-08-09 Unified request/actual-model composite display**: the request log, model distribution, and trend chart each previously showed half the information — the trend chart aggregated by the client-requested model name (e.g. `claude-fable-5`), the log table/model distribution by the actually-forwarded-and-executed model name (e.g. `gpt-5.6-sol`), the two scopes disagreed, and cost was priced by the request name. Now a unified composite label `request model (actual model)` (e.g. `claude-fable-5 (gpt-5.6-sol)`) is used for the trend legend, the log "Model" column, log model distribution, Key-dialog model stats, and the concurrency ticker, so one row shows "what the client wanted, what upstream actually used"; when the two are the same or only one exists, the single name is shown without parentheses. Model adaptation is computed at the request entry, cost estimation uses the actually-executed model (falls back to the request name when unadapted), and the Key's `model` override precedence is unchanged. Log expanded details, JSONL, and CSV keep independent `reqModel` / `overrideModel` raw fields. Added regression tests for the display label and log dimensions.

- **2026-08-09 Dynamic protocol identification persistence and Chat routing fix**: dynamically-probed upstream protocol previously lived only in the model-capability cache and was briefly lost when a model probe failed (30 s TTL), so unknown relays correctly identified as Messages / Responses were wrongly downgraded back to Chat passthrough at request time. Dynamic protocol now goes into a separate cache (24 h TTL), decoupled from the model-list cache, so a failed probe no longer loses the protocol classification; `/v1/chat/completions` multi-protocol pool detection switched from a static URL whitelist to using the dynamic protocol result, and relays identified as Messages / Responses automatically enable Chat→Messages / Chat→Responses conversion fallback. Added a background per-URL-deduplicated upstream re-probe every 10 minutes; multiple Keys in one pool share one probe, and newly-configured/edited Keys take effect without restart. Added regression tests for protocol-cache persistence and Chat routing.

- **2026-08-04 WebSocket status-push throttling and send backpressure (OOM governance)**: Dashboard status broadcasting previously serialized all Key states fully and broadcast them live on every request completion/state change, with no throttling and no send-queue cap; background or slow tabs let `ws.send` buffers grow unbounded, repeatedly triggering `JavaScript heap out of memory` (4 GB heap). Now throttled-and-merged: status pushes happen at most once per second in any window, multiple changes merge into one full snapshot (full not incremental, no information loss), plus a 10-second heartbeat as a final-consistency backstop; any WebSocket connection whose send buffer exceeds 4 MiB is actively disconnected, the frontend auto-reconnects (HTTP polling every 5 s keeps the view alive during the gap), and a full snapshot arrives immediately after reconnecting. Capacity backoff/recovery, request queue, and other business paths are unaffected. Also refined 429/quota handling: classification regex hardened (removed the loose bare `limit exceeded` match, restricted to `usage limit`/specific limit messages), `failReason` persisted to the state file, and auto-recovery now skips quota-exhausted Keys (daily probes no longer wrongly recover an exhausted Key only to 429 it again).
- **2026-08-03 Root-cause fix for a stuck coding stream (8h41m hang)**: the Codex CLI waited forever during an upstream weekly-quota-exhaustion storm (this incident ~8h41m). Root causes fixed: ① `responsesIdleTimeout` unit bug — default was mistyped as `90*60*60*1000` (90 hours), actually clamped to 24 hours, inconsistent with the documented 90 minutes; changed to `90*60*1000`, 90 minutes effective by default. ② Quota/usage-limit 429s (`WEEKLY_LIMIT_EXCEEDED`, `MONTHLY_LIMIT_EXCEEDED`, `usage limit exceeded`, etc.) were treated as transient capacity with repeated backoff+requeue, sweeping every Key into a 429 storm; now classified as `insufficient_quota` → `markFailure` current-period cooldown and fast failure, and only 429s not classified as quota plus `model_at_capacity` take the `capUntil` transient backoff. ③ Queue deadlock — `enqueueRequest` didn't trigger `processQueue`, and periodic cleanup silently dropped waiting requests (socket not closed, CLI hung forever); now enqueue triggers a `setImmediate` drain plus a 5-second periodic drain, so requests past `capacityMaxWaitSeconds` actually get 503, and the silent-drop logic was removed. ④ New no-progress watchdog `responsesNoProgressTimeout` for coding-protocol streams (default 15 min): when a long stream stops sending bytes upstream but doesn't disconnect, it forces a failure terminal state and destroys the upstream connection, covering "accepted but never returns" hung connections; idle timeout only runs from request start and can't cover mid-stream stalls. ⑤ `autoResume` moved from `resume --last` (multi-instance concurrency causes mutual kicks/wrong-session restores) to `fixed_session` + a deterministic `sessionId` (pinned to the specific coding session, eliminating duplicate-session races). Added queue-drain and quota-classification regression tests.
- **2026-08-03 Idle-resume runner stall takeover**: fixed the issue where, after a capacity error leaves the Codex CLI stopped in local Planning and the proxy has had no new Key application for a long time, a still-alive resume runner permanently blocked the "once per idle period" rule. Added a 20-minute controlled stall grace (default) and a one-restart cap (default): only with no in-flight proxy requests, and with the random run ID, PID start tick, lease PGID, and actual process group all matching, does it send `SIGTERM` to that runner's negative PGID; `SIGKILL` only if still alive after 30 s; after confirming the process group exited it launches one extra time. Never scans, terminates, or affects manually-started Codex/Claude CLIs; records the event and skips when identity is unknown, the lease is stale, the PID is reused, or process groups disagree. Added regression tests for stall, TERM/KILL, ownership verification, Key-heartbeat reset, and single restart.
- **2026-08-02 SSE capacity-error backoff**: fixed the issue where the proxy wrongly took the hard-failure path when upstream explicitly returned `model_at_capacity` inside an HTTP-200 Responses / Messages SSE stream. These errors now behave like HTTP 429: only set `capUntil` transient backoff, no `failCode`, no cross-period discard or `all_keys_failed`; all protocol conversions and native Responses streams share this handling. Data already emitted to the client is never auto-replayed, avoiding duplicated text or tool calls.
- **2026-08-02 Responses / Messages stream terminal-state consistency**: the native `/responses` passthrough stream actually used by the Codex CLI now confirms `response.completed` via a side-channel SSE probe; on upstream HTTP-200 premature EOF/close/abort/error/timeout it preserves the original bytes and appends one `response.failed` instead of handing the raw broken stream to the CLI. The Claude Code `/v1/messages` conversion stream gains the same lifecycle protection: only a real upstream `[DONE]` sends one `message_stop`; abnormal termination emits an Anthropic `event: error`, never a fake completion or duplicated terminal state. The reverse Messages→Chat conversion likewise only sends one `[DONE]` after a real `message_stop`, and abnormal cases output an OpenAI-compatible SSE error. The dedicated long-stream total/idle timeouts now apply to both Responses and Messages, keeping the original config key names for compatibility; added bidirectional terminal-state, EOF, error, UTF-8 split, and no-newline terminal regression tests.
- **2026-08-02 Idle-resume safe leases and deterministic sessions**: idle resume no longer scans, terminates, or `SIGKILL`s arbitrary `codex` processes by project directory, avoiding collateral damage to manually-started CLIs, branches, or subagents. Each resume is identified by an atomic JSON lease of random run ID, PID, process group, and `/proc` start time; only a verifiable lease counts as its own runner, and the proxy never sends termination signals to external processes. With requests still in flight, opening new terminals is deferred; each project tries only once per continuous Key idle period until a new Key is actually applied or that project's config is edited — fixing endless replay and mutual termination after failures. Launcher success only means Windows accepted the request; runner state and real exit signals are the diagnostic basis; 120 s without runner state logs an explicit timeout. System Config adds a "fixed session" mode; only that mode replaces `{sessionId}`, normal command mode keeps the original command; `resume --last` records a session-uncertainty hint. Added runner lifecycle regression tests.
- **2026-08-01 Version-baseline identification and upgrade prompt**: added the `release-baseline.txt` version baseline file so source installs (cloned/copied source repo or source ZIP, no build metadata) can also show a local version number and judge upgradability. Each vX.Y.Z release syncs the file (git-tracked in the source repo, shipped inside Release assets, content matching the Release tag). Provenance priority extended to: official release package (build-info manifest validation) > clean official Git tag > source baseline file > unknown (with reason). The "System Config → Check for updates" dialog adds an explicit info bar of "Local version / latest official Release / gap"; the version box shows local version, provenance, latest official Release, and gap text. The badge has three states: amber pulsing `⬆` when an update exists, hidden when none, and gray neutral `⬆` when the baseline is unknown (all open the dialog to view the latest Release). GitHub cache TTL shortened from 6 h to 1 h.
- **2026-08-01 Idle-resume TTY and leftover-process cleanup + capacity/429 transient backoff**: the idle-resume launcher could open a window and start a runner, but failures stayed frequent (exit code 2). Root-cause investigation found three layers, fixed one by one. ① Command error: `-p` in the configured `resume --last -p '<prompt>'` is codex's `--profile` flag (not prompt), parsed as `invalid value ... for '--profile'` and exiting 2 immediately; codex's prompt is a positional argument, changed to `resume --last '<prompt>'`. ② `setsid bash -lc` inside the runner stripped the command from the terminal, and the interactive `codex resume` TUI needs a PTY; switched to `script -qec` to allocate a pseudo-terminal for the command (keeping `setsid` process-group isolation, `-e` forwarding the subcommand's exit code). ③ Early versions tried scanning and terminating leftover codex by project path; that approach was superseded by the 2026-08-02 identity-lease scheme, and current versions neither scan nor terminate external CLIs. ④ Re-trigger protection: after idle resume takes effect, loading a giant session (hundreds of MB / hundreds of thousands of lines) takes minutes; during that time no Key is used so idle keeps exceeding the threshold, and the auto re-trigger every debounce period (default 10 min) would kill the still-loading previous resume as an "old command" replacement — an "never finishes" loop; current version tries once per idle period and defers startup while requests are in flight. Also added `capacityBackoffSeconds` (default 60) and `capacityMaxWaitSeconds` (default 300): HTTP 429 and `model_at_capacity` errors become transient backoff (only set `capUntil`, no `failCode`), so a reset:"never" Key is no longer cooled for the whole period and one capacity blip doesn't cascade-disable all Keys; capacity-failed requests go straight back on the queue instead of hot-polling every Key, and the queue wait cap is exempted from the default 30 s timeout per `capacityMaxWaitSeconds`; `checkAllFailed` ignores pure-backoff Keys, avoiding false `all_keys_failed`. Added capacity-backoff lifecycle regression tests.
- **2026-08-01 Fix idle-resume launcher failure**: every idle-resume trigger previously failed — when `resume-codex.sh` used `cmd.exe /c start` to open a visible Windows terminal, WSL paths (`/mnt/c/...`) starting with `/` were parsed by cmd as switches (`Invalid switch - "/mnt"`), `start` immediately returned exit code 1, and the runner never started or ran. Fixed: convert the WSL path to Windows backslash form with `wslpath -w` before handing it to `start`, keeping the visible terminal window; on launcher failure the cmd stderr (GBK→UTF-8) is written back into state-file field 5, shown as `launcherError` and carried into the `auto_resume_launcher_failed` event message. End-to-end verified: state machine `starting → running → exited` completes fully and the command really executes.
- **2026-07-31 Model-level cost estimation**: System Config adds the `modelPricing` rule array, per-exact-final-model input/output unit prices and `bytesPerToken`; non-matching models keep falling back to global prices. Each request freezes the rule at forwarding start; later changes never recompute in-flight or existing stats, trends, or historical log costs.
- **2026-07-31 Codex SQLite log maintenance**: System Config adds an optional Codex SQLite log-maintenance switch, database path detection, capacity threshold, retention duration, check interval, and immediate-check status. Before enabling save, the server re-validates the current user's `~/.codex/` regular `logs*.sqlite` file and the `logs(id, ts)` structure; supports auto-converting `\\wsl.localhost\distro\...` / `\\wsl$\distro\...` to WSL internal paths. In the background, Python standard-library SQLite short transactions delete expired rows in bounded amounts, yields when busy, never restarts the proxy/watchdog/Codex CLI, and never proactively runs `VACUUM`, checkpoints, or touches WAL/SHM. Added SQLite maintenance regression tests; the Release package includes the runtime helper and continues excluding all test sources.
- **2026-07-31 Fix downstream aggregation crash loop**: fixed the repeated `ReferenceError: client is not defined` crash-and-restart cycle after proxy restart (added the `client` variable declaration in `forwardRequest` and passed it through to 5 `recordRequest` calls, the log entry reusing the same value). Added regression tests: runtime assertions that `recordRequest` correctly accumulates `clients` and ignores undefined clients, and a source-contract assertion that `forwardRequest` must declare `client` before calling `recordRequest`.
- **2026-07-31 Downstream trend by app**: the "🔻 Downstream" trend mode changed from terminal-state distribution to a stacked bar chart aggregating request counts by downstream app client (request User-Agent) — Codex CLI / Claude Code / Cursor / Chatbox / Cherry Studio / NextChat / LobeChat / OpenAI SDK / Vercel AI SDK / curl, top 8 apps shown, the rest in "Other", hover for per-app counts. Request records gained a `client` dimension: `recordRequest` also accumulates `clients` in hour/day buckets; full-text search and CSV gained a client column, and log detail shows "Client". Terminal states no longer appear in the trend chart; they live in the log/CSV "Stream result/terminal reason" and the `stream_terminal` event.
- **2026-07-31 Upstream aggregation and health recovery in trends**: the trend chart gained a "🔺 Upstream" mode aggregating usage counts by Key upstream domain (top 8 domains, rest in "Other", hover for domain + #Key list + count); the original status-code distribution was restored as the "💚 Health" label, no longer conflating the two upstream/downstream semantics; label order is Model/Volume/Count/Health/Upstream/Downstream/Cost/Latency.
- **2026-07-31 Runtime data governance**: added `state.json` hour/day stats trimming, capacity caps, and atomic backup-restore; request JSONL split by day and bounded by both retention period and total capacity; `.log-summary.json` bounded; the WSL watchdog gained `proxy-log-rotator.js` console-log rotation and old-large-file migration. Cleanup runs right after saving System Config; without the rotator it no longer falls back to unbounded appending of `proxy.log`.
- **2026-07-31 Upstream error capture and downstream terminal-state trend**: Responses→Chat conversion no longer silently drops upstream SSE or non-2xx HTTP error bodies; error messages are length-limited and sanitized, recorded as `streamErrorMsg`, and classified by content as `model_at_capacity` (capacity), `insufficient_quota` (quota), `upstream_api_error` (other upstream errors). SSE errors explicitly emit `response.failed`; when every candidate Key finally fails and a `502` is returned, a new `downstream_terminal` fires; a successful fallback is not misreported as a downstream failure. Logs, CSV, and historical full-text search gained error classification/source fields; terminal states stay in the log/CSV "Stream result/terminal reason" fields and the `stream_terminal` event scope. Backend added regression tests for HTTP error-body sanitization, capacity classification, historical search, and fallback semantics.
- **2026-07-30 Log query and event handling**: the default log view prefers the last 50 in-memory records; after a proxy restart with insufficient memory, a bounded Worker fills in the saved log tail so history isn't mistaken for being wiped. Fixed a time-range-left-blank bug being parsed as Unix time 0 and filtering both historical and in-memory logs to empty. Historical filtering, cursor pagination, and export are now also done by the Worker reverse-scanning JSONL, removing full-count/read from the main thread. Added minute and daily summaries, background rebuild on first missing, on-demand WebSocket subscription, and a 350 ms batch refresh. Added a log event center by upstream/model/path/group where you can acknowledge, silence, send notifications, and manually pause or resume a group; it never auto-mutates Keys or restarts the proxy.
- **2026-07-29 Release runtime slimming**: build artifacts no longer include the builder or any regression tests; the `package.json` inside the Release removes source-only scripts, and tests/builds are pinned to the GitHub source repo.
- **2026-07-28 Cancellable and forced restart control**: the safe-restart draining phase gained cancellation control, restoring new-request admission without faking restored already-rejected queued requests; a second confirmation enables forced restart only after 30 s of draining. Forced restart explicitly records and warns it interrupts active streams; added restart lifecycle regression tests and synced the standalone backup dashboard.
- **2026-07-28 Key-heartbeat idle resume and watchdog reload**: idle resume now relies only on the persisted heartbeat of actually-applied upstream Keys, no longer affected by ordinary requests or state-file throttling; checked right after startup/config save. Safe restart asks the watchdog to `exec`-reload itself after draining, keeping the single-instance lock, and added idle-resume and watchdog-reload regression tests.
- **2026-07-28 Responses stream terminal-state fix**: the conversion stream only sends `response.completed` after a real upstream `[DONE]`; fixed a trailing-`[DONE]`-without-newline being mis-parsed and the active-stream socket idle timeout being ignored. Abnormal EOF/close/error/timeout explicitly send `response.failed`; logs gained a searchable `stream_terminal` diagnostic event and stream-terminal fields; added a standalone stream lifecycle regression test.
- **2026-07-28 Admin Token in memory**: the Dashboard admin Token now lives only in the current page's memory; leftover session-storage residue is cleared at startup, HTTP and WebSocket share one auth state, and WebSocket re-authenticates on `4001`. Thanks to @anupamme for the security improvement idea.
- **2026-07-28 Release provenance identification**: added `npm run build:release` producing Release assets without runtime-sensitive files, `build-info.json`, and a SHA-256 manifest. Unmodified official Release assets and clean official Git Release tags auto-compare versions; dev/custom copies fail closed. The watchdog now locates the proxy from its own directory, removing fixed machine paths.
- **2026-07-28 Version-baseline fix**: removed the fixed local version and specific machine-directory notes. `updateBaselineTag` remains an advanced manual baseline for custom builds; when provenance is unknown only the Release is shown, no false update reporting. Overwrite-style one-click upgrades stay disabled.
- **2026-07-28 Restart timing and version check**: the restart overlay gained a separate 1-second timer and status-request timeout, no longer stuck at `0 s` when the old process's connection switches. The Dashboard added GitHub Release cache checking, a blinking update indicator, a version/Release-Notes dialog, and a System Config version link; overwrite-style one-click upgrades stay disabled.
- **2026-07-28 Dashboard restart progress**: "Restart proxy" in System Config gained a fullscreen dynamic progress showing drain and watchdog-recovery status; the panel auto-refreshes after confirming the new process is ready via instance ID. Added `GET /__restart-status`; during restart API requests explicitly return a briefly-unavailable state.
- **2026-07-28 Health trend chart**: the trend chart gained a 4th "health trend" mode showing stacked 200/4xx/5xx/failure bars per window, with 24h/7d/30d windows; backend `recordRequest` gained a `statusCode` parameter, request status codes aggregated by hour into `hourly.statusCodes`, pushed to the frontend over WebSocket
- **2026-07-28 close15 hardening**: the fetch wrapper uses `new URL()` origin checks (no longer string contains); WebSocket connections need token auth; `/metrics` is covered by the admin Token auth scope; the watchdog auto-kills orphaned processes after a 30 s drain timeout (no longer waits forever)
- **2026-07-28 close14 hardening**: timeout retry fix (a pre-first-byte timeout no longer wastes an available Key); watchdog drain conflict fix (when the port is free but the old process hasn't exited, don't start the new process first); `/__admin_token` → `/__auth_check` (no longer exposes the actual Token); the fetch wrapper only attaches Authorization to same-origin requests

## License

MIT

---

Official site: [OpenAPI.im](https://openapi.im) | Author Twitter: [@C_2049s](https://twitter.com/C_2049s)
