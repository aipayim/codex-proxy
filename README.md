# Codex Multi-Key Proxy

多 API Key 代理 + 实时监控面板。支持智能调度（按 Key 优先级+冷却状态轮询，可选轮询均摊模式）、自动容灾切换（遇到 401/402/403/429/5xx 自动尝试下一个 Key）、滑动窗口成功率（5 分钟 / 1 小时）、延迟百分位 P50/P95/P99、请求队列缓冲（最长 30 秒）、WebSocket 实时推送、Prometheus `/metrics`、Webhook/桌面通知/声音告警、完整前后端管理面板。

## 目录结构

```
codex-proxy/
├── proxy.js              # 核心代理服务器（含内嵌完整监控面板）
├── keys.json             # API Key 列表（每个 key 可独立配置 url/reset/status/remark）
├── config.json           # 系统配置（prices、webhookUrl、notifications、log 等）
├── state.json            # 自动生成，持久化统计数据与冷却/废弃状态
├── state.json.bak        # 每小时自动备份
├── dashboard.html        # 独立监控面板文件（file:// 打开会有引导提示）
├── codex-proxy.service   # systemd 服务模板（含 {{PROXY_DIR}} 占位符）
├── install.sh            # 一键安装脚本（自动读取自身路径，替换 {{PROXY_DIR}}）
├── edit-keys.sh          # 命令行快速编辑 keys.json 的辅助脚本
├── package.json          # npm 依赖（仅 ws）
├── logs/                 # 自动生成，按天滚动的 JSONL 日志文件（保留 N 天）
└── README.md             # 本文件
```

## AI Agent 一键安装

以下步骤可完全由 AI 或用户手动逐条执行：

### 1. 环境准备

```bash
# 检查 Node.js 版本（需要 ≥ 16）
node -v

# 克隆或复制本项目到任意目录
# cd /path/to/codex-proxy
```

### 2. 安装依赖 + 配置 Key

```bash
cd /path/to/codex-proxy

# 安装 npm 依赖（仅 ws）
npm install

# 编辑 keys.json，填入你的 API Key
#   key:    API 密钥，必须以 sk- 开头
#   url:    中转地址（http/https，每个 key 可不同）
#   reset:  额度重置周期 daily / weekly / never
#   remark: 备注（可选）
#   status: 可选字段，active / shielded / deleted
#   priority: 可选字段，整数（默认 0），数值越大调度优先级越高（同 reset 类型内优先）
#   models:   可选字段，数组，指定该 Key 可处理的模型名列表。
#            未设置或空数组时匹配所有模型（通配）。设置后仅路由匹配的模型请求到此 Key。
#   model:    可选字段，字符串。非空时转发上游请求时强制将 body.model 替换为此值。
#            与 models 独立：models 控制路由准入，model 控制上游实际使用的模型。
#   resetDay: 可选字段，1-7（1=周一…7=周日）。仅对 weekly 生效，指定每周哪天 00:00 重置。
#            未设置时按 Key 首次启用日自动对齐。
#
# 示例：
# [
#   {"key": "sk-xxx...", "url": "https://api.openai.com/v1",   "reset": "weekly", "remark": "主力 Key"},
#   {"key": "sk-yyy...", "url": "https://api.provider.com/v1", "reset": "daily",  "remark": "备用额度卡", "models": ["gpt-5.5", "gpt-5.4-mini"]},
#   {"key": "sk-zzz...", "url": "http://proxy.example.com:8080", "reset": "never",  "remark": "一次性", "status": "shielded"}
# ]
```

> 💡 推荐中转代理（注册送额度）：[https://api.fenno.ai/register?aff=FBGPVRCAA765](https://api.fenno.ai/register?aff=FBGPVRCAA765)

### 3. 启动代理

```bash
# 方式 A：手动启动（前台）
node proxy.js

# 方式 B：后台运行
nohup node proxy.js > proxy.log 2>&1 &

# 方式 C：systemd 开机自启（推荐）
# install.sh 会自动：
#   1. 检测自身路径作为 PROXY_DIR
#   2. 用 sed 替换 codex-proxy.service 中的 {{PROXY_DIR}}
#   3. 复制到 /etc/systemd/system/ 并 enable + start
#   4. 在 $HOME/bin 创建 codex 包装脚本（可选）
bash install.sh
```

### 4. 验证代理运行

```bash
# 检查状态
curl http://localhost:3456/__status

# 打开监控面板
# 浏览器访问 http://localhost:3456/
```

### 5. 配置 Codex CLI 使用本地代理

```bash
# 创建或编辑 ~/.codex/config.toml
mkdir -p ~/.codex
cat > ~/.codex/config.toml << 'EOF'
base_url = "http://localhost:3456"
EOF
```

### 6. 创建包装脚本（实现 codex 自动启动代理）

`install.sh` 默认在第 5 步创建包装脚本到 `$HOME/bin/codex`，
确保 `$HOME/bin` 在 PATH 中且优先于系统 codex 即可。

手动创建包装脚本：

```bash
mkdir -p ~/bin
cat > ~/bin/codex << 'WRAPPER'
#!/bin/bash
PROXY_DIR="/path/to/codex-proxy"   # ← 替换为你的实际路径
if ! curl -sf http://localhost:3456/__status > /dev/null 2>&1; then
  echo "[codex] Starting proxy..." >&2
  nohup node "$PROXY_DIR/proxy.js" > "$PROXY_DIR/proxy.log" 2>&1 &
  sleep 2
fi
exec /usr/lib/node_modules/@openai/codex/bin/codex.js "$@"
WRAPPER
chmod +x ~/bin/codex

# 确认 ~/bin 在 PATH 中（写入 ~/.bashrc 或 ~/.zshrc 等）
echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bashrc
```

包装脚本逻辑：
1. 请求 `http://localhost:3456/__status` 检测代理是否运行
2. 如未运行 → `nohup node proxy.js` 后台启动
3. 等待 2 秒后 `exec` 真正的 Codex CLI 二进制

之后运行 `codex` 即可自动启动代理。

### 7. 搞定

```bash
codex
# 代理自动启动（如未运行）→ Codex CLI 正常使用
```

## Key 调度顺序

`pickKey()` 采用**双层优先级调度**：

| 层级 | 依据 | 顺序 |
|------|------|------|
| **第一层** | 额度重置周期 `reset` | daily → weekly → never |
| **第二层** | 用户设置的 `priority` 数值 | 同 reset 组内，数值越大越优先，不限上限（默认 0） |

> **每周重置说明**：自 v2 起，每周重置不再按固定自然周（周一 00:00），而是每个 Key **独立按启用时间算 7 天**。
> 例如周三启用的 Key → 下周三 00:00 重置。可通过 `resetDay` 字段（或管理面板「重置日」）固定周几重置。

两种调度模式均遵循此双层顺序：

### 默认模式（`roundRobin: false`）

每次请求，从高优组到低优组逐层尝试，每组内按 priority 降序（同分按 keys.json 顺序）：

1. **daily 类型** → 取第一个不在冷却的 Key（高 priority 优先）
2. **weekly 类型** → 同上
3. **never 类型** → 同上
4. **兜底**：同类型中第一个 Key（无视冷却）

### 轮询均摊模式（`roundRobin: true`）

按 reset 类型分组，组内按 priority 降序排列，同组内轮询使用：

| 分组 | 调度顺序 |
|------|----------|
| daily → priority: 10（高优） | 在 daily 组内先轮询 |
| daily → priority: 0（默认） | 高优 Key 冷却后切至此组轮询 |
| daily 用完后 → weekly 组 | 同上分层逻辑 |
| weekly 用完后 → never 组 | 同上分层逻辑 |

例：`priority: 10` 的 daily Key 在 daily 组内独享轮询，冷却后自动切换到 daily 组内 `priority: 0` 的 Key 轮询；daily 组全部冷却后切入 weekly 组。

两种模式中，可用 Key 数（`activeCount`）仅统计 `status === "active"` 的 Key，
屏蔽（shielded）和软删除（deleted）不计入。

## 模型路由

支持按请求中的 `model` 字段自动路由到支持该模型的 Key（代理从请求 body 中解析 `model` 字段）。

### 配置

在 `keys.json` 中为 Key 添加 `models` 数组：

```json
{"key": "sk-xxx...", "url": "https://...", "models": ["gpt-5.5", "gpt-5.4-mini"], "reset": "weekly"}
```

| `models` 字段 | 行为 |
|---|---|---|
| 未设置或 `[]` | 匹配所有模型（通配/向后兼容） |
| `["gpt-5.5", "gpt-5.4"]` | 仅匹配列出的模型 |

### 路由逻辑

1. `pickKey()` 收到请求中的 `model` 参数
2. 过滤 Key 池：`models` 未设置或包含该模型的 Key 进入候选池
3. 在候选池内按原有优先级/轮询逻辑选择最优 Key
4. 如无任何 Key 匹配 → 回落通配 Key（`models` 未设置的 Key）
5. 如无通配 Key → `502 All keys exhausted`

### 模型覆盖

新增 `model`（单数）字段，与 `models`（数组）独立工作：

| 字段 | 作用 | 时机 |
|---|---|---|
| `models`（指定模型） | 路由准入：该 Key 只接哪些模型的请求 | `pickKey` 时 |
| `model`（覆盖模型） | 转发替换：强制上游使用此模型 | 发送请求时 |

**行为**：
- `model` 填充后 → 无论 CLI 发什么模型，代理在转发前将 `body.model` 强制替换为该值
- `model` 为空 → 透传 CLI 的原始模型（与旧版一致）
- 若该 Key 的 API Key 不支持 `model` 指定的模型，上游返回错误，Key 进入冷却
- 不影响 `models` 路由过滤：两者可组合使用

**典型场景**：
```bash
# 某个 Key 的配置
#   models: ["gpt-5.5"]      → 只接 gpt-5.5 的请求进来
#   model:  "gpt-5.6-sol"    → 但实际转发时改为 gpt-5.6-sol 发给上游
#
# CLI 发 model=gpt-5.5 → pickKey 匹配 → 转发时替换为 gpt-5.6-sol
```

### 管理面板

「管理 Key」弹窗每行提供两列模型输入：
- **指定模型**（`models`，逗号分隔）：路由准入过滤
- **覆盖模型**（`model`，单个值）：转发时强制替换

保存后生效，无需重启。

## 自动切换故障 Key

`forwardRequest()` 在以下情况自动切换到下一个 Key：

| 上游状态码 | 处理方式 |
|---|---|
| 401 Unauthorized | `markFailure` → 切换 |
| 402 Payment Required | `markFailure` → 切换 |
| 403 Forbidden | `markFailure` → 切换 |
| 429 Too Many Requests | `markFailure` → 切换 |
| 5xx Server Error | `markFailure` → 切换 |
| 连接超时 / DNS 错误 / TLS 错误 | `markFailure` → 切换 |
| 流传输中断 | `markFailure` → 切换 |
| 2xx / 3xx 成功 | `markSuccess` → 响应原路返回 |
| 其他 4xx | 透传给 Codex（不切换） |

全部 Key 切换失败后返回 `502 {"error": "All keys exhausted"}`（旧版存在全部失败后挂起不响应的 bug，已修复）。

## 冷却、废弃与自动锁死

- Key 返回 401/402/403/429/5xx → `failCode` + `failPeriod` 写入 `state.json` → 该周期内 `inCooldown()` 返回 true → 不再被 `pickKey()` 选中
- 同 Key **连续两个周期**（天/周）都失败 → 自动标记 `status: "discarded"` → 永久跳过（直到手动重置）
- `reset: "never"` 的 Key 一次失败即永久冷却
- **自动锁死**（`enableAutoLock: true`）：对 `lockFailCodes`（默认 401,403）中的错误码，连续失败达到 `lockAfterFailCount`（默认 3 次）后，自动标记 `status: "locked"` → 永久跳过（直到手动解锁）
  - 锁死计数仅统计同周期内同失败码的连续失败，成功请求或不同错误码会重置计数
  - 锁死状态写入 `state.json`，不影响 `keys.json`（避免面板保存覆盖）
  - 管理弹窗显示 🔒 锁死徽章 + 🔓 解锁按钮

## 管理 Key 状态重置

面板「管理 Key」模态框每行提供 🔄 按钮，调用 `POST /__reset-key`。

后端处理：
- 清除 `failCode` / `failTime` / `failPeriod`
- 如 Key 之前被自动标记为 `discarded` 或 `locked`，恢复为 `active`
- 保存 `state.json` + 广播 WebSocket 更新
- 不重启代理，下次轮询到该 Key 即正常尝试

可用于充值后快速恢复冷却/废弃/锁死 Key。

### 批量测试结果重置

批量测试后，「重置所有 Key 的状态码」按钮调用 `POST /__apply-test-result`，根据每 Key **实际测试结果**同步：

| 测试结果 | 操作 |
|---|---|
| 200 成功 | 清除 `failCode`（等同于重置冷却，下次可用） |
| 429/401/403 等失败码 | 调用 `markFailure()` 写入正确的 `failCode`，Key 进入冷却状态 |
| 网络异常（无状态码） | 跳过，不改变该 Key 状态 |

与旧版「一律重置」不同，新版保留失败 Key 的真实冷却状态，避免误将限流 Key 放行。

## 管理弹窗

`http://localhost:3456/` 面板点击「管理 Key」按钮打开管理弹窗，提供：

- **查看**：全部 Key 列表（含屏蔽/软删除），脱敏显示 ID、状态、备注、地址
- **增删**：添加新 Key、软删除（`status="deleted"` 保留在 JSON）
- **屏蔽/恢复**：🔇 屏蔽（不参与调度）、🔓 恢复
- **重置冷却**：🔄 清除冷却/废弃状态
- **搜索过滤**：实时搜索 ID/备注/地址
- **状态码筛选**：输入 `401` 等过滤指定失败码的 Key
- **状态筛选**：下拉选择 全部/可用/冷却中/废弃/锁死
- **数量显示**：实时显示 `共 X 个，筛选后 Y 个`
- **自动分组**：按备注前缀（中文逗号/英文逗号/空格分割的第一段）自动分组折叠
- **一键折叠/展开**：📂 按钮折叠或展开所有分组
- **拖拽排序**：拖动行调整顺序，自动保存到 keys.json
- **优先级设置**：每行「优先」数字输入框，数值越大调度越优先（同 reset 类型组内优先）
- **全选 + 批量操作**：批量重置 / 批量屏蔽 / 批量删除
- **批量导入**：📋 粘贴多行 `sk-xxx url 周期 备注` 快速导入
- **单 Key 测试**：🔍 调用 `GET /v1/models` 测试连通性，返回模型名 + 耗时
- **批量测试**：勾选多个 Key → 🔍 批量测试 → 面板实时显示每 Key 结果（成功/失败）→ 通过测试的 Key 可一键重置恢复使用
- **覆盖模型**：每行「覆盖模型」输入框，填入后转发时强制替换 `body.model`。与「指定模型」（路由准入）独立协作
- **重置所有 Key 的状态码**：批量测试后根据每 Key 实际测试结果同步状态（200 成功 → 清空冷却；429 失败 → 写入 429 进入冷却），而非统一重置为可用
- **CSV 导出**：导出完整统计数据

## 监控面板

`http://localhost:3456/` 内嵌完整监控面板：

### 顶部摘要
可用数/总数、冷却中、🔒 锁死数、并发请求、总流量、总请求、健康评分、预估费用

### 排序/筛选/搜索/批量操作
- 排序：默认 / 健康评分 / 平均延迟 / 5 分钟成功率
- 筛选：全部 / 可用 / 冷却中 / 废弃 / 🔒 锁死（与重置筛选可组合使用）
- 重置筛选：每日重置 / 每周重置 / 永不过期（可与状态筛选组合）
- 状态码筛选：输入 `401` 等过滤指定失败码的 Key
- 搜索：ID / 备注 / 地址
- 实时显示筛选后数量：`显示 X / Y 个`
- 批量：勾选卡片 → 批量重置 / 批量屏蔽

### 流量趋势图
24 小时 / 7 天 / 30 天切换，每小时柱状图，X 轴标签密度自适配。
屏蔽 Key 的流量也纳入趋势图统计。

### Key 卡片
脱敏显示（点击明文切换）、重置类型徽章、并发徽章、健康评分进度条、折叠按钮、冷却倒计时、统计指标（请求数/流量/延迟/P50-P95-P99/滑动成功率/费用）、日/小时明细、失败码悬停中文含义、最后失败时间、活跃 Key 发光高亮、锁死 Key 紫色标记

### 状态栏快捷操作
- 🔍 测试连通性
- 🔄 重置冷却/废弃/锁死
- 🔇 屏蔽此 Key

### 一键折叠
📂 按钮一键折叠/展开全部卡片

### 日志查看器
最近 2000 条请求记录，WebSocket 实时推送（日志弹窗打开时自动追加），时间/Key/方法/模型/路径/状态码/流量/延迟。
支持筛选：按 Key 序号、状态码（支持 `4xx` `5xx` 通配）、模型名子串、时间范围（5 分钟/15 分钟/1 小时/全部）。
支持 CSV 导出当前筛选结果。

### Key 管理
增删改、屏蔽/取消屏蔽、软删除（`status="deleted"` 保留在 JSON）、重置冷却状态、设置每周重置日（周一~周日或自动）、搜索/分组/拖拽排序、全选批量操作、批量导入 CSV、单 Key 连通性测试

### 系统配置
Webhook URL、价格参数、桌面通知/声音开关、🔄 重启代理按钮

## API 接口

| 接口 | 方法 | 说明 |
|---|---|---|
| `/` 或 `/dashboard` | GET | 监控面板 HTML |
| `/__status` | GET | JSON 状态（所有 Key 的完整指标） |
| `/__keys` | GET | 读取 keys.json（富化 `_locked`/`_failCode`/`_available` 字段） |
| `/__keys` | PUT | 写入 keys.json（自动重载） |
| `/__config` | GET | 读取 config.json |
| `/__config` | PUT | 写入 config.json（自动重载） |
| `/__reset-key` | POST | 重置指定 Key 的冷却/废弃状态（`{"idx": 1}`） |
| `/__apply-test-result` | POST | 应用批量测试结果：`{"idx":1, "failCode":429}` → markFailure；`failCode=null/200` → 清空冷却（`{"idx":1, "failCode":null}`） |
| `/__test-key` | POST | 单 Key 连通性测试（`{"key":"sk-...","url":"https://..."}`） |
| `/__patch-key-status` | POST | 修改 Key 状态（`{"idx":1,"status":"shielded"}`） |
| `/__restart` | POST | 热重启代理进程（新进程启动后旧进程退出） |
| `/__logs` | GET | 请求日志（`?key=11&status=502&model=gpt-5.6-sol&since=ts&until=ts&limit=200&offset=0&format=csv`，支持 `4xx`/`5xx` 通配） |
| `/__export-logs` | GET | 导出历史日志（`?date=2026-07-10&key=11&status=502&model=gpt-5.6-sol&format=csv`，无 date 时返回内存日志） |
| `/__export` | GET | CSV 导出统计报表 |
| `/__pathstats` | GET | 按路径/模型的请求分布 |
| `/metrics` | GET | Prometheus 格式指标 |
| `ws://localhost:3456/` | WS | WebSocket 实时推送 |

### /__status 字段说明

| 字段 | 类型 | 说明 |
|---|---|---|
| `idx` | int | Key 序号 |
| `key` | string | 脱敏显示（前 6 + ... + 后 4） |
| `url` | string | 中转地址 |
| `reset` | string | daily / weekly / never |
| `remark` | string | 备注 |
| `available` | bool | 当前可用（`!inCooldown()`） |
| `status` | string | active / discarded / locked |
| `failCode` | int/null | 上次失败码 |
| `failTime` | int/null | 上次失败时间戳 |
| `failPeriod` | string/null | 失效周期标识 |
| `failCount` | int | 连续失败计数（仅 lockFailCodes 中的错误码） |
| `locked` | bool | 是否被自动锁死 |
| `active` | bool | 当前是否有请求在处理 |
| `activeRequests` | int | 当前并发请求数 |
| `healthScore` | int | 0-100 |
| `avgDuration` | int | 平均延迟 (ms) |
| `avgTtfb` | int | 平均首字节 (ms) |
| `p50` / `p95` / `p99` | int/null | 延迟百分位 (ms) |
| `sliding5mRate` | float/null | 5 分钟滑动成功率 |
| `sliding1hRate` | float/null | 1 小时滑动成功率 |
| `totalCost` | float | 累计估算费用 (USD) |
| `totalRequests` / `successRequests` / `failRequests` | int | 请求计数 |
| `inputBytes` / `outputBytes` | int | 累计流量 |
| `lastUsed` | int/null | 最后使用时间戳 |
| `daily` | object | 按日统计 `{"YYYY-MM-DD": {...}}` |
| `hourly` | object | 按小时统计 `{"YYYY-MM-DD-HH": {...}}` |

### /metrics Prometheus

| 指标名 | 类型 | 标签 | 说明 |
|---|---|---|---|
| `codex_proxy_accounts_total` | gauge | — | 总 Key 数 |
| `codex_proxy_keys_active` | gauge | — | 当前可用 Key 数 |
| `codex_proxy_queue_depth` | gauge | — | 请求队列深度 |
| `codex_proxy_key_requests_total` | counter | key, url | 累计请求数 |
| `codex_proxy_key_bytes_total` | counter | key, type(input/output) | 累计字节 |
| `codex_proxy_key_health_score` | gauge | key | 健康评分 |
| `codex_proxy_request_queue_max_wait_seconds` | gauge | — | 队列超时设置 |

### WebSocket 协议

连接后自动推送：

```json
{"type": "status", "data": [...]}
{"type": "notification", "notificationType": "all_keys_failed", "time": "..."}
```

WebSocket 连接失败时前端自动降级为 HTTP 轮询（每 5 秒）。

## 失败码含义

面板卡片悬停失败码显示中文含义：

| 状态码 | 含义 |
|---|---|
| 401 | API Key 无效或已过期 |
| 402 | 额度不足，账号已欠费 |
| 403 | 权限不足，Key 无访问权限 |
| 429 | 请求过频繁，触发了速率限制 |
| 500 | 上游服务器内部错误 |
| 502 | 上游网关错误 |
| 503 | 服务暂时不可用 |
| 504 | 上游超时 |

## 重启代理

两种方式：

1. **面板操作**：配置弹窗 → 🔄 重启代理 → 确认
2. **命令行**：`pkill -f "node.*proxy\.js" && nohup node proxy.js &`

`POST /__restart` 使用 `spawn(..., {detached: true})` 启动新进程后立即退出当前进程，
确保重启期间正在处理的请求由 codex 自动重试。

## config.json 系统配置

```json
{
  "webhookUrl": "https://hooks.example.com/webhook?key=your_key_here",
  "prices": { "inputPer1M": 5, "outputPer1M": 15 },
  "bytesPerToken": 3,
  "notifications": { "sound": true, "desktop": true },
  "autoRecover": true,
  "autoRecoverInterval": 1,
  "autoRecoverCodes": [401,402,403,429,500,502,503,504],
  "autoRecoverDiscarded": false,
  "roundRobin": false,
  "enableAutoLock": true,
  "lockAfterFailCount": 3,
  "lockFailCodes": ["401","403"],
  "logFile": true,
  "logRetentionDays": 7,
  "logDetail": "full"
}
```

| 字段 | 说明 |
|---|---|
| `webhookUrl` | 全部 Key 失效时 POST JSON 告警（兼容企业微信/钉钉/Telegram） |
| `prices.inputPer1M` | 输入价格（$/百万 token） |
| `prices.outputPer1M` | 输出价格（$/百万 token） |
| `bytesPerToken` | 每 token 近似字节数（默认 3，中文约 1.5-2，英文约 4） |
| `notifications.sound` | 是否播放声音提醒 |
| `notifications.desktop` | 是否发送桌面通知 |
| `autoRecover` | 是否启用自动恢复冷却 Key |
| `autoRecoverInterval` | 探测间隔（小时，最小 0.5） |
| `autoRecoverCodes` | 需要检测的失败码数组，如 `[401,429,500]` |
| `autoRecoverDiscarded` | 是否也检测 `discarded` 状态的 Key |
| `roundRobin` | 是否启用轮询均摊模式（见「Key 调度顺序」） |
| `enableAutoLock` | 是否启用自动锁死（true/false，默认 true） |
| `lockAfterFailCount` | 连续 N 次失败后自动锁死（默认 3） |
| `lockFailCodes` | 只有这些错误码会计入连续失败计数（默认 `["401","403"]`） |
| `logFile` | 是否启用文件日志（true/false，默认 true）。关闭后仅内存缓存 2000 条，不写磁盘 |
| `logRetentionDays` | 日志文件保留天数（默认 7）。设为 0 关闭自动清理 |
| `logDetail` | 日志详情级别：`"full"`（完整，含模型名）或 `"basic"`（简洁，不含模型名） |

## 自动恢复冷却 Key

后台定时检测冷却中的 Key，通过 `GET /v1/models` 探测连通性，恢复成功后自动清除冷却/废弃状态。

### 行为

- 跳过不在 `autoRecoverCodes` 列表中的失败码
- `discarded` 状态仅在 `autoRecoverDiscarded=true` 时检测
- 探测成功（200 OK）→ 自动清除 `failCode`/`failTime`/`failPeriod`，若 `discarded` 恢复 `active`
- 日志输出 `[proxy] auto-recover: #N recovered`
- 配置保存后立即生效，无需重启（定时器自动重置）

## 费用估算

```
tokens ≈ bytes / bytesPerToken
费用 = (inputTokens / 1_000_000) × inputPer1M + (outputTokens / 1_000_000) × outputPer1M
```

> ⚠️ 精确 token 追踪不可用（上游中转不返回 `usage` 字段），此为字节->token 估算。

## Webhook 告警格式

```json
{
  "event": "all_keys_failed",
  "time": "2026-06-17T12:00:00.000Z",
  "accounts": 5,
  "proxy": { "accounts": 5, "queueDepth": 3 }
}
```

## 请求队列

所有可用 Key 均冷却时，新请求进入缓冲区：
- 队列内请求在 Key 恢复时自动处理
- 最长等待 30 秒 → 超时返回 `503`
- 请求端主动断开 → 自动移除

## systemd 服务管理

```bash
# 安装
bash install.sh

# 管理
systemctl status codex-proxy    # 状态
journalctl -u codex-proxy -f    # 实时日志
systemctl restart codex-proxy   # 重启
systemctl stop codex-proxy      # 停止
systemctl disable codex-proxy   # 取消开机自启
```

## install.sh 工作原理

1. 用 `readlink -f "$0"` 获取脚本所在目录作为 `PROXY_DIR`
2. `npm install` 安装 `ws` 依赖
3. 创建默认 `config.json` 和 `state.json`（如不存在）
4. 用 `sed "s|{{PROXY_DIR}}|$PROXY_DIR|g"` 替换服务模板中的占位符 → 复制到 `/etc/systemd/system/` → `systemctl daemon-reload` `enable` `restart`
5. 在 `${WRAPPER_DIR:-$HOME/bin}` 创建 `codex` 包装脚本（`CODEX_BIN` 环境变量可指定 codex.js 路径）

环境变量覆盖：
```bash
WRAPPER_DIR=/custom/bin CODEX_BIN=/opt/codex/bin/codex.js bash install.sh
```

## 常见问题

**Q: 启动后 `http://localhost:3456/` 没反应？**
A: 代理是否运行？检查 `ps aux | grep proxy.js`。没运行则 `node proxy.js &`。

**Q: WSL2 中面板打不开？**
A: 用 `localhost` 而非 `127.0.0.1`。WSL2 的 `127.0.0.1` 指向 Windows 自身回环。

**Q: 双击 dashboard.html 无法连接？**
A: 独立面板需要代理运行中。使用 `http://localhost:3456/` 获取完整功能。

**Q: 如何屏蔽 Key 又不删除？**
A: 管理界面点击 🔇，或 keys.json 设 `"status": "shielded"`。

**Q: 删除的 Key 怎么恢复？**
A: 前端删除是软删除（设 `status="deleted"`），Key 仍在 keys.json。编辑 keys.json 删除 `status` 字段或改回 `"active"` 即可。

**Q: Key 被自动锁死了怎么恢复？**
A: 管理弹窗找到 🔒 锁死的 Key，点击 🔓 解锁按钮，或手动调用 `POST /__reset-key {"idx": N}`。可在配置中关闭自动锁死（取消勾选「启用自动锁死」）或调整阈值 `lockAfterFailCount`。

**Q: 费用估算不准？**
A: 调整 `config.json` 中 `bytesPerToken` 和 `prices`。不同模型 token 密度不同。

**Q: 面板显示「加载中」？**
A: 检查浏览器控制台（F12）是否有 JS 错误。打开 `http://localhost:3456/` 而非 `file://`。如代理刚重启，等待几秒后刷新。

**Q: 面板上批量操作怎么用？**
A: 勾选卡片左上角 checkbox → 顶部操作栏出现 → 点击批量重置或批量屏蔽。

**Q: 管理弹窗如何批量导入 Key？**
A: 点击 📋 按钮 → 粘贴每行一个 `sk-xxx url 周期 备注` → 确定。

**Q: 配置弹窗的「重启代理」按钮点不了？**
A: 该功能在更新 proxy.js 后需重启代理才能生效。首次使用需命令行重启一次。

## License

MIT
