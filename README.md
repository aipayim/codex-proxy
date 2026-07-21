# OpenAPI Multi-Key Proxy

## 核心能力

**三向协议转换**：本代理自动在 OpenAI Responses API、Anthropic Messages API、OpenAI Chat Completions API 三者之间双向转换。下游任意客户端（Codex CLI、Claude Code CLI、Chat 应用）可连接任意上游模型（OpenAI、Anthropic、DeepSeek、Kimi、Qwen、Gemini、Grok 等），零配置自动检测。

**超越 cc-switch 之处**：
- **运行时代理**，非配置管理工具——cc-switch 是本地配置切换器，本代理是网络层透明代理，无需修改客户端配置即可工作
- **三向全协议转换**：cc-switch 仅支持 Anthropic→OpenAI 单向；本代理支持 Responses↔Chat↔Messages 三向互转 + 混合账号 2 阶段 fallback
- **多 Key 智能调度**：基于健康评分、冷却状态、滑动成功率、延迟百分位的自动路由，非简单的轮询或手动选择
- **系统级容灾**：自动锁死、自动恢复、废弃检测、队列缓冲、并发管控——无需人工干预
- **完整监控面板**：实时仪表盘、按 Key 统计、流量趋势、请求日志（含 sparkline/错误聚类/模型分布）、Prometheus 指标、Webhook/桌面通知

**提供商标识智能转换**：不同 API 供应商的协议差异自动适配：
- **阿里云百炼 (DashScope)** 的 OpenAI 兼容接口支持完整 `cache_control` 透传（Messages 协议中的缓存标记自动保留到 Chat 协议），无需手动配置
- Bailian DeepSeek / QwQ 等深度思考模型的 `reasoning_content` 字段在 Messages ↔ Chat 协议转换中自动映射为 `thinking` / `thinking_delta` 内容块
- Streaming 流中 `reasoning_content` → `thinking_delta`、非流响应中 `message.reasoning_content` → `thinking` content block 双向转换
- `thinking` / `enable_thinking` 参数在 Bailian 上游自动启用，对不兼容的标准 Chat 上游自动剥离
- 提供商标识可扩展：新增供应商只需扩展 `CACHE_CONTROL_COMPATIBLE_HOSTS` 列表

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
├── watchdog.sh           # 进程守护脚本（WSL 无 systemd 环境用），每 10 秒检测崩溃并自动重启
├── start-proxy.sh        # 一键启动 watchdog + 代理（替代 systemctl start）
├── resume-codex.sh       # autoResume 辅助脚本：通过 cmd.exe 创建 Windows 可见终端运行 wsl 命令
├── proxy.pid             # 自动生成，记录代理进程 PID（watchdog 依赖此文件检测存活）
└── README.md             # 本文件
```

## 新机器部署清单

以下清单列出完整部署所需的所有文件和配置，AI agent 可逐项执行：

### 需从源码复制的文件

| 文件 | 说明 | 安装方式 |
|------|------|----------|
| `proxy.js` | 核心代理（含内嵌面板） | 复制到目标目录即可 |
| `dashboard.html` | 独立面板备用 | 同目录放置 |
| `package.json` | npm 依赖声明 | 复制后 `npm install` |
| `watchdog.sh` | 进程守护（WSL2） | 复制 + `install.sh` 替换路径 |
| `start-proxy.sh` | watchdog 启动器 | 复制 + `install.sh` 替换路径 |
| `resume-codex.sh` | autoResume 辅助 | 复制到同目录 |

### 系统级文件（install.sh 自动生成）

| 目标路径 | 内容 | 说明 |
|----------|------|------|
| `/usr/local/bin/codex-watchdog.sh` | WSL 开机引导脚本 | 由 `install.sh` 根据检测到的 `$PROXY_DIR` 生成 |
| `/etc/wsl.conf` | WSL 配置 | 写入 `[boot] command` 指向 watchdog 引导脚本 |
| `~/bin/codex` | codex 包装脚本 | 自动拉起代理 + watchdog，然后 `exec` 真实 codex |
| `~/.bashrc` | PATH 追加 | 确保 `~/bin` 在 PATH 中（含登录 shell） |

### 配置初始化（install.sh 自动创建默认值）

| 文件 | 初始状态 | 后续操作 |
|------|----------|----------|
| `config.json` | 全套默认参数（autoResume/autoRecover/autoLock 等） | 通过面板「系统配置」调整 |
| `state.json` | 空状态 | 自动写入运行时数据 |
| `keys.json` | `[]` | **必须**通过面板「管理 Key」或手动编辑填入 API Key |

### 依赖检查

- Node.js ≥ 16（`node -v`）
- npm（随 Node.js 自带）
- 仅 `ws` 依赖（`npm install` 自动安装）

---

## AI Agent 一键安装

以下步骤由 `install.sh` 自动完成，也可由 AI 或用户手动逐条执行：

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
#   resetHours: 可选字段，1-168。仅对 hourly 生效，指定每 N 小时重置周期（默认 5）。
#   activatedAt: 自动生成，首次启用的毫秒时间戳。删除 state.json 后不丢失，编辑面板保存时自动保留。
#   maxReqPerMin: 可选字段，覆盖全局 maxRequestsPerMin 的每分钟请求数上限。
#   maxTokPerMin: 可选字段，覆盖全局 maxTokensPerMin 的每分钟 token 上限。
#
# 示例：
# [
#   {"key": "sk-xxx...", "url": "https://api.openai.com/v1",   "reset": "weekly", "remark": "主力 Key"},
#   {"key": "sk-yyy...", "url": "https://api.provider.com/v1", "reset": "daily",  "remark": "备用额度卡", "models": ["gpt-5.5", "gpt-5.4-mini"]},
#   {"key": "sk-zzz...", "url": "http://proxy.example.com:8080", "reset": "never",  "remark": "一次性", "status": "shielded"}
# ]
```

> 💡 推荐中转代理（注册送额度）：[https://api.fenno.ai/register?aff=FBGPVRCAA765](https://api.fenno.ai/register?aff=FBGPVRCAA765)

### 3. 一键安装（推荐）

```bash
bash install.sh
```

脚本自动执行（详见「install.sh 工作原理」）：
- 安装 npm 依赖
- 创建默认配置文件
- 检测 WSL2 → 安装 watchdog 守护 + 开机自启
- 检测 Linux systemd → 安装系统服务
- 创建 `~/bin/codex` 包装脚本（自动拉起代理）
- 配置 PATH 环境变量

### 4. 配置 Codex CLI 使用本地代理

```bash
# 创建或编辑 ~/.codex/config.toml
mkdir -p ~/.codex
cat > ~/.codex/config.toml << 'EOF'
base_url = "http://localhost:3456"
EOF
```

### 5. 验证

```bash
# 启动代理
bash start-proxy.sh

# 检查状态
curl http://localhost:3456/__status

# 打开监控面板
# 浏览器访问 http://localhost:3456/

# 使用 codex（包装脚本自动确保代理在运行）
codex
```

## Key 调度顺序

`pickKey()` 采用**双层优先级调度**：

| 层级 | 依据 | 顺序 |
|------|------|------|
| **第一层** | 额度重置周期 `reset` | daily → weekly → never（hourly 同 daily 优先级） |
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

例：`priority: 10` 的 daily Key 在 daily 组内轮询使用，冷却后自动切换到 daily 组内 `priority: 0` 的 Key 轮询；daily 组全部冷却后切入 weekly 组。

> **轮询均摊模式下**（`roundRobin: true`），weekly 组内部进一步按 `resetDay` 拆分为亚组（周一/周二/…/周日/自动）。每亚组内的 Key **全部冷却后才切入下一亚组**，同一周内不同天的 Key 不会混杂使用，流量更均衡。
> 
> 亚组内再按 `priority` 拆分为多层，每层独立轮询。每次 pickKey 从最高 priority 层开始扫描，有可用即取，用尽（全部冷却）后才切到低 priority 层。高 priority 层的 Key 一旦恢复就立即被重新使用，不会因低 priority 层未用完而被忽略。

### 每周 Key 按到期日排序

可在系统配置中勾选「每周 Key 按到期日排序」。启用后 `pickKey()` 从 weekly 组选取 Key 时，不再按 priority 排序，而是按**下次重置时间最近优先**：

| 当前日 | 选取顺序 |
|--------|----------|
| 周一 | 周二 → 周三 → ... → 周日 → 周一（当天）→ 无 resetDay |
| 周三 | 周四 → 周五 → ... → 周二 → 周三（当天）→ 无 resetDay |

算法：计算每个 weekly Key 的 `resetDay` 距今天数，**距离下次重置越近的 Key 越先使用**，当天重置的 Key 排在同组最后，`resetDay` 未设置的 Key 排最后。

> **轮询均摊模式下**（`roundRobin: true`），weekly 组会按 `resetDay` 拆为独立亚组，按到期日顺序逐组亚组。每个亚组内再按 `priority` 分层，每层独立轮询。从高 priority 层到低 priority 层逐层扫描，当前层全部冷却才切到下一层，高 priority 一旦恢复立即切回。

配置字段：`weeklySortBy`（`"priority"` / `"expiry"`），默认 `"priority"`。

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
- **备注切换**：点击表头 **备注 🔄** 在「编辑备注」与「显示首次启用+时长」间切换，切换前自动保存当前编辑内容
- **排序**：下拉选择「默认顺序」或「按重置日（周一→周日）」或「首次启用（早→晚）」或「使用时长（长→短）」
- **状态码筛选**：输入 `401` 等过滤指定失败码的 Key
- **状态筛选**：下拉选择 全部/可用/冷却中/废弃/锁死
- **隐藏已屏蔽**：🙈 按钮一键隐藏所有「已屏蔽」的 Key，方便对非屏蔽 Key 批量操作。状态跨模态打开保持。再次点击 🙉 恢复显示
- **数量显示**：实时显示 `共 X 个，筛选后 Y 个`，并单独统计已屏蔽数量
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
- 排序：默认 / 按到期日（最近→最远）/ 首次启用（早→晚）/ 使用时长（长→短）/ 健康评分 / 平均延迟 / 5 分钟成功率
- 筛选：全部 / 可用 / 冷却中 / 废弃 / 🔒 锁死（与重置筛选可组合使用）
- 重置筛选：每日重置 / 每周重置 / 永不过期（可与状态筛选组合）
- 状态码筛选：输入 `401` 等过滤指定失败码的 Key
- 搜索：ID / 备注 / 地址
- 实时显示筛选后数量：`显示 X / Y 个`
- 批量：勾选卡片 → 批量操作栏出现（含「全选」「全取消」按钮）/ 批量重置 / 批量屏蔽 / ⚡优先使用（逐个使用勾选的key，用完恢复常态）/ ⭕优先轮询（勾选的key间轮询，全冷却后恢复常态）

### 流量趋势图
24 小时 / 7 天 / 30 天切换，每小时柱状图，X 轴标签密度自适配。
屏蔽 Key 的流量也纳入趋势图统计。
点击标题可在「📊 流量趋势」（按字节数）和「📈 次数趋势」（按请求数）间切换，柱高自动按当前模式归一化。

### Key 卡片
脱敏显示（点击明文切换）、重置类型徽章（每周 Key 额外显示具体周几/自动）、并发徽章、批量优先徽章、健康评分进度条、折叠按钮、冷却倒计时、统计指标（请求数/流量/延迟/P50-P95-P99/滑动成功率/费用）、首次启用时间+启用至今、日/小时明细、失败码悬停中文含义、最后失败时间、活跃 Key 发光高亮、锁死 Key 紫色标记

### 状态栏快捷操作
- 🔍 测试连通性
- 🔄 重置冷却/废弃/锁死
- 🔇 屏蔽此 Key

### 一键折叠
📂 按钮一键折叠/展开全部卡片

### 日志查看器
顶部统计卡片展示全局指标：总请求数、成功率（按百分比着色：≥95% 绿、≥80% 黄、<80% 红）、平均耗时、P95、P99、4xx 数、5xx 数、超时数。
统计卡片下方为 **30 分钟请求量 sparkline 图**（蓝色柱=成功请求，红色柱=含错误的分钟，悬停显示具体数字）和 **模型分布行**（Top 8 模型：请求数、错误数、平均耗时，点击 Key 号可查看该 Key 的独立统计弹窗）。
可折叠 **错误分布区**：按错误码聚类显示次数（超时/4xx/5xx），点击展开详情。
表格列：序 / 时间（可排序） / Key 序号（可排序，点击弹出该 Key 独立统计卡片） / 上游 URL / HTTP 方法 / 模型（可排序） / 路径 / 状态码（可排序） / 上行流量 / 下行流量 / 耗时（可排序） / 首字节耗时。
事件行（紫色/绿色/红色/橙色背景）：协议转换（带 `R→C`/`M→C`/`C→M` 方向标签）、自动恢复、自动锁死、废弃——以不同颜色区分。
**点击行展开详情**：显示完整时间戳、模型、协议转换标志、URL、请求/响应字节数等详细信息。
实时推送：WebSocket 自动追加新日志到当前页末尾，同时更新 sparkline/模型分布/错误聚类。
分页：每页 200 条，上一页/下一页按钮 + 页码显示。
支持筛选：按 Key 序号、状态码（支持 `4xx` `5xx` 通配）、模型名子串、时间范围（5 分钟/15 分钟/1 小时/24 小时/7 天/30 天/自定义范围）。
自定义范围使用 `<input type="datetime-local">` 选择起止时间，选中「自定义范围」后显示输入框。
选择 24h/7d/30d 或自定义范围时，服务端自动读取 `logs/` 目录下的历史 JSONL 文件与内存日志合并去重，支持回溯已保存的日志文件。
支持 CSV 导出当前筛选结果。
服务端 `GET /__logs` 返回 `{ entries: [...], stats: { total, totalAll, successRate, p95, p99, avgDuration, error4xx, error5xx, errorTimeout } }` 结构（`totalAll` 为全量行数，用于分页计算）。

### Key 管理
增删改、屏蔽/取消屏蔽、软删除（`status="deleted"` 保留在 JSON）、重置冷却状态、设置每周重置日（周一~周日或自动）、搜索/分组/拖拽排序、全选批量操作、批量导入 CSV、单 Key 连通性测试
### 系统配置

Webhook URL、价格参数、桌面通知/声音开关、🔄 自动恢复冷却 Key（间隔/固定/快速三种模式独立配置）、失败码列表、是否检测 discarded Key、🔁 轮询均摊、📅 每周 Key 按到期日排序、🧬 闲置自动恢复（autoResume）、项目列表（项目名/WSL 路径/启动命令 动态增减）、cmd.exe 路径、🔒 自动锁死阈值与监控码、日志文件/保留天数/详情级别、🔄 重启代理按钮

## API 接口

| 接口 | 方法 | 说明 |
|---|---|---|
| `/` 或 `/dashboard` | GET | 监控面板 HTML |
| `/__status` | GET | JSON 状态（所有 Key 的完整指标） |
| `/__keys` | GET | 读取 keys.json（富化 `_locked`/`_failCode`/`_activatedAt`/`_available` 字段） |
| `/__keys` | PUT | 写入 keys.json（自动重载；自动清除因 reset/resetDay 变更导致的过期 failCode） |
| `/__config` | GET | 读取 config.json |
| `/__config` | PUT | 写入 config.json（自动重载） |
| `/__reset-key` | POST | 重置指定 Key 的冷却/废弃状态（`{"idx": 1}`） |
| `/__apply-test-result` | POST | 应用批量测试结果：`{"idx":1, "failCode":429}` → markFailure；`failCode=null/200` → 清空冷却（`{"idx":1, "failCode":null}`） |
| `/__test-key` | POST | 单 Key 连通性测试（`{"key":"sk-...","url":"https://..."}`），返回 `model`（逗号分隔可用模型列表）和 `modelCount`（模型数量） |
| `/__patch-key-status` | POST | 修改 Key 状态（`{"idx":1,"status":"shielded"}`） |
| `/__boost-batch` | POST | 批量优先：`{"mode":"use","idxs":[1,3,5]}`（逐个使用）或 `{"mode":"roundrobin","idxs":[1,3,5]}`（轮询）或 `{"mode":""}`（取消） |
| `/__restart` | POST | 热重启代理进程（新进程启动后旧进程退出） |
| `/__logs` | GET | 请求日志（`?key=11&status=502&model=gpt-5.6-sol&since=ts&until=ts&limit=200&offset=0&format=csv`，支持 `4xx`/`5xx` 通配） |
| `/__export-logs` | GET | 导出历史日志（`?date=2026-07-10&key=11&status=502&model=gpt-5.6-sol&format=csv`，无 date 时返回内存日志） |
| `/__export` | GET | CSV 导出统计报表 |
| `/__pathstats` | GET | 按路径/模型的请求分布 |
| `/metrics` | GET | Prometheus 格式指标 |
| `/v1/responses` | POST | **协议转换**：接收 Codex CLI 的 Responses API 请求，自动转换为 Chat Completions 格式转发给上游（非 OpenAI / ofox），并将响应流式转换回 Responses 格式 |
| `/v1/messages` | POST | **协议转换**：接收 Claude Code CLI 的 Messages API 请求，自动转换为 Chat Completions 格式转发给上游（非 Anthropic），并将响应流式转换回 Messages 格式 |
| `/v1/chat/completions` | POST | **协议转换**：接收 Chat Completions 请求，如上游为 Anthropic 则自动转换为 Messages 格式转发，并将响应流式转换回 Chat 格式；非 Anthropic 上游直接透传 |
| `ws://localhost:3456/` | WS | WebSocket 实时推送 |

### 协议转换说明

协议转换层使任意下游客户端可连接任意上游模型：

| 下游客户端 | 请求路径 | 转换方向 | 支持的上游 |
|---|---|---|---|
| Codex CLI | `/v1/responses` | Responses → Chat → Responses | 任意 OpenAI 兼容 API |
| Claude Code CLI | `/v1/messages` | Messages → Chat → Messages | 任意 OpenAI 兼容 API |
| Chat 客户端 | `/v1/chat/completions` | Chat → Messages → Chat | Anthropic |

- 转换基于路径（`/v1/responses`, `/v1/messages`, `/v1/chat/completions`）自动触发，无需配置
- 上游检测基于 `keys.json` 中的 `url` 字段：`api.openai.com`/`api.ofox.ai` = Responses 原生，`api.anthropic.com` = Messages 原生，其余 = Chat 通用
- 所有上游模型（含 OpenAI、Kimi、DeepSeek、Grok、Qwen、Gemini 等）均支持三种下游客户端
- 当前仅支持流式（`stream: true`）请求；非流式请求将按流式处理返回 SSE

#### 混合账号 fallback（Chat 客户端 → Anthropic）

`/v1/chat/completions` 路由在存在 Anthropic 账号时自动启用两阶段 fallback：

1. **Phase 1**：优先尝试所有 Anthropic 账号，body 自动转换为 Messages 格式，响应流式转换回 Chat 格式
2. **Phase 2**：若所有 Anthropic 账号均失败（冷却/限流/超时），自动使用原始 Chat 格式重试非 Anthropic 账号

Anthropic 账号和非 Anthropic 账号可共存，无需额外配置。

#### Responses→Chat 支持字段

`/v1/responses` → Chat 转换支持以下参数映射：

| Responses 字段 | Chat 字段 | 说明 |
|---|---|---|
| `model` | `model` | 模型名 |
| `input` | `messages` | 支持 string / array 格式 |
| `instructions` | `system message` | 转为 system role |
| `max_output_tokens` | `max_tokens` | 最大输出 Token |
| `temperature` | `temperature` | 采样温度 |
| `top_p` | `top_p` | 核采样 |
| `stop` | `stop` | 停止序列 |
| `tools` | `tools` | 工具定义 |
| `tool_choice` | `tool_choice` | 工具选择策略 |
| `metadata` | `metadata` | 自定义元数据 |

不支持（丢弃）：`include`、`previous_response_id`、`store`

### /__status 字段说明

| 字段 | 类型 | 说明 |
|---|---|---|
| `idx` | int | Key 序号 |
| `key` | string | 脱敏显示（前 6 + ... + 后 4） |
| `url` | string | 中转地址 |
| `reset` | string | daily / weekly / never / hourly |
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
| `boostedBatch` | array | 批量优先中的 Key 序号列表（始终为 1-based） |
| `boostedBatchMode` | string | 批量优先模式：`"use"` / `"roundrobin"` / `""` |

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
{"type": "status", "data": [...], "boostedIdx": -1, "boostedBatch": [], "boostedBatchMode": ""}
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

`POST /__restart` 先调用 `server.close()` 释放端口，再 `spawn(..., {detached: true})` 启动新进程，最后退出当前进程，避免多进程端口冲突。
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
  "autoResumeProjects": [],
  "cmdPath": "/mnt/c/Windows/System32/cmd.exe",
  "weeklySortBy": "priority",
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
| `autoRecoverDaily` | 是否启用固定时间检测（true/false，默认 false） |
| `autoRecoverDailyDays` | 每 N 天检测一次（默认 1） |
| `autoRecoverDailyHour` | 检测时间：时（0-23，默认 8） |
| `autoRecoverDailyMinute` | 检测时间：分（0-59，默认 0） |
| `autoRecoverPoll` | 是否启用快速恢复（true/false，默认 false）。Key 出现指定失败码时，自动启动短间隔轮询检测，全部恢复后停止 |
| `autoRecoverPollInterval` | 轮询间隔（分钟，默认 5，最小 1） |
| `autoRecoverPollCodes` | 触发的失败码数组，如 `[500,502,503,504]`。Key 出现其中任意状态码即激活快速轮询 |
| `autoRecoverDelays` | 检测间隔数组（毫秒），默认 `[800]`。所有检测模式共用，每个 Key 测试完随机选一个值作为下一 Key 的等待时间。最多 10 个，范围 100–10000。推荐 `[800,1200,500]` 模拟人工操作节奏，降低批量风控概率 |
| `rateLimit` | 是否启用分钟级限速（true/false，默认 true） |
| `maxRequestsPerMin` | 单个 Key 每分钟最大请求数（默认 10）。可在 keys.json 中按 Key 覆盖（`maxReqPerMin`） |
| `maxTokensPerMin` | 单个 Key 每分钟最大 token 数（默认 0=不限）。可在 keys.json 中按 Key 覆盖（`maxTokPerMin`） |
| `defaultResetHours` | `hourly` 类型的默认重置周期（小时，默认 5）。可在 keys.json 中按 Key 覆盖（`resetHours`） |
| `autoResume` | 是否启用闲置自动恢复（true/false，默认 false） |
| `autoResumeIdleMinutes` | 空闲阈值（分钟，默认 10） |
| `autoResumeDebounceMinutes` | 防抖间隔（分钟，默认 3） |
| `autoResumeProjects` | 项目列表数组，每项含 name/path/cmd，最多 10 个 |
| `cmdPath` | cmd.exe 路径（默认 `/mnt/c/Windows/System32/cmd.exe`） |
| `weeklySortBy` | weekly 组排序方式：`"priority"`（按 priority+索引）或 `"expiry"`（按最先到期先使用） |
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
- 批量检测时按 `autoRecoverDelays` 配置的间隔串行执行，每 Key 测试完随机选一个间隔值再测下一个（默认 800ms），避免同时批量请求触发上游风控

### 三种模式

| 模式 | 说明 |
|------|------|
| **定时检测并恢复**（间隔模式） | 每 N 小时检测一次（默认 1 小时），基于 `setInterval` |
| **固定时间检测**（日历模式） | 每 N 天的指定 HH:MM 检测一次（默认每天 08:00），基于 `setTimeout` 链 |
| **快速恢复**（事件驱动模式） | 当 Key 出现 `autoRecoverPollCodes` 中的状态码（默认 500/502/503/504）时，自动以短间隔（默认 5 分钟）轮询检测，全部恢复后自动停止。再出现时再自动激活，基于 `setTimeout` 链 + `markFailure` 事件钩子 |

三种模式可独立启用/关闭，也可同时开启。同时开启时面板上显示三条独立的倒计时。失败码和 discarded 配置由三种模式共用。

## 闲置自动恢复（autoResume）

代理空闲超过阈值时，自动在 Windows 可见终端中重新打开项目终端窗口（适用于 WSL2 环境，确保 codex CLI 运行在可见窗口中）。

### 工作原理

1. 代理记录每次 codex CLI 转发请求的时间（`lastRequestTime`），仅 `/v1/*`、`/responses` 等实际 API 调用计入活动时间。仪表盘页面（`/`、`/dashboard`）、管理接口（`/__*`）、Prometheus 指标（`/metrics`）不重置空闲计时，即使 WebSocket 断开降级为 HTTP 轮询也不影响
2. 每 30 秒检测空闲时长，超过 `autoResumeIdleMinutes`（默认 10 分钟）且距上次恢复超过 `autoResumeDebounceMinutes`（默认 3 分钟）时触发
3. `checkAutoResume()` 遍历 `autoResumeProjects` 列表，为每个项目执行 `triggerResume()`
4. 通过 `cmd.exe /c start` 启动新的 Windows 可见 cmd 窗口 → 运行 wsl.exe → bash → 执行项目命令
5. 每个项目启动时写入 PID 到 `/tmp/codex-resume-<项目名>.pid`，再次触发时自动 kill 旧进程

### 配置字段

```json
{
  "autoResume": true,
  "autoResumeIdleMinutes": 10,
  "autoResumeDebounceMinutes": 3,
  "autoResumeProjects": [
    {"name": "project-a", "path": "/mnt/e/project-a", "cmd": "codex chat"},
    {"name": "project-b", "path": "/mnt/e/project-b", "cmd": "./run.sh"}
  ],
  "cmdPath": "/mnt/c/Windows/System32/cmd.exe"
}
```

| 字段 | 说明 |
|------|------|
| `autoResume` | 是否启用闲置自动恢复（true/false） |
| `autoResumeIdleMinutes` | 空闲阈值（分钟，默认 10）。代理无任何请求超过此分钟后触发 |
| `autoResumeDebounceMinutes` | 防抖间隔（分钟，默认 3）。防止频繁触发 |
| `autoResumeProjects` | 项目列表，最多 10 个。每个项目包含 `name`（显示名）、`path`（WSL 路径，支持 `E:\xxx` 格式自动转换）、`cmd`（要执行的命令） |
| `cmdPath` | cmd.exe 路径（默认 `/mnt/c/Windows/System32/cmd.exe`） |

### 面板状态

- **配置弹窗**：显示 `🧬 闲置恢复: 空闲 Xm，上次触发 Ym 前` 实时状态行
- **仪表盘**：工具栏右侧显示 `🧬空闲Xm/恢复Ym前`

### 路径自检

`path` 字段支持以下格式，保存配置时自动标准化：
- WSL Linux 路径：`/mnt/e/codex-proxy` → 不变
- Windows 路径：`E:\codex-proxy` → `/mnt/e/codex-proxy`
- 混合路径：自动转换为 WSL 绝对路径

### 依赖脚本

`resume-codex.sh` 位于 `codex-proxy/` 目录，通过 `cmd.exe /c start` + `wsl.exe` 打开可见终端窗口。
shell 命令中的单引号自动转义，确保安全执行。

### 注意事项

- **仅适用于 WSL2**：依赖 `cmd.exe` 和 `wsl.exe` 创建 Windows 可见终端，纯 Linux 环境无效
- **路径必须存在**：`path` 目录在 WSL 中必须可 `cd` 进入
- **命令不含交互输入**：自动打开的进程无 stdin 交互，适合 `codex chat`（持续输出模式）或 `node server.js` 等长时间运行命令
- **PID 文件**：存放于 `/tmp/codex-resume-*.pid`，系统重启后自动清理
- **重入保护**：防抖机制确保两次触发间隔至少 `autoResumeDebounceMinutes`

#### 快速恢复的工作原理

1. Key 获取到 500/502/503/504 等失败码 → `markFailure()` 检测到该码在 `autoRecoverPollCodes` 中且当前无 timer 运行 → 启动 `schedulePollRecover()`
2. 每 `autoRecoverPollInterval` 分钟（默认 5）：检查是否仍有 Key 持有匹配的失败码 → 有则调用 `GET /v1/models` 测试连通性，成功后自动清除冷却；无则停止（不留定时器）
3. 停止后如有 Key 再次出现匹配的失败码 → 重新激活（事件驱动，不依赖轮询）

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

## 进程守护

### systemd 环境（Linux 服务器）

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

服务模板 `codex-proxy.service` 已内置 `Restart=always` + `RestartSec=5`，进程崩溃后 systemd 自动拉起。

### WSL2 环境（无 systemd）

WSL2 默认不使用 systemd，使用内置的 watchdog 脚本实现同等守护能力：

```bash
# 一键启动 watchdog + 代理
bash start-proxy.sh

# 查看状态
curl http://localhost:3456/__status

# 停止代理（watchdog 会在 10 秒内重新拉起）
pkill -f 'node.*proxy\.js'

# 完全停止 watchdog + 代理
pkill -f watchdog.sh
```

#### 工作原理

| 组件 | 作用 |
|---|---|
| `watchdog.sh` | 每 10 秒检查 `proxy.pid` 中的 PID 是否存活。进程消失则自动 `nohup node proxy.js &` 拉起并写入日志 |
| `start-proxy.sh` | 前台启动 watchdog（手动用）。`bash start-proxy.sh --boot` 后台启动（WSL 开机用） |
| `proxy.pid` | `proxy.js` 启动时自动写入 `process.pid`，退出时自动清理 |
| `/etc/wsl.conf` | 已配置 `[boot] command = /usr/local/bin/codex-watchdog.sh`，Windows 启动 WSL 时自动加载 watchdog |

#### 开机自启

`/etc/wsl.conf` 已配置：

```ini
[boot]
command = /usr/local/bin/codex-watchdog.sh
```

`codex-watchdog.sh` 依次执行：修复 opencode 网络路由 → `start-proxy.sh --boot` → watchdog 驻留后台。

**使其生效**：需在 Windows PowerShell 中执行一次 `wsl --shutdown` 后重新打开 WSL 终端，或重启 Windows。

#### 资源占用

watchdog 99.9% 时间处于 `sleep 10` 阻塞态，**CPU 占用为 0**，内存约 **500KB**。

## install.sh 工作原理

`install.sh` 是全环境引导脚本，自动检测运行环境（WSL2 / Linux systemd）并执行对应操作：

### 执行流程

| 步骤 | 操作 | 说明 |
|------|------|------|
| 1 | `npm install` | 安装 `ws` 依赖 |
| 2 | 创建 `config.json` / `state.json` / `keys.json` | 仅文件不存在时创建（不覆盖已有配置） |
| 3 | 检查 Node.js ≥ 16 | 版本不足时退出 |
| 4 | 安装系统服务 | **WSL2**：创建 watchdog 引导脚本 + `/etc/wsl.conf`；**systemd**：安装 systemd service 并 `enable` `start` |
| 5 | 创建 `codex` 包装脚本 | 自动检测 `CODEX_BIN` 路径，写入 `~/bin/codex`；确保 `~/bin` 加入 `~/.bashrc` PATH；确保登录 shell 加载 `.bashrc` |
| 6 | 输出摘要 + 下一步指引 | 显示环境信息、文件位置、后续操作提示 |

### 环境变量覆盖

```bash
WRAPPER_DIR=/custom/bin CODEX_BIN=/opt/codex/bin/codex.js bash install.sh
```

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `WRAPPER_DIR` | `$HOME/bin` | 包装脚本输出目录 |
| `CODEX_BIN` | 自动检测 | 真实 codex CLI 路径。自动搜索 `/usr/lib/node_modules/@openai/codex/bin/codex.js`、`/usr/local/bin/codex`、`/opt/codex/bin/codex`、`$HOME/codex/bin/codex` 等常见位置 |

### WSL2 自动安装内容

| 文件 | 路径 | 说明 |
|------|------|------|
| watchdog 引导 | `/usr/local/bin/codex-watchdog.sh` | WSL 开机时自动启动 watchdog（由 `/etc/wsl.conf` 调用） |
| WSL 配置 | `/etc/wsl.conf` | 追加 `[boot] command` 指向上述引导脚本 |
| 包装脚本 | `$WRAPPER_DIR/codex` | 运行 `codex` 时自动：检测代理存活 → 如未运行则启动 watchdog → `exec` 真实 codex |
| PATH 配置 | `~/.bashrc` / `~/.profile` | 确保 `$HOME/bin` 在 PATH 中，登录 shell 加载 `.bashrc` |

## 常见问题

**Q: 启动后 `http://localhost:3456/` 没反应？**
A: 代理是否运行？检查 `ps aux | grep proxy.js`。没运行则 `node proxy.js &`。

**Q: 代理进程崩溃了怎么办？会自动重启吗？**
A: 已配置 watchdog 进程守护。watchdog 每 10 秒检测一次代理进程，发现崩溃自动 `nohup node proxy.js &` 拉起。终端输入 `codex` 时包装脚本也会自动检测并启动 watchdog。
运行 `ps aux | grep watchdog` 确认 watchdog 在线。

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
