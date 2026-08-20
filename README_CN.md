# OpenAPI Multi-Key Proxy

> 官网：[OpenAPI.im](https://openapi.im) | 作者推特：[@C_2049s](https://twitter.com/C_2049s)

[English Full README (README_EN.md)](README_EN.md) · [简洁版 (README.md)](README.md)

## 核心能力

**三向协议转换**：本代理自动在 OpenAI Responses API、Anthropic Messages API、OpenAI Chat Completions API 三者之间双向转换，形成 3×3 全协议适配矩阵。下游任意客户端（Codex CLI、Claude Code CLI、Chat 应用）可连接任意上游（OpenAI、Anthropic、DeepSeek、Kimi、Qwen、Gemini、Grok 及任意 Chat 兼容中继），零配置自动检测上游协议，同协议透传、跨协议转换。

**模型智能适配**：协议层之外，代理运行时探测各上游实际可用的模型列表并缓存（10 分钟 TTL），自动翻译下游模型名与上游可识别模型名之间的差异。Claude Code 发 `claude-*`、上游是 gpt 中转 → 自动就近映射到上游可用的 gpt 模型；Codex/其他终端发 `gpt-*`、上游是 claude 中转 → 自动映射到 claude 模型。优先级：Key 配置的 `model` 覆盖字段 > 上游能力适配 > 原样透传。上游探测失败时保守透传不硬猜，不影响其他客户端。

**超越 cc-switch 之处**：
- **运行时代理**，非配置管理工具——cc-switch 是本地配置切换器，本代理是网络层透明代理，无需修改客户端配置即可工作
- **三向全协议转换**：cc-switch 仅支持 Anthropic→OpenAI 单向；本代理支持 Responses↔Chat↔Messages 三向互转 + 任意协议混合账号多池回退
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
├── config.json           # 系统配置（prices、webhookUrl、notifications、groups、log、discussions 等）
├── state.json            # 自动生成，持久化统计数据与冷却/废弃状态
├── state.json.bak        # 每小时自动备份
├── dashboard.html        # 独立监控面板文件（file:// 打开会有引导提示）
├── discussions-cache.json # 自动生成，GitHub 会话流本地快照（最新 10 条+回复+分类，200KB 软上限，可安全删除）
├── codex-proxy.service   # systemd 服务模板（含 {{PROXY_DIR}} 占位符）
├── install.sh            # 一键安装脚本（自动读取自身路径，替换 {{PROXY_DIR}}）
├── edit-keys.sh          # 命令行快速编辑 keys.json 的辅助脚本
├── package.json          # npm 依赖（仅 ws）
├── build-release.js      # 生成带来源及完整性元数据的发布资产
├── log-query-worker.js   # 历史 JSONL 查询与汇总 Worker（不阻塞代理主线程）
├── proxy-log-rotator.js  # WSL watchdog 控制台日志分段轮转器
├── test-release-provenance.js # 发布来源判定回归测试
├── test-stream-lifecycle.js # Responses / Messages 流终态回归测试
├── test-protocol-matrix.js # 3×3 协议适配矩阵（转换器 + 流转换 + 协议探测 + buildForwardPlan）回归测试
├── test-restart-lifecycle.js # 重启排空、取消与强制重启回归测试
├── test-auto-resume-lifecycle.js # Key 心跳闲置恢复与 watchdog 重载回归测试
├── test-resume-runner-lifecycle.js # 闲置恢复 runner 租约、退出与信号回归测试
├── test-log-operations-lifecycle.js # 日志游标、汇总与事件处置回归测试
├── test-runtime-storage-lifecycle.js # state/JSONL/proxy.log 容量治理回归测试
├── test-discussions-lifecycle.js # GitHub 会话流（列表/缓存/快照/降级/回退链/写防抖/token 保留）回归测试
├── logs/                 # 自动生成，按天分段的 JSONL 日志及本地汇总/事件 sidecar
├── watchdog.sh           # 进程守护脚本（WSL 无 systemd 环境用），每 10 秒检测崩溃并自动重启
├── start-proxy.sh        # 一键启动 watchdog + 代理（替代 systemctl start）
├── resume-codex.sh       # autoResume 辅助脚本：通过 cmd.exe 创建 Windows 可见终端运行 wsl 命令
├── proxy.pid             # 自动生成，记录代理进程 PID（watchdog 依赖此文件检测存活）
└── README.md             # 本文件
```

## 新机器部署清单

以下清单列出完整部署所需的所有文件和配置，AI agent 可逐项执行：

### 源码仓库与 Release 资产

GitHub 源码仓库保留构建器和全部回归测试，供发布维护者在构建前运行 `npm test`。面向普通用户的 GitHub Release 资产是运行时包：不包含 `build-release.js` 或任何 `test-*.js`，其 `package.json` 也不提供 `npm test` 或 `npm run build:release`。因此，测试和构建必须在源码仓库中完成，再上传生成的独立 Release 目录。

### 需从源码复制的文件

| 文件 | 说明 | 安装方式 |
|------|------|----------|
| `build-release.js` | 发布资产生成工具 | 发布维护者需要；与 `proxy.js` 同级 |
| `test-release-provenance.js` | 发布来源回归测试 | 发布维护者需要；与 `proxy.js` 同级 |
| `test-stream-lifecycle.js` | Responses / Messages 流终态回归测试 | 发布维护者需要；与 `proxy.js` 同级 |
| `test-protocol-matrix.js` | 3×3 协议适配矩阵（转换器 / 流转换 / 非流式 / 协议探测 / buildForwardPlan）回归测试 | 发布维护者需要；与 `proxy.js` 同级 |
| `test-restart-lifecycle.js` | 重启生命周期回归测试 | 发布维护者需要；与 `proxy.js` 同级 |
| `test-auto-resume-lifecycle.js` | Key 心跳闲置恢复与 watchdog 重载回归测试 | 发布维护者需要；与 `proxy.js` 同级 |
| `test-resume-runner-lifecycle.js` | 闲置恢复 runner 租约、退出与信号回归测试 | 发布维护者需要；与 `proxy.js` 同级 |
| `test-log-operations-lifecycle.js` | 日志查询、汇总与事件处置回归测试 | 发布维护者需要；与 `proxy.js` 同级 |
| `test-runtime-storage-lifecycle.js` | 状态、请求日志和控制台日志容量治理回归测试 | 发布维护者需要；与 `proxy.js` 同级 |
| `proxy.js` | 核心代理（含内嵌面板） | 复制到目标目录即可 |
| `log-query-worker.js` | 历史日志查询与汇总 Worker | 必须与 `proxy.js` 同级 |
| `proxy-log-rotator.js` | WSL `proxy.log` 分段轮转器 | 必须与 `watchdog.sh` 同级 |
| `dashboard.html` | 独立面板备用 | 同目录放置 |
| `package.json` | npm 依赖声明 | 复制后 `npm install` |
| `watchdog.sh` | 进程守护（WSL2） | 复制到同目录；脚本自动以自身位置作为代理目录 |
| `start-proxy.sh` | watchdog 启动器 | 复制到同目录；脚本自动以自身位置作为代理目录 |
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

### 可选：多端口分组辅助脚本

如需使用多端口分组的三振回退功能，可安装以下辅助脚本到 `/usr/local/bin/`：

| 脚本 | 用途 | 安装命令 |
|------|------|----------|
| `codex-ask-b` | 调用 Group B（gpt-5.6-sol） | 见「Codex CLI 集成与任务路由」章节 |
| `codex-ask-c` | 调用 Group C（gpt-5.6-luna） | 见「Codex CLI 集成与任务路由」章节 |

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
#   group:   可选字段，字符串，所属分组名（默认 "A"）。不同分组的 Key 由不同端口的代理服务器独立调度。
#
# 示例：
# [
#   {"key": "sk-xxx...", "url": "https://api.openai.com/v1",   "reset": "weekly", "remark": "主力 Key"},
#   {"key": "sk-yyy...", "url": "https://api.provider.com/v1", "reset": "daily",  "remark": "备用额度卡", "models": ["gpt-5.5", "gpt-5.4-mini"]},
#   {"key": "sk-zzz...", "url": "http://proxy.example.com:8080", "reset": "never",  "remark": "一次性", "status": "shielded"}
#   {"key": "sk-tw1...", "url": "https://api.example.com/v1", "reset": "daily",  "remark": "错峰 Key", "tz": "+8", "timeWindow": {"start": 22, "end": 8}}
# ]
```

> 💡 推荐中转代理（注册送额度）：[https://anytokens.cc/register?aff=4U66LUYVAT3X](https://anytokens.cc/register?aff=4U66LUYVAT3X)

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
- `model` 填充后 → 无论 CLI 发什么模型，代理在转发前将 `body.model` 强制替换为该值（最高优先级）
- `model` 为空 → 走**模型智能适配**：代理探测该上游的模型列表（按上游 URL 去重缓存，成功 TTL 10 分钟，失败 30 秒快速重试，失败期间降级 `unknown`），按需就近映射——
  - Claude Code 发 `claude-opus-4-5` 等 claude 模型名、上游是 gpt 中转 → 自动映射到上游可用且最强的 gpt 模型
  - Claude Code 发 `claude-haiku-4-5`（小档位）→ 映射到上游最小的 gpt 模型
  - Codex CLI 发 `gpt-5.6`、上游是 claude 中转 → 自动映射到 claude 模型
  - 上游能力为 claude/mixed、或模型名非 claude/gpt 家族 → 原样透传（与旧版一致）
  - 上游探测失败（`unknown`）→ 保守透传不硬猜，不影响其他客户端
- 若该 Key 的 API Key 不支持最终使用的模型，上游返回错误，Key 进入冷却（可用 `model` 覆盖字段修正）
- 不影响 `models` 路由过滤：两者可组合使用

**典型场景**：
```bash
# 某个 Key 的配置
#   models: ["gpt-5.5"]      → 只接 gpt-5.5 的请求进来
#   model:  "gpt-5.6-sol"    → 但实际转发时改为 gpt-5.6-sol 发给上游
#
# CLI 发 model=gpt-5.5 → pickKey 匹配 → 转发时替换为 gpt-5.6-sol
```

**Claude Code 接入 gpt 中转（零配置）**：把 gpt 中转的 Key 放入任意分组（如 A 组 3456），Claude Code 直接指向该端口即可。代理对 `GET /v1/models` 自动返回 claude-* 档位列表（`claude-opus-4-5` / `claude-sonnet-4-5` / `claude-haiku-4-5`，组内存在 claude 能力上游时则透传其真实 claude 列表），转发时按上述规则就近映射到 gpt 模型。若映射结果不合口味，给该 Key 设置 `model` 字段即可强制覆盖。

### 错峰时段

错峰时段 = 给 Key 划定每天可参与调度的小时段，非时段内的 Key 不参与调度。典型用法：把部分 Key 设为夜间时段（22:00-08:00）避开白天高峰，或多组 Key 分时段轮换以摊平上游压力。

为 Key 设置可用时段，不在时段内的 Key 不参与调度。

在 `keys.json` 中为 Key 添加 `timeWindow` 和 `tz` 字段：

```json
{"key": "sk-xxx...", "url": "https://...", "reset": "daily", "tz": "+8", "timeWindow": {"start": 22, "end": 8}}
```

| 字段 | 说明 |
|---|---|
| `tz` | 时区偏移，字符串，如 `"+8"`、`"-5"`，默认 `"+0"`（UTC） |
| `timeWindow` | `{"start": 22, "end": 8}`，小时级（0-23），可选字段，不设=全时段 |

**时段规则**：
- `start < end`：同天窗口（如 `08:00-17:00`）
- `start > end`：跨夜窗口（如 `22:00-08:00`）
- `start == end`：全时段（无限制）

**与现有功能叠加**：
- 轮询/随机轮询/批量优先：只在时段内的 Key 参与
- cooldown/rate limit：叠加生效
- 每周重置/优先级组：不受影响

**管理面板**：
- 卡片底部状态栏显示错峰时段（绿色=时段内，橙色=非时段）
- 管理弹窗：筛选「时段内」/「非时段」，批量「⏰ 错峰时段」/「⏰ 清除时段」。「错峰时段」弹窗内会实时预览窗口形态（同天/跨夜/全时段）与当前时段内/外状态，方便确认设置效果。

### 管理面板

「管理 Key」弹窗每行提供两列模型输入：
- **指定模型**（`models`，逗号分隔）：路由准入过滤
- **覆盖模型**（`model`，单个值）：转发时强制替换

保存后生效，无需重启。

## 局域网访问

默认情况下，代理仅监听 `localhost`（127.0.0.1），只有本机可以使用。如需让局域网内其他电脑使用本机代理的大模型，可在系统配置中切换网络模式。

### 启用步骤

1. 打开 Dashboard（`http://localhost:3456/`）→ 系统配置
2. 在「🌐 网络模式」中选择「局域网可用」
3. 设置「LAN 连接密码」（建议设置，防止未授权访问）
4. 保存 → 代理自动重启，监听地址变为 `0.0.0.0`

### 局域网客户端配置

其他电脑配置如下：

| 配置项 | 值 |
|---|---|
| Base URL | `http://<本机局域网IP>:3456` |
| API Key | 设置的 LAN 连接密码 |

例如本机 IP 为 `192.168.1.19`，密码为 `my-secret-key`：

```bash
curl http://192.168.1.19:3456/v1/models \
  -H "Authorization: Bearer my-secret-key"
```

### 安全说明

| 场景 | 行为 |
|---|---|
| 本机请求 | 任何 Bearer token 均通过（与默认模式一致） |
| LAN 请求 + 已设密码 | 必须提供正确的 Bearer token |
| LAN 请求 + 未设密码 | 放行（向后兼容，但不推荐） |

> 如需保护 Dashboard 管理面板，请另行设置「管理 Token」（与 LAN 连接密码独立）。

## 自动切换故障 Key

`forwardRequest()` 在以下情况自动切换到下一个 Key：

| 上游状态码 | 处理方式 |
|---|---|
| 401 Unauthorized | `markFailure` → 切换 |
| 402 Payment Required | `markFailure` → 切换 |
| 403 Forbidden | `markFailure` → 切换 |
| 429 且错误体归因为配额/用量超限（`insufficient_quota`） | `markFailure` → 该周期冷却、切换（配额耗尽不会在几秒内恢复） |
| 429 其他（纯瞬时容量/速率）或明确的 `model_at_capacity` | `markCapacityBackoff` → 临时跳过；尚未向下游提交响应时重新入队等待 |
| 5xx Server Error（非容量不足） | `markFailure` → 切换 |
| 连接超时 / DNS 错误 / TLS 错误 | `markFailure` → 切换 |
| 流传输中断 | `markFailure` → 切换 |
| 2xx / 3xx 成功 | `markSuccess` → 响应原路返回 |
| 其他 4xx | 透传给 Codex（不切换） |

容量不足既可能作为 HTTP `429` / 5xx 返回，也可能在 HTTP `200` 已建立的 SSE 中以 `response.failed`、`error` 或协议等价事件返回。**瞬时**容量压力（`model_at_capacity` 或未归类为配额问题的 `429`）进入 `capUntil` 短暂退避，不写 `failCode`、不触发跨周期废弃。**配额/用量超限**（错误体含 `quota`、`billing`、`usage limit`、`*_limit_exceeded`、`weekly/monthly/daily limit` 等，如 `WEEKLY_LIMIT_EXCEEDED` / `MONTHLY_LIMIT_EXCEEDED`）被归类为 `insufficient_quota`，走 `markFailure` 进入当前周期冷却，避免把所有 Key 拖进无效的容量重试风暴。纯瞬时限流文本（如 `rate limit exceeded`，不含上述配额关键词）仍按瞬时容量处理，仅退避、不写冷却。已向下游输出的流不会自动重放到另一 Key，以免重复文本或工具调用；代理会保留协议失败终态，下一次请求再使用恢复后的可用池。

全部 Key 切换失败后返回 `502 {"error": "All keys exhausted"}`（旧版存在全部失败后挂起不响应的 bug，已修复）。

## 冷却、废弃与自动锁死

- Key 返回 401/402/403、配额类 429、非容量 5xx、连接或流传输失败 → `failCode` + `failPeriod` 写入 `state.json` → 该周期内 `inCooldown()` 返回 true → 不再被 `pickKey()` 选中
- 瞬时容量压力（未归类为配额的 HTTP 429 或明确的 `model_at_capacity`，包括 SSE 流终态）仅写入 `capUntil` 短暂退避，不作为跨周期失败或废弃依据
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
- **排序**：下拉选择「默认顺序」或「按重置日（周一→周日）」或「首次启用（早→晚）」或「使用时长（长→短）」或「按分组」
- **状态码筛选**：输入 `401` 等过滤指定失败码或最后响应状态码的 Key（失败 Key 匹配 failCode，可用 Key 匹配 lastStatus）
- **状态筛选**：下拉选择 全部/可用/冷却中/废弃/锁死/屏蔽/启用时长/最后失败/最后响应/周重置日
- **启用时长筛选**：选择「启用时长」后出现 X 天输入框，筛选启用距今 ≥ X 天的 Key。可与状态码筛选组合使用（如：状态码 `429` + 启用时长 `30` 天 = 筛选出失败码为 429 且启用超 30 天的 Key）
- **最后失败筛选**：选择「最后失败」后出现 X 天输入框，筛选最后失败距今 ≥ X 天的 Key（如：最后失败 `30` 天 = 筛选出 30 天前失败且未恢复的 Key）
- **最后响应视图**：选择「最后响应」后表格切换为精简视图（9列），显示所有 Key 最后一次请求的状态码（彩色显示：200=绿/429=黄/4xx=橙/5xx=红/网络错误=红/null=灰）、最后响应时间（X天X小时）、响应模型。若 `lastStatus` 为空则回退显示 `failCode`（确保所有 Key 都有状态码显示）。可用的 Key 默认视图也显示绿色 200 徽章。列头可点击排序（升序/降序）。配合「🧹 清理失败」可一键选中长期失败的 Key 后批量屏蔽
- **周重置日筛选**：选择「周重置日」后出现日选择下拉（全部/自动/周一~周日），筛选指定重置日的 Key。可与其他条件组合（如：状态码 `401` + 周重置日 `周一` = 筛选出失败码为 401 且重置日为周一的 Key）
- **错峰时段筛选**：下拉选择「时段内」或「非时段」，筛选当前在/不在可用时段内的 Key
- **错峰时段设置**：勾选 Key 后点击「⏰ 错峰时段」，弹出表单设置时区（UTC-12~UTC+12）和可用时段（开始/结束小时）；弹窗实时预览窗口形态（同天/跨夜/全时段）与当前时段内/外状态。点击「⏰ 清除时段」一键清除选中 Key 的时段设置（等效于全时段可用）
- **错峰时段悬停**：设置了时段的 Key 行悬停时显示完整时段信息（时区 + 时间范围 + 当前状态：时段内/非时段），未设置时段的 Key 无提示
- **隐藏已屏蔽**：🙈 按钮一键隐藏所有「已屏蔽」的 Key（默认开启），方便对非屏蔽 Key 批量操作。状态跨模态打开保持。再次点击 🙉 恢复显示
- **数量显示**：实时显示 `共 X 个，筛选后 Y 个`，并单独统计已屏蔽数量
- **自动分组**：按备注前缀（中文逗号/英文逗号/空格分割的第一段）自动分组折叠
- **一键折叠/展开**：📂 按钮折叠或展开所有分组
- **拖拽排序**：拖动行调整顺序，自动保存到 keys.json
- **优先级设置**：每行「优先」数字输入框，数值越大调度越优先（同 reset 类型组内优先）
- **全选 + 批量操作**：批量重置 / 批量屏蔽 / 批量删除 / 🧹清理失败（自动勾选长期失败的 Key 后批量屏蔽）
- **批量导入**：📋 弹出多行文本框，每行一个 Key，格式 `sk-xxx URL [重置类型] [优先级] [分组] [备注]`，URL 为必填项，允许重复 Key
- **单 Key 测试**：🔍 调用 `GET /v1/models` 测试连通性，返回模型名 + 耗时
- **批量测试**：勾选多个 Key → 🔍 批量测试 → 面板实时显示每 Key 结果（成功/失败）→ 通过测试的 Key 可一键重置恢复使用
- **覆盖模型**：每行「覆盖模型」输入框，填入后转发时强制替换 `body.model`。与「指定模型」（路由准入）独立协作
- **分组管理**：每行「分组」输入框，设置 Key 所属分组（默认 A）；支持批量迁移至目标分组
- **重置所有 Key 的状态码**：批量测试后根据每 Key 实际测试结果同步状态（200 成功 → 清空冷却；429 失败 → 写入 429 进入冷却），而非统一重置为可用
- **CSV 导出（管理面板内）**：📥 勾选 Key → 📥 导出 → 选择导出字段（Key/URL 必选，重置类型/优先级/分组/备注/指定模型/覆盖模型/重置日/时区/时段 可选），生成带 BOM 的 CSV 文件
- **未保存变更提醒**：修改后关闭面板时提示保存，避免误操作丢失数据
- **分组状态码统计**：每个分组 header 显示该组 Key 总数 + 各状态码数量（如 `▼ 备注 (12个) 200×8 429×3`），颜色与表格状态码一致；点击状态码 badge 可筛选并自动勾选该组该状态码下的所有 Key（badge 外框变色 + 出现取消按钮），方便批量操作；筛选「周重置日」时自动按 周一~周日 分组

## 监控面板

`http://localhost:3456/` 内嵌完整监控面板：

### 界面语言

Dashboard 支持中文和 English。点击右上角的语言按钮即可切换；选择会保存在当前浏览器的 `localStorage` 中，后续打开面板时会继续使用上次选择。默认语言为中文。语言切换只影响当前浏览器界面，不需要重启代理。

如果修改了 `proxy.js` 中的 Dashboard 或 i18n 代码，需要重启正在运行的代理进程并刷新浏览器页面，运行中的 Node.js 进程不会自动加载源码变更。

### 顶部摘要
可用数/总数、冷却中、🔒 锁死数、并发请求、总流量、总请求、健康评分、预估费用

### 排序/筛选/搜索/批量操作
- 排序：默认 / 按到期日（最近→最远）/ 首次启用（早→晚）/ 使用时长（长→短）/ 健康评分 / 平均延迟 / 5 分钟成功率 / 按分组
- 筛选：全部 / 可用 / 冷却中 / 废弃 / 🔒 锁死 / 屏蔽 / 启用时长 / 周重置日（与状态码筛选可组合使用）
- 重置筛选：每日重置 / 每周重置 / 每 N 小时重置 / 永不过期（可与状态筛选组合）；选择「每周重置」后可继续按周重置日筛选：全部 / 周一至周日 / 自动（未设置 `resetDay`）
- 状态码筛选：输入 `401` 等过滤指定失败码或最后响应状态码的 Key
- 搜索：ID / 备注 / 地址
- 实时显示筛选后数量：`显示 X / Y 个`
- 批量：勾选卡片 → 批量操作栏出现（含「全选」「全取消」按钮）/ 批量重置 / 批量屏蔽 / ⚡优先使用（逐个使用勾选的key，用完恢复常态）/ ⭕优先轮询（勾选的key间轮询，全冷却后恢复常态）/ 🎲随机轮询（Fisher-Yates洗牌随机顺序，每轮每个key恰好出现一次后重洗）
- **CSV 导出（主面板）**：⬇ 导出 CSV → 弹出字段选择窗口 → 默认导出当前页面可见的所有 Key（受搜索/筛选/分组等条件过滤后），可勾选导出字段（Key/URL 必选；配置字段：重置类型/备注/分组/优先级/指定模型/覆盖模型/重置日/时区/时段；统计字段：状态/失败码/请求数/成功数/失败数/输入输出字节/平均耗时/健康分/费用），生成带 BOM 的 `openapi-export-YYYYMMDD.csv`

**批量清理过期 Key 示例**：状态码输入 `429` → 全部状态选「启用时长」→ 输入 `30` → 点击「全选」→ 点击「批量屏蔽」→ 保存

### 趋势图
24 小时 / 7 天 / 30 天切换，每小时柱状图，X 轴标签密度自适配。
屏蔽 Key 的流量也纳入趋势图统计。
点击标签切换趋势模式，共 8 种：

| 标签 | 说明 |
|------|------|
| 📊 模型 | 堆叠柱状图，按模型分色显示各模型请求数占比，最多显示前 8 个模型，其余归入「其他」，默认显示；图例使用 `请求模型 (实际模型)` 复合标签（如 `claude-fable-5 (gpt-5.6-sol)`），实际未适配时仅显示单名 |
| 📊 流量 | 按字节数显示流量趋势，悬停查看上下行流量及 Key 级明细 |
| 📈 次数 | 按请求数显示次数趋势 |
| 💚 健康 | 堆叠柱状图，分色显示 200（绿）/ 4xx（黄）/ 5xx（红）/ 失败（灰），反映上游 HTTP 状态码分布 |
| 🔺 上游 | 堆叠柱状图，按 Key 的上游域名归集显示各上游使用次数，悬停查看域名 + #Key 列表及次数，最多显示前 8 个域名，其余归入「其他」 |
| 🔻 下游 | 堆叠柱状图，按下游应用客户端（请求 User-Agent）归集显示各应用请求数，悬停查看各应用及次数，最多显示前 8 个应用，其余归入「其他」；流终态细节请查看日志/CSV 的流结果字段 |
| 💰 费用 | 按小时预估费用（USD），悬停查看精确值 |
| ⏱ 延迟 | 按小时平均延迟，悬停查看具体值及请求数 |

### Key 卡片
脱敏显示（点击明文切换）、重置类型徽章（每周 Key 额外显示具体周几/自动）、并发徽章、批量优先徽章、健康评分进度条、折叠按钮、冷却倒计时、统计指标（请求数/流量/延迟/P50-P95-P99/滑动成功率/费用）、首次启用时间+启用至今、日/小时明细、失败码悬停中文含义、最后失败时间、活跃 Key 发光高亮、锁死 Key 紫色标记、底部状态栏错峰时段（绿色=时段内，橙色=非时段）

### 状态栏快捷操作
- 🔍 测试连通性
- 🔄 重置冷却/废弃/锁死
- 🔇 屏蔽此 Key

### 一键折叠
📂 按钮一键折叠/展开全部卡片

### 日志查看器
打开日志不会同步读取完整日志文件。默认“即时视图”优先返回内存中的最新 50 条；若代理刚重启、内存记录不足，会由 `log-query-worker.js` 异步回读最多 100 条已保存日志，与内存记录去重合并后显示最近 50 条。因此重启后打开日志仍会显示保存的历史尾部，并在面板中标注“历史尾部”。该读取不依赖“日志文件”开关：即使已停止写入新文件，只要旧 JSONL 仍在，仍可查询和导出。面板还会显示发现和扫描的历史文件数；Worker 暂不可用时，会明确说明仅显示当前内存记录。表格只创建当前可见行；WebSocket 也仅在日志窗口打开时订阅，实时到达的记录每 350ms 批量刷新一次。

默认日志页展示最新 50 条；点击“浏览历史”可切换到可分页的历史视图，再用上一页、下一页按游标翻看旧记录，并可随时返回最新视图。筛选、指定时间范围或导出也会进入历史模式。未填写时间范围即查询全部可用时间，不会被当作 Unix 时间 0。历史 JSONL 由 `log-query-worker.js` 反向分块扫描，主代理不读取整文件、不做全量行数统计；每页最多 100 条，通过游标前后翻页，单次查询受 64MB / 30 秒边界保护。关闭窗口或发起新查询会取消旧查询。

统计卡片、最近 30 分钟趋势、模型分布和错误分布来自服务端分钟/日汇总，而非当前表格页面。汇总保存在 `logs/.log-summary.json`；首次缺失时会在后台 Worker 自动重建，亦可手动点击“重建汇总”。日汇总跨自然日边界时标记为近似值，不会阻塞代理请求。

支持按 Key、状态码（含 `4xx` / `5xx`）、模型、上游域名、路径、分组、全文和时间范围筛选。表格保留请求与运行事件，点击行按需展开完整的流终态、URL、耗时和字节详情。CSV / JSONL 历史导出同样走 Worker，最多导出 2000 条。

日志窗口内置“运行事件”中心。它会按上游、模型、路径和分组监测失败突增、流终态失败和可选的 P95 延迟劣化；可确认、静默或启用桌面/Webhook 通知。“刷新事件”会显示刷新中、成功时间或失败状态。对分组事件可人工临时暂停 1–60 分钟，暂停期间该分组的新代理请求返回 `503`，可随时恢复。该操作不会自动修改 Key、不会自动重启代理，且暂停状态不跨代理重启保留。

`GET /__logs` 返回 `{ entries, stats, overview, mode, hasMore, nextCursor, truncated }`：无筛选时为 `recent`，内存不足会补齐已保存的日志尾部；有筛选/游标/时间范围时为 `history`。`recent` 响应还会带 `source`、`historyChecked`、`historyUnavailable`、`historyFilesAvailable` 和 `historyFilesScanned`；`history` 响应带 `filesAvailable` 和 `filesScanned`。这些字段分别说明是否包含保存历史、是否完成尾部检查、Worker 是否暂不可用，以及发现/实际扫描的符合格式的日志文件数。`overview` 是服务端汇总；`truncated=true` 表示本页触及扫描边界，应缩小筛选范围或继续使用时间/维度筛选。

### 运行时数据治理

运行时文件不会再无限增长。治理在代理进程内执行，不关闭监听端口、不重启 watchdog，也不会暂停正在传输的请求：

| 文件 | 默认策略 | 说明 |
|---|---|---|
| `state.json` | 小时桶 35 天、日桶 180 天、文件上限 32 MiB | 过期桶和超限时最旧的完整时间桶会被裁剪；保存使用临时文件原子替换，并每小时写入 `state.json.bak`。备份仅用于损坏恢复，不参与趋势统计。 |
| `logs/YYYY-MM-DD*.jsonl` | 按天保留 7 天、所有分段合计 256 MiB、单段 16 MiB | 同日超限生成编号分段；按天清理和总容量清理同时生效。`logRetentionDays=0` 只关闭按天删除，容量上限仍生效。单条日志超过 512 KiB 会被截断为有限诊断字段或丢弃。 |
| `logs/.log-summary.json` | 8 MiB | 只保留最近汇总桶；它是统计加速缓存，缺失时可由 Worker 重建，不是原始日志。 |
| WSL `proxy.log` | 当前段 10 MiB，另保留 5 个归档 | `proxy-log-rotator.js` 通过管道写入并轮转为 `proxy.log.1` 等文件，配置每 10 秒重新读取。旧的超大单文件首次启动时会迁移为有界尾部。systemd 部署使用 journald，不使用该文件。 |
| Codex `logs*.sqlite`（可选） | 默认关闭；达到 2048 MiB 后保留最近 12 小时 | 只允许当前代理用户 `~/.codex/` 下的主 `.sqlite` 文件。检查主库与 `-wal` 合计容量；每轮最多 5 个 1000 行短事务，数据库忙时 1 秒内让步并在下个周期重试。 |

点击「系统配置」保存后会立即执行一次状态压缩和请求日志清理；WSL 控制台日志的新容量值最迟约 10 秒生效。轮转器是 WSL watchdog 的必需运行文件，缺失时 watchdog 会拒绝以无界方式启动代理，而不会把输出重新追加到无限增长的 `proxy.log`。

这些操作不等同于重启代理。若正在运行的实例尚未加载本次源码变更，需在合适的维护窗口人工重启后才会使用新代码；重启本身仍可能中断活跃流，不能用来安全“暂停并恢复”任意 Codex CLI 任务。

#### Codex SQLite 日志维护

在「系统配置」启用后，代理会先检测数据库路径、常规文件属性和 `logs(id, ts)` 结构；检测失败时不能保存启用配置。路径框示例为 `/root/.codex/logs_2.sqlite`。粘贴 Windows Explorer 路径 `\\wsl.localhost\Ubuntu\root\.codex\logs_2.sqlite` 或 `\\wsl$\Ubuntu\root\.codex\logs_2.sqlite` 时，面板和服务端会转换为 `/root/.codex/logs_2.sqlite` 后再校验；其他 Windows 路径、符号链接、`-wal` / `-shm` 辅助文件和 `~/.codex/` 外的文件会被拒绝。

达到设置的「主库 + WAL」容量阈值后，后台独立进程仅删除 `ts` 早于保留期的记录。它依赖 Python 3 自带的 `sqlite3`，不需要新增 npm 原生依赖；缺少 Python 3 时启用保存会明确失败。启用保存时若数据库正忙，会拒绝该次保存并提示稍后重试，避免未验证配置生效；已保存的后台任务遇到忙状态则显示“数据库忙，已跳过”，等待下一周期，不会强抢写锁。它不重启/暂停代理、watchdog 或正在运行的 Codex CLI；「立即检查」「立即清理」都只使用已经保存并验证过的配置。

「立即检查」是只读检查：返回主库/WAL 容量、当前总容量与是否达到触发阈值，不删除任何记录。「立即清理」会真正执行删除并按触发容量/保留时长清理，随后 `VACUUM` 物理缩小库文件；为安全起见，它只在 Codex 空闲时执行（在途请求 = 0、排队 = 0，且距上次请求 ≥ 60 秒），否则拒绝并提示“Codex 仍在使用中”。`VACUUM` 需要约等于库容量的临时磁盘空间、耗时取决于库大小，且只有确实删除过记录才触发。

SQLite 删除行后通常只会把页留给后续写入复用，物理文件不保证立刻变小，因此定时后台维护**不会主动执行 `VACUUM`、checkpoint 或删除 WAL/SHM 文件**；确需物理压缩时，请等待 Codex CLI 停止并静默 60 秒后使用「立即清理」，或人工维护窗口单独处理。

### Key 管理
增删改、屏蔽/取消屏蔽、软删除（`status="deleted"` 保留在 JSON）、重置冷却状态、设置每周重置日（周一~周日或自动）、搜索/分组/拖拽排序、全选批量操作、批量导入 CSV、单 Key 连通性测试
### 系统配置

Webhook URL、价格参数、桌面通知/声音开关、🔄 自动恢复冷却 Key（间隔/固定/快速三种模式独立配置）、失败码列表、是否检测 discarded Key、🔁 轮询均摊、📅 每周 Key 按到期日排序、🧬 闲置自动恢复（autoResume）、项目列表（项目名/WSL 路径/启动命令/恢复模式 resumeMode/sessionId 动态增减）、cmd.exe 路径、🔒 自动锁死阈值与监控码、日志文件/保留天数/详情级别、运行时文件容量/保留策略（JSONL、`state.json`、WSL `proxy.log`）、🗄 Codex SQLite 日志维护（开关、路径检测、容量阈值、保留时长、周期与立即检查）、日志事件规则（失败/流失败/可选延迟阈值与默认静默时间）、⏱ 其他协议流最大时长（默认30分钟）与 Responses / Messages 编码流专用总时长（默认不限）/上游空闲超时（默认90分钟）/无进展看门狗（默认15分钟）、🔐 管理 Token（可选，设置后管理接口需 Bearer 认证，Dashboard 弹窗输入）、🔌 端口分组管理（动态添加/删除/修改端口）、🔄 重启代理按钮（全屏显示提交、排空、取消重启、30 秒后可二次确认强制重启、watchdog 拉起和新实例就绪进度）、⬆ GitHub Release 更新检查（官方发布包/干净官方 Tag/源码基线文件自动识别，定制构建可选手动基线）、💬 GitHub 互动（会话流：启用开关、显示条数、可选 GitHub Token）

> **GitHub 互动（会话流）配置字段**（`config.json` 的 `discussions` 分组，面板「系统配置 → 💬 GitHub 互动」维护）：
> - `enabled`（布尔）：会话流面板总开关，默认 `false`
> - `maxItems`（1~50，默认 10）：列表条数
> - `githubToken`（可选）：GitHub Token，需对目标仓库具备 Discussions 读/写权限（fine-grained PAT：Repository permissions → Discussions 设为 Read and write；classic token：勾选 `repo` 作用域）。仅存本地 `config.json`，面板回显掩码、永不明文泄露（`GET /__config` 返回 `hasDiscussionsToken`）；保存时省略该键则保留原值，传显式空串可清除
> - 无 Token 时面板只读（`writeEnabled=false`），所有写端点返回 401 `missing_github_token`
> - 该分组仅影响会话流，不影响代理转发主流程

## API 接口

> 所有 `/__*` 管理接口及 `/metrics` 在设置了管理 Token 后需 `Authorization: Bearer <token>` 认证（constant-time 比较，空 token 拒绝）。WebSocket 连接需在 URL 中携带 `?token=<token>` 参数。Dashboard 首次打开时弹出 Token 输入框；Token 仅保存在当前页面内存，刷新或关闭页面后需重新输入。升级后的首次加载会清除旧版浏览器会话存储中的 Token。无 Token 时 Dashboard 正常使用。

| 接口 | 方法 | 说明 |
|---|---|---|
| `/` 或 `/dashboard` | GET | 监控面板 HTML |
| `/__status` | GET | JSON 状态（所有 Key 的完整指标） |
| `/__keys` | GET | 读取 keys.json（富化 `_locked`/`_failCode`/`_failTime`/`_activatedAt`/`_available`/`_lastStatus`/`_lastTime`/`_lastModel`/`_inTimeWindow` 字段） |
| `/__keys` | PUT | 写入 keys.json（自动重载；自动清除因 reset/resetDay 变更导致的过期 failCode） |
| `/__config` | GET | 读取 config.json |
| `/__config` | PUT | 写入 config.json（自动重载） |
| `/__codex-log-maintenance/check` | POST | 检测候选 Codex SQLite 路径和 `logs(id, ts)` 结构；不写配置、不删除日志。请求体为 `{"codexLogMaintenance":{"dbPath":"/root/.codex/logs_2.sqlite"}}` |
| `/__codex-log-maintenance/run` | POST | 只读立即检查已保存且已启用的 Codex SQLite 维护配置（容量/有效性），不删除记录；不接受任意路径，不重启代理或 Codex CLI |
| `/__codex-log-maintenance/clean` | POST | 立即清理：仅当 Codex 空闲（在途/排队为 0 且静默 60 秒）时按触发容量/保留时长删除过期记录并 `VACUUM` 缩小库文件；忙时返回 409 `database_active` |
| `/__reset-key` | POST | 重置指定 Key 的冷却/废弃状态（`{"idx": 1}`） |
| `/__apply-test-result` | POST | 应用批量测试结果：`{"idx":1, "failCode":429}` → markFailure；`failCode=null/200` → 清空冷却（`{"idx":1, "failCode":null}`） |
| `/__test-key` | POST | 单 Key 连通性测试（`{"key":"sk-...","url":"https://..."}`），返回 `model`（逗号分隔可用模型列表）和 `modelCount`（模型数量） |
| `/__patch-key-status` | POST | 修改 Key 状态（`{"idx":1,"status":"shielded"}`） |
| `/__patch-key` | POST | 修改 Key 配置（`{"idx":1,"tz":"+8","timeWindow":{"start":22,"end":8}}`），`timeWindow:null` 清除时段 |
| `/__boost-batch` | POST | 批量优先：`{"mode":"use","idxs":[1,3,5]}`（逐个使用）或 `{"mode":"roundrobin","idxs":[1,3,5]}`（轮询）或 `{"mode":"random","idxs":[1,3,5]}`（随机轮询）或 `{"mode":""}`（取消） |
| `/__restart` | POST | 返回 `202` 后进入重启排空：拒绝排队请求、暂停新的 API 请求，等待在途请求结束后退出，由 watchdog 拉起新进程 |
| `/__restart-status` | GET | 重启进度：返回实例 ID、启动时间、阶段（`ready` / `draining` / `stopping`）、在途与排队请求数，以及 `restartId`、`canCancel`、`canForce`、`forceAvailableInMs`；供 Dashboard 轮询，不含敏感信息 |
| `/__restart/cancel` | POST | 仅在 `draining` 时可取消本次安全重启并恢复新请求接入；已返回 `503` 的排队请求不会重新入队；成功返回 `200`，其他阶段返回 `409` |
| `/__restart/force` | POST | 仅在 `draining` 已持续至少 30 秒后可强制退出；会中断活跃流和在途请求，成功返回 `202`，其他阶段或等待时间未到返回 `409` |
| `/__update-status` | GET | 查询 `aipayim/codex-proxy` 的最新正式 GitHub Release；服务端缓存 1 小时、支持 `?refresh=1` 手工复查（至少间隔 60 秒）。官方发布包、干净官方 Git Tag 或源码基线文件（`release-baseline.txt`）自动比较；定制构建仅在配置了有效手动基线 Tag 时比较；不下载或执行远端代码 |
| `/__auth_check` | GET | 检查管理 Token 是否已配置，返回 `{configured: true/false}`（不暴露实际 Token） |
| `/__config` `_groupAction` | PUT | 端口分组管理：`{"_groupAction":"addGroup","_groupName":"B","_groupPort":3457}` 或 `"removeGroup"` / `"setGroupPort"` / `{"_groupAction":"toggleGroup","_groupName":"B","_groupEnabled":false}` |
| `/__test_port?port=3457` | GET | 检测分组端口是否运行（查询内存中 `servers` 注册表） |
| `/__keys` `_batchGroup` | PUT | 批量迁移分组：`{"_batchGroup":"B", …完整 keys 数组…}` |
| `/__logs` | GET | 日志查询。无筛选默认返回最新 50 条，内存不足时由 Worker 补齐已保存日志尾部；筛选、时间范围或 `cursor` 自动进入 Worker 历史模式。支持 `key/status/model/upstream/path/group/q/since/until/limit/cursor`，状态可用 `4xx`/`5xx` 通配；返回游标而非 `offset`/总行数 |
| `/__export-logs` | GET | Worker 历史导出（`?date=2026-07-10&key=11&status=502&model=gpt-5.6-sol&format=csv` 或 `jsonl`），最多 2000 条 |
| `/__log-overview` | GET | 服务端分钟/日汇总，支持 `since/until`，返回趋势、维度分布、统计及当前运行事件 |
| `/__incidents` | GET | 日志事件、临时分组暂停和汇总重建状态 |
| `/__incident-action` | POST | 人工处置日志事件：`acknowledge`、`snooze`、`pause_group`、`resume_group`；暂停仅允许有效分组且最长 60 分钟 |
| `/__logs/rebuild-summary` | POST | 在后台 Worker 重建历史日汇总；返回 `202`，不阻塞代理请求 |
| `/__export` | GET | CSV 导出统计报表 |
| `/__pathstats` | GET | 按路径/模型的请求分布 |
| `/metrics` | GET | Prometheus 格式指标 |
| `/responses` | POST | 原生 Responses 透传；`stream:true` 时保留上游字节并检查 `response.completed`，若上游提前结束则补发一次 `response.failed` |
| `/v1/responses` | POST | **全协议适配**：接收 Responses API 请求，上游为原生 Responses 则直通，否则自动转换为 Chat / Messages 格式转发，并将响应流式（或非流式）转换回 Responses 格式 |
| `/v1/messages` | POST | **全协议适配**：接收 Messages API 请求，上游为原生 Messages 则直通，否则自动转换为 Chat / Responses 格式转发，并将响应转换回 Messages 格式；仅在上游明确完成终态后发送 `message_stop` |
| `/v1/chat/completions` | POST | **全协议适配**：接收 Chat Completions 请求，上游为原生 Chat 则直通，否则自动转换为 Messages / Responses 格式转发，并将响应转换回 Chat 格式 |
| `ws://localhost:3456/?token=<token>` | WS | WebSocket 实时推送（设置了管理 Token 时需在 URL 中携带 token 参数） |
| `/__discussions` | GET | GitHub 会话流：最新 `maxItems` 条 Discussions 列表（只读，匿名即可）；`?number=n` 附带该会话回复。服务端 60s 缓存 + 落盘快照回退；失败返回 `lastError` 与缓存/快照数据，不影响代理主流程 |
| `/__discussions/categories` | GET | 会话分类（GraphQL，需已配置 Token；未配置返回空） |
| `/__discussions/test-token` | POST | 用已保存的 GitHub Token 测试连通性（`GET /user` + Discussions 只读探测），返回 `{ok, login, discussionsScope}`；**仅验证 Token 有效与读取权限，发布/回复的写权限以实际发布为准**。可传 `{"token":"..."}` 一次性测试新值（不保存） |
| `/__discussions/comment` | POST | 在指定会话发布公开留言：`{"number": 1, "body": "..."}`；需已配置 Token，30s 防抖，正文最长 1000 字 |
| `/__discussions/create` | POST | 发起新会话：`{"title": "...", "body": "...", "category": "<分类 id>"}`；需已配置 Token，标题最长 120、正文最长 1000 |

> **会话流（GitHub 互动）**：Dashboard「💬 会话流」面板展示 `aipayim/codex-proxy` 仓库最近动态，定位是「最近动态提醒 + 轻阅读 + 快速参与」，**不是完整 Discussions 客户端**——深度浏览/历史检索引导 GitHub 官方页面（每条「在 GitHub 打开 ↗」）。未配置 GitHub Token 时仅可查看；配置后可在面板内直接留言、回复与发起新会话（内容将**公开发布**到 GitHub Discussions）。Token 仅存本地 `config.json`，前端掩码回显；读取走 60s 缓存 + 匿名配额绰绰有余，未读提醒为 60s 低频轮询（**自己发布的留言/发起的会话不计入未读提醒**）。`discussions-cache.json` 为本地落盘快照（最新 10 条 + 已展开回复 + 分类，200KB 软上限），是**运行时数据，可安全删除**，重启或网络恢复后自动重建。**权限说明**：「测试连通」仅验证 Token 有效与读取权限，GitHub 不提供只读方式预判写权限；若发布/回复提示 `Resource not accessible by personal access token`，请到 GitHub 将 Token 的 Discussions 权限改为 Read and write（或 classic token 补 `repo` 作用域）。**安全提示**：留言/发起会话是写端点，若将管理端口暴露到局域网/公网，必须同时设置「管理 Token」。

### 协议转换说明

协议转换层使任意下游客户端可连接任意上游模型，形成 3×3 全协议适配矩阵：

| 下游客户端 | 请求路径 | 转换方向 | 支持的上游 |
|---|---|---|---|
| Codex CLI | `/responses` | 原生 Responses 直通 + 终态保护 | 原生 Responses 上游 |
| Codex CLI | `/v1/responses` | Responses → Chat / Messages → Responses | 任意 Chat / Messages / Responses 上游 |
| Claude Code CLI | `/v1/messages` | Messages → Chat / Responses → Messages | 任意 Chat / Messages / Responses 上游 |
| Chat 客户端 | `/v1/chat/completions` | Chat → Messages / Responses → Chat | 任意 Chat / Messages / Responses 上游 |

- 上游协议自动检测：`keys.json` 中的 `url` 静态判定（`api.openai.com`/`api.ofox.ai` = Responses 原生，`api.anthropic.com` = Messages 原生，其余 = Chat 通用），并对未知中继做动态探测（`/v1/models` 可用即 Chat，否则依次探测 `/v1/messages`、`/v1/responses` 端点）。动态结果写入独立协议缓存（24 小时 TTL），与模型列表缓存（成功 10 分钟 / 失败 30 秒）解耦：即使中继的 `/v1/models` 探测失败，已识别出的 Messages / Responses 协议也不会被错误降级回 Chat。后台每 10 分钟按 URL 去重重探各上游（同池多 Key 仅触发一次探测），新增/改配 Key 无需重启即可生效
- `/v1/chat/completions` 路由的多协议池判定同样使用动态协议：组内任一账号（静态白名单或动态探测）被识别为 Messages / Responses 上游，即自动启用 Chat→Messages / Chat→Responses 转换回退，无需在 `url` 上显式标记
- 同协议上游优先透传（字节直通 / 终态保护），跨协议才做转换，避免无谓重写；协议池顺序：下游 Chat → `chat, messages, responses`；下游 Messages → `messages, chat, responses`；下游 Responses → `responses, chat, messages`
- 任一 Key 失败自动切换到池内下一协议的下游 Key 重试，`#N` 强制指定 Key、boost/轮询、冷却与容量重排队照常生效
- 转换同时支持**流式与非流式**：跨协议时流式 body 强制 `stream:true`，响应经 SSE 转换器回写；非流式请求走一次性 JSON 响应转换器
- 转换字段覆盖：文本、system/instructions、工具定义与调用链（`tool_use`↔`function_call`）、思考内容（`thinking`↔`reasoning`↔`reasoning_content`）、图片、usage 统计

#### Responses / Messages 流终态与编码 CLI 断开排查

对于 `Responses → Chat → Responses` 转换，代理只会在上游 SSE 明确发送 `[DONE]` 后输出下游的 `response.completed` 和最终 `[DONE]`。即使上游把 `data: [DONE]` 放在连接末尾而没有换行，也会被识别为正常完成。

原生 `/responses` 流不会被转换或重写。代理旁路解析 SSE 帧，只有收到上游 `response.completed` 才视为成功；`[DONE]` 本身不等同于 Responses 成功。若上游在 `response.completed` 前 EOF、关闭连接、被中止、发生 SSE 错误、空闲超时或达到已设置的总时长，代理会在保留已收到原始字节后补发一次 `response.failed`，绝不会伪造 `response.completed` 或 `[DONE]`。上游已经发送 `response.failed` 或 `response.incomplete` 时，不会重复注入失败事件。

`/v1/messages` 的 Chat→Messages 转换同样不会把截断伪装成完成：只有上游 Chat SSE 明确发送 `[DONE]` 后，代理才会输出一次 `message_delta` 和一次 Anthropic `message_stop`。在 EOF、连接关闭/中止、SSE 错误、空闲超时或总时长到达时，已收到的增量会保留，随后只补一次 Anthropic `event: error`，不会补 `message_stop`。反向的 Messages→Chat 转换也只在真实 `message_stop` 后输出一次 Chat `data: [DONE]`；异常终止返回 OpenAI 兼容 SSE 错误，不会伪造 `[DONE]`。

系统配置将普通协议与编码协议流分开：`streamLifetime` 仍是其他协议流的最大时长（默认 30 分钟）；为兼容既有 `config.json`，配置键仍叫 `responsesStreamLifetime` / `responsesIdleTimeout`，但现在同时适用于 Codex Responses 与 Claude Messages。前者是最大总时长（默认 `0`，即不作硬切断），后者是上游完全无数据时的空闲超时（默认 `5400000` ms，即 90 分钟，`0` 可关闭）。非零值最少 60000 ms、最多 24 小时。这样长时任务不会被普通的 30 分钟总时长中断，同时仍可按需保留空闲连接保护。

编码协议流还有第三道独立保护：**无进展看门狗**（`responsesNoProgressTimeout`，默认 `900000` ms 即 15 分钟，范围 1 分钟–24 小时）。空闲超时从请求发起时起算，无法捕捉「健康前奏之后才开始停滞」的长流；看门狗则跟踪距离最近一次真实上游 SSE 数据的时间，只要编码流连续超过该时长没有任何字节到达，代理会强制写入失败终态（`response.failed` / Anthropic `event: error`）并销毁上游连接，向 CLI 明确报错，而不是让已接受但永不返回的连接永久挂起。`responsesStreamLifetime=0` 不受影响。

每个转换请求和受保护的原生 `/responses` 请求会在普通请求日志中记录 `streamOutcome`、`streamReason`、`stopReason`、`streamSawDone`、`streamId`、`streamErrorMsg` 和 `terminalSource`，并额外写入一条带上游地址的 `stream_terminal` 事件。`stopReason` 是协议层终止原因（下游 `stop_reason` 等价物）：Chat 上游 `finish_reason: length` 或 Responses 上游 `status: incomplete`（`incomplete_details.reason = max_output_tokens`）时记为 `max_tokens`，表示响应被模型输出上限截断——日志表格该行模型名旁显示琥珀色「截断」徽标，详情视图与 CSV 新增「终止原因」列，全文检索同样覆盖该字段；正常结束为 `end_turn` / `tool_use` 等。可搜索终态原因：`upstream_done`、`upstream_eof_without_done`、`upstream_eof_without_completed`、`upstream_close`、`upstream_aborted`、`upstream_incomplete`、`upstream_error`、`upstream_idle_timeout`、`stream_lifetime_timeout`、`no_progress_timeout`、`client_disconnect`、`model_at_capacity`、`insufficient_quota`、`upstream_api_error`。HTTP 状态码仍表示传输层响应；HTTP `200` 但 `streamOutcome=failed` 会按失败计入成功率、模型错误数和错误分布。

上游在 SSE 流内或非 2xx HTTP 错误体内返回的错误对象（例如 `Selected model is at capacity. Please try a different model.`、`You exceeded your current quota`、`WEEKLY_LIMIT_EXCEEDED` / `MONTHLY_LIMIT_EXCEEDED` 等）不再被静默丢弃：代理会把限长并脱敏后的错误信息记入 `streamErrorMsg`，同时按内容归类为 `model_at_capacity` / `insufficient_quota` / `upstream_api_error`。SSE 错误会向 Codex CLI 输出 `response.failed`，向 Claude Code 输出 Anthropic `event: error`；如果一个请求的所有可用 Key 均失败且最终返回 `502`，代理还会写入 `downstream_terminal` 事件。自动切换到其他 Key 后成功不会产生该事件，也不会算作下游失败。日志列表、全文检索和 CSV 可查看错误分类、信息及来源；趋势图「🔻 下游」已改为按下游应用（请求 User-Agent）归集的请求数堆叠图，不再展示流终态，流终态细节请通过日志/CSV 的「流结果/流终态原因/终止原因」字段查看。代理只能记录实际经过代理的 HTTP/SSE 内容，无法读取 Codex CLI 本地终端中未经过代理的提示。

出现 Codex CLI 的 `stream disconnected before completion` 后，先检查工作区和日志，确认是否已有部分文件修改、命令执行或工具调用结果；不要盲目重放整个编码任务。此类改动在代理重启后生效，应等待所有在途 CLI 任务结束，再在维护窗口重启。

#### 混合账号 fallback（Chat 客户端 → Anthropic）

`/v1/chat/completions` 路由在组内存在 Messages / Responses 账号（静态白名单或动态探测结果均可，后者见「上游协议自动检测」）时自动启用多协议池回退：先尝试同协议 Chat 上游（字节直通），Chat 池全部失败后自动转换为 Messages 格式尝试 Anthropic 上游，再转换为 Responses 格式尝试 Responses 原生上游；任一协议池内的所有 Key 均失败才会切到下一协议池。

不同协议（Chat / Messages / Responses）上游可任意共存，无需额外配置。

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
| `failReason` | string/null | 失败分类原因（`insufficient_quota` / `model_at_capacity` / `upstream_api_error`）。配额类失败时记录，用于让自动恢复跳过配额耗尽的 Key |
| `failCount` | int | 连续失败计数（仅 lockFailCodes 中的错误码） |
| `locked` | bool | 是否被自动锁死 |
| `active` | bool | 当前是否有请求在处理 |
| `activeRequests` | int | 当前并发请求数 |
| `actives` | array | 每个活跃请求的详情：`{model, since}`（model=模型名，since=请求开始时间戳） |
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
| `lastStatus` | int/null | 最后一次请求的上游状态码（200/429/5xx 等），仅在请求完成后写入 |
| `lastTime` | int/null | 最后一次请求完成的时间戳（ms） |
| `lastModel` | string/null | 最后一次请求使用的模型名 |
| `timeWindow` | object/null | 错峰时段 `{"start":22,"end":8}`，小时级，`start==end` 表示全时段 |
| `tz` | string/null | 时区偏移，如 `"+8"`、`"-5"`，默认 `"+0"` |
| `inTimeWindow` | bool/null | 当前是否在可用时段内（仅 `/__keys` GET 返回） |
| `daily` | object | 按日统计 `{"YYYY-MM-DD": {...}}` |
| `hourly` | object | 按小时统计 `{"YYYY-MM-DD-HH": {...}}` |
| `boostedBatch` | array | 批量优先中的 Key 序号列表（始终为 1-based） |
| `boostedBatchMode` | string | 批量优先模式：`"use"` / `"roundrobin"` / `"random"` / `""` |

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

`status` 推送是全量快照且**已节流合并**：任意窗口内最多每秒广播 1 次，期间发生的多次状态变更合并为一次推送（全量非增量，不丢信息）；另有 10 秒心跳兜底保证最终一致。WebSocket 连接失败时前端自动降级为 HTTP 轮询（每 5 秒）。

为避免慢速/后台标签页的看板客户端无限缓冲导致代理进程内存耗尽（OOM），每个 WebSocket 连接的发送缓冲超过 4MiB 时会被主动断开；前端会自动重连（断开期间继续以 HTTP 轮询维持展示），重连后立即收到全量状态。

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

1. **面板操作**：配置弹窗 → 🔄 重启代理 → 确认（推荐）
2. **命令行**：`kill $(cat proxy.pid)`（仅在维护窗口使用，watchdog 会在 10 秒内拉起新进程）

面板确认后会立即显示全屏进度：提交重启请求、排空在途请求、等待旧实例退出与 watchdog 拉起、检测新实例就绪。新实例的 ID 变化且状态为 `ready` 后，Dashboard 自动重新载入，不会再出现无反馈的空白等待。嵌入式 Dashboard 和独立备用面板在 `draining` 阶段都可选择「取消重启」。

`POST /__restart` 返回 `202` 后，旧实例进入 `draining`：已排队请求会收到 `503`，新的 API 请求暂时收到带 `Retry-After: 5` 的 `503`，在途请求继续完成。旧实例保留重启控制和进度轮询接口，排空后写入 watchdog 重载标记并退出；watchdog 在看到该标记后以 `exec` 方式重新读取自身脚本，再检测端口并拉起新进程。整个过程始终只有一个 watchdog 锁持有者，避免并发状态冲突。请让客户端按自身重试策略处理这段短暂不可用时间。

首次从不支持该标记的旧 watchdog 升级时，旧脚本无法自行识别新的重载请求。请在没有活跃 CLI 任务的维护窗口，先人工停止已确认 PID 的旧 watchdog，再执行 `bash start-proxy.sh --boot` 启动新脚本；新脚本会接管仍在运行的代理。随后通过面板执行一次普通安全重启，使新 `proxy.js` 载入。此后面板的「重启代理」会同时完成 watchdog 的 `exec` 重载，不会额外启动竞争锁的第二个 watchdog。

若发现重启是在仍有任务运行时误触，可在旧实例仍为 `draining` 时调用 `POST /__restart/cancel` 或点击「取消重启」。代理会立即恢复接入新请求，但此前已被拒绝的排队请求不能恢复，客户端应自行重试。

排空超过 30 秒后，面板才会显示「强制重启」，且还要求第二次确认。`POST /__restart/force` 会使旧实例立即退出，因此会断开 SSE/流式响应和其他在途连接；Codex CLI 任务可能部分执行或报出 `stream disconnected before completion`。该操作绝不会自动执行，只应在确认可以中断当前工作后使用。

## 版本检查与升级

Dashboard 打开后会检查一次 [官方 GitHub Release](https://github.com/aipayim/codex-proxy/releases)，前端每 30 分钟复查；服务端使用 ETag 和 1 小时缓存。Release 元数据始终可以查看，但只有确定了本地构建基线后才会比较版本并在顶部「配置」右侧显示闪烁的 ⬆ 标识。

代理识别的是**构建来源**，不会根据目录名、磁盘、主机名、Windows/WSL 环境猜测"开发机"，也不会在源码中写死本机路径或 Release 版本。启动时只执行一次本地判定，不在代理请求热路径运行：

| 优先级 | 本地状态 | 比较行为 |
|---|---|---|
| 1 | 系统配置填写了 `updateBaselineTag` | 作为定制构建的显式基线 |
| 2 | 官方发布资产中的 `build-info.json` 与 `release-manifest.json` 校验通过 | 自动使用发布 Tag |
| 3 | 官方 GitHub 远程、工作树干净且 `HEAD` 恰好位于稳定 Release Tag | 自动使用该 Git Tag |
| 4 | 源码安装目录中的 `release-baseline.txt` 记录了有效基线（克隆/复制源码仓库或源码 ZIP 的场景） | 自动使用文件中的版本号 |
| 5 | 开发分支、本地修改、未知远程、缺少 Git/发布元数据/基线文件或清单校验失败 | 基线未知，只展示 Release，不提示更新 |

`release-baseline.txt` 是随源码仓库与 Release 资产同步维护的版本基线文件（单行 `vX.Y.Z`），让没有构建元数据的源码安装也能显示本机版本号并判断是否可升级。`updateBaselineTag` 是高级定制选项，格式为 `vX.Y.Z` 或 `X.Y.Z`，例如 `v1.2.3`。普通用户不需要填写它：未修改的官方发布资产会自动识别；定制构建留空是正确默认值。若 GitHub 的最新正式 Release 高于已确定基线，才显示更新标识。

### 生成官方发布资产

不要把 GitHub 的源码快照直接当作可自动识别的发布包。发布维护者应在待发布源码中先执行测试，再生成独立资产目录：

```bash
npm test
npm run build:release -- --tag v1.2.3 --out ./dist
```

构建器会生成 `dist/codex-proxy-v1.2.3/`，其中包含：

- `build-info.json`：由构建过程写入的 Release Tag、提交标识和清单摘要；
- `release-manifest.json`：代理代码、启动脚本和包元数据的 SHA-256 清单；
- 运行所需的白名单文件。

发布器会将输出资产的 `package.json` 版本同步为 Release Tag，并移除仅在源码仓库可用的 `scripts`（包括 `npm test` 和 `npm run build:release`），但不会修改开发源码。它不会复制 `build-release.js`、任何 `test-*.js`、`config.json`、Key、状态、日志、PID 或本机路径。应将该目录归档后作为 GitHub Release 资产提供给普通用户。安装后的用户配置不在清单内；但代理代码或受保护脚本被修改时，清单校验会失败并自动退回“来源未知”，避免误报更新。

清单用于本地完整性和版本来源判定，不执行远端代码，也不启用覆盖式升级。无论来源状态如何，“一键升级”都保持禁用，避免覆盖本地代码、配置或运行状态。安全升级流程是：备份当前代理目录（含 `keys.json`、`config.json`、`state.json`），审核 Release，手动合并需要的改动，执行 `node -c proxy.js`，再在维护窗口重启代理。仅在 GitHub 发现新版本时，版本更新弹窗完整展示上述安全升级步骤（含升级期间可能中断正在运行的请求的提醒），并在弹窗内提供「GitHub 升级说明」入口；本机已是最新或无法比较时，弹窗只显示“一键升级已禁用”单句提示。Release 更新说明以受限 Markdown 渲染展示（标题、无序列表、代码标记与 `https://` 链接；渲染前先 HTML 转义、仅放行 http/https 链接，不执行原始 HTML），超长内容保留滚动查看。

## config.json 系统配置

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

| 字段 | 说明 |
|---|---|
| `webhookUrl` | 全部 Key 失效时 POST JSON 告警（兼容企业微信/钉钉/Telegram） |
| `prices.inputPer1M` | 默认输入价格（$/百万 token），未命中 `modelPricing` 时使用 |
| `prices.outputPer1M` | 默认输出价格（$/百万 token），未命中 `modelPricing` 时使用 |
| `bytesPerToken` | 默认每 token 近似字节数（默认 3，中文约 1.5-2，英文约 4），未命中 `modelPricing` 时使用 |
| `modelPricing` | 可选的按模型定价规则数组，默认 `[]`；命中规则时覆盖以上三项全局价格/估算参数，未命中时仍使用全局值 |
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

> **配额 Key 不参与自动恢复**：被归类为 `insufficient_quota` 的 Key（配额/用量超限，如 `WEEKLY_LIMIT_EXCEEDED`）会被自动恢复（间隔/固定/快速）跳过，因为 `/v1/models` 探测对已耗尽额度的 Key 仍返回 200，无法区分配额状态。这类 Key 只能靠周期翻转或真实请求成功（`markSuccess`）自然恢复，也可手动「重置冷却」。
| `rateLimit` | 是否启用分钟级限速（true/false，默认 true） |
| `maxRequestsPerMin` | 单个 Key 每分钟最大请求数（默认 10）。可在 keys.json 中按 Key 覆盖（`maxReqPerMin`） |
| `maxTokensPerMin` | 单个 Key 每分钟最大 token 数（默认 0=不限）。可在 keys.json 中按 Key 覆盖（`maxTokPerMin`） |
| `defaultResetHours` | `hourly` 类型的默认重置周期（小时，默认 5）。可在 keys.json 中按 Key 覆盖（`resetHours`） |
| `autoResume` | 是否启用闲置自动恢复（true/false，默认 false） |
| `autoResumeIdleMinutes` | 空闲阈值（分钟，默认 10） |
| `autoResumeDebounceMinutes` | 旧配置兼容字段（分钟，默认 3）；同一 Key 闲置周期只执行一次初始启动 |
| `autoResumeRunnerStallMinutes` | 已验证恢复 runner 的停滞宽限（分钟，默认 20，`0` 关闭，范围 0–1440）。仅在无在途请求、且自 runner 启动或上次实际 Key 应用后都没有新 Key 应用时生效 |
| `autoResumeRunnerMaxStallRestarts` | 同一 Key 闲置周期允许的已验证 runner 停滞重启次数（默认 1，`0` 关闭，范围 0–3）。不会对普通失败 runner 盲目重放 |
| `autoResumeProjects` | 项目列表数组，最多 10 个。每项含 `name`/`path`/`cmd`；`resumeMode` 可选 `"command"`（默认，`resume --last` 等原样命令）或 `"fixed_session"`；`fixed_session` 需提供 `sessionId`，且 `cmd` 必须包含 `{sessionId}` 占位符（启动时替换为该会话 ID）。`resume --last` 会恢复到「最近一个」会话，多个 Codex 实例并发时可能互踢或恢复错会话；需要确定性恢复请使用 `fixed_session` |
| `cmdPath` | cmd.exe 路径（默认 `/mnt/c/Windows/System32/cmd.exe`） |
| `weeklySortBy` | weekly 组排序方式：`"priority"`（按 priority+索引）或 `"expiry"`（按最先到期先使用） |
| `roundRobin` | 是否启用轮询均摊模式（见「Key 调度顺序」） |
| `enableAutoLock` | 是否启用自动锁死（true/false，默认 true） |
| `lockAfterFailCount` | 连续 N 次失败后自动锁死（默认 3） |
| `lockFailCodes` | 只有这些错误码会计入连续失败计数（默认 `["401","403"]`） |
| `logFile` | 是否启用文件日志（true/false，默认 true）。关闭后仅内存缓存 2000 条，不再写入新日志；已有 JSONL 仍可在日志查看器中查询和导出 |
| `logRetentionDays` | 日志文件保留天数（默认 7）。设为 0 关闭自动清理 |
| `logMaxMiB` | 请求 JSONL 文件总容量上限（默认 256 MiB，范围 16–4096）。按最旧分段删除，独立于按天保留 |
| `logSegmentMaxMiB` | 单个请求 JSONL 分段上限（默认 16 MiB，范围 1–256，不能超过 `logMaxMiB`）。同一天超过后写入编号分段 |
| `stateHourlyRetentionDays` | `state.json` 小时统计保留天数（默认 35，范围 31–365） |
| `stateDailyRetentionDays` | `state.json` 日统计保留天数（默认 180，范围 30–3650） |
| `stateMaxMiB` | `state.json` 容量上限（默认 32 MiB，范围 4–256）。超限时按完整时间桶删除最旧统计，并通过临时文件原子替换 |
| `proxyLogMaxMiB` | WSL watchdog 当前 `proxy.log` 分段上限（默认 10 MiB，范围 1–100） |
| `proxyLogKeepFiles` | WSL watchdog 保留的 `proxy.log.N` 归档数（默认 5，范围 1–20）；总量约为当前段加这些归档 |
| `codexLogMaintenance.enabled` | 是否启用 Codex SQLite 日志维护（默认 `false`）。启用保存时必须通过路径与 SQLite 结构检测。 |
| `codexLogMaintenance.dbPath` | 当前代理运行用户 `~/.codex/` 下的 `logs*.sqlite` 主文件，例如 `/root/.codex/logs_2.sqlite`。支持从 `\\wsl.localhost\发行版\...` / `\\wsl$\发行版\...` 自动转换；不接受普通 Windows 路径、符号链接、`-wal` 或 `-shm`。 |
| `codexLogMaintenance.thresholdMiB` | 主库与 WAL 合计达到该容量后才维护，默认 2048 MiB，范围 64–102400。 |
| `codexLogMaintenance.retainHours` | 只删除早于该时长的 `logs.ts` 记录，默认 12 小时，范围 1–8760。 |
| `codexLogMaintenance.checkIntervalMinutes` | 后台检查周期，默认 15 分钟，范围 5–1440。每轮最多 5 x 1000 行短事务；SQLite 忙时跳过并等待下一周期。 |
| `logDetail` | 日志详情级别：`"full"`（完整，含模型名）或 `"basic"`（简洁，不含模型名） |
| `logIncidents` | 日志事件规则对象。可配置是否启用/通知、观察窗口、最低请求数、失败次数及失败率、流失败次数、可选 P95 请求/首字节阈值、自动恢复时间和默认静默分钟数；默认只告警，不会自动暂停分组、重启或变更 Key |
| `updateBaselineTag` | 高级可选项。定制构建的已人工确认上游稳定 Release Tag，格式 `vX.Y.Z` 或 `X.Y.Z`。官方发布资产、干净官方 Git Tag 和源码基线文件（`release-baseline.txt`）自动识别，无需填写；来源未知的定制构建留空时只显示 Release 信息，不比较更新 |
| `groups` | 端口分组映射，如 `{"A": 3456, "B": 3457}`。A 组始终运行且不可删除，B/C/D 等通过面板动态管理 |
| `groupEnabled` | 分组开关状态，如 `{"B": true, "C": false}`。关闭的分组重启后不启动端口。默认全部启用 |
| `taskInsight` | 任务洞察配置对象。默认关闭；字段见下方「任务洞察」章节 |

## 任务洞察（代理流水解析/提炼）

可选功能：把经过代理的 AI 编码请求按客户端+分组聚合成「任务会话」，自动记录指令、工具调用、涉及文件、用量与费用，并可调用 LLM 生成结构化摘要（决定/风险）。默认关闭，全部信号需显式开启。

### 开启与信号

在 `config.json` 中加入 `taskInsight`：

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

| 字段 | 说明 |
|------|------|
| `enabled` | 总开关，默认 `false` |
| `signals.instructions` | 记录请求中的指令（仅保留前 200 字符，空白归一） |
| `signals.tools` | 记录工具名（如 `read_file`）与文件/目录路径（正则提取，不存参数全文） |
| `signals.usage` | 记录真实 token 用量（从上游 SSE 提取，非字节估算）与估算费用 |
| `signals.correlate` | 会话关联：锚定 45 分钟窗口内唯一的 autoResume 活跃项目；同客户端空闲超时后自动收束为新会话 |
| `retentionDays` | 会话文件保留天数（范围 1–3650，默认 30），到期自动删除 `tasks/` 下旧文件 |

### 蒸馏（LLM 摘要）

对已完成的会话生成 `{summary, decisions, risks}` 结构化摘要，受每日预算护栏约束。

| 字段 | 说明 |
|------|------|
| `distill.enabled` | 是否启用蒸馏 |
| `distill.engine` | `ollama`=本地 Ollama（数据不出本机）；`proxy`=经本代理转发（计代理 token 且不超过每日预算，Key 不外泄）；`external`=直连第三方 API（隐私自担） |
| `distill.model` | 摘要模型名 |
| `distill.baseUrl` | OpenAI 兼容端点。`external` 必填；`ollama` 留空时默认 `http://127.0.0.1:11434/v1` |
| `distill.dailyBudgetYuan` | 每日蒸馏费用上限（默认 1），按上游用量估算，超额即停 |
| `distill.report` | `daily`/`weekly` 报告周期 |

### 数据与隐私

- 会话持久化在 `tasks/YYYY-MM-DD.jsonl`（按会话开始日分文件），仅存本机。
- 指令仅截断前 200 字符；工具只存名称、文件只存正则提取的路径；不保存完整工具参数或请求原文。
- 蒸馏只发送结构化快照（≤6000 字符），绝不包含任何 API Key。
- 会话在空闲收束或关闭功能时落盘；运行时仅保留最近 2000 个内存会话。

### 相关接口

| 接口 | 说明 |
|------|------|
| `GET /__task-insight-status` | 任务洞察当前状态（开关、信号、蒸馏状态与预算） |
| `GET /__tasks?from&to&project&status&model&q&limit` | 查询会话（默认最新在前，`limit` ≤500） |
| `GET /__tasks/export` | 导出 CSV（带 BOM，≤2000 行） |
| `GET /__tasks/report?mode=daily|weekly` | 按项目聚合的报告 |
| `POST /__tasks/distill-now` | 手动触发蒸馏 |

管理面板工具栏「📋 任务流水」可查看/导出会话；系统配置弹窗可切换引擎与预算。



本代理支持单进程监听多个端口，不同端口的 Key 池相互独立，实现多模型分层隔离。

### 分组机制

- 每个 Key 有一个 `group` 字段（默认 `"A"`），标记所属分组
- `config.json` 中 `groups` 定义每组监听的端口号，Keys 通过 `group` 字段匹配到对应端口
- 分组 A 的代理始终运行在配置端口（默认 3456），不可删除。其他分组通过系统配置弹窗动态增删
- CLI 启动时可通过 `--groups "A=3456,B=3457"` 覆盖端口分配，重启后恢复 config.json 配置

### 端口路由

| 端口 | 分组 | 默认用途 | 说明 |
|------|------|----------|------|
| 3456 | A | 日常任务 | 始终运行，默认分组，所有未指定 group 的 Key 归属此组 |
| 3457 | B | 复杂任务（Sol 等） | 可选，通过系统配置添加 |
| 3458 | C | 简单任务（Luna 等） | 可选，通过系统配置添加 |
| 更多 | D~Z | 自定义 | 动态添加，端口自定义 |

### 使用场景

将不同模型的 Key 分配到不同分组，codex CLI 通过指定不同端口使用不同模型：

```bash
# 日常任务（端口 A，默认 3456）— 使用 Tree 等标准模型
codex "写一个 Python 脚本"

# 复杂任务（端口 B，3457）— 在 codex CLI 中配置 base_url 为 http://localhost:3457
# 对应 Key 的 model 字段可设为 "gpt-5.6-sol" 等高级模型

# 简单任务（端口 C，3458）— 快速问答，使用低成本模型
```

### 管理面板

- **Key 管理**：每行显示「分组」输入框，可单独设置每个 Key 的归属分组
- **卡片显示**：非 A 分组的 Key 右上角显示分组字母徽章
- **分组筛选**：顶部下拉菜单按分组过滤 Key 卡片
- **批量迁移**：勾选多个 Key → 批量操作栏「迁移至分组」→ 选择目标分组
- **系统配置**：端口分组管理区动态添加/删除分组，修改端口后自动同步服务器
- **端口检测**：每组端口右侧自动显示 🟢（运行中）/🔴（未运行），打开配置弹窗时自动探测
- **启用/禁用**：非 A 分组可点击 🔴 禁用 / 🟢 启用按钮开关端口，状态保存至 `config.json` 的 `groupEnabled` 字段，重启后保持

### 路由逻辑

1. 请求到达端口 P → 查找 config.json 中 `groups` 找到对应分组名 G
2. 在分组 G 的 Key 池内按原有优先级/轮询/冷却逻辑选择 Key
3. 如分组 G 对应的服务器未运行 → 502 "Group X server not running"
4. 分组间 Key 池完全隔离，互不影响

> **调度隔离说明**：每个分组独立运行完整的 `pickKey()` 调度逻辑，包含原有的双层优先级（reset → priority）、轮询均摊（`roundRobin`）、冷却判断、模型路由等全部规则。分组 A 使用什么调度规则，分组 B/C/D 就使用完全相同的规则——只是各自的 Key 池不同。
>
> 不存在「先轮询完 A 组再轮询 B 组」或「跨组混合调度」。
> 请求到哪个端口，就在该端口对应分组的 Key 池内独立调度，互不感知。

### Codex CLI 集成与任务路由

多端口分组的核心价值在于：**根据任务复杂度自动选择合适的模型**。以下是如何与 Codex CLI 结合使用的完整指南。

#### 端口分组定义

| 端口 | 分组 | 模型 | 定位 | 密钥容量 | 适用场景 |
|------|------|------|------|----------|----------|
| 3456 | A（主力） | 默认 | 日常任务 | 249（高） | 代码编辑、测试、文档、格式化、重构 |
| 3457 | B（高级） | gpt-5.6-sol | 复杂任务 | 2（低） | 架构设计、疑难调试、三振回退 |
| 3458 | C（基础） | gpt-5.6-luna | 最简单任务 | 1（极低） | 简单查询、格式检查、小型修改 |

#### 三振回退机制

当 Codex CLI 遇到复杂问题时，采用以下策略：

```
第1-2次尝试 → Group A（端口 3456，默认模型）
     ↓ 失败
第3次失败 → 调用 codex-ask-b（Group B，gpt-5.6-sol）
     ↓ 获取方案
审查方案 → 应用代码修改 → 继续执行
     ↓ 如果 B 也失败
报告用户 → 人工干预
```

**回退触发条件**（以下任一种情况计为一次失败）：
- 编译/测试失败
- 运行时 panic 或错误
- 方案被用户拒绝
- 同一个问题超过30分钟未解决

#### 调用方式

**Group A（默认）**：Codex CLI 启动时自动使用，无需额外操作。

**Group B**：使用 `codex-ask-b` 脚本调用（需先安装）：

```bash
# 安装脚本（一次性）
curl -o /usr/local/bin/codex-ask-b https://raw.githubusercontent.com/.../codex-ask-b
chmod +x /usr/local/bin/codex-ask-b

# 使用方式
codex-ask-b "
【任务】${当前任务描述}
【失败上下文】${前3次尝试的错误/失败原因}
【代码位置】${相关文件路径}
【已尝试的方案】${已试过的方法}
请给出完整解决方案（含代码）。
"
```

**Group C**：使用 `codex-ask-c` 脚本调用（需先安装）：

```bash
# 安装脚本（一次性）
curl -o /usr/local/bin/codex-ask-c https://raw.githubusercontent.com/.../codex-ask-c
chmod +x /usr/local/bin/codex-ask-c

# 使用方式（最简单任务：格式检查、简单查询、小型修改）
codex-ask-c "检查这个函数的格式"
codex-ask-c "简单查询：1+1等于几"
```

#### 指定 Key 发送（`#N` 语法）

在 model 名称后加 `#N`（N 为 Key 编号）可**直接路由到指定 Key**，跳过 `pickKey()` 调度。适用于测试特定 Key、调试问题 Key、或手动控制请求分配。

**语法**：`model#N`，其中 N 为 keys.json 中的 1-based 编号。

```
模型名示例：
  o3#87       → 使用 Key #87（B 组），模型名 o3 发送到上游
  gpt-4o#6    → 使用 Key #6（C 组），模型名 gpt-4o 发送到上游
  o3#253      → 使用 Key #253（A 组）
```

**Codex CLI 配置**：

```toml
# ~/.codex/config.toml
# 不加 #N：自动调度
model = "o3"

# 加 #N：固定到指定 Key
model = "o3#87"
```

**Claude Code CLI 配置**：

```json
// ~/.claude/settings.json
{
  "model": "o3#87",
  "apiBaseUrl": "http://localhost:3456"
}
```

**行为说明**：
- 如果 Key #N 不存在 → 返回 `400 Key #N does not exist`
- 如果 Key #N 的 URL 不可达 → 正常返回 502 错误
- 如果 Key 配置了 `model` 覆盖字段（`acct.model`） → 优先使用覆盖模型，而非 `#N` 前的模型名
- 如果 Key 没有覆盖模型 → 从 model 名中去除 `#N` 后缀，以干净的模型名发送上游

#### Group C 上游 API 兼容性说明

部分上游 API 的非流式响应存在格式缺陷：`message.content` 字段缺失，但报告了 `completion_tokens`。此问题仅影响非流式请求，流式请求正常。

**解决方案**：`codex-ask-c` 脚本默认使用流式请求（`stream: true`），从 SSE 事件中提取 `delta.content`，拼装为标准响应格式。

**如遇类似问题的排查步骤**：
1. 使用 `curl` 测试上游 API 的流式响应是否正常
2. 检查非流式响应中 `message.content` 是否存在
3. 如果流式正常但非流式异常，在脚本中使用 `stream: true` 并自行拼装响应

#### AGENTS.md 配置示例

在项目的 `AGENTS.md` 中添加以下内容，让 AI 智能体知道如何使用不同端口：

```markdown
## 端口分组路由与三振回退规则

### 端口分组概览
| 端口 | 分组 | 模型 | 定位 | 密钥容量 |
|------|------|------|------|----------|
| 3456 | A（主力） | 默认 | 日常任务 | 249（高） |
| 3457 | B（高级） | gpt-5.6-sol | 复杂任务 | 2（低） |
| 3458 | C（基础） | gpt-5.6-luna | 最简单任务 | 1（极低） |

### 三振回退流程
1. 第1-2次：使用 Group A（端口 3456）正常尝试
2. 第3次失败：调用 `codex-ask-b` 获取 Group B 模型的解决方案
3. 如果 B 也失败：报错给用户

### 调用方式
- Group A：默认，Codex CLI 自动使用
- Group B：`codex-ask-b "提示词"`
- Group C：`codex-ask-c "提示词"`

### 何时使用 Group B vs Group C
| 判断依据 | 使用 Group B | 使用 Group C |
|----------|-------------|-------------|
| 任务复杂度 | 复杂（架构设计、疑难调试、算法） | 简单（格式检查、简单查询、小型修改） |
| 触发方式 | 三振回退（Group A 失败3次）或明确需要高级模型 | 任务本身就很简单 |
| 模型能力 | gpt-5.6-sol（强推理） | gpt-5.6-luna（基础能力） |
```

#### 注意事项

- **禁止嵌套 Codex CLI**：活跃的 Codex CLI 会话内不能启动另一个 Codex CLI（会导致卡死）
- **Group B 密钥有限**：仅2个密钥，避免频繁调用
- **Group C 模型基础**：仅处理最简单任务，不用于复杂任务或回退
- **端口独立**：每个端口的 Key 池完全隔离，A 组的429不影响 B/C 组
- **脚本安装**：`codex-ask-b` 和 `codex-ask-c` 需手动安装到 `/usr/local/bin/`，详见上方安装说明

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

最后一次实际应用 Key 超过阈值时，自动在 Windows 可见终端中重新打开项目终端窗口（适用于 WSL2 环境，确保 codex CLI 运行在可见窗口中）。

### 工作原理

1. 代理仅在选中的上游 Key 已写入实际转发请求时记录 `lastKeyUseTime`。普通下游连接、管理接口、仪表盘/轮询请求、以及一个持续很久但没有再次应用 Key 的流都不会重置此计时
2. 运行时心跳保存在 `.auto-resume-runtime.json`，并镜像到 `state.json`；因此完整状态写入被节流、代理安全重启或旧状态文件被覆盖时，最近一次 Key 应用时间不会丢失。首次运行且没有历史心跳时，以代理启动时刻建立基线，避免把未知历史误判为故障
3. 启动和保存配置后立即检测一次，之后每 30 秒检测空闲时长；超过 `autoResumeIdleMinutes`（默认 10 分钟）后进入一个新的 Key 闲置周期
4. 仍有在途请求时暂缓打开新的 Codex 终端；请求结束后继续用同一个 Key 心跳判断，不会把正常长流误判为失效，也不依赖 `/goal`
5. 每个项目在同一个闲置周期只执行一次初始启动；只有代理再次实际应用 Key，或用户修改该项目的路径/命令/会话配置后，才会开启下一次初始尝试。普通失败 runner 不会按防抖间隔无限重复拉起
6. `checkAutoResume()` 遍历 `autoResumeProjects` 列表，为每个项目执行 `triggerResume()`；通过 `cmd.exe /c start` 启动新的 Windows 可见 cmd 窗口 → 运行 wsl.exe → bash → 执行项目命令
7. 每个 runner 写入原子 JSON 租约到 `/tmp/codex-resume-<项目名>.pid`，其中包含随机运行 ID、PGID 和 Linux 进程启动时间；只要租约仍可验证，就跳过重复启动
8. 默认另有一次受控停滞重启：已启动的受管 runner 在 `autoResumeRunnerStallMinutes`（默认 20 分钟）内没有新 Key 应用、且代理无在途请求时，代理会重新核验随机运行 ID、PID 启动 tick、独立 PGID 与实际 `/proc` 进程组；仅全部一致才向负 PGID 发送 `SIGTERM`，30 秒仍存活才发送 `SIGKILL`。确认该进程组已退出后，才会在本闲置周期额外启动一次
9. 代理绝不按项目目录扫描、终止或 SIGKILL 任意外部 `codex` 进程。`cmd.exe start` 返回成功仅表示 Windows 接受启动请求；只有 runner 状态文件才能表示其已启动、退出或收到信号

### 配置字段

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

| 字段 | 说明 |
|------|------|
| `autoResume` | 是否启用闲置自动恢复（true/false） |
| `autoResumeIdleMinutes` | Key 应用阈值（分钟，默认 10）。最后一次实际应用 Key 超过此分钟后触发 |
| `autoResumeDebounceMinutes` | 旧配置兼容字段（分钟，默认 3）。同一 Key 闲置周期内只执行一次初始启动 |
| `autoResumeRunnerStallMinutes` | 受管 runner 停滞宽限（分钟，默认 20，`0` 关闭）。仅当代理没有在途请求，并且自 runner 启动或最近一次实际 Key 应用后始终无 Key 应用时，才会开始受控接管 |
| `autoResumeRunnerMaxStallRestarts` | 同一 Key 闲置周期允许的受控停滞重启次数（默认 1，`0` 关闭，范围 0–3）。达到上限后不再反复启动 |
| `autoResumeProjects` | 项目列表，最多 10 个。每个项目包含 `name`（显示名）、`path`（WSL 路径，支持 `E:\xxx` 格式自动转换）、`cmd`（要执行的命令）；可选 `resumeMode` 和 `sessionId` |
| `resumeMode` | 项目级可选模式：默认 `command` 原样执行 `cmd`；`fixed_session` 要求 `cmd` 含 `{sessionId}` 占位符 |
| `sessionId` | `fixed_session` 的 Codex 会话 ID。启动时会安全替换命令中的 `{sessionId}`，避免 `resume --last` 选到不确定的分支/子会话 |
| `cmdPath` | cmd.exe 路径（默认 `/mnt/c/Windows/System32/cmd.exe`） |

### 面板状态

- **配置弹窗**：显示 `🧬 闲置恢复: Key 闲置 ${formatIdle}，上次触发 Ym 前` 实时状态行（秒级两位小数，支持 d/h/m/s 级联）
- **仪表盘**：工具栏右侧显示 `🧬Key闲置 ${formatIdle}/恢复Ym前`（同上级联格式），有活跃请求时自动归零 `0.00s`
- **并发 Key 跑马灯**：时间戳行右侧内联显示「⚡ 并发中：」+ 每个活跃请求的 badge（Key 编号、模型名、已用时间，如 `#3 gpt-5.6-sol 12s`），已用时间每秒实时更新；badge 从左向右排列，超出容器宽度时自动折叠多余 badge 并在前面显示「+N」（灰色）提示未显示数量；无并发时整个 ticker 隐藏，不占空间

### 路径自检

`path` 字段支持以下格式，保存配置时自动标准化：
- WSL Linux 路径：`/mnt/<drive>/path/to/project` → 不变
- Windows 路径：`D:\path\to\project` → `/mnt/d/path/to/project`
- 混合路径：自动转换为 WSL 绝对路径

### 依赖脚本

`resume-codex.sh` 位于 `codex-proxy/` 目录，通过 `cmd.exe /c start` + `wsl.exe` 打开可见终端窗口。它使用原子 JSON 状态文件记录 `starting`、`running`、`exited`、`failed`、`terminated`；runner 自身和命令进程组收到的 `HUP`/`INT`/`TERM` 会分别标明来源与信号。状态文件是诊断记录，不含 API Key 或命令正文。

### 注意事项

- **仅适用于 WSL2**：依赖 `cmd.exe` 和 `wsl.exe` 创建 Windows 可见终端，纯 Linux 环境无效
- **路径必须存在**：`path` 目录在 WSL 中必须可 `cd` 进入
- **终端边界**：`codex resume` 仍是 TUI，Windows/WSL/PTY 链可能显示控制字符；这不等于任务已经恢复。日志中的 `runner 已启动` 只表示命令进程已创建，新的 Key 应用才表示观察到代理活动
- **会话选择**：`resume --last` 在并行项目分支或子代理存在时不具确定性。需要可靠续接时，使用 `fixed_session`、会话 ID 和 `{sessionId}`；会话切换后应人工更新该 ID
- **PID 租约**：存放于 `/tmp/codex-resume-*.pid`，只管理由本功能创建且运行 ID、启动时间、独立进程组都可验证的进程。不会清理同目录的人工 Codex
- **受控停滞恢复**：默认在一个已验证 runner 连续 20 分钟无新 Key 应用且无在途代理请求时，先温和终止其独立进程组；30 秒仍存活才强制终止，确认退出后只额外重启一次。设置 `autoResumeRunnerStallMinutes: 0` 或 `autoResumeRunnerMaxStallRestarts: 0` 可关闭此保护
- **初始单次尝试**：普通启动或失败 runner 在同一 Key 闲置周期不会无限重放；只有上面的严格受控停滞恢复可以额外重启一次

#### 快速恢复的工作原理

1. Key 获取到 500/502/503/504 等失败码 → `markFailure()` 检测到该码在 `autoRecoverPollCodes` 中且当前无 timer 运行 → 启动 `schedulePollRecover()`
2. 每 `autoRecoverPollInterval` 分钟（默认 5）：检查是否仍有 Key 持有匹配的失败码 → 有则调用 `GET /v1/models` 测试连通性，成功后自动清除冷却；无则停止（不留定时器）
3. 停止后如有 Key 再次出现匹配的失败码 → 重新激活（事件驱动，不依赖轮询）

## 费用估算

全局 `prices` 与 `bytesPerToken` 是兼容旧配置的兜底值。若不同模型的输入/输出价格或 token 密度不同，可在 `modelPricing` 填写规则；每个规则必须同时包含 `model`、`inputPer1M`、`outputPer1M`、`bytesPerToken`：

```json
{
  "modelPricing": [
    { "model": "gpt-5.6-sol", "inputPer1M": 5, "outputPer1M": 15, "bytesPerToken": 3 },
    { "model": "gpt-5.6-luna", "inputPer1M": 1, "outputPer1M": 3, "bytesPerToken": 4 }
  ]
}
```

- 默认值是 `[]`，因此不配置时行为与旧版本完全一致。
- `model` 按最终转发并记录的模型名精确匹配（保存和请求名首尾空白会被忽略）；大小写不同、别名、前缀或包含关系都不会匹配。例如 `gpt-5.6-sol` 不会匹配 `gpt-5.6-sol-preview`，未命中则回退全局 `prices` / `bytesPerToken`。
- 同一个模型只能有一条规则，最多 50 条；模型名最长 80 个字符。输入/输出单价必须为 0–1,000,000，`bytesPerToken` 必须为 0.1–100。
- 单价单位仍为“美元 / 百万 token”。规则内的 `bytesPerToken` 只控制该模型的费用字节→token 估算，并非上游返回的精确用量；分钟 token 限速（`maxTokensPerMin` / `maxTokPerMin`）仍使用全局 `bytesPerToken`。

```
tokens ≈ bytes / bytesPerToken
费用 = (inputTokens / 1_000_000) × inputPer1M + (outputTokens / 1_000_000) × outputPer1M
```

> ⚠️ 精确 token 追踪不可用（上游中转不返回 `usage` 字段），此为字节->token 估算。每个请求在开始转发时冻结当时匹配的价格规则；之后保存的新规则只影响新请求，不会改写进行中的长流或已写入 `state.json` 的总费用、小时/日趋势及历史日志。因此同一历史时间段可能包含当时不同价格规则下的估算结果。

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

# 停止代理（仅在维护窗口使用，watchdog 会在 10 秒内重新拉起）
kill $(cat proxy.pid)

# 完全停止 watchdog + 代理（仅在维护窗口使用）
pkill -f watchdog.sh
```

#### 工作原理

| 组件 | 作用 |
|---|---|
| `watchdog.sh` | 每 10 秒检测代理存活：flock 单实例锁 → 检查 `proxy.pid` + 端口绑定 + 命令行验证。脚本以自身目录定位代理，不含固定机器路径。进程消失则自动拉起；非 proxy 进程占用端口只报警不杀。面板安全重启会请求 watchdog 以 `exec` 重载自身脚本，再拉起代理，不会创建第二个 watchdog。proxy.js 在 A 端口成功监听后自行写入 PID 文件 |
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
| 3 | 检查 Node.js ≥ 16，提示 Python 3 SQLite 支持 | Node 版本不足时退出；Python 3 仅为可选 Codex SQLite 日志维护所需，缺失不阻止安装 |
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
A: 先调整全局 `bytesPerToken` 和 `prices`；不同模型 token 密度不同，可用 `modelPricing` 为模型设置精确名称的单独单价和 `bytesPerToken`。这仍是字节估算，改价不会回算历史费用。

**Q: 面板显示「加载中」？**
A: 检查浏览器控制台（F12）是否有 JS 错误。打开 `http://localhost:3456/` 而非 `file://`。如代理刚重启，等待几秒后刷新。

**Q: 面板上批量操作怎么用？**
A: 勾选卡片左上角 checkbox → 顶部操作栏出现 → 点击批量重置或批量屏蔽。

**Q: 管理弹窗如何批量导入 Key？**
A: 点击 📋 按钮 → 粘贴每行一个 `sk-xxx url 周期 备注` → 确定。

**Q: 配置弹窗的「重启代理」按钮点不了？**
A: 新版会在点击后显示重启进度，并在新实例就绪后自动刷新。若当前运行的是更新前的 `proxy.js`，需先在维护窗口手工重启一次，加载新版后该交互才会生效。

**Q: 为什么刷新 Dashboard 后需要重新输入管理 Token？**
A: 管理 Token 仅保存在当前页面内存，不写入浏览器持久化或会话存储。刷新、关闭页面或 WebSocket 认证失效后需要重新认证。

**Q: 为什么没有出现更新标识，或「一键升级」被禁用？**
A: 未修改的官方 Release 资产会自动识别版本；源码安装（克隆/复制源码仓库或源码 ZIP）会通过 `release-baseline.txt` 记录的本机版本自动识别；干净官方 Git Tag 也会自动识别。GitHub 有更高正式 Release 时会显示标识。开发分支、本地修改或来源未知的副本默认不显示标识，只展示 Release；这时可保持空白，或在确有把握时填写「定制构建基线 Tag（高级）」。可在系统配置中点击「检查更新」复查，弹窗和版本盒会显示本机版本号与最新正式 Release 的差距。为避免覆盖本地修改，面板只提供 Release 信息和安全升级步骤，不会自动替换源码。

## 贡献者致谢

本节归集经审查后被采纳的公开贡献、问题报告和设计建议；后续贡献者将按其实际贡献持续补充。

| 贡献者 | 贡献 |
|---|---|
| [@anupamme](https://github.com/anupamme) | 提出管理 Token 不应保存于浏览器会话存储的安全改进思路（PR [#1](https://github.com/aipayim/codex-proxy/pull/1)）。当前版本在最新代码上完成了内存态适配，并补齐 WebSocket 认证路径。 |

## 更新日志

- **2026-08-12 更新弹窗安全提示展示时机与备份清单微调**：安全升级步骤改为仅在 GitHub 存在新版本时展示；本机已是最新或来源未知时，弹窗只显示“一键升级已禁用，避免覆盖本地代码、配置或运行状态。”单句提示。备份步骤明确列出 `keys.json`（可能含 API Key 与上游地址等敏感信息，禁止上传到 GitHub/Release 或公开），与 `config.json`、`state.json` 一并备份。中英文翻译键同步更新。

- **2026-08-12 更新弹窗安全步骤展示条件与 Release 说明渲染修正**：修正上一版“仅在发现更新或本地来源未知时才显示完整安全升级步骤”的判断——现在只要成功获取 Release 信息（无论已是最新还是有更新），版本更新弹窗都完整展示五步安全升级流程与「GitHub 升级说明」入口；仅当 Release 获取失败时才退回单句禁用提示。另将 Release 更新说明由 `<pre>` 纯文本改为受限 Markdown 渲染（支持标题、无序列表、代码标记与 `https://` 链接），渲染前先 HTML 转义、仅放行 http/https 链接，杜绝 Release 内容注入 HTML/脚本；超长内容仍保留滚动查看。同步更新 README 对应说明。

- **2026-08-12 版本更新弹窗恢复完整安全升级步骤**：此前弹窗安全区域被压缩为一句“一键升级已禁用”，完整的“备份 → 审核 Release → 手动合并 → `node -c proxy.js` → 维护窗口重启”安全升级方案只在 README 中保留。现弹窗在 GitHub 存在新版本时，以列表形式完整展示上述安全升级步骤，并在弹窗内新增「GitHub 升级说明」入口链接；本机已是最新或无法比较时只显示单句禁用提示。备份步骤明确列出 `keys.json`（可能含 API Key，禁止上传/公开）。中英文翻译键一并补齐（`upd.safetyGuideTitle`、`upd.safetyStep1`–`upd.safetyStep5`、`upd.safetyUnverified`），并更新 `upd.safetyFull` 文案。仅修改 Dashboard 前端展示，更新检查后端逻辑不变。

- **2026-08-12 README 发布状态与 Release 链接修正**：修正 v2.55.0 发布说明中的 Markdown 换行问题，并将公开首页和 Release 文档中的 v2.55.0 状态统一为已发布；修复普通用户 Release 资产不包含 `build-release.js` 但文档仍指向该文件的问题，改为指向源码仓库的发布构建说明。

- **2026-08-11 Dashboard 中英文界面与语言切换**：Dashboard 新增中文/English 双语界面，支持通过右上角语言按钮切换，并使用当前浏览器的 `localStorage` 记住选择。默认仍为中文；切换只影响当前浏览器，不需要重启代理。修改 Dashboard 或 i18n 源码后需重启运行中的 Node.js 进程并刷新页面。

- **2026-08-10 终止原因跟踪与费用趋势修复**：新增 `stopReason`（协议层终止原因）全链路跟踪——Chat 上游 `finish_reason: length` 与 Responses 上游 `status: incomplete`（`incomplete_details.reason = max_output_tokens`）统一识别为 `max_tokens`，表示响应被模型输出上限截断。日志请求条目与 `stream_terminal` 事件记录该字段，日志表格在模型名旁显示琥珀色「截断」徽标、详情视图显示「终止原因」、CSV 新增 `stopReason` 列、全文检索覆盖该字段。此前 responses→messages 转换把截断一律当作 `end_turn`，信息丢失，现已区分。另修复「💰 费用」趋势图无柱状图问题：`vals` 计算误取不存在的 `cost` 字段（实际字段为 `totalCost`），导致全 `undefined`、`max` 恒为 1，整个趋势容器被 `display:none` 隐藏；且 `Math.max(...vals, 1)` 的 `$1` 下限使小时费用（通常远小于 1 美元）的柱子被压成 2px 细线。费用模式现取真实 `totalCost`、以实际最大值为基准（全零才兜底），柱状图恢复正常比例显示。

- **2026-08-09 请求/实际模型统一复合显示**：请求日志、模型分布、趋势图此前各显示一半信息——趋势图按“客户端请求的模型名”聚合（如 `claude-fable-5`），日志表格/模型分布按“实际转发并执行的模型名”聚合（如 `gpt-5.6-sol`），两处口径不一致且费用估算按请求名计价。现新增统一复合标签 `请求模型 (实际模型)`（如 `claude-fable-5 (gpt-5.6-sol)`），同时用于趋势图例、日志「模型」列、日志模型分布、Key 弹窗模型统计与并发跑马灯，一行即可看出“客户端要什么、上游实际用了什么”；两模型相同或只有其一时不加括号直接显示单名。模型适配计算提前到请求入口，费用估算改用实际执行模型（未适配时回退请求名），Key 配置的 `model` 覆盖字段优先级不变。日志展开详情、JSONL 与 CSV 仍保留独立的 `reqModel` / `overrideModel` 原始字段。新增模型显示标签与日志维度回归测试。

- **2026-08-09 动态协议识别持久化与 Chat 路由判定修复**：动态探测出的上游协议此前只存放在模型能力缓存里，模型探测失败（30 秒 TTL）后会短暂丢失，导致已正确识别为 Messages / Responses 的未知中继在请求时被错误降级回 Chat 直通。现动态协议写入独立缓存（24 小时 TTL），与模型列表缓存解耦，探测失败不再丢失协议分类；`/v1/chat/completions` 的多协议池判定从静态 URL 白名单改为使用动态协议结果，识别为中继 Messages / Responses 的中继自动启用 Chat→Messages / Chat→Responses 转换回退。新增后台每 10 分钟按 URL 去重的上游重探，同池多 Key 共享一次探测，新配/改配 Key 无需重启生效。新增协议缓存持久性与 Chat 路由判定回归测试。

- **2026-08-04 WebSocket 状态推送节流与发送背压（OOM 治理）**：Dashboard 状态广播此前在每次请求完成/状态变更时全量序列化全部 Key 状态并实时群发，无节流、无发送队列上限；后台或慢速标签页使 `ws.send` 缓冲无限增长，多次触发 `JavaScript heap out of memory`（堆 4GB）。现改为节流合并：任意窗口内状态推送最多每秒 1 次，多次变更合并为一次全量快照（全量非增量，不丢信息），另有 10 秒心跳兜底保证最终一致；每个 WebSocket 连接发送缓冲超过 4MiB 时主动断开，前端自动重连（断开期间以 5 秒 HTTP 轮询维持展示），重连后立即收到全量状态。容量退避/恢复、请求队列等业务路径不受影响。另细化 429/配额处理：分类正则加固（去除可能误判的裸 `limit exceeded` 匹配，改为限定 `usage limit`/具体限额消息）、`failReason` 持久化到状态文件，并让自动恢复跳过配额耗尽的 Key（每日探针不会把配额已尽的 Key 误恢复后再次 429）。
- **2026-08-03 编码流挂起根因修复（8h41m 长卡）**：Codex CLI 在一次上游周配额耗尽风暴中永久等待（本次实际约 8h41m）。根因修复：① `responsesIdleTimeout` 单位 bug——默认值误写成 `90*60*60*1000`（90 小时），实际被钳制为 24 小时，与文档声称的 90 分钟不符；已改为 `90*60*1000`，默认生效 90 分钟。② 配额/用量超限 429（`WEEKLY_LIMIT_EXCEEDED`、`MONTHLY_LIMIT_EXCEEDED`、`usage limit exceeded` 等）此前被当作瞬时容量反复退避+重排队，刷遍全部 Key 形成 429 风暴；现归因为 `insufficient_quota` 走 `markFailure` 当前周期冷却并快速失败，只有未归因为配额问题的 429 与 `model_at_capacity` 才走 `capUntil` 瞬时退避。③ 队列死锁——`enqueueRequest` 不触发 `processQueue`，且周期清理会静默丢弃等待中的请求（socket 不关、CLI 永久挂起）；现入队即 `setImmediate` 排空、新增 5 秒周期排空，让 `capacityMaxWaitSeconds` 超时的请求真正收到 503，并移除静默丢弃逻辑。④ 新增编码协议流**无进展看门狗** `responsesNoProgressTimeout`（默认 15 分钟）：长流上游停止发送字节但不断开时强制写失败终态并销毁上游连接，兜底「已接受但永不返回」的挂死连接；空闲超时只从请求发起时起算，无法覆盖这种中途停滞。⑤ `autoResume` 从 `resume --last`（多实例并发时会互踢/恢复错会话）改为 `fixed_session` + 确定 `sessionId`（固定到具体编码会话，消除重复会话竞争）。新增队列排空与配额分类回归测试。
- **2026-08-03 闲置恢复 runner 停滞接管**：修复容量错误后 Codex CLI 停在本地 Planning、代理长期没有新 Key 应用时，已启动的恢复 runner 仍存活而使“同一闲置周期仅一次”永久阻断的问题。新增默认 20 分钟的受控停滞宽限和默认一次的重启上限：只有无在途代理请求、随机运行 ID、PID 启动 tick、租约 PGID 与实际进程组全部匹配时，才会向该 runner 的负 PGID 发 `SIGTERM`；30 秒仍存活才发 `SIGKILL`，确认进程组退出后才额外启动一次。不会扫描、终止或影响手工启动的 Codex/Claude CLI；身份不明、遗留租约、PID 重用或进程组不一致时只记录事件并跳过。新增停滞、TERM/KILL、归属校验、Key 心跳重置和单次重启回归测试。
- **2026-08-02 SSE 容量错误退避**：修复上游在 HTTP 200 的 Responses / Messages SSE 内明确返回 `model_at_capacity` 时，代理仍误走硬失败路径的问题。此类错误现与 HTTP 429 一样只设置 `capUntil` 临时退避，不写 `failCode`、不触发跨周期废弃或 `all_keys_failed`；所有协议转换和原生 Responses 流共享该处理。已经向客户端输出的数据不会被自动重放，避免重复文本或工具调用。
- **2026-08-02 Responses / Messages 流终态一致性**：实际 Codex CLI 使用的原生 `/responses` 直通流现以旁路 SSE 探针确认 `response.completed`，上游 HTTP 200 提前 EOF/关闭/中止/错误/超时时会保留原字节并补一次 `response.failed`，不再把裸断流直接交给 CLI。Claude Code 的 `/v1/messages` 转换流新增同等生命周期保护：只有 Chat 上游明确 `[DONE]` 才发送一次 `message_stop`；异常终止改发 Anthropic `event: error`，不会伪造完成或重复终态。反向 Messages→Chat 转换同样只在真实 `message_stop` 后发送一次 `[DONE]`，异常输出 OpenAI 兼容 SSE 错误。专用长流总时长/空闲超时现在同时用于 Responses 与 Messages，保留原配置键名以兼容已有配置；新增双向终态、EOF、错误、UTF-8 分片和无换行终态回归测试。
- **2026-08-02 闲置恢复安全租约与确定会话**：闲置恢复不再按项目目录扫描、终止或 `SIGKILL` 任意 `codex` 进程，避免误伤人工启动的 CLI、分支或子代理。每次恢复由随机运行 ID、PID、进程组和 `/proc` 启动时间组成的原子 JSON 租约标识；只有租约可验证时才会视为自身 runner，代理不会向外部进程发送终止信号。仍有在途请求时会暂缓打开新终端；每个项目在一次连续 Key 闲置周期只尝试一次，直到实际应用新的 Key 或修改该项目配置才允许下一次，修复失败后不断重放/相互终止的问题。启动器返回成功仅表示 Windows 已接受请求，runner 状态和真实退出信号才是诊断依据；120 秒未收到 runner 状态会明确记录超时。系统配置新增“固定会话”模式，只有该模式才替换 `{sessionId}`，普通命令模式保留原命令；`resume --last` 会记录会话不确定性提示。新增 runner 生命周期回归测试。
- **2026-08-01 版本基线识别与升级提示**：新增 `release-baseline.txt` 版本基线文件，使源码安装（克隆/复制源码仓库或源码 ZIP，无构建元数据）也能显示本机版本号并判断是否可升级。每次发布 vX.Y.Z 时同步更新该文件（源码仓库 git 跟踪，并随 Release 资产内置，内容与发布 Tag 一致）。来源识别优先级扩展为：官方发布包（build-info 清单校验）> 干净官方 Git Tag > 源码基线文件 > 未知（带原因）。「系统配置 → 检查更新」弹窗新增「本机版本 / 最新正式 Release / 差距」显式信息条，本机版本号一目了然；版本盒同时显示本机版本、来源与最新正式 Release 及差距文案。徽标三态：有更新琥珀脉冲 `⬆`、无更新隐藏、基线未知灰色中性 `⬆`（均可打开弹窗查看最新 Release）。GitHub 缓存 TTL 由 6 小时缩短为 1 小时。
- **2026-08-01 闲置恢复 TTY 与残留进程清理 + 容量/429 瞬态退避**：闲置恢复启动器此前已能打开窗口并启动 runner，但失败仍频繁（退出码 2）。根因排查发现三层问题并逐一修复。① 命令错误：配置里 `resume --last -p '<prompt>'` 的 `-p` 是 codex 的 `--profile` 标志（并非 prompt），被解析为 `invalid value ... for '--profile'` 立即退出 2；codex 的 prompt 是位置参数，已改为 `resume --last '<prompt>'`。② runner 内 `setsid bash -lc` 把命令从终端剥离，`codex resume` 交互 TUI 需要 PTY；改用 `script -qec` 为命令分配伪终端（保留 `setsid` 进程组隔离，`-e` 回传子命令退出码）。③ 早期版本曾尝试按项目路径扫描并终止残留 codex；该方案已由 2026-08-02 的身份租约方案取代，当前版本不会扫描或终止外部 CLI。④ 重触发保护：空闲恢复生效后，巨型会话（数百 MB / 数十万行）加载需数分钟，期间无 Key 使用导致闲置时间持续超阈值，每 debounce 周期（默认 10 分钟）的自动重触发会把仍在加载的上一次 resume 当作旧命令替换终止，形成"永远跑不完"的循环；当前版本改为同一闲置周期单次尝试，并在有在途请求时暂缓启动。另新增 `capacityBackoffSeconds`（默认 60 秒）与 `capacityMaxWaitSeconds`（默认 300 秒）：HTTP 429 与 `model_at_capacity` 错误改为瞬态退避（仅设 `capUntil`，不写 `failCode`），不再把 reset:"never" 的 Key 冷却到整个周期、也不会一次容量波动级联禁用全部 Key；容量失败请求直接重新入队而非热轮询刷遍全部 Key，队列等待上限按 `capacityMaxWaitSeconds` 豁免默认 30 秒超时；`checkAllFailed` 忽略纯退避 Key，避免误报 all_keys_failed。新增容量退避生命周期回归测试。
- **2026-08-01 修复闲置恢复启动器失败**：闲置恢复此前每次触发都失败——`resume-codex.sh` 用 `cmd.exe /c start` 打开可见 Windows 终端时，WSL 路径（`/mnt/c/...`）以 `/` 开头会被 cmd 解析成开关（`无效开关 - "/mnt"`），`start` 立即返回退出码 1，runner 从未启动、命令从未执行。修复：启动前用 `wslpath -w` 把 WSL 路径转为 Windows 反斜杠形式再交给 `start`，并保留可见终端窗口；启动器失败时把 cmd 的 stderr（GBK 转 UTF-8）写回状态文件第 5 字段，代理将其显示为 `launcherError` 并带进 `auto_resume_launcher_failed` 事件消息。端到端实测通过：状态机 `starting → running → exited` 完整走通，命令真实执行。
- **2026-07-31 模型级费用估算**：系统配置新增 `modelPricing` 规则数组，可按最终转发的精确模型名分别设置输入/输出单价和 `bytesPerToken`；未命中模型继续回退全局价格。每个请求在开始转发时冻结规则，后续变更不回算进行中或既有统计、趋势及历史日志费用。
- **2026-07-31 Codex SQLite 日志维护**：系统配置新增可选的 Codex SQLite 日志维护开关、数据库路径检测、容量阈值、保留时长、检查周期和立即检查状态。启用保存前服务端会复验当前用户 `~/.codex/` 下的常规 `logs*.sqlite` 文件及 `logs(id, ts)` 结构；支持将 `\\wsl.localhost\发行版\...` / `\\wsl$\发行版\...` 自动转换为 WSL 内部路径。后台通过 Python 标准库的 SQLite 短事务限量删除过期行，忙时让步、不重启代理/watchdog/Codex CLI，且不主动执行 `VACUUM`、checkpoint 或操作 WAL/SHM。新增 SQLite 维护回归测试，Release 包包含运行 helper、继续排除所有测试源码。
- **2026-07-31 修复下游归集崩溃循环**：修复重启后代理反复 `ReferenceError: client is not defined` 崩溃并被 watchdog 重启的问题（`forwardRequest` 中新增 `client` 变量声明并透传给 5 处 `recordRequest` 调用，日志条目复用同一值）。新增回归测试：运行时断言 `recordRequest` 正确累计 `clients` 且未定义客户端被忽略、源契约断言 `forwardRequest` 必须先声明 `client` 再调用 `recordRequest`。
- **2026-07-31 下游趋势图按应用归集**：趋势图「🔻 下游」由「流终态分布」改为「按下游应用客户端（请求 User-Agent）归集请求数」的堆叠柱状图（Codex CLI / Claude Code / Cursor / Chatbox / Cherry Studio / NextChat / LobeChat / OpenAI SDK / Vercel AI SDK / curl 等，最多显示前 8 个应用，其余归入「其他」，悬停查看各应用次数）。请求记录新增 `client` 维度：`recordRequest` 同时累加小时/日桶的 `clients`，日志全文检索与 CSV 新增客户端列，日志详情显示「客户端」。流终态不再显示于趋势图，改由日志/CSV 的「流结果/流终态原因」与 `stream_terminal` 事件承载。
- **2026-07-31 趋势图上游归集与健康恢复**：趋势图新增「🔺 上游」模式，按 Key 的上游域名归集显示各上游使用次数（最多前 8 个域名，其余归入「其他」，悬停查看域名 + #Key 列表 + 次数）；原状态码分布恢复为「💚 健康」标签，两种上游/下游语义不再混用，标签顺序为 模型/流量/次数/健康/上游/下游/费用/延迟。
- **2026-07-31 运行时数据治理**：新增 `state.json` 小时/日统计裁剪、容量上限与原子备份恢复；请求 JSONL 按天分段并同时受保留期和总容量限制；`.log-summary.json` 有界；WSL watchdog 新增 `proxy-log-rotator.js` 控制台日志轮转及旧大文件迁移。保存系统配置后立即执行清理，缺失轮转器时不再回退到无限追加 `proxy.log`。
- **2026-07-31 上游错误捕获与下游流终态趋势**：Responses→Chat 转换不再静默丢弃上游 SSE 或非 2xx HTTP 错误体；错误信息会限长脱敏，记录为 `streamErrorMsg` 并按内容归类为 `model_at_capacity`（容量不足）、`insufficient_quota`（配额不足）、`upstream_api_error`（其他上游错误）。SSE 错误明确输出 `response.failed`；全部候选 Key 最终失败并返回 `502` 时新增 `downstream_terminal`，自动回退成功不误报为下游失败。日志、CSV 和历史全文检索新增错误分类/来源字段；流终态记录保持在日志/CSV 的「流结果/流终态原因」字段与 `stream_terminal` 事件范畴。后端补充 HTTP 错误体脱敏、容量分类、历史检索和回退语义回归测试。
- **2026-07-30 日志查询与事件处置**：默认日志视图优先使用内存尾部 50 条；代理重启后内存不足时由受限 Worker 补齐已保存的日志尾部，避免误以为历史日志被清空。修复未填写时间范围被误解析为 Unix 时间 0、导致历史与内存日志同时被筛空的问题。历史筛选、游标分页和导出同样由 Worker 反向扫描 JSONL，取消主线程全量计数/读取。新增分钟与日汇总、首次缺失时后台重建、按需 WebSocket 订阅及 350ms 批量刷新。新增按上游/模型/路径/分组的日志事件中心，可确认、静默、发送通知，并可人工临时暂停或恢复分组；不会自动修改 Key 或重启代理。
- **2026-07-29 Release 运行时精简**：构建产物不再包含构建器或任何回归测试；Release 内的 `package.json` 移除源码专用脚本，测试和构建固定在 GitHub 源码仓库完成。
- **2026-07-28 可取消与强制重启控制**：安全重启排空阶段新增取消控制，恢复新请求接入但不虚假恢复已拒绝的排队请求；排空满 30 秒后才允许二次确认强制重启。强制重启明确记录和提示会中断活跃流，新增重启生命周期回归测试，并同步独立备用面板。
- **2026-07-28 Key 心跳闲置恢复与 watchdog 重载**：闲置恢复改为仅依据实际应用上游 Key 的持久化心跳，不再受普通请求或状态文件节流影响；启动/保存配置后立即检查。安全重启在排空后请求 watchdog `exec` 重载自身，保持单实例锁，并新增闲置恢复与 watchdog 重载回归测试。
- **2026-07-28 Responses 流终态修复**：转换流仅在收到上游 `[DONE]` 后发送 `response.completed`；修复末尾无换行的 `[DONE]` 被漏解析、活动流 socket 空闲超时被忽略的问题。异常 EOF/关闭/错误/超时明确发送 `response.failed`，日志新增可检索的 `stream_terminal` 诊断事件和流终态字段；新增独立流生命周期回归测试。
- **2026-07-28 管理 Token 内存态**：Dashboard 管理 Token 改为仅存于当前页面内存；启动时清除旧版会话存储残留，HTTP 与 WebSocket 共用同一认证状态，WebSocket 返回 `4001` 时重新认证。感谢 @anupamme 提供安全改进思路。
- **2026-07-28 发布来源识别**：新增 `npm run build:release`，生成不含运行敏感文件的 Release 资产、`build-info.json` 和 SHA-256 清单。未修改的官方发布资产及干净官方 Git Release Tag 自动比较版本；开发/定制副本 fail-closed。watchdog 改为从自身目录定位代理，移除固定机器路径。
- **2026-07-28 版本基线修正**：移除固定本地版本和具体本机目录说明。`updateBaselineTag` 保留为定制构建的高级手动基线；来源未知时只展示 Release，不误报更新。覆盖式一键升级继续禁用。
- **2026-07-28 重启计时与版本检查**：重启遮罩新增独立 1 秒计时和状态请求超时，不再因旧进程连接切换而停在 `0 秒`。Dashboard 新增 GitHub Release 缓存检查、更新闪烁标识、版本/Release Notes 弹窗及系统配置版本链接；覆盖式一键升级保持禁用。
- **2026-07-28 Dashboard 重启进度**：系统配置的「重启代理」新增全屏动态进度，展示排空与 watchdog 恢复状态；通过实例 ID 确认新进程就绪后自动刷新面板。新增 `GET /__restart-status`，重启期间 API 请求明确返回短暂不可用状态。
- **2026-07-28 健康趋势图**：趋势图新增第 4 模式「健康趋势」，每个时段显示 200/4xx/5xx/失败的堆叠柱状图，支持 24h/7d/30d 时段；后端 `recordRequest` 新增 `statusCode` 参数，请求状态码按小时聚合存入 `hourly.statusCodes`，随 WebSocket 推送至前端
- **2026-07-28 close15 安全加固**：fetch 包装器改用 `new URL()` origin 判断（不再用字符串包含）；WebSocket 连接需 token 认证；`/metrics` 纳入管理 Token 认证范围；watchdog drain 超时 30s 后自动 kill 孤儿进程（不再无限等待）
- **2026-07-28 close14 安全加固**：timeout 重试修复（首包前超时不再浪费可用 Key）；watchdog drain 冲突修复（端口空闲但旧进程未退时不抢先启动新进程）；`/__admin_token` → `/__auth_check`（不再暴露实际 Token）；fetch 包装器仅对同源请求附加 Authorization

## License

MIT

---

官网：[OpenAPI.im](https://openapi.im) | 作者推特：[@C_2049s](https://twitter.com/C_2049s)
