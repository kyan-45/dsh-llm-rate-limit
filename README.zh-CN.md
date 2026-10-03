# dsh-llm-rate-limit（DSH 插件）

> 简体中文用户文档。开发者文档见 [README.md](./README.md)（英文）。

## 这个插件是干什么的？

一句话：**给你的 DeepSeek 请求做"门卫"，让 429（限流报错）尽量不发生。**

DSH 里的 agent 经常同时发很多 LLM 请求（并行工具调用、子 agent、多会话），共用同一个 DeepSeek 账号。DeepSeek 的账号级并发上限是有限的（例如 `deepseek-flash` 2500、`deepseek-v4-pro` 500），超了就返回 429。

内置的 `dsh-llm-retry` 插件只能在**收到 429 之后**才重试恢复；这个插件则在**请求发出去之前**先排好队、数好额度，让请求始终压在限额之内。两者搭配使用效果最好：本插件负责"不越线"，llm-retry 负责兜底。

工作原理（通俗版）：

- 每个 LLM 请求出发前先过一道**准入队列**，同一时间能出去几个请求由你配置的 `maxConcurrent` 决定（也可开启自适应并发，越拥越收缩、越闲越大）；
- 观察到 429 / 503 / 529 后自动**冷却**一段时间，期间新请求排队而不是继续撞墙；冷却尊重服务方给的 `Retry-After`，并逐级加码；
- 可选的**每分钟请求数（RPM）/ 每分钟 token 数（TPM）自设预算**，超了就排队；
- 可选的**公平排队**：交互 > 后台 > 工具类任务按权重轮转，单个会话不会把队列占满，排得久的请求会被优先插队；
- 本插件**从不自己重试、从不重新发请求**——被拒的请求会以明确的失败码结束当前步骤。

## 安装

把插件目录（或 git/npm 地址）加入你的 DSH profile：

```bash
dsh plugin --profile <你的profile名> add ./dsh-llm-rate-limit
```

然后**重启 DSH** 生效。

> 桌面版用户：等价操作是打开 dsh 应用的"插件"管理界面安装（本插件在 desktop profile 中以 Junction 链接方式安装，改源码即生效）。

安装后默认配置就能用（详见下文"配置"）。

## 在哪里看它的工作状态？

三处，任选：

1. **设置面板**：DSH 设置 → "LLM 限流"（`settings.plugins.tab`），可以看到当前生效配置、实时状态（在途 / 排队 / 冷却剩余 / 最近一次 429）和本次启动以来的统计（观测到 429、成功流、失败流、排队超时）。界面跟随 DSH 自身语言切换。
2. **斜杠命令**：在 DSH 里输入 `/rate-limit`（或 `/rate-limit <路由名>`），打印一份确定性的文本报告：每个路由的在途/排队/峰值、RPM/TPM 用量、冷却状态、超时直方图等。
3. **HTTP 路由**：`GET /plugins/dsh-llm-rate-limit/state`，返回 JSON 实时快照。

## 配置

在 profile 的 `cordis.patch.yml`（或宿主组合 overlay）里找到本插件的 `config:` 块。本插件安装时自带的 `cordis.patch.yml` 已填好一套稳妥的默认值，通常不用改。**所有键都可以省略**，省略时按该键的中性默认值工作。

### 常用 6 个键（先掌握这些就够了）

| 键 | 自带默认 | 说明 |
| --- | --- | --- |
| `enabled` | `true` | 总开关；`false` 时装插件但完全不拦截 |
| `providers` | `[]` | 要拦截的路由名列表；**留空 = 所有路由都拦截**；只填某几个就只管那几个 |
| `maxConcurrent` | `4` | 每个路由同一时刻最多几个请求在途。单台 DSH、独享 key：flash 可 4–16，pro 建议 2–4；多客户端共用账号就小一点 |
| `maxWaitMs` | `120000` | 一个请求最多排多久（毫秒），超了就以"排队超时"失败而不是无限挂住 |
| `cooldownBaseMs` | `5000` | 第一次 429 后的冷却时长（毫秒）；之后每个 429 翻倍，封顶 `cooldownMaxMs`。服务方给了 `Retry-After` 时直接用它 |
| `adaptiveConcurrency` | `true`（自带配置） | 自适应并发：连发 20 次没出事 +1，见到 429 立刻 ×0.5，钳在 `[minConcurrent, maxConcurrent]` 之间 |

### 进阶键（按需开启）

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `rpmLimit` / `rpmWindowMs` | `0`（关）/ `60000` | 自设"每分钟最多几个请求"预算，`0` 表示不限制 |
| `tpmLimit` / `tpmWindowMs` | `0`（关）/ `60000` | 自设"每分钟最多多少 token"预算；请求进入时先**预留**估算的输入+输出，流式结束用真实 usage 结算 |
| `fairness` | `false` | `true` 启用加权公平排队：`interactiveWeight:backgroundWeight:utilityWeight`（默认 6:2:1），会话内轮转 + `agingThresholdMs` 插队兜底；`false` 时就是普通先进先出 |
| `releaseSpreadMs` | `0`（关） | "滴水"放行：冷却结束后不是一拥而上，而是每 N 毫秒放一个，避免惊群。建议与 `rpmLimit` 配套（如 200ms + 300/分钟） |
| `cooldownJitter` | `'none'` | 冷却窗口抖动：`none` / `equal`（半窗内均匀）/ `full`（全窗内随机），多个客户端共享账号时开 `equal` 能错开冷却结束点 |
| `cooldownHysteresisMs` | `0` | 阶梯迟滞：冷却结束、请求成功后，要安静这么久才把"翻倍阶梯"降回去，避免 429 时反复拉锯 |
| `overloadCooldownMultiplier` | `1` | 遇到 503/529（过载）时冷却窗口再乘几倍 |
| `queueLimit` | `256` | 每个路由最多排多少个请求，再来的直接拒绝（`RATE_LIMIT_QUEUE_FULL`） |
| `systemPrompt` / `sectionOrder` | `true` / `90` | 是否往系统提示词里加一段"本部署有并发门控，请节制成并行"的说明，让模型自己配合排队 |

### 配置坏了会怎样？

**不会炸。** 每个非法值（类型不对、越界）都只打一条 `ignoring invalid <key>...` 日志并回退到该键默认值，插件照常启动。

## 请求被拒了，是什么码？

被拒的请求在流里收到一个终止 `finish` 块，带明确的失败码（都是"到此为止"的终态，本插件永不发 `{kind:'retry'}`）：

| 码 | 什么时候出现 |
| --- | --- |
| `RATE_LIMIT_GATE_TIMEOUT` | 排队超过 `maxWaitMs` 还没等到空位 |
| `RATE_LIMIT_QUEUE_FULL` | 队列已满（`queueLimit` 个在等），新来的直接让路 |
| `RATE_LIMIT_BUDGET_EXCEEDED` | 这个请求自己预留的 token 数就超过整个 TPM 预算，排再久也塞不下 |
| `RATE_LIMIT_GATE_ABORTED` | 排队期间请求被取消（signal abort），按"取消"而非"限流"计 |
| `RATE_LIMIT_GATE_DISPOSED` | 插件被卸载/重载时还排着队 |

日志里每次拒绝都有一条 `dsh-llm-rate-limit: shed <码> on "<路由>"（<原因>）`，方便定位。

## 存储

**没有持久化。** 队列、预算、冷却阶梯、AIMD 并发值全在内存里，DSH 重启即清零（统计也是"本次启动以来"）。设置面板读的是实时快照，不需要数据库。

## 已知限制（心里有数即可）

- **不跨进程协调**：两台 DSH（或 DSH + 别的客户端）共用同一个账号，各自维护一套内存状态、互不知道。共享账号时请各自把预算设成份额，并开 `cooldownJitter: equal` 错开冷却。
- **读不到响应头**：`llm/stream` 瀑布线拿不到响应对象，所以 `x-ratelimit-*` 这类头读不了；限流信号只来自 429/503/529 失败事件、流的 usage 块和终止原因。
- **不按模型/key 细分**：DeepSeek 公布的是账号级限额，所以门控粒度是"路由（提供方）"。
- 恢复仍归 `dsh-llm-retry` 管：漏网的 429 由它指数退避重试。

## 开发

- 跑全部测试：`node --test`（含 v1 字节级回归 + v2 集成 + 面板 face 回归）。
- 算法细节、逐键默认值与降级行为：见英文 [README.md](./README.md)。
