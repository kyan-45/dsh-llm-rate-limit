# dsh-llm-rate-limit

Proactive LLM rate limiting for **DeepSeek Harness (DSH)** — v2.

Instead of reacting to provider 429s after the fact, this plugin keeps
outbound model requests *under* the provider's limit. Every
`ctx.llm.stream()` call passes a per-provider admission queue before the
provider fetch starts; a request is granted only when **all** hard gates allow
it at that instant, and observed throttles feed an adaptive cooldown ladder and
an AIMD concurrency bound.

## Why

DeepSeek's documented rate limit is **account-level concurrency** (a request is
"in flight" from send to response completion; limits are 2500 for
`deepseek-flash` and 500 for `deepseek-v4-pro` per account — requests beyond
the limit get HTTP 429). DSH agents routinely exceed this on their own:
parallel tool calls, subagents, and concurrent sessions all share the same
account. The built-in `dsh-llm-retry` plugin *recovers* from a 429 at the agent
step boundary (exponential backoff + Retry-After), but each retry is a fresh
provider request. This plugin *prevents* the 429 in the first place.

The two work together: the gate keeps traffic below the limit, and llm-retry
still handles any 429 that slips through. The plugin itself never retries and
never re-issues a provider request.

## What it does (v2 algorithm set)

- **Admission queue (`llm/stream`)** — one pump loop per route; every wait
  state is a hard eligibility gate: concurrency, cooldown, RPM, TPM, drip
  pacing. Waiting is bounded by `maxWaitMs` and happens only *before* the
  provider fetch.
- **Concurrency, static or AIMD** — by default a static per-route bound
  (`maxConcurrent`, exactly v1). With `adaptiveConcurrency: true` the bound
  grows additively (+1 per `aimdIncreaseEvery` clean settles) and drops
  multiplicatively (× `aimdDecreaseFactor` on a 429/503/529 or a queue
  wait-timeout), clamped to `[minConcurrent, maxConcurrent]`. AIMD never sheds
  or retries anything; it only moves the bound.
- **Sliding-window RPM budget** — an exact log of admission timestamps
  (no counter-reset boundary burst), size bounded by `rpmLimit`. Charged at
  grant, never at enqueue.
- **Sliding-window TPM budget** — a request *reserves* an estimated
  input + output token count at admission and the stream's `usage` chunk
  *settles* it with the observed value. Without observed usage the full
  reservation stays charged until it expires (conservative by design). The
  token log is capped by `tpmLogMaxEntries` with pairwise-merge compaction.
- **Priority + fairness** — requests are classified by
  `GenerateOptions.purpose`: `interactive` (undefined), `background`
  (`compaction`), `utility` (`session-title`). Classes are served by an
  integer-stride weighted scheduler (`interactiveWeight:backgroundWeight:
  utilityWeight`, default 6:2:1 ⇒ strides 10/30/60), so every backlogged class
  makes progress — there is deliberately **no** strict priority, so
  interactive work cannot starve the rest. Inside a class, per-session
  round-robin keeps one session's fan-out from taking every slot, and
  `agingThresholdMs` forces the oldest waiter past the schedule once it has
  waited that long. A session's service counter is **retained when its queue
  drains**, so a session that finishes a burst and immediately re-arrives is
  not treated as freshly served (it yields to sessions that have not been
  served yet); the retained state is bounded to 1024 drained sessions per route
  with the oldest forgotten first, and a retained counter is clamped to 16 so a
  long-lived session cannot be pushed arbitrarily far behind newer ones. With
  `fairness: false` (default) the queue is exactly v1's single FIFO.
- **Cooldown: Retry-After, ladder, jitter, hysteresis** — the cooldown window
  is exactly

  ```
  base   = providerRetryAfterMs > 0 ? providerRetryAfterMs : cooldownBaseMs
  window = min(base × 2^min(escalations, 4), cooldownMaxMs)
  window = status ∈ {503, 529} ? min(window × overloadCooldownMultiplier, cooldownMaxMs) : window
  window = providerDirected ? window : jitteredWindow(window, cooldownJitter)
  ```

  so a positive `Retry-After`/`providerRetryAfterMs` hint **replaces the base**
  of the ladder (it is not a fixed extra delay) and, being server-directed, is
  never jittered; it is still capped by `cooldownMaxMs` like any other window.
  A locally computed base keeps v1's ladder `cooldownBaseMs × 2^escalations`
  (exponent capped at 4, i.e. up to 16×) and is jittered when
  `cooldownJitter` is `equal` (`w/2 + rng × w/2`) or `full` (`rng × w`).
  With `cooldownHysteresisMs > 0` the ladder survives the window closing and
  individual successes, decaying only after that much quiet time.
- **Drip release** — with `releaseSpreadMs > 0` a drained queue is released
  one waiter per step (each step `releaseSpreadMs + jitter`) instead of
  fanning out in one tick, so the provider is not hit by a thundering herd the
  moment a cooldown closes. Every waiter's own `maxWaitMs` still bounds the
  drip.
- **Wait cap + queue cap** — a request that cannot be admitted within
  `maxWaitMs` fails the step with `RATE_LIMIT_GATE_TIMEOUT` instead of
  hanging; a request arriving when `queueLimit` waiters are already queued is
  shed immediately with `RATE_LIMIT_QUEUE_FULL`; a request whose own TPM
  reservation exceeds the whole budget is shed immediately with
  `RATE_LIMIT_BUDGET_EXCEEDED` (it could never fit in one window).
- **Model-visible note** — an optional system-prompt section tells the model
  that requests are rate-gated so it can pace parallel work.
- **Operator observability** — one state route (a strict superset of the v1
  JSON, so the existing Settings card keeps working), a `/rate-limit` command
  that prints a deterministic text report, and log lines for every shed and
  throttle observation.

## Failure codes

Every refusal is a terminal `finish` chunk yielded by the gate; the plugin
never emits a failure for a request it admitted and never returns
`{ kind: 'retry' }`.

| code | emitted when | message |
| --- | --- | --- |
| `RATE_LIMIT_GATE_TIMEOUT` | a queued wait exceeded `maxWaitMs` (concurrency, cooldown, RPM, TPM, or drip pacing) | `dsh-llm-rate-limit: queued wait exceeded maxWaitMs without a free slot` (v1, byte-identical) |
| `RATE_LIMIT_QUEUE_FULL` | `queueLimit` waiters already queued | `dsh-llm-rate-limit: queue full (<queued>/<queueLimit> waiters on route "<provider>")` |
| `RATE_LIMIT_BUDGET_EXCEEDED` | the request's own TPM reservation exceeds `tpmLimit` | `dsh-llm-rate-limit: request needs <need> tokens but the route's per-minute budget is <tpmLimit>` |
| `RATE_LIMIT_GATE_ABORTED` | `options.signal` aborted while queued (reported as `kind: 'aborted'`, i.e. a cancellation, not a throttle error) | `dsh-llm-rate-limit: queued request cancelled before a slot was granted` |
| `RATE_LIMIT_GATE_DISPOSED` | the plugin was disposed while the request was queued | `dsh-llm-rate-limit: gate disposed while request was queued` |

## Install

```bash
dsh plugin --profile <name> add ./dsh-llm-rate-limit
```

Then restart DSH. Adjust the configuration in `cordis.patch.yml` (or your home
composition overlay) to taste — see the table below.

## Configuration

Set in the patch `config:` block. **Every key is optional.** The eight v1 keys
keep their v1 meanings; every v2 key defaults to the neutral value that makes
that dimension behave exactly like v1, so a v1-only config is unchanged.

### v1 keys

| key | default | meaning |
| --- | --- | --- |
| `enabled` | `true` | master switch; `false` registers nothing at all |
| `providers` | `[]` | route names to gate; empty = every route (unlisted routes pass ungated) |
| `maxConcurrent` | `4` | max in-flight provider requests per route (also the AIMD ceiling) |
| `maxWaitMs` | `120000` | max queue wait before refusing a request |
| `cooldownBaseMs` | `5000` | first 429 cooldown window (Retry-After replaces it when present) |
| `cooldownMaxMs` | `80000` | cap for the escalated cooldown window |
| `systemPrompt` | `true` | inject the model-visible note |
| `sectionOrder` | `90` | order of that note |

### v2 keys

| key | default | meaning |
| --- | --- | --- |
| `rpmLimit` | `0` (off) | admissions per `rpmWindowMs`; `0` disables request-rate accounting |
| `rpmWindowMs` | `60000` | RPM sliding-window length |
| `tpmLimit` | `0` (off) | token budget per `tpmWindowMs`; `0` disables token accounting |
| `tpmWindowMs` | `60000` | TPM sliding-window length |
| `tpmOutputReserveTokens` | `1024` | output tokens reserved when `maxTokens` is absent |
| `tpmLogMaxEntries` | `4096` (≥ 16) | compaction cap for the TPM log |
| `adaptiveConcurrency` | `false` | `true` enables the AIMD bound |
| `minConcurrent` | `1` | AIMD floor (clamped to `maxConcurrent`) |
| `aimdInitialConcurrent` | `null` | starting bound; `null` ⇒ `clamp(ceil(maxConcurrent / 2), minConcurrent, maxConcurrent)` |
| `aimdIncreaseEvery` | `20` | clean settles per +1 |
| `aimdDecreaseFactor` | `0.5` | multiplicative decrease on throttle (`0 < f < 1`) |
| `aimdTimeoutDecrease` | `true` | treat a queue wait-timeout as a loss signal |
| `fairness` | `false` | `true` = weighted classes + per-session RR; `false` = v1 FIFO |
| `interactiveWeight` | `6` | class weight (stride `round(60 / weight)`) |
| `backgroundWeight` | `2` | class weight |
| `utilityWeight` | `1` | class weight |
| `agingThresholdMs` | `0` (off) | force the oldest waiter after this long |
| `queueLimit` | `256` | waiters per route before shed with `RATE_LIMIT_QUEUE_FULL` |
| `releaseSpreadMs` | `0` (off) | drip release step; each subsequent grant lands in `[1, 2] × step` |
| `cooldownJitter` | `'none'` | `'none' \| 'equal' \| 'full'`; never applied to a server-directed Retry-After |
| `cooldownHysteresisMs` | `0` (off) | quiet time before the escalation ladder decays |
| `overloadCooldownMultiplier` | `1` | extra multiplier for 503/529 windows |

Invalid values are never fatal: each one logs
`dsh-llm-rate-limit: ignoring invalid <key>=<JSON> (expected <rule>); using <default>`
and falls back to its default.

### Choosing `maxConcurrent`

- Single DSH installation, dedicated API key: `4`–`16` is safe for
  `deepseek-flash` (limit 2500) and `2`–`4` for `deepseek-v4-pro` (limit 500).
- Shared account (other clients, CI, other workspaces): keep it small (2–4)
  and prefer the cooldown over raw throughput — the gate's job is to avoid
  429s, not to maximize requests.
- `rpmLimit` / `tpmLimit` are *self-imposed* budgets: no provider documents an
  RPM/TPM figure, so pick a value below the account's real quota and tune it
  from the `/rate-limit` report.

## Operator access

- `GET /plugins/dsh-llm-rate-limit/state` — the live JSON snapshot (GET only;
  405 otherwise; guarded by the host `connection` service when present). It is
  a strict superset of the v1 payload: every v1 field (`provider`, `active`,
  `queued`, `queuedPeak`, `waits`, `waitTimeouts`, `cooldownRemainingMs`,
  `last429At`, and `stats.rateLimit429/success/error/timeout`) keeps its name,
  type and unit.
- `/rate-limit [route]` — a deterministic, ASCII-only text report: per-route
  in-flight/adaptive bound, queued and peak, class distribution, RPM/TPM
  usage, cooldown state, wait histogram by reason, sheds and throttles. The
  optional `route` argument is honoured only when it names a governed route
  (the configured provider names, or `*`); any other input — including a typo
  or a route that is not governed — filters nothing and prints the full report
  for every route.
- Logs: one summary line at apply, one line per shed
  (`dsh-llm-rate-limit: shed <code> on "<provider>" (<reason>)`) and v1's
  unchanged throttle-observation line.

## Storage

None. The gate is purely in-memory (per host process start). Stats reset on
restart; the settings panel reads live state over
`/plugins/dsh-llm-rate-limit/state`.

## Remaining limitations

- **In-memory only.** Budgets, queues, the cooldown ladder and the AIMD bound
  live in one process; a restart starts from the configured defaults.
- **No cross-process coordination.** Two DSH hosts (or a host plus another
  client) sharing one provider account do not share RPM/TPM/concurrency state,
  so each host must be configured with a share of the account budget. The
  per-process jitter does decorrelate their cooldown windows, but there is no
  shared token bucket and no leader election.
- **No response-header parsing.** The `llm/stream` waterfall receives only
  `(options, next)` — there is no response object and no header access — so
  `x-ratelimit-*`-style headers cannot be read. The only throttle signals used
  are the `agent/request-error` failure (`status`, `code`,
  `providerRetryAfterMs`), the stream's `usage` chunk and its terminal
  `finish` reason.
- **No retry ownership.** Recovery stays with `dsh-llm-retry`; the plugin
  never retries and never returns `{ kind: 'retry' }`.
- **No latency-based concurrency control** (Vegas/Gradient2): no server-side
  queue-delay signal is available, so AIMD is used instead of a latency
  estimator.
- **No per-model or per-key budgets.** The documented provider limit is
  account-level, so the route (`options.provider`) is the granularity.

## Non-fatal degradation

- Host without the `llm` service / `llm/stream` waterfall: the listener is
  still registered (it only fires if the host dispatches the event); the
  plugin logs and continues.
- The model-visible note is registered through the host's `systemPrompt`
  service (read via `ctx.get('systemPrompt')`); when the service is absent the
  section is skipped and a warning is logged.
- No `webServer`/`httpServer`: the state route is skipped. No `commands`
  service: `/rate-limit` is skipped. No `connection` service: the state route
  skips the `requestRejection` guard. No `tokenMeter`: input-token estimation
  falls back to a local 4-chars-per-token heuristic.
- When `providers` is empty every route is governed by the `"*"` limiter; when
  it is non-empty only the listed routes are gated and unlisted routes pass
  through ungated (no `"*"` fallback for them).
- Every timer is `unref()`'d except one: the per-route deadline anchor is
  `ref()`'d only while a waiter is queued (it is what makes a queued wait
  terminate at `maxWaitMs`). An idle plugin holds **zero** referenced handles,
  so the host can exit immediately, and `dispose()` clears every timer and
  refuses every waiter, so a reload cannot fire into the next generation.

## Tests

```bash
node --test
```

The suite covers the v1 contract (`test/limiter.test.js` and
`test/index.test.js` are byte-for-byte unchanged and still pass), the config
schema and its validation, both sliding windows, AIMD, the scheduler, the
cooldown machine, token estimation, and the v2 integration surface (wiring,
gate outcomes, the no-leak invariant, disposal and the timer anchor).
