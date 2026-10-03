/**
 * Core limiter: per-provider admission gate, sliding-window budgets, AIMD
 * concurrency, fair queueing, and cooldown.
 *
 * Every outbound provider request is admitted through `gate()`, which queues
 * the request until **all** of the following hard gates are satisfied:
 * concurrency (static `maxConcurrent` or the AIMD bound), the cooldown window,
 * the RPM sliding window, the TPM sliding window, and drip pacing. Waiting
 * happens only in the queue, never inside the provider call path, and a waiter
 * is charged (RPM record, TPM reservation) only at grant.
 *
 * v1 compatibility: `active`, `waiters`, `cooldownUntil`, `cooldownTimer`,
 * `pumpTimer`, `cooldownEscalations`, `waits`, `queuedPeak`, `timeoutCount`,
 * `last429At`, `activeCount`, `queued`, `cooldownMs`, `acquire`, `drop`,
 * `release`, `expireWaiters`, `pump`, `observe429`, `closeCooldown`,
 * `resetEscalation` and `snapshot` keep their names, units, and meanings. Every
 * v2 subsystem is inert under the neutral defaults, so a v1-only config
 * behaves exactly like v1.
 *
 * @module dsh-llm-rate-limit/limiter
 */
import { normalizeLimiterOptions } from './config.js';
import { RequestWindow, TokenWindow } from './window.js';
import { Aimd } from './aimd.js';
import { FairQueue, FifoQueue, classify, keyFor } from './scheduler.js';
import { Cooldown } from './cooldown.js';
import { usageTokens } from './tokens.js';
import { WAIT_BUCKETS } from './observability.js';

/** Failure codes emitted by the admission gate. */
export const FAILURE_CODES = Object.freeze({
    TIMEOUT: 'RATE_LIMIT_GATE_TIMEOUT',
    QUEUE_FULL: 'RATE_LIMIT_QUEUE_FULL',
    BUDGET_EXCEEDED: 'RATE_LIMIT_BUDGET_EXCEEDED',
    ABORTED: 'RATE_LIMIT_GATE_ABORTED',
    DISPOSED: 'RATE_LIMIT_GATE_DISPOSED',
});

/** The v1 timeout message, byte-identical. */
export const TIMEOUT_MESSAGE = 'dsh-llm-rate-limit: queued wait exceeded maxWaitMs without a free slot';

const ABORTED_MESSAGE = 'dsh-llm-rate-limit: queued request cancelled before a slot was granted';
const DISPOSED_MESSAGE = 'dsh-llm-rate-limit: gate disposed while request was queued';

/** HTTP statuses that count as a throttle signal. */
const THROTTLE_STATUSES = Object.freeze([429, 503, 529]);
/** Failure codes that count as a throttle signal. */
const THROTTLE_CODES = Object.freeze(['RATE_LIMIT', 'QUOTA', 'OVERLOADED']);

/** True when a downstream failure carries a throttle signal. */
export function isThrottleFailure(failure) {
    if (failure === undefined || failure === null)
        return false;
    if (THROTTLE_STATUSES.includes(failure.status))
        return true;
    return THROTTLE_CODES.includes(failure.code);
}

/** Build a refusal outcome (`ok: false`). */
function refusal(reason, code, message, waitMs) {
    return { ok: false, reason, code, message, waitMs };
}

/**
 * One provider route's admission gate.
 */
export class ProviderLimiter {
    /**
     * @param provider - provider route name, e.g. "deepseek-official".
     * @param config - raw or resolved limiter configuration.
     * @param deps - optional `{ now, random, setTimeout, clearTimeout }`
     *   injections for deterministic tests.
     */
    constructor(provider, config, deps = {}) {
        this.provider = provider;
        this.config = normalizeLimiterOptions(config);
        /** v1 fields, unchanged in name and meaning. */
        this.maxConcurrent = this.config.maxConcurrent;
        this.maxWaitMs = this.config.maxWaitMs;
        this.active = 0;
        this.cooldownTimer = null;
        this.pumpTimer = null;
        this.waits = 0;
        this.queuedPeak = 0;
        this.timeoutCount = 0;
        this.last429At = null;

        /** v2 subsystems. */
        this.queue = this.config.fairness ? new FairQueue(this.config) : new FifoQueue();
        this.cooldown = new Cooldown(this.config);
        this.rpm = new RequestWindow(this.config.rpmLimit, this.config.rpmWindowMs);
        this.tpm = new TokenWindow(this.config.tpmLimit, this.config.tpmWindowMs, this.config.tpmLogMaxEntries);
        this.aimd = new Aimd(this.config);
        this.nextDispatchAt = 0;
        this.deadlineTimer = null;
        this.hysteresisTimer = null;
        this.disposed = false;
        this.tickets = new Set();
        this.waitsByReason = { concurrency: 0, cooldown: 0, rpm: 0, tpm: 0, release: 0 };
        this.waitHistogram = new Array(WAIT_BUCKETS.length).fill(0);
        this.sheds = { queueFull: 0, budgetExceeded: 0, aborted: 0, disposed: 0 };
        this.spreadGrants = 0;
        this.bursts = 0;
        this.waiterSeq = 0;
        this.ticketSeq = 0;
        this.deps = {
            now: typeof deps.now === 'function' ? deps.now : Date.now,
            random: typeof deps.random === 'function' ? deps.random : Math.random,
            setTimeout: typeof deps.setTimeout === 'function'
                ? deps.setTimeout
                : (fn, ms) => globalThis.setTimeout(fn, ms),
            clearTimeout: typeof deps.clearTimeout === 'function'
                ? deps.clearTimeout
                : (handle) => globalThis.clearTimeout(handle),
        };
    }

    get queued() {
        return this.queue.size;
    }

    get activeCount() {
        return this.active;
    }

    /** The queued waiter records (v1's `waiters`, as a snapshot). */
    get waiters() {
        return this.queue.toArray();
    }

    /** Milliseconds until the cooldown window closes (0 when none is pending). */
    get cooldownMs() {
        const remaining = this.cooldown.until - this.deps.now();
        return remaining > 0 ? remaining : 0;
    }

    /** v1-compatible view of the cooldown state machine. */
    get cooldownUntil() {
        return this.cooldown.until;
    }

    set cooldownUntil(value) {
        this.cooldown.until = value;
    }

    /** v1-compatible view of the escalation ladder. */
    get cooldownEscalations() {
        return this.cooldown.escalations;
    }

    set cooldownEscalations(value) {
        this.cooldown.escalations = value;
    }

    /** The concurrency bound that applies right now. */
    effectiveConcurrency() {
        return this.config.adaptiveConcurrency
            ? this.aimd.effectiveConcurrency()
            : this.config.maxConcurrent;
    }

    // ---- admission --------------------------------------------------------

    /**
     * v1 shape: acquire one slot and report whether it was granted.
     * Abort, wait-timeout, queue-full, budget-exceeded and disposed all
     * resolve `false`.
     * @param signal - abort signal; cancels a pending wait.
     */
    acquire(signal) {
        return this.gate({ signal }).then((outcome) => outcome.ok === true);
    }

    /**
     * Rich admission form. Resolves with an `Outcome`:
     * `{ ok: true, ticket }` or `{ ok: false, reason, code, message, waitMs }`.
     *
     * @param request - `{ signal?, purpose?, sessionId?, estimatedTokens? }`.
     */
    gate(request) {
        const req = request ?? {};
        const now = this.deps.now();
        if (this.disposed)
            return Promise.resolve(refusal('disposed', FAILURE_CODES.DISPOSED, DISPOSED_MESSAGE, 0));
        const signal = req.signal;
        if (signal !== undefined && signal.aborted) {
            this.sheds.aborted += 1;
            return Promise.resolve(refusal('aborted', FAILURE_CODES.ABORTED, ABORTED_MESSAGE, 0));
        }
        if (this.queue.size >= this.config.queueLimit) {
            this.sheds.queueFull += 1;
            const message = `dsh-llm-rate-limit: queue full (${this.queue.size}/${this.config.queueLimit} waiters on route "${this.provider}")`;
            return Promise.resolve(refusal('queue-full', FAILURE_CODES.QUEUE_FULL, message, 0));
        }
        const need = Number.isFinite(req.estimatedTokens) && req.estimatedTokens > 0
            ? Math.ceil(req.estimatedTokens)
            : 0;
        if (this.tpm.enabled && need > this.config.tpmLimit) {
            this.sheds.budgetExceeded += 1;
            const message = `dsh-llm-rate-limit: request needs ${need} tokens but the route's per-minute budget is ${this.config.tpmLimit}`;
            return Promise.resolve(refusal('budget-exceeded', FAILURE_CODES.BUDGET_EXCEEDED, message, 0));
        }

        return new Promise((resolve) => {
            const waiter = {
                id: ++this.waiterSeq,
                resolve,
                enqueuedAt: now,
                deadline: now + this.config.maxWaitMs,
                classId: classify(req.purpose),
                key: keyFor(req.sessionId),
                need,
                signal,
                onAbort: null,
                reason: null,
                granted: false,
                resolved: false,
            };
            this.queue.enqueue(waiter);
            this.queuedPeak = Math.max(this.queuedPeak, this.queue.size);
            if (signal !== undefined && typeof signal.addEventListener === 'function') {
                waiter.onAbort = () => {
                    if (waiter.granted || waiter.resolved)
                        return;
                    if (!this.queue.remove(waiter))
                        return;
                    waiter.resolved = true;
                    this.sheds.aborted += 1;
                    this.rearmDeadline();
                    waiter.resolve(refusal('aborted', FAILURE_CODES.ABORTED, ABORTED_MESSAGE, this.deps.now() - waiter.enqueuedAt));
                };
                signal.addEventListener('abort', waiter.onAbort, { once: true });
            }
            this.armDeadline(now);
            this.pump(now);
        });
    }

    /**
     * Remove one waiter from the queue (v1 abort bookkeeping). Returns whether
     * the waiter was still queued; a queued waiter is refused as `aborted`.
     */
    drop(waiter) {
        if (waiter === undefined || waiter === null || waiter.granted || waiter.resolved)
            return false;
        if (!this.queue.remove(waiter))
            return false;
        waiter.resolved = true;
        this.detachAbort(waiter);
        this.rearmDeadline();
        waiter.resolve(refusal('aborted', FAILURE_CODES.ABORTED, ABORTED_MESSAGE, this.deps.now() - waiter.enqueuedAt));
        return true;
    }

    /**
     * Release one previously acquired slot and wake the next waiter.
     */
    release() {
        if (this.disposed)
            return;
        if (this.active > 0)
            this.active -= 1;
        this.pump(this.deps.now());
    }

    /**
     * Refuse queued callers that waited longer than `maxWaitMs` so the agent
     * loop can surface the failure instead of hanging indefinitely.
     * @returns true if any waiter was dropped.
     */
    expireWaiters() {
        const now = this.deps.now();
        let dropped = false;
        for (const waiter of this.queue.toArray()) {
            if (now - waiter.enqueuedAt <= this.maxWaitMs)
                continue;
            if (!this.queue.remove(waiter))
                continue;
            waiter.resolved = true;
            this.detachAbort(waiter);
            this.timeoutCount += 1;
            if (this.config.adaptiveConcurrency)
                this.aimd.onTimeout(now);
            waiter.resolve(refusal('wait-timeout', FAILURE_CODES.TIMEOUT, TIMEOUT_MESSAGE, now - waiter.enqueuedAt));
            dropped = true;
        }
        if (dropped) {
            this.rearmDeadline();
            this.pump(now);
        }
        else {
            this.rearmDeadline();
        }
        return dropped;
    }

    /**
     * The single dispatch loop. Every timer, release, enqueue and settle ends
     * here; it grants only waiters that satisfy every hard gate.
     */
    pump(nowIn) {
        if (this.disposed)
            return;
        const now = nowIn ?? this.deps.now();
        // A window whose end has been reached is closed here, exactly as v1's
        // pump timer did: this is what resets the escalation ladder (when
        // hysteresis is disabled) no matter which timer woke the pump first.
        this.expireCooldown(now);
        let grants = 0;
        for (;;) {
            if (this.active >= this.effectiveConcurrency())
                break;
            if (this.queue.size === 0) {
                this.nextDispatchAt = 0;
                this.clearPumpTimer();
                break;
            }
            const candidate = this.queue.pick(now);
            if (candidate === null)
                break;
            let chosen = candidate;
            const blockedAt = this.eligibleAt(candidate, now);
            if (blockedAt > now) {
                chosen = null;
                for (const classId of this.queue.classesByStridePass()) {
                    const alternative = this.queue.pickInClass(classId, now);
                    if (alternative !== null && this.eligibleAt(alternative, now) <= now) {
                        chosen = alternative;
                        break;
                    }
                }
                if (chosen === null) {
                    if (candidate.reason === null)
                        candidate.reason = this.blockedReason(candidate, now);
                    this.wakeAt(blockedAt, now);
                    break;
                }
            }
            this.grant(chosen, now);
            grants += 1;
            if (this.config.releaseSpreadMs > 0 && this.queue.size > 0) {
                this.spreadGrants += 1;
                this.nextDispatchAt = now
                    + this.config.releaseSpreadMs
                    + this.deps.random() * this.config.releaseSpreadMs;
                this.wakeAt(this.nextDispatchAt, now);
                break;
            }
        }
        if (grants > 1)
            this.bursts += 1;
        this.armDeadline(now);
    }

    /** Earliest time this waiter may be granted. */
    eligibleAt(waiter, now) {
        let at = now;
        if (this.cooldown.until > at)
            at = this.cooldown.until;
        const rpmAt = this.rpm.eligibleAt(now);
        if (rpmAt > at)
            at = rpmAt;
        const tpmAt = this.tpm.eligibleAt(waiter.need, now);
        if (tpmAt > at)
            at = tpmAt;
        if (this.nextDispatchAt > at)
            at = this.nextDispatchAt;
        return at;
    }

    /** Which hard gate currently blocks this waiter (for the histogram). */
    blockedReason(waiter, now) {
        let at = now;
        let reason = null;
        if (this.cooldown.until > at) {
            at = this.cooldown.until;
            reason = 'cooldown';
        }
        const rpmAt = this.rpm.eligibleAt(now);
        if (rpmAt > at) {
            at = rpmAt;
            reason = 'rpm';
        }
        const tpmAt = this.tpm.eligibleAt(waiter.need, now);
        if (tpmAt > at) {
            at = tpmAt;
            reason = 'tpm';
        }
        if (this.nextDispatchAt > at) {
            at = this.nextDispatchAt;
            reason = 'release';
        }
        return reason;
    }

    /** Admit one waiter: charge the budgets, mint the ticket, resolve. */
    grant(waiter, now) {
        this.queue.noteGrant(waiter);
        this.queue.remove(waiter);
        waiter.granted = true;
        this.detachAbort(waiter);
        this.active += 1;
        this.rpm.record(now);
        const ticket = {
            id: ++this.ticketSeq,
            grantedAt: now,
            need: waiter.need,
            tpmEntry: this.tpm.enabled ? this.tpm.reserve(waiter.need, now) : null,
            settled: false,
            origin: 'grant',
        };
        this.tickets.add(ticket);
        const waitMs = Math.max(0, now - waiter.enqueuedAt);
        if (waitMs > 0 && waiter.reason === null)
            waiter.reason = 'concurrency';
        this.recordWait(waitMs, waiter.reason);
        this.waits += 1;
        waiter.resolved = true;
        waiter.resolve({ ok: true, ticket });
    }

    /**
     * Settle one granted ticket exactly once: return the slot, settle the TPM
     * reservation, apply AIMD feedback, and re-enter the pump.
     *
     * @param ticket - the ticket returned by `gate()`.
     * @param outcome - `{ kind, usage?, failure? }`.
     */
    settle(ticket, outcome) {
        const now = this.deps.now();
        if (ticket === undefined || ticket === null)
            return;
        if (ticket.settled) {
            this.pump(now);
            return;
        }
        ticket.settled = true;
        this.tickets.delete(ticket);
        if (this.active > 0)
            this.active -= 1;
        if (ticket.tpmEntry !== null && ticket.tpmEntry !== undefined)
            this.tpm.settle(ticket.tpmEntry, outcome?.usage ?? null, now);
        const kind = outcome?.kind;
        if (kind === 'success') {
            if (this.config.adaptiveConcurrency)
                this.aimd.onSuccess(now);
            this.resetEscalation(now);
        }
        else if (kind === 'error' && isThrottleFailure(outcome?.failure)) {
            if (this.config.adaptiveConcurrency)
                this.aimd.onThrottle(now);
        }
        this.pump(now);
    }

    // ---- cooldown ---------------------------------------------------------

    /**
     * Extend the cooldown window for one observed throttle. Uses the
     * provider's Retry-After hint verbatim when present, otherwise
     * exponential backoff on top of the configured base.
     *
     * @param delayMs - provider retry-after hint in ms, or undefined.
     * @param config - resolved limiter configuration (v1 signature).
     * @param status - HTTP status, for the 503/529 overload multiplier.
     * @param now - optional explicit clock.
     * @returns the total cooldown window in ms that now applies.
     */
    observe429(delayMs, config, status, now) {
        const cfg = config ?? this.config;
        const at = now ?? this.deps.now();
        const window = this.cooldown.observe429(delayMs, cfg, status, at, this.deps.random);
        this.last429At = new Date(at).toISOString();
        if (this.cooldownTimer !== null)
            this.deps.clearTimeout(this.cooldownTimer);
        this.cooldownTimer = this.deps.setTimeout(() => {
            this.cooldownTimer = null;
            this.closeCooldown();
        }, Math.max(1, this.cooldown.until - at));
        this.cooldownTimer.unref?.();
        this.clearHysteresisTimer();
        this.pump(at);
        return this.cooldown.until - at;
    }

    /**
     * Close the open cooldown window: stop blocking, drop the escalation
     * ladder when hysteresis is disabled, and re-run the pump. Idempotent.
     */
    closeCooldown(nowIn) {
        if (this.cooldown.until === 0)
            return;
        const now = nowIn ?? this.deps.now();
        this.expireCooldown(now, true);
        this.pump(now);
    }

    /**
     * Close a cooldown whose end time has been reached. Called from `pump`
     * (whichever timer woke it) so the ladder resets exactly once, and from
     * `closeCooldown` (which may close early).
     *
     * @returns true when a window was actually closed.
     */
    expireCooldown(now, force = false) {
        if (this.cooldown.until === 0)
            return false;
        if (!force && this.cooldown.until > now)
            return false;
        this.cooldown.close(now, this.config);
        if (this.cooldownTimer !== null) {
            this.deps.clearTimeout(this.cooldownTimer);
            this.cooldownTimer = null;
        }
        return true;
    }

    /**
     * A clean success. With `cooldownHysteresisMs: 0` the ladder is zeroed
     * immediately (v1); with hysteresis the ladder survives and a decay timer
     * is (re)armed for `now + cooldownHysteresisMs`.
     */
    resetEscalation(nowIn) {
        const now = nowIn ?? this.deps.now();
        if (this.config.cooldownHysteresisMs === 0) {
            this.cooldown.escalations = 0;
            this.cooldown.hysteresisUntil = 0;
            this.clearHysteresisTimer();
            return;
        }
        this.cooldown.hysteresisUntil = now + this.config.cooldownHysteresisMs;
        if (this.hysteresisTimer !== null)
            this.deps.clearTimeout(this.hysteresisTimer);
        this.hysteresisTimer = this.deps.setTimeout(() => {
            this.hysteresisTimer = null;
            this.cooldown.hysteresisUntil = 0;
            this.cooldown.escalations = 0;
        }, Math.max(1, this.config.cooldownHysteresisMs));
        this.hysteresisTimer.unref?.();
    }

    // ---- timers -----------------------------------------------------------

    /**
     * (Re)arm the liveness anchor: the single ref'd timer of this limiter,
     * armed only while at least one waiter is queued.
     */
    armDeadline(nowIn) {
        if (this.disposed) {
            this.clearDeadline();
            return;
        }
        const now = nowIn ?? this.deps.now();
        const earliest = this.queue.earliestDeadline();
        if (earliest === null) {
            this.clearDeadline();
            return;
        }
        this.clearDeadline();
        this.deadlineTimer = this.deps.setTimeout(() => {
            this.deadlineTimer = null;
            this.expireWaiters();
        }, Math.max(1, earliest - now));
        this.deadlineTimer.unref?.();
        this.deadlineTimer.ref?.();
    }

    rearmDeadline() {
        this.armDeadline();
    }

    clearDeadline() {
        if (this.deadlineTimer !== null) {
            this.deps.clearTimeout(this.deadlineTimer);
            this.deadlineTimer = null;
        }
    }

    clearPumpTimer() {
        if (this.pumpTimer !== null) {
            this.deps.clearTimeout(this.pumpTimer);
            this.pumpTimer = null;
        }
    }

    clearHysteresisTimer() {
        if (this.hysteresisTimer !== null) {
            this.deps.clearTimeout(this.hysteresisTimer);
            this.hysteresisTimer = null;
        }
    }

    /** Wake the pump at `time`; every wake timer is unref'd. */
    wakeAt(time, now) {
        const delay = Math.max(1, time - now);
        this.clearPumpTimer();
        this.pumpTimer = this.deps.setTimeout(() => {
            this.pumpTimer = null;
            this.pump(this.deps.now());
        }, delay);
        this.pumpTimer.unref?.();
    }

    detachAbort(waiter) {
        if (waiter.onAbort !== null && waiter.signal !== undefined && typeof waiter.signal.removeEventListener === 'function') {
            waiter.signal.removeEventListener('abort', waiter.onAbort);
            waiter.onAbort = null;
        }
    }

    recordWait(waitMs, reason) {
        let bucket = WAIT_BUCKETS.findIndex((bound) => waitMs < bound);
        if (bucket === -1)
            bucket = WAIT_BUCKETS.length - 1;
        this.waitHistogram[bucket] += 1;
        if (reason !== null && this.waitsByReason[reason] !== undefined)
            this.waitsByReason[reason] += 1;
    }

    // ---- lifecycle / observability ---------------------------------------

    /**
     * Refuse every waiter, force-settle every outstanding ticket, and clear
     * every timer. After it returns `queue.size === 0` and `active === 0`, and
     * no timer can fire into the next generation.
     */
    dispose() {
        if (this.disposed)
            return;
        this.disposed = true;
        const now = this.deps.now();
        this.clearDeadline();
        this.clearPumpTimer();
        this.clearHysteresisTimer();
        if (this.cooldownTimer !== null) {
            this.deps.clearTimeout(this.cooldownTimer);
            this.cooldownTimer = null;
        }
        for (const waiter of this.queue.clear()) {
            this.detachAbort(waiter);
            if (waiter.resolved)
                continue;
            waiter.resolved = true;
            this.sheds.disposed += 1;
            waiter.resolve(refusal('disposed', FAILURE_CODES.DISPOSED, DISPOSED_MESSAGE, now - waiter.enqueuedAt));
        }
        for (const ticket of [...this.tickets]) {
            if (!ticket.settled) {
                ticket.settled = true;
                this.active -= 1;
            }
        }
        this.tickets.clear();
        this.active = 0;
        this.cooldown.until = 0;
        this.cooldown.hysteresisUntil = 0;
        this.nextDispatchAt = 0;
    }

    /** Snapshot for the settings panel / logs / command report. */
    snapshot() {
        const now = this.deps.now();
        return {
            provider: this.provider,
            active: this.active,
            queued: this.queued,
            queuedPeak: this.queuedPeak,
            waits: this.waits,
            waitTimeouts: this.timeoutCount,
            cooldownRemainingMs: this.cooldownMs,
            last429At: this.last429At,
            effectiveConcurrency: this.effectiveConcurrency(),
            staticMaxConcurrent: this.config.maxConcurrent,
            adaptive: this.aimd.snapshot(),
            rpm: this.rpm.snapshot(now),
            tpm: this.tpm.snapshot(now),
            classes: this.config.fairness ? this.queue.classCounts() : null,
            cooldown: {
                escalations: this.cooldown.escalations,
                hysteresisRemainingMs: Math.max(0, this.cooldown.hysteresisUntil - now),
                multiplier: this.config.overloadCooldownMultiplier,
            },
            sheds: { ...this.sheds },
            dispatch: {
                nextDispatchInMs: Math.max(0, this.nextDispatchAt - now),
                spreadGrants: this.spreadGrants,
                bursts: this.bursts,
            },
            waitsByReason: { ...this.waitsByReason },
            waitHistogram: [...this.waitHistogram],
        };
    }
}

/**
 * Build a limiter map for the configured providers.
 * @param providers - provider names to throttle (empty = every provider).
 * @param config - limiter configuration.
 * @param deps - optional timer/clock injections, forwarded to each limiter.
 * @returns map from provider name to ProviderLimiter; the "*" entry is the
 *   fallback for unlisted providers.
 */
export function createLimiters(providers, config, deps) {
    const map = new Map();
    const list = Array.isArray(providers) ? providers : [];
    for (const name of list)
        map.set(name, new ProviderLimiter(name, config, deps));
    if (!list.includes('*'))
        map.set('*', new ProviderLimiter('*', config, deps));
    return map;
}

/**
 * Pick the limiter that governs one provider route.
 * @param limiters - map built by createLimiters.
 * @param provider - route name from the request.
 */
export function limiterFor(limiters, provider) {
    return limiters.get(provider) ?? limiters.get('*');
}

/**
 * Detect a terminal finish chunk in the harness chunk protocol:
 * `{ type: 'finish', reason: { kind, failure? } }`.
 * @param chunk - one chunk from the provider stream.
 * @returns "success" for a clean finish (no failure), "aborted" or "error"
 *   for the respective failure kinds, or undefined for non-terminal chunks.
 */
function terminalKind(chunk) {
    if (chunk?.type === 'finish' && typeof chunk.reason === 'object' && chunk.reason !== null) {
        if (chunk.reason.failure !== undefined)
            return chunk.reason.kind === 'aborted' ? 'aborted' : 'error';
        return 'success';
    }
    return undefined;
}

/** Build the synthetic terminal chunk for a refused admission. */
function refusalChunk(outcome) {
    const aborted = outcome.reason === 'aborted' || outcome.reason === 'disposed';
    return {
        type: 'finish',
        reason: {
            kind: aborted ? 'aborted' : 'error',
            failure: { code: outcome.code, message: outcome.message },
        },
    };
}

/**
 * Throttle the head of an async generator: admit the request on the first
 * `next()` (which is what actually starts the provider fetch) and settle the
 * slot exactly once on success/error/abort/abandonment. Chunks pass through
 * unchanged.
 *
 * @param source - the default (downstream) chunk stream.
 * @param limiter - the ProviderLimiter for the route.
 * @param signal - request abort signal.
 * @param onSettled - `(kind, details) => void`, fired exactly once with
 *   "success" | "error" | "aborted" | "timeout".
 * @param meta - optional `{ purpose?, sessionId?, estimatedTokens? }`.
 * @returns an async generator yielding the same chunks.
 */
export function gateStream(source, limiter, signal, onSettled, meta) {
    let ticket = null;
    let settled = false;
    let observedUsage = null;
    let shedOutcome = null;
    const settle = (kind, failure) => {
        if (settled)
            return;
        settled = true;
        const details = {
            usage: observedUsage,
            failure: failure ?? null,
            shedCode: shedOutcome !== null ? shedOutcome.code : null,
            waitMs: shedOutcome !== null ? shedOutcome.waitMs : 0,
        };
        if (ticket !== null)
            limiter.settle(ticket, { kind, usage: observedUsage, failure: failure ?? null });
        onSettled(kind, details);
    };
    return (async function* () {
        const outcome = await limiter.gate({
            signal,
            purpose: meta?.purpose,
            sessionId: meta?.sessionId,
            estimatedTokens: meta?.estimatedTokens ?? 0,
        });
        if (!outcome.ok) {
            shedOutcome = outcome;
            const abortedLike = outcome.reason === 'aborted' || outcome.reason === 'disposed';
            settle(abortedLike ? 'aborted' : 'timeout', null);
            yield refusalChunk(outcome);
            return;
        }
        ticket = outcome.ticket;
        // The host's `llm/stream` waterfall is *synchronous*: `next()` returns
        // the inner chunk stream (a generator) directly, and the outermost
        // listener's value is what the host advances with `for await`. A test
        // that models `next` as async instead hands a Promise that resolves to
        // that stream. Normalize both shapes so the consumer below iterates the
        // real stream either way (a plain generator has no `.then` and is left
        // untouched).
        if (typeof source?.then === 'function')
            source = await source;
        let onAbort = null;
        if (signal !== undefined && typeof signal.addEventListener === 'function') {
            onAbort = () => settle('aborted', null);
            signal.addEventListener('abort', onAbort, { once: true });
            if (signal.aborted)
                settle('aborted', null);
        }
        try {
            for await (const chunk of source) {
                if (chunk?.type === 'usage')
                    observedUsage = usageTokens(chunk.usage);
                yield chunk;
                const kind = terminalKind(chunk);
                if (kind !== undefined)
                    settle(kind, chunk.reason?.failure ?? null);
            }
            if (!settled)
                settle('success', null);
        }
        catch (error) {
            settle('error', null);
            // Deliberate v2 delta: the downstream error is re-thrown unchanged.
            throw error;
        }
        finally {
            if (onAbort !== null && typeof signal.removeEventListener === 'function')
                signal.removeEventListener('abort', onAbort);
            // T5-4 resolution: a consumer that abandons the stream before any
            // terminal chunk was observed (and without an abort) is settled as
            // `aborted`, keeping its token reservation charged — the §7.2
            // release-table rule and the safer of the two formerly-conflicting
            // obligations. An abandoned stream must NOT count as a clean
            // success, so it does not feed aimd.onSuccess() (which would grow
            // the bound past an incomplete request and let 429s slip through)
            // or resetEscalation() (which would restart the cooldown ladder
            // from the base instead of keeping it escalated).
            if (!settled)
                settle('aborted', null);
        }
    })();
}
