/**
 * Sliding-window budget accounting.
 *
 * `RequestWindow` implements the exact RPM sliding-window log: the window is
 * a sorted list of admission timestamps, so there is no counter-reset boundary
 * burst, and `rpmLimit` bounds the log's size by construction (at most
 * `rpmLimit` entries are ever inside the window).
 *
 * `TokenWindow` implements the TPM budget as a sliding-window log of
 * `{ at, tokens }` entries with a running sum. A request *reserves* tokens at
 * admission; the stream's `usage` chunk later *settles* the reservation with
 * the observed value. A reservation is never released on failure: without
 * observed usage the full reservation stays charged and expires with the
 * window (conservative by design).
 *
 * Both classes are pure with respect to the clock: every method takes `now`
 * as an explicit parameter so tests are deterministic.
 *
 * @module dsh-llm-rate-limit/window
 */

/** Lazy-prune cursor compaction threshold for `RequestWindow`. */
const PRUNE_COMPACT_THRESHOLD = 4_096;

/**
 * Exact sliding-window request-rate limiter (RPM).
 */
export class RequestWindow {
    /**
     * @param limit - maximum admissions per window; `<= 0` disables the window.
     * @param windowMs - window length in ms.
     */
    constructor(limit, windowMs) {
        this.limit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 0;
        this.windowMs = Number.isFinite(windowMs) && windowMs > 0 ? windowMs : 60_000;
        /** Ascending admission timestamps. */
        this.stamps = [];
        /** Lazy prune cursor into `stamps`. */
        this.head = 0;
    }

    /** True when the budget is enforced. */
    get enabled() {
        return this.limit > 0;
    }

    /** Drop timestamps that have left the window. */
    prune(now) {
        const { stamps } = this;
        while (this.head < stamps.length && now - stamps[this.head] >= this.windowMs)
            this.head += 1;
        if (this.head > PRUNE_COMPACT_THRESHOLD) {
            this.stamps = stamps.slice(this.head);
            this.head = 0;
        }
    }

    /** Admissions currently counted inside the window. */
    used(now) {
        this.prune(now);
        return this.stamps.length - this.head;
    }

    /**
     * Earliest time at which one more admission fits inside the budget.
     * @returns 0 when the window is disabled or has headroom.
     */
    eligibleAt(now) {
        if (!this.enabled)
            return 0;
        this.prune(now);
        const count = this.stamps.length - this.head;
        if (count + 1 <= this.limit)
            return 0;
        const mustExpire = count - this.limit + 1;
        return this.stamps[this.head + mustExpire - 1] + this.windowMs;
    }

    /** Charge one admission at `now`. */
    record(now) {
        if (!this.enabled)
            return;
        this.prune(now);
        this.stamps.push(now);
    }

    /** Observability snapshot. */
    snapshot(now) {
        const nextEligibleAt = this.eligibleAt(now);
        return {
            limit: this.limit,
            windowMs: this.windowMs,
            used: this.used(now),
            nextEligibleInMs: Math.max(0, nextEligibleAt - now),
        };
    }
}

/**
 * Follow `mergedInto` links to the live log entry that currently carries the
 * tokens of `entry`. `compact()` replaces entries with merged copies, so a
 * reservation handed out by `reserve()` (or a copy of it) may no longer be in
 * `entries`; settle must still reach the object that is.
 */
function liveEntry(entry) {
    let current = entry;
    while (current.mergedInto !== null && current.mergedInto !== undefined)
        current = current.mergedInto;
    return current;
}

/**
 * Sliding-window token budget (TPM) with reservation/settlement.
 */
export class TokenWindow {
    /**
     * @param limit - token budget per window; `<= 0` disables the window.
     * @param windowMs - window length in ms.
     * @param maxEntries - compaction cap for the log (at least 16).
     */
    constructor(limit, windowMs, maxEntries) {
        this.limit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 0;
        this.windowMs = Number.isFinite(windowMs) && windowMs > 0 ? windowMs : 60_000;
        this.maxEntries = Number.isFinite(maxEntries) && maxEntries >= 16 ? Math.floor(maxEntries) : 4_096;
        /** Ascending `{ at, tokens, settled }` entries. */
        this.entries = [];
        /** Running sum of `entries[].tokens`. */
        this.sum = 0;
        /** Tokens reserved by requests that have not settled yet. */
        this.reserved = 0;
    }

    /** True when the budget is enforced. */
    get enabled() {
        return this.limit > 0;
    }

    /** Drop entries that have left the window. */
    prune(now) {
        while (this.entries.length > 0 && now - this.entries[0].at >= this.windowMs) {
            const entry = this.entries.shift();
            // Mark it: a late settle() must not move `sum` for a charge that has
            // already left the window.
            entry.pruned = true;
            this.sum -= entry.tokens;
            if (entry.unsettledTokens > 0) {
                this.reserved = Math.max(0, this.reserved - entry.unsettledTokens);
                entry.unsettledTokens = 0;
            }
        }
    }

    /** Tokens currently counted inside the window. */
    used(now) {
        this.prune(now);
        return this.sum;
    }

    /**
     * Earliest time at which `need` more tokens fit inside the budget.
     * @returns 0 when the window is disabled or has headroom.
     */
    eligibleAt(need, now) {
        if (!this.enabled)
            return 0;
        this.prune(now);
        if (this.sum + need <= this.limit)
            return 0;
        let rest = this.sum;
        for (const entry of this.entries) {
            rest -= entry.tokens;
            if (rest + need <= this.limit)
                return entry.at + this.windowMs;
        }
        return this.entries.length > 0 ? this.entries[this.entries.length - 1].at + this.windowMs : now;
    }

    /** Reserve `need` tokens at `now`; returns the entry to settle later. */
    reserve(need, now) {
        if (!this.enabled)
            return null;
        this.prune(now);
        const tokens = Number.isFinite(need) && need > 0 ? need : 0;
        const entry = {
            at: now,
            tokens,
            unsettledTokens: tokens,
            settled: false,
            pruned: false,
            mergedInto: null,
        };
        this.entries.push(entry);
        this.sum += tokens;
        this.reserved += tokens;
        return entry;
    }

    /**
     * Replace a reservation with the observed usage. `null`/non-finite keeps
     * the reservation (a reservation is never released on failure).
     *
     * `entry` may have been merged into a later log entry by `compact()`; the
     * delta is therefore applied to the *live* entry that currently carries the
     * tokens, so `sum` always equals `Σ entries[].tokens`. A settle that arrives
     * after the charge left the window is a no-op for `sum`, because the window
     * no longer contains it.
     */
    settle(entry, actualTokens, now) {
        if (entry === undefined || entry === null)
            return;
        const observed = Number.isFinite(actualTokens) ? Math.max(0, actualTokens) : null;
        const before = entry.tokens;
        const actual = observed === null ? before : observed;
        const delta = actual - before;
        entry.tokens = actual;
        const live = liveEntry(entry);
        if (live.pruned === true) {
            // The charge already expired with the window: update the detached
            // record only, never the running sum.
            entry.unsettledTokens = 0;
            entry.settled = true;
            return;
        }
        if (live !== entry)
            live.tokens += delta;
        this.sum += delta;
        if (entry.unsettledTokens > 0) {
            const released = entry.unsettledTokens;
            entry.unsettledTokens = 0;
            if (live !== entry)
                live.unsettledTokens = Math.max(0, live.unsettledTokens - released);
            this.reserved = Math.max(0, this.reserved - released);
        }
        entry.settled = true;
        if (this.entries.length > this.maxEntries)
            this.compact();
    }

    /**
     * Pairwise-merge adjacent entries until the log is at most half the cap.
     * Tokens are conserved; the merged timestamp is the later one, which only
     * makes the window more conservative. Each source entry records the merged
     * target (`mergedInto`) so a settle of an already-merged reservation still
     * reaches the live entry.
     */
    compact() {
        const target = Math.floor(this.maxEntries / 2);
        while (this.entries.length > target) {
            const merged = [];
            for (let i = 0; i < this.entries.length; i += 2) {
                const a = this.entries[i];
                const b = this.entries[i + 1];
                if (b === undefined) {
                    merged.push(a);
                    break;
                }
                const combined = {
                    at: Math.max(a.at, b.at),
                    tokens: a.tokens + b.tokens,
                    unsettledTokens: a.unsettledTokens + b.unsettledTokens,
                    settled: a.settled && b.settled,
                    pruned: false,
                    mergedInto: null,
                };
                a.mergedInto = combined;
                b.mergedInto = combined;
                merged.push(combined);
            }
            this.entries = merged;
        }
    }

    /** Observability snapshot. */
    snapshot(now) {
        // `nextEligibleInMs` answers "when could a real (≥1 token) request be
        // admitted again?", so a completely full window reports a delay even
        // though a hypothetical 0-token request would fit immediately.
        const nextEligibleAt = this.eligibleAt(1, now);
        return {
            limit: this.limit,
            windowMs: this.windowMs,
            used: this.used(now),
            reserved: this.reserved,
            nextEligibleInMs: Math.max(0, nextEligibleAt - now),
        };
    }
}
