/**
 * Priority classes and fair queueing.
 *
 * Classes are derived from the only request-side signals DSH exposes:
 * `GenerateOptions.purpose` and `GenerateOptions.sessionId`. There is no
 * strict priority: a deficit-round-robin style weighted scheduler serves every
 * backlogged class in proportion to its weight, so no class can be starved,
 * and per-session round-robin inside a class stops one session's fan-out from
 * taking every slot. An optional age-based override bounds the worst-case
 * latency of a low-weight class.
 *
 * `FifoQueue` is the v1-compatible discipline used when `fairness: false`.
 *
 * @module dsh-llm-rate-limit/scheduler
 */

/** Fixed class order — also the deterministic tie-break. */
export const CLASS_IDS = Object.freeze(['interactive', 'background', 'utility']);

/** Weight scale: `stride = round(WEIGHT_SCALE / weight)`. */
export const WEIGHT_SCALE = 60;

/** The anonymous session key used when a request carries no `sessionId`. */
export const ANONYMOUS_KEY = '<anonymous>';

/**
 * Derive the scheduling class from `GenerateOptions.purpose`.
 * An unknown future purpose must not preempt interactive work.
 */
export function classify(purpose) {
    if (purpose === undefined || purpose === null)
        return 'interactive';
    if (purpose === 'compaction')
        return 'background';
    if (purpose === 'session-title')
        return 'utility';
    return 'background';
}

/** The per-session fairness key. */
export function keyFor(sessionId) {
    return typeof sessionId === 'string' && sessionId !== '' ? sessionId : ANONYMOUS_KEY;
}

function positive(value, fallback) {
    return Number.isFinite(value) && value > 0 ? value : fallback;
}

/**
 * Maximum number of per-session service counters a `FairQueue` retains after
 * their queues drain. When the cap is reached the oldest drained session is
 * forgotten first, so a session that re-arrives promptly always keeps its
 * counter while stale sessions are dropped. This is the memory bound of the
 * retained fairness state: the map can never exceed this many entries.
 */
export const RETAINED_SERVED_LIMIT = 1_024;

/**
 * Upper bound stored for a retained service counter. `served` is a relative
 * round-robin counter; clamping it when a session drains keeps a long-lived
 * session from being pushed arbitrarily far behind newly arrived ones (the
 * difference between any two sessions is bounded by this value), while any
 * positive retained value still makes a drained-then-re-arrived session yield
 * to a genuinely fresh one.
 */
export const RETAINED_SERVED_MAX = 16;

/** v1 queue discipline: a single FIFO array, no class machinery. */
export class FifoQueue {
    constructor() {
        this.items = [];
    }

    get size() {
        return this.items.length;
    }

    enqueue(waiter) {
        this.items.push(waiter);
    }

    remove(waiter) {
        const index = this.items.indexOf(waiter);
        if (index === -1)
            return false;
        this.items.splice(index, 1);
        return true;
    }

    /** The head waiter, or null. */
    pick() {
        return this.items.length > 0 ? this.items[0] : null;
    }

    /** No class fan-out in FIFO mode. */
    pickInClass() {
        return null;
    }

    /** No classes in FIFO mode. */
    classesByStridePass() {
        return [];
    }

    /** Bookkeeping hook (a no-op for FIFO). */
    noteGrant() {
    }

    /** Earliest waiter deadline, or null when empty. */
    earliestDeadline() {
        let earliest = null;
        for (const waiter of this.items) {
            if (earliest === null || waiter.deadline < earliest)
                earliest = waiter.deadline;
        }
        return earliest;
    }

    toArray() {
        return this.items.slice();
    }

    clear() {
        const removed = this.items;
        this.items = [];
        return removed;
    }

    classCounts() {
        return null;
    }
}

/**
 * Work-conserving weighted queue: class strides + per-session round-robin +
 * optional aging override.
 */
export class FairQueue {
    /**
     * @param config - normalized limiter options.
     */
    constructor(config) {
        this.weights = {
            interactive: positive(config.interactiveWeight, 6),
            background: positive(config.backgroundWeight, 2),
            utility: positive(config.utilityWeight, 1),
        };
        this.stride = {
            interactive: Math.max(1, Math.round(WEIGHT_SCALE / this.weights.interactive)),
            background: Math.max(1, Math.round(WEIGHT_SCALE / this.weights.background)),
            utility: Math.max(1, Math.round(WEIGHT_SCALE / this.weights.utility)),
        };
        /** Virtual pass counters; the smallest pass is served next. */
        this.pass = { interactive: 0, background: 0, utility: 0 };
        /** classId -> Map<key, { list: Waiter[], served: number }> */
        this.classes = new Map();
        /**
         * Bounded memory of drained sessions' service counters, keyed by
         * `classId\0key`. A key whose queue empties is *not* treated as freshly
         * served: its counter moves here and is restored on re-arrival, which is
         * what makes the §5.5 anti-monopoly guarantee hold across re-arrival.
         * Map insertion order doubles as the eviction order (oldest first).
         */
        this.retainedServed = new Map();
        this.total = 0;
        this.agingThresholdMs = Number.isFinite(config.agingThresholdMs) && config.agingThresholdMs > 0
            ? config.agingThresholdMs
            : 0;
    }

    get size() {
        return this.total;
    }

    /** Number of drained sessions whose service counter is retained. */
    get retainedSessions() {
        return this.retainedServed.size;
    }

    /** Take (and forget in the live map) the retained counter for a key. */
    takeRetainedServed(classId, key) {
        const retentionKey = `${classId}\u0000${key}`;
        if (!this.retainedServed.has(retentionKey))
            return null;
        const served = this.retainedServed.get(retentionKey);
        this.retainedServed.delete(retentionKey);
        return served;
    }

    /**
     * Remember a drained key's service counter so its next arrival continues
     * the round-robin instead of restarting it. Oldest entries are evicted
     * first once the map exceeds `RETAINED_SERVED_LIMIT`.
     */
    retainServed(classId, key, served) {
        const retentionKey = `${classId}\u0000${key}`;
        this.retainedServed.delete(retentionKey);
        this.retainedServed.set(retentionKey, Math.min(Math.max(served, 0), RETAINED_SERVED_MAX));
        while (this.retainedServed.size > RETAINED_SERVED_LIMIT)
            this.retainedServed.delete(this.retainedServed.keys().next().value);
    }

    enqueue(waiter) {
        const classId = waiter.classId;
        let byKey = this.classes.get(classId);
        if (byKey === undefined) {
            byKey = new Map();
            this.classes.set(classId, byKey);
        }
        let record = byKey.get(waiter.key);
        if (record === undefined) {
            record = { list: [], served: 0 };
            const retained = this.takeRetainedServed(classId, waiter.key);
            if (retained !== null)
                record.served = retained;
            byKey.set(waiter.key, record);
        }
        record.list.push(waiter);
        this.total += 1;
    }

    remove(waiter) {
        const byKey = this.classes.get(waiter.classId);
        if (byKey === undefined)
            return false;
        const record = byKey.get(waiter.key);
        if (record === undefined)
            return false;
        const index = record.list.indexOf(waiter);
        if (index === -1)
            return false;
        record.list.splice(index, 1);
        if (record.list.length === 0) {
            // Retain the counter instead of discarding it with the record.
            this.retainServed(waiter.classId, waiter.key, record.served);
            byKey.delete(waiter.key);
        }
        if (byKey.size === 0)
            this.classes.delete(waiter.classId);
        this.total -= 1;
        return true;
    }

    /**
     * Pick the next waiter: aging override, then smallest class stride pass,
     * then per-session round-robin inside that class. Pure — grant bookkeeping
     * happens in `noteGrant`.
     */
    pick(now) {
        const aged = this.oldestWaiter();
        if (this.agingThresholdMs > 0 && aged !== null && now - aged.enqueuedAt >= this.agingThresholdMs)
            return aged;
        const classId = this.minPassClass();
        if (classId === null)
            return null;
        return this.pickInClass(classId, now);
    }

    /** Per-session round-robin head of one class. */
    pickInClass(classId) {
        const byKey = this.classes.get(classId);
        if (byKey === undefined || byKey.size === 0)
            return null;
        let best = null;
        let bestServed = Infinity;
        let bestEnqueuedAt = Infinity;
        for (const record of byKey.values()) {
            const head = record.list[0];
            if (head === undefined)
                continue;
            if (record.served < bestServed
                || (record.served === bestServed && head.enqueuedAt < bestEnqueuedAt)) {
                best = head;
                bestServed = record.served;
                bestEnqueuedAt = head.enqueuedAt;
            }
        }
        return best;
    }

    /** Non-empty classes ordered by ascending stride pass (tie: CLASS_IDS). */
    classesByStridePass() {
        const ids = CLASS_IDS.filter((classId) => (this.classes.get(classId)?.size ?? 0) > 0);
        return ids.sort((a, b) => (this.pass[a] - this.pass[b]) || (CLASS_IDS.indexOf(a) - CLASS_IDS.indexOf(b)));
    }

    /** The class that would be served next, or null. */
    minPassClass() {
        const classes = this.classesByStridePass();
        return classes.length > 0 ? classes[0] : null;
    }

    /** Grant bookkeeping: advance the class pass and the session counter. */
    noteGrant(waiter) {
        const classId = waiter.classId;
        const record = this.classes.get(classId)?.get(waiter.key);
        if (record !== undefined)
            record.served += 1;
        this.pass[classId] += this.stride[classId];
        const smallest = Math.min(this.pass.interactive, this.pass.background, this.pass.utility);
        if (smallest > 1e9) {
            for (const id of CLASS_IDS)
                this.pass[id] -= smallest;
        }
    }

    /** The globally oldest queued waiter (head of each key FIFO). */
    oldestWaiter() {
        let oldest = null;
        for (const byKey of this.classes.values()) {
            for (const record of byKey.values()) {
                const head = record.list[0];
                if (head !== undefined && (oldest === null || head.enqueuedAt < oldest.enqueuedAt))
                    oldest = head;
            }
        }
        return oldest;
    }

    /** Earliest waiter deadline, or null when empty. */
    earliestDeadline() {
        let earliest = null;
        for (const byKey of this.classes.values()) {
            for (const record of byKey.values()) {
                for (const waiter of record.list) {
                    if (earliest === null || waiter.deadline < earliest)
                        earliest = waiter.deadline;
                }
            }
        }
        return earliest;
    }

    toArray() {
        const waiters = [];
        for (const classId of CLASS_IDS) {
            const byKey = this.classes.get(classId);
            if (byKey === undefined)
                continue;
            for (const record of byKey.values())
                waiters.push(...record.list);
        }
        return waiters;
    }

    clear() {
        const removed = this.toArray();
        this.classes = new Map();
        this.retainedServed = new Map();
        this.total = 0;
        this.pass = { interactive: 0, background: 0, utility: 0 };
        return removed;
    }

    /** Per-class queue/keys counters for the snapshot (`fairness: true`). */
    classCounts() {
        const counts = {};
        for (const classId of CLASS_IDS) {
            const byKey = this.classes.get(classId);
            let queued = 0;
            if (byKey !== undefined) {
                for (const record of byKey.values())
                    queued += record.list.length;
            }
            counts[classId] = { queued, keys: byKey?.size ?? 0 };
        }
        return counts;
    }
}
