/**
 * Priority classes, weighted scheduling, per-session round-robin, aging, the
 * v1 FIFO mode, and the bounded retention of per-session service counters that
 * makes the anti-monopoly guarantee hold across a drained key (finding F1).
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
    CLASS_IDS,
    FairQueue,
    FifoQueue,
    RETAINED_SERVED_LIMIT,
    RETAINED_SERVED_MAX,
    WEIGHT_SCALE,
    classify,
    keyFor,
} from '../lib/scheduler.js';
import { ProviderLimiter } from '../lib/limiter.js';

/** Resolve after the microtask/event queue drains (grant bookkeeping). */
const tick = () => new Promise((resolve) => setImmediate(resolve));

/** A deterministic clock + timer harness for limiter-level tests. */
function fakeClock(start = 1_000_000) {
    let now = start;
    const handles = [];
    const deps = {
        now: () => now,
        random: () => 0.5,
        setTimeout(fn, ms) {
            const handle = {
                fn,
                ms,
                at: now + ms,
                unrefd: false,
                refd: false,
                cleared: false,
                fired: false,
                unref() { this.unrefd = true; return this; },
                ref() { this.refd = true; return this; },
            };
            handles.push(handle);
            return handle;
        },
        clearTimeout(handle) {
            if (handle)
                handle.cleared = true;
        },
    };
    return {
        deps,
        handles,
        get now() { return now; },
        advance(ms) { now += ms; },
    };
}

const FAIR_CONFIG = {
    fairness: true,
    interactiveWeight: 6,
    backgroundWeight: 2,
    utilityWeight: 1,
    agingThresholdMs: 0,
};

let seq = 0;

function waiter(classId, key, enqueuedAt) {
    seq += 1;
    return {
        id: seq,
        classId,
        key,
        enqueuedAt,
        deadline: enqueuedAt + 1_000_000,
        reason: null,
        granted: false,
        resolved: false,
    };
}

/** pick → noteGrant → remove, returning the granted class per step. */
function drain(queue, steps) {
    const order = [];
    for (let i = 0; i < steps; i += 1) {
        const chosen = queue.pick(i);
        assert.ok(chosen !== null, 'a waiter must be available');
        order.push(chosen.classId);
        queue.noteGrant(chosen);
        queue.remove(chosen);
    }
    return order;
}

test('classify: purpose mapping and the unknown-purpose default', () => {
    assert.equal(classify(undefined), 'interactive');
    assert.equal(classify(null), 'interactive');
    assert.equal(classify('compaction'), 'background');
    assert.equal(classify('session-title'), 'utility');
    assert.equal(classify('something-new'), 'background', 'an unknown purpose must not preempt interactive');
    assert.deepEqual([...CLASS_IDS], ['interactive', 'background', 'utility']);
});

test('keyFor: sessionId or the anonymous key', () => {
    assert.equal(keyFor('session-1'), 'session-1');
    assert.equal(keyFor(undefined), '<anonymous>');
    assert.equal(keyFor(''), '<anonymous>');
});

test('FifoQueue: v1 FIFO discipline, size, remove and clear', () => {
    const queue = new FifoQueue();
    const a = waiter('interactive', 'a', 0);
    const b = waiter('background', 'a', 1);
    queue.enqueue(a);
    queue.enqueue(b);
    assert.equal(queue.size, 2);
    assert.equal(queue.pick(99), a, 'strict FIFO regardless of class');
    assert.deepEqual(queue.classesByStridePass(), []);
    assert.equal(queue.classCounts(), null);
    assert.equal(queue.remove(a), true);
    assert.equal(queue.remove(a), false);
    assert.equal(queue.size, 1);
    assert.equal(queue.earliestDeadline(), b.deadline);
    assert.deepEqual(queue.clear(), [b]);
    assert.equal(queue.size, 0);
    assert.equal(queue.earliestDeadline(), null);
});

test('FairQueue: strides follow the weights (6:2:1 ⇒ 10/30/60)', () => {
    const queue = new FairQueue(FAIR_CONFIG);
    assert.equal(WEIGHT_SCALE, 60);
    assert.equal(queue.stride.interactive, Math.round(WEIGHT_SCALE / 6));
    assert.equal(queue.stride.background, Math.round(WEIGHT_SCALE / 2));
    assert.equal(queue.stride.utility, Math.round(WEIGHT_SCALE / 1));
    assert.deepEqual(queue.stride, { interactive: 10, background: 30, utility: 60 });
});

test('FairQueue: admission order under contention is interactive, background, utility', () => {
    const queue = new FairQueue(FAIR_CONFIG);
    queue.enqueue(waiter('interactive', 's', 0));
    queue.enqueue(waiter('background', 's', 1));
    queue.enqueue(waiter('utility', 's', 2));
    assert.deepEqual(drain(queue, 3), ['interactive', 'background', 'utility']);
});

test('FairQueue: weighted share over 90 grants is exactly 6:2:1', () => {
    const queue = new FairQueue(FAIR_CONFIG);
    const counts = { interactive: 0, background: 0, utility: 0 };
    for (const classId of CLASS_IDS)
        queue.enqueue(waiter(classId, classId, 0));
    for (let i = 0; i < 90; i += 1) {
        const chosen = queue.pick(i);
        counts[chosen.classId] += 1;
        queue.noteGrant(chosen);
        queue.remove(chosen);
        queue.enqueue(waiter(chosen.classId, chosen.classId, i));
    }
    assert.deepEqual(counts, { interactive: 60, background: 20, utility: 10 });
    assert.equal(queue.size, 3, 'one backlogged waiter per class remains');
});

test('FairQueue: no class is skipped for more than 8 consecutive grants', () => {
    const queue = new FairQueue(FAIR_CONFIG);
    const sequence = [];
    for (const classId of CLASS_IDS)
        queue.enqueue(waiter(classId, classId, 0));
    for (let i = 0; i < 90; i += 1) {
        const chosen = queue.pick(i);
        sequence.push(chosen.classId);
        queue.noteGrant(chosen);
        queue.remove(chosen);
        queue.enqueue(waiter(chosen.classId, chosen.classId, i));
    }
    for (const classId of CLASS_IDS) {
        let run = 0;
        let worst = 0;
        for (const granted of sequence) {
            run = granted === classId ? 0 : run + 1;
            worst = Math.max(worst, run);
        }
        assert.ok(worst <= 8, `${classId} was skipped for ${worst} consecutive grants`);
    }
});

test('FairQueue: per-session round-robin stops one session from taking every slot', () => {
    const queue = new FairQueue(FAIR_CONFIG);
    queue.enqueue(waiter('interactive', 'A', 0));
    queue.enqueue(waiter('interactive', 'A', 1));
    queue.enqueue(waiter('interactive', 'A', 2));
    queue.enqueue(waiter('interactive', 'B', 10));
    const keys = [];
    for (let i = 0; i < 3; i += 1) {
        const chosen = queue.pick(i);
        keys.push(chosen.key);
        queue.noteGrant(chosen);
        queue.remove(chosen);
    }
    assert.deepEqual(keys, ['A', 'B', 'A'], 'the greedy session is interleaved, not monopolising');
});

test('FairQueue: aging override forces the oldest waiter past the stride schedule', () => {
    const aged = new FairQueue({ ...FAIR_CONFIG, agingThresholdMs: 100 });
    aged.enqueue(waiter('utility', 'u', 1_000));
    aged.enqueue(waiter('interactive', 'i', 1_050));
    assert.equal(aged.pick(1_099).classId, 'interactive', 'below the threshold the schedule wins');
    assert.equal(aged.pick(1_100).classId, 'utility', 'at the threshold the oldest waiter is forced');

    const fresh = new FairQueue({ ...FAIR_CONFIG, agingThresholdMs: 0 });
    fresh.enqueue(waiter('utility', 'u', 1_000));
    fresh.enqueue(waiter('interactive', 'i', 1_050));
    assert.equal(fresh.pick(99_999).classId, 'interactive', 'aging disabled ⇒ no override');
});

test('FairQueue: pass overflow guard keeps the counters bounded', () => {
    const queue = new FairQueue(FAIR_CONFIG);
    const only = waiter('interactive', 'a', 0);
    queue.enqueue(only);
    queue.pass.interactive = 1e9 + 10;
    queue.pass.background = 1e9 + 20;
    queue.pass.utility = 1e9 + 30;
    queue.noteGrant(only);
    const smallest = Math.min(queue.pass.interactive, queue.pass.background, queue.pass.utility);
    assert.ok(smallest <= 1e9, `passes were rebased (min ${smallest})`);
});

// NOTE (t7 repair, finding F1): the expectation below was ['A', 'B', 'A'] before
// the repair, which asserted the *bug* — it required session A to get a second
// consecutive grant before backlogged session B, i.e. exactly the anti-monopoly
// violation the verifier's ADV-b2 counterexample reports. A#1 is admitted
// instantly on the idle route, so A's per-key queue drains and (before the fix)
// its `served` counter was discarded; A#2 then tied with B and won on enqueue
// time. With the counter retained across the drain, B is served first, which is
// what this test's own message always claimed.
test('ProviderLimiter (fairness): a greedy session cannot take every slot', async () => {
    let clock = 1_000_000;
    const limiter = new ProviderLimiter('fair', {
        maxConcurrent: 1,
        maxWaitMs: 60_000,
        fairness: true,
    }, {
        now: () => clock,
        random: () => 0.5,
        setTimeout: () => ({ unref() { return this; }, ref() { return this; } }),
        clearTimeout: () => {},
    });
    const granted = [];
    const track = (label) => (outcome) => {
        if (outcome.ok === true)
            granted.push(label);
    };
    const first = await limiter.gate({ sessionId: 'A' });
    assert.equal(first.ok, true, 'the first request takes the single slot');
    const queuedA = limiter.gate({ sessionId: 'A' }).then(track('A'));
    const queuedA2 = limiter.gate({ sessionId: 'A' }).then(track('A'));
    const queuedB = limiter.gate({ sessionId: 'B' }).then(track('B'));
    assert.equal(limiter.queued, 3);
    clock += 1;
    limiter.release();
    clock += 1;
    limiter.release();
    clock += 1;
    limiter.release();
    await Promise.all([queuedA, queuedA2, queuedB]);
    assert.deepEqual(granted, ['B', 'A', 'A'], 'session B is served before session A gets a second slot');
    limiter.dispose();
});

test('FairQueue: a drained session keeps its service counter across re-arrival', () => {
    const queue = new FairQueue(FAIR_CONFIG);
    const only = waiter('interactive', 'A', 0);
    queue.enqueue(only);
    const picked = queue.pick(0);
    assert.equal(picked, only);
    queue.noteGrant(picked);
    queue.remove(picked);
    assert.equal(queue.size, 0, 'the per-key queue drained');
    assert.equal(queue.classes.size, 0, 'no live record is kept for a drained key');
    assert.equal(queue.retainedSessions, 1, 'the drained session state is retained, not discarded');

    queue.enqueue(waiter('interactive', 'A', 1));
    queue.enqueue(waiter('interactive', 'B', 2));
    assert.equal(queue.pick(3).key, 'B', 'a re-arriving session yields to a genuinely fresh one');
});

test('FairQueue: retained service counters are bounded by count and by value', () => {
    const queue = new FairQueue(FAIR_CONFIG);
    const total = RETAINED_SERVED_LIMIT + 500;
    for (let i = 0; i < total; i += 1) {
        const one = waiter('interactive', `s-${i}`, i);
        queue.enqueue(one);
        queue.noteGrant(one);
        queue.remove(one);
    }
    assert.equal(queue.size, 0);
    assert.equal(queue.classes.size, 0, 'no live per-key records are kept');
    assert.equal(queue.retainedSessions, RETAINED_SERVED_LIMIT, 'the retention cap is enforced');
    assert.equal(queue.takeRetainedServed('interactive', `s-${total - 1}`), 1, 'the newest drain is remembered');
    assert.equal(queue.takeRetainedServed('interactive', 's-0'), null, 'the oldest drain was evicted first');

    const heavy = waiter('interactive', 'heavy', 0);
    queue.enqueue(heavy);
    for (let i = 0; i < 40; i += 1)
        queue.noteGrant(heavy);
    queue.remove(heavy);
    assert.equal(
        queue.takeRetainedServed('interactive', 'heavy'),
        RETAINED_SERVED_MAX,
        'the retained counter of a long-lived session is clamped',
    );
});

// The exact sequence reported as finding F1 (ADV-b2): fairness on, one slot,
// idle route, gate A, gate A, gate B, settle A#1 ⇒ A#1, B, A#2.
test('ProviderLimiter (fairness): a drained session that re-arrives does not monopolise (F1)', async () => {
    const clock = fakeClock();
    const limiter = new ProviderLimiter('rearrival', {
        maxConcurrent: 1,
        maxWaitMs: 60_000,
        fairness: true,
        agingThresholdMs: 0,
    }, clock.deps);
    const order = [];
    const tickets = [];
    const track = (label) => (outcome) => {
        order.push(label);
        tickets.push(outcome.ticket);
    };
    limiter.gate({ sessionId: 'A' }).then(track('A#1'));
    limiter.gate({ sessionId: 'A' }).then(track('A#2'));
    limiter.gate({ sessionId: 'B' }).then(track('B'));
    await tick();
    assert.deepEqual(order, ['A#1'], 'only the idle-route admission happened so far');
    assert.equal(limiter.queued, 2);

    limiter.settle(tickets[0], { kind: 'success' });
    await tick();
    assert.deepEqual(order, ['A#1', 'B'], 'session A must not receive two consecutive grants while B is backlogged');

    limiter.settle(tickets[1], { kind: 'success' });
    await tick();
    assert.deepEqual(order, ['A#1', 'B', 'A#2'], 'session A is only served again after B');
    assert.equal(limiter.queued, 0);
    assert.equal(limiter.active, 1);
    limiter.settle(tickets[2], { kind: 'success' });
    assert.equal(limiter.active, 0);
    limiter.dispose();
});
