/**
 * Independent adversarial verification of dsh-llm-rate-limit v2 (task t4).
 *
 * Written by the verifier, NOT by the implementer. Every test below drives an
 * injected clock (ProviderLimiter's `deps` argument) and never sleeps for a
 * real second, so the file is deterministic and fast. Each test tries to
 * falsify one property the implementation claims; a failing assertion is a
 * finding, not a test bug.
 *
 * Scope discipline: this file only imports from lib/ and never writes to it.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { apply } from '../../lib/index.js';
import { ProviderLimiter, createLimiters, gateStream, limiterFor } from '../../lib/limiter.js';
import { Aimd } from '../../lib/aimd.js';
import { RequestWindow, TokenWindow } from '../../lib/window.js';
import { Cooldown, jitteredWindow } from '../../lib/cooldown.js';
import { FairQueue } from '../../lib/scheduler.js';

/** Resolve after the microtask queue drains (grant/release bookkeeping). */
const tick = () => new Promise((resolve) => setImmediate(resolve));

/**
 * A deterministic clock + timer harness.
 *
 * `advance(ms)` moves time forward and fires every timer that becomes due, in
 * chronological order, letting callbacks schedule further timers. This mirrors
 * what a real event loop would do, unlike a harness that only flips a number.
 */
function makeClock(start = 1_700_000_000_000) {
    let now = start;
    let seq = 0;
    const timers = new Map();
    const created = [];
    const deps = {
        now: () => now,
        random: () => 0.5,
        setTimeout(fn, ms) {
            const handle = {
                id: ++seq,
                at: now + (Number.isFinite(ms) && ms > 0 ? ms : 0),
                fn,
                cleared: false,
                fired: false,
                refd: false,
                unrefd: false,
                unref() { this.unrefd = true; return this; },
                ref() { this.refd = true; return this; },
            };
            timers.set(handle.id, handle);
            created.push(handle);
            return handle;
        },
        clearTimeout(handle) {
            if (handle === null || handle === undefined)
                return;
            const found = timers.get(handle.id);
            if (found !== undefined) {
                found.cleared = true;
                timers.delete(handle.id);
            }
            else {
                handle.cleared = true;
            }
        },
    };
    function nextDue(target) {
        let best = null;
        for (const handle of timers.values()) {
            if (handle.at > target)
                continue;
            if (best === null || handle.at < best.at || (handle.at === best.at && handle.id < best.id))
                best = handle;
        }
        return best;
    }
    return {
        deps,
        created,
        get now() { return now; },
        advance(ms) {
            const target = now + ms;
            let fired = 0;
            for (;;) {
                const handle = nextDue(target);
                if (handle === null)
                    break;
                now = Math.max(now, handle.at);
                timers.delete(handle.id);
                handle.fired = true;
                handle.fn();
                fired += 1;
                if (fired > 200_000)
                    throw new Error('fake clock runaway');
            }
            now = target;
            return fired;
        },
        pending() { return [...timers.values()]; },
        refdPending() { return [...timers.values()].filter((handle) => handle.refd); },
    };
}

/** A minimal cordis-style ctx for exercising lib/index.js. */
function makeCtx(services = {}) {
    const hooks = {};
    const effects = [];
    const ctx = {
        on(event, listener) {
            (hooks[event] ??= []).push(listener);
            return () => {
                const index = hooks[event].indexOf(listener);
                if (index !== -1)
                    hooks[event].splice(index, 1);
            };
        },
        effect(fn, label) {
            const cleanup = fn();
            effects.push({ label, cleanup });
            return () => {};
        },
        get(service) { return services[service]; },
        logger: () => ({ info() {}, warn() {} }),
        dispose() {
            for (const { cleanup } of effects)
                if (typeof cleanup === 'function')
                    cleanup();
        },
    };
    return { ctx, hooks, effects, services };
}

/** Wrap an object graph in a Proxy that records every mutation attempt. */
function mutationRecorder(root) {
    const attempts = [];
    const seen = new WeakMap();
    const wrap = (value, path) => {
        if (value === null || typeof value !== 'object')
            return value;
        if (seen.has(value))
            return seen.get(value);
        const proxy = new Proxy(value, {
            get(target, prop, receiver) {
                return wrap(Reflect.get(target, prop, receiver), `${path}.${String(prop)}`);
            },
            set(target, prop) {
                attempts.push({ kind: 'set', path: `${path}.${String(prop)}` });
                return true;
            },
            defineProperty(target, prop) {
                attempts.push({ kind: 'defineProperty', path: `${path}.${String(prop)}` });
                return true;
            },
            deleteProperty(target, prop) {
                attempts.push({ kind: 'deleteProperty', path: `${path}.${String(prop)}` });
                return true;
            },
            setPrototypeOf(target) {
                attempts.push({ kind: 'setPrototypeOf', path });
                return true;
            },
        });
        seen.set(value, proxy);
        return proxy;
    };
    return { proxy: wrap(root, '$'), attempts };
}

/** Recursively freeze an object graph (strict-mode writes then throw). */
function deepFreeze(value) {
    if (value === null || typeof value !== 'object')
        return value;
    for (const key of Object.keys(value))
        deepFreeze(value[key]);
    return Object.freeze(value);
}

/** maxWaitMs used by BASE = 60_000; one ms past it expires every waiter. */
const EXPIRE_MS = 60_001;

const BASE = {
    maxConcurrent: 4,
    maxWaitMs: 60_000,
    cooldownBaseMs: 1_000,
    cooldownMaxMs: 8_000,
    rpmLimit: 0,
    rpmWindowMs: 60_000,
    tpmLimit: 0,
    tpmWindowMs: 60_000,
    adaptiveConcurrency: false,
    fairness: false,
    releaseSpreadMs: 0,
    cooldownJitter: 'none',
    cooldownHysteresisMs: 0,
    overloadCooldownMultiplier: 1,
};

// ---------------------------------------------------------------------------
// (a) sliding-window accounting
// ---------------------------------------------------------------------------

test('ADV-a: sliding-window RPM accounting is exact under a burst and after simulated clock advancement', async () => {
    const clock = makeClock();
    const t0 = clock.now;
    const limiter = new ProviderLimiter('rpm', { ...BASE, maxConcurrent: 10, rpmLimit: 3, rpmWindowMs: 1_000 }, clock.deps);

    const stamps = [];
    for (let i = 0; i < 3; i += 1) {
        const outcome = await limiter.gate({});
        assert.equal(outcome.ok, true, `burst request ${i} is admitted`);
        stamps.push(outcome.ticket.grantedAt);
    }
    assert.deepEqual(stamps, [t0, t0, t0], 'a burst of limit-many requests is admitted in one tick');
    assert.equal(limiter.rpm.used(clock.now), 3);

    const fourth = limiter.gate({});
    await tick();
    assert.equal(limiter.queued, 1, 'the (limit+1)-th request queues instead of being refused');
    assert.equal(limiter.active, 3);

    clock.advance(999);
    await tick();
    assert.equal(limiter.queued, 1, 'still queued one millisecond before the first stamp expires');
    assert.equal(limiter.active, 3);

    clock.advance(1);
    const o4 = await fourth;
    await tick();
    assert.equal(o4.ok, true, 'granted the moment the oldest stamp leaves the window');
    assert.equal(o4.ticket.grantedAt, t0 + 1_000);
    assert.equal(limiter.queued, 0);
    limiter.settle(o4.ticket, { kind: 'success' });

    // Exact sliding window: a fixed-window counter would admit two at t0+1000.
    const clock2 = makeClock();
    const t1 = clock2.now;
    const exact = new ProviderLimiter('rpm-exact', { ...BASE, maxConcurrent: 10, rpmLimit: 2, rpmWindowMs: 1_000 }, clock2.deps);
    await exact.gate({});
    clock2.advance(500);
    await exact.gate({});
    clock2.advance(500);
    assert.equal((await exact.gate({})).ok, true, 'one stamp expired at t0+1000');
    const blocked = exact.gate({});
    await tick();
    assert.equal(exact.queued, 1, 'the second admission at t0+1000 must wait for t0+500 to expire');
    clock2.advance(499);
    await tick();
    assert.equal(exact.queued, 1, 'not eligible at t0+1499');
    clock2.advance(1);
    const ob = await blocked;
    assert.equal(ob.ok, true);
    assert.equal(ob.ticket.grantedAt, t1 + 1_500, 'no boundary burst: eligibility is stamp+windowMs');
});

test('ADV-a2: the RPM sliding-window log stays bounded while used() stays exact', () => {
    const window = new RequestWindow(2, 1_000);
    for (let i = 0; i < 20_000; i += 1)
        window.record(i * 1_000);
    assert.ok(window.stamps.length <= 4_096 + 2, `log is compacted, length=${window.stamps.length}`);
    assert.equal(window.used(19_999_001), 1, 'the newest stamp is still counted');
});

// ---------------------------------------------------------------------------
// (b) per-session fairness
// ---------------------------------------------------------------------------

test('ADV-b: per-session fairness: a flooding session does not starve a single-request session', async () => {
    const clock = makeClock();
    const fairCfg = {
        ...BASE,
        maxConcurrent: 1,
        fairness: true,
        agingThresholdMs: 0,
        interactiveWeight: 6,
        backgroundWeight: 2,
        utilityWeight: 1,
        queueLimit: 1_000,
    };

    // Setup 1: the route is already busy when the flood arrives, so the flood
    // session is continuously backlogged. Round-robin must serve the single
    // request second (one grant for the flooder, then the other session).
    const busy = new ProviderLimiter('fair-busy', fairCfg, clock.deps);
    const hold = (await busy.gate({ sessionId: 'hold' })).ticket;
    const busyOrder = [];
    const busyTickets = [];
    for (let i = 0; i < 20; i += 1)
        busy.gate({ sessionId: 'flood' }).then((outcome) => { busyOrder.push('flood'); busyTickets.push(outcome.ticket); });
    busy.gate({ sessionId: 'single' }).then((outcome) => { busyOrder.push('single'); busyTickets.push(outcome.ticket); });
    await tick();
    assert.equal(busy.queued, 21, 'the whole burst is queued behind the held slot');
    busy.settle(hold, { kind: 'success' });
    await tick();
    assert.equal(busyOrder[0], 'flood', 'the earliest-queued session wins the tie for the first grant');
    assert.equal(busy.queue.classes.get('interactive').get('flood').served, 1, 'the served counter advanced');
    busy.settle(busyTickets[0], { kind: 'success' });
    await tick();
    assert.equal(busyOrder[1], 'single', `the single-request session is served next (observed: ${busyOrder.join(',')})`);

    // Setup 2: the route is idle, so the flood session's first request is
    // admitted instantly. The single request must still not be starved behind
    // the 19 remaining flood requests.
    const idle = new ProviderLimiter('fair-idle', fairCfg, clock.deps);
    const idleOrder = [];
    const idleTickets = [];
    for (let i = 0; i < 20; i += 1)
        idle.gate({ sessionId: 'flood' }).then((outcome) => { idleOrder.push('flood'); idleTickets.push(outcome.ticket); });
    idle.gate({ sessionId: 'single' }).then((outcome) => { idleOrder.push('single'); idleTickets.push(outcome.ticket); });
    await tick();
    assert.deepEqual(idleOrder, ['flood'], 'the first flood request is admitted instantly');
    for (let i = 1; i <= 20; i += 1) {
        idle.settle(idleTickets[i - 1], { kind: 'success' });
        await tick();
    }
    idle.settle(idleTickets[20], { kind: 'success' });
    assert.equal(idleOrder.length, 21);
    assert.equal(idleOrder.filter((entry) => entry === 'single').length, 1);
    assert.ok(idleOrder.indexOf('single') <= 2, `the single request is served within the first three grants (index ${idleOrder.indexOf('single')})`);
    assert.equal(idle.queued, 0);
    assert.equal(idle.active, 0);

    // Control: strict FIFO serves all 20 flood requests before the single one.
    const fifo = new ProviderLimiter('fifo', { ...fairCfg, fairness: false }, clock.deps);
    const fifoOrder = [];
    const fifoTickets = [];
    for (let i = 0; i < 20; i += 1)
        fifo.gate({ sessionId: 'flood' }).then((outcome) => { fifoOrder.push('flood'); fifoTickets.push(outcome.ticket); });
    fifo.gate({ sessionId: 'single' }).then((outcome) => { fifoOrder.push('single'); fifoTickets.push(outcome.ticket); });
    await tick();
    fifo.settle(fifoTickets[0], { kind: 'success' });
    await tick();
    assert.equal(fifoOrder[1], 'flood', 'control: a FIFO queue serves the flooder second');
});

test('ADV-b2: FALSIFICATION: the §5.5 anti-monopoly guarantee does not hold across an emptied key list', async () => {
    // Minimal counterexample: session A has two requests, session B has one.
    // A#1 is admitted instantly (route idle), A#2 is enqueued, then B.
    // §5.5 promises "one session's fan-out can occupy at most one grant per
    // scheduler step"; with the counter retained, B must be granted before A#2.
    // Observed: A#2 is granted first, i.e. A occupies two consecutive grants.
    const clock = makeClock();
    const limiter = new ProviderLimiter('anti-monopoly', {
        ...BASE,
        maxConcurrent: 1,
        fairness: true,
        agingThresholdMs: 0,
        queueLimit: 10,
    }, clock.deps);
    const order = [];
    const tickets = [];
    limiter.gate({ sessionId: 'A' }).then((outcome) => { order.push('A#1'); tickets.push(outcome.ticket); });
    limiter.gate({ sessionId: 'A' }).then((outcome) => { order.push('A#2'); tickets.push(outcome.ticket); });
    limiter.gate({ sessionId: 'B' }).then((outcome) => { order.push('B'); tickets.push(outcome.ticket); });
    await tick();
    assert.deepEqual(order, ['A#1'], 'only the instant admission happened so far');
    limiter.settle(tickets[0], { kind: 'success' });
    await tick();
    assert.deepEqual(
        order.slice(0, 2),
        ['A#1', 'B'],
        `session A must not receive two consecutive grants while B is backlogged (observed: ${order.join(',')})`,
    );
});

// ---------------------------------------------------------------------------
// (c) AIMD
// ---------------------------------------------------------------------------

test('ADV-c: AIMD converges and stays within [minConcurrent, maxConcurrent] across a 429/success pattern', () => {
    const base = { minConcurrent: 2, maxConcurrent: 9, aimdInitialConcurrent: 5, aimdIncreaseEvery: 3, aimdDecreaseFactor: 0.5, aimdTimeoutDecrease: true };
    const aimd = new Aimd(base);

    for (let i = 0; i < 50; i += 1) {
        aimd.onThrottle(i * 10);
        assert.ok(Number.isInteger(aimd.current), 'current stays an integer');
        assert.ok(aimd.current >= 2 && aimd.current <= 9, `throttle kept current in bounds: ${aimd.current}`);
    }
    assert.equal(aimd.current, 2, 'sustained throttles converge to the floor');

    for (let i = 0; i < 100; i += 1) {
        aimd.onSuccess(i * 10);
        assert.ok(aimd.current >= 2 && aimd.current <= 9, `success kept current in bounds: ${aimd.current}`);
    }
    assert.equal(aimd.current, 9, 'sustained successes converge to the ceiling');

    const alternating = new Aimd(base);
    for (let i = 0; i < 400; i += 1) {
        if (i % 2 === 0)
            alternating.onSuccess(i);
        else
            alternating.onThrottle(i);
        assert.ok(alternating.current >= 2 && alternating.current <= 9, `alternating pattern in bounds: ${alternating.current}`);
    }
    assert.equal(alternating.current, 2, 'success/throttle alternation converges to the floor');

    // Pathological: an enormous decrease factor still cannot leave the bounds.
    const wild = new Aimd({ ...base, aimdDecreaseFactor: 0.0001, aimdIncreaseEvery: 1, minConcurrent: 1, maxConcurrent: 2, aimdInitialConcurrent: 2 });
    for (let i = 0; i < 200; i += 1) {
        wild.onThrottle(i);
        assert.ok(wild.current >= 1 && wild.current <= 2, `wild factor in bounds: ${wild.current}`);
        wild.onSuccess(i);
        assert.ok(wild.current >= 1 && wild.current <= 2, `wild factor in bounds: ${wild.current}`);
    }
    const noTimeoutLoss = new Aimd({ ...base, aimdTimeoutDecrease: false });
    for (let i = 0; i < 20; i += 1)
        noTimeoutLoss.onTimeout(i);
    assert.equal(noTimeoutLoss.current, 5, 'a timeout is not a loss when aimdTimeoutDecrease is false');
});

test('ADV-c2: wired AIMD feedback from settle() stays inside its configured bounds', async () => {
    const clock = makeClock();
    const limiter = new ProviderLimiter('aimd-wired', {
        ...BASE,
        maxConcurrent: 4,
        minConcurrent: 1,
        aimdInitialConcurrent: 2,
        aimdIncreaseEvery: 2,
        aimdDecreaseFactor: 0.5,
        aimdTimeoutDecrease: true,
        adaptiveConcurrency: true,
        queueLimit: 1_000,
    }, clock.deps);

    assert.equal(limiter.effectiveConcurrency(), 2, 'initial AIMD bound');
    limiter.settle((await limiter.gate({})).ticket, { kind: 'success' });
    limiter.settle((await limiter.gate({})).ticket, { kind: 'success' });
    assert.equal(limiter.effectiveConcurrency(), 3, 'two clean settles raise the bound by one');
    limiter.settle((await limiter.gate({})).ticket, { kind: 'error', failure: { status: 429 } });
    assert.equal(limiter.effectiveConcurrency(), 1, 'a 429 halves it');

    for (let i = 0; i < 30; i += 1) {
        limiter.settle({ settled: false, tpmEntry: null }, { kind: 'error', failure: { status: 503 } });
        assert.ok(limiter.effectiveConcurrency() >= 1, 'never below the floor');
    }
    for (let i = 0; i < 200; i += 1) {
        limiter.settle({ settled: false, tpmEntry: null }, { kind: 'success' });
        assert.ok(limiter.effectiveConcurrency() <= 4, 'never above the ceiling');
    }
    assert.equal(limiter.effectiveConcurrency(), 4, 'converges to the ceiling');
    assert.ok(limiter.active <= limiter.effectiveConcurrency(), 'active never exceeds the effective bound');
    assert.equal(limiter.aimd.snapshot().current, limiter.effectiveConcurrency());
});

// ---------------------------------------------------------------------------
// (d) cooldown release staggering
// ---------------------------------------------------------------------------

test('ADV-d: cooldown release is staggered: waiters are not all released in the same millisecond', async () => {
    const clock = makeClock();
    const t0 = clock.now;
    const cfg = { ...BASE, maxConcurrent: 10, releaseSpreadMs: 100, queueLimit: 1_000 };
    const limiter = new ProviderLimiter('spread', cfg, clock.deps);

    // A throttle observed while the queue is empty must open a window cleanly.
    const windowMs = limiter.observe429(undefined, cfg);
    assert.equal(windowMs, 1_000, 'cooldown base window');
    assert.equal(limiter.queued, 0);

    const grantedAt = [];
    for (let i = 0; i < 5; i += 1)
        limiter.gate({}).then((outcome) => grantedAt.push(outcome.ticket.grantedAt));
    await tick();
    assert.equal(limiter.queued, 5);
    assert.equal(grantedAt.length, 0, 'nothing is granted while the cooldown is open');

    clock.advance(999);
    await tick();
    assert.equal(grantedAt.length, 0, 'nothing is granted before the cooldown ends');

    clock.advance(1);
    await tick();
    assert.equal(grantedAt.length, 1, 'exactly one waiter is released when the cooldown closes');

    clock.advance(3_000);
    await tick();
    assert.equal(grantedAt.length, 5, 'the drip drains the whole queue');

    const sorted = [...grantedAt].sort((a, b) => a - b);
    assert.deepEqual(sorted, [t0 + 1_000, t0 + 1_150, t0 + 1_300, t0 + 1_450, t0 + 1_600]);
    assert.equal(new Set(sorted).size, 5, 'no two waiters share a millisecond');
    for (let i = 1; i < sorted.length; i += 1) {
        const gap = sorted[i] - sorted[i - 1];
        assert.ok(gap >= 100 && gap <= 200, `drip gap within [spread, 2*spread], observed ${gap}`);
    }
    assert.equal(limiter.spreadGrants, 4, 'four spread grants were recorded');

    // Control: releaseSpreadMs 0 fans the same burst out in one tick.
    const clock2 = makeClock();
    const burst = new ProviderLimiter('burst', { ...cfg, releaseSpreadMs: 0 }, clock2.deps);
    burst.observe429(undefined, { ...cfg, releaseSpreadMs: 0 });
    const burstAt = [];
    for (let i = 0; i < 5; i += 1)
        burst.gate({}).then((outcome) => burstAt.push(outcome.ticket.grantedAt));
    clock2.advance(1_000);
    await tick();
    assert.equal(burstAt.length, 5);
    assert.equal(new Set(burstAt).size, 1, 'control: without drip all five grants share one millisecond');
});

// ---------------------------------------------------------------------------
// (e) slot-leak freedom
// ---------------------------------------------------------------------------

test('ADV-e: no slot leaks across 1000 mixed acquire/abort/timeout/release cycles', async () => {
    const clock = makeClock();
    const limiter = new ProviderLimiter('leak', {
        ...BASE,
        maxConcurrent: 1,
        maxWaitMs: 100,
        adaptiveConcurrency: true,
        minConcurrent: 1,
        aimdInitialConcurrent: 1,
        aimdIncreaseEvery: 50,
        aimdDecreaseFactor: 0.5,
        aimdTimeoutDecrease: true,
        fairness: true,
        queueLimit: 10_000,
    }, clock.deps);

    let hold = (await limiter.gate({})).ticket;
    assert.equal(limiter.active, 1);
    const outcomes = { granted: 0, aborted: 0, timeout: 0 };

    for (let i = 0; i < 1_000; i += 1) {
        const mode = i % 3;
        if (mode === 0) {
            const controller = new AbortController();
            const pending = limiter.gate({ signal: controller.signal });
            controller.abort();
            const outcome = await pending;
            assert.equal(outcome.ok, false, 'aborted waiter is refused');
            assert.equal(outcome.reason, 'aborted');
            outcomes.aborted += 1;
        }
        else if (mode === 1) {
            const pending = limiter.gate({});
            clock.advance(151);
            limiter.expireWaiters();
            const outcome = await pending;
            assert.equal(outcome.ok, false, 'timed-out waiter is refused');
            assert.equal(outcome.reason, 'wait-timeout');
            outcomes.timeout += 1;
        }
        else {
            const pending = limiter.gate({});
            limiter.settle(hold, { kind: 'success' });
            const outcome = await pending;
            assert.equal(outcome.ok, true, 'released waiter is granted');
            limiter.settle(outcome.ticket, { kind: 'success' });
            hold = (await limiter.gate({})).ticket;
            outcomes.granted += 1;
        }
        assert.ok(limiter.active <= limiter.effectiveConcurrency(), `active ${limiter.active} <= ${limiter.effectiveConcurrency()} at cycle ${i}`);
        assert.ok(limiter.active === 1, `exactly one slot held at cycle ${i}`);
    }

    limiter.settle(hold, { kind: 'success' });
    assert.deepEqual(outcomes, { granted: 333, aborted: 334, timeout: 333 });
    assert.equal(limiter.active, 0, 'no slot leaks');
    assert.equal(limiter.queued, 0);
    assert.equal(limiter.tickets.size, 0, 'no unsettled ticket leaks');
    assert.equal(limiter.waiters.length, 0);
    assert.equal(clock.pending().length, 0, 'no timer leaks');

    // Part 2: 1000 plain v1-style acquire()/release() pairs.
    const clock2 = makeClock();
    const v1 = new ProviderLimiter('v1style', { ...BASE, maxConcurrent: 2, maxWaitMs: 50, queueLimit: 10_000 }, clock2.deps);
    for (let i = 0; i < 1_000; i += 1) {
        assert.equal(await v1.acquire(undefined), true);
        if (i % 5 === 0)
            assert.equal(await v1.acquire(undefined), true);
        assert.ok(v1.activeCount <= 2, `active ${v1.activeCount} <= 2`);
        v1.release();
        v1.release();
        assert.equal(v1.activeCount, 0, `all slots returned at cycle ${i}`);
    }
    assert.equal(v1.queued, 0);
    assert.equal(v1.activeCount, 0);
});

// ---------------------------------------------------------------------------
// (f) disposal safety
// ---------------------------------------------------------------------------

test('ADV-f: disposal leaves no pending timer and no post-disposal waiter resolution', async () => {
    const clock = makeClock();
    const limiter = new ProviderLimiter('dispose', {
        ...BASE,
        maxConcurrent: 1,
        maxWaitMs: 60_000,
        rpmLimit: 5,
        tpmLimit: 100_000,
        cooldownHysteresisMs: 5_000,
        queueLimit: 10,
    }, clock.deps);

    const granted = await limiter.gate({});
    assert.equal(granted.ok, true);
    let disposedOutcomes = 0;
    const controller = new AbortController();
    for (let i = 0; i < 3; i += 1)
        limiter.gate(i === 1 ? { signal: controller.signal } : {}).then((outcome) => {
            disposedOutcomes += 1;
            assert.equal(outcome.ok, false);
            assert.equal(outcome.reason, 'disposed');
        });
    limiter.observe429(undefined, { ...BASE, cooldownHysteresisMs: 5_000 });
    limiter.resetEscalation(clock.now);
    await tick();
    assert.equal(limiter.queued, 3);
    assert.ok(clock.pending().length > 0, 'timers exist before disposal');

    limiter.dispose();
    await tick();

    assert.equal(limiter.disposed, true);
    assert.equal(limiter.queued, 0, 'the queue is drained');
    assert.equal(limiter.active, 0, 'in-flight slots are force-settled');
    assert.equal(limiter.tickets.size, 0);
    assert.equal(disposedOutcomes, 3, 'every queued waiter is refused exactly once');
    assert.equal(clock.pending().length, 0, 'no timer is left armed');
    assert.equal(limiter.deadlineTimer, null);
    assert.equal(limiter.pumpTimer, null);
    assert.equal(limiter.cooldownTimer, null);
    assert.equal(limiter.hysteresisTimer, null);

    // An abort that arrives after disposal must not resolve anything again.
    controller.abort();
    clock.advance(10 * 60_000);
    await tick();
    assert.equal(clock.advance(0), 0, 'no timer can fire into the next generation');
    assert.equal(disposedOutcomes, 3, 'no post-disposal waiter resolution');

    // gate(), settle() and release() after disposal are inert.
    const after = await limiter.gate({});
    assert.equal(after.ok, false);
    assert.equal(after.reason, 'disposed');
    assert.equal(limiter.queued, 0);
    limiter.settle(granted.ticket, { kind: 'success' });
    limiter.release();
    assert.equal(limiter.active, 0, 'settle/release after dispose cannot corrupt the counter');
    assert.equal(limiter.dispose(), undefined);
});

// ---------------------------------------------------------------------------
// (g) options immutability
// ---------------------------------------------------------------------------

function optionsPayload() {
    return {
        provider: 'deepseek-official',
        purpose: 'compaction',
        sessionId: 'session-1',
        maxTokens: 256,
        system: 's'.repeat(400),
        tools: [{ name: 'read', description: 'read a file', parameters: { type: 'object', properties: { path: { type: 'string' } } } }],
        messages: [
            { role: 'user', source: { case: 'text', text: 'hello' }, content: 'h'.repeat(64) },
            { role: 'assistant', content: 'a'.repeat(160) },
        ],
    };
}

test('ADV-g: the deep-frozen options object is never mutated and never attempted to be mutated', async () => {
    // Variant 1: a Proxy records every set/defineProperty/delete attempt.
    const plain = optionsPayload();
    const snapshot = structuredClone(plain);
    const { proxy, attempts } = mutationRecorder(plain);
    const meter = { calls: 0, estimateMessage() { this.calls += 1; return 10; } };
    const host = makeCtx({ tokenMeter: meter });
    apply(host.ctx, { ...BASE, enabled: true, systemPrompt: false, tpmLimit: 100_000, rpmLimit: 100, fairness: true });

    const listener = host.hooks['llm/stream'][0];
    assert.equal(typeof listener, 'function', 'the llm/stream listener is registered');
    const source = (async function* () {
        yield { type: 'usage', usage: { totalTokens: 12 } };
        yield { type: 'finish', reason: { kind: 'stop' } };
    })();
    const chunks = [];
    for await (const chunk of await listener(proxy, async () => source))
        chunks.push(chunk.type);
    assert.deepEqual(chunks, ['usage', 'finish']);
    await tick();

    assert.equal(attempts.length, 0, `no mutation attempt on options (observed: ${JSON.stringify(attempts)})`);
    assert.deepEqual(plain, snapshot, 'the options object is byte-equal after the call');
    assert.ok(meter.calls >= 1, 'TPM estimation actually walked the payload (so the scan was exercised)');
    host.ctx.dispose();

    // Variant 2: a deeply frozen object; a strict-mode write would throw.
    const frozenPayload = optionsPayload();
    const frozenSnapshot = structuredClone(frozenPayload);
    deepFreeze(frozenPayload);
    const host2 = makeCtx({});
    apply(host2.ctx, { ...BASE, enabled: true, systemPrompt: false, tpmLimit: 100_000, rpmLimit: 100, fairness: true });
    const listener2 = host2.hooks['llm/stream'][0];
    const source2 = (async function* () {
        yield { type: 'finish', reason: { kind: 'stop' } };
    })();
    const seen = [];
    for await (const chunk of await listener2(frozenPayload, async () => source2))
        seen.push(chunk.reason.kind);
    assert.deepEqual(seen, ['stop'], 'a deeply frozen options object flows through without a write attempt');
    assert.deepEqual(frozenPayload, frozenSnapshot);
    host2.ctx.dispose();
});

test('ADV-p: gateStream passes the numeric usageTokens() value in details.usage (§5.4), not the raw usage chunk', async () => {
    const limiter = new ProviderLimiter('usage-detail', { ...BASE, maxConcurrent: 2, tpmLimit: 100_000 });
    const seen = [];
    const source = (async function* () {
        yield { type: 'usage', usage: { inputTokens: 7, outputTokens: 5 } };
        yield { type: 'finish', reason: { kind: 'stop' } };
    })();
    for await (const _ of gateStream(source, limiter, undefined, (kind, details) => seen.push({ kind, usage: details?.usage })))
        void _;
    assert.equal(seen.length, 1);
    assert.equal(seen[0].kind, 'success');
    assert.equal(typeof seen[0].usage, 'number', 'details.usage is the settled token count (§5.4), not a TokenUsage object');
    assert.equal(seen[0].usage, 12, 'usageTokens(input+output)');

    // §5.1.1 types details.usage as TokenUsage|null; §5.4 says the terminal
    // chunk passes usageTokens() (a number) — the implementation follows §5.4.
    const noUsage = new ProviderLimiter('usage-none', { ...BASE, maxConcurrent: 2, tpmLimit: 100_000 });
    const none = [];
    const source2 = (async function* () {
        yield { type: 'finish', reason: { kind: 'stop' } };
    })();
    for await (const _ of gateStream(source2, noUsage, undefined, (kind, details) => none.push({ kind, usage: details?.usage })))
        void _;
    assert.equal(none[0].usage, null, 'no usage chunk yields null, never NaN');
});

test('ADV-q: TokenWindow is inert at limit 0 (reserve returns null, used/eligibleAt are 0)', () => {
    const window = new TokenWindow(0, 60_000, 16);
    assert.equal(window.enabled, false);
    assert.equal(window.reserve(5_000, 1_000), null, '§5.4 leaves reserve unguarded, but limit 0 must stay inert (§3.3)');
    assert.equal(window.used(1_000), 0, 'no token accounting at limit 0');
    assert.equal(window.eligibleAt(9_999_999, 1_000), 0);
    assert.equal(window.entries.length, 0, 'nothing is logged at limit 0');
    assert.equal(window.snapshot(1_000).used, 0);
});

// ---------------------------------------------------------------------------
// additional adversarial edges
// ---------------------------------------------------------------------------

test('ADV-h: an abort racing a slot release resolves exactly once, in the right disposition', async () => {
    const clock = makeClock();
    const make = () => new ProviderLimiter('race', { ...BASE, maxConcurrent: 1, queueLimit: 10 }, clock.deps);

    // (1) abort first, then release: the waiter is refused and the release grants nobody.
    const first = make();
    const hold1 = (await first.gate({})).ticket;
    const controller1 = new AbortController();
    let resolutions1 = 0;
    const pending1 = first.gate({ signal: controller1.signal }).then((outcome) => {
        resolutions1 += 1;
        return outcome;
    });
    assert.equal(first.queued, 1);
    controller1.abort();
    first.settle(hold1, { kind: 'success' });
    const outcome1 = await pending1;
    await tick();
    assert.equal(resolutions1, 1, 'one resolution');
    assert.equal(outcome1.ok, false);
    assert.equal(outcome1.reason, 'aborted');
    assert.equal(first.queued, 0);
    assert.equal(first.active, 0, 'the aborted waiter never held a slot');

    // (2) release first, then abort: the grant wins and the abort is a no-op.
    const second = make();
    const hold2 = (await second.gate({})).ticket;
    const controller2 = new AbortController();
    let resolutions2 = 0;
    const pending2 = second.gate({ signal: controller2.signal }).then((outcome) => {
        resolutions2 += 1;
        return outcome;
    });
    second.settle(hold2, { kind: 'success' });
    controller2.abort();
    const outcome2 = await pending2;
    await tick();
    assert.equal(resolutions2, 1, 'no second resolution from the late abort');
    assert.equal(outcome2.ok, true, 'the grant wins the race');
    assert.equal(second.active, 1);
    assert.equal(second.queued, 0);
    second.settle(outcome2.ticket, { kind: 'aborted' });
    assert.equal(second.active, 0);

    // (3) an already-aborted signal is refused before enqueueing.
    const third = make();
    const controller3 = new AbortController();
    controller3.abort();
    const outcome3 = await third.gate({ signal: controller3.signal });
    assert.equal(outcome3.ok, false);
    assert.equal(outcome3.reason, 'aborted');
    assert.equal(third.queued, 0);
    assert.equal(clock.pending().length, 0);
});

test('ADV-i: two routes keep independent queues and budgets', async () => {
    const clock = makeClock();
    const cfg = { ...BASE, maxConcurrent: 2, rpmLimit: 2, rpmWindowMs: 1_000, queueLimit: 100 };
    const limiters = createLimiters(['providerA', 'providerB'], cfg, clock.deps);
    const a = limiterFor(limiters, 'providerA');
    const b = limiterFor(limiters, 'providerB');
    assert.notEqual(a, b);

    const first = await a.gate({});
    await a.gate({});
    const aThird = a.gate({});
    await tick();
    assert.equal(a.queued, 1, 'route A queues its third request on its own RPM budget');
    assert.equal(b.queued, 0, 'route B is untouched');
    assert.equal(b.rpm.used(clock.now), 0, 'route A RPM charge does not leak into route B');
    assert.equal((await b.gate({})).ok, true, 'route B admits while route A is saturated');
    assert.equal(limiterFor(limiters, 'unknown-route')?.provider, '*', 'unlisted routes use the * fallback');
    assert.equal(limiters.size, 3);

    a.settle(first.ticket, { kind: 'success' });
    clock.advance(1_000);
    const drained = await aThird;
    assert.equal(drained.ok, true, 'route A drains on its own clock');
    a.settle(drained.ticket, { kind: 'success' });
});

test('ADV-j: throttle semantics: empty queue, ladder, cap, Retry-After and 503 multiplier', () => {
    const cfg = { ...BASE, maxConcurrent: 2, cooldownBaseMs: 100, cooldownMaxMs: 1_000 };
    // A fake clock keeps the ladder arithmetic exact (a real Date.now can drift 1ms).
    const clock = makeClock();
    const at = (limiter) => new ProviderLimiter(limiter, cfg, clock.deps);

    // Ladder on a fresh limiter with no waiters.
    const ladder = at('ladder');
    assert.equal(ladder.queued, 0);
    assert.deepEqual(
        [ladder.observe429(undefined, cfg), ladder.observe429(undefined, cfg), ladder.observe429(undefined, cfg), ladder.observe429(undefined, cfg), ladder.observe429(undefined, cfg)],
        [100, 200, 400, 800, 1_000],
        'v1 ladder base*2^esc capped at cooldownMaxMs',
    );
    assert.ok(typeof ladder.last429At === 'string');
    assert.equal(ladder.cooldownEscalations, 5);

    // Retry-After: first observation is verbatim; a larger hint is capped.
    const fresh = at('retry');
    assert.equal(fresh.observe429(300, cfg), 300, 'Retry-After 300 is used verbatim at escalation 0');
    assert.equal(fresh.observe429(5_000, cfg), 1_000, 'a Retry-After above cooldownMaxMs is capped by the cap');

    const escalated = at('retry2');
    escalated.observe429(undefined, cfg); // escalations -> 1
    assert.equal(escalated.observe429(300, cfg), 600, 'at escalation 1 the hint is multiplied by the ladder factor (spec §5.6 pseudocode)');

    const zero = at('retry3');
    assert.equal(zero.observe429(0, cfg), 100, 'a zero hint falls back to the local base');
    assert.equal(at('retry4').observe429(Number.NaN, cfg), 100, 'a NaN hint falls back to the local base');

    // 503 overload multiplier.
    const overloadCfg = { ...cfg, overloadCooldownMultiplier: 2 };
    assert.equal(at('overload').observe429(undefined, overloadCfg, 503), 200, '503 doubles the local window');
    assert.equal(at('overload2').observe429(undefined, overloadCfg, 529), 200, '529 doubles the local window');

    // A server-directed hint is never jittered, even in 'full' mode.
    const fullJitterCfg = { ...cfg, cooldownJitter: 'full' };
    assert.equal(at('jitter').observe429(500, fullJitterCfg), 500);
    assert.equal(at('jitter2').observe429(undefined, fullJitterCfg), 50, 'full jitter with rng 0.5 halves the local window');
});

test('ADV-k: timers: exactly one refd anchor while a waiter is queued, none when idle', async () => {
    const clock = makeClock();
    const limiter = new ProviderLimiter('anchor', { ...BASE, maxConcurrent: 1, queueLimit: 10 }, clock.deps);
    assert.equal(clock.refdPending().length, 0, 'idle limiter refs nothing');

    const hold = (await limiter.gate({})).ticket;
    const pending = limiter.gate({});
    await tick();
    assert.equal(limiter.queued, 1);
    assert.equal(clock.refdPending().length, 1, 'exactly one refd handle while a waiter is queued');

    limiter.observe429(undefined, BASE);
    assert.equal(clock.refdPending().length, 1, 'the cooldown timer is unrefd');
    assert.equal(limiter.cooldownTimer.unrefd, true);
    assert.equal(limiter.deadlineTimer.refd, true);

    limiter.settle(hold, { kind: 'success' });
    clock.advance(1_000);
    const outcome = await pending;
    await tick();
    assert.equal(outcome.ok, true, 'the waiter is granted once the cooldown closes');
    assert.equal(limiter.queued, 0);
    assert.equal(clock.refdPending().length, 0, 'the anchor is released when the queue empties');
    limiter.settle(outcome.ticket, { kind: 'success' });
    limiter.closeCooldown(clock.now + 10_000);
    limiter.resetEscalation(clock.now);
    assert.equal(clock.pending().length, 0, 'no timer survives an idle limiter');
});

test('ADV-l: TPM accounting: reservation, settlement, budget-exceeded and conservative expiry', async () => {
    const clock = makeClock();
    const cfg = { ...BASE, maxConcurrent: 5, tpmLimit: 1_000, tpmWindowMs: 1_000, tpmOutputReserveTokens: 100, queueLimit: 100 };
    const limiter = new ProviderLimiter('tpm', cfg, clock.deps);

    // reservation over the whole budget is shed immediately, never queued
    const shed = await limiter.gate({ estimatedTokens: 1_001 });
    assert.equal(shed.ok, false);
    assert.equal(shed.reason, 'budget-exceeded');
    assert.equal(shed.code, 'RATE_LIMIT_BUDGET_EXCEEDED');
    assert.equal(limiter.queued, 0);
    assert.equal(limiter.active, 0);

    // a granted request reserves, and settlement with observed usage returns headroom
    const first = await limiter.gate({ estimatedTokens: 600 });
    assert.equal(first.ok, true);
    assert.equal(limiter.tpm.used(clock.now), 600);
    limiter.settle(first.ticket, { kind: 'success', usage: 110 });
    assert.equal(limiter.tpm.used(clock.now), 110, 'usage replaces the reservation');

    // a failure keeps the full reservation (never released)
    const second = await limiter.gate({ estimatedTokens: 500 });
    assert.equal(second.ok, true);
    limiter.settle(second.ticket, { kind: 'error', usage: null, failure: { code: 'RATE_LIMIT' } });
    assert.equal(limiter.tpm.used(clock.now), 610, 'the reservation stays charged without usage');

    // the third request must wait for the first entry to expire
    const third = limiter.gate({ estimatedTokens: 500 });
    await tick();
    assert.equal(limiter.queued, 1, 'the TPM window is a hard gate');
    clock.advance(999);
    await tick();
    assert.equal(limiter.queued, 1);
    clock.advance(1);
    const outcome = await third;
    assert.equal(outcome.ok, true, 'granted when the token window frees up');
    limiter.settle(outcome.ticket, { kind: 'success', usage: 5 });
    // At exactly t0+1000 both t0 entries have left the window, so only the
    // settled 5-token charge of the third request remains.
    assert.equal(limiter.tpm.used(clock.now), 5);

    // TokenWindow compaction conserves tokens
    const window = new TokenWindow(100_000, 60_000, 16);
    for (let i = 0; i < 64; i += 1)
        window.settle(window.reserve(10, i), 10, i);
    assert.ok(window.entries.length <= 16, `entries capped, length=${window.entries.length}`);
    assert.equal(window.used(63), 640, 'compaction conserves the token sum');
});

test('ADV-m: queue cap and FIFO vs fairness shed order are enforced', async () => {
    const clock = makeClock();
    const limiter = new ProviderLimiter('cap', { ...BASE, maxConcurrent: 1, queueLimit: 2 }, clock.deps);
    await limiter.gate({});
    const first = limiter.gate({});
    const second = limiter.gate({});
    await tick();
    assert.equal(limiter.queued, 2);
    const third = await limiter.gate({});
    assert.equal(third.ok, false);
    assert.equal(third.reason, 'queue-full');
    assert.equal(third.code, 'RATE_LIMIT_QUEUE_FULL');
    assert.equal(limiter.queued, 2, 'a shed request is never enqueued');
    assert.equal(limiter.queuedPeak, 2);
    assert.ok(third.message.includes('2/2'), `message names the cap: ${third.message}`);
    clock.advance(EXPIRE_MS);
    limiter.expireWaiters();
    assert.equal((await first).ok, false);
    assert.equal((await second).ok, false);
    assert.equal(limiter.queued, 0);
    assert.equal(limiter.timeoutCount, 2);
});

test('ADV-n: fairness classes and aging override behave as specified', () => {
    const cfg = { ...BASE, fairness: true, interactiveWeight: 6, backgroundWeight: 2, utilityWeight: 1, agingThresholdMs: 0 };
    const queue = new FairQueue(cfg);
    assert.deepEqual(queue.stride, { interactive: 10, background: 30, utility: 60 });

    // All three classes permanently backlogged: utility must not be skipped
    // for more than 8 consecutive grants, and no class is skipped longer.
    let clock = 0;
    const sequence = [];
    for (let i = 0; i < 400; i += 1) {
        for (const classId of ['interactive', 'background', 'utility'])
            queue.enqueue({ id: `${i}:${classId}`, classId, key: `s-${classId}`, enqueuedAt: clock += 1, deadline: 1e15 });
        const picked = queue.pick(clock);
        sequence.push(picked.classId);
        queue.noteGrant(picked);
        queue.remove(picked);
        for (const classId of ['interactive', 'background', 'utility']) {
            if (classId === picked.classId)
                continue;
            queue.remove(queue.toArray().find((waiter) => waiter.classId === classId));
        }
    }
    for (const target of ['interactive', 'background', 'utility']) {
        let run = 0;
        let maxRun = 0;
        for (const classId of sequence) {
            if (classId === target) {
                maxRun = Math.max(maxRun, run);
                run = 0;
            }
            else {
                run += 1;
            }
        }
        maxRun = Math.max(maxRun, run);
        assert.ok(maxRun <= 8, `class ${target} is never skipped for more than 8 consecutive grants (observed ${maxRun})`);
    }
    const head = sequence.slice(0, 3);
    assert.deepEqual(head, ['interactive', 'background', 'utility'], 'the first three grants respect the class order at equal pass');

    // Aging override: a low-weight waiter older than agingThresholdMs preempts.
    const aging = new FairQueue({ ...cfg, agingThresholdMs: 100 });
    aging.enqueue({ id: 'u', classId: 'utility', key: 'u', enqueuedAt: 1_000, deadline: 1e15 });
    aging.enqueue({ id: 'i', classId: 'interactive', key: 'i', enqueuedAt: 1_000, deadline: 1e15 });
    assert.equal(aging.pick(1_050).id, 'i', 'not aged yet: the stride schedule picks interactive');
    assert.equal(aging.pick(1_100).id, 'u', 'aged waiter preempts the stride schedule');
});

test('ADV-o: jitteredWindow and Cooldown pure semantics', () => {
    assert.equal(jitteredWindow(100, 'none', () => 0.9), 100);
    assert.equal(jitteredWindow(100, 'equal', () => 0), 50);
    assert.equal(jitteredWindow(100, 'equal', () => 1), 100);
    assert.equal(jitteredWindow(100, 'full', () => 0), 0);
    assert.equal(jitteredWindow(100, 'full', () => 1), 100);
    assert.equal(jitteredWindow(-5, 'full', () => 1), 0);
    assert.equal(jitteredWindow(100, 'full', () => Number.NaN), 0, 'a NaN draw degrades to 0, never NaN');

    const cooldown = new Cooldown({ cooldownBaseMs: 100, cooldownMaxMs: 1_000, cooldownHysteresisMs: 0 });
    assert.equal(cooldown.observe429(undefined, { cooldownBaseMs: 100, cooldownMaxMs: 1_000 }, undefined, 0), 100);
    assert.equal(cooldown.escalations, 1);
    assert.equal(cooldown.close(100, { cooldownHysteresisMs: 0 }), true);
    assert.equal(cooldown.escalations, 0, 'no hysteresis: close resets the ladder');
    assert.equal(cooldown.close(200, { cooldownHysteresisMs: 0 }), false, 'close is idempotent');

    const hysteretic = new Cooldown({ cooldownBaseMs: 100, cooldownMaxMs: 1_000, cooldownHysteresisMs: 500 });
    hysteretic.observe429(undefined, { cooldownBaseMs: 100, cooldownMaxMs: 1_000 }, undefined, 0);
    hysteretic.close(100, { cooldownHysteresisMs: 500 });
    assert.equal(hysteretic.escalations, 1, 'hysteresis: close keeps the ladder');
    assert.equal(hysteretic.observe429(undefined, { cooldownBaseMs: 100, cooldownMaxMs: 1_000 }, undefined, 150), 200);
});
