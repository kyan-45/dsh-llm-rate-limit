/**
 * t8 re-verification: is finding F1 actually closed after the t7 repair?
 *
 * Written by the verifier after the repair, from the guarantee (not from the
 * diff): a session's round-robin `served` counter must survive a drain, so a
 * session that re-arrives continues the rotation instead of restarting it.
 *
 * The original t4 gate (test/verify/verify-limiter-adversarial.test.js, ADV-b2)
 * is deliberately NOT imported or re-derived here; every scenario below is an
 * independent construction that goes further than the one-position original.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

import { ProviderLimiter } from '../../lib/limiter.js';
import { FairQueue } from '../../lib/scheduler.js';
import { Cooldown } from '../../lib/cooldown.js';

const tick = () => new Promise((resolve) => setImmediate(resolve));

/** Deterministic clock + timer harness (same shape as the t4 gate). */
function makeClock(start = 1_700_000_000_000) {
    let now = start;
    let seq = 0;
    const timers = new Map();
    const deps = {
        now: () => now,
        random: () => 0.5,
        setTimeout(fn, ms) {
            const handle = {
                id: ++seq,
                at: now + (Number.isFinite(ms) && ms > 0 ? ms : 0),
                fn,
                cleared: false,
                unref() { return this; },
                ref() { return this; },
            };
            timers.set(handle.id, handle);
            return handle;
        },
        clearTimeout(handle) {
            if (handle !== null && handle !== undefined)
                timers.delete(handle.id);
        },
    };
    return {
        deps,
        get now() { return now; },
        advance(ms) {
            const target = now + ms;
            for (;;) {
                let best = null;
                for (const handle of timers.values()) {
                    if (handle.at > target)
                        continue;
                    if (best === null || handle.at < best.at || (handle.at === best.at && handle.id < best.id))
                        best = handle;
                }
                if (best === null)
                    break;
                now = Math.max(now, best.at);
                timers.delete(best.id);
                best.fn();
            }
            now = target;
            return 0;
        },
        pending() { return [...timers.values()]; },
    };
}

const CFG = {
    maxConcurrent: 1,
    maxWaitMs: 60_000,
    cooldownBaseMs: 1_000,
    cooldownMaxMs: 8_000,
    rpmLimit: 0,
    tpmLimit: 0,
    adaptiveConcurrency: false,
    fairness: true,
    agingThresholdMs: 0,
    queueLimit: 10_000,
};

/** Grant-order recorder for a limiter (one grant per settle at maxConcurrent 1). */
function recorder(limiter) {
    const log = [];
    return {
        log,
        /** Queue a request and remember the waiter's session key. */
        ask(sessionId, purpose) {
            limiter.gate({ sessionId, purpose }).then((outcome) => {
                assert.equal(outcome.ok, true, 'every request in these scenarios is admitted');
                log.push({ session: sessionId, ticket: outcome.ticket });
            });
        },
        /** Keys with at least one queued waiter, in enqueue order (oldest first). */
        queuedKeys() {
            return limiter.waiters.map((waiter) => waiter.key);
        },
        /** Settle the ticket of grant #index (default: the newest) and return the new grant. */
        async settle(index) {
            const target = index ?? log.length - 1;
            const ticket = log[target].ticket;
            limiter.settle(ticket, { kind: 'success' });
            await tick();
            return log[log.length - 1];
        },
    };
}

// ---------------------------------------------------------------------------
// F1R-1: the original counterexample, independently rebuilt
// ---------------------------------------------------------------------------

test('F1R-1: the original idle-route re-arrival counterexample now orders A#1, B, A#2', async () => {
    const clock = makeClock();
    const limiter = new ProviderLimiter('f1r1', CFG, clock.deps);
    const rec = recorder(limiter);

    rec.ask('A'); // admitted instantly (route idle, one slot)
    await tick();
    clock.advance(1);
    rec.ask('A'); // drained session re-arrives: its served counter must survive
    clock.advance(1);
    rec.ask('B');
    await tick();
    assert.deepEqual(rec.log.map((entry) => entry.session), ['A'], 'only A#1 is in flight');

    await rec.settle(0);
    assert.equal(rec.log[1].session, 'B', `after A#1, session B must be served next (observed ${rec.log.map((e) => e.session).join(',')})`);

    await rec.settle(1);
    assert.equal(rec.log[2].session, 'A', 'then the drained session re-arrives for its second grant');
    assert.deepEqual(rec.log.map((entry) => entry.session), ['A', 'B', 'A']);
});

// ---------------------------------------------------------------------------
// F1R-2 (variation i): three sessions, interleaved drain and re-arrival
// ---------------------------------------------------------------------------

test('F1R-2 (i): three sessions with interleaved drain/re-arrival rotate fairly at maxConcurrent=1', async () => {
    const clock = makeClock();
    const limiter = new ProviderLimiter('f1r2', CFG, clock.deps);
    const rec = recorder(limiter);
    const violations = [];

    const ask = (session) => {
        clock.advance(1); // unique enqueue timestamps so the tie-break is well defined
        rec.ask(session);
    };
    const settleAndCheck = async (index) => {
        const before = rec.queuedKeys();
        const granted = await rec.settle(index);
        const others = before.filter((key) => key !== granted.session);
        const history = rec.log.map((entry) => entry.session);
        const previous = history[history.length - 2];
        if (previous === granted.session && others.length > 0)
            violations.push(`${history.join(',')} (repeated ${granted.session} while ${[...new Set(others)].join('/')} waited)`);
        return granted;
    };

    ask('A'); // A#1 instant grant
    await tick();
    ask('A'); // A re-arrives before B: the F1 shape
    ask('B');
    ask('C');
    await tick();

    await settleAndCheck(0); // frees the slot while A#2, B, C are queued
    assert.deepEqual(rec.log.map((e) => e.session), ['A', 'B'], 'A must not overtake B and C with a fresh counter');
    await settleAndCheck(1); // B#1 done
    ask('B');
    assert.deepEqual(rec.log.map((e) => e.session), ['A', 'B', 'C'], 'C is next before B\'s second request');
    await settleAndCheck(2); // C#1 done
    ask('C');
    await settleAndCheck(3); // A#2 (rotation wraps)
    assert.deepEqual(rec.log.map((e) => e.session), ['A', 'B', 'C', 'A', 'B'], 'the rotation continues');

    // Bounded continuation, keeping every served counter below the 16 clamp so
    // the minimum-served expectation stays exact: ask A, B, C, then free a slot.
    for (let round = 0; round < 9; round += 1) {
        ask('A');
        ask('B');
        ask('C');
        await tick();
        await settleAndCheck(rec.log.length - 1);
    }
    assert.deepEqual(violations, [], `no session may repeat a grant while another session waits: ${violations[0] ?? ''}`);
    const grantsPerSession = {};
    for (const entry of rec.log)
        grantsPerSession[entry.session] = (grantsPerSession[entry.session] ?? 0) + 1;
    assert.ok(grantsPerSession.A >= 4 && grantsPerSession.B >= 4 && grantsPerSession.C >= 4, `all three sessions progressed: ${JSON.stringify(grantsPerSession)}`);
    assert.ok(Math.max(...Object.values(grantsPerSession)) < 16, 'no counter reached the retention clamp');
});

// ---------------------------------------------------------------------------
// F1R-3 (variation ii): the A,A,B sequence at maxConcurrent=3
// ---------------------------------------------------------------------------

test('F1R-3 (ii): the A,A,B sequence stays fair at maxConcurrent=3', async () => {
    // (a) The literal sequence on an idle route: all three are admitted at once.
    const idleClock = makeClock();
    const idle = new ProviderLimiter('f1r3-idle', { ...CFG, maxConcurrent: 3 }, idleClock.deps);
    const idleOutcomes = [];
    for (const sessionId of ['A', 'A', 'B']) {
        idleClock.advance(1);
        idleOutcomes.push(await idle.gate({ sessionId }));
    }
    assert.deepEqual(idleOutcomes.map((outcome) => outcome.ok), [true, true, true], 'three slots admit A, A and B');
    assert.equal(idle.active, 3);
    assert.equal(idle.queued, 0, 'nothing was queued, so there is no ordering to break');

    // (b) The same shape with the route saturated: A, A, B queued behind three
    // holds, then the holds released one at a time.
    const clock = makeClock();
    const limiter = new ProviderLimiter('f1r3-busy', { ...CFG, maxConcurrent: 3 }, clock.deps);
    const holds = [];
    for (let i = 0; i < 3; i += 1)
        holds.push((await limiter.gate({ sessionId: `hold${i}` })).ticket);
    const rec = recorder(limiter);
    rec.ask('A');
    clock.advance(1);
    rec.ask('A');
    clock.advance(1);
    rec.ask('B');
    await tick();
    assert.equal(limiter.queued, 3, 'A, A and B wait behind the three holds');

    for (const hold of holds) {
        limiter.settle(hold, { kind: 'success' });
        await tick();
    }
    assert.deepEqual(
        rec.log.map((entry) => entry.session),
        ['A', 'B', 'A'],
        'at concurrency 3 the rotation still inserts B before A\'s second grant',
    );
    assert.equal(limiter.active, 3);
});

// ---------------------------------------------------------------------------
// F1R-4 (variation iii): one session drains and re-arrives repeatedly
// ---------------------------------------------------------------------------

test('F1R-4 (iii): a session that fully drains and re-arrives six times never gets two consecutive grants', async () => {
    const clock = makeClock();
    const limiter = new ProviderLimiter('f1r4', CFG, clock.deps);
    const rec = recorder(limiter);

    rec.ask('A'); // A#1 admitted instantly and then completely drains
    await tick();

    for (let round = 1; round <= 6; round += 1) {
        // The in-flight ticket is always the newest grant (maxConcurrent is 1).
        const before = rec.log[rec.log.length - 1];
        assert.equal(before.session, 'A', `round ${round}: A is the session in flight before the round`);
        clock.advance(1);
        rec.ask('A'); // A re-arrives first in every round
        clock.advance(1);
        rec.ask('B');
        await tick();

        const inFlightBefore = rec.log.length - 1;
        const afterFirstRelease = await rec.settle(inFlightBefore);
        assert.equal(afterFirstRelease.session, 'B', `round ${round}: B is served before the re-arrived A (observed ${afterFirstRelease.session})`);
        const afterSecondRelease = await rec.settle(rec.log.length - 1);
        assert.equal(afterSecondRelease.session, 'A', `round ${round}: A then continues the rotation`);
        assert.equal(limiter.active, 1, `round ${round}: exactly one slot is held`);
    }

    const sequence = rec.log.map((entry) => entry.session);
    assert.equal(sequence[0], 'A');
    for (let i = 1; i < sequence.length; i += 1)
        assert.notEqual(sequence[i], sequence[i - 1], `no two consecutive grants from one session: ${sequence.slice(0, i + 1).join(',')}`);
    assert.deepEqual(sequence, ['A', 'B', 'A', 'B', 'A', 'B', 'A', 'B', 'A', 'B', 'A', 'B', 'A']);
});

// ---------------------------------------------------------------------------
// F1R-5: model-based fuzz — the retained-counter rule must hold generally
// ---------------------------------------------------------------------------

test('F1R-5: across arbitrary drain/re-arrival the granted session is always the minimum-served backlogged session', async () => {
    const clock = makeClock();
    const sessions = ['s1', 's2', 's3', 's4'];
    const limiter = new ProviderLimiter('fuzz', CFG, clock.deps);
    const rec = recorder(limiter);

    // Independent model: served never resets (that is the repaired contract),
    // and the tie-break is the oldest queued waiter of the session.
    const served = new Map(sessions.map((session) => [session, 0]));
    const pending = new Map(sessions.map((session) => [session, []]));
    let seq = 0;
    let recorded = 0;
    let reArrivals = 0;

    const drainGrants = () => {
        while (recorded < rec.log.length) {
            const entry = rec.log[recorded];
            recorded += 1;
            served.set(entry.session, served.get(entry.session) + 1);
            pending.get(entry.session).shift();
        }
    };

    // Deterministic LCG so a failure is reproducible.
    let rngState = 12345;
    const nextRandom = () => {
        rngState = (rngState * 1103515245 + 12345) % 2147483648;
        return rngState / 2147483648;
    };

    clock.advance(1);
    rec.ask('s1'); // initial in-flight request
    await tick();
    drainGrants();

    let totalGrants = 1;
    while (totalGrants < 48) {
        const queued = sessions.filter((session) => pending.get(session).length > 0);
        const totalPending = queued.reduce((sum, session) => sum + pending.get(session).length, 0);
        const canAsk = Math.max(...served.values()) < 15;
        const wantsToAsk = queued.length === 0 || (totalPending < 6 && nextRandom() < 0.55);
        if (canAsk && wantsToAsk) {
            const candidates = sessions.filter((session) => pending.get(session).length < 3);
            const session = candidates[Math.floor(nextRandom() * candidates.length)];
            if (pending.get(session).length === 0 && served.get(session) > 0)
                reArrivals += 1; // drained and came back: the F1 shape
            seq += 1;
            pending.get(session).push(seq);
            clock.advance(1);
            rec.ask(session);
            await tick();
            drainGrants();
            totalGrants = recorded;
            continue;
        }
        if (queued.length === 0)
            continue;
        assert.deepEqual(
            [...new Set(rec.queuedKeys())].sort(),
            [...queued].sort(),
            'the model and the limiter agree on who is backlogged',
        );
        // Expectation from the contract, not from the code: the minimum-served
        // backlogged session wins; ties go to the oldest queued waiter.
        const minServed = Math.min(...queued.map((session) => served.get(session)));
        const candidates = queued.filter((session) => served.get(session) === minServed);
        const expected = candidates.reduce((a, b) => (pending.get(a)[0] <= pending.get(b)[0] ? a : b));
        await rec.settle(rec.log.length - 1);
        const granted = rec.log[rec.log.length - 1].session;
        assert.equal(granted, expected, `grant #${recorded + 1} must serve the minimum-served backlogged session (queued: ${queued.join(',')}, served: ${JSON.stringify(Object.fromEntries(served))})`);
        drainGrants();
        totalGrants = recorded;
    }

    assert.ok(reArrivals >= 10, `the fuzz actually exercised drain/re-arrival (${reArrivals} events)`);
    assert.ok(totalGrants >= 48, `fuzz completed ${totalGrants} grants`);
    assert.equal(limiter.active, 1);
});

// ---------------------------------------------------------------------------
// F1R-6: the retained state is memory-bounded (count and value)
// ---------------------------------------------------------------------------

function drainDistinctSessions(queue, from, to) {
    for (let i = from; i < to; i += 1) {
        const waiter = { id: `w${i}`, classId: 'interactive', key: `session-${i}`, enqueuedAt: i, deadline: 1e15 };
        queue.enqueue(waiter);
        const picked = queue.pick(i);
        queue.noteGrant(picked);
        queue.remove(picked);
    }
}

test('F1R-6: retained fairness state is bounded by count (1024) and by value (16)', async () => {
    const queue = new FairQueue({ interactiveWeight: 6, backgroundWeight: 2, utilityWeight: 1, agingThresholdMs: 0 });
    drainDistinctSessions(queue, 0, 2_000);
    const afterTwoThousand = queue.retainedSessions;
    drainDistinctSessions(queue, 2_000, 12_000);
    const afterTwelveThousand = queue.retainedSessions;

    assert.ok(afterTwoThousand <= 1_024, `2000 distinct drained sessions retained ${afterTwoThousand} entries (bound 1024)`);
    assert.ok(afterTwelveThousand <= 1_024, `12000 distinct drained sessions retained ${afterTwelveThousand} entries (bound 1024)`);
    const values = [...queue.retainedServed.values()];
    assert.ok(values.length > 0);
    assert.ok(values.every((value) => Number.isInteger(value) && value <= 16), `every retained counter is clamped (max ${Math.max(...values)})`);
    // The most recent drain is retained; the cap evicted the oldest entries.
    assert.ok(queue.retainedServed.has('interactive\u0000session-11999'), 'the newest drained session is retained');

    // One session draining 500 times cannot accumulate 500 entries or a 500 counter.
    const repeated = new FairQueue({ interactiveWeight: 6, backgroundWeight: 2, utilityWeight: 1, agingThresholdMs: 0 });
    for (let i = 0; i < 500; i += 1) {
        const waiter = { id: `r${i}`, classId: 'interactive', key: 'repeat', enqueuedAt: i, deadline: 1e15 };
        repeated.enqueue(waiter);
        const picked = repeated.pick(i);
        repeated.noteGrant(picked);
        repeated.remove(picked);
    }
    assert.equal(repeated.retainedSessions, 1, 'a repeated drain/re-arrival keeps exactly one entry');
    assert.equal([...repeated.retainedServed.values()][0], 16, 'its counter is clamped to 16');

    // Integration: a real limiter with 3000 distinct one-shot sessions is bounded too.
    const clock = makeClock();
    const limiter = new ProviderLimiter('f1r6', CFG, clock.deps);
    for (let i = 0; i < 3_000; i += 1) {
        clock.advance(1);
        const outcome = await limiter.gate({ sessionId: `route-session-${i}` });
        assert.equal(outcome.ok, true);
        limiter.settle(outcome.ticket, { kind: 'success' });
    }
    assert.ok(limiter.queue.retainedSessions <= 1_024, `limiter retained ${limiter.queue.retainedSessions} entries (bound 1024)`);
    assert.equal(limiter.active, 0);
    assert.equal(limiter.queued, 0);
});

test('F1R-7: dispose releases the retained fairness state', async () => {
    const clock = makeClock();
    const limiter = new ProviderLimiter('f1r7', CFG, clock.deps);
    for (let i = 0; i < 50; i += 1) {
        clock.advance(1);
        const outcome = await limiter.gate({ sessionId: `s-${i}` });
        limiter.settle(outcome.ticket, { kind: 'success' });
    }
    assert.ok(limiter.queue.retainedSessions > 0, 'retention is populated before disposal');
    limiter.dispose();
    assert.equal(limiter.queue.retainedSessions, 0, 'dispose clears the retained map');
});

// ---------------------------------------------------------------------------
// F1R-8: retention is class-aware (non-interactive classes behave the same)
// ---------------------------------------------------------------------------

test('F1R-8: the drain/re-arrival rotation also holds inside the utility class', async () => {
    const clock = makeClock();
    const limiter = new ProviderLimiter('f1r8', CFG, clock.deps);
    const rec = recorder(limiter);

    rec.ask('A', 'session-title'); // utility class, admitted instantly
    await tick();
    clock.advance(1);
    rec.ask('A', 'session-title');
    clock.advance(1);
    rec.ask('B', 'session-title');
    await tick();
    assert.equal(limiter.queue.classes.has('utility'), true, 'the requests are classified as utility');

    await rec.settle(0);
    assert.deepEqual(rec.log.map((entry) => entry.session), ['A', 'B'], 'the utility class keeps the same rotation');
    await rec.settle(1);
    assert.deepEqual(rec.log.map((entry) => entry.session), ['A', 'B', 'A']);
});

// ---------------------------------------------------------------------------
// F1R-10: discrimination check — the new counterexamples must detect the bug
// ---------------------------------------------------------------------------

/**
 * Emulate the pre-repair `FairQueue.remove()` on one runtime instance (the old
 * code deleted a key's record without retaining `served`). Nothing on disk is
 * modified; this only proves the scenarios above are not passing vacuously.
 */
function emulatePreRepairRemove(limiter) {
    limiter.queue.remove = function remove(waiter) {
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
        if (record.list.length === 0)
            byKey.delete(waiter.key);
        if (byKey.size === 0)
            this.classes.delete(waiter.classId);
        this.total -= 1;
        return true;
    };
}

test('F1R-10: the counterexamples detect the pre-repair behaviour (discrimination check)', async () => {
    // (a) The original shape must reappear (A#1, A#2, B) under the old remove().
    const clockA = makeClock();
    const oldA = new ProviderLimiter('disc-a', CFG, clockA.deps);
    emulatePreRepairRemove(oldA);
    const orderA = [];
    const ticketsA = [];
    for (const sessionId of ['A', 'A', 'B']) {
        clockA.advance(1);
        oldA.gate({ sessionId }).then((outcome) => {
            orderA.push(sessionId);
            ticketsA.push(outcome.ticket);
        });
    }
    await tick();
    oldA.settle(ticketsA[0], { kind: 'success' });
    await tick();
    oldA.settle(ticketsA[1], { kind: 'success' });
    await tick();
    assert.deepEqual(orderA, ['A', 'A', 'B'], `the old remove() reproduces F1 exactly (observed ${orderA.join(',')})`);

    // (b) The repeated-drain shape must also reappear under the old remove().
    const clockB = makeClock();
    const oldB = new ProviderLimiter('disc-b', CFG, clockB.deps);
    emulatePreRepairRemove(oldB);
    const orderB = [];
    const ticketsB = [];
    const askB = (sessionId) => {
        clockB.advance(1);
        oldB.gate({ sessionId }).then((outcome) => {
            orderB.push(sessionId);
            ticketsB.push(outcome.ticket);
        });
    };
    askB('A');
    await tick();
    for (let round = 0; round < 3; round += 1) {
        const inFlight = ticketsB.length - 1;
        askB('A');
        askB('B');
        await tick();
        oldB.settle(ticketsB[inFlight], { kind: 'success' });
        await tick();
        oldB.settle(ticketsB[ticketsB.length - 1], { kind: 'success' });
        await tick();
    }
    assert.deepEqual(orderB.slice(0, 3), ['A', 'A', 'B'], `the re-arriving session wins the tie under the old remove() (observed ${orderB.join(',')})`);
});

// ---------------------------------------------------------------------------
// F1R-11: the age override still rescues a heavily-served returning session
// ---------------------------------------------------------------------------

test('F1R-11: aging still forces a heavily-served returning session past the retained counter', async () => {
    const clock = makeClock();
    const limiter = new ProviderLimiter('f1r11', { ...CFG, agingThresholdMs: 100 }, clock.deps);

    // Give session X the maximum retained counter: grant it and let it drain.
    for (let i = 0; i < 20; i += 1) {
        clock.advance(1);
        const outcome = await limiter.gate({ sessionId: 'X' });
        limiter.settle(outcome.ticket, { kind: 'success' });
    }
    assert.equal(limiter.queue.retainedServed.get('interactive\u0000X'), 16, 'X carries the clamped retained counter');

    // Saturate the route, then let X re-arrive before a crowd of fresh sessions.
    const rec = recorder(limiter);
    const hold = (await limiter.gate({ sessionId: 'hold' })).ticket;
    clock.advance(1);
    rec.ask('X');
    const xEnqueuedAt = clock.now;
    for (let i = 0; i < 20; i += 1) {
        clock.advance(1);
        rec.ask(`fresh${i}`);
    }
    await tick();
    assert.equal(limiter.queued, 21);

    // Below the aging threshold the retained counter keeps X behind every fresh
    // session (all at served 0), which is the intended anti-monopoly effect.
    clock.advance(50);
    limiter.settle(hold, { kind: 'success' });
    await tick();
    assert.equal(rec.log[0].session, 'fresh0', 'before aging, a fresh session wins over X');

    // Past the threshold the oldest waiter is forced regardless of its counter.
    clock.advance(100 - (clock.now - xEnqueuedAt) + 1);
    limiter.settle(rec.log[0].ticket, { kind: 'success' });
    await tick();
    assert.equal(rec.log[1].session, 'X', `aging forces the oldest waiter through (observed ${rec.log[1].session})`);
});

// ---------------------------------------------------------------------------
// F1R-9: the README's cooldown formula, checked against the implementation
// ---------------------------------------------------------------------------

test('F1R-9: README cooldown formula matches the implemented cooldown machine', () => {
    const readme = readFileSync(new URL('../../README.md', import.meta.url), 'utf8');
    const compact = readme.replace(/\s+/g, ' ');
    assert.ok(compact.includes('base = providerRetryAfterMs > 0 ? providerRetryAfterMs : cooldownBaseMs'), 'README states the base rule');
    assert.ok(compact.includes('window = min(base × 2^min(escalations, 4), cooldownMaxMs)'), 'README states the ladder + cap rule');
    assert.ok(compact.includes('status ∈ {503, 529} ? min(window × overloadCooldownMultiplier, cooldownMaxMs)'), 'README states the overload rule');
    assert.ok(compact.includes('providerDirected ? window : jitteredWindow(window, cooldownJitter)'), 'README states the jitter rule');
    assert.ok(compact.includes('replaces the base'), 'README does not claim the hint is added verbatim on top of the ladder');

    const cfg = { cooldownBaseMs: 100, cooldownMaxMs: 1_000, cooldownJitter: 'none', overloadCooldownMultiplier: 1, cooldownHysteresisMs: 0 };
    const ladder = new ProviderLimiter('readme-ladder', cfg, makeClock().deps);
    assert.deepEqual(
        [ladder.observe429(undefined, cfg), ladder.observe429(undefined, cfg), ladder.observe429(undefined, cfg), ladder.observe429(undefined, cfg), ladder.observe429(undefined, cfg), ladder.observe429(undefined, cfg)],
        [100, 200, 400, 800, 1_000, 1_000],
        'local base × 2^min(esc,4), capped (README line 2)',
    );

    const hintAtZero = new ProviderLimiter('readme-hint0', cfg, makeClock().deps);
    assert.equal(hintAtZero.observe429(300, cfg), 300, 'hint replaces the base at escalation 0');
    const hintAtOne = new ProviderLimiter('readme-hint1', cfg, makeClock().deps);
    hintAtOne.observe429(undefined, cfg);
    assert.equal(hintAtOne.observe429(300, cfg), 600, 'hint replaces the base and is then multiplied by the ladder (README line 2)');
    const hintAboveCap = new ProviderLimiter('readme-cap', cfg, makeClock().deps);
    assert.equal(hintAboveCap.observe429(5_000, cfg), 1_000, 'a hint above the cap is capped like any other window (README line 3)');
    assert.equal(new ProviderLimiter('readme-zero', cfg, makeClock().deps).observe429(0, cfg), 100, 'a non-positive hint is not provider-directed');

    const overload = new ProviderLimiter('readme-503', { ...cfg, overloadCooldownMultiplier: 2 }, makeClock().deps);
    assert.equal(overload.observe429(undefined, { ...cfg, overloadCooldownMultiplier: 2 }, 503), 200, '503 multiplier (README line 3)');
    const jitterCfg = { ...cfg, cooldownJitter: 'full' };
    assert.equal(new ProviderLimiter('readme-jitter-hint', jitterCfg, makeClock().deps).observe429(500, jitterCfg), 500, 'a server-directed hint is never jittered (README line 4)');
    assert.equal(new ProviderLimiter('readme-jitter-local', jitterCfg, makeClock().deps).observe429(undefined, jitterCfg), 50, 'a local window is jittered (README line 4)');

    // The pure Cooldown machine and the limiter agree on the same inputs.
    const machine = new Cooldown(cfg);
    assert.equal(machine.observe429(undefined, cfg, undefined, 0), 100);
    assert.equal(machine.observe429(300, cfg, undefined, 0), 600);
    assert.equal(machine.observe429(5_000, cfg, undefined, 0), 1_000, 'cap applies after the ladder multiplication');
});
