/**
 * Cooldown: Retry-After verbatim, the v1 escalation ladder, jitter modes with
 * an injected rng, hysteresis, and drip release.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Cooldown, jitteredWindow } from '../lib/cooldown.js';
import { ProviderLimiter } from '../lib/limiter.js';

/** A deterministic clock + timer harness for the limiter. */
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
        pending() { return handles.filter((handle) => !handle.cleared && !handle.fired); },
        fire(handle) { handle.fired = true; handle.fn(); },
        runDue() {
            let fired = 0;
            for (;;) {
                const due = handles.filter((handle) => !handle.cleared && !handle.fired && handle.at <= now);
                if (due.length === 0 || fired > 500)
                    break;
                for (const handle of due) {
                    handle.fired = true;
                    handle.fn();
                }
                fired += due.length;
            }
            return fired;
        },
    };
}

const BASE_CONFIG = { cooldownBaseMs: 100, cooldownMaxMs: 1_000, cooldownJitter: 'none', overloadCooldownMultiplier: 1 };

test('jitteredWindow: none is exact, equal/full follow the injected rng', () => {
    assert.equal(jitteredWindow(1_000, 'none', () => 0.9), 1_000);
    assert.equal(jitteredWindow(1_000, 'equal', () => 0), 500);
    assert.equal(jitteredWindow(1_000, 'equal', () => 0.5), 750);
    assert.equal(jitteredWindow(1_000, 'equal', () => 1), 1_000);
    assert.equal(jitteredWindow(1_000, 'full', () => 0), 0);
    assert.equal(jitteredWindow(1_000, 'full', () => 0.5), 500);
    assert.equal(jitteredWindow(1_000, 'full', () => 2), 1_000, 'out-of-range rng is clamped');
    assert.equal(jitteredWindow(1_000, 'full', () => Number.NaN), 0);
});

test('Cooldown: Retry-After is used verbatim and never jittered', () => {
    for (const mode of ['none', 'equal', 'full']) {
        const cooldown = new Cooldown({ ...BASE_CONFIG, cooldownJitter: mode });
        assert.equal(cooldown.observe429(500, { ...BASE_CONFIG, cooldownJitter: mode }, undefined, 1_000, () => 0.5), 500);
        assert.equal(cooldown.until, 1_500);
    }
    // A provider delay longer than the cap is still capped by cooldownMaxMs
    // (v1 semantics); it only escapes *jitter*, never the ceiling.
    const cooldown = new Cooldown(BASE_CONFIG);
    assert.equal(cooldown.observe429(5_000, BASE_CONFIG, undefined, 0, () => 0.5), 1_000);
});

test('Cooldown: the v1 escalation ladder doubles and caps at cooldownMaxMs', () => {
    const cooldown = new Cooldown(BASE_CONFIG);
    const windows = [];
    for (let i = 0; i < 6; i += 1)
        windows.push(cooldown.observe429(undefined, BASE_CONFIG, undefined, 1_000, () => 0.5));
    assert.deepEqual(windows, [100, 200, 400, 800, 1_000, 1_000]);
    assert.equal(cooldown.escalations, 6);
});

test('Cooldown: a locally computed window is jittered', () => {
    const equal = new Cooldown({ ...BASE_CONFIG, cooldownJitter: 'equal' });
    assert.equal(equal.observe429(undefined, { ...BASE_CONFIG, cooldownJitter: 'equal' }, undefined, 0, () => 0.5), 75);
    const full = new Cooldown({ ...BASE_CONFIG, cooldownJitter: 'full' });
    assert.equal(full.observe429(undefined, { ...BASE_CONFIG, cooldownJitter: 'full' }, undefined, 0, () => 0.5), 50);
});

test('Cooldown: 503/529 windows get the overload multiplier', () => {
    const config = { ...BASE_CONFIG, overloadCooldownMultiplier: 2 };
    const cooldown = new Cooldown(config);
    assert.equal(cooldown.observe429(undefined, config, 503, 0, () => 0.5), 200);
    assert.equal(cooldown.observe429(undefined, config, 529, 0, () => 0.5), 400);
    assert.equal(cooldown.observe429(undefined, config, 503, 0, () => 0.5), 800);
    assert.equal(cooldown.observe429(undefined, config, 503, 0, () => 0.5), 1_000, 'capped');
    // A server-directed delay is still multiplied (it is an overload signal).
    const directed = new Cooldown(config);
    assert.equal(directed.observe429(500, config, 503, 0, () => 0.5), 1_000);
    // A plain 429 keeps the un-multiplied window.
    const plain = new Cooldown(config);
    assert.equal(plain.observe429(undefined, config, 429, 0, () => 0.5), 100);
});

test('Cooldown: close resets the ladder only when hysteresis is disabled', () => {
    const plain = new Cooldown(BASE_CONFIG);
    plain.observe429(undefined, BASE_CONFIG, undefined, 0, () => 0.5);
    assert.equal(plain.close(10, BASE_CONFIG), true);
    assert.equal(plain.escalations, 0);
    assert.equal(plain.until, 0);
    assert.equal(plain.close(20, BASE_CONFIG), false, 'idempotent');

    const sticky = new Cooldown({ ...BASE_CONFIG, cooldownHysteresisMs: 500 });
    sticky.observe429(undefined, { ...BASE_CONFIG, cooldownHysteresisMs: 500 }, undefined, 0, () => 0.5);
    sticky.observe429(undefined, { ...BASE_CONFIG, cooldownHysteresisMs: 500 }, undefined, 0, () => 0.5);
    assert.equal(sticky.escalations, 2);
    sticky.close(10, { ...BASE_CONFIG, cooldownHysteresisMs: 500 });
    assert.equal(sticky.escalations, 2, 'the ladder survives the window closing');
});

test('Cooldown: an observation cancels a pending hysteresis decay', () => {
    const config = { ...BASE_CONFIG, cooldownHysteresisMs: 500 };
    const cooldown = new Cooldown(config);
    cooldown.hysteresisUntil = 5_000;
    cooldown.observe429(undefined, config, undefined, 1_000, () => 0.5);
    assert.equal(cooldown.hysteresisUntil, 0);
});

test('ProviderLimiter: hysteresis survives window close and successes, then decays', () => {
    const clock = fakeClock();
    const limiter = new ProviderLimiter('hyst', {
        maxConcurrent: 4,
        maxWaitMs: 60_000,
        cooldownBaseMs: 100,
        cooldownMaxMs: 1_000,
        cooldownHysteresisMs: 500,
    }, clock.deps);
    limiter.observe429();
    limiter.observe429();
    assert.equal(limiter.cooldownEscalations, 2);
    limiter.closeCooldown();
    assert.equal(limiter.cooldownEscalations, 2, 'the ladder is not reset by the window closing');
    limiter.resetEscalation();
    assert.equal(limiter.cooldownEscalations, 2, 'the ladder is not reset by the first success either');
    assert.ok(limiter.hysteresisTimer !== null, 'a decay timer is armed');
    assert.equal(limiter.hysteresisTimer.unrefd, true, 'the decay timer is unref’d');
    clock.advance(500);
    clock.runDue();
    assert.equal(limiter.cooldownEscalations, 0, 'the ladder decays after the quiet period');

    const plain = new ProviderLimiter('plain', {
        maxConcurrent: 4,
        maxWaitMs: 60_000,
        cooldownBaseMs: 100,
        cooldownMaxMs: 1_000,
    }, fakeClock().deps);
    plain.observe429();
    assert.equal(plain.cooldownEscalations, 1);
    plain.resetEscalation();
    assert.equal(plain.cooldownEscalations, 0, 'no hysteresis ⇒ v1 reset on success');
    plain.observe429();
    plain.closeCooldown();
    assert.equal(plain.cooldownEscalations, 0, 'no hysteresis ⇒ v1 reset on close');
    limiter.dispose();
    plain.dispose();
});

test('ProviderLimiter: drip release staggers the post-cooldown drain', async () => {
    const clock = fakeClock();
    const limiter = new ProviderLimiter('drip', {
        maxConcurrent: 10,
        maxWaitMs: 60_000,
        cooldownBaseMs: 100,
        cooldownMaxMs: 1_000,
        releaseSpreadMs: 200,
    }, clock.deps);
    limiter.observe429();
    const queued = [
        limiter.gate({}),
        limiter.gate({}),
        limiter.gate({}),
    ];
    assert.equal(limiter.queued, 3, 'all three wait for the cooldown');
    const start = clock.now;
    limiter.closeCooldown(start);
    assert.equal(limiter.active, 1, 'exactly one waiter is released in the first tick');
    assert.equal(limiter.queued, 2, 'the rest are still queued');
    assert.equal(limiter.nextDispatchAt, start + 300, 'full jitter over one extra spread interval (rng 0.5)');
    assert.equal(limiter.spreadGrants, 1);

    clock.advance(300);
    clock.runDue();
    assert.equal(limiter.active, 2, 'the second release happens a full step later, not in the same tick');
    assert.equal(limiter.queued, 1);
    clock.advance(300);
    clock.runDue();
    assert.equal(limiter.active, 3);
    assert.equal(limiter.queued, 0, 'the queue drained one step at a time');
    const outcomes = await Promise.all(queued);
    assert.ok(outcomes.every((outcome) => outcome.ok === true));
    limiter.dispose();
});

test('ProviderLimiter: without drip release the drain is one tick (v1 behaviour)', async () => {
    const clock = fakeClock();
    const limiter = new ProviderLimiter('burst', {
        maxConcurrent: 10,
        maxWaitMs: 60_000,
        cooldownBaseMs: 100,
        cooldownMaxMs: 1_000,
    }, clock.deps);
    limiter.observe429();
    const queued = [limiter.gate({}), limiter.gate({}), limiter.gate({})];
    limiter.closeCooldown();
    assert.equal(limiter.active, 3, 'all waiters fan out in the same tick');
    assert.equal(limiter.queued, 0);
    assert.equal(limiter.bursts, 1);
    const outcomes = await Promise.all(queued);
    assert.ok(outcomes.every((outcome) => outcome.ok === true));
    limiter.dispose();
});
