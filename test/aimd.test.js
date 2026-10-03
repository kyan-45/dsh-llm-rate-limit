/**
 * AIMD adaptive concurrency: increase cadence, multiplicative decrease,
 * clamps, and timeout feedback.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Aimd } from '../lib/aimd.js';
import { normalizeLimiterOptions } from '../lib/config.js';

function aimdFor(overrides) {
    const config = normalizeLimiterOptions({
        maxConcurrent: 4,
        adaptiveConcurrency: true,
        minConcurrent: 1,
        aimdIncreaseEvery: 3,
        aimdDecreaseFactor: 0.5,
        ...overrides,
    });
    return new Aimd(config);
}

test('AIMD: starts at aimdInitialConcurrent (null ⇒ ceil(maxConcurrent / 2))', () => {
    assert.equal(aimdFor({}).current, 2);
    assert.equal(aimdFor({ aimdInitialConcurrent: 3 }).current, 3);
    assert.equal(aimdFor({ aimdInitialConcurrent: 99 }).current, 4, 'clamped to the ceiling');
});

test('AIMD: additive increase every `aimdIncreaseEvery` clean settles', () => {
    const aimd = aimdFor({});
    aimd.onSuccess(0);
    aimd.onSuccess(0);
    assert.equal(aimd.current, 2, 'not yet');
    aimd.onSuccess(0);
    assert.equal(aimd.current, 3, 'one increase');
    assert.equal(aimd.increases, 1);
    aimd.onSuccess(0);
    aimd.onSuccess(0);
    aimd.onSuccess(0);
    assert.equal(aimd.current, 4, 'ceiling reached');
    aimd.onSuccess(0);
    aimd.onSuccess(0);
    aimd.onSuccess(0);
    assert.equal(aimd.current, 4, 'never above the ceiling');
    assert.equal(aimd.increases, 2);
});

test('AIMD: multiplicative decrease on a throttle, floored at minConcurrent', () => {
    const aimd = aimdFor({});
    aimd.onThrottle(100);
    assert.equal(aimd.current, 1);
    assert.equal(aimd.decreases, 1);
    assert.equal(aimd.lastDecreaseAt, 100);
    aimd.onThrottle(200);
    assert.equal(aimd.current, 1, 'already at the floor');
    assert.equal(aimd.decreases, 1, 'a decrease is only counted when the bound moves');
});

test('AIMD: effectiveConcurrency is clamped to [minConcurrent, maxConcurrent]', () => {
    const aimd = aimdFor({});
    assert.equal(aimd.effectiveConcurrency(), 2);
    aimd.onSuccess(0);
    aimd.onSuccess(0);
    aimd.onSuccess(0);
    assert.equal(aimd.effectiveConcurrency(), 3);
    aimd.onThrottle(0);
    assert.equal(aimd.effectiveConcurrency(), 1);
});

test('AIMD: a throttle resets the success streak', () => {
    const aimd = aimdFor({});
    aimd.onSuccess(0);
    aimd.onSuccess(0);
    aimd.onThrottle(0);
    assert.equal(aimd.successStreak, 0);
    aimd.onSuccess(0);
    aimd.onSuccess(0);
    assert.equal(aimd.current, 1, 'the streak restarted');
});

test('AIMD: a wait-timeout decreases only when aimdTimeoutDecrease is on', () => {
    const strict = aimdFor({});
    strict.onTimeout(0);
    assert.equal(strict.current, 1);

    const tolerant = aimdFor({ aimdTimeoutDecrease: false });
    tolerant.onTimeout(0);
    assert.equal(tolerant.current, 2, 'timeouts do not feed the decrease');
});

test('AIMD: snapshot reports the documented fields', () => {
    const aimd = aimdFor({});
    assert.deepEqual(aimd.snapshot(), {
        current: 2,
        floor: 1,
        ceiling: 4,
        successStreak: 0,
        increases: 0,
        decreases: 0,
    });
});
