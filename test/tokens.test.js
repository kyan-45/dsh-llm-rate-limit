/**
 * Token estimation, reservation, and usage settlement.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { estimateInputTokens, reservationFor, usageTokens } from '../lib/tokens.js';

const CFG = { tpmLimit: 10_000, tpmOutputReserveTokens: 1_024 };

test('estimateInputTokens: local heuristic without a tokenMeter', () => {
    assert.equal(estimateInputTokens(undefined, undefined), 0);
    assert.equal(estimateInputTokens({}, undefined), 0);
    assert.equal(estimateInputTokens({ system: 'a'.repeat(40) }, undefined), 10);
    assert.equal(estimateInputTokens({ system: 'abc' }, undefined), 1);
    const tool = { name: 'abcd', description: '', parameters: {} };
    assert.equal(
        estimateInputTokens({ tools: [tool] }, undefined),
        Math.ceil((4 + 0 + JSON.stringify({}).length) / 4),
    );
    const message = { role: 'user', content: 'x'.repeat(40) };
    assert.equal(estimateInputTokens({ messages: [message] }, undefined), Math.ceil(JSON.stringify(message).length / 4));

    const sum = estimateInputTokens({ system: 'a'.repeat(40), messages: [message] }, undefined);
    assert.equal(sum, 10 + Math.ceil(JSON.stringify(message).length / 4));
});

test('estimateInputTokens: prefers tokenMeter.estimateMessage for sourced messages', () => {
    const sourced = { source: 'user', kind: 'text' };
    const unsourced = { role: 'user', content: 'abcd' };
    const meter = {
        calls: 0,
        estimateMessage(message) {
            this.calls += 1;
            return message === sourced ? 7 : 999;
        },
    };
    const total = estimateInputTokens({ messages: [sourced, unsourced] }, meter);
    assert.equal(meter.calls, 1, 'the meter is only asked about sourced messages');
    assert.equal(total, 7 + Math.ceil(JSON.stringify(unsourced).length / 4));
});

test('estimateInputTokens: a throwing or odd tokenMeter falls back, never throws', () => {
    const throwing = { estimateMessage() { throw new Error('nope'); } };
    const sourced = { source: 'x', text: 'abcd' };
    assert.equal(
        estimateInputTokens({ messages: [sourced] }, throwing),
        Math.ceil(JSON.stringify(sourced).length / 4),
    );
    const odd = { estimateMessage: () => Number.NaN };
    assert.equal(
        estimateInputTokens({ messages: [sourced] }, odd),
        Math.ceil(JSON.stringify(sourced).length / 4),
    );
});

test('estimateInputTokens: odd payloads contribute 0 instead of throwing', () => {
    const circular = { source: 'x' };
    circular.self = circular;
    const total = estimateInputTokens({ messages: [circular], tools: [null], system: 42 }, undefined);
    assert.equal(total, 0);
    assert.ok(Number.isFinite(total));
});

test('reservationFor: disabled TPM costs nothing, enabled TPM adds the output reserve', () => {
    assert.equal(reservationFor({ system: 'a'.repeat(40) }, { tpmLimit: 0, tpmOutputReserveTokens: 1_024 }, undefined), 0);
    assert.equal(reservationFor({ system: 'a'.repeat(40) }, CFG, undefined), 10 + 1_024);
    assert.equal(reservationFor({ system: 'a'.repeat(40), maxTokens: 512 }, CFG, undefined), 10 + 512);
    assert.equal(reservationFor({ system: 'a'.repeat(40), maxTokens: 0 }, CFG, undefined), 10 + 1_024);
    assert.equal(reservationFor({ system: 'a'.repeat(40), maxTokens: -5 }, CFG, undefined), 10 + 1_024);
});

test('usageTokens: never NaN, tolerates odd usage objects', () => {
    assert.equal(usageTokens(undefined), null);
    assert.equal(usageTokens(null), null);
    assert.equal(usageTokens('nope'), null);
    assert.equal(usageTokens({}), null);
    assert.equal(usageTokens({ totalTokens: 5 }), 5);
    assert.equal(usageTokens({ totalTokens: -5 }), 0);
    assert.equal(usageTokens({ inputTokens: 3, outputTokens: 4 }), 7);
    assert.equal(usageTokens({ inputTokens: 3 }), 3);
    assert.equal(usageTokens({ totalTokens: Number.NaN, inputTokens: 3, outputTokens: 4 }), 7);
    assert.equal(usageTokens({ totalTokens: Number.NaN }), null);
    assert.equal(usageTokens({ totalTokens: 'x', inputTokens: Number.NaN, outputTokens: Number.NaN }), null);
    for (const value of [{}, { totalTokens: Number.NaN }, { inputTokens: Number.POSITIVE_INFINITY }])
        assert.ok(usageTokens(value) === null || Number.isFinite(usageTokens(value)));
});
