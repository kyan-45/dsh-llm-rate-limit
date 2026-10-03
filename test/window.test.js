/**
 * Sliding-window budgets: RPM pruning/eligibility and TPM
 * reservation/settlement/compaction.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RequestWindow, TokenWindow } from '../lib/window.js';

test('RequestWindow: disabled when the limit is 0', () => {
    const window = new RequestWindow(0, 1_000);
    assert.equal(window.enabled, false);
    assert.equal(window.eligibleAt(500), 0);
    window.record(500);
    assert.equal(window.used(500), 0);
    assert.deepEqual(window.snapshot(500), { limit: 0, windowMs: 1_000, used: 0, nextEligibleInMs: 0 });
});

test('RequestWindow: exact sliding-window admission and rejection', () => {
    const window = new RequestWindow(2, 1_000);
    assert.equal(window.eligibleAt(0), 0);
    window.record(0);
    assert.equal(window.eligibleAt(10), 0, 'one of two used');
    window.record(10);
    assert.equal(window.used(20), 2);
    assert.equal(window.eligibleAt(20), 1_000, 'the oldest admission must expire first');
    assert.equal(window.eligibleAt(1_000), 0, 'a boundary entry expires at exactly windowMs');
    assert.equal(window.used(1_000), 1);
});

test('RequestWindow: the (limit + 1)-th admission waits for the right entry', () => {
    const window = new RequestWindow(3, 100);
    window.record(0);
    window.record(10);
    window.record(20);
    assert.equal(window.eligibleAt(30), 100);
    assert.deepEqual(window.snapshot(50), { limit: 3, windowMs: 100, used: 3, nextEligibleInMs: 50 });
    assert.equal(window.used(99), 3);
    assert.equal(window.used(100), 2, 'the 0ms entry expired');
    assert.equal(window.eligibleAt(100), 0, 'two of three used ⇒ headroom again');
});

test('RequestWindow: the log is bounded by the limit and the cursor compacts', () => {
    const window = new RequestWindow(5_000, 60_000);
    for (let i = 0; i < 5_000; i += 1)
        window.record(i);
    assert.equal(window.used(4_999), 5_000);
    assert.equal(window.stamps.length, 5_000, 'never more than `limit` entries');
    assert.equal(window.used(200_000), 0);
    assert.equal(window.head, 0, 'prune cursor compacted');
    assert.equal(window.stamps.length, 0, 'no stale timestamps retained');
});

test('TokenWindow: disabled when the limit is 0', () => {
    const window = new TokenWindow(0, 1_000, 16);
    assert.equal(window.enabled, false);
    assert.equal(window.reserve(500, 10), null);
    assert.equal(window.used(10), 0);
    assert.equal(window.eligibleAt(50_000, 10), 0);
});

test('TokenWindow: budget admission and rejection', () => {
    const window = new TokenWindow(100, 1_000, 64);
    assert.equal(window.eligibleAt(60, 0), 0);
    window.reserve(60, 0);
    assert.equal(window.used(10), 60);
    assert.equal(window.eligibleAt(40, 10), 0, '40 fits in the remaining 40');
    assert.equal(window.eligibleAt(41, 10), 1_000, '41 does not fit; the 60-token entry must expire');
    assert.deepEqual(window.snapshot(10), {
        limit: 100,
        windowMs: 1_000,
        used: 60,
        reserved: 60,
        nextEligibleInMs: 0,
    });
    const full = new TokenWindow(100, 1_000, 64);
    full.reserve(100, 0);
    assert.deepEqual(full.snapshot(10), {
        limit: 100,
        windowMs: 1_000,
        used: 100,
        reserved: 100,
        nextEligibleInMs: 990,
    });
});

test('TokenWindow: eligibleAt walks entries until enough expires', () => {
    const window = new TokenWindow(100, 1_000, 64);
    window.reserve(30, 0);
    window.reserve(30, 100);
    window.reserve(30, 200);
    assert.equal(window.used(250), 90);
    assert.equal(window.eligibleAt(60, 250), 1_100, 'needs the first two entries to expire');
    assert.equal(window.eligibleAt(15, 250), 1_000, 'one entry expiring is enough');
});

test('TokenWindow: usage settles the reservation and returns headroom', () => {
    const window = new TokenWindow(100, 1_000, 64);
    const entry = window.reserve(60, 0);
    window.settle(entry, 20, 10);
    assert.equal(window.used(10), 20);
    assert.equal(window.reserved, 0);
    assert.equal(window.eligibleAt(60, 10), 0, '20 + 60 fits');
});

test('TokenWindow: a missing or non-finite usage keeps the full charge', () => {
    const window = new TokenWindow(1_000, 1_000, 64);
    const kept = window.reserve(120, 0);
    window.settle(kept, null, 10);
    assert.equal(window.used(10), 120, 'no usage observed ⇒ reservation kept');
    assert.equal(window.reserved, 0, 'the request is done, so it is no longer "unsettled"');
    const odd = window.reserve(80, 20);
    window.settle(odd, Number.NaN, 30);
    assert.equal(window.used(30), 200, 'never NaN');
    assert.ok(Number.isFinite(window.used(30)));
    window.settle(kept, 0, 40);
    assert.equal(window.used(40), 80, 'a reported zero usage releases the charge');
});

test('TokenWindow: entries expire with the window', () => {
    const window = new TokenWindow(100, 1_000, 64);
    window.reserve(60, 0);
    window.reserve(30, 500);
    assert.equal(window.used(900), 90);
    assert.equal(window.used(1_000), 30);
    assert.equal(window.used(1_500), 0);
});

test('TokenWindow: compaction conserves tokens and caps the log', () => {
    const window = new TokenWindow(1_000_000, 60_000, 16);
    for (let i = 0; i < 64; i += 1)
        window.reserve(10, i);
    assert.equal(window.entries.length, 64);
    assert.equal(window.sum, 640);
    window.settle(window.entries[0], 10, 100);
    assert.ok(window.entries.length <= 8, `log compacted to ${window.entries.length}`);
    assert.equal(window.used(100), 640, 'tokens are conserved by compaction');
    assert.equal(window.reserved, 630, 'only unsettled tokens stay reserved');
});

/** Σ entries[].tokens — the value `sum` must always equal (t5 finding F-1). */
function sumOf(window) {
    return window.entries.reduce((total, entry) => total + entry.tokens, 0);
}

test('TokenWindow: settling a reservation that compaction merged keeps used() exact', () => {
    const window = new TokenWindow(1_000_000, 60_000, 16);
    const reservations = [];
    for (let i = 0; i < 20; i += 1)
        reservations.push(window.reserve(1_000, i));
    assert.equal(window.entries.length, 20);
    assert.equal(window.used(20), 20_000);
    assert.equal(window.reserved, 20_000);

    // Settling one reservation triggers compaction, which replaces the whole log
    // with merged copies — orphaning every other ticket's entry object.
    window.settle(reservations[0], 0, 20);
    assert.ok(window.entries.length <= 8, `log compacted to ${window.entries.length}`);
    assert.equal(window.used(20), 19_000);
    assert.equal(sumOf(window), window.sum, 'sum === Σ entries after compaction');

    // A ticket whose entry was merged must still land its delta on the live
    // entry that now carries the tokens, or `sum` drifts for good.
    assert.equal(window.entries.includes(reservations[1]), false, 'the reservation was merged away');
    assert.ok(reservations[1].mergedInto !== null && reservations[1].mergedInto !== undefined, 'the merge relation is recorded');
    window.settle(reservations[1], 0, 21);
    assert.equal(window.used(21), 18_000);
    assert.equal(sumOf(window), window.sum, 'sum === Σ entries after settling a merged reservation');

    // Past the window every charge expires: a drifted sum would go negative and
    // silently overstate the TPM budget.
    assert.equal(window.used(60_100), 0, 'used() returns to 0, never negative');
    assert.equal(window.reserved, 0, 'reserved returns to 0');
    assert.equal(sumOf(window), 0);
});

test('TokenWindow: a settle that arrives after its entry left the window cannot move sum', () => {
    const window = new TokenWindow(1_000_000, 1_000, 16);
    const late = window.reserve(500, 0);
    assert.equal(window.used(1_000), 0, 'the reservation expired with the window');
    window.settle(late, 100, 1_000);
    assert.equal(window.used(1_000), 0, 'a late settle cannot push the window negative');
    assert.equal(window.reserved, 0);
    assert.equal(sumOf(window), window.sum);
});
