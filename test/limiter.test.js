import assert from 'node:assert/strict';
import { ProviderLimiter, gateStream, createLimiters, limiterFor } from '../lib/limiter.js';

// ---- concurrency gate -----------------------------------------------------
const config = { maxConcurrent: 2, maxWaitMs: 500, cooldownBaseMs: 100, cooldownMaxMs: 1000 };
const limiter = new ProviderLimiter('test', config);

// 1: two immediate grants, third queues.
assert.equal(await limiter.acquire(undefined), true, 'first grant');
assert.equal(await limiter.acquire(undefined), true, 'second grant');
const third = limiter.acquire(undefined);
assert.equal(limiter.activeCount, 2, 'active at limit');
assert.equal(limiter.queued, 1, 'one waiter queued');
limiter.release();
assert.equal(await third, true, 'third granted after release');
assert.equal(limiter.activeCount, 2);
limiter.release();
limiter.release();
assert.equal(limiter.activeCount, 0, 'all released');

// 2: abort cancels a queued wait.
limiter.acquire(undefined);
limiter.acquire(undefined); // full
const ac = new AbortController();
const queued = limiter.acquire(ac.signal);
ac.abort();
assert.equal(await queued, false, 'aborted wait resolves false');
assert.equal(limiter.queued, 0, 'aborted waiter dropped');
limiter.release();
limiter.release();

// ---- cooldown ----------------------------------------------------------------
const c2 = new ProviderLimiter('test', config);
const w = c2.observe429(undefined, config);
assert.ok(w >= 100, 'first cooldown >= base');
// while in cooldown, new acquires queue.
const w2 = c2.acquire(undefined);
assert.equal(c2.queued, 1, 'queued during cooldown');
assert.equal(await w2, true, 'granted when cooldown closed');
assert.equal(c2.cooldownEscalations, 0, 'cooldown timer reset escalation');

// escalation: two consecutive 429s double the window.
const c3 = new ProviderLimiter('test', config);
c3.observe429(undefined, config);
const w4 = c3.observe429(undefined, config);
assert.ok(w4 >= 200, 'second 429 doubles the base');

// Retry-After hint replaces the base.
const c4 = new ProviderLimiter('test', config);
const w5 = c4.observe429(500, config);
assert.equal(w5, 500, 'retry-after hint used verbatim');

// ---- gateStream ----------------------------------------------------------------
const c5 = new ProviderLimiter('test', { ...config, maxWaitMs: 200 });
let settledKind = null;
const src = (async function* () {
    yield { type: 'text-delta', text: 'a' };
    yield { type: 'finish', reason: { kind: 'stop' } };
})();
const gated = gateStream(src, c5, new AbortController().signal, (k) => { settledKind = k; });
const chunks = [];
for await (const chunk of gated)
    chunks.push(chunk);
assert.equal(chunks.length, 2, 'chunks pass through');
assert.equal(settledKind, 'success', 'settled success on clean finish');
assert.equal(c5.activeCount, 0, 'slot released after stream');

// stream that ends in a terminal error still releases the slot.
const c6 = new ProviderLimiter('test', { ...config, maxWaitMs: 200 });
let errSettled = null;
const errSrc = (async function* () {
    yield { type: 'finish', reason: { kind: 'error', failure: { code: 'RATE_LIMIT', message: '429' } } };
})();
const errGated = gateStream(errSrc, c6, new AbortController().signal, (k) => { errSettled = k; });
for await (const _ of errGated) { /* consume */ }
assert.equal(errSettled, 'error', 'error finish settles error');
assert.equal(c6.activeCount, 0, 'slot released on error stream');

// aborted finish settles aborted.
const c6b = new ProviderLimiter('test', { ...config, maxWaitMs: 200 });
let abSettled = null;
const abSrc = (async function* () {
    yield { type: 'finish', reason: { kind: 'aborted', failure: { code: 'ABORTED', message: 'canceled' } } };
})();
for await (const _ of gateStream(abSrc, c6b, new AbortController().signal, (k) => { abSettled = k; })) { }
assert.equal(abSettled, 'aborted', 'aborted finish settles aborted');

// queue timeout path.
const c7 = new ProviderLimiter('test', { ...config, maxWaitMs: 50 });
c7.acquire(undefined);
c7.acquire(undefined); // full
await new Promise((r) => setTimeout(r, 80));
c7.expireWaiters();
const c8 = new ProviderLimiter('test', { ...config, maxWaitMs: 50 });
c8.acquire(undefined);
c8.acquire(undefined);
const timedOut = c8.acquire(new AbortController().signal);
await new Promise((r) => setTimeout(r, 80));
c8.expireWaiters();
assert.equal(await timedOut, false, 'waiter times out after maxWaitMs');

// ---- map helpers ----------------------------------------------------------------
const map = createLimiters(['a'], config);
assert.equal(limiterFor(map, 'a')?.provider, 'a');
assert.equal(limiterFor(map, 'b')?.provider, '*', 'unknown routes fall back to *');
const allMap = createLimiters([], config);
assert.equal(allMap.size, 1, 'no provider list = single * limiter');

console.log('limiter unit tests: all passed');
