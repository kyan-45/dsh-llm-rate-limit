/**
 * v2 integration: apply() wiring and degradations, gateStream outcomes, the
 * no-leak invariant, disposal, the timer anchor, and the spec's own evidence
 * items (v1 test hashes, dependency-free package.json).
 */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { VERSION, apply, name } from '../lib/index.js';
import { V2_KEYS } from '../lib/config.js';
import { ProviderLimiter, createLimiters, gateStream } from '../lib/limiter.js';

const V1_LIMITER_TEST_SHA256 = '9B6696E54086D9A22C1C6F2423CCBBD92298982DC6ABD9F141AC4778D0358831';
const V1_INDEX_TEST_SHA256 = 'C5AC5EB7847696194CD649FAEDD6146B7746B7FEC40B81EFA8FE516DE7A48934';

/** A deterministic clock + timer harness. */
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

/** A cordis-style ctx stub covering the services v2 may look up. */
function makeCtx() {
    const hooks = {};
    const effects = [];
    const logs = [];
    const routes = [];
    const commands = [];
    const ctx = {
        on(event, listener, options) {
            hooks[event] = hooks[event] ?? [];
            const disposer = () => {
                const index = hooks[event].indexOf(listener);
                if (index !== -1)
                    hooks[event].splice(index, 1);
            };
            (options?.prepend ? hooks[event].unshift(listener) : hooks[event].push(listener));
            return disposer;
        },
        effect(fn, label) {
            const cleanup = fn();
            effects.push({ label, cleanup });
            return () => {
                if (typeof cleanup === 'function')
                    cleanup();
            };
        },
        get(service) {
            if (service === 'llm')
                return ctx.llm;
            if (service === 'webServer')
                return ctx.webServer;
            if (service === 'httpServer')
                return undefined;
            if (service === 'systemPrompt')
                return ctx.systemPromptImpl;
            if (service === 'commands')
                return ctx.commands;
            if (service === 'tokenMeter')
                return ctx.tokenMeter;
            if (service === 'connection')
                return ctx.connection;
            return undefined;
        },
        logger: (n) => ({
            info: (message) => logs.push(`[${n}] ${message}`),
            warn: (message) => logs.push(`[${n}] WARN ${message}`),
        }),
        waterfalls: hooks,
        announceService(service) {
            for (const listener of [...(hooks['internal/service'] ?? [])])
                listener(service);
        },
        dispose() {
            for (const effect of [...effects]) {
                if (typeof effect.cleanup === 'function')
                    effect.cleanup();
            }
        },
    };
    return { ctx, hooks, effects, logs, routes, commands };
}

function fakeRes() {
    return {
        status: null,
        headers: null,
        body: null,
        writeHead(status, headers) {
            this.status = status;
            this.headers = headers;
        },
        end(body) {
            this.body = body;
        },
    };
}

function passthroughSource(chunks = [{ type: 'text-delta', text: 'x' }, { type: 'finish', reason: { kind: 'stop' } }]) {
    return (async function* () {
        for (const chunk of chunks)
            yield chunk;
    })();
}

// ---- apply() wiring -------------------------------------------------------

test('apply: a host with no optional services degrades non-fatally', () => {
    const { ctx, hooks, logs } = makeCtx();
    assert.doesNotThrow(() => apply(ctx, { enabled: true, maxConcurrent: 2, systemPrompt: true }));
    assert.equal(hooks['llm/stream'].length, 1, 'the stream gate is still registered');
    assert.equal(hooks['agent/request-error'].length, 1);
    assert.equal(hooks['internal/service'].length, 1);
    assert.ok(logs.some((line) => line.includes('systemPrompt service unavailable')));
    ctx.dispose();
    assert.equal(hooks['llm/stream'].length, 0, 'disposal removes the listeners again');
    assert.equal(hooks['agent/request-error'].length, 0);
});

test('apply: one listener, one prompt section, one web route, one command', () => {
    const { ctx, hooks, routes, commands } = makeCtx();
    const sections = [];
    ctx.systemPromptImpl = { section: (spec) => { sections.push(spec); } };
    ctx.webServer = { register: (route) => { routes.push(route); return route; } };
    ctx.commands = { register: (definition) => { commands.push(definition); } };
    apply(ctx, { enabled: true, maxConcurrent: 3, systemPrompt: true, sectionOrder: 42 });
    assert.equal(hooks['llm/stream'].length, 1, 'exactly one llm/stream listener');
    assert.equal(sections.length, 1, 'exactly one prompt section');
    assert.equal(sections[0].order, 42);
    assert.equal(routes.length, 1, 'exactly one web route');
    assert.equal(routes[0].kind, 'exact');
    assert.equal(routes[0].path, '/plugins/dsh-llm-rate-limit/state');
    assert.equal(commands.length, 1, 'exactly one command');
    assert.equal(commands[0].name, 'rate-limit');
    assert.equal(typeof commands[0].handler, 'function');
    ctx.dispose();
    assert.equal(hooks['llm/stream'].length, 0, 'disposal detaches the listener');
    assert.equal(hooks['agent/request-error'].length, 0);
    assert.equal(hooks['internal/service'].length, 0);
});

test('apply: the state route is GET-only and the snapshot is a v1 superset', async () => {
    const { ctx, routes } = makeCtx();
    ctx.webServer = { register: (route) => { routes.push(route); return route; } };
    apply(ctx, { enabled: true, maxConcurrent: 5, systemPrompt: false });
    const route = routes[0];

    const post = fakeRes();
    await route.handler({ method: 'POST' }, post);
    assert.equal(post.status, 405);
    assert.equal(post.headers.allow, 'GET');

    const get = fakeRes();
    await route.handler({ method: 'GET' }, get);
    assert.equal(get.status, 200);
    assert.equal(get.headers['cache-control'], 'no-store');
    const snapshot = JSON.parse(get.body);
    assert.equal(snapshot.enabled, true);
    assert.equal(snapshot.version, '2.0.0');
    for (const key of ['enabled', 'providers', 'maxConcurrent', 'maxWaitMs', 'cooldownBaseMs', 'cooldownMaxMs']) {
        assert.ok(Object.hasOwn(snapshot.config, key), `config.${key} kept`);
    }
    assert.equal(Object.keys(snapshot.config).length, 30, 'all 30 resolved keys are exposed');
    for (const key of ['rateLimit429', 'success', 'error', 'aborted', 'timeout'])
        assert.equal(typeof snapshot.stats[key], 'number', `stats.${key} kept`);
    for (const key of ['provider', 'active', 'queued', 'queuedPeak', 'waits', 'waitTimeouts', 'cooldownRemainingMs', 'last429At'])
        assert.ok(Object.hasOwn(snapshot.limiters[0], key), `limiters[0].${key} kept`);
    assert.equal(snapshot.limiters[0].provider, '*');
    assert.equal(snapshot.limiters[0].effectiveConcurrency, 5);
    assert.equal(snapshot.limiters[0].classes, null, 'classes is null while fairness is off');
    assert.doesNotThrow(() => JSON.stringify(snapshot));
    ctx.dispose();
});

test('apply: the /rate-limit command report is deterministic and ASCII-only', () => {
    const { ctx, commands } = makeCtx();
    ctx.commands = { register: (definition) => { commands.push(definition); } };
    apply(ctx, { enabled: true, systemPrompt: false });
    const result = commands[0].handler({ rawInput: '', signal: new AbortController().signal });
    assert.equal(result.kind, 'success');
    assert.match(result.text, /^dsh-llm-rate-limit v2\.0\.0 - 1 route\(s\), up /);
    assert.match(result.text, /route "\*": in-flight 0\/4/);
    assert.match(result.text, /config: rpmLimit=0 tpmLimit=0 maxConcurrent=4/);
    assert.ok(/^[\x20-\x7E\n]*$/.test(result.text), 'report is ASCII-only');
    // §5.7 C: an unknown route name filters nothing, so the command falls back
    // to the full report rather than printing "0 route(s)".
    const unknown = commands[0].handler({ rawInput: 'deepseek-official' });
    assert.equal(unknown.kind, 'success');
    assert.match(unknown.text, /^dsh-llm-rate-limit v2\.0\.0 - 1 route\(s\), up /, 'unknown input reports every route');
    assert.match(unknown.text, /route "\*": in-flight 0\/4/);
    // A route name that does exist is honoured as a filter.
    const known = commands[0].handler({ rawInput: '*' });
    assert.equal(known.kind, 'success');
    assert.match(known.text, /^dsh-llm-rate-limit v2\.0\.0 - 1 route\(s\), up /);
    assert.match(known.text, /route "\*": in-flight 0\/4/);
    const second = commands[0].handler({ rawInput: '' });
    assert.equal(second.text.split('\n')[0].startsWith('dsh-llm-rate-limit v2.0.0 - 1 route(s)'), true);
    ctx.dispose();
});

test('apply: services announced after apply() are picked up', () => {
    const { ctx, hooks, routes, commands } = makeCtx();
    apply(ctx, { enabled: true, systemPrompt: false });
    assert.equal(routes.length, 0, 'no webServer yet');
    assert.equal(commands.length, 0, 'no commands service yet');
    ctx.webServer = { register: (route) => { routes.push(route); return route; } };
    ctx.commands = { register: (definition) => { commands.push(definition); } };
    ctx.announceService('webServer');
    ctx.announceService('commands');
    assert.equal(routes.length, 1);
    assert.equal(commands.length, 1);
    assert.equal(hooks['llm/stream'].length, 1, 'the llm announcement does not double-register');
    ctx.announceService('llm');
    assert.equal(hooks['llm/stream'].length, 1);
    ctx.dispose();
});

// ---- gateStream outcomes --------------------------------------------------

test('gateStream: chunks pass through unchanged and settle exactly once', async () => {
    const limiter = new ProviderLimiter('t', { maxConcurrent: 2, maxWaitMs: 500, cooldownBaseMs: 100, cooldownMaxMs: 1_000 });
    const chunks = [
        { type: 'text-delta', text: 'a' },
        { type: 'usage', usage: { inputTokens: 3, outputTokens: 4 } },
        { type: 'finish', reason: { kind: 'stop' } },
    ];
    const settled = [];
    const seen = [];
    for await (const chunk of gateStream(passthroughSource(chunks), limiter, undefined, (kind, details) => settled.push([kind, details])))
        seen.push(chunk);
    assert.deepEqual(seen, chunks, 'never filtered, injected or mutated');
    assert.equal(settled.length, 1, 'settled exactly once');
    assert.equal(settled[0][0], 'success');
    assert.equal(settled[0][1].usage, 7, 'the usage chunk is forwarded for TPM settlement');
    assert.equal(limiter.active, 0);
    limiter.dispose();
});

test('gateStream: terminal error/aborted finishes settle with the right kind', async () => {
    const limiter = new ProviderLimiter('t', { maxConcurrent: 2, maxWaitMs: 500, cooldownBaseMs: 100, cooldownMaxMs: 1_000 });
    const failure = { code: 'RATE_LIMIT', message: '429', status: 429 };
    const errorKinds = [];
    for await (const _ of gateStream(
        passthroughSource([{ type: 'finish', reason: { kind: 'error', failure } }]),
        limiter,
        undefined,
        (kind, details) => errorKinds.push([kind, details.failure?.code]),
    )) { /* consume */ }
    assert.deepEqual(errorKinds, [['error', 'RATE_LIMIT']]);

    const abortedKinds = [];
    for await (const _ of gateStream(
        passthroughSource([{ type: 'finish', reason: { kind: 'aborted', failure: { code: 'ABORTED' } } }]),
        limiter,
        undefined,
        (kind) => abortedKinds.push(kind),
    )) { /* consume */ }
    assert.deepEqual(abortedKinds, ['aborted']);
    assert.equal(limiter.active, 0);
    limiter.dispose();
});

test('gateStream: a queued request that exhausts maxWaitMs yields the v1 timeout failure', async () => {
    const limiter = new ProviderLimiter('t', { maxConcurrent: 1, maxWaitMs: 20, cooldownBaseMs: 100, cooldownMaxMs: 1_000 });
    const held = await limiter.gate({});
    const settled = [];
    const chunks = [];
    for await (const chunk of gateStream(passthroughSource(), limiter, undefined, (kind, details) => settled.push([kind, details])))
        chunks.push(chunk);
    assert.equal(chunks.length, 1, 'one synthetic terminal chunk');
    assert.deepEqual(chunks[0], {
        type: 'finish',
        reason: {
            kind: 'error',
            failure: {
                code: 'RATE_LIMIT_GATE_TIMEOUT',
                message: 'dsh-llm-rate-limit: queued wait exceeded maxWaitMs without a free slot',
            },
        },
    });
    assert.equal(settled[0][0], 'timeout');
    assert.equal(settled[0][1].shedCode, 'RATE_LIMIT_GATE_TIMEOUT');
    assert.equal(limiter.timeoutCount, 1);
    limiter.settle(held.ticket, { kind: 'success' });
    assert.equal(limiter.active, 0);
    limiter.dispose();
});

test('gateStream: queue-full and budget-exceeded shed immediately, never queued', async () => {
    const limiter = new ProviderLimiter('t', {
        maxConcurrent: 1,
        maxWaitMs: 60_000,
        cooldownBaseMs: 100,
        cooldownMaxMs: 1_000,
        queueLimit: 1,
        tpmLimit: 100,
    }, fakeClock().deps);
    const held = await limiter.gate({});
    const queued = limiter.gate({});
    const full = await limiter.gate({});
    assert.deepEqual({ ok: full.ok, reason: full.reason, code: full.code, waitMs: full.waitMs }, {
        ok: false,
        reason: 'queue-full',
        code: 'RATE_LIMIT_QUEUE_FULL',
        waitMs: 0,
    });
    assert.match(full.message, /queue full \(1\/1 waiters on route "t"\)/);
    assert.equal(limiter.queued, 1, 'the shed request never joined the queue');

    limiter.settle(held.ticket, { kind: 'success' });
    const granted = await queued;
    assert.equal(granted.ok, true);
    limiter.settle(granted.ticket, { kind: 'success' });

    const over = await limiter.gate({ estimatedTokens: 500 });
    assert.equal(over.reason, 'budget-exceeded');
    assert.equal(over.code, 'RATE_LIMIT_BUDGET_EXCEEDED');
    assert.match(over.message, /request needs 500 tokens but the route's per-minute budget is 100/);
    assert.equal(limiter.queued, 0, 'a request that can never fit is never queued');
    assert.equal(limiter.sheds.queueFull, 1);
    assert.equal(limiter.sheds.budgetExceeded, 1);
    limiter.dispose();
});

test('AIMD wiring: settle feeds increase on success and decrease on a throttle', async () => {
    const clock = fakeClock();
    const limiter = new ProviderLimiter('aimd', {
        maxConcurrent: 4,
        maxWaitMs: 60_000,
        adaptiveConcurrency: true,
        minConcurrent: 1,
        aimdIncreaseEvery: 2,
        aimdDecreaseFactor: 0.5,
    }, clock.deps);
    assert.equal(limiter.effectiveConcurrency(), 2, 'starts at ceil(maxConcurrent / 2)');
    const first = await limiter.gate({});
    limiter.settle(first.ticket, { kind: 'success' });
    const second = await limiter.gate({});
    limiter.settle(second.ticket, { kind: 'success' });
    assert.equal(limiter.effectiveConcurrency(), 3, 'two clean settles ⇒ +1');
    assert.equal(limiter.aimd.increases, 1);
    const third = await limiter.gate({});
    limiter.settle(third.ticket, { kind: 'error', failure: { code: 'RATE_LIMIT', status: 429 } });
    assert.equal(limiter.effectiveConcurrency(), 1, 'floor(3 × 0.5), clamped to the floor');
    assert.equal(limiter.aimd.decreases, 1);
    const fourth = await limiter.gate({});
    limiter.settle(fourth.ticket, { kind: 'error', failure: { status: 503 } });
    assert.equal(limiter.effectiveConcurrency(), 1, 'never below minConcurrent');
    const fifth = await limiter.gate({});
    limiter.settle(fifth.ticket, { kind: 'error', failure: { code: 'AUTH', status: 401 } });
    assert.equal(limiter.effectiveConcurrency(), 1, 'a non-throttle error does not move the bound');
    const sixth = await limiter.gate({});
    limiter.settle(sixth.ticket, { kind: 'aborted' });
    assert.equal(limiter.effectiveConcurrency(), 1, 'a cancellation does not move the bound');
    assert.equal(limiter.active, 0);
    limiter.dispose();
});

test('gateStream: an aborted wait is reported as a cancellation, not a throttle error', async () => {
    const limiter = new ProviderLimiter('t', { maxConcurrent: 1, maxWaitMs: 60_000, cooldownBaseMs: 100, cooldownMaxMs: 1_000 });
    const held = await limiter.gate({});
    const controller = new AbortController();
    const settled = [];
    const chunks = [];
    const gate = gateStream(passthroughSource(), limiter, controller.signal, (kind, details) => settled.push([kind, details]));
    const consuming = (async () => {
        for await (const chunk of gate)
            chunks.push(chunk);
    })();
    await Promise.resolve();
    controller.abort();
    await consuming;
    assert.deepEqual(chunks[0], {
        type: 'finish',
        reason: {
            kind: 'aborted',
            failure: {
                code: 'RATE_LIMIT_GATE_ABORTED',
                message: 'dsh-llm-rate-limit: queued request cancelled before a slot was granted',
            },
        },
    });
    assert.equal(settled[0][0], 'aborted');
    assert.equal(limiter.sheds.aborted, 1);
    limiter.settle(held.ticket, { kind: 'success' });
    limiter.dispose();
});

test('gateStream: a consumer that abandons the stream releases the slot', async () => {
    const limiter = new ProviderLimiter('t', { maxConcurrent: 1, maxWaitMs: 500, cooldownBaseMs: 100, cooldownMaxMs: 1_000 });
    const source = (async function* () {
        yield { type: 'text-delta', text: 'a' };
        yield { type: 'text-delta', text: 'b' };
        yield { type: 'finish', reason: { kind: 'stop' } };
    })();
    const settled = [];
    for await (const _ of gateStream(source, limiter, undefined, (kind) => settled.push(kind)))
        break;
    assert.equal(limiter.active, 0, 'the slot is returned even though the terminal chunk never arrived');
    // T5-4: an abandoned stream (no terminal chunk, no abort) settles as
    // `aborted`, not `success`, so it is not mistaken for a clean settle.
    assert.deepEqual(settled, ['aborted']);
    limiter.dispose();
});

test('gateStream: an abandoned stream does not inflate the AIMD bound (T5-4)', async () => {
    const limiter = new ProviderLimiter('t54a', {
        maxConcurrent: 4,
        minConcurrent: 1,
        adaptiveConcurrency: true,
        aimdInitialConcurrent: 2,
        aimdIncreaseEvery: 1, // every clean settle would grow the bound by one
        maxWaitMs: 500,
        cooldownBaseMs: 100,
        cooldownMaxMs: 1_000,
        cooldownHysteresisMs: 0,
    });
    assert.equal(limiter.effectiveConcurrency(), 2, 'the AIMD bound starts at aimdInitialConcurrent');
    const source = (async function* () {
        yield { type: 'text-delta', text: 'a' };
        yield { type: 'text-delta', text: 'b' };
    })();
    const settled = [];
    for await (const _ of gateStream(source, limiter, undefined, (kind) => settled.push(kind)))
        break;
    assert.deepEqual(settled, ['aborted']);
    assert.equal(limiter.effectiveConcurrency(), 2, 'an abandoned stream must not grow the AIMD bound (the T5-4 leak that let 429s through)');
    assert.equal(limiter.aimd.increases, 0, 'no additive increase is booked for an abandoned stream');
    limiter.dispose();
});

test('settle: an aborted ticket does not reset the cooldown ladder where a success would (T5-4)', () => {
    const limiter = new ProviderLimiter('t54b', {
        maxConcurrent: 4,
        minConcurrent: 1,
        adaptiveConcurrency: false,
        maxWaitMs: 60_000,
        cooldownBaseMs: 1_000,
        cooldownMaxMs: 8_000,
        cooldownHysteresisMs: 0, // a success settle zeroes the ladder immediately
    });
    limiter.cooldownEscalations = 3;
    assert.equal(limiter.cooldownEscalations, 3);
    limiter.settle({ id: 1, settled: false, tpmEntry: null }, { kind: 'aborted', usage: null, failure: null });
    assert.equal(limiter.cooldownEscalations, 3, 'an aborted settle leaves the escalation ladder untouched');
    // Control: the old T5-4 behaviour settled an abandoned stream as `success`,
    // which WOULD zero the ladder at hysteresis 0.
    limiter.cooldownEscalations = 3;
    limiter.settle({ id: 2, settled: false, tpmEntry: null }, { kind: 'success', usage: null, failure: null });
    assert.equal(limiter.cooldownEscalations, 0, 'a success settle resets the ladder at hysteresis 0 (what an abandoned stream wrongly did before the fix)');
    limiter.dispose();
});

test('gateStream: a throwing source is re-thrown unchanged after settling', async () => {
    const limiter = new ProviderLimiter('t', { maxConcurrent: 1, maxWaitMs: 500, cooldownBaseMs: 100, cooldownMaxMs: 1_000 });
    const boom = new Error('provider exploded');
    const source = (async function* () {
        yield { type: 'text-delta', text: 'a' };
        throw boom;
    })();
    const settled = [];
    await assert.rejects(async () => {
        for await (const _ of gateStream(source, limiter, undefined, (kind) => settled.push(kind))) { /* consume */ }
    }, (error) => error === boom);
    assert.deepEqual(settled, ['error']);
    assert.equal(limiter.active, 0);
    limiter.dispose();
});

test('gateStream: an abort while in flight settles immediately', async () => {
    const limiter = new ProviderLimiter('t', { maxConcurrent: 1, maxWaitMs: 500, cooldownBaseMs: 100, cooldownMaxMs: 1_000 });
    const controller = new AbortController();
    const settled = [];
    const source = (async function* () {
        yield { type: 'text-delta', text: 'a' };
        await new Promise(() => { /* never resolves */ });
    })();
    const gate = gateStream(source, limiter, controller.signal, (kind) => settled.push(kind));
    const first = await gate.next();
    assert.equal(first.value.type, 'text-delta');
    assert.equal(limiter.active, 1);
    controller.abort();
    assert.equal(limiter.active, 0, 'the slot is returned without waiting for the consumer to unwind');
    assert.deepEqual(settled, ['aborted']);
    await gate.return();
    limiter.dispose();
});

// ---- no-leak, timers, disposal -------------------------------------------

test('timer anchor: exactly one refd handle while a waiter is queued, none when idle', async () => {
    const clock = fakeClock();
    const limiter = new ProviderLimiter('anchor', { maxConcurrent: 1, maxWaitMs: 60_000 }, clock.deps);
    assert.equal(limiter.deadlineTimer, null, 'no anchor before any waiter');
    const held = await limiter.gate({});
    assert.equal(limiter.queued, 0);
    assert.equal(limiter.deadlineTimer, null, 'an in-flight request needs no anchor');

    const queued = limiter.gate({});
    assert.equal(limiter.queued, 1);
    assert.ok(limiter.deadlineTimer !== null, 'a queued waiter arms the deadline anchor');
    assert.equal(limiter.deadlineTimer.refd, true, 'the deadline anchor is refd');
    assert.equal(limiter.deadlineTimer.unrefd, true, 'it is unrefd first, then refd');
    const refd = clock.handles.filter((handle) => handle.refd && !handle.cleared);
    assert.equal(refd.length, 1, 'exactly one refd handle in the whole limiter');
    for (const handle of clock.handles) {
        if (handle === limiter.deadlineTimer || handle.cleared || handle.fired)
            continue;
        assert.equal(handle.refd, false, 'every other live timer stays unrefd');
    }

    limiter.settle(held.ticket, { kind: 'success' });
    assert.equal(limiter.queued, 0);
    assert.equal(limiter.deadlineTimer, null, 'the anchor is released when the queue empties');
    assert.equal((await queued).ok, true);
    limiter.dispose();
    assert.equal(limiter.active, 0);
    assert.equal(limiter.queued, 0);
});

test('dispose: refuses every waiter, clears every timer, and nothing resolves afterwards', async () => {
    const clock = fakeClock();
    const limiter = new ProviderLimiter('dispose', {
        maxConcurrent: 2,
        maxWaitMs: 60_000,
        cooldownBaseMs: 100,
        cooldownMaxMs: 1_000,
        cooldownHysteresisMs: 500,
        releaseSpreadMs: 25,
        fairness: true,
    }, clock.deps);
    const held = await limiter.gate({});
    limiter.observe429();
    limiter.resetEscalation();
    const waiting = limiter.gate({});
    const waiting2 = limiter.gate({});
    assert.equal(limiter.queued, 2);
    assert.ok(limiter.cooldownTimer !== null, 'a cooldown timer exists');
    assert.ok(limiter.hysteresisTimer !== null, 'a hysteresis timer exists');
    assert.ok(limiter.pumpTimer !== null, 'a pump wake timer exists');
    assert.ok(limiter.deadlineTimer !== null, 'the deadline anchor exists');

    limiter.dispose();
    assert.equal(limiter.queued, 0, 'no waiter remains queued');
    assert.equal(limiter.active, 0, 'no slot remains held');
    assert.equal(limiter.deadlineTimer, null);
    assert.equal(limiter.pumpTimer, null);
    assert.equal(limiter.cooldownTimer, null);
    assert.equal(limiter.hysteresisTimer, null);
    assert.equal(limiter.queue.size, 0);
    assert.equal(limiter.queue.classes?.size ?? 0, 0, 'no per-session records survive');
    for (const handle of clock.handles)
        assert.equal(handle.cleared || handle.fired, true, 'every created timer was cleared or already fired');

    const first = await waiting;
    const second = await waiting2;
    for (const outcome of [first, second]) {
        assert.equal(outcome.ok, false);
        assert.equal(outcome.reason, 'disposed');
        assert.equal(outcome.code, 'RATE_LIMIT_GATE_DISPOSED');
    }
    assert.equal(limiter.sheds.disposed, 2);

    const after = await limiter.gate({});
    assert.equal(after.reason, 'disposed', 'new requests are refused immediately');
    assert.equal(clock.runDue(), 0, 'no timer can fire into the next generation');

    // Settling the force-settled ticket is idempotent and never double-decrements.
    limiter.settle(held.ticket, { kind: 'aborted' });
    assert.equal(limiter.active, 0);
    limiter.dispose();
    assert.equal(limiter.active, 0);
});

test('no slot leak across 1000+ release/abort/timeout cycles', async () => {
    const clock = fakeClock();
    const limiter = new ProviderLimiter('leak', {
        maxConcurrent: 2,
        maxWaitMs: 50,
        cooldownBaseMs: 100,
        cooldownMaxMs: 1_000,
        rpmLimit: 4_000,
        rpmWindowMs: 1_000,
        tpmLimit: 100_000,
        tpmWindowMs: 1_000,
        tpmOutputReserveTokens: 10,
        adaptiveConcurrency: false,
        fairness: true,
        agingThresholdMs: 1_000,
        queueLimit: 64,
        releaseSpreadMs: 0,
        cooldownJitter: 'equal',
        cooldownHysteresisMs: 200,
    }, clock.deps);

    let cycles = 0;
    for (let i = 0; i < 400; i += 1) {
        clock.advance(1);
        // 1: acquire / queue / release.
        const a = await limiter.gate({ sessionId: `s${i % 7}`, estimatedTokens: 5 });
        const b = await limiter.gate({ sessionId: `s${i % 5}`, estimatedTokens: 5 });
        assert.equal(a.ok && b.ok, true);
        const queued = limiter.gate({ sessionId: `s${i % 3}`, estimatedTokens: 5 });
        assert.equal(limiter.queued, 1);
        limiter.settle(a.ticket, { kind: 'success', usage: 3 });
        limiter.settle(b.ticket, { kind: 'success', usage: 3 });
        const c = await queued;
        assert.equal(c.ok, true);
        limiter.settle(c.ticket, { kind: 'error', failure: { code: 'RATE_LIMIT', status: 429 } });
        cycles += 1;

        // 2: abort while queued.
        const d = await limiter.gate({ sessionId: `s${i % 7}`, estimatedTokens: 5 });
        const e = await limiter.gate({ sessionId: `s${i % 5}`, estimatedTokens: 5 });
        const controller = new AbortController();
        const aborted = limiter.gate({ signal: controller.signal, sessionId: `s${i % 3}` });
        controller.abort();
        assert.equal((await aborted).reason, 'aborted');
        limiter.settle(d.ticket, { kind: 'success', usage: 2 });
        limiter.settle(e.ticket, { kind: 'aborted' });
        cycles += 1;

        // 3: wait-timeout while queued.
        const f = await limiter.gate({ sessionId: `s${i % 11}`, estimatedTokens: 5 });
        const g = await limiter.gate({ sessionId: `s${i % 13}`, estimatedTokens: 5 });
        const timedOut = limiter.gate({ sessionId: `s${i % 17}`, estimatedTokens: 5 });
        clock.advance(60);
        limiter.expireWaiters();
        assert.equal((await timedOut).reason, 'wait-timeout');
        limiter.settle(f.ticket, { kind: 'success', usage: 1 });
        limiter.settle(g.ticket, { kind: 'aborted' });
        cycles += 1;

        assert.equal(limiter.active, 0, `iteration ${i}: active must return to zero`);
        assert.equal(limiter.queued, 0, `iteration ${i}: no waiter may remain queued`);
    }

    assert.ok(cycles >= 1_000, `ran ${cycles} cycles`);
    assert.equal(limiter.active, 0);
    assert.equal(limiter.queued, 0);
    assert.equal(limiter.queue.size, 0);
    assert.equal(limiter.queue.classes.size, 0, 'no per-session key lists leak');
    assert.equal(limiter.tickets.size, 0, 'no ticket leaks');
    assert.equal(limiter.deadlineTimer, null);
    assert.equal(limiter.pumpTimer, null);
    assert.ok(limiter.rpm.stamps.length - limiter.rpm.head <= 4_000, 'the RPM log stays bounded by the limit');
    assert.equal(limiter.tpm.reserved, 0, 'every TPM reservation was settled');
    limiter.dispose();
});

// ---- spec §11.4 evidence --------------------------------------------------

test('spec evidence: the v1 test files are byte-identical', () => {
    const hash = (relative) => createHash('sha256')
        .update(readFileSync(new URL(relative, import.meta.url)))
        .digest('hex')
        .toUpperCase();
    assert.equal(hash('./limiter.test.js'), V1_LIMITER_TEST_SHA256, 'limiter.test.js unchanged');
    assert.equal(hash('./index.test.js'), V1_INDEX_TEST_SHA256, 'index.test.js unchanged');
});

test('spec evidence: package.json is 2.0.0 and declares no runtime dependencies', () => {
    const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
    assert.equal(pkg.version, '2.0.0');
    assert.equal(VERSION, pkg.version, 'lib/index.js VERSION mirrors package.json');
    assert.equal(Object.hasOwn(pkg, 'dependencies'), false, 'no runtime dependency set');
    for (const [peer, meta] of Object.entries(pkg.peerDependenciesMeta ?? {}))
        assert.equal(meta.optional, true, `${peer} must stay an optional peer`);
    assert.equal(name, 'dsh-llm-rate-limit');
    assert.equal(pkg.name, 'dsh-llm-rate-limit');
});

test('spec evidence: cordis.patch.yml documents every new key and the shipped values', () => {
    const patch = readFileSync(new URL('../cordis.patch.yml', import.meta.url), 'utf8');
    const shipped = {
        rpmLimit: 300,
        rpmWindowMs: 60_000,
        tpmLimit: 2_000_000,
        tpmWindowMs: 60_000,
        tpmOutputReserveTokens: 8_192,
        adaptiveConcurrency: true,
        minConcurrent: 1,
        aimdIncreaseEvery: 20,
        aimdDecreaseFactor: 0.5,
        aimdTimeoutDecrease: true,
        fairness: true,
        interactiveWeight: 6,
        backgroundWeight: 2,
        utilityWeight: 1,
        agingThresholdMs: 30_000,
        queueLimit: 256,
        releaseSpreadMs: 200,
        cooldownJitter: 'equal',
        cooldownHysteresisMs: 60_000,
        overloadCooldownMultiplier: 2,
    };
    // Every new key appears somewhere in the file (active value or comment).
    for (const key of V2_KEYS)
        assert.ok(new RegExp(`^\\s*#?\\s*${key}\\b`, 'm').test(patch), `${key} is documented`);
    // The active block carries the spec's shipped safety budgets verbatim.
    for (const [key, value] of Object.entries(shipped)) {
        const match = patch.match(new RegExp(`^\\s{8}${key}:\\s*(\\S+)`, 'm'));
        assert.ok(match, `${key} is set in the config block`);
        assert.equal(match[1], String(value), `${key} shipped value`);
    }
    // The two keys the shipped block leaves unset are documented with their defaults.
    assert.match(patch, /^\s*#?\s*tpmLogMaxEntries: 4096$/m);
    assert.match(patch, /aimdInitialConcurrent: null/);
});

test('spec evidence: README documents the new keys and the remaining limitations', () => {
    const readme = readFileSync(new URL('../README.md', import.meta.url), 'utf8');
    for (const key of V2_KEYS)
        assert.ok(readme.includes(`\`${key}\``), `README documents ${key}`);
    for (const limitation of ['In-memory only', 'No cross-process coordination', 'No response-header parsing'])
        assert.ok(readme.includes(limitation), `README states the limitation: ${limitation}`);
});

test('RPM budget: admission is charged at grant and the next request queues', async () => {
    const clock = fakeClock();
    const limiter = new ProviderLimiter('rpm', {
        maxConcurrent: 4,
        maxWaitMs: 5_000,
        rpmLimit: 2,
        rpmWindowMs: 1_000,
    }, clock.deps);
    const first = await limiter.gate({});
    const second = await limiter.gate({});
    assert.equal(first.ok && second.ok, true);
    assert.equal(limiter.rpm.used(clock.now), 2, 'charged at grant');
    const third = limiter.gate({});
    assert.equal(limiter.active, 2);
    assert.equal(limiter.queued, 1, 'the third admission is rejected until the window slides');
    limiter.settle(first.ticket, { kind: 'success' });
    limiter.settle(second.ticket, { kind: 'success' });
    assert.equal(limiter.active, 0);
    assert.equal(limiter.queued, 1, 'a free slot is not enough: the RPM window is the blocker');
    assert.equal(limiter.waitsByReason.rpm, 0);
    clock.advance(1_000);
    limiter.pump();
    const outcome = await third;
    assert.equal(outcome.ok, true, 'admitted once the window slid');
    assert.equal(limiter.waitsByReason.rpm, 1, 'the wait reason is recorded at grant');
    limiter.settle(outcome.ticket, { kind: 'success' });
    assert.equal(limiter.active, 0);
    limiter.dispose();
});

test('TPM budget: a reservation rejects the next request until it settles or expires', async () => {
    const clock = fakeClock();
    const limiter = new ProviderLimiter('tpm', {
        maxConcurrent: 4,
        maxWaitMs: 5_000,
        tpmLimit: 100,
        tpmWindowMs: 1_000,
    }, clock.deps);
    const first = await limiter.gate({ estimatedTokens: 60 });
    assert.equal(first.ok, true);
    assert.equal(limiter.tpm.used(clock.now), 60);
    const second = limiter.gate({ estimatedTokens: 60 });
    assert.equal(limiter.queued, 1, '60 + 60 exceeds the budget ⇒ queued');
    assert.equal(second instanceof Promise, true);
    limiter.settle(first.ticket, { kind: 'success', usage: 20 });
    const outcome = await second;
    assert.equal(outcome.ok, true, 'the observed usage frees the headroom');
    assert.equal(limiter.tpm.used(clock.now), 80);
    assert.equal(limiter.waitsByReason.tpm, 1);

    const third = limiter.gate({ estimatedTokens: 50 });
    assert.equal(limiter.queued, 1, '80 + 50 exceeds the budget again');
    clock.advance(1_000);
    limiter.pump();
    const thirdOutcome = await third;
    assert.equal(thirdOutcome.ok, true, 'expired reservations free the budget');
    limiter.settle(outcome.ticket, { kind: 'success' });
    limiter.settle(thirdOutcome.ticket, { kind: 'success' });
    assert.equal(limiter.active, 0);
    limiter.dispose();
});

test('createLimiters: the "*" fallback and per-provider routes are preserved', () => {
    const map = createLimiters(['a'], { maxConcurrent: 1, maxWaitMs: 100 });
    assert.equal(map.size, 2);
    assert.equal(map.get('a').provider, 'a');
    assert.equal(map.get('*').provider, '*');
    const all = createLimiters([], { maxConcurrent: 1, maxWaitMs: 100 });
    assert.equal(all.size, 1);
    for (const limiter of [...map.values(), ...all.values()])
        limiter.dispose();
});
