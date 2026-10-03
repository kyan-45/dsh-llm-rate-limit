import assert from 'node:assert/strict';
import { apply, name } from '../lib/index.js';

// ---- minimal cordis-style ctx stub ------------------------------------------
function makeCtx(config) {
    const hooks = {};
    let nextServiceId = 0;
    const effects = [];
    const ctx = {
        on(name, listener, options) {
            hooks[name] = hooks[name] ?? [];
            const disposer = () => {
                const i = hooks[name].indexOf(listener);
                if (i !== -1)
                    hooks[name].splice(i, 1);
            };
            (options?.prepend ? hooks[name].unshift(listener) : hooks[name].push(listener));
            return disposer;
        },
        effect(fn, label) {
            const cleanup = fn();
            effects.push({ label, cleanup });
            return () => {
                const i = effects.findIndex((e) => e.label === label);
                if (i !== -1) {
                    cleanup?.();
                    effects.splice(i, 1);
                }
            };
        },
        get(svc) {
            if (svc === 'llm')
                return ctx.llm;
            if (svc === 'webServer')
                return ctx.webServer;
            if (svc === 'systemPrompt')
                return ctx.systemPromptImpl;
            return undefined;
        },
        logger: (n) => ({
            info: (msg) => logs.push(`[${n}] ${msg}`),
            warn: (msg) => logs.push(`[${n}] WARN ${msg}`),
        }),
        llm: undefined,
        webServer: undefined,
        systemPromptImpl: undefined,
        systemPrompt: undefined,
        systemPrompt: {
            sections: [],
            section(spec) {
                this.sections.push(spec);
            },
        },
        announceService(name) {
            const listeners = hooks['internal/service'] ?? [];
            for (const l of listeners)
                l(name);
        },
        waterfalls: hooks,
        dispose() {
            for (const e of [...effects])
                if (typeof e.cleanup === 'function')
                    e.cleanup();
        },
    };
    return ctx;
}

const logs = [];

// 1: plain apply with a config — registers llm/stream and agent/request-error hooks.
{
    logs.length = 0;
    const ctx = makeCtx();
    // Expose the systemPrompt service through ctx.get, mirroring the host.
    const sp = {
        sections: [],
        section(spec) {
            this.sections.push(spec);
        },
    };
    ctx.systemPromptImpl = sp;
    apply(ctx, {
        enabled: true,
        maxConcurrent: 3,
        maxWaitMs: 1000,
        cooldownBaseMs: 100,
        cooldownMaxMs: 1000,
        systemPrompt: true,
    });
    assert.equal(name, 'dsh-llm-rate-limit');
    assert.ok(ctx.waterfalls['llm/stream'], 'llm/stream listener registered');
    assert.ok(ctx.waterfalls['agent/request-error'], 'agent/request-error listener registered');
    assert.equal(sp.sections.length, 1, 'system prompt section registered via ctx.get');
    ctx.dispose();
    console.log('apply test passed');
}

// 2: the llm/stream listener wraps the downstream stream with the gate.
{
    logs.length = 0;
    const ctx = makeCtx();
    apply(ctx, { enabled: true, maxConcurrent: 1, maxWaitMs: 2000, cooldownBaseMs: 50, cooldownMaxMs: 500, systemPrompt: false });

    const streamListener = ctx.waterfalls['llm/stream'][0];
    // downstream stream: two chunks then a clean finish.
    const downstream = (async function* () {
        yield { type: 'text-delta', text: 'x' };
        yield { type: 'finish', reason: { kind: 'stop' } };
    })();
    let outcomes = [];
    const gated = await streamListener(
        { provider: 'deepseek-official', signal: new AbortController().signal },
        async () => downstream,
    );
    for await (const chunk of gated)
        outcomes.push(chunk.type ?? chunk.reason?.kind);
    assert.deepEqual(outcomes, ['text-delta', 'finish'], 'gate passes chunks through');
    ctx.dispose();
    console.log('llm/stream gate test passed');
}

// 3: an ungoverned route passes through untouched.
{
    logs.length = 0;
    const ctx = makeCtx();
    apply(ctx, { enabled: true, providers: ['deepseek-official'], maxConcurrent: 1, systemPrompt: false });
    const streamListener = ctx.waterfalls['llm/stream'][0];
    let called = 0;
    const passthrough = async (options, next) => {
        called++;
        return next();
    };
    const source = (async function* () { yield { type: 'finish', reason: { kind: 'stop' } }; })();
    const result = await streamListener({ provider: 'other-provider', signal: undefined }, async () => source);
    assert.equal(result === source, true, 'unlisted provider stream returned un-gated');
    ctx.dispose();
    console.log('ungoverned route test passed');
}

// 4: agent/request-error observes a RATE_LIMIT failure and opens a cooldown.
{
    logs.length = 0;
    const ctx = makeCtx();
    apply(ctx, { enabled: true, maxConcurrent: 1, cooldownBaseMs: 100, cooldownMaxMs: 1000, systemPrompt: false });
    const listener = ctx.waterfalls['agent/request-error'][0];
    let downstreamCalls = 0;
    const payload = {
        turn: 1,
        step: 1,
        provider: 'deepseek-official',
        failure: { code: 'RATE_LIMIT', message: '429', providerRetryAfterMs: 250, status: 429 },
        signal: new AbortController().signal,
    };
    const result = await listener(payload, async () => {
        downstreamCalls++;
        return undefined;
    });
    assert.equal(result, undefined, 'passes through to downstream');
    assert.equal(downstreamCalls, 1, 'downstream was called');
    ctx.dispose();
    console.log('429 observation test passed');
}

// 5: non-429 failures are ignored.
{
    logs.length = 0;
    const ctx = makeCtx();
    apply(ctx, { enabled: true, systemPrompt: false });
    const listener = ctx.waterfalls['agent/request-error'][0];
    const payload = {
        turn: 1,
        step: 1,
        provider: 'deepseek-official',
        failure: { code: 'AUTH', message: '401' },
        signal: new AbortController().signal,
    };
    await listener(payload, async () => undefined);
    ctx.dispose();
    console.log('non-429 passthrough test passed');
}

// 6: disabled plugin registers nothing.
{
    logs.length = 0;
    const ctx = makeCtx();
    apply(ctx, { enabled: false });
    assert.equal(ctx.waterfalls['llm/stream'], undefined, 'no stream gate when disabled');
    assert.equal(ctx.waterfalls['agent/request-error'], undefined, 'no 429 hook when disabled');
    assert.ok(ctx.systemPromptImpl === undefined || ctx.systemPromptImpl.sections.length === 0, 'no prompt section when disabled');
    ctx.dispose();
    console.log('disabled test passed');
}

// 7: web routes registered when webServer present.
{
    logs.length = 0;
    const ctx = makeCtx();
    const registeredRoutes = [];
    ctx.webServer = {
        register(route) {
            registeredRoutes.push(route);
            return route;
        },
    };
    apply(ctx, { enabled: true, systemPrompt: false });
    assert.equal(registeredRoutes.length, 1, 'state route registered');
    assert.equal(registeredRoutes[0].path, '/plugins/dsh-llm-rate-limit/state');
    ctx.dispose();
    console.log('web route test passed');
}

console.log('index integration tests: all passed');
