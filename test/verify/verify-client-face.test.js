/**
 * Regression guard for the web/renderer side of dsh-llm-rate-limit.
 *
 * The host boots the client entry with a ctx *proxy*: reading a `ctx.<service>`
 * that the module face did not declare in its `inject` list throws at
 * activation — which strands the entry's fiber in the FAILED state and makes
 * web-boot fail the whole app ("1 entry did not activate").
 *
 * This test loads the real `client/client.js` under a host-like ctx proxy and
 * proves (a) the face declares every service `apply()` touches, (b) `apply()`
 * then runs cleanly and registers both settings slots, and (c) the historical
 * bug (face `inject = ["locale"]` only) would have thrown at activation.
 *
 * Nothing here writes to lib/ or client/ — it only reads client.js and
 * exercises its exports in a sandbox.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { test } from 'node:test';

const root = new URL('../../', import.meta.url);
const readRoot = (relative) => readFileSync(new URL(relative, root), 'utf8');

/**
 * Execute `client.js` inside a minimal browser-like sandbox, capture the
 * `window.__ModuleLoader__.load` registration, run its factory, and return the
 * module face ({ apply, inject, name }). An optional `faceInject` override
 * lets a test simulate a different declaration without editing the file.
 */
function loadClientFace(source, { faceInject } = {}) {
    let registration;
    const fakeRequire = (spec) => {
        // The factory only references these modules at this point; the
        // components are not rendered, so minimal stand-ins are enough.
        if (spec === 'react')
            return { useState: () => [undefined, () => { }], useCallback: (fn) => fn, useEffect: () => { } };
        if (spec === 'react/jsx-runtime')
            return { jsx: (type, props) => ({ type, props }), jsxs: (type, props) => ({ type, props }) };
        throw new Error(`client.js requested an unknown module: ${spec}`);
    };
    const sandbox = {
        window: { __ModuleLoader__: { load(record) { registration = record; } } },
        require: fakeRequire,
    };
    vm.createContext(sandbox);
    vm.runInContext(source, sandbox, { filename: 'client.js' });
    assert.ok(registration, 'client.js must register a factory with window.__ModuleLoader__');
    const face = registration.factory(fakeRequire);
    assert.strictEqual(face.name, 'dsh-llm-rate-limit');
    assert.strictEqual(typeof face.apply, 'function', 'client face must export apply()');
    if (faceInject !== undefined)
        face.inject = faceInject;
    return face;
}

/**
 * Build a host-like ctx proxy: `locale` and `slots` are the only services the
 * client can use, and each one *throws on access* unless it appears in the
 * face `inject` list — the exact activation rule that produced the crash.
 */
function hostLikeCtx(face) {
    const declared = new Set(Array.isArray(face.inject) ? face.inject : []);
    const registered = [];
    const dicts = {};
    const services = {
        // Mirror the host LocaleRuntime surface the client now uses:
        // register(ns, {zh,en}) + bind(ns) + uSES getSnapshot/subscribe.
        locale: {
            register: (ns, dict) => {
                dicts[ns] = dict;
                return () => { };
            },
            bind: (ns) => (key) => dicts[ns]?.en?.[key] ?? key,
            getSnapshot: () => ({ active: 'en', revision: 0 }),
            subscribe: () => () => { },
        },
        slots: {
            inject(_key, install) { install(); },
            register(meta) { registered.push(meta.name); },
        },
    };
    const ctx = new Proxy({}, {
        get(_target, prop) {
            if (prop === 'locale' || prop === 'slots') {
                if (!declared.has(prop))
                    throw new Error(`client ctx: reading undeclared service "${prop}" — declare it in the face inject list`);
                return services[prop];
            }
            // Real cordis effect semantics: the callback runs on activation and
            // its cleanup runs on unload.
            if (prop === 'effect')
                return (fn) => {
                    const cleanup = fn();
                    return () => cleanup?.();
                };
            return undefined;
        },
    });
    return { ctx, registered };
}

/** Strip comments so only real `ctx.<service>` code accesses are scanned. */
function ctxServicesIn(source) {
    const code = source
        .replace(/\/\*[\s\S]*?\*\//g, '')
        .replace(/^\s*\/\/.*$/gm, '');
    return [...new Set([...code.matchAll(/\bctx\.([A-Za-z_$][\w$]*)/g)].map((m) => m[1]))];
}

test('client face declares every ctx service apply() touches', () => {
    const source = readRoot('client/client.js');
    const face = loadClientFace(source);
    const used = ctxServicesIn(source);
    const alwaysAvailable = new Set(['effect']); // base ctx, not an injectable service
    const services = used.filter((name) => !alwaysAvailable.has(name));
    assert.ok(services.length > 0, 'expected client.js to use at least one ctx service');
    for (const service of services)
        assert.ok(face.inject.includes(service), `client.js uses ctx.${service} but face inject=${JSON.stringify(face.inject)} omits it`);
    // The two settings slots are what this panel is for; both must be declared.
    assert.ok(face.inject.includes('slots'), 'settings slot registration requires face inject to include "slots"');
    assert.ok(face.inject.includes('locale'), 'i18n requires face inject to include "locale"');
});

test('client apply() runs cleanly under a host-like ctx proxy and registers both settings slots', () => {
    const face = loadClientFace(readRoot('client/client.js'));
    const { ctx, registered } = hostLikeCtx(face);
    assert.doesNotThrow(() => face.apply(ctx), 'apply() must not throw once every used service is declared');
    assert.ok(registered.includes('settings.section'), 'settings.section slot registered');
    assert.ok(registered.includes('settings.plugins.tab'), 'settings.plugins.tab slot registered');
});

test('REGRESSION: omitting "slots" from the face inject reproduces the activation crash', () => {
    const face = loadClientFace(readRoot('client/client.js'), { faceInject: ['locale'] });
    const { ctx } = hostLikeCtx(face);
    assert.throws(
        () => face.apply(ctx),
        /undeclared service "slots"/,
        'with the historical inject=["locale"] the entry must throw at activation (the root cause of "1 entry did not activate")',
    );
});
