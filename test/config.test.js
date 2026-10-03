/**
 * Configuration resolution: defaults, idempotence, validation fallbacks,
 * cross-key clamping, and the v1-only equivalence contract.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
    ALL_KEYS,
    CONFIG_DEFAULTS,
    LIMITER_KEYS,
    V1_KEYS,
    V2_KEYS,
    normalizeLimiterOptions,
    resolveConfig,
} from '../lib/config.js';

/** The resolved defaults, including the two derived keys. */
const DEFAULTS = {
    enabled: true,
    providers: [],
    maxConcurrent: 4,
    maxWaitMs: 120_000,
    cooldownBaseMs: 5_000,
    cooldownMaxMs: 80_000,
    systemPrompt: true,
    sectionOrder: 90,
    rpmLimit: 0,
    rpmWindowMs: 60_000,
    tpmLimit: 0,
    tpmWindowMs: 60_000,
    tpmOutputReserveTokens: 1_024,
    tpmLogMaxEntries: 4_096,
    adaptiveConcurrency: false,
    minConcurrent: 1,
    aimdInitialConcurrent: 2,
    aimdIncreaseEvery: 20,
    aimdDecreaseFactor: 0.5,
    aimdTimeoutDecrease: true,
    fairness: false,
    interactiveWeight: 6,
    backgroundWeight: 2,
    utilityWeight: 1,
    agingThresholdMs: 0,
    queueLimit: 256,
    releaseSpreadMs: 0,
    cooldownJitter: 'none',
    cooldownHysteresisMs: 0,
    overloadCooldownMultiplier: 1,
};

test('config schema: 8 v1 keys + 22 v2 keys = 30 resolved keys', () => {
    assert.equal(V1_KEYS.length, 8);
    assert.equal(V2_KEYS.length, 22);
    assert.equal(ALL_KEYS.length, 30);
    assert.deepEqual([...V1_KEYS, ...V2_KEYS], [...ALL_KEYS]);
});

test('defaults: resolveConfig({}) matches the spec table exactly', () => {
    const resolved = resolveConfig({});
    assert.deepEqual(Object.keys(resolved).sort(), [...ALL_KEYS].sort());
    for (const [key, expected] of Object.entries(DEFAULTS))
        assert.deepEqual(resolved[key], expected, `${key} default`);
    // `undefined` is never returned: the two input-form defaults are materialized.
    for (const key of ALL_KEYS)
        assert.notEqual(resolved[key], undefined, `${key} must be concrete`);
});

test('defaults: CONFIG_DEFAULTS mirrors the resolved defaults', () => {
    const resolved = resolveConfig({});
    for (const key of ALL_KEYS) {
        const documented = CONFIG_DEFAULTS[key];
        if (documented === null)
            continue; // derived keys (cooldownMaxMs, aimdInitialConcurrent)
        assert.deepEqual(resolved[key], documented, `${key} documented default`);
    }
    assert.equal(resolved.cooldownMaxMs, CONFIG_DEFAULTS.cooldownBaseMs * 16);
    assert.equal(resolved.aimdInitialConcurrent, Math.ceil(CONFIG_DEFAULTS.maxConcurrent / 2));
});

test('resolveConfig is idempotent for raw, resolved and undefined inputs', () => {
    const inputs = [
        undefined,
        null,
        {},
        { maxConcurrent: 2, maxWaitMs: 500, cooldownBaseMs: 100, cooldownMaxMs: 1_000 },
        {
            enabled: false,
            providers: [' deepseek-official ', '*'],
            maxConcurrent: 7,
            rpmLimit: 12.9,
            tpmLimit: 999,
            adaptiveConcurrency: true,
            aimdInitialConcurrent: 3,
            fairness: true,
            cooldownJitter: 'full',
            cooldownHysteresisMs: 1_000,
        },
    ];
    for (const input of inputs) {
        const once = resolveConfig(input);
        const twice = resolveConfig(once);
        assert.deepEqual(twice, once, `idempotent for ${JSON.stringify(input)}`);
    }
});

test('v1-only config: every new key stays at its neutral default', () => {
    const resolved = resolveConfig({
        enabled: true,
        providers: ['deepseek-official'],
        maxConcurrent: 2,
        maxWaitMs: 500,
        cooldownBaseMs: 100,
        cooldownMaxMs: 1_000,
        systemPrompt: true,
        sectionOrder: 90,
    });
    for (const key of V2_KEYS) {
        if (key === 'aimdInitialConcurrent') {
            assert.equal(resolved[key], 1, 'ceil(2/2) clamped');
            continue;
        }
        assert.deepEqual(resolved[key], DEFAULTS[key], `${key} stays neutral`);
    }
    assert.equal(resolved.maxConcurrent, 2);
    assert.equal(resolved.maxWaitMs, 500);
    assert.equal(resolved.cooldownBaseMs, 100);
    assert.equal(resolved.cooldownMaxMs, 1_000);
});

test('validation: invalid values fall back to the default and warn once each', () => {
    const warnings = [];
    const resolved = resolveConfig({
        enabled: 'yes',
        systemPrompt: 1,
        adaptiveConcurrency: 'true',
        aimdTimeoutDecrease: 'no',
        fairness: 0,
        providers: 'deepseek-official',
        maxConcurrent: 0,
        minConcurrent: 1.5,
        aimdIncreaseEvery: 0,
        tpmOutputReserveTokens: -1,
        queueLimit: 2.5,
        tpmLogMaxEntries: 8,
        aimdInitialConcurrent: 0,
        rpmLimit: -1,
        tpmLimit: Number.NaN,
        maxWaitMs: 0,
        rpmWindowMs: 'soon',
        tpmWindowMs: -5,
        cooldownHysteresisMs: -1,
        agingThresholdMs: 'old',
        releaseSpreadMs: -2,
        cooldownBaseMs: 0,
        cooldownMaxMs: 10,
        overloadCooldownMultiplier: 0,
        interactiveWeight: 0,
        backgroundWeight: '2',
        utilityWeight: -1,
        aimdDecreaseFactor: 1,
        cooldownJitter: 'bogus',
        sectionOrder: 1.5,
    }, (message) => warnings.push(message));

    assert.equal(warnings.length, 30, 'one warning per rejected key');
    for (const message of warnings) {
        assert.ok(message.startsWith('dsh-llm-rate-limit: ignoring invalid '), message);
        assert.ok(message.includes('(expected '), message);
        assert.ok(message.includes('); using '), message);
    }
    assert.match(warnings[0], /^dsh-llm-rate-limit: ignoring invalid enabled="yes" \(expected boolean\); using true$/);
    // Base rejected ⇒ its default; the max is then validated against that base.
    assert.equal(resolved.cooldownBaseMs, 5_000);
    assert.equal(resolved.cooldownMaxMs, 80_000);
    assert.equal(resolved.maxConcurrent, 4);
    assert.equal(resolved.minConcurrent, 1);
    assert.equal(resolved.aimdInitialConcurrent, 2);
    assert.equal(resolved.cooldownJitter, 'none');
    assert.equal(resolved.sectionOrder, 90);
    assert.deepEqual(resolved.providers, []);
});

test('validation: an invalid cooldownMaxMs falls back to cooldownBaseMs * 16', () => {
    const resolved = resolveConfig({ cooldownBaseMs: 1_000, cooldownMaxMs: 1_000 });
    assert.equal(resolved.cooldownMaxMs, 16_000);
});

test('validation: fractional limits are floored, not rejected', () => {
    const resolved = resolveConfig({ rpmLimit: 12.9, tpmLimit: 999.4 });
    assert.equal(resolved.rpmLimit, 12);
    assert.equal(resolved.tpmLimit, 999);
});

test('clamping: minConcurrent never exceeds maxConcurrent', () => {
    const resolved = resolveConfig({ maxConcurrent: 3, minConcurrent: 10 });
    assert.equal(resolved.minConcurrent, 3);
    assert.equal(resolved.aimdInitialConcurrent, 3, 'the initial value is clamped into [min, max]');
});

test('clamping: aimdInitialConcurrent is materialized and clamped into range', () => {
    assert.equal(resolveConfig({ maxConcurrent: 8 }).aimdInitialConcurrent, 4);
    assert.equal(resolveConfig({ maxConcurrent: 8, aimdInitialConcurrent: 99 }).aimdInitialConcurrent, 8);
    assert.equal(resolveConfig({ maxConcurrent: 8, aimdInitialConcurrent: 1, minConcurrent: 5 }).aimdInitialConcurrent, 5);
    assert.equal(resolveConfig({ maxConcurrent: 1 }).aimdInitialConcurrent, 1);
});

test('providers: entries are trimmed, empties dropped, non-arrays rejected', () => {
    const warnings = [];
    const resolved = resolveConfig({ providers: [' a ', '', '  ', 'b', 7, null] }, (m) => warnings.push(m));
    assert.deepEqual(resolved.providers, ['a', 'b']);
    assert.equal(warnings.length, 0, 'entry filtering is not a rejection');
    const rejected = resolveConfig({ providers: 'a' }, (m) => warnings.push(m));
    assert.deepEqual(rejected.providers, []);
    assert.equal(warnings.length, 1);
});

test('normalizeLimiterOptions: subset only, idempotent, v1 config accepted', () => {
    const v1Only = { maxConcurrent: 2, maxWaitMs: 500, cooldownBaseMs: 100, cooldownMaxMs: 1_000 };
    const normalized = normalizeLimiterOptions(v1Only);
    assert.deepEqual(Object.keys(normalized).sort(), [...LIMITER_KEYS].sort());
    for (const excluded of ['enabled', 'providers', 'systemPrompt', 'sectionOrder'])
        assert.equal(Object.hasOwn(normalized, excluded), false, `${excluded} excluded`);
    assert.equal(normalized.maxConcurrent, 2);
    assert.equal(normalized.maxWaitMs, 500);
    assert.equal(normalized.cooldownBaseMs, 100);
    assert.equal(normalized.cooldownMaxMs, 1_000);
    assert.equal(normalized.rpmLimit, 0);
    assert.equal(normalized.fairness, false);
    assert.deepEqual(normalizeLimiterOptions(normalized), normalized, 'idempotent');
    assert.deepEqual(normalizeLimiterOptions(resolveConfig(v1Only)), normalizeLimiterOptions(v1Only));
});
