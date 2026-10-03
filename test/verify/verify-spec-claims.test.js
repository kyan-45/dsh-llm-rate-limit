/**
 * Independent re-checks of t3's claims and of the spec's structural contract
 * (task t4). Written by the verifier from the spec text, not from the
 * implementer's tests. Nothing here writes to lib/ or docs/.
 */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, readdirSync } from 'node:fs';
import { test } from 'node:test';

import { ALL_KEYS, LIMITER_KEYS, V1_KEYS, V2_KEYS, resolveConfig } from '../../lib/config.js';
import { VERSION, apply, name } from '../../lib/index.js';
import { ProviderLimiter, createLimiters, gateStream, limiterFor } from '../../lib/limiter.js';
import { buildStateSnapshot, renderReport } from '../../lib/observability.js';

const root = new URL('../../', import.meta.url);
const readRoot = (relative) => readFileSync(new URL(relative, root), 'utf8');
const sha256 = (relative) => createHash('sha256').update(readFileSync(new URL(relative, root))).digest('hex').toUpperCase();

/** Spec §3.3 neutral defaults, transcribed from the design document. */
const NEUTRAL_V2 = {
    rpmLimit: 0,
    rpmWindowMs: 60_000,
    tpmLimit: 0,
    tpmWindowMs: 60_000,
    tpmOutputReserveTokens: 1_024,
    tpmLogMaxEntries: 4_096,
    adaptiveConcurrency: false,
    minConcurrent: 1,
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

/** Spec §3.5 shipped cordis.patch.yml config block. */
const SHIPPED_BLOCK = {
    enabled: true,
    providers: [],
    maxConcurrent: 4,
    maxWaitMs: 120_000,
    cooldownBaseMs: 5_000,
    cooldownMaxMs: 80_000,
    systemPrompt: true,
    sectionOrder: 90,
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

/** Spec §11.4 pinned v1 hashes. */
const V1_LIMITER_TEST_SHA256 = '9B6696E54086D9A22C1C6F2423CCBBD92298982DC6ABD9F141AC4778D0358831';
const V1_INDEX_TEST_SHA256 = 'C5AC5EB7847696194CD649FAEDD6146B7746B7FEC40B81EFA8FE516DE7A48934';

/** Extract the active (non-comment) `config:` block from cordis.patch.yml. */
function parseShippedBlock(text) {
    const lines = text.split(/\r?\n/);
    const configIndex = lines.findIndex((line) => /^\s*config:\s*$/.test(line));
    assert.notEqual(configIndex, -1, 'cordis.patch.yml has a config: block');
    const baseIndent = lines[configIndex].match(/^\s*/)[0].length;
    const parsed = {};
    for (let i = configIndex + 1; i < lines.length; i += 1) {
        const line = lines[i];
        if (/^\s*$/.test(line) || /^\s*#/.test(line))
            continue;
        const indent = line.match(/^\s*/)[0].length;
        if (indent <= baseIndent)
            break;
        const match = line.match(/^\s*([A-Za-z0-9_]+):\s*(.*?)\s*$/);
        if (match === null)
            continue;
        const raw = match[2].replace(/\s+#.*$/, '').trim();
        let value;
        if (raw === '[]')
            value = [];
        else if (raw === 'true' || raw === 'false')
            value = raw === 'true';
        else if (/^-?\d+(\.\d+)?$/.test(raw))
            value = Number(raw);
        else
            value = raw.replace(/^['"]|['"]$/g, '');
        parsed[match[1]] = value;
    }
    return parsed;
}

test('CLAIM-t3-1: all 30 config keys resolve; the 22 v2 keys default to the neutral v1-equivalent values', () => {
    const resolved = resolveConfig({});
    assert.deepEqual([...Object.keys(resolved)].sort(), [...ALL_KEYS].sort(), 'all 30 keys are returned');
    assert.equal(Object.keys(resolved).length, 30);
    assert.deepEqual(resolved.providers, []);
    for (const [key, expected] of Object.entries(NEUTRAL_V2))
        assert.deepEqual(resolved[key], expected, `${key} defaults to ${JSON.stringify(expected)}`);
    assert.equal(resolved.aimdInitialConcurrent, 2, 'derived: clamp(ceil(4/2), 1, 4)');
    assert.equal(resolved.cooldownMaxMs, 80_000, 'derived: cooldownBaseMs * 16');
    assert.equal(resolved.aimdInitialConcurrent, Math.min(Math.max(Math.ceil(resolved.maxConcurrent / 2), resolved.minConcurrent), resolved.maxConcurrent));

    // A v1-only config behaves exactly like a v1 config plus the neutral v2 keys.
    const v1Only = { maxConcurrent: 2, maxWaitMs: 5_000, cooldownBaseMs: 100, cooldownMaxMs: 1_000 };
    assert.deepEqual(resolveConfig(v1Only), resolveConfig({ ...v1Only, ...NEUTRAL_V2 }));
    assert.deepEqual(resolveConfig(v1Only), resolveConfig(resolveConfig(v1Only)), 'idempotent');
    const untouched = { maxConcurrent: 2, cooldownBaseMs: 10 };
    resolveConfig(untouched);
    assert.deepEqual(untouched, { maxConcurrent: 2, cooldownBaseMs: 10 }, 'the input is never mutated');
    assert.deepEqual(resolveConfig({ unknownKey: 1 }), resolveConfig({}), 'unknown keys are ignored');

    // Validation fallbacks, one per rule.
    assert.equal(resolveConfig({ rpmLimit: -1 }).rpmLimit, 0);
    assert.equal(resolveConfig({ rpmLimit: 2.5 }).rpmLimit, 2, 'floored');
    assert.equal(resolveConfig({ cooldownMaxMs: 50 }).cooldownMaxMs, 80_000, 'cooldownMaxMs must exceed the base');
    assert.equal(resolveConfig({ cooldownJitter: 'sometimes' }).cooldownJitter, 'none');
    assert.equal(resolveConfig({ tpmLogMaxEntries: 5 }).tpmLogMaxEntries, 4_096);
    assert.equal(resolveConfig({ minConcurrent: 99, maxConcurrent: 3 }).minConcurrent, 3, 'cross-key clamp');
    assert.equal(resolveConfig({ minConcurrent: 99, maxConcurrent: 3 }).aimdInitialConcurrent, 3);
    assert.equal(resolveConfig({ maxConcurrent: 0 }).maxConcurrent, 4);
    assert.deepEqual(V1_KEYS, ['enabled', 'providers', 'maxConcurrent', 'maxWaitMs', 'cooldownBaseMs', 'cooldownMaxMs', 'systemPrompt', 'sectionOrder']);
    assert.equal(V2_KEYS.length, 22);
    assert.equal(LIMITER_KEYS.length, 26);
});

test('CLAIM-t3-2: cordis.patch.yml ships exactly the §3.5 block and leaves the two omitted keys neutral', () => {
    const parsed = parseShippedBlock(readRoot('cordis.patch.yml'));
    assert.deepEqual(parsed, SHIPPED_BLOCK, 'the active config block equals spec §3.5');
    assert.equal(Object.keys(parsed).length, 28);
    assert.ok(!Object.hasOwn(parsed, 'tpmLogMaxEntries'), 'tpmLogMaxEntries is not an active key');
    assert.ok(!Object.hasOwn(parsed, 'aimdInitialConcurrent'), 'aimdInitialConcurrent is not an active key');

    const resolved = resolveConfig(parsed);
    assert.equal(resolved.tpmLogMaxEntries, 4_096, 'omitted key resolves to its neutral default');
    assert.equal(resolved.aimdInitialConcurrent, 2, 'omitted key resolves to the derived neutral default');
    assert.deepEqual(
        resolved,
        resolveConfig({ ...parsed, tpmLogMaxEntries: 4_096, aimdInitialConcurrent: 2 }),
        'the shipped config is identical to one that names the neutral values explicitly',
    );

    // The shipped values are documented in comments, including every neutral default.
    const text = readRoot('cordis.patch.yml');
    for (const key of V2_KEYS)
        assert.ok(text.includes(key), `cordis.patch.yml mentions ${key}`);
    assert.ok(/tpmLogMaxEntries: 4096/.test(text), 'the omitted key is documented with its value');
    assert.ok(/aimdInitialConcurrent: null/.test(text), 'the omitted derived key is documented');
});

test('CLAIM-t3-3: the two v1 test files match the spec-pinned v1 hashes and are still v1-only', () => {
    assert.equal(sha256('test/limiter.test.js'), V1_LIMITER_TEST_SHA256, 'limiter.test.js is byte-identical to v1');
    assert.equal(sha256('test/index.test.js'), V1_INDEX_TEST_SHA256, 'index.test.js is byte-identical to v1');
    const limiterTest = readRoot('test/limiter.test.js');
    const indexTest = readRoot('test/index.test.js');
    for (const key of V2_KEYS) {
        assert.ok(!limiterTest.includes(key), `v1 limiter.test.js does not reference ${key}`);
        assert.ok(!indexTest.includes(key), `v1 index.test.js does not reference ${key}`);
    }
    assert.ok(limiterTest.includes('limiter unit tests: all passed'));
    assert.ok(indexTest.includes('index integration tests: all passed'));
});

test('CLAIM-t3-4: package.json is 2.0.0 with no runtime dependencies and only optional peers', () => {
    const pkg = JSON.parse(readRoot('package.json'));
    assert.equal(pkg.version, '2.0.0');
    assert.equal(VERSION, pkg.version, 'lib/index.js VERSION mirrors package.json');
    assert.ok(!Object.hasOwn(pkg, 'dependencies'), 'no runtime dependencies');
    assert.ok(pkg.peerDependenciesMeta !== undefined);
    for (const [peer, meta] of Object.entries(pkg.peerDependenciesMeta))
        assert.equal(meta.optional, true, `${peer} is optional`);
    assert.deepEqual(Object.keys(pkg.exports).sort(), ['.', './client', './cordis.patch.yml', './package.json'].sort());
    assert.equal(pkg.type, 'module');
});

test('CLAIM-t3-5: the file plan exists: 9 lib modules + v1 tests + 7 new test files + docs', () => {
    const lib = readdirSync(new URL('lib/', root)).filter((entry) => entry.endsWith('.js')).sort();
    assert.deepEqual(lib, ['aimd.js', 'config.js', 'cooldown.js', 'index.js', 'limiter.js', 'observability.js', 'scheduler.js', 'tokens.js', 'window.js']);
    const tests = readdirSync(new URL('test/', root)).filter((entry) => entry.endsWith('.test.js')).sort();
    assert.deepEqual(tests, ['aimd.test.js', 'config.test.js', 'cooldown.test.js', 'index.test.js', 'limiter.test.js', 'scheduler.test.js', 'tokens.test.js', 'v2-integration.test.js', 'window.test.js']);
    assert.equal(tests.length - 2, 7, 'seven new v2 test files beyond the two v1 files');

    const readme = readRoot('README.md');
    for (const key of [...V1_KEYS, ...V2_KEYS])
        assert.ok(readme.includes(key), `README documents ${key}`);
    assert.ok(readme.includes('/rate-limit'), 'README documents the command');
    assert.ok(readme.includes('RATE_LIMIT_QUEUE_FULL') && readme.includes('RATE_LIMIT_BUDGET_EXCEEDED'), 'README documents the new failure codes');
});

test('CLAIM-t3-6: the state snapshot is a JSON-serializable v1 superset and the report is ASCII + deterministic', () => {
    const limiter = new ProviderLimiter('*', { maxConcurrent: 4, rpmLimit: 300, rpmWindowMs: 60_000, tpmLimit: 2_000_000, tpmWindowMs: 60_000, adaptiveConcurrency: true, fairness: true }, { now: () => 1_700_000_000_000 });
    const snapshot = buildStateSnapshot({
        version: VERSION,
        config: resolveConfig({}),
        stats: { rateLimit429: 1, success: 2, error: 3, timeout: 4, aborted: 5, rateLimit503: 6, rateLimit529: 7 },
        limiters: new Map([['*', limiter]]),
        startedAt: 1_700_000_000_000,
        now: 1_700_000_083_000,
    });
    assert.deepEqual(JSON.parse(JSON.stringify(snapshot)), snapshot, 'no undefined holes; JSON round-trips');
    assert.deepEqual(Object.keys(snapshot.config).sort(), [...ALL_KEYS].sort(), 'all 30 keys appear in the snapshot config');
    const route = snapshot.limiters[0];
    for (const field of ['provider', 'active', 'queued', 'queuedPeak', 'waits', 'waitTimeouts', 'cooldownRemainingMs', 'last429At'])
        assert.ok(Object.hasOwn(route, field), `v1 field ${field} is present`);
    for (const field of ['rateLimit429', 'success', 'error', 'timeout'])
        assert.ok(Object.hasOwn(snapshot.stats, field), `client-used stat ${field} is present`);
    assert.equal(route.classes !== null, true, 'fairness: true exposes class counters');

    const report = renderReport(snapshot, '');
    assert.equal(renderReport(snapshot, ''), report, 'the report is deterministic');
    const nonAscii = [...report].filter((char) => char.codePointAt(0) > 127);
    assert.deepEqual(nonAscii, [], `the report is ASCII-only (found ${JSON.stringify(nonAscii)})`);
    assert.ok(report.includes('dsh-llm-rate-limit v2.0.0 - '), 'the header uses an ASCII hyphen, not the spec sample em dash');
    assert.ok(report.includes('route "*"'));
    assert.ok(report.includes('in-flight 0/2 (adaptive 2, floor 1, ceiling 4)'), `route line: ${report.split('\n')[1]}`);
    assert.equal(renderReport(snapshot, 'nope'), renderReport({ ...snapshot, limiters: [] }, ''), 'an unknown filter prints no routes but keeps the header/stats');
});

test('CLAIM-t3-7: lib/*.js imports only relative modules and never a runtime dependency', () => {
    for (const file of readdirSync(new URL('lib/', root)).filter((entry) => entry.endsWith('.js'))) {
        const source = readRoot(`lib/${file}`);
        const specifiers = [...source.matchAll(/from\s+'([^']+)'/g)].map((match) => match[1]);
        for (const specifier of specifiers)
            assert.ok(specifier.startsWith('./') || specifier.startsWith('../'), `${file} imports only relative modules (saw ${specifier})`);
        assert.ok(!/require\s*\(/.test(source), `${file} is ESM`);
    }
});

test('CLAIM-t3-8: the v1 public API surface is preserved', () => {
    assert.equal(name, 'dsh-llm-rate-limit');
    assert.equal(typeof apply, 'function');
    assert.equal(typeof ProviderLimiter, 'function');
    assert.equal(typeof createLimiters, 'function');
    assert.equal(typeof limiterFor, 'function');
    assert.equal(typeof gateStream, 'function');
    const limiter = new ProviderLimiter('api', { maxConcurrent: 2 });
    for (const method of ['acquire', 'drop', 'release', 'expireWaiters', 'pump', 'observe429', 'closeCooldown', 'resetEscalation', 'snapshot'])
        assert.equal(typeof limiter[method], 'function', `limiter.${method} is a function`);
    for (const field of ['active', 'waits', 'queuedPeak', 'timeoutCount', 'last429At', 'cooldownUntil', 'cooldownEscalations'])
        assert.ok(field in limiter, `limiter.${field} exists`);
    assert.equal(limiter.cooldownMs, 0);
    assert.equal(limiter.activeCount, 0);
    assert.equal(limiter.queued, 0);
    assert.deepEqual(limiter.waiters, []);
    const map = createLimiters(['a'], { maxConcurrent: 1 });
    assert.equal(limiterFor(map, 'a')?.provider, 'a');
    assert.equal(limiterFor(map, 'zzz')?.provider, '*');
});

test('CLAIM-t3-9: the five pure modules read no clock, no RNG and no timers of their own', () => {
    for (const file of ['window.js', 'aimd.js', 'scheduler.js', 'cooldown.js', 'tokens.js']) {
        const source = readRoot(`lib/${file}`);
        // Strip comments so a doc reference ("defaults to Math.random") cannot trip the check.
        const code = source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
        assert.ok(!/Date\.now/.test(code), `${file} does not read Date.now`);
        assert.ok(!/Math\.random/.test(code), `${file} does not call Math.random`);
        assert.ok(!/setTimeout|setInterval/.test(code), `${file} owns no timer`);
    }
});
