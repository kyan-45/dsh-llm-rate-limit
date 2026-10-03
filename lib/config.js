/**
 * Configuration resolution for dsh-llm-rate-limit v2.
 *
 * The v2 schema is the eight v1 keys plus 22 new keys. Every new key defaults
 * to the value that makes v2 behave *exactly* like v1 in that dimension
 * (`rpmLimit: 0`, `adaptiveConcurrency: false`, `fairness: false`, …), so a
 * v1-only configuration keeps v1 behaviour and a deployment opts into the v2
 * behaviour by setting the new keys explicitly (see cordis.patch.yml).
 *
 * `resolveConfig` is a pure function of its input: it is idempotent
 * (`resolveConfig(resolveConfig(x))` deep-equals `resolveConfig(x)`), it never
 * mutates the caller's object, and it returns all 30 keys with concrete
 * values. `aimdInitialConcurrent` is the only key whose *input* default is
 * `null`; it is materialized to the computed integer at resolution time.
 *
 * Rejected values are never fatal: each one is replaced by its default and
 * reported through the optional `warn` sink, which the host wiring points at
 * the plugin logger. `resolveConfig` stays usable as a pure helper when the
 * sink is omitted.
 *
 * @module dsh-llm-rate-limit/config
 */

/** The eight v1 keys — names and meanings are frozen by the v1 contract. */
export const V1_KEYS = Object.freeze([
    'enabled',
    'providers',
    'maxConcurrent',
    'maxWaitMs',
    'cooldownBaseMs',
    'cooldownMaxMs',
    'systemPrompt',
    'sectionOrder',
]);

/** The 22 keys v2 adds. */
export const V2_KEYS = Object.freeze([
    'rpmLimit',
    'rpmWindowMs',
    'tpmLimit',
    'tpmWindowMs',
    'tpmOutputReserveTokens',
    'tpmLogMaxEntries',
    'adaptiveConcurrency',
    'minConcurrent',
    'aimdInitialConcurrent',
    'aimdIncreaseEvery',
    'aimdDecreaseFactor',
    'aimdTimeoutDecrease',
    'fairness',
    'interactiveWeight',
    'backgroundWeight',
    'utilityWeight',
    'agingThresholdMs',
    'queueLimit',
    'releaseSpreadMs',
    'cooldownJitter',
    'cooldownHysteresisMs',
    'overloadCooldownMultiplier',
]);

/** Every key `resolveConfig` returns (8 + 22 = 30). */
export const ALL_KEYS = Object.freeze([...V1_KEYS, ...V2_KEYS]);

/**
 * The subset `ProviderLimiter` reads. It excludes the presentation/registration
 * keys (`enabled`, `providers`, `systemPrompt`, `sectionOrder`), which belong
 * to the host wiring in lib/index.js.
 */
export const LIMITER_KEYS = Object.freeze([
    'maxConcurrent',
    'maxWaitMs',
    'cooldownBaseMs',
    'cooldownMaxMs',
    'rpmLimit',
    'rpmWindowMs',
    'tpmLimit',
    'tpmWindowMs',
    'tpmOutputReserveTokens',
    'tpmLogMaxEntries',
    'adaptiveConcurrency',
    'minConcurrent',
    'aimdInitialConcurrent',
    'aimdIncreaseEvery',
    'aimdDecreaseFactor',
    'aimdTimeoutDecrease',
    'fairness',
    'interactiveWeight',
    'backgroundWeight',
    'utilityWeight',
    'agingThresholdMs',
    'queueLimit',
    'releaseSpreadMs',
    'cooldownJitter',
    'cooldownHysteresisMs',
    'overloadCooldownMultiplier',
]);

/**
 * Documented defaults, exactly as fixed by spec §3.2/§3.3.
 * `cooldownMaxMs` is derived from the resolved `cooldownBaseMs` (×16) and
 * `aimdInitialConcurrent` is materialized from `maxConcurrent`; both are
 * handled in `resolveConfig` and shown here as `null` input defaults.
 */
export const CONFIG_DEFAULTS = Object.freeze({
    enabled: true,
    providers: [],
    maxConcurrent: 4,
    maxWaitMs: 120_000,
    cooldownBaseMs: 5_000,
    cooldownMaxMs: null, // cooldownBaseMs * 16
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
    aimdInitialConcurrent: null, // clamp(ceil(maxConcurrent / 2), minConcurrent, maxConcurrent)
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
});

/** The three accepted `cooldownJitter` modes. */
export const COOLDOWN_JITTER_MODES = Object.freeze(['none', 'equal', 'full']);

/** @returns true when a value counts as configured (not undefined/null). */
function present(value) {
    return value !== undefined && value !== null;
}

/** JSON for a log line; never throws on odd values. */
function json(value) {
    try {
        const text = JSON.stringify(value);
        return text === undefined ? String(value) : text;
    }
    catch {
        return String(value);
    }
}

/** Clamp an integer into [lo, hi]. */
function clampInt(value, lo, hi) {
    return Math.min(Math.max(Math.round(value), lo), hi);
}

/**
 * Resolve a raw configuration block into the complete, validated v2 config.
 *
 * @param raw - the raw `config:` block (may be undefined, may be a v1-only
 *   object, may already be resolved).
 * @param warn - optional `(message: string) => void` sink for rejected values.
 * @returns an object with all 30 keys and concrete values.
 */
export function resolveConfig(raw, warn) {
    const c = present(raw) && typeof raw === 'object' ? raw : {};
    const w = typeof warn === 'function' ? warn : null;
    const bad = (key, value, rule, fallback) => {
        w?.(`dsh-llm-rate-limit: ignoring invalid ${key}=${json(value)} (expected ${rule}); using ${json(fallback)}`);
        return fallback;
    };
    const booleanKey = (key, fallback) => {
        const v = c[key];
        if (!present(v))
            return fallback;
        return typeof v === 'boolean' ? v : bad(key, v, 'boolean', fallback);
    };
    const numberKey = (key, fallback, rule, accept) => {
        const v = c[key];
        if (!present(v))
            return fallback;
        return typeof v === 'number' && Number.isFinite(v) && accept(v) ? v : bad(key, v, rule, fallback);
    };
    const integerKey = (key, fallback, min) => numberKey(key, fallback, `integer >= ${min}`, (v) => Number.isInteger(v) && v >= min);

    const enabled = booleanKey('enabled', true);
    const providers = resolveProviders(c.providers, (msg) => w?.(msg));
    const maxConcurrent = integerKey('maxConcurrent', 4, 1);
    const maxWaitMs = numberKey('maxWaitMs', 120_000, 'number > 0', (v) => v > 0);
    const cooldownBaseMs = numberKey('cooldownBaseMs', 5_000, 'number > 0', (v) => v > 0);
    const cooldownMaxMs = numberKey('cooldownMaxMs', cooldownBaseMs * 16, `number > cooldownBaseMs (${cooldownBaseMs})`, (v) => v > cooldownBaseMs);
    const systemPrompt = booleanKey('systemPrompt', true);
    const sectionOrder = numberKey('sectionOrder', 90, 'integer', (v) => Number.isInteger(v));

    const rpmLimit = numberKey('rpmLimit', 0, 'number >= 0', (v) => v >= 0);
    const rpmWindowMs = numberKey('rpmWindowMs', 60_000, 'number > 0', (v) => v > 0);
    const tpmLimit = numberKey('tpmLimit', 0, 'number >= 0', (v) => v >= 0);
    const tpmWindowMs = numberKey('tpmWindowMs', 60_000, 'number > 0', (v) => v > 0);
    const tpmOutputReserveTokens = integerKey('tpmOutputReserveTokens', 1_024, 0);
    const tpmLogMaxEntries = integerKey('tpmLogMaxEntries', 4_096, 16);
    const adaptiveConcurrency = booleanKey('adaptiveConcurrency', false);
    const minConcurrentRaw = integerKey('minConcurrent', 1, 1);
    const aimdInitialRaw = present(c.aimdInitialConcurrent)
        ? (Number.isInteger(c.aimdInitialConcurrent) && c.aimdInitialConcurrent >= 1
            ? c.aimdInitialConcurrent
            : bad('aimdInitialConcurrent', c.aimdInitialConcurrent, 'null or integer >= 1', null))
        : null;
    const aimdIncreaseEvery = integerKey('aimdIncreaseEvery', 20, 1);
    const aimdDecreaseFactor = numberKey('aimdDecreaseFactor', 0.5, 'number with 0 < f < 1', (v) => v > 0 && v < 1);
    const aimdTimeoutDecrease = booleanKey('aimdTimeoutDecrease', true);
    const fairness = booleanKey('fairness', false);
    const interactiveWeight = numberKey('interactiveWeight', 6, 'number > 0', (v) => v > 0);
    const backgroundWeight = numberKey('backgroundWeight', 2, 'number > 0', (v) => v > 0);
    const utilityWeight = numberKey('utilityWeight', 1, 'number > 0', (v) => v > 0);
    const agingThresholdMs = numberKey('agingThresholdMs', 0, 'number >= 0', (v) => v >= 0);
    const queueLimit = integerKey('queueLimit', 256, 1);
    const releaseSpreadMs = numberKey('releaseSpreadMs', 0, 'number >= 0', (v) => v >= 0);
    const cooldownJitter = COOLDOWN_JITTER_MODES.includes(c.cooldownJitter)
        ? c.cooldownJitter
        : (present(c.cooldownJitter)
            ? bad('cooldownJitter', c.cooldownJitter, "'none' | 'equal' | 'full'", 'none')
            : 'none');
    const cooldownHysteresisMs = numberKey('cooldownHysteresisMs', 0, 'number >= 0', (v) => v >= 0);
    const overloadCooldownMultiplier = numberKey('overloadCooldownMultiplier', 1, 'number > 0', (v) => v > 0);

    // Cross-key clamping, in the spec's fixed order.
    const minConcurrent = Math.min(minConcurrentRaw, maxConcurrent);
    const aimdInitialConcurrent = clampInt(
        aimdInitialRaw ?? Math.ceil(maxConcurrent / 2),
        minConcurrent,
        maxConcurrent,
    );

    return {
        enabled,
        providers,
        maxConcurrent,
        maxWaitMs,
        cooldownBaseMs,
        cooldownMaxMs,
        systemPrompt,
        sectionOrder,
        rpmLimit: Math.floor(rpmLimit),
        rpmWindowMs,
        tpmLimit: Math.floor(tpmLimit),
        tpmWindowMs,
        tpmOutputReserveTokens,
        tpmLogMaxEntries,
        adaptiveConcurrency,
        minConcurrent,
        aimdInitialConcurrent,
        aimdIncreaseEvery,
        aimdDecreaseFactor,
        aimdTimeoutDecrease,
        fairness,
        interactiveWeight,
        backgroundWeight,
        utilityWeight,
        agingThresholdMs,
        queueLimit,
        releaseSpreadMs,
        cooldownJitter,
        cooldownHysteresisMs,
        overloadCooldownMultiplier,
    };
}

/**
 * Validate the `providers` list: an array of non-empty trimmed strings.
 * A non-array is rejected (default `[]`); invalid entries are filtered.
 */
function resolveProviders(value, warn) {
    if (!present(value))
        return [];
    if (!Array.isArray(value)) {
        warn(`dsh-llm-rate-limit: ignoring invalid providers=${json(value)} (expected array of non-empty strings); using []`);
        return [];
    }
    return value
        .filter((p) => typeof p === 'string' && p.trim() !== '')
        .map((p) => p.trim());
}

/**
 * Resolve the subset of configuration `ProviderLimiter` reads. Idempotent, and
 * it accepts an unresolved v1 config (`{ maxConcurrent, maxWaitMs,
 * cooldownBaseMs, cooldownMaxMs }`) without throwing, so a directly
 * constructed limiter behaves exactly like a resolved one.
 *
 * @param rawOrResolved - raw or resolved configuration.
 * @returns a fresh object holding only the limiter keys.
 */
export function normalizeLimiterOptions(rawOrResolved) {
    const full = resolveConfig(rawOrResolved);
    const out = {};
    for (const key of LIMITER_KEYS)
        out[key] = full[key];
    return out;
}
