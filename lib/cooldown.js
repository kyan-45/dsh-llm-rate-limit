/**
 * Cooldown state machine: Retry-After, the v1 escalation ladder, jitter,
 * hysteresis.
 *
 * Rules fixed by the design contract:
 * - a provider-directed delay (`providerRetryAfterMs > 0`) is used **verbatim**
 *   and never jittered;
 * - a locally computed window keeps the v1 ladder `base * 2^min(esc, 4)`,
 *   capped by `cooldownMaxMs`, and is optionally jittered;
 * - a 503/529 window is additionally multiplied by
 *   `overloadCooldownMultiplier`;
 * - with `cooldownHysteresisMs: 0` the ladder resets when the window closes
 *   and on the first clean success — exactly v1;
 * - with hysteresis enabled the ladder only decays after a quiet period
 *   (armed by a clean success, cancelled by the next observation).
 *
 * The machine itself owns no timers and reads no clock: `now`/`rng` are
 * explicit parameters. `ProviderLimiter` owns the timers.
 *
 * @module dsh-llm-rate-limit/cooldown
 */

/**
 * Apply one jitter mode to a locally computed window.
 *
 * @param windowMs - the un-jittered window in ms.
 * @param mode - `'none' | 'equal' | 'full'`.
 * @param rng - `() => number` in `[0, 1)`; defaults to `Math.random`.
 * @returns the jittered window in ms.
 */
export function jitteredWindow(windowMs, mode, rng) {
    const w = Number.isFinite(windowMs) && windowMs > 0 ? windowMs : 0;
    const draw = typeof rng === 'function' ? rng() : 0;
    const f = Number.isFinite(draw) ? Math.min(Math.max(draw, 0), 1) : 0;
    if (mode === 'equal')
        return w / 2 + f * (w / 2);
    if (mode === 'full')
        return f * w;
    return w;
}

/**
 * Cooldown window + escalation ladder state.
 */
export class Cooldown {
    /**
     * @param config - normalized limiter options (used when a call omits one).
     */
    constructor(config) {
        this.config = config ?? {};
        /** Absolute time the window closes; 0 when no window is open. */
        this.until = 0;
        /** Consecutive throttle observations (the ladder exponent). */
        this.escalations = 0;
        /** Absolute time at which a hysteresis decay fires; 0 when none is armed. */
        this.hysteresisUntil = 0;
    }

    /**
     * Observe one throttle and (re)arm the window.
     *
     * @param delayMs - provider Retry-After hint in ms (verbatim when > 0).
     * @param config - config override (v1 passes it explicitly).
     * @param status - HTTP status, used for the 503/529 overload multiplier.
     * @param now - current time.
     * @param rng - random source for jitter modes.
     * @returns the total window in ms that now applies (from `now`).
     */
    observe429(delayMs, config, status, now, rng) {
        const cfg = config ?? this.config;
        const serverDirected = Number.isFinite(delayMs) && delayMs > 0;
        const base = serverDirected
            ? delayMs
            : (Number.isFinite(cfg.cooldownBaseMs) && cfg.cooldownBaseMs > 0 ? cfg.cooldownBaseMs : 5_000);
        const cap = Number.isFinite(cfg.cooldownMaxMs) && cfg.cooldownMaxMs > 0
            ? cfg.cooldownMaxMs
            : base * 16;
        const factor = 2 ** Math.min(this.escalations, 4);
        let window = Math.min(base * factor, cap);
        if (status === 503 || status === 529) {
            const multiplier = Number.isFinite(cfg.overloadCooldownMultiplier)
                ? cfg.overloadCooldownMultiplier
                : 1;
            window = Math.min(window * multiplier, cap);
        }
        if (!serverDirected)
            window = jitteredWindow(window, cfg.cooldownJitter, rng);
        const until = Math.max(now + window, this.until);
        this.until = until;
        this.escalations += 1;
        // A throttle observation always cancels a pending hysteresis decay.
        this.hysteresisUntil = 0;
        return until - now;
    }

    /**
     * Close the open window (idempotent).
     * @returns true when a window was actually open.
     */
    close(now, config) {
        if (this.until === 0)
            return false;
        this.until = 0;
        const hysteresis = (config ?? this.config).cooldownHysteresisMs;
        if (!(Number.isFinite(hysteresis) && hysteresis > 0))
            this.escalations = 0;
        return true;
    }
}
