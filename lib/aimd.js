/**
 * AIMD adaptive concurrency controller.
 *
 * Additive increase on clean settles, multiplicative decrease on a throttle
 * signal (429/503/529 failure or a gate wait-timeout), clamped to
 * `[minConcurrent, maxConcurrent]`. With `adaptiveConcurrency: false` the
 * limiter never reads this controller, so the static v1 gate is preserved.
 *
 * The controller is clock-injected (`now` is an explicit parameter) and has no
 * timers of its own.
 *
 * @module dsh-llm-rate-limit/aimd
 */

/**
 * Additive-increase / multiplicative-decrease concurrency controller.
 */
export class Aimd {
    /**
     * @param config - normalized limiter options.
     */
    constructor(config) {
        this.floor = Number.isInteger(config.minConcurrent) ? config.minConcurrent : 1;
        this.ceiling = Number.isInteger(config.maxConcurrent) ? config.maxConcurrent : 1;
        this.increaseEvery = Number.isInteger(config.aimdIncreaseEvery) && config.aimdIncreaseEvery >= 1
            ? config.aimdIncreaseEvery
            : 20;
        this.decreaseFactor = Number.isFinite(config.aimdDecreaseFactor)
            && config.aimdDecreaseFactor > 0
            && config.aimdDecreaseFactor < 1
            ? config.aimdDecreaseFactor
            : 0.5;
        this.timeoutDecrease = config.aimdTimeoutDecrease !== false;
        const initial = Number.isFinite(config.aimdInitialConcurrent)
            ? config.aimdInitialConcurrent
            : Math.ceil(this.ceiling / 2);
        this.current = this.clamp(initial);
        this.successStreak = 0;
        this.increases = 0;
        this.decreases = 0;
        this.lastDecreaseAt = 0;
    }

    /** Clamp to the configured bounds as an integer. */
    clamp(value) {
        return Math.min(Math.max(Math.round(value), this.floor), this.ceiling);
    }

    /** The concurrency bound the gate may use right now. */
    effectiveConcurrency() {
        return this.clamp(this.current);
    }

    /** One clean terminal settle: additive increase every `increaseEvery`. */
    onSuccess(now) {
        this.successStreak += 1;
        if (this.successStreak >= this.increaseEvery) {
            this.successStreak = 0;
            if (this.current < this.ceiling) {
                this.current += 1;
                this.increases += 1;
            }
        }
        return this.current;
    }

    /** One throttle observation: multiplicative decrease down to the floor. */
    onThrottle(now) {
        this.successStreak = 0;
        const next = Math.max(this.floor, Math.floor(this.current * this.decreaseFactor));
        if (next < this.current) {
            this.current = next;
            this.decreases += 1;
        }
        this.lastDecreaseAt = now;
        return this.current;
    }

    /** A gate wait-timeout is a loss signal when `aimdTimeoutDecrease` is on. */
    onTimeout(now) {
        if (this.timeoutDecrease)
            return this.onThrottle(now);
        return this.current;
    }

    /** Observability snapshot. */
    snapshot() {
        return {
            current: this.current,
            floor: this.floor,
            ceiling: this.ceiling,
            successStreak: this.successStreak,
            increases: this.increases,
            decreases: this.decreases,
        };
    }
}
