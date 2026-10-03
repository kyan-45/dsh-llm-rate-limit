/**
 * Operator observability: the state snapshot behind the existing web route
 * and the deterministic text report behind the `/rate-limit` command.
 *
 * The snapshot is a strict superset of the v1 route JSON (every v1 field keeps
 * its name, type, and unit) and is JSON-serializable: no `BigInt`, no
 * `undefined` holes (`null` is used instead).
 *
 * @module dsh-llm-rate-limit/observability
 */

/** Wait-time histogram bounds, in ms (`Infinity` closes the last bucket). */
export const WAIT_BUCKETS = Object.freeze([100, 1_000, 5_000, 30_000, 120_000, Infinity]);

function iso(time) {
    return new Date(Number.isFinite(time) ? time : Date.now()).toISOString();
}

function count(value) {
    return Number.isFinite(value) ? value : 0;
}

/** Human duration, e.g. `1m23s` / `12s` / `450ms`. */
export function formatDuration(ms) {
    const total = Math.max(0, Math.round(Number.isFinite(ms) ? ms : 0));
    if (total < 1_000)
        return `${total}ms`;
    const seconds = Math.floor(total / 1_000);
    if (seconds < 60)
        return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60)
        return `${minutes}m${seconds % 60}s`;
    const hours = Math.floor(minutes / 60);
    return `${hours}h${minutes % 60}m`;
}

function histogramLabel(index) {
    const upper = WAIT_BUCKETS[index];
    if (upper === undefined)
        return `>=${WAIT_BUCKETS[WAIT_BUCKETS.length - 1]}ms`;
    const lower = index === 0 ? 0 : WAIT_BUCKETS[index - 1];
    return upper === Infinity ? `>=${lower}ms` : `<${upper}ms`;
}

/**
 * Build the JSON state snapshot.
 *
 * @param input - `{ version, config, stats, limiters, startedAt, now }` where
 *   `limiters` is a Map of route -> ProviderLimiter.
 */
export function buildStateSnapshot(input) {
    const { version, config, stats, limiters, startedAt, now } = input;
    const aggregate = {
        queueFull: 0,
        budgetExceeded: 0,
        aborted: 0,
        disposed: 0,
        waitsByReason: { concurrency: 0, cooldown: 0, rpm: 0, tpm: 0, release: 0 },
        increases: 0,
        decreases: 0,
        waitHistogram: new Array(WAIT_BUCKETS.length).fill(0),
    };
    const routes = [];
    for (const limiter of limiters.values()) {
        const snap = limiter.snapshot();
        routes.push(snap);
        aggregate.queueFull += count(snap.sheds?.queueFull);
        aggregate.budgetExceeded += count(snap.sheds?.budgetExceeded);
        aggregate.aborted += count(snap.sheds?.aborted);
        aggregate.disposed += count(snap.sheds?.disposed);
        for (const key of Object.keys(aggregate.waitsByReason))
            aggregate.waitsByReason[key] += count(snap.waitsByReason?.[key]);
        aggregate.increases += count(snap.adaptive?.increases);
        aggregate.decreases += count(snap.adaptive?.decreases);
        const histogram = Array.isArray(snap.waitHistogram) ? snap.waitHistogram : [];
        for (let i = 0; i < aggregate.waitHistogram.length; i += 1)
            aggregate.waitHistogram[i] += count(histogram[i]);
    }
    return {
        enabled: config.enabled === true,
        version: version ?? null,
        startedAt: iso(startedAt),
        now: iso(now),
        config: { ...config },
        stats: {
            rateLimit429: count(stats?.rateLimit429),
            rateLimit503: count(stats?.rateLimit503),
            rateLimit529: count(stats?.rateLimit529),
            success: count(stats?.success),
            error: count(stats?.error),
            aborted: count(stats?.aborted),
            timeout: count(stats?.timeout),
            shed: {
                queueFull: aggregate.queueFull,
                budgetExceeded: aggregate.budgetExceeded,
                abortedWait: aggregate.aborted,
                disposed: aggregate.disposed,
            },
            waitsByReason: aggregate.waitsByReason,
            adapt: { increases: aggregate.increases, decreases: aggregate.decreases },
            waitHistogram: aggregate.waitHistogram,
        },
        limiters: routes,
    };
}

function classLine(classes) {
    if (classes === null || classes === undefined)
        return null;
    const parts = [];
    for (const classId of ['interactive', 'background', 'utility']) {
        const entry = classes[classId] ?? { queued: 0, keys: 0 };
        parts.push(`${classId} ${entry.queued}/${entry.keys}key${entry.keys === 1 ? '' : 's'}`);
    }
    return `  classes: ${parts.join(' ')}`;
}

/**
 * Render the deterministic ASCII report for the `/rate-limit` command.
 *
 * @param snapshot - a `buildStateSnapshot()` result.
 * @param filter - when non-empty, only routes with this provider name.
 */
export function renderReport(snapshot, filter) {
    const wanted = typeof filter === 'string' ? filter.trim() : '';
    const config = snapshot?.config ?? {};
    const stats = snapshot?.stats ?? {};
    const routes = [...(snapshot?.limiters ?? [])]
        .filter((route) => wanted === '' || route.provider === wanted)
        .sort((a, b) => (a.provider < b.provider ? -1 : a.provider > b.provider ? 1 : 0));
    const startedAt = Date.parse(snapshot?.startedAt ?? '');
    const now = Date.parse(snapshot?.now ?? '');
    const up = Number.isFinite(startedAt) && Number.isFinite(now) ? formatDuration(now - startedAt) : 'unknown';
    const lines = [
        `dsh-llm-rate-limit v${snapshot?.version ?? '?'} - ${routes.length} route(s), up ${up}, enabled=${snapshot?.enabled === true}`,
    ];
    for (const route of routes) {
        const adaptive = route.adaptive ?? {};
        lines.push(`route "${route.provider}": in-flight ${route.active}/${route.effectiveConcurrency} (adaptive ${adaptive.current}, floor ${adaptive.floor}, ceiling ${adaptive.ceiling}), queued ${route.queued}, peak ${route.queuedPeak}`);
        const classes = classLine(route.classes);
        if (classes !== null)
            lines.push(classes);
        lines.push(`  rpm: ${route.rpm?.used ?? 0}/${route.rpm?.limit ?? 0} per ${Math.round((route.rpm?.windowMs ?? 0) / 1000)}s, next in ${route.rpm?.nextEligibleInMs ?? 0}ms`);
        lines.push(`  tpm: ${route.tpm?.used ?? 0}/${route.tpm?.limit ?? 0} per ${Math.round((route.tpm?.windowMs ?? 0) / 1000)}s, reserved ${route.tpm?.reserved ?? 0}`);
        const cooldown = route.cooldown ?? {};
        const cooldownText = route.cooldownRemainingMs > 0 ? `${route.cooldownRemainingMs}ms` : 'none';
        lines.push(`  cooldown: ${cooldownText} (escalations ${cooldown.escalations ?? 0}, hysteresis ${cooldown.hysteresisRemainingMs ?? 0}ms, multiplier ${cooldown.multiplier ?? 1})`);
        const reasons = route.waitsByReason ?? {};
        lines.push(`  waits: ${route.waits} granted, ${route.waitTimeouts} timeout [concurrency ${reasons.concurrency ?? 0}, cooldown ${reasons.cooldown ?? 0}, rpm ${reasons.rpm ?? 0}, tpm ${reasons.tpm ?? 0}, release ${reasons.release ?? 0}]`);
        const histogram = Array.isArray(route.waitHistogram) ? route.waitHistogram : [];
        const histogramText = WAIT_BUCKETS
            .map((_, index) => `${histogramLabel(index)}:${histogram[index] ?? 0}`)
            .join(' ');
        lines.push(`  wait-histogram(ms): ${histogramText}`);
        const sheds = route.sheds ?? {};
        lines.push(`  sheds: queue-full ${sheds.queueFull ?? 0}, budget ${sheds.budgetExceeded ?? 0}, aborted ${sheds.aborted ?? 0}, disposed ${sheds.disposed ?? 0}`);
        lines.push(`  throttles: 429:${stats.rateLimit429 ?? 0} 503:${stats.rateLimit503 ?? 0} 529:${stats.rateLimit529 ?? 0}, last ${route.last429At ?? 'never'}`);
    }
    lines.push(`stats: success ${stats.success ?? 0}, error ${stats.error ?? 0}, aborted ${stats.aborted ?? 0}, wait-timeout ${stats.timeout ?? 0}, queue-full ${stats.shed?.queueFull ?? 0}, budget ${stats.shed?.budgetExceeded ?? 0}`);
    lines.push(`config: rpmLimit=${config.rpmLimit} tpmLimit=${config.tpmLimit} maxConcurrent=${config.maxConcurrent} adaptive=${config.adaptiveConcurrency} fairness=${config.fairness} spread=${config.releaseSpreadMs}ms`);
    return lines.join('\n');
}
