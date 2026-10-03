/**
 * dsh-llm-rate-limit — keep outbound LLM requests *under* provider rate
 * limits instead of recovering from 429s.
 *
 * The v2 gate sits on the `llm/stream` waterfall and admits every model call
 * through, in order of the hard gates it can enforce locally:
 *
 * 1. concurrency — a static per-route bound, or an AIMD bound that grows on
 *    clean settles and halves on a throttle;
 * 2. cooldown — a Retry-After-aware window opened by observed 429/503/529s,
 *    with the v1 escalation ladder, optional jitter, and optional hysteresis;
 * 3. an exact sliding-window RPM budget;
 * 4. a sliding-window TPM budget (reserved at admission, settled by the
 *    stream's usage chunk);
 * 5. drip release, so a drained queue does not burst back in one tick.
 *
 * Waiting requests are queued by priority class (interactive / background /
 * utility) with per-session round-robin inside a class, so no class is starved
 * and one session's fan-out cannot take every slot.
 *
 * The plugin never retries and never mutates `options`: recovery stays with
 * the built-in `dsh-llm-retry`, and `agent/request-error` is used only to feed
 * the cooldown ladder. Everything is non-fatal — a host without the `llm`
 * service, the `webServer`, the `commands` service, the `tokenMeter`, or the
 * `systemPrompt` service simply degrades and logs.
 *
 * @module dsh-llm-rate-limit
 */
import { resolveConfig } from './config.js';
import { createLimiters, limiterFor, gateStream } from './limiter.js';
import { reservationFor } from './tokens.js';
import { buildStateSnapshot, renderReport } from './observability.js';

export const name = 'dsh-llm-rate-limit';

/** Package version, mirrored by package.json (asserted by the test suite). */
export const VERSION = '2.0.0';

/** Maps an emitted failure code onto the wait/shed reason used in logs. */
const SHED_REASONS = Object.freeze({
    RATE_LIMIT_GATE_TIMEOUT: 'wait-timeout',
    RATE_LIMIT_QUEUE_FULL: 'queue-full',
    RATE_LIMIT_BUDGET_EXCEEDED: 'budget-exceeded',
    RATE_LIMIT_GATE_ABORTED: 'aborted',
    RATE_LIMIT_GATE_DISPOSED: 'disposed',
});

/**
 * Mount the rate-limit gate into the host composition.
 *
 * @param ctx - the plugin context (cordis-style).
 * @param rawConfig - the raw `config:` block from the bundle patch.
 */
export function apply(ctx, rawConfig) {
    const config = resolveConfig(rawConfig, (message) => log(ctx, message));
    if (!config.enabled) {
        log(ctx, 'rate-limit gate disabled by configuration');
        return;
    }

    const limiters = createLimiters(config.providers, config);
    /** A route is gated when no explicit provider list is configured (all
     *  routes), or when the route has its own limiter entry. */
    const isGoverned = (provider) => config.providers.length === 0 || limiters.has(provider);
    const stats = { rateLimit429: 0, rateLimit503: 0, rateLimit529: 0, success: 0, error: 0, aborted: 0, timeout: 0 };
    const startedAt = Date.now();

    /** Non-strict service lookup: an absent service must never throw. */
    const getService = (serviceName) => {
        try {
            return ctx.get?.(serviceName);
        }
        catch {
            return undefined;
        }
    };
    const stateSnapshot = () => buildStateSnapshot({
        version: VERSION,
        config,
        stats,
        limiters,
        startedAt,
        now: Date.now(),
    });

    /** Fired exactly once per gated request by gateStream. */
    const onSettled = (kind, details, provider) => {
        if (stats[kind] !== undefined)
            stats[kind] += 1;
        const shedCode = details?.shedCode ?? null;
        if (shedCode === null)
            return;
        // v1 counted every gate-derived terminal chunk as a wait timeout.
        if (kind === 'aborted')
            stats.timeout += 1;
        log(ctx, `dsh-llm-rate-limit: shed ${shedCode} on "${provider}" (${SHED_REASONS[shedCode] ?? 'refused'})`);
    };

    // Periodic expiry sweep so queued requests cannot wait past maxWaitMs
    // even when no release event fires (the per-limiter ref'd deadline anchor
    // is what actually wakes expiry promptly).
    const expirySweep = setInterval(() => {
        for (const limiter of limiters.values())
            limiter.expireWaiters();
    }, 5_000);
    expirySweep.unref?.();
    ctx.effect(() => () => clearInterval(expirySweep), 'dsh-llm-rate-limit: expiry sweep');

    // One effect tears down every limiter: timers, queued waiters and
    // outstanding tickets, so a reload cannot fire into the next generation.
    ctx.effect(() => () => {
        for (const limiter of limiters.values())
            limiter.dispose();
    }, 'dsh-llm-rate-limit: dispose limiters');

    // ---- llm/stream gate -------------------------------------------------
    // The LLM service dispatches `llm/stream` on every ctx.llm.stream() call;
    // our listener sits outermost-last (registered at apply time) and wraps
    // the downstream chunk stream with the admission gate.
    let streamListener = null;
    const registerStreamGate = () => {
        if (streamListener !== null || typeof ctx.on !== 'function')
            return;
        ctx.effect(() => {
            // The host runs `llm/stream` as a *synchronous* waterfall: each
            // listener must return the (possibly wrapped) chunk stream, and the
            // outermost value is what `stream()` hands to `for await`/`yield*`.
            // An `async` listener would return a `Promise` instead of that
            // stream, which is what threw `yield* … is not async iterable`
            // when the host advanced the stream. Keep this listener synchronous:
            // call `next()` (it returns the inner generator synchronously) and
            // hand `gateStream(...)` straight back as the yielded value.
            streamListener = ctx.on('llm/stream', (options, next) => {
                if (options === undefined || options?.provider === undefined)
                    return next();
                if (!isGoverned(options.provider))
                    return next();
                const limiter = limiterFor(limiters, options.provider);
                if (limiter === undefined)
                    return next();
                const source = next();
                // Only pay for estimation when the TPM budget is enforced.
                const estimatedTokens = config.tpmLimit > 0
                    ? reservationFor(options, config, getService('tokenMeter'))
                    : 0;
                return gateStream(
                    source,
                    limiter,
                    options.signal,
                    (kind, details) => onSettled(kind, details, options.provider),
                    {
                        purpose: options.purpose,
                        sessionId: options.sessionId,
                        estimatedTokens,
                    },
                );
            });
            return () => {
                if (streamListener !== null) {
                    streamListener();
                    streamListener = null;
                }
            };
        }, 'dsh-llm-rate-limit: llm/stream listener');
    };

    // ---- agent/request-error: cooldown ladder on throttles ----------------
    let requestErrorListener = null;
    const registerRequestError = () => {
        if (requestErrorListener !== null || typeof ctx.on !== 'function')
            return;
        ctx.effect(() => {
            requestErrorListener = ctx.on('agent/request-error', (payload, next) => {
                const failure = payload?.failure;
                const code = failure?.code;
                const status = failure?.status;
                const throttled = code === 'RATE_LIMIT'
                    || code === 'QUOTA'
                    || code === 'OVERLOADED'
                    || status === 429
                    || status === 503
                    || status === 529;
                if (!throttled)
                    return next();
                const provider = payload?.provider;
                if (provider === undefined || !isGoverned(provider))
                    return next();
                const limiter = limiterFor(limiters, provider);
                if (limiter !== undefined) {
                    const delayMs = Number.isFinite(failure?.providerRetryAfterMs)
                        ? failure.providerRetryAfterMs
                        : undefined;
                    limiter.observe429(delayMs, config, status);
                    if (status === 503)
                        stats.rateLimit503 += 1;
                    else if (status === 529)
                        stats.rateLimit529 += 1;
                    else
                        stats.rateLimit429 += 1;
                    const label = code ?? (status !== undefined ? `HTTP ${status}` : 'throttle');
                    log(ctx, `observed ${label} on "${provider}": cooldown now ${Math.round(limiter.cooldownMs / 1_000)}s (retryAfter=${delayMs ?? 'n/a'}, escalations=${limiter.cooldownEscalations})`);
                }
                // Hand the failure to the rest of the chain (the built-in
                // dsh-llm-retry still schedules its step-level retry). The
                // plugin never owns recovery and never returns { kind: 'retry' }.
                return next();
            });
            return () => {
                if (requestErrorListener !== null) {
                    requestErrorListener();
                    requestErrorListener = null;
                }
            };
        }, 'dsh-llm-rate-limit: request-error listener');
    };

    // ---- one web route for the Settings card ------------------------------
    let webRegistered = false;
    const registerWeb = () => {
        if (webRegistered)
            return;
        const rawWebServer = getService('webServer') ?? getService('httpServer');
        if (rawWebServer === undefined)
            return;
        webRegistered = true;
        const connection = () => getService('connection');
        const server = {
            register(route) {
                return rawWebServer.register({
                    ...route,
                    async handler(req, res) {
                        const gate = connection();
                        if (gate !== undefined) {
                            const rejection = gate.requestRejection?.(req);
                            if (rejection !== undefined) {
                                res.writeHead(rejection, {
                                    'content-type': 'application/json; charset=utf-8',
                                    'cache-control': 'no-store',
                                });
                                res.end(JSON.stringify({
                                    error: rejection === 503 ? 'authentication unavailable'
                                        : rejection === 401 ? 'unauthorized'
                                        : 'forbidden',
                                }));
                                return;
                            }
                        }
                        await route.handler(req, res);
                    },
                });
            },
        };
        ctx.effect(() => server.register({
            kind: 'exact',
            path: '/plugins/dsh-llm-rate-limit/state',
            handler: async (req, res) => {
                if (req.method !== 'GET') {
                    res.writeHead(405, { allow: 'GET', 'cache-control': 'no-store' });
                    res.end();
                    return;
                }
                res.writeHead(200, {
                    'content-type': 'application/json; charset=utf-8',
                    'cache-control': 'no-store',
                });
                res.end(JSON.stringify(stateSnapshot()));
            },
        }), 'dsh-llm-rate-limit: state route');
    };

    // ---- /rate-limit command ---------------------------------------------
    let commandRegistered = false;
    const registerCommand = () => {
        if (commandRegistered)
            return;
        const commands = getService('commands');
        if (commands === undefined || typeof commands.register !== 'function')
            return;
        commandRegistered = true;
        try {
            ctx.effect(() => commands.register({
                name: 'rate-limit',
                description: 'Show live dsh-llm-rate-limit state (per-route queues, budgets, adaptive limit, cooldown, sheds).',
                input: { hint: '[route]' },
                handler: (invocation) => {
                    try {
                        const requested = invocation?.rawInput?.trim() ?? '';
                        // §5.7 C: unknown input filters nothing, so an empty or
                        // unrecognised route name reports every route instead of
                        // "0 route(s)". renderReport itself keeps its pure
                        // "filter exactly what you are given" semantics, which
                        // the verifier's claim test pins for an unknown filter.
                        const filter = requested !== '' && limiters.has(requested) ? requested : '';
                        return {
                            kind: 'success',
                            text: renderReport(stateSnapshot(), filter),
                        };
                    }
                    catch (error) {
                        return { kind: 'error', text: `dsh-llm-rate-limit: ${error?.message ?? String(error)}` };
                    }
                },
            }), 'dsh-llm-rate-limit: command');
        }
        catch (error) {
            commandRegistered = false;
            log(ctx, `commands.register failed (${error?.message ?? String(error)}) — /rate-limit command skipped`);
        }
    };

    // ---- optional model-visible notice ------------------------------------
    // `systemPrompt` is a host service: reading ctx.systemPrompt directly
    // without an inject declaration throws at proxy level, so go through the
    // non-strict ctx.get accessor (undefined when the service is absent).
    const registerPromptSection = () => {
        if (!config.systemPrompt)
            return;
        const systemPromptService = getService('systemPrompt');
        if (systemPromptService !== undefined && typeof systemPromptService.section === 'function') {
            ctx.effect(() => systemPromptService.section({
                name: 'dsh-llm-rate-limit:gating',
                order: config.sectionOrder,
                text: () => {
                    return [
                        'Note: this deployment gates outbound LLM requests with a concurrency limiter to stay under the provider rate limit. A model request that cannot obtain a slot is queued, and provider 429 responses pause new requests for a short adaptive cooldown. If you see repeated RATE_LIMIT failures, reduce parallel work (fewer subagents / parallel tool batches) so the gate can drain its queue.',
                    ].join('\n');
                },
            }), 'dsh-llm-rate-limit: prompt section');
        }
        else {
            log(ctx, 'systemPrompt service unavailable — prompt section skipped');
        }
    };

    // ---- service-appearance hook -----------------------------------------
    // `llm`, `webServer`/`httpServer` and `commands` may be instantiated after
    // this plugin's apply(); re-register lazily on first announcement.
    let serviceListener = null;
    const registerServiceHook = () => {
        if (serviceListener !== null || typeof ctx.on !== 'function')
            return;
        ctx.effect(() => {
            serviceListener = ctx.on('internal/service', (service) => {
                if (service === 'llm' || service === 'LLMRuntime') {
                    registerStreamGate();
                    return;
                }
                if (service === 'webServer' || service === 'httpServer') {
                    registerWeb();
                    return;
                }
                if (service === 'commands')
                    registerCommand();
            });
            return () => {
                if (serviceListener !== null) {
                    serviceListener();
                    serviceListener = null;
                }
            };
        }, 'dsh-llm-rate-limit: internal/service listener');
    };

    // All registrations are synchronous, so a test (or the host) can inspect
    // the effect list immediately after apply() returns.
    registerStreamGate();
    registerRequestError();
    registerWeb();
    registerCommand();
    registerPromptSection();
    registerServiceHook();

    log(ctx, `rate-limit gate enabled: ${config.maxConcurrent} concurrent / ${limiters.size} route(s), cooldown base ${config.cooldownBaseMs}ms max ${config.cooldownMaxMs}ms; rpm=${config.rpmLimit === 0 ? 'off' : `${config.rpmLimit}/${config.rpmWindowMs}ms`} tpm=${config.tpmLimit === 0 ? 'off' : `${config.tpmLimit}/${config.tpmWindowMs}ms`} adaptive=${config.adaptiveConcurrency} fairness=${config.fairness} spread=${config.releaseSpreadMs}ms jitter=${config.cooldownJitter} hysteresis=${config.cooldownHysteresisMs}ms`);
}

/**
 * Emit one plugin log line through either scoped-logger shape the host may
 * publish.
 * @param ctx - the plugin context.
 * @param message - the line to log.
 */
function log(ctx, message) {
    const logger = ctx.logger;
    if (typeof logger === 'function')
        logger('dsh-llm-rate-limit').info?.(message);
    else
        logger?.info?.(message);
}
