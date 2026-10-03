/**
 * Token estimation and usage settlement for the TPM budget.
 *
 * Estimation runs once per admission and only when `tpmLimit > 0`; with the
 * TPM budget disabled the reservation is 0, so no `tokenMeter` call and no
 * payload scan happen at all. The estimate is a *reservation*: the stream's
 * `usage` chunk replaces it, and a request that never reports usage keeps its
 * full reservation until it expires with the window.
 *
 * A missing `tokenMeter` service, a throwing `estimateMessage`, a non-JSON
 * message, and an odd `usage` object are all non-fatal.
 *
 * @module dsh-llm-rate-limit/tokens
 */

/** Local fallback: 4 characters per token, rounded up. */
function charsToTokens(value) {
    if (typeof value !== 'string' || value.length === 0)
        return 0;
    return Math.ceil(value.length / 4);
}

function safeLength(value) {
    return typeof value === 'string' ? value.length : 0;
}

function jsonLength(value) {
    try {
        const text = JSON.stringify(value);
        return typeof text === 'string' ? text.length : 0;
    }
    catch {
        return 0;
    }
}

/**
 * Estimate the input tokens of one request.
 *
 * @param options - `GenerateOptions` (read-only; never mutated).
 * @param tokenMeter - optional host service with `estimateMessage(message)`.
 * @returns a non-negative integer estimate.
 */
export function estimateInputTokens(options, tokenMeter) {
    const request = options ?? {};
    let total = 0;
    total += charsToTokens(request.system);
    for (const schema of Array.isArray(request.tools) ? request.tools : []) {
        if (schema === undefined || schema === null)
            continue;
        const parameters = jsonLength(schema.parameters ?? {});
        total += Math.ceil((safeLength(schema.name) + safeLength(schema.description) + parameters) / 4);
    }
    for (const message of Array.isArray(request.messages) ? request.messages : []) {
        if (message === undefined || message === null)
            continue;
        const usableMeter = tokenMeter !== undefined
            && tokenMeter !== null
            && typeof tokenMeter.estimateMessage === 'function';
        if (usableMeter && typeof message === 'object' && message.source !== undefined) {
            try {
                const measured = tokenMeter.estimateMessage(message);
                if (Number.isFinite(measured) && measured > 0) {
                    total += measured;
                    continue;
                }
            }
            catch {
                // fall through to the local heuristic
            }
        }
        total += Math.ceil(jsonLength(message) / 4);
    }
    return Math.max(0, Math.ceil(total));
}

/**
 * The tokens one request reserves against the TPM window: the input estimate
 * plus the output reservation (`options.maxTokens` when it is a positive
 * number, otherwise `tpmOutputReserveTokens`).
 *
 * @returns 0 when the TPM budget is disabled.
 */
export function reservationFor(options, config, tokenMeter) {
    if (!Number.isFinite(config?.tpmLimit) || config.tpmLimit <= 0)
        return 0;
    const maxTokens = options?.maxTokens;
    const output = Number.isFinite(maxTokens) && maxTokens > 0
        ? maxTokens
        : config.tpmOutputReserveTokens;
    return estimateInputTokens(options, tokenMeter) + Math.max(0, Number.isFinite(output) ? output : 0);
}

/**
 * The charge implied by a `usage` chunk.
 *
 * @param usage - a `TokenUsage` object (or anything else).
 * @returns a non-negative finite number, or `null` when no usable usage was
 *   reported. Never `NaN`.
 */
export function usageTokens(usage) {
    if (usage === undefined || usage === null || typeof usage !== 'object')
        return null;
    if (Number.isFinite(usage.totalTokens))
        return Math.max(0, usage.totalTokens);
    const input = Number.isFinite(usage.inputTokens) ? usage.inputTokens : null;
    const output = Number.isFinite(usage.outputTokens) ? usage.outputTokens : null;
    if (input === null && output === null)
        return null;
    return Math.max(0, (input ?? 0) + (output ?? 0));
}
