/**
 * Settings card: live state of the LLM rate-limit gate.
 *
 * Reads /plugins/dsh-llm-rate-limit/state (GET) and renders, per provider
 * route, the in-flight / queued / cooldown counters plus the global stats.
 * The limiter itself is configured in the bundle patch (see
 * cordis.patch.yml); this panel is read-only because changing the gate
 * mid-composition would require re-resolving the provider adapters, and
 * HMR of the patch is the supported reload path.
 *
 * @module dsh-llm-rate-limit/client
 */
window.__ModuleLoader__.load({
    id: "dsh-llm-rate-limit",
    factory: (require) => {
        var module = { exports: {} };
        var exports = module.exports;
        Object.defineProperty(exports, Symbol.toStringTag, { value: "Module" });
        let react = require("react");
        let jsx_runtime = require("react/jsx-runtime");

        const zh = {
            nav: "LLM 限流",
            title: "LLM 速率限制门控",
            hint: "本部署对出站 LLM 请求做并发门控，避免触发提供方 429。观察到 429 时会自动冷却一段时间（尊重 Retry-After 并逐级加码），冷却期间新请求排队。",
            configTitle: "当前配置",
            providers: "门控路由",
            maxConcurrent: "最大并发",
            maxWait: "排队上限 (秒)",
            cooldownBase: "冷却基础 (秒)",
            cooldownMax: "冷却上限 (秒)",
            liveTitle: "实时状态",
            inFlight: "在途",
            queued: "排队",
            queuePeak: "峰值排队",
            grants: "放行了多少次",
            waitTimeouts: "排队超时",
            cooldown: "冷却剩余",
            cooldownNone: "无",
            last429: "最近一次 429",
            last429None: "从未",
            statsTitle: "统计（本次启动以来）",
            stat429: "观测到 429",
            statSuccess: "成功流",
            statError: "失败流",
            statTimeout: "排队超时拒绝",
            enabled: "已启用",
            disabled: "未启用",
            error: "加载失败",
            reload: "刷新",
            emptyProviders: "未指定路由 → 门控所有路由",
        };
        const en = {
            nav: "LLM rate limit",
            title: "LLM rate-limit gate",
            hint: "Outbound LLM requests pass a concurrency gate to stay under the provider's limit. Observed 429s open an adaptive cooldown (honoring Retry-After, escalating per consecutive 429); queued requests wait for it to close.",
            configTitle: "Current configuration",
            providers: "Gated routes",
            maxConcurrent: "Max concurrent",
            maxWait: "Queue cap (s)",
            cooldownBase: "Cooldown base (s)",
            cooldownMax: "Cooldown cap (s)",
            liveTitle: "Live state",
            inFlight: "in flight",
            queued: "queued",
            queuePeak: "peak queue",
            grants: "grants",
            waitTimeouts: "wait timeouts",
            cooldown: "cooldown left",
            cooldownNone: "none",
            last429: "last 429",
            last429None: "never",
            statsTitle: "Stats (since start)",
            stat429: "observed 429s",
            statSuccess: "successful streams",
            statError: "failed streams",
            statTimeout: "queued-timeout refusals",
            enabled: "enabled",
            disabled: "disabled",
            error: "Failed to load",
            reload: "Reload",
            emptyProviders: "no routes listed → all routes are gated",
        };

        const css = {
            root: {
                boxSizing: "border-box",
                display: "flex",
                flexDirection: "column",
                gap: 14,
                width: "100%",
                minWidth: 0,
                padding: 12,
            },
            h2: { margin: "0 0 2px", fontSize: 15, fontWeight: 600, color: "var(--dsw-alias-label-primary, inherit)" },
            hint: { margin: 0, fontSize: 12, lineHeight: 1.55, color: "var(--dsw-alias-label-tertiary, #888)" },
            block: { display: "flex", flexDirection: "column", gap: 6 },
            label: { fontSize: 12, fontWeight: 600, color: "var(--dsw-alias-label-secondary, #666)" },
            kv: {
                display: "flex",
                gap: 8,
                flexWrap: "wrap",
                alignItems: "baseline",
                fontSize: 12.5,
            },
            k: { color: "var(--dsw-alias-label-tertiary, #777)", minWidth: 130 },
            v: {
                fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
                color: "var(--dsw-alias-label-primary, #111)",
            },
            item: {
                display: "flex",
                flexDirection: "column",
                gap: 4,
                border: "1px solid var(--dsw-alias-border-l2, #ddd)",
                borderRadius: 10,
                padding: 10,
            },
            itemKey: {
                fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
                fontSize: 11.5,
                color: "var(--dsw-alias-label-tertiary, #777)",
            },
            badge: (active) => ({
                fontSize: 11,
                padding: "2px 8px",
                borderRadius: 999,
                background: active ? "var(--dsw-alias-state-business-primary, #2563eb)" : "var(--dsw-alias-bg-layer-1, #eee)",
                color: active ? "#fff" : "var(--dsw-alias-label-tertiary, #666)",
                whiteSpace: "nowrap",
            }),
            errorText: { fontSize: 12, color: "var(--dsw-alias-label-error, #d33)" },
            btn: {
                fontFamily: "inherit",
                fontSize: 13,
                padding: "6px 14px",
                borderRadius: 8,
                border: "1px solid var(--dsw-alias-border-l3, #bbb)",
                background: "var(--dsw-alias-bg-module-platform, #f5f5f5)",
                color: "var(--dsw-alias-label-primary, #111)",
                cursor: "pointer",
            },
        };

        function fetchState() {
            return fetch("/plugins/dsh-llm-rate-limit/state", { cache: "no-store" }).then(async (r) => {
                if (!r.ok)
                    throw new Error(`state ${r.status}`);
                return r.json();
            });
        }

        function seconds(ms) {
            return Math.round((ms ?? 0) / 1000);
        }

        function RateLimitPanel({ t, locale }) {
            // The host LocaleFace's getSnapshot/subscribe pair is uSES-safe, so
            // the panel re-renders live when the active locale switches.
            react.useSyncExternalStore(
                (onStoreChange) => locale?.subscribe?.(onStoreChange) ?? (() => { }),
                () => locale?.getSnapshot?.() ?? null,
            );
            const [state, setState] = react.useState(null);
            const [error, setError] = react.useState(null);
            const load = react.useCallback(() => {
                setError(null);
                fetchState()
                    .then((s) => setState(s))
                    .catch((e) => setError(e.message ?? String(e)));
            }, []);
            react.useEffect(() => {
                load();
                const timer = setInterval(load, 5000);
                return () => clearInterval(timer);
            }, [load]);

            const cfg = state?.config ?? {};
            const stats = state?.stats ?? {};
            const limiters = state?.limiters ?? [];
            const cooldownActive = limiters.some((l) => (l.cooldownRemainingMs ?? 0) > 0);

            return (jsx_runtime.jsxs)("div", {
                style: css.root,
                children: [
                    jsx_runtime.jsxs("div", {
                        children: [
                            jsx_runtime.jsxs("h2", {
                                style: css.h2,
                                children: [
                                    t("title"),
                                    jsx_runtime.jsx("span", {
                                        style: { ...css.badge(cooldownActive), marginLeft: 8 },
                                        children: state?.enabled ? t("enabled") : t("disabled"),
                                    }),
                                ]
                            }),
                            jsx_runtime.jsx("p", { style: css.hint, children: t("hint") }),
                        ]
                    }),
                    jsx_runtime.jsxs("div", {
                        style: css.block,
                        children: [
                            jsx_runtime.jsx("span", { style: css.label, children: t("configTitle") }),
                            jsx_runtime.jsxs("div", {
                                style: css.kv,
                                children: [
                                    jsx_runtime.jsx("span", { style: css.k, children: t("providers") }),
                                    jsx_runtime.jsx("span", {
                                        style: css.v,
                                        children: cfg.providers?.length > 0 ? cfg.providers.join(", ") : t("emptyProviders")
                                    }),
                                ]
                            }),
                            jsx_runtime.jsxs("div", {
                                style: css.kv,
                                children: [
                                    jsx_runtime.jsx("span", { style: css.k, children: t("maxConcurrent") }),
                                    jsx_runtime.jsx("span", { style: css.v, children: String(cfg.maxConcurrent ?? "—") }),
                                    jsx_runtime.jsx("span", { style: css.k, marginLeft: 24, children: t("maxWait") }),
                                    jsx_runtime.jsx("span", { style: css.v, children: cfg.maxWaitMs === undefined ? "—" : seconds(cfg.maxWaitMs) }),
                                ]
                            }),
                            jsx_runtime.jsxs("div", {
                                style: css.kv,
                                children: [
                                    jsx_runtime.jsx("span", { style: css.k, children: t("cooldownBase") }),
                                    jsx_runtime.jsx("span", { style: css.v, children: cfg.cooldownBaseMs === undefined ? "—" : seconds(cfg.cooldownBaseMs) }),
                                    jsx_runtime.jsx("span", { style: css.k, marginLeft: 24, children: t("cooldownMax") }),
                                    jsx_runtime.jsx("span", { style: css.v, children: cfg.cooldownMaxMs === undefined ? "—" : seconds(cfg.cooldownMaxMs) }),
                                ]
                            }),
                        ]
                    }),
                    jsx_runtime.jsxs("div", {
                        style: css.block,
                        children: [
                            jsx_runtime.jsx("span", { style: css.label, children: t("liveTitle") }),
                            ...(limiters.length > 0
                                ? limiters.map((l) => (jsx_runtime.jsxs)("div", {
                                    style: css.item,
                                    key: l.provider,
                                    children: [
                                        jsx_runtime.jsxs("div", {
                                            style: css.kv,
                                            children: [
                                                jsx_runtime.jsx("span", { style: css.itemKey, children: l.provider }),
                                                jsx_runtime.jsx("span", {
                                                    style: css.badge((l.cooldownRemainingMs ?? 0) > 0),
                                                    children: (l.cooldownRemainingMs ?? 0) > 0 ? `${t("cooldown")} ${seconds(l.cooldownRemainingMs)}s` : t("cooldownNone")
                                                }),
                                            ]
                                        }),
                                        jsx_runtime.jsxs("div", {
                                            style: css.kv,
                                            children: [
                                                jsx_runtime.jsx("span", { style: css.k, children: t("inFlight") }),
                                                jsx_runtime.jsx("span", { style: css.v, children: String(l.active ?? 0) }),
                                                jsx_runtime.jsx("span", { style: css.k, marginLeft: 24, children: t("queued") }),
                                                jsx_runtime.jsx("span", { style: css.v, children: String(l.queued ?? 0) }),
                                                jsx_runtime.jsx("span", { style: css.k, marginLeft: 24, children: t("queuePeak") }),
                                                jsx_runtime.jsx("span", { style: css.v, children: String(l.queuedPeak ?? 0) }),
                                            ]
                                        }),
                                        jsx_runtime.jsxs("div", {
                                            style: css.kv,
                                            children: [
                                                jsx_runtime.jsx("span", { style: css.k, children: t("grants") }),
                                                jsx_runtime.jsx("span", { style: css.v, children: String(l.waits ?? 0) }),
                                                jsx_runtime.jsx("span", { style: css.k, marginLeft: 24, children: t("waitTimeouts") }),
                                                jsx_runtime.jsx("span", { style: css.v, children: String(l.waitTimeouts ?? 0) }),
                                                jsx_runtime.jsx("span", { style: css.k, marginLeft: 24, children: t("last429") }),
                                                jsx_runtime.jsx("span", { style: css.v, children: l.last429At ?? t("last429None") }),
                                            ]
                                        }),
                                    ]
                                }, l.provider))
                                : jsx_runtime.jsx("p", { style: css.hint, children: "…" })),
                        ]
                    }),
                    jsx_runtime.jsxs("div", {
                        style: css.block,
                        children: [
                            jsx_runtime.jsx("span", { style: css.label, children: t("statsTitle") }),
                            jsx_runtime.jsxs("div", {
                                style: css.kv,
                                children: [
                                    jsx_runtime.jsx("span", { style: css.k, children: t("stat429") }),
                                    jsx_runtime.jsx("span", { style: css.v, children: String(stats.rateLimit429 ?? 0) }),
                                    jsx_runtime.jsx("span", { style: css.k, marginLeft: 24, children: t("statSuccess") }),
                                    jsx_runtime.jsx("span", { style: css.v, children: String(stats.success ?? 0) }),
                                    jsx_runtime.jsx("span", { style: css.k, marginLeft: 24, children: t("statError") }),
                                    jsx_runtime.jsx("span", { style: css.v, children: String(stats.error ?? 0) }),
                                    jsx_runtime.jsx("span", { style: css.k, marginLeft: 24, children: t("statTimeout") }),
                                    jsx_runtime.jsx("span", { style: css.v, children: String(stats.timeout ?? 0) }),
                                ]
                            }),
                        ]
                    }),
                    error !== null && (jsx_runtime.jsx("div", { style: css.errorText, role: "alert", children: `${t("error")}: ${error}` })),
                    jsx_runtime.jsx("button", {
                        style: css.btn,
                        onClick: load,
                        children: t("reload"),
                    }),
                ]
            });
        }

        const name = "dsh-llm-rate-limit";
        /** Dictionary namespace owned by this plugin (registered on the host LocaleRuntime). */
        const NS = "dsh.llm-rate-limit";
        // The module face must declare EVERY ctx.<service> apply() touches: the
        // host ctx is a proxy and reading an undeclared service throws at
        // activation (a FAILED fiber -> "1 entry did not activate"). apply()
        // below calls ctx.locale AND ctx.slots (settings.section /
        // settings.plugins.tab), so both are declared — matching every host
        // reference plugin that registers into settings slots.
        const inject = ["locale", "slots"];

        function apply(ctx) {
            // Host i18n idiom (same as ui-settings-plugins & co): register the
            // BCP-47 dictionaries on the LocaleRuntime and bind the namespace.
            // The active-language fallback chain is resolved by the host — a
            // plugin must NOT sniff ctx.locale.getLocale() itself (it is a
            // frozen snapshot object, not a language string).
            ctx.effect(() => ctx.locale.register(NS, {
                zh,
                en
            }), "dsh-llm-rate-limit: dictionaries");
            const t = ctx.locale.bind(NS);
            ctx.slots.inject("settings.section", () => {
                ctx.slots.register({
                    name: "settings.section",
                    id: "dsh-llm-rate-limit",
                    order: 46,
                    label: () => t("nav"),
                }, () => jsx_runtime.jsx(RateLimitPanel, { t, locale: ctx.locale }));
            });
            ctx.slots.inject("settings.plugins.tab", () => {
                ctx.slots.register({
                    name: "settings.plugins.tab",
                    id: "dsh-llm-rate-limit",
                    order: 72,
                    label: () => t("nav"),
                }, () => jsx_runtime.jsx(RateLimitPanel, { t, locale: ctx.locale }));
            });
        }

        exports.apply = apply;
        exports.inject = inject;
        exports.name = name;
        return module.exports;
    },
});
