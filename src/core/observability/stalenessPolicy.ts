import type { KnownMarketType } from '../events/EventBus';

export type StalenessPolicyRule = {
    topic: string;
    expectedIntervalMs: number;
    staleThresholdMs: number;
    marketType?: KnownMarketType;
    symbol?: string;
    startupGraceMs?: number;
    minSamples?: number;
};

export type ResolvedStalenessPolicy = {
    expectedIntervalMs: number;
    staleThresholdMs: number;
    startupGraceMs: number;
    minSamples: number;
};

export const DEFAULT_STALENESS_POLICY_RULES: StalenessPolicyRule[] = [
    // OI: collectors/pollers often return exchange snapshots on a multi-minute cadence.
    { topic: 'market:oi_agg', expectedIntervalMs: 5 * 60_000, staleThresholdMs: 15 * 60_000, startupGraceMs: 60_000, minSamples: 2 },
    // Funding: the economically meaningful cadence is hours (usually 8h).
    {
        topic: 'market:funding_agg',
        expectedIntervalMs: 8 * 60 * 60_000,
        staleThresholdMs: 12 * 60 * 60_000,
        startupGraceMs: 120_000,
        minSamples: 1,
    },
];

export function resolveStalenessPolicy(
    rules: readonly StalenessPolicyRule[],
    input: { topic: string; symbol?: string; marketType?: KnownMarketType }
): ResolvedStalenessPolicy | undefined {
    let best: StalenessPolicyRule | undefined;
    let bestScore = -1;

    for (const rule of rules) {
        if (rule.topic !== input.topic) continue;
        if (rule.symbol && input.symbol && rule.symbol !== input.symbol) continue;
        if (rule.symbol && !input.symbol) continue;
        if (rule.marketType && input.marketType && rule.marketType !== input.marketType) continue;
        if (rule.marketType && !input.marketType) continue;

        // Prefer more specific rules: topic+symbol+marketType > topic+symbol > topic+marketType > topic.
        let score = 0;
        if (rule.symbol) score += 4;
        if (rule.marketType) score += 2;
        if (score > bestScore) {
            best = rule;
            bestScore = score;
        }
    }

    if (!best) return undefined;
    return {
        expectedIntervalMs: best.expectedIntervalMs,
        staleThresholdMs: Math.max(best.expectedIntervalMs, best.staleThresholdMs),
        startupGraceMs: Math.max(0, best.startupGraceMs ?? 0),
        minSamples: Math.max(1, Math.floor(best.minSamples ?? 1)),
    };
}

