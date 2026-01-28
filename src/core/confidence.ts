export const CONFIDENCE_FORMULA_VERSION = 'v1';

export interface ConfidenceInputs {
    freshSourcesCount: number;
    expectedSources?: number;
    staleSourcesDroppedCount?: number;
    mismatchDetected?: boolean;
    gapDetected?: boolean;
    sequenceBroken?: boolean;
    lagDetected?: boolean;
    outlierDetected?: boolean;
    fallbackPenalty?: number;
    sourcePenalty?: number;
    sourceCap?: number;
}

export interface ConfidenceExplain {
    score: number;
    penalties: Array<{ reason: string; value: number }>;
    inputs: Record<string, number | boolean | undefined>;
}

export function computeConfidenceScore(inputs: ConfidenceInputs): number {
    return computeConfidenceExplain(inputs).score;
}

export function computeConfidenceExplain(inputs: ConfidenceInputs): ConfidenceExplain {
    const fresh = Math.max(0, inputs.freshSourcesCount ?? 0);
    const stale = Math.max(0, inputs.staleSourcesDroppedCount ?? 0);
    const expected = inputs.expectedSources ?? 0;
    const total = fresh + stale;

    const base = expected > 0 ? clamp01(fresh / expected) : total > 0 ? clamp01(fresh / total) : 0;

    const penalties: Array<{ reason: string; value: number }> = [];
    let score = base;
    if (inputs.mismatchDetected) {
        penalties.push({ reason: 'mismatchDetected', value: 0.5 });
        score *= 0.5;
    }
    if (inputs.gapDetected) {
        penalties.push({ reason: 'gapDetected', value: 0.7 });
        score *= 0.7;
    }
    if (inputs.sequenceBroken) {
        penalties.push({ reason: 'sequenceBroken', value: 0.5 });
        score *= 0.5;
    }
    if (inputs.lagDetected) {
        penalties.push({ reason: 'lagDetected', value: 0.8 });
        score *= 0.8;
    }
    if (inputs.outlierDetected) {
        penalties.push({ reason: 'outlierDetected', value: 0.8 });
        score *= 0.8;
    }
    if (inputs.fallbackPenalty !== undefined) {
        const penalty = clamp01(inputs.fallbackPenalty);
        penalties.push({ reason: 'fallbackPenalty', value: penalty });
        score *= penalty;
    }
    if (inputs.sourcePenalty !== undefined) {
        const penalty = clamp01(inputs.sourcePenalty);
        penalties.push({ reason: 'sourcePenalty', value: penalty });
        score *= penalty;
    }

    if (inputs.sourceCap !== undefined) {
        const cap = clamp01(inputs.sourceCap);
        penalties.push({ reason: 'sourceCap', value: cap });
        score = Math.min(score, cap);
    }

    return {
        score: clamp01(score),
        penalties,
        inputs: {
            freshSourcesCount: fresh,
            expectedSources: expected || undefined,
            staleSourcesDroppedCount: stale || undefined,
            mismatchDetected: inputs.mismatchDetected ?? undefined,
            gapDetected: inputs.gapDetected ?? undefined,
            sequenceBroken: inputs.sequenceBroken ?? undefined,
            lagDetected: inputs.lagDetected ?? undefined,
            outlierDetected: inputs.outlierDetected ?? undefined,
            fallbackPenalty: inputs.fallbackPenalty,
            sourcePenalty: inputs.sourcePenalty,
            sourceCap: inputs.sourceCap,
        },
    };
}

export type TrustContext = 'liquidation' | 'trade';

export interface SourceTrustAdjustments {
    sourcePenalty: number;
    sourceCap?: number;
    reasons: string[];
}

interface TrustRule {
    id: string;
    context: TrustContext;
    match: RegExp;
    penalty?: number;
    cap?: number;
}

const TRUST_RULES: TrustRule[] = [
    {
        id: 'OKX_LIQUIDATIONS_LIMITED',
        context: 'liquidation',
        match: /okx/i,
        cap: 0.7,
    },
    {
        id: 'BYBIT_BANKRUPTCY_PRICE',
        context: 'liquidation',
        match: /bybit/i,
        penalty: 0.9,
    },
    {
        id: 'BINANCE_AGGTRADE_PRECISION',
        context: 'trade',
        match: /binance/i,
        penalty: 0.95,
    },
];

export function getSourceTrustAdjustments(sources: string[], context: TrustContext): SourceTrustAdjustments {
    let sourcePenalty = 1;
    let sourceCap = 1;
    const reasons = new Set<string>();

    for (const source of sources) {
        for (const rule of TRUST_RULES) {
            if (rule.context !== context) continue;
            if (!rule.match.test(source)) continue;
            if (rule.penalty !== undefined) sourcePenalty *= clamp01(rule.penalty);
            if (rule.cap !== undefined) sourceCap = Math.min(sourceCap, clamp01(rule.cap));
            reasons.add(rule.id);
        }
    }

    return {
        sourcePenalty,
        sourceCap: sourceCap < 1 ? sourceCap : undefined,
        reasons: Array.from(reasons).sort(),
    };
}

function clamp01(value: number): number {
    if (!Number.isFinite(value)) return 0;
    return Math.max(0, Math.min(1, value));
}
