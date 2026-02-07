import {
    asTsMs,
    createMeta,
    nowMs,
    type AggregatedQualityFlags,
    type EventBus,
    type KnownMarketType,
    type MarketCvdAggEvent,
    type MarketCvdEvent,
} from '../core/events/EventBus';
import { computeConfidenceExplain, computeConfidenceScore, getSourceTrustAdjustments } from '../core/confidence';
import { stableRecordFromEntries, stableSortStrings } from '../core/determinism';
import { sourceRegistry } from '../core/market/SourceRegistry';
import { normalizeMarketType, normalizeSymbol } from '../core/market/symbols';
import { logger } from '../infra/logger';

export type CvdMismatchType = 'NONE' | 'SIGN' | 'DISPERSION';
export type CvdMismatchReason = Exclude<CvdMismatchType, 'NONE'>;

export interface CvdMismatchPolicy {
    ewmaAlpha: number;
    minEwmaAbs: number;
    minAbsScaled: number;
    minScale: number;
    maxScale: number;
    signAgreementThreshold: number;
    zThresh: number;
    zMax: number;
    ratioThresh: number;
    ratioMax: number;
    penaltySign: number;
    penaltyDispersion: number;
}

export const DEFAULT_CVD_MISMATCH_POLICY: CvdMismatchPolicy = {
    ewmaAlpha: 0.2,
    minEwmaAbs: 1e-6,
    minAbsScaled: 1e-3,
    minScale: 0.25,
    maxScale: 4,
    signAgreementThreshold: 2 / 3,
    zThresh: 3,
    zMax: 6,
    ratioThresh: 4,
    ratioMax: 6,
    penaltySign: 0.5,
    penaltyDispersion: 0.85,
};

export interface CvdAggregatorOptions {
    ttlMs?: number;
    providerId?: string;
    weights?: Record<string, number>;
    unitMultipliers?: Record<string, number>;
    signOverrides?: Record<string, number>;
    mismatchPolicy?: Partial<CvdMismatchPolicy>;
    now?: () => number;
}

interface SourceState {
    cvdTotal: number;
    cvdDelta: number;
    bucketStartTs: number;
    bucketEndTs: number;
    ts: number;
}

export class CvdAggregator {
    private readonly bus: EventBus;
    private readonly ttlMs: number;
    private readonly providerId: string;
    private readonly weights: Record<string, number>;
    private readonly unitMultipliers: Record<string, number>;
    private readonly signOverrides: Record<string, number>;
    private readonly now: () => number;
    private readonly mismatchPolicy: CvdMismatchPolicy;
    private readonly sources = new Map<string, Map<KnownMarketType, Map<string, SourceState>>>();
    private readonly debug: boolean;
    private readonly lastDebugBucketByKey = new Map<string, number>();
    private readonly lastMismatchBucketByKey = new Map<string, number>();
    private readonly ewmaAbsByKey = new Map<string, Map<string, number>>();
    private unsubscribe?: () => void;

    constructor(bus: EventBus, options: CvdAggregatorOptions = {}) {
        this.bus = bus;
        this.ttlMs = Math.max(1_000, options.ttlMs ?? 120_000);
        this.providerId = options.providerId ?? 'local_cvd_agg';
        this.weights = options.weights ?? {};
        this.unitMultipliers = options.unitMultipliers ?? {};
        this.signOverrides = options.signOverrides ?? {};
        this.now = options.now ?? nowMs;
        this.mismatchPolicy = resolveCvdMismatchPolicy(options.mismatchPolicy);
        this.debug = readFlag('BOT_CVD_DEBUG', false);
    }

    start(): void {
        if (this.unsubscribe) return;
        const onSpot = (evt: MarketCvdEvent) => this.onCvd(evt, 'spot');
        const onFutures = (evt: MarketCvdEvent) => this.onCvd(evt, 'futures');
        this.bus.subscribe('market:cvd_spot', onSpot);
        this.bus.subscribe('market:cvd_futures', onFutures);
        this.unsubscribe = () => {
            this.bus.unsubscribe('market:cvd_spot', onSpot);
            this.bus.unsubscribe('market:cvd_futures', onFutures);
        };
    }

    stop(): void {
        this.unsubscribe?.();
        this.unsubscribe = undefined;
    }

    private onCvd(evt: MarketCvdEvent, marketType: KnownMarketType): void {
        if (!evt.streamId) return;
        const symbol = evt.symbol;
        const normalizedSymbol = normalizeSymbol(symbol);
        const ts = evt.meta.tsEvent;
        const byType = this.sources.get(symbol) ?? new Map();
        const byStream = byType.get(marketType) ?? new Map();
        byStream.set(evt.streamId, {
            cvdTotal: evt.cvdTotal,
            cvdDelta: evt.cvdDelta,
            bucketStartTs: evt.bucketStartTs,
            bucketEndTs: evt.bucketEndTs,
            ts,
        });
        byType.set(marketType, byStream);
        this.sources.set(symbol, byType);

        const aggregate = this.computeAggregate(symbol, marketType, ts, evt.bucketStartTs, byStream);
        if (!aggregate) {
            sourceRegistry.recordSuppression(
                { symbol: normalizedSymbol, marketType: normalizeMarketType(marketType), metric: 'flow' },
                'STALE_INPUT'
            );
            return;
        }

        if (marketType === 'futures' && this.providerId === 'local_cvd_agg' && aggregate.mismatchDetected) {
            this.logMismatchContext(symbol, marketType, evt.bucketStartTs, evt.bucketEndTs, aggregate);
        }

        if (this.debug) {
            this.logDebug(symbol, marketType, evt.bucketStartTs, evt.bucketEndTs, aggregate);
        }

        const payload = {
            symbol,
            ts,
            cvd: aggregate.cvdTotal,
            cvdDelta: aggregate.cvdDelta,
            cvdFutures: marketType === 'futures' ? aggregate.cvdTotal : undefined,
            bucketStartTs: evt.bucketStartTs,
            bucketEndTs: evt.bucketEndTs,
            bucketSizeMs: evt.bucketSizeMs,
            unit: evt.unit,
            venueBreakdown: aggregate.venueBreakdown,
            sourcesUsed: aggregate.sourcesUsed,
            weightsUsed: aggregate.weightsUsed,
            qualityFlags: aggregate.qualityFlags,
            freshSourcesCount: aggregate.sourcesUsed.length,
            staleSourcesDropped: aggregate.staleSourcesDropped.length ? aggregate.staleSourcesDropped : undefined,
            mismatchDetected: aggregate.mismatchDetected,
            mismatchType: aggregate.mismatchType,
            mismatchReason: aggregate.mismatchReason,
            signAgreementRatio: aggregate.signAgreementRatio,
            scaleFactors: aggregate.scaleFactors,
            scaledVenueBreakdown: aggregate.scaledVenueBreakdown,
            confidenceScore: aggregate.confidenceScore,
            provider: this.providerId,
            marketType,
            meta: createMeta('global_data', {
                tsEvent: asTsMs(ts),
                tsIngest: asTsMs(evt.meta.tsIngest ?? this.now()),
                correlationId: evt.meta.correlationId,
            }),
        } satisfies MarketCvdAggEvent;

        if (marketType === 'spot') {
            this.bus.publish('market:cvd_spot_agg', payload);
        } else if (marketType === 'futures') {
            this.bus.publish('market:cvd_futures_agg', payload);
        }
        sourceRegistry.markAggEmitted(
            { symbol: normalizedSymbol, marketType: normalizeMarketType(marketType), metric: 'flow' },
            aggregate.sourcesUsed,
            ts
        );
    }

    private computeAggregate(
        symbol: string,
        marketType: KnownMarketType,
        nowTs: number,
        bucketStartTs: number,
        sources: Map<string, SourceState>
    ): {
        cvdTotal: number;
        cvdDelta: number;
        sourcesUsed: string[];
        weightsUsed: Record<string, number>;
        venueBreakdown: Record<string, number>;
        qualityFlags: AggregatedQualityFlags;
        staleSourcesDropped: string[];
        mismatchDetected: boolean;
        mismatchType: CvdMismatchType;
        mismatchReason?: CvdMismatchReason;
        confidenceScore: number;
        scaleFactors?: Record<string, number>;
        scaledVenueBreakdown?: Record<string, number>;
        signAgreementRatio?: number | null;
    } | null {
        let weightedTotal = 0;
        let weightedDelta = 0;
        let weightTotal = 0;
        const venueEntries: Array<[string, number]> = [];
        const venueDeltaEntries: Array<[string, number]> = [];
        const sourcesUsed: string[] = [];
        const weightsEntries: Array<[string, number]> = [];
        const staleDropped: string[] = [];

        for (const [streamId, state] of sources.entries()) {
            if (nowTs - state.ts > this.ttlMs) {
                staleDropped.push(streamId);
                continue;
            }
            const weight = this.weights[streamId] ?? 1;
            const normalizedTotal = this.normalizeCvdValue(streamId, state.cvdTotal);
            const normalizedDelta = this.normalizeCvdValue(streamId, state.cvdDelta);
            weightedTotal += normalizedTotal * weight;
            if (state.bucketStartTs === bucketStartTs) {
                weightedDelta += normalizedDelta * weight;
                venueDeltaEntries.push([streamId, normalizedDelta]);
            }
            weightTotal += weight;
            venueEntries.push([streamId, normalizedTotal]);
            sourcesUsed.push(streamId);
            weightsEntries.push([streamId, weight]);
        }

        if (!sourcesUsed.length || weightTotal === 0) return null;
        const orderedSources = stableSortStrings(sourcesUsed);
        const orderedStale = stableSortStrings(staleDropped);
        const venueBreakdown = stableRecordFromEntries(venueEntries);
        const weightsUsed = stableRecordFromEntries(weightsEntries);
        const mismatchDetails = evaluateCvdMismatchV1(
            `${symbol}:${marketType}`,
            venueDeltaEntries,
            this.ewmaAbsByKey,
            this.mismatchPolicy
        );
        const mismatchDetected = mismatchDetails.mismatchType !== 'NONE';
        const trust = getSourceTrustAdjustments(orderedSources, 'trade');
        const baseConfidence = computeConfidenceScore({
            freshSourcesCount: orderedSources.length,
            staleSourcesDroppedCount: orderedStale.length,
            sourcePenalty: trust.sourcePenalty,
            sourceCap: trust.sourceCap,
        });
        const confidenceScore = clamp01(baseConfidence * mismatchDetails.confidencePenalty);
        return {
            cvdTotal: weightedTotal,
            cvdDelta: weightedDelta,
            sourcesUsed: orderedSources,
            weightsUsed,
            venueBreakdown,
            qualityFlags: {
                staleSourcesDropped: orderedStale.length ? orderedStale : undefined,
                mismatchDetected,
            },
            staleSourcesDropped: orderedStale,
            mismatchDetected,
            mismatchType: mismatchDetails.mismatchType,
            mismatchReason: mismatchDetails.mismatchReason,
            confidenceScore,
            scaleFactors: mismatchDetails.scaleFactors,
            scaledVenueBreakdown: mismatchDetails.scaledVenueBreakdown,
            signAgreementRatio: mismatchDetails.signAgreementRatio,
        };
    }

    private normalizeCvdValue(streamId: string, value: number): number {
        const multiplier = this.unitMultipliers[streamId] ?? 1;
        const sign = this.signOverrides[streamId] ?? 1;
        const normalizedSign = sign >= 0 ? 1 : -1;
        return value * multiplier * normalizedSign;
    }

    private logDebug(
        symbol: string,
        marketType: KnownMarketType,
        bucketStartTs: number,
        bucketEndTs: number,
        aggregate: {
            sourcesUsed: string[];
            staleSourcesDropped: string[];
            venueBreakdown: Record<string, number>;
            mismatchDetected: boolean;
            confidenceScore: number;
        }
    ): void {
        const key = `${symbol}:${marketType}`;
        const lastBucket = this.lastDebugBucketByKey.get(key);
        if (lastBucket === bucketEndTs) return;
        this.lastDebugBucketByKey.set(key, bucketEndTs);

        const freshSourcesCount = aggregate.sourcesUsed.length;
        const staleSourcesDroppedCount = aggregate.staleSourcesDropped.length;
        const total = freshSourcesCount + staleSourcesDroppedCount;
        const base = total > 0 ? freshSourcesCount / total : 0;
        const trust = getSourceTrustAdjustments(aggregate.sourcesUsed, 'trade');
        const explain = computeConfidenceExplain({
            freshSourcesCount,
            staleSourcesDroppedCount,
            mismatchDetected: aggregate.mismatchDetected,
            sourcePenalty: trust.sourcePenalty,
            sourceCap: trust.sourceCap,
        });

        logger.info(
            `[CvdDebug] ${JSON.stringify({
                symbol,
                marketType,
                bucketStartTs,
                bucketEndTs,
                sourcesUsed: aggregate.sourcesUsed,
                staleSourcesDropped: aggregate.staleSourcesDropped,
                venueBreakdown: aggregate.venueBreakdown,
                mismatchDetected: aggregate.mismatchDetected,
                confidence: {
                    base,
                    penalties: explain.penalties,
                    score: aggregate.confidenceScore,
                },
            })}`
        );
    }

    private logMismatchContext(
        symbol: string,
        marketType: KnownMarketType,
        bucketStartTs: number,
        bucketEndTs: number,
        aggregate: {
            sourcesUsed: string[];
            weightsUsed: Record<string, number>;
            staleSourcesDropped: string[];
            venueBreakdown: Record<string, number>;
            scaledVenueBreakdown?: Record<string, number>;
            scaleFactors?: Record<string, number>;
            mismatchDetected: boolean;
            mismatchType: CvdMismatchType;
            mismatchReason?: CvdMismatchReason;
            confidenceScore: number;
            signAgreementRatio?: number | null;
        }
    ): void {
        const key = `${symbol}:${marketType}:${this.providerId}`;
        const lastBucket = this.lastMismatchBucketByKey.get(key);
        if (lastBucket === bucketEndTs) return;
        this.lastMismatchBucketByKey.set(key, bucketEndTs);

        const unitMultipliers = this.buildOverrideMap(this.unitMultipliers, aggregate.sourcesUsed);
        const signOverrides = this.buildOverrideMap(this.signOverrides, aggregate.sourcesUsed);

        logger.info(
            `[CvdMismatchContext] ${JSON.stringify({
                symbol,
                marketType,
                bucketStartTs,
                bucketEndTs,
                provider: this.providerId,
                sourcesUsed: aggregate.sourcesUsed,
                staleSourcesDropped: aggregate.staleSourcesDropped,
                weightsUsed: aggregate.weightsUsed,
                venueBreakdown: aggregate.venueBreakdown,
                scaledVenueBreakdown: aggregate.scaledVenueBreakdown,
                mismatchDetected: aggregate.mismatchDetected,
                mismatchType: aggregate.mismatchType,
                mismatchReason: aggregate.mismatchReason,
                confidenceScore: aggregate.confidenceScore,
                signAgreementRatio: aggregate.signAgreementRatio,
                scaleFactors: aggregate.scaleFactors,
                unitMultipliers,
                signOverrides,
            })}`
        );
    }

    private buildOverrideMap(
        overrides: Record<string, number>,
        sources: string[]
    ): Record<string, number> | undefined {
        const entries: Array<[string, number]> = [];
        for (const source of sources) {
            const value = overrides[source];
            if (value === undefined || value === 1) continue;
            entries.push([source, value]);
        }
        return entries.length ? stableRecordFromEntries(entries) : undefined;
    }
}

export interface CvdMismatchDetails {
    mismatchType: CvdMismatchType;
    mismatchReason?: CvdMismatchReason;
    mismatchDetected: boolean;
    confidencePenalty: number;
    scaleFactors?: Record<string, number>;
    scaledVenueBreakdown?: Record<string, number>;
    signAgreementRatio?: number | null;
}

export function evaluateCvdMismatchV1(
    key: string,
    venueDeltas: Array<[string, number]>,
    ewmaAbsByKey: Map<string, Map<string, number>>,
    policy: CvdMismatchPolicy
): CvdMismatchDetails {
    if (venueDeltas.length < 2) {
        return { mismatchType: 'NONE', mismatchDetected: false, confidencePenalty: 1 };
    }

    const ewmaMap = ewmaAbsByKey.get(key) ?? new Map<string, number>();
    const ewmaValues: number[] = [];
    for (const [streamId, value] of venueDeltas) {
        const abs = Math.abs(value);
        if (!Number.isFinite(abs) || abs < policy.minEwmaAbs) continue;
        const prev = ewmaMap.get(streamId);
        const next = prev === undefined ? abs : prev + policy.ewmaAlpha * (abs - prev);
        ewmaMap.set(streamId, next);
        if (Number.isFinite(next) && next > 0) ewmaValues.push(next);
    }
    ewmaAbsByKey.set(key, ewmaMap);

    const medianEwma = median(ewmaValues);
    const scaledEntries: Array<[string, number]> = [];
    const scaleEntries: Array<[string, number]> = [];
    for (const [streamId, value] of venueDeltas) {
        const ewma = ewmaMap.get(streamId);
        let scale = 1;
        if (medianEwma !== null && ewma !== undefined && ewma > 0) {
            scale = clamp(medianEwma / ewma, policy.minScale, policy.maxScale);
        }
        if (scale !== 1) scaleEntries.push([streamId, scale]);
        scaledEntries.push([streamId, value * scale]);
    }

    const scaledVenueBreakdown = stableRecordFromEntries(scaledEntries);
    const scaleFactors = scaleEntries.length ? stableRecordFromEntries(scaleEntries) : undefined;

    const absScaledValues = scaledEntries
        .map(([, value]) => Math.abs(value))
        .filter((value) => Number.isFinite(value));
    const medianAbs = median(absScaledValues) ?? 0;
    if (medianAbs < policy.minAbsScaled) {
        return {
            mismatchType: 'NONE',
            mismatchDetected: false,
            mismatchReason: undefined,
            confidencePenalty: 1,
            scaleFactors,
            scaledVenueBreakdown,
            signAgreementRatio: null,
        };
    }

    const signCandidates = scaledEntries.filter(
        ([, value]) => Number.isFinite(value) && Math.abs(value) >= policy.minAbsScaled
    );
    if (signCandidates.length < 2) {
        return {
            mismatchType: 'NONE',
            mismatchDetected: false,
            mismatchReason: undefined,
            confidencePenalty: 1,
            scaleFactors,
            scaledVenueBreakdown,
            signAgreementRatio: null,
        };
    }

    let pos = 0;
    let neg = 0;
    for (const [, value] of signCandidates) {
        if (value > 0) pos += 1;
        else if (value < 0) neg += 1;
    }
    const signTotal = pos + neg;
    const signAgreementRatio = signTotal > 0 ? Math.max(pos, neg) / signTotal : null;

    let mismatchType: CvdMismatchType = 'NONE';
    if (signTotal >= 2 && signAgreementRatio !== null && signAgreementRatio < policy.signAgreementThreshold) {
        mismatchType = 'SIGN';
    }

    const values = signCandidates.map(([, value]) => value);
    const med = median(values);
    const absMedian = med === null ? 0 : Math.abs(med);
    const deviations = med === null ? [] : values.map((value) => Math.abs(value - med));
    const mad = deviations.length ? median(deviations) : null;
    let maxAbsZ = 0;
    if (mad !== null && mad > 0 && med !== null) {
        for (const value of values) {
            const z = 0.6745 * (value - med) / mad;
            maxAbsZ = Math.max(maxAbsZ, Math.abs(z));
        }
    }
    const absValues = values.map((value) => Math.abs(value));
    const maxAbs = absValues.length ? Math.max(...absValues) : 0;
    const ratioToMedian = absMedian > 0 ? maxAbs / absMedian : 0;

    if (mismatchType === 'NONE' && absMedian >= policy.minAbsScaled) {
        const zTrigger = mad !== null && mad > 0 && maxAbsZ >= policy.zThresh;
        const ratioTrigger = ratioToMedian >= policy.ratioThresh;
        if (zTrigger || ratioTrigger) {
            mismatchType = 'DISPERSION';
        }
    }

    let dispersionSeverity = 0;
    if (absMedian >= policy.minAbsScaled) {
        if (maxAbsZ > 0 && policy.zMax > policy.zThresh) {
            const capped = Math.min(maxAbsZ, policy.zMax);
            const scaled = (capped - policy.zThresh) / (policy.zMax - policy.zThresh);
            dispersionSeverity = Math.max(dispersionSeverity, clamp01(scaled));
        }
        if (ratioToMedian > 0 && policy.ratioMax > policy.ratioThresh) {
            const capped = Math.min(ratioToMedian, policy.ratioMax);
            const scaled = (capped - policy.ratioThresh) / (policy.ratioMax - policy.ratioThresh);
            dispersionSeverity = Math.max(dispersionSeverity, clamp01(scaled));
        }
    }

    let confidencePenalty = 1;
    if (mismatchType === 'SIGN') {
        confidencePenalty = policy.penaltySign;
    } else if (mismatchType === 'DISPERSION') {
        confidencePenalty = 1 - dispersionSeverity * (1 - policy.penaltyDispersion);
    }
    confidencePenalty = clamp01(confidencePenalty);

    return {
        mismatchType,
        mismatchReason: mismatchType === 'NONE' ? undefined : mismatchType,
        mismatchDetected: mismatchType !== 'NONE',
        confidencePenalty,
        scaleFactors,
        scaledVenueBreakdown,
        signAgreementRatio,
    };
}

function readFlag(name: string, fallback: boolean): boolean {
    const raw = process.env[name];
    if (raw === undefined) return fallback;
    const normalized = raw.trim().toLowerCase();
    if (normalized === '0' || normalized === 'false' || normalized === 'off') return false;
    if (normalized === '1' || normalized === 'true' || normalized === 'on') return true;
    return fallback;
}

function readNumber(name: string): number | undefined {
    const raw = process.env[name];
    if (raw === undefined) return undefined;
    const value = Number(raw);
    return Number.isFinite(value) ? value : undefined;
}

function resolveCvdMismatchPolicy(overrides: Partial<CvdMismatchPolicy> = {}): CvdMismatchPolicy {
    const envOverrides: Partial<CvdMismatchPolicy> = {};
    const assignEnv = (key: keyof CvdMismatchPolicy, envName: string) => {
        const value = readNumber(envName);
        if (value !== undefined) envOverrides[key] = value;
    };

    assignEnv('ewmaAlpha', 'BOT_CVD_MISMATCH_EWMA_ALPHA');
    assignEnv('minEwmaAbs', 'BOT_CVD_MISMATCH_MIN_EWMA_ABS');
    assignEnv('minAbsScaled', 'BOT_CVD_MISMATCH_MIN_ABS_SCALED');
    assignEnv('minScale', 'BOT_CVD_MISMATCH_MIN_SCALE');
    assignEnv('maxScale', 'BOT_CVD_MISMATCH_MAX_SCALE');
    assignEnv('signAgreementThreshold', 'BOT_CVD_MISMATCH_SIGN_AGREEMENT_THRESHOLD');
    assignEnv('zThresh', 'BOT_CVD_MISMATCH_Z_THRESH');
    assignEnv('zMax', 'BOT_CVD_MISMATCH_Z_MAX');
    assignEnv('ratioThresh', 'BOT_CVD_MISMATCH_RATIO_THRESH');
    assignEnv('ratioMax', 'BOT_CVD_MISMATCH_RATIO_MAX');
    assignEnv('penaltySign', 'BOT_CVD_MISMATCH_PENALTY_SIGN');
    assignEnv('penaltyDispersion', 'BOT_CVD_MISMATCH_PENALTY_DISPERSION');

    const merged: CvdMismatchPolicy = {
        ...DEFAULT_CVD_MISMATCH_POLICY,
        ...envOverrides,
        ...overrides,
    };

    return {
        ...merged,
        ewmaAlpha: clamp(merged.ewmaAlpha, 0.01, 1),
        minEwmaAbs: Math.max(0, merged.minEwmaAbs),
        minAbsScaled: Math.max(0, merged.minAbsScaled),
        minScale: Math.max(0.01, merged.minScale),
        maxScale: Math.max(merged.maxScale, merged.minScale),
        signAgreementThreshold: clamp(merged.signAgreementThreshold, 0.5, 1),
        zThresh: Math.max(0, merged.zThresh),
        zMax: Math.max(merged.zMax, merged.zThresh),
        ratioThresh: Math.max(1, merged.ratioThresh),
        ratioMax: Math.max(merged.ratioMax, merged.ratioThresh),
        penaltySign: clamp01(merged.penaltySign),
        penaltyDispersion: clamp01(merged.penaltyDispersion),
    };
}

function clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, value));
}

function clamp01(value: number): number {
    if (!Number.isFinite(value)) return 0;
    return clamp(value, 0, 1);
}

function median(values: number[]): number | null {
    if (!values.length) return null;
    const sorted = [...values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 0) {
        return (sorted[mid - 1] + sorted[mid]) / 2;
    }
    return sorted[mid];
}
