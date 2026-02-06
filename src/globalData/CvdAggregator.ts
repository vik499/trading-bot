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
import { detectMismatch } from './quality';

export interface CvdAggregatorOptions {
    ttlMs?: number;
    providerId?: string;
    weights?: Record<string, number>;
    unitMultipliers?: Record<string, number>;
    signOverrides?: Record<string, number>;
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
    private readonly sources = new Map<string, Map<KnownMarketType, Map<string, SourceState>>>();
    private readonly debug: boolean;
    private readonly lastDebugBucketByKey = new Map<string, number>();
    private unsubscribe?: () => void;

    constructor(bus: EventBus, options: CvdAggregatorOptions = {}) {
        this.bus = bus;
        this.ttlMs = Math.max(1_000, options.ttlMs ?? 120_000);
        this.providerId = options.providerId ?? 'local_cvd_agg';
        this.weights = options.weights ?? {};
        this.unitMultipliers = options.unitMultipliers ?? {};
        this.signOverrides = options.signOverrides ?? {};
        this.now = options.now ?? nowMs;
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

        const aggregate = this.computeAggregate(ts, evt.bucketStartTs, byStream);
        if (!aggregate) {
            sourceRegistry.recordSuppression(
                { symbol: normalizedSymbol, marketType: normalizeMarketType(marketType), metric: 'flow' },
                'STALE_INPUT'
            );
            return;
        }

        if (this.debug) {
            this.logDebug(symbol, marketType, evt.bucketStartTs, evt.bucketEndTs, aggregate);
        }

        const payload: MarketCvdAggEvent = {
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
            confidenceScore: aggregate.confidenceScore,
            provider: this.providerId,
            marketType,
            meta: createMeta('global_data', {
                tsEvent: asTsMs(ts),
                tsIngest: asTsMs(evt.meta.tsIngest ?? this.now()),
                correlationId: evt.meta.correlationId,
            }),
        };

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
        confidenceScore: number;
    } | null {
        let weightedTotal = 0;
        let weightedDelta = 0;
        let weightTotal = 0;
        const venueEntries: Array<[string, number]> = [];
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
        const mismatchDetected = detectMismatch(venueBreakdown);
        const trust = getSourceTrustAdjustments(orderedSources, 'trade');
        const confidenceScore = computeConfidenceScore({
            freshSourcesCount: orderedSources.length,
            staleSourcesDroppedCount: orderedStale.length,
            mismatchDetected,
            sourcePenalty: trust.sourcePenalty,
            sourceCap: trust.sourceCap,
        });
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
            confidenceScore,
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
}

function readFlag(name: string, fallback: boolean): boolean {
    const raw = process.env[name];
    if (raw === undefined) return fallback;
    const normalized = raw.trim().toLowerCase();
    if (normalized === '0' || normalized === 'false' || normalized === 'off') return false;
    if (normalized === '1' || normalized === 'true' || normalized === 'on') return true;
    return fallback;
}
