import { inheritMeta, type AggregatedQualityFlags, type EventBus, type MarketCvdAggEvent, type MarketCvdEvent, type MarketType } from '../core/events/EventBus';
import { computeConfidenceScore, getSourceTrustAdjustments } from '../core/confidence';
import { stableRecordFromEntries, stableSortStrings } from '../core/determinism';
import { sourceRegistry } from '../core/market/SourceRegistry';
import { normalizeMarketType, normalizeSymbol } from '../core/market/symbols';
import { detectMismatch } from './quality';

export interface CvdAggregatorOptions {
    ttlMs?: number;
    providerId?: string;
    weights?: Record<string, number>;
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
    private readonly sources = new Map<string, Map<MarketType, Map<string, SourceState>>>();
    private unsubscribe?: () => void;

    constructor(bus: EventBus, options: CvdAggregatorOptions = {}) {
        this.bus = bus;
        this.ttlMs = Math.max(1_000, options.ttlMs ?? 120_000);
        this.providerId = options.providerId ?? 'local_cvd_agg';
        this.weights = options.weights ?? {};
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

    private onCvd(evt: MarketCvdEvent, marketType: MarketType): void {
        if (!evt.streamId) return;
        const symbol = evt.symbol;
        const normalizedSymbol = normalizeSymbol(symbol);
        const ts = evt.meta.ts;
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

        const payload: MarketCvdAggEvent = {
            symbol,
            ts,
            cvd: aggregate.cvdTotal,
            cvdDelta: aggregate.cvdDelta,
            cvdSpot: marketType === 'spot' ? aggregate.cvdTotal : undefined,
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
            meta: inheritMeta(evt.meta, 'global_data', { ts }),
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
            weightedTotal += state.cvdTotal * weight;
            if (state.bucketStartTs === bucketStartTs) {
                weightedDelta += state.cvdDelta * weight;
            }
            weightTotal += weight;
            venueEntries.push([streamId, state.cvdTotal]);
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
}
