import { inheritMeta, type AggregatedQualityFlags, type EventBus, type MarketPriceIndexEvent, type TickerEvent } from '../core/events/EventBus';
import { computeConfidenceScore } from '../core/confidence';
import { stableRecordFromEntries, stableSortStrings } from '../core/determinism';
import { sourceRegistry } from '../core/market/SourceRegistry';
import { normalizeMarketType, normalizeSymbol } from '../core/market/symbols';
import { detectMismatch } from './quality';

export interface PriceIndexAggregatorOptions {
    ttlMs?: number;
    providerId?: string;
    weights?: Record<string, number>;
}

interface SourceState {
    indexPrice: number;
    ts: number;
}

export class PriceIndexAggregator {
    private readonly bus: EventBus;
    private readonly ttlMs: number;
    private readonly providerId: string;
    private readonly weights: Record<string, number>;
    private readonly sources = new Map<string, Map<string, SourceState>>();
    private unsubscribe?: () => void;

    constructor(bus: EventBus, options: PriceIndexAggregatorOptions = {}) {
        this.bus = bus;
        this.ttlMs = Math.max(1_000, options.ttlMs ?? 120_000);
        this.providerId = options.providerId ?? 'local_price_index_agg';
        this.weights = options.weights ?? {};
    }

    start(): void {
        if (this.unsubscribe) return;
        const handler = (evt: TickerEvent) => this.onTicker(evt);
        this.bus.subscribe('market:ticker', handler);
        this.unsubscribe = () => this.bus.unsubscribe('market:ticker', handler);
    }

    stop(): void {
        this.unsubscribe?.();
        this.unsubscribe = undefined;
    }

    private onTicker(evt: TickerEvent): void {
        if (!evt.streamId) return;
        const indexRaw = evt.indexPrice;
        if (indexRaw === undefined) return;
        const indexPrice = Number(indexRaw);
        if (!Number.isFinite(indexPrice)) return;

        const symbol = evt.symbol;
        const normalizedSymbol = normalizeSymbol(symbol);
        const normalizedMarketType = normalizeMarketType(evt.marketType);
        const ts = evt.meta.ts;
        const key = `${normalizedSymbol}:${normalizedMarketType}`;
        const byStream = this.sources.get(key) ?? new Map<string, SourceState>();
        byStream.set(evt.streamId, { indexPrice, ts });
        this.sources.set(key, byStream);

        const aggregate = this.computeAggregate(ts, byStream);
        if (!aggregate) {
            sourceRegistry.recordSuppression(
                { symbol: normalizedSymbol, marketType: normalizedMarketType, metric: 'price' },
                'STALE_INPUT'
            );
            return;
        }

        const payload: MarketPriceIndexEvent = {
            symbol,
            ts,
            indexPrice: aggregate.indexPrice,
            marketType: normalizedMarketType,
            venueBreakdown: aggregate.venueBreakdown,
            sourcesUsed: aggregate.sourcesUsed,
            weightsUsed: aggregate.weightsUsed,
            qualityFlags: aggregate.qualityFlags,
            freshSourcesCount: aggregate.sourcesUsed.length,
            staleSourcesDropped: aggregate.staleSourcesDropped.length ? aggregate.staleSourcesDropped : undefined,
            mismatchDetected: aggregate.mismatchDetected,
            confidenceScore: aggregate.confidenceScore,
            provider: this.providerId,
            meta: inheritMeta(evt.meta, 'global_data', { ts }),
        };
        this.bus.publish('market:price_index', payload);
        sourceRegistry.markAggEmitted(
            { symbol: normalizedSymbol, marketType: normalizedMarketType, metric: 'price' },
            aggregate.sourcesUsed,
            ts
        );
    }

    private computeAggregate(
        nowTs: number,
        sources: Map<string, SourceState>
    ): {
        indexPrice: number;
        sourcesUsed: string[];
        weightsUsed: Record<string, number>;
        venueBreakdown: Record<string, number>;
        qualityFlags: AggregatedQualityFlags;
        staleSourcesDropped: string[];
        mismatchDetected: boolean;
        confidenceScore: number;
    } | null {
        let weightedSum = 0;
        let weightTotal = 0;
        const sourcesUsed: string[] = [];
        const weightsEntries: Array<[string, number]> = [];
        const venueEntries: Array<[string, number]> = [];
        const staleDropped: string[] = [];

        for (const [streamId, state] of sources.entries()) {
            if (nowTs - state.ts > this.ttlMs) {
                staleDropped.push(streamId);
                continue;
            }
            const weight = this.weights[streamId] ?? 1;
            weightedSum += state.indexPrice * weight;
            weightTotal += weight;
            sourcesUsed.push(streamId);
            weightsEntries.push([streamId, weight]);
            venueEntries.push([streamId, state.indexPrice]);
        }

        if (!sourcesUsed.length || weightTotal === 0) return null;
        const orderedSources = stableSortStrings(sourcesUsed);
        const orderedStale = stableSortStrings(staleDropped);
        const weightsUsed = stableRecordFromEntries(weightsEntries);
        const venueBreakdown = stableRecordFromEntries(venueEntries);
        const mismatchDetected = detectMismatch(venueBreakdown);
        const confidenceScore = computeConfidenceScore({
            freshSourcesCount: orderedSources.length,
            staleSourcesDroppedCount: orderedStale.length,
            mismatchDetected,
        });
        return {
            indexPrice: weightedSum / weightTotal,
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
