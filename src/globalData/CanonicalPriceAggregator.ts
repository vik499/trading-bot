import {
    inheritMeta,
    type EventBus,
    type MarketPriceCanonicalEvent,
    type TickerEvent,
} from '../core/events/EventBus';
import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import { computeConfidenceScore } from '../core/confidence';
import { stableRecordFromEntries, stableSortStrings } from '../core/determinism';
import { sourceRegistry } from '../core/market/SourceRegistry';
import { inferMarketTypeFromStreamId, normalizeMarketType, normalizeSymbol } from '../core/market/symbols';
import { detectMismatch } from './quality';

export interface CanonicalPriceAggregatorOptions {
    ttlMs?: number;
    providerId?: string;
    weights?: Record<string, number>;
}

interface PriceFieldState {
    value: number;
    ts: number;
}

interface SourceState {
    indexPrice?: PriceFieldState;
    markPrice?: PriceFieldState;
    lastPrice?: PriceFieldState;
}

interface PriceAggregate {
    price?: number;
    sourcesUsed: string[];
    weightsUsed: Record<string, number>;
    venueBreakdown: Record<string, number>;
    staleSourcesDropped: string[];
}

export class CanonicalPriceAggregator {
    private readonly bus: EventBus;
    private readonly ttlMs: number;
    private readonly providerId: string;
    private readonly weights: Record<string, number>;
    private readonly sources = new Map<string, Map<string, SourceState>>();
    private unsubscribe?: () => void;

    constructor(bus: EventBus, options: CanonicalPriceAggregatorOptions = {}) {
        this.bus = bus;
        this.ttlMs = Math.max(1_000, options.ttlMs ?? 120_000);
        this.providerId = options.providerId ?? 'local_price_canonical_agg';
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
        const symbol = evt.symbol;
        const ts = evt.meta.ts;
        const normalizedSymbol = normalizeSymbol(symbol);
        const normalizedMarketType = normalizeMarketType(evt.marketType ?? inferMarketTypeFromStreamId(evt.streamId));
        const key = `${normalizedSymbol}:${normalizedMarketType}`;
        const byStream = this.sources.get(key) ?? new Map<string, SourceState>();
        const state = byStream.get(evt.streamId) ?? {};

        const indexPrice = parsePrice(evt.indexPrice);
        if (indexPrice !== undefined) state.indexPrice = { value: indexPrice, ts };
        const markPrice = parsePrice(evt.markPrice);
        if (markPrice !== undefined) state.markPrice = { value: markPrice, ts };
        const lastPrice = parsePrice(evt.lastPrice);
        if (lastPrice !== undefined) state.lastPrice = { value: lastPrice, ts };

        byStream.set(evt.streamId, state);
        this.sources.set(key, byStream);

        const indexAgg = this.computeAggregate(ts, byStream, 'indexPrice');
        const markAgg = this.computeAggregate(ts, byStream, 'markPrice');
        const lastAgg = this.computeAggregate(ts, byStream, 'lastPrice');

        const indexFresh = indexAgg.price !== undefined;
        const markFresh = markAgg.price !== undefined;
        const lastFresh = lastAgg.price !== undefined;

        const primary = indexFresh ? indexAgg : markFresh ? markAgg : lastAgg;
        if (primary.price === undefined) {
            sourceRegistry.recordSuppression(
                { symbol: normalizedSymbol, marketType: normalizedMarketType, metric: 'price' },
                'NO_CANONICAL_PRICE'
            );
            return;
        }

        const priceTypeUsed = indexFresh ? 'index' : markFresh ? 'mark' : 'last';
                const fallbackReason = indexFresh ? undefined : markFresh ? indexAgg.reason : markAgg.reason;

        const mismatchDetected = detectMismatch(primary.venueBreakdown);
        const confidenceScore = computeConfidenceScore({
            freshSourcesCount: primary.sourcesUsed.length,
            staleSourcesDroppedCount: primary.staleSourcesDropped.length,
            mismatchDetected,
            fallbackPenalty: priceTypeUsed === 'index' ? 1 : priceTypeUsed === 'mark' ? 0.85 : 0.6,
        });

        if (primary.sourcesUsed.length === 0) {
            logger.warn(
                m(
                    'warn',
                    `[CanonicalPriceAggregator] no sourcesUsed for price_canonical symbol=${symbol} marketType=${normalizedMarketType}`
                )
            );
            sourceRegistry.recordSuppression(
                { symbol: normalizedSymbol, marketType: normalizedMarketType, metric: 'price' },
                'NO_CANONICAL_PRICE'
            );
            return;
        }

        const payload: MarketPriceCanonicalEvent = {
            symbol,
            ts,
            marketType: normalizedMarketType,
            indexPrice: indexAgg.price,
            markPrice: markAgg.price,
            lastPrice: lastAgg.price,
            priceTypeUsed,
            fallbackReason,
            sourcesUsed: primary.sourcesUsed,
            freshSourcesCount: primary.sourcesUsed.length,
            staleSourcesDropped: primary.staleSourcesDropped.length ? primary.staleSourcesDropped : undefined,
            mismatchDetected,
            confidenceScore,
            provider: this.providerId,
            meta: inheritMeta(evt.meta, 'global_data', { ts }),
        };

        this.bus.publish('market:price_canonical', payload);
        sourceRegistry.markAggEmitted(
            { symbol: normalizedSymbol, marketType: normalizedMarketType, metric: 'price' },
            primary.sourcesUsed,
            ts
        );
    }

    private computeAggregate(
        nowTs: number,
        sources: Map<string, SourceState>,
        field: keyof SourceState
    ): PriceAggregate & { reason?: MarketPriceCanonicalEvent['fallbackReason'] } {
        let weightedSum = 0;
        let weightTotal = 0;
        const sourcesUsed: string[] = [];
        const weightsUsedEntries: Array<[string, number]> = [];
        const venueEntries: Array<[string, number]> = [];
        const staleSourcesDropped: string[] = [];
        let sawAny = false;
        let sawFresh = false;

        for (const [streamId, state] of sources.entries()) {
            const entry = state[field];
            if (!entry) continue;
            sawAny = true;
            if (nowTs - entry.ts > this.ttlMs) {
                staleSourcesDropped.push(streamId);
                continue;
            }
            sawFresh = true;
            const weight = this.weights[streamId] ?? 1;
            weightedSum += entry.value * weight;
            weightTotal += weight;
            sourcesUsed.push(streamId);
            weightsUsedEntries.push([streamId, weight]);
            venueEntries.push([streamId, entry.value]);
        }

        const orderedSources = stableSortStrings(sourcesUsed);
        const weightsUsed = stableRecordFromEntries(weightsUsedEntries);
        const venueBreakdown = stableRecordFromEntries(venueEntries);
        const orderedStale = stableSortStrings(staleSourcesDropped);
                const reason =
                        field === 'indexPrice'
                                ? sawFresh
                                        ? undefined
                                        : sawAny
                                            ? 'INDEX_STALE'
                                            : 'NO_INDEX'
                                : field === 'markPrice'
                                    ? sawFresh
                                            ? undefined
                                            : sawAny
                                                ? 'MARK_STALE'
                                                : 'NO_MARK'
                                    : undefined;

        return {
            price: weightTotal > 0 ? weightedSum / weightTotal : undefined,
            sourcesUsed: orderedSources,
            weightsUsed,
            venueBreakdown,
            staleSourcesDropped: orderedStale,
            reason: reason as MarketPriceCanonicalEvent['fallbackReason'] | undefined,
        };
    }
}

function parsePrice(value: string | number | undefined): number | undefined {
    if (value === undefined) return undefined;
    const parsed = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
}
