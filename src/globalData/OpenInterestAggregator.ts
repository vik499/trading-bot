import {
    inheritMeta,
    type EventBus,
    type MarketOpenInterestAggEvent,
    type OpenInterestEvent,
    type MarketPriceCanonicalEvent,
} from '../core/events/EventBus';
import { computeConfidenceScore } from '../core/confidence';
import { stableRecordFromEntries, stableSortStrings } from '../core/determinism';
import { sourceRegistry } from '../core/market/SourceRegistry';
import { normalizeMarketType, normalizeSymbol } from '../core/market/symbols';

export interface OpenInterestAggregatorOptions {
    ttlMs?: number;
    canonicalTtlMs?: number;
    canonicalMinConfidence?: number;
    providerId?: string;
    weights?: Record<string, number>;
}

interface SourceState {
    openInterest: number;
    ts: number;
    openInterestUnit: OpenInterestEvent['openInterestUnit'];
    openInterestValueUsd?: number;
}

export class OpenInterestAggregator {
    private readonly bus: EventBus;
    private readonly ttlMs: number;
    private readonly canonicalTtlMs: number;
    private readonly canonicalMinConfidence: number;
    private readonly providerId: string;
    private readonly weights: Record<string, number>;
    private readonly sources = new Map<string, Map<string, SourceState>>();
    private readonly canonicalPrice = new Map<
        string,
        { price: number; ts: number; confidenceScore?: number; priceTypeUsed?: MarketPriceCanonicalEvent['priceTypeUsed'] }
    >();
    private unsubscribeOi?: () => void;
    private unsubscribeCanonical?: () => void;

    constructor(bus: EventBus, options: OpenInterestAggregatorOptions = {}) {
        this.bus = bus;
        this.ttlMs = Math.max(10_000, options.ttlMs ?? 120_000);
        this.canonicalTtlMs = Math.max(1_000, options.canonicalTtlMs ?? this.ttlMs);
        this.canonicalMinConfidence = Math.max(0, options.canonicalMinConfidence ?? 0.7);
        this.providerId = options.providerId ?? 'local_oi_agg';
        this.weights = options.weights ?? {};
    }

    start(): void {
        if (this.unsubscribeOi || this.unsubscribeCanonical) return;
        const handler = (evt: OpenInterestEvent) => this.onOi(evt);
        this.bus.subscribe('market:oi', handler);
        this.unsubscribeOi = () => this.bus.unsubscribe('market:oi', handler);

        const onCanonical = (evt: MarketPriceCanonicalEvent) => this.onCanonical(evt);
        this.bus.subscribe('market:price_canonical', onCanonical);
        this.unsubscribeCanonical = () => this.bus.unsubscribe('market:price_canonical', onCanonical);
    }

    stop(): void {
        this.unsubscribeOi?.();
        this.unsubscribeCanonical?.();
        this.unsubscribeOi = undefined;
        this.unsubscribeCanonical = undefined;
    }

    private onCanonical(evt: MarketPriceCanonicalEvent): void {
        const price = evt.indexPrice ?? evt.markPrice;
        if (!Number.isFinite(price)) return;
        const normalizedMarketType = normalizeMarketType(evt.marketType ?? 'futures');
        const key = this.canonicalKey(evt.symbol, normalizedMarketType);
        this.canonicalPrice.set(key, {
            price: price as number,
            ts: evt.ts,
            confidenceScore: evt.confidenceScore,
            priceTypeUsed: evt.priceTypeUsed,
        });
    }

    private onOi(evt: OpenInterestEvent): void {
        const symbol = evt.symbol;
        const normalizedSymbol = normalizeSymbol(symbol);
        const normalizedMarketType = normalizeMarketType(evt.marketType ?? 'futures');
        const streamId = evt.streamId || 'unknown';
        const ts = evt.meta.ts;
        const { valueUsd: openInterestValueUsd, priceTypeUsed } = this.resolveUsdValue(evt, ts, normalizedMarketType);
        const sources = this.sources.get(symbol) ?? new Map<string, SourceState>();
        sources.set(streamId, {
            openInterest: evt.openInterest,
            ts,
            openInterestUnit: evt.openInterestUnit ?? 'unknown',
            openInterestValueUsd,
        });
        this.sources.set(symbol, sources);

        const { aggregate, breakdown } = this.computeAggregate(ts, sources);
        if (aggregate === undefined) {
            sourceRegistry.recordSuppression(
                { symbol: normalizedSymbol, marketType: normalizedMarketType, metric: 'derivatives' },
                'STALE_INPUT'
            );
            return;
        }

        const payload: MarketOpenInterestAggEvent = {
            symbol,
            ts,
            openInterest: aggregate,
            openInterestUnit: breakdown.unit,
            openInterestValueUsd: breakdown.openInterestValueUsd,
            priceTypeUsed,
            marketType: normalizedMarketType,
            venueBreakdown: breakdown.values,
            sourcesUsed: breakdown.sourcesUsed,
            weightsUsed: breakdown.weightsUsed,
            freshSourcesCount: breakdown.sourcesUsed.length,
            staleSourcesDropped: breakdown.staleSourcesDropped.length ? breakdown.staleSourcesDropped : undefined,
            mismatchDetected: breakdown.mismatchDetected,
            confidenceScore: breakdown.confidenceScore,
            qualityFlags: {
                consistentUnits: !breakdown.mismatchDetected,
                mismatchDetected: breakdown.mismatchDetected,
                staleSourcesDropped: breakdown.staleSourcesDropped.length ? breakdown.staleSourcesDropped : undefined,
            },
            provider: this.providerId,
            meta: inheritMeta(evt.meta, 'global_data', { ts }),
        };
        this.bus.publish('market:oi_agg', payload);
        sourceRegistry.markAggEmitted(
            { symbol: normalizedSymbol, marketType: normalizedMarketType, metric: 'derivatives' },
            breakdown.sourcesUsed,
            ts
        );
    }

    private resolveUsdValue(
        evt: OpenInterestEvent,
        ts: number,
        marketType: ReturnType<typeof normalizeMarketType>
    ): { valueUsd?: number; priceTypeUsed?: MarketPriceCanonicalEvent['priceTypeUsed'] } {
        if (evt.openInterestUnit === 'usd') return { valueUsd: evt.openInterest };
        if (evt.openInterestUnit !== 'base') return {};

        const canonical = this.canonicalPrice.get(this.canonicalKey(evt.symbol, marketType));
        if (!canonical) return {};
        if (ts - canonical.ts > this.canonicalTtlMs) return {};
        if ((canonical.confidenceScore ?? 0) < this.canonicalMinConfidence) return {};
        return { valueUsd: evt.openInterest * canonical.price, priceTypeUsed: canonical.priceTypeUsed };
    }

    private canonicalKey(symbol: string, marketType: ReturnType<typeof normalizeMarketType>): string {
        const normalizedSymbol = normalizeSymbol(symbol);
        return `${normalizedSymbol}:${marketType}`;
    }

    private computeAggregate(
        nowTs: number,
        sources: Map<string, SourceState>
    ): {
        aggregate?: number;
        breakdown: {
            values: Record<string, number>;
            unit: OpenInterestEvent['openInterestUnit'];
            openInterestValueUsd?: number;
            sourcesUsed: string[];
            weightsUsed: Record<string, number>;
            staleSourcesDropped: string[];
            mismatchDetected: boolean;
            confidenceScore: number;
        };
    } {
        const grouped = new Map<
            OpenInterestEvent['openInterestUnit'],
            {
                aggregate: number;
                valuesEntries: Array<[string, number]>;
                notionalSum: number;
                notionalSeen: number;
                seen: number;
                sourcesUsed: string[];
                weightsEntries: Array<[string, number]>;
            }
        >();
        const staleSourcesDropped: string[] = [];
        const unitsSeen = new Set<OpenInterestEvent['openInterestUnit']>();

        for (const [streamId, state] of sources.entries()) {
            if (nowTs - state.ts > this.ttlMs) {
                staleSourcesDropped.push(streamId);
                continue;
            }
            const unit = state.openInterestUnit ?? 'unknown';
            const entry =
                grouped.get(unit) ?? {
                    aggregate: 0,
                    valuesEntries: [],
                    notionalSum: 0,
                    notionalSeen: 0,
                    seen: 0,
                    sourcesUsed: [],
                    weightsEntries: [],
                };
            const weight = this.weights[streamId] ?? 1;
            entry.aggregate += state.openInterest * weight;
            entry.valuesEntries.push([streamId, state.openInterest]);
            if (state.openInterestValueUsd !== undefined) {
                entry.notionalSum += state.openInterestValueUsd * weight;
                entry.notionalSeen += 1;
            }
            entry.seen += 1;
            entry.sourcesUsed.push(streamId);
            entry.weightsEntries.push([streamId, weight]);
            grouped.set(unit, entry);
            unitsSeen.add(unit);
        }

        if (grouped.size === 0) {
            const orderedStale = stableSortStrings(staleSourcesDropped);
            return {
                breakdown: {
                    values: {},
                    unit: 'unknown',
                    sourcesUsed: [],
                    weightsUsed: {},
                    staleSourcesDropped: orderedStale,
                    mismatchDetected: false,
                    confidenceScore: computeConfidenceScore({
                        freshSourcesCount: 0,
                        staleSourcesDroppedCount: orderedStale.length,
                        mismatchDetected: false,
                    }),
                },
            };
        }

        const sorted = Array.from(grouped.entries()).sort(([unitA, a], [unitB, b]) => {
            if (b.seen !== a.seen) return b.seen - a.seen;
            return unitA.localeCompare(unitB);
        });

        const [unit, entry] = sorted[0];
        if (entry.seen === 0) {
            const orderedStale = stableSortStrings(staleSourcesDropped);
            return {
                breakdown: {
                    values: {},
                    unit,
                    sourcesUsed: [],
                    weightsUsed: {},
                    staleSourcesDropped: orderedStale,
                    mismatchDetected: unitsSeen.size > 1,
                    confidenceScore: computeConfidenceScore({
                        freshSourcesCount: 0,
                        staleSourcesDroppedCount: orderedStale.length,
                        mismatchDetected: unitsSeen.size > 1,
                    }),
                },
            };
        }

        const openInterestValueUsd = entry.notionalSeen === entry.seen ? entry.notionalSum : undefined;
        const mismatchDetected = unitsSeen.size > 1;
        const orderedSources = stableSortStrings(entry.sourcesUsed);
        const orderedStale = stableSortStrings(staleSourcesDropped);
        const values = stableRecordFromEntries(entry.valuesEntries);
        const weightsUsed = stableRecordFromEntries(entry.weightsEntries);
        const confidenceScore = computeConfidenceScore({
            freshSourcesCount: orderedSources.length,
            staleSourcesDroppedCount: orderedStale.length,
            mismatchDetected,
        });
        return {
            aggregate: entry.aggregate,
            breakdown: {
                values,
                unit,
                openInterestValueUsd,
                sourcesUsed: orderedSources,
                weightsUsed,
                staleSourcesDropped: orderedStale,
                mismatchDetected,
                confidenceScore,
            },
        };
    }
}
