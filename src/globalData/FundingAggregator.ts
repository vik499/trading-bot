import {
    inheritMeta,
    type EventBus,
    type FundingRateEvent,
    type MarketFundingAggEvent,
} from '../core/events/EventBus';
import { computeConfidenceScore } from '../core/confidence';
import { stableRecordFromEntries, stableSortStrings } from '../core/determinism';
import { sourceRegistry } from '../core/market/SourceRegistry';
import { normalizeMarketType, normalizeSymbol } from '../core/market/symbols';
import { detectMismatch } from './quality';

export interface FundingAggregatorOptions {
    ttlMs?: number;
    providerId?: string;
    weights?: Record<string, number>;
}

interface SourceState {
    fundingRate: number;
    ts: number;
}

export class FundingAggregator {
    private readonly bus: EventBus;
    private readonly ttlMs: number;
    private readonly providerId: string;
    private readonly weights: Record<string, number>;
    private readonly sources = new Map<string, Map<string, SourceState>>();
    private unsubscribe?: () => void;

    constructor(bus: EventBus, options: FundingAggregatorOptions = {}) {
        this.bus = bus;
        this.ttlMs = Math.max(10_000, options.ttlMs ?? 120_000);
        this.providerId = options.providerId ?? 'local_funding_agg';
        this.weights = options.weights ?? {};
    }

    start(): void {
        if (this.unsubscribe) return;
        const handler = (evt: FundingRateEvent) => this.onFunding(evt);
        this.bus.subscribe('market:funding', handler);
        this.unsubscribe = () => this.bus.unsubscribe('market:funding', handler);
    }

    stop(): void {
        this.unsubscribe?.();
        this.unsubscribe = undefined;
    }

    private onFunding(evt: FundingRateEvent): void {
        const symbol = evt.symbol;
        const normalizedSymbol = normalizeSymbol(symbol);
        const normalizedMarketType = normalizeMarketType(evt.marketType ?? 'futures');
        const streamId = evt.streamId || 'unknown';
        const ts = evt.meta.ts;
        const sources = this.sources.get(symbol) ?? new Map<string, SourceState>();
        sources.set(streamId, { fundingRate: evt.fundingRate, ts });
        this.sources.set(symbol, sources);

        const { aggregate, breakdown } = this.computeAggregate(ts, sources);
        if (aggregate === undefined) {
            sourceRegistry.recordSuppression(
                { symbol: normalizedSymbol, marketType: normalizedMarketType, metric: 'derivatives' },
                'STALE_INPUT'
            );
            return;
        }

        const payload: MarketFundingAggEvent = {
            symbol,
            ts,
            fundingRate: aggregate,
            marketType: normalizedMarketType,
            venueBreakdown: breakdown.values,
            sourcesUsed: breakdown.sourcesUsed,
            weightsUsed: breakdown.weightsUsed,
            freshSourcesCount: breakdown.sourcesUsed.length,
            staleSourcesDropped: breakdown.staleSourcesDropped.length ? breakdown.staleSourcesDropped : undefined,
            mismatchDetected: breakdown.mismatchDetected,
            confidenceScore: breakdown.confidenceScore,
            qualityFlags: {
                mismatchDetected: breakdown.mismatchDetected,
                staleSourcesDropped: breakdown.staleSourcesDropped.length ? breakdown.staleSourcesDropped : undefined,
            },
            provider: this.providerId,
            meta: inheritMeta(evt.meta, 'global_data', { ts }),
        };
        this.bus.publish('market:funding_agg', payload);
        sourceRegistry.markAggEmitted(
            { symbol: normalizedSymbol, marketType: normalizedMarketType, metric: 'derivatives' },
            breakdown.sourcesUsed,
            ts
        );
    }

    private computeAggregate(
        nowTs: number,
        sources: Map<string, SourceState>
    ): {
        aggregate?: number;
        breakdown: {
            values: Record<string, number>;
            sourcesUsed: string[];
            weightsUsed: Record<string, number>;
            staleSourcesDropped: string[];
            mismatchDetected: boolean;
            confidenceScore: number;
        };
    } {
        let weightedSum = 0;
        let weightTotal = 0;
        const valueEntries: Array<[string, number]> = [];
        const sourcesUsed: string[] = [];
        const weightsEntries: Array<[string, number]> = [];
        const staleSourcesDropped: string[] = [];

        for (const [streamId, state] of sources.entries()) {
            if (nowTs - state.ts > this.ttlMs) {
                staleSourcesDropped.push(streamId);
                continue;
            }
            const weight = this.weights[streamId] ?? 1;
            weightedSum += state.fundingRate * weight;
            weightTotal += weight;
            valueEntries.push([streamId, state.fundingRate]);
            sourcesUsed.push(streamId);
            weightsEntries.push([streamId, weight]);
        }

        if (sourcesUsed.length === 0 || weightTotal === 0) {
            const orderedStale = stableSortStrings(staleSourcesDropped);
            return {
                breakdown: {
                    values: {},
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
        const orderedSources = stableSortStrings(sourcesUsed);
        const orderedStale = stableSortStrings(staleSourcesDropped);
        const values = stableRecordFromEntries(valueEntries);
        const weightsUsed = stableRecordFromEntries(weightsEntries);
        const mismatchDetected = detectMismatch(values);
        const confidenceScore = computeConfidenceScore({
            freshSourcesCount: orderedSources.length,
            staleSourcesDroppedCount: orderedStale.length,
            mismatchDetected,
        });
        return {
            aggregate: weightedSum / weightTotal,
            breakdown: {
                values,
                sourcesUsed: orderedSources,
                weightsUsed,
                staleSourcesDropped: orderedStale,
                mismatchDetected,
                confidenceScore,
            },
        };
    }
}
