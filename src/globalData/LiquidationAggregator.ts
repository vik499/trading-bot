import { asTsMs, inheritMeta, type EventBus, type LiquidationEvent, type MarketLiquidationsAggEvent } from '../core/events/EventBus';
import { bucketCloseTs, bucketStartTs as bucketStartTsFromTs } from '../core/buckets';
import { computeConfidenceScore, getSourceTrustAdjustments } from '../core/confidence';
import { stableRecordFromEntries, stableSortStrings } from '../core/determinism';
import { sourceRegistry } from '../core/market/SourceRegistry';
import { normalizeMarketType, normalizeSymbol } from '../core/market/symbols';
import { detectMismatch } from './quality';

export interface LiquidationAggregatorOptions {
    bucketMs?: number;
    ttlMs?: number;
    providerId?: string;
    weights?: Record<string, number>;
}

interface SourceBucketState {
    liquidationCount: number;
    liquidationUsd: number;
    liquidationBase: number;
    buyUsd: number;
    sellUsd: number;
    buyBase: number;
    sellBase: number;
    usdCount: number;
    ts: number;
}

interface BucketState {
    bucketStartTs: number;
    bucketEndTs: number;
    lastMeta: LiquidationEvent['meta'];
    marketType: LiquidationEvent['marketType'];
    sources: Map<string, SourceBucketState>;
}

export class LiquidationAggregator {
    private readonly bus: EventBus;
    private readonly bucketMs: number;
    private readonly ttlMs: number;
    private readonly providerId: string;
    private readonly weights: Record<string, number>;
    private readonly buckets = new Map<string, BucketState>();
    private unsubscribe?: () => void;

    constructor(bus: EventBus, options: LiquidationAggregatorOptions = {}) {
        this.bus = bus;
        this.bucketMs = Math.max(100, options.bucketMs ?? 10_000);
        this.ttlMs = Math.max(1_000, options.ttlMs ?? 120_000);
        this.providerId = options.providerId ?? 'local_liq_agg';
        this.weights = options.weights ?? {};
    }

    start(): void {
        if (this.unsubscribe) return;
        const handler = (evt: LiquidationEvent) => this.onLiquidation(evt);
        this.bus.subscribe('market:liquidation', handler);
        this.unsubscribe = () => this.bus.unsubscribe('market:liquidation', handler);
    }

    stop(): void {
        this.unsubscribe?.();
        this.unsubscribe = undefined;
    }

    private onLiquidation(evt: LiquidationEvent): void {
        if (!evt.streamId) return;
        const ts = evt.meta.ts;
        const bucketEndTs = bucketCloseTs(ts, this.bucketMs);
        const bucketStartTs = bucketStartTsFromTs(ts, this.bucketMs);
        const symbol = evt.symbol;
        const marketType = normalizeMarketType(evt.marketType ?? 'futures');
        const key = `${symbol}:${marketType}`;
        const current = this.buckets.get(key);

        if (current && current.bucketStartTs !== bucketStartTs) {
            this.emitBucket(symbol, current);
            this.buckets.delete(key);
        }

        const state: BucketState = current ?? {
            bucketStartTs,
            bucketEndTs,
            lastMeta: evt.meta,
            marketType,
            sources: new Map(),
        };

        const source = state.sources.get(evt.streamId) ?? {
            liquidationCount: 0,
            liquidationUsd: 0,
            liquidationBase: 0,
            buyUsd: 0,
            sellUsd: 0,
            buyBase: 0,
            sellBase: 0,
            usdCount: 0,
            ts,
        };

        const qty = evt.size ?? 0;
        const hasUsd = Number.isFinite(evt.notionalUsd);
        const notionalUsd = hasUsd ? (evt.notionalUsd as number) : 0;

        source.liquidationCount += 1;
        source.liquidationBase += qty;
        if (evt.side === 'Buy') source.buyBase += qty;
        if (evt.side === 'Sell') source.sellBase += qty;
        if (hasUsd) {
            source.usdCount += 1;
            source.liquidationUsd += notionalUsd;
            if (evt.side === 'Buy') source.buyUsd += notionalUsd;
            if (evt.side === 'Sell') source.sellUsd += notionalUsd;
        }
        source.ts = ts;
        state.lastMeta = evt.meta;
        state.bucketStartTs = bucketStartTs;
        state.bucketEndTs = bucketEndTs;
        state.sources.set(evt.streamId, source);

        this.buckets.set(key, state);
    }

    private emitBucket(symbol: string, bucket: BucketState): void {
        const normalizedSymbol = normalizeSymbol(symbol);
        const normalizedMarketType = normalizeMarketType(bucket.marketType ?? 'futures');
        let totalUsd = 0;
        let totalBase = 0;
        let buyUsd = 0;
        let sellUsd = 0;
        let buyBase = 0;
        let sellBase = 0;
        let totalCount = 0;
        let weightTotal = 0;
        let usdSeen = 0;
        let usdRequired = 0;
        const venueEntries: Array<[string, number]> = [];
        const sourcesUsed: string[] = [];
        const weightsEntries: Array<[string, number]> = [];
        const staleDropped: string[] = [];

        for (const [streamId, state] of bucket.sources.entries()) {
            if (bucket.bucketEndTs - state.ts > this.ttlMs) {
                staleDropped.push(streamId);
                continue;
            }
            const weight = this.weights[streamId] ?? 1;
            totalUsd += state.liquidationUsd * weight;
            totalBase += state.liquidationBase * weight;
            totalCount += state.liquidationCount;
            buyUsd += state.buyUsd * weight;
            sellUsd += state.sellUsd * weight;
            buyBase += state.buyBase * weight;
            sellBase += state.sellBase * weight;
            weightTotal += weight;
            venueEntries.push([streamId, state.liquidationUsd || state.liquidationBase]);
            usdSeen += state.usdCount;
            usdRequired += state.liquidationCount;
            sourcesUsed.push(streamId);
            weightsEntries.push([streamId, weight]);
        }

        if (!sourcesUsed.length || weightTotal === 0) {
            sourceRegistry.recordSuppression(
                { symbol: normalizedSymbol, marketType: normalizedMarketType, metric: 'derivatives' },
                'STALE_INPUT'
            );
            return;
        }

        const useUsd = usdRequired > 0 && usdSeen === usdRequired;
        const unit = useUsd ? 'usd' : 'base';
        const liquidationNotional = useUsd ? totalUsd : totalBase;
        const sideBreakdown = useUsd ? { buy: buyUsd, sell: sellUsd } : { buy: buyBase, sell: sellBase };
        const orderedSources = stableSortStrings(sourcesUsed);
        const orderedStale = stableSortStrings(staleDropped);
        const venueBreakdown = stableRecordFromEntries(venueEntries);
        const weightsUsed = stableRecordFromEntries(weightsEntries);
        const mismatchDetected = detectMismatch(venueBreakdown);
        const trust = getSourceTrustAdjustments(orderedSources, 'liquidation');
        const confidenceScore = computeConfidenceScore({
            freshSourcesCount: orderedSources.length,
            staleSourcesDroppedCount: orderedStale.length,
            mismatchDetected,
            sourcePenalty: trust.sourcePenalty,
            sourceCap: trust.sourceCap,
        });

        const payload: MarketLiquidationsAggEvent = {
            symbol,
            ts: bucket.bucketEndTs,
            liquidationCount: totalCount,
            liquidationNotional,
            unit,
            sideBreakdown,
            marketType: normalizedMarketType,
            venueBreakdown,
            sourcesUsed: orderedSources,
            weightsUsed,
            freshSourcesCount: orderedSources.length,
            staleSourcesDropped: orderedStale.length ? orderedStale : undefined,
            mismatchDetected,
            confidenceScore,
            qualityFlags: {
                mismatchDetected,
                staleSourcesDropped: orderedStale.length ? orderedStale : undefined,
            },
            provider: this.providerId,
            bucketStartTs: bucket.bucketStartTs,
            bucketEndTs: bucket.bucketEndTs,
            bucketSizeMs: this.bucketMs,
            meta: inheritMeta(bucket.lastMeta, 'global_data', { tsEvent: asTsMs(bucket.bucketEndTs) }),
        };
        this.bus.publish('market:liquidations_agg', payload);
        sourceRegistry.markAggEmitted(
            { symbol: normalizedSymbol, marketType: normalizedMarketType, metric: 'derivatives' },
            orderedSources,
            bucket.bucketEndTs
        );
    }
}
