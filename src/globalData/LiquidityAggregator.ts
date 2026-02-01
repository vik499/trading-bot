import {
    asSeq,
    asTsMs,
    inheritMeta,
    type EventBus,
    type EventMeta,
    type MarketLiquidityAggEvent,
    type MarketDisconnected,
    type OrderbookL2DeltaEvent,
    type OrderbookL2SnapshotEvent,
    type OrderbookLevel,
    type Seq,
    type TsMs,
} from '../core/events/EventBus';
import { bucketCloseTs, bucketStartTs as bucketStartTsFromTs } from '../core/buckets';
import { computeConfidenceExplain } from '../core/confidence';
import { stableRecordFromEntries, stableSortStrings } from '../core/determinism';
import { sourceRegistry } from '../core/market/SourceRegistry';
import { normalizeMarketType, normalizeSymbol } from '../core/market/symbols';
import { detectMismatch } from './quality';

export interface LiquidityAggregatorOptions {
    ttlMs?: number;
    providerId?: string;
    weights?: Record<string, number>;
    depthLevels?: number;
    bucketMs?: number;
}

interface OrderbookState {
    bids: Map<number, number>;
    asks: Map<number, number>;
    lastUpdateId?: number;
    prevUpdateId?: number;
    status: 'OK' | 'RESYNCING';
    sequenceBroken: boolean;
    hasSnapshot: boolean;
}

interface VenueLiquidity {
    ts: number;
    bestBid?: number;
    bestAsk?: number;
    spread?: number;
    depthBid?: number;
    depthAsk?: number;
    imbalance?: number;
    midPrice?: number;
}

interface BucketCursor {
    bucketStartTs: number;
    bucketEndTs: number;
}

export class LiquidityAggregator {
    private readonly bus: EventBus;
    private readonly ttlMs: number;
    private readonly providerId: string;
    private readonly weights: Record<string, number>;
    private readonly depthLevels: number;
    private readonly bucketMs: number;
    private readonly orderbooks = new Map<string, Map<string, OrderbookState>>();
    private readonly metrics = new Map<string, Map<string, VenueLiquidity>>();
    private readonly buckets = new Map<string, BucketCursor>();
    private unsubscribe?: () => void;

    constructor(bus: EventBus, options: LiquidityAggregatorOptions = {}) {
        this.bus = bus;
        this.ttlMs = Math.max(1_000, options.ttlMs ?? 120_000);
        this.providerId = options.providerId ?? 'local_liquidity_agg';
        this.weights = options.weights ?? {};
        this.depthLevels = Math.max(1, options.depthLevels ?? 10);
        this.bucketMs = Math.max(100, options.bucketMs ?? 1_000);
    }

    start(): void {
        if (this.unsubscribe) return;
        const onSnapshot = (evt: OrderbookL2SnapshotEvent) => this.onSnapshot(evt);
        const onDelta = (evt: OrderbookL2DeltaEvent) => this.onDelta(evt);
        const onDisconnected = (_evt: MarketDisconnected) => this.resetAll();
        this.bus.subscribe('market:orderbook_l2_snapshot', onSnapshot);
        this.bus.subscribe('market:orderbook_l2_delta', onDelta);
        this.bus.subscribe('market:disconnected', onDisconnected);
        this.unsubscribe = () => {
            this.bus.unsubscribe('market:orderbook_l2_snapshot', onSnapshot);
            this.bus.unsubscribe('market:orderbook_l2_delta', onDelta);
            this.bus.unsubscribe('market:disconnected', onDisconnected);
        };
    }

    stop(): void {
        this.unsubscribe?.();
        this.unsubscribe = undefined;
    }

    private onSnapshot(evt: OrderbookL2SnapshotEvent): void {
        if (!evt.streamId) return;
        const normalizedSymbol = normalizeSymbol(evt.symbol);
        const normalizedMarketType = normalizeMarketType(evt.marketType);
        const key = this.key(normalizedSymbol, normalizedMarketType);
        const state = this.ensureState(key, evt.streamId);
        state.bids = toLevelMap(evt.bids);
        state.asks = toLevelMap(evt.asks);
        state.prevUpdateId = state.lastUpdateId;
        state.lastUpdateId = evt.updateId;
        state.status = 'OK';
        state.sequenceBroken = false;
        state.hasSnapshot = true;
        this.updateMetrics(evt, state, key, normalizedSymbol, normalizedMarketType);
    }

    private onDelta(evt: OrderbookL2DeltaEvent): void {
        if (!evt.streamId) return;
        const normalizedSymbol = normalizeSymbol(evt.symbol);
        const normalizedMarketType = normalizeMarketType(evt.marketType);
        const key = this.key(normalizedSymbol, normalizedMarketType);
        const state = this.ensureState(key, evt.streamId);
        if (!state.hasSnapshot || state.status === 'RESYNCING') {
            this.enterResync(key, evt.streamId, state);
            return;
        }
        const strictSequence = this.requiresStrictSequence(evt.streamId, evt.updateId, evt.exchangeTs);
        if (state.lastUpdateId !== undefined) {
            if (evt.updateId <= state.lastUpdateId) return;
            if (strictSequence && evt.updateId > state.lastUpdateId + 1) {
                this.enterResync(key, evt.streamId, state);
                return;
            }
        }
        applyLevels(state.bids, evt.bids);
        applyLevels(state.asks, evt.asks);
        state.prevUpdateId = state.lastUpdateId;
        state.lastUpdateId = evt.updateId;
        this.updateMetrics(evt, state, key, normalizedSymbol, normalizedMarketType);
    }

    private updateMetrics(
        evt: OrderbookL2SnapshotEvent | OrderbookL2DeltaEvent,
        state: OrderbookState,
        key: string,
        symbol: string,
        marketType: ReturnType<typeof normalizeMarketType>
    ): void {
        const byStream = this.metrics.get(key) ?? new Map();
        this.advanceBuckets(key, symbol, marketType, evt.meta.ts, evt.meta, byStream);

        const bestBid = bestPrice(state.bids, 'bid');
        const bestAsk = bestPrice(state.asks, 'ask');
        const depthBid = depthSum(state.bids, 'bid', this.depthLevels);
        const depthAsk = depthSum(state.asks, 'ask', this.depthLevels);
        const spread = bestBid !== undefined && bestAsk !== undefined ? bestAsk - bestBid : undefined;
        const totalDepth = depthBid + depthAsk;
        const imbalance = totalDepth > 0 ? (depthBid - depthAsk) / totalDepth : undefined;
        const midPrice = bestBid !== undefined && bestAsk !== undefined ? (bestBid + bestAsk) / 2 : undefined;

        const venueMetrics: VenueLiquidity = {
            ts: evt.meta.ts,
            bestBid,
            bestAsk,
            spread,
            depthBid,
            depthAsk,
            imbalance,
            midPrice,
        };

        byStream.set(evt.streamId, venueMetrics);
        this.metrics.set(key, byStream);
    }

    private advanceBuckets(
        key: string,
        symbol: string,
        marketType: ReturnType<typeof normalizeMarketType>,
        ts: number,
        parentMeta: EventMeta,
        sources: Map<string, VenueLiquidity>
    ): void {
        const bucketEndTs = bucketCloseTs(ts, this.bucketMs);
        const bucketStartTs = bucketStartTsFromTs(ts, this.bucketMs);
        const cursor = this.buckets.get(key);

        if (!cursor) {
            this.buckets.set(key, { bucketStartTs, bucketEndTs });
            return;
        }

        while (ts >= cursor.bucketEndTs) {
            this.emitAggregate(symbol, marketType, cursor.bucketStartTs, cursor.bucketEndTs, parentMeta, sources);
            cursor.bucketStartTs += this.bucketMs;
            cursor.bucketEndTs += this.bucketMs;
        }
    }

    private emitAggregate(
        symbol: string,
        marketType: ReturnType<typeof normalizeMarketType>,
        bucketStartTs: number,
        bucketEndTs: number,
        parentMeta: EventMeta,
        sources: Map<string, VenueLiquidity>
    ): void {
        const normalizedSymbol = normalizeSymbol(symbol);
        if (marketType === 'unknown') return;
        let bestBid = 0;
        let bestAsk = 0;
        let spread = 0;
        let depthBid = 0;
        let depthAsk = 0;
        let imbalance = 0;
        let midPrice = 0;
        let weightBestBid = 0;
        let weightBestAsk = 0;
        let weightSpread = 0;
        let weightDepthBid = 0;
        let weightDepthAsk = 0;
        let weightImbalance = 0;
        let weightMid = 0;
        const venueEntries: Array<[string, number]> = [];
        const sourcesUsed: string[] = [];
        const weightsEntries: Array<[string, number]> = [];
        const staleDropped: string[] = [];
        const venueStatus: Record<
            string,
            {
                freshness: 'fresh' | 'stale' | 'dropped';
                sequenceBroken?: boolean;
                lastSeq?: Seq;
                prevSeq?: Seq;
                lastTsEvent?: TsMs;
            }
        > = {};

        const orderbookStates = this.orderbooks.get(this.key(normalizedSymbol, marketType)) ?? new Map();
        const streamIds = new Set<string>([...sources.keys(), ...orderbookStates.keys()]);

        for (const streamId of streamIds) {
            const metrics = sources.get(streamId);
            if (!metrics) {
                const state = orderbookStates.get(streamId);
                venueStatus[streamId] = {
                    freshness: 'dropped',
                    sequenceBroken: state?.sequenceBroken,
                    lastSeq: state?.lastUpdateId !== undefined ? asSeq(state.lastUpdateId) : undefined,
                    prevSeq: state?.prevUpdateId !== undefined ? asSeq(state.prevUpdateId) : undefined,
                };
                continue;
            }
            const isStale = bucketEndTs - metrics.ts > this.ttlMs;
            const inBucket = metrics.ts >= bucketStartTs && metrics.ts < bucketEndTs;
            if (isStale || !inBucket) {
                if (isStale) staleDropped.push(streamId);
                const state = orderbookStates.get(streamId);
                venueStatus[streamId] = {
                    freshness: isStale ? 'stale' : 'dropped',
                    sequenceBroken: state?.sequenceBroken,
                    lastSeq: state?.lastUpdateId !== undefined ? asSeq(state.lastUpdateId) : undefined,
                    prevSeq: state?.prevUpdateId !== undefined ? asSeq(state.prevUpdateId) : undefined,
                    lastTsEvent: asTsMs(metrics.ts),
                };
                continue;
            }
            const state = orderbookStates.get(streamId);
            venueStatus[streamId] = {
                freshness: 'fresh',
                sequenceBroken: state?.sequenceBroken,
                lastSeq: state?.lastUpdateId !== undefined ? asSeq(state.lastUpdateId) : undefined,
                prevSeq: state?.prevUpdateId !== undefined ? asSeq(state.prevUpdateId) : undefined,
                lastTsEvent: asTsMs(metrics.ts),
            };
            const weight = this.weights[streamId] ?? 1;
            if (metrics.bestBid !== undefined) {
                bestBid += metrics.bestBid * weight;
                weightBestBid += weight;
            }
            if (metrics.bestAsk !== undefined) {
                bestAsk += metrics.bestAsk * weight;
                weightBestAsk += weight;
            }
            if (metrics.spread !== undefined) {
                spread += metrics.spread * weight;
                weightSpread += weight;
            }
            if (metrics.depthBid !== undefined) {
                depthBid += metrics.depthBid * weight;
                weightDepthBid += weight;
            }
            if (metrics.depthAsk !== undefined) {
                depthAsk += metrics.depthAsk * weight;
                weightDepthAsk += weight;
            }
            if (metrics.imbalance !== undefined) {
                imbalance += metrics.imbalance * weight;
                weightImbalance += weight;
            }
            if (metrics.midPrice !== undefined) {
                midPrice += metrics.midPrice * weight;
                weightMid += weight;
                venueEntries.push([streamId, metrics.midPrice]);
            }
            sourcesUsed.push(streamId);
            weightsEntries.push([streamId, weight]);
        }

        if (!sourcesUsed.length) {
            sourceRegistry.recordSuppression(
                { symbol: normalizedSymbol, marketType, metric: 'liquidity' },
                'STALE_INPUT'
            );
            return;
        }

        const orderedSources = stableSortStrings(sourcesUsed);
        const orderedStale = stableSortStrings(staleDropped);
        const venueBreakdown = stableRecordFromEntries(venueEntries);
        const weightsUsed = stableRecordFromEntries(weightsEntries);
        const sequenceBroken = this.isSequenceBrokenForSources(orderbookStates, orderedSources);
        if (sequenceBroken) {
            sourceRegistry.recordSuppression(
                { symbol: normalizedSymbol, marketType, metric: 'liquidity' },
                'RESYNC_ACTIVE'
            );
        }

        const mismatchDetected = detectMismatch(Object.keys(venueBreakdown).length ? venueBreakdown : undefined);
        const confidenceExplain = computeConfidenceExplain({
            freshSourcesCount: orderedSources.length,
            staleSourcesDroppedCount: orderedStale.length,
            mismatchDetected,
            sequenceBroken,
        });
        const confidenceScore = confidenceExplain.score;

        const payload: MarketLiquidityAggEvent = {
            symbol,
            ts: bucketEndTs,
            bestBid: weightBestBid ? bestBid / weightBestBid : undefined,
            bestAsk: weightBestAsk ? bestAsk / weightBestAsk : undefined,
            spread: weightSpread ? spread / weightSpread : undefined,
            depthBid: weightDepthBid ? depthBid / weightDepthBid : undefined,
            depthAsk: weightDepthAsk ? depthAsk / weightDepthAsk : undefined,
            imbalance: weightImbalance ? imbalance / weightImbalance : undefined,
            midPrice: weightMid ? midPrice / weightMid : undefined,
            depthMethod: 'levels',
            depthLevels: this.depthLevels,
            depthUnit: 'base',
            bucketStartTs,
            bucketEndTs,
            bucketSizeMs: this.bucketMs,
            marketType,
            venueBreakdown: Object.keys(venueBreakdown).length ? venueBreakdown : undefined,
            sourcesUsed: orderedSources,
            weightsUsed,
            freshSourcesCount: orderedSources.length,
            staleSourcesDropped: orderedStale.length ? orderedStale : undefined,
            mismatchDetected,
            confidenceScore,
            confidenceExplain: {
                score: confidenceExplain.score,
                penalties: confidenceExplain.penalties,
                inputs: confidenceExplain.inputs,
            },
            qualityFlags: {
                mismatchDetected,
                staleSourcesDropped: orderedStale.length ? orderedStale : undefined,
                sequenceBroken,
            },
            venueStatus: Object.keys(venueStatus).length ? venueStatus : undefined,
            provider: this.providerId,
            meta: inheritMeta(parentMeta, 'global_data', { tsEvent: asTsMs(bucketEndTs) }),
        };
        this.bus.publish('market:liquidity_agg', payload);
        sourceRegistry.markAggEmitted(
            { symbol: normalizedSymbol, marketType, metric: 'liquidity' },
            orderedSources,
            bucketEndTs
        );
    }

    private ensureState(key: string, streamId: string): OrderbookState {
        const byStream = this.orderbooks.get(key) ?? new Map();
        const existing = byStream.get(streamId);
        if (existing) return existing;
        const fresh: OrderbookState = {
            bids: new Map(),
            asks: new Map(),
            status: 'RESYNCING',
            sequenceBroken: false,
            hasSnapshot: false,
        };
        byStream.set(streamId, fresh);
        this.orderbooks.set(key, byStream);
        return fresh;
    }

    private enterResync(key: string, streamId: string, state: OrderbookState): void {
        if (state.status === 'RESYNCING' && state.sequenceBroken) return;
        state.status = 'RESYNCING';
        state.sequenceBroken = true;
        state.hasSnapshot = false;
        state.lastUpdateId = undefined;
        state.bids = new Map();
        state.asks = new Map();
        const byStream = this.metrics.get(key);
        if (byStream) {
            byStream.delete(streamId);
        }
    }

    private isSequenceBrokenForSources(orderbookStates: Map<string, OrderbookState>, sources: string[]): boolean {
        for (const streamId of sources) {
            const state = orderbookStates.get(streamId);
            if (state?.sequenceBroken && state.status === 'RESYNCING') return true;
        }
        return false;
    }

    private requiresStrictSequence(streamId: string, updateId?: number, exchangeTs?: number): boolean {
        if (/binance/i.test(streamId)) return false;
        if (updateId !== undefined && exchangeTs !== undefined && Math.abs(updateId - exchangeTs) <= 10_000) return false;
        return true;
    }

    private key(symbol: string, marketType: ReturnType<typeof normalizeMarketType>): string {
        return `${symbol}:${marketType}`;
    }

    private resetAll(): void {
        this.orderbooks.clear();
        this.metrics.clear();
        this.buckets.clear();
    }
}

function toLevelMap(levels: OrderbookLevel[]): Map<number, number> {
    const map = new Map<number, number>();
    for (const lvl of levels) {
        if (!Number.isFinite(lvl.price) || !Number.isFinite(lvl.size)) continue;
        if (lvl.size <= 0) continue;
        map.set(lvl.price, lvl.size);
    }
    return map;
}

function applyLevels(map: Map<number, number>, levels: OrderbookLevel[]): void {
    for (const lvl of levels) {
        if (!Number.isFinite(lvl.price)) continue;
        if (!Number.isFinite(lvl.size) || lvl.size <= 0) {
            map.delete(lvl.price);
            continue;
        }
        map.set(lvl.price, lvl.size);
    }
}

function bestPrice(map: Map<number, number>, side: 'bid' | 'ask'): number | undefined {
    if (map.size === 0) return undefined;
    let best: number | undefined;
    for (const price of map.keys()) {
        if (best === undefined) {
            best = price;
            continue;
        }
        if (side === 'bid' && price > best) best = price;
        if (side === 'ask' && price < best) best = price;
    }
    return best;
}

function depthSum(map: Map<number, number>, side: 'bid' | 'ask', levels: number): number {
    if (map.size === 0) return 0;
    const prices = Array.from(map.keys()).sort((a, b) => (side === 'bid' ? b - a : a - b));
    let sum = 0;
    for (let i = 0; i < prices.length && i < levels; i += 1) {
        const size = map.get(prices[i]) ?? 0;
        sum += size;
    }
    return sum;
}
