import type { MarketType } from '../events/EventBus';
import { normalizeMarketType, normalizeSymbol } from './symbols';

export type MetricKey = 'price' | 'flow' | 'liquidity' | 'derivatives';
export type RawFeed = 'trades' | 'orderbook' | 'oi' | 'funding' | 'markPrice' | 'indexPrice' | 'klines';
export type SuppressionReason =
    | 'NO_CANONICAL_PRICE'
    | 'CONFIDENCE_TOO_LOW'
    | 'RESYNC_ACTIVE'
    | 'STALE_INPUT'
    | 'LAG_TOO_HIGH'
    | 'GAPS_DETECTED';

export interface MetricKeyInput {
    symbol: string;
    marketType?: MarketType;
    metric: MetricKey;
}

export interface RawKeyInput {
    symbol: string;
    marketType?: MarketType;
    feed: RawFeed;
}

export interface SourceRegistrySnapshot {
    symbol: string;
    marketType: MarketType;
    expected: Record<MetricKey, string[]>;
    usedAgg: Record<MetricKey, string[]>;
    usedRaw: Record<RawFeed, string[]>;
    lastSeenRawTs: Record<RawFeed, number | null>;
    lastSeenAggTs: {
        priceCanonical: number | null;
        flowAgg: number | null;
        liquidityAgg: number | null;
        derivativesAgg: number | null;
    };
    suppressions: Record<MetricKey, { count: number; reasons: Record<string, number> }>;
    nonMonotonicSources: string[];
}

interface MetricState {
    expected: Set<string>;
    usedAgg: Set<string>;
    lastAggTs?: number;
    suppressions: Map<string, number>;
}

interface RawState {
    usedRaw: Set<string>;
    lastRawTs?: number;
    lastBySource: Map<string, number>;
    nonMonotonicSources: Set<string>;
}

const METRICS: MetricKey[] = ['price', 'flow', 'liquidity', 'derivatives'];
const FEEDS: RawFeed[] = ['trades', 'orderbook', 'oi', 'funding', 'markPrice', 'indexPrice', 'klines'];

export class SourceRegistry {
    private readonly metricStates = new Map<string, MetricState>();
    private readonly rawStates = new Map<string, RawState>();

    reset(): void {
        this.metricStates.clear();
        this.rawStates.clear();
    }

    registerExpected(key: MetricKeyInput, expectedSources: string[]): void {
        const metricKey = this.metricKey(key);
        const state = this.ensureMetricState(metricKey);
        state.expected = new Set(expectedSources.filter((src) => src.trim().length > 0));
    }

    markRawSeen(key: RawKeyInput, exchange: string, tsMeta: number): void {
        const rawKey = this.rawKey(key);
        const state = this.ensureRawState(rawKey);
        const prev = state.lastBySource.get(exchange);
        if (prev !== undefined && tsMeta < prev) {
            if (key.feed !== 'klines') {
                state.nonMonotonicSources.add(exchange);
            }
            return;
        }
        state.lastBySource.set(exchange, tsMeta);
        state.usedRaw.add(exchange);
        state.lastRawTs = Math.max(state.lastRawTs ?? 0, tsMeta);
    }

    markAggEmitted(key: MetricKeyInput, sourcesUsed: string[], tsMeta: number): void {
        const metricKey = this.metricKey(key);
        const state = this.ensureMetricState(metricKey);
        state.usedAgg = new Set(sourcesUsed.filter((src) => src.trim().length > 0));
        state.lastAggTs = tsMeta;
    }

    recordSuppression(key: MetricKeyInput, reason: SuppressionReason): void {
        const metricKey = this.metricKey(key);
        const state = this.ensureMetricState(metricKey);
        const prev = state.suppressions.get(reason) ?? 0;
        state.suppressions.set(reason, prev + 1);
    }

    snapshot(tsMeta: number, symbol: string, marketType?: MarketType): SourceRegistrySnapshot {
        const normalizedSymbol = normalizeSymbol(symbol);
        const normalizedMarketType = normalizeMarketType(marketType);

        const expected = this.buildMetricRecord(normalizedSymbol, normalizedMarketType, (state) => Array.from(state.expected));
        const usedAgg = this.buildMetricRecord(normalizedSymbol, normalizedMarketType, (state) => Array.from(state.usedAgg));
        const suppressions = this.buildMetricRecord(normalizedSymbol, normalizedMarketType, (state) => {
            const reasons: Record<string, number> = {};
            for (const [reason, count] of state.suppressions.entries()) {
                reasons[reason] = count;
            }
            const count = Object.values(reasons).reduce((sum, value) => sum + value, 0);
            return { count, reasons };
        });

        const usedRaw = this.buildFeedRecord(normalizedSymbol, normalizedMarketType, (state) => Array.from(state.usedRaw));
        const lastSeenRawTs = this.buildFeedRecord(normalizedSymbol, normalizedMarketType, (state) => state.lastRawTs ?? null);

        const nonMonotonicSources = this.collectNonMonotonicSources(normalizedSymbol, normalizedMarketType);

        return {
            symbol: normalizedSymbol,
            marketType: normalizedMarketType,
            expected,
            usedAgg,
            usedRaw,
            lastSeenRawTs,
            lastSeenAggTs: {
                priceCanonical: this.metricLastAggTs(normalizedSymbol, normalizedMarketType, 'price'),
                flowAgg: this.metricLastAggTs(normalizedSymbol, normalizedMarketType, 'flow'),
                liquidityAgg: this.metricLastAggTs(normalizedSymbol, normalizedMarketType, 'liquidity'),
                derivativesAgg: this.metricLastAggTs(normalizedSymbol, normalizedMarketType, 'derivatives'),
            },
            suppressions,
            nonMonotonicSources,
        };
    }

    private metricKey(key: MetricKeyInput): string {
        const symbol = normalizeSymbol(key.symbol);
        const marketType = normalizeMarketType(key.marketType);
        return `${symbol}:${marketType}:${key.metric}`;
    }

    private rawKey(key: RawKeyInput): string {
        const symbol = normalizeSymbol(key.symbol);
        const marketType = normalizeMarketType(key.marketType);
        return `${symbol}:${marketType}:${key.feed}`;
    }

    private ensureMetricState(metricKey: string): MetricState {
        const existing = this.metricStates.get(metricKey);
        if (existing) return existing;
        const fresh: MetricState = {
            expected: new Set(),
            usedAgg: new Set(),
            suppressions: new Map(),
        };
        this.metricStates.set(metricKey, fresh);
        return fresh;
    }

    private ensureRawState(rawKey: string): RawState {
        const existing = this.rawStates.get(rawKey);
        if (existing) return existing;
        const fresh: RawState = {
            usedRaw: new Set(),
            lastBySource: new Map(),
            nonMonotonicSources: new Set(),
        };
        this.rawStates.set(rawKey, fresh);
        return fresh;
    }

    private buildMetricRecord<T>(
        symbol: string,
        marketType: MarketType,
        mapper: (state: MetricState) => T
    ): Record<MetricKey, T> {
        const result = {} as Record<MetricKey, T>;
        for (const metric of METRICS) {
            const state = this.metricStates.get(`${symbol}:${marketType}:${metric}`);
            result[metric] = mapper(state ?? { expected: new Set(), usedAgg: new Set(), suppressions: new Map() });
        }
        return result;
    }

    private buildFeedRecord<T>(
        symbol: string,
        marketType: MarketType,
        mapper: (state: RawState) => T
    ): Record<RawFeed, T> {
        const result = {} as Record<RawFeed, T>;
        for (const feed of FEEDS) {
            const state = this.rawStates.get(`${symbol}:${marketType}:${feed}`);
            result[feed] = mapper(state ?? { usedRaw: new Set(), lastBySource: new Map(), nonMonotonicSources: new Set() });
        }
        return result;
    }

    private metricLastAggTs(symbol: string, marketType: MarketType, metric: MetricKey): number | null {
        const state = this.metricStates.get(`${symbol}:${marketType}:${metric}`);
        return state?.lastAggTs ?? null;
    }

    private collectNonMonotonicSources(symbol: string, marketType: MarketType): string[] {
        const sources = new Set<string>();
        for (const feed of FEEDS) {
            const state = this.rawStates.get(`${symbol}:${marketType}:${feed}`);
            if (!state) continue;
            state.nonMonotonicSources.forEach((src) => sources.add(src));
        }
        return Array.from(sources);
    }
}

export const sourceRegistry = new SourceRegistry();
