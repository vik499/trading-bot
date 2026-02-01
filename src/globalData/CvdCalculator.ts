import {
    asTsMs,
    createMeta,
    nowMs,
    type EventBus,
    type KnownMarketType,
    type MarketCvdEvent,
    type TradeEvent,
} from '../core/events/EventBus';
import { bucketCloseTs, bucketStartTs as bucketStartTsFromTs } from '../core/buckets';

export interface CvdCalculatorOptions {
    bucketMs?: number;
    marketTypes?: KnownMarketType[];
    now?: () => number;
}

interface CvdState {
    bucketStartTs: number;
    bucketEndTs: number;
    cvdDelta: number;
    cvdTotal: number;
    lastMeta: TradeEvent['meta'];
}

export class CvdCalculator {
    private readonly bus: EventBus;
    private readonly bucketMs: number;
    private readonly marketTypes: Set<KnownMarketType>;
    private readonly now: () => number;
    private readonly state = new Map<string, CvdState>();
    private unsubscribe?: () => void;

    constructor(bus: EventBus, options: CvdCalculatorOptions = {}) {
        this.bus = bus;
        this.bucketMs = Math.max(100, options.bucketMs ?? 1_000);
        this.marketTypes = new Set(options.marketTypes ?? ['spot', 'futures']);
        this.now = options.now ?? nowMs;
    }

    start(): void {
        if (this.unsubscribe) return;
        const handler = (evt: TradeEvent) => this.onTrade(evt);
        this.bus.subscribe('market:trade', handler);
        this.unsubscribe = () => this.bus.unsubscribe('market:trade', handler);
    }

    stop(): void {
        this.unsubscribe?.();
        this.unsubscribe = undefined;
    }

    private onTrade(evt: TradeEvent): void {
        const marketType = evt.marketType;
        if (!this.marketTypes.has(marketType)) return;
        if (!evt.streamId) return;
        const ts = evt.meta.tsEvent;
        const bucketEndTs = bucketCloseTs(ts, this.bucketMs);
        const bucketStartTs = bucketStartTsFromTs(ts, this.bucketMs);
        const key = `${evt.symbol}:${marketType}:${evt.streamId}`;
        const existing = this.state.get(key);

        if (existing && existing.bucketStartTs !== bucketStartTs) {
            this.emitBucket(evt.symbol, evt.streamId, marketType, existing);
            existing.bucketStartTs = bucketStartTs;
            existing.bucketEndTs = bucketEndTs;
            existing.cvdDelta = 0;
        }

        const state = existing ?? {
            bucketStartTs,
            bucketEndTs,
            cvdDelta: 0,
            cvdTotal: 0,
            lastMeta: evt.meta,
        };

        const delta = evt.side === 'Buy' ? evt.size : -evt.size;
        state.cvdDelta += delta;
        state.cvdTotal += delta;
        state.lastMeta = evt.meta;
        state.bucketStartTs = bucketStartTs;
        state.bucketEndTs = bucketEndTs;

        this.state.set(key, state);
    }

    private emitBucket(symbol: string, streamId: string, marketType: KnownMarketType, state: CvdState): void {
        const payload: MarketCvdEvent = {
            symbol,
            streamId,
            marketType,
            bucketStartTs: state.bucketStartTs,
            bucketEndTs: state.bucketEndTs,
            bucketSizeMs: this.bucketMs,
            cvdDelta: state.cvdDelta,
            cvdTotal: state.cvdTotal,
            unit: 'base',
            exchangeTs: state.bucketEndTs,
            meta: createMeta('global_data', {
                tsEvent: asTsMs(state.bucketEndTs),
                tsIngest: asTsMs(state.lastMeta.tsIngest ?? this.now()),
                correlationId: state.lastMeta.correlationId,
                streamId,
            }),
        };

        if (marketType === 'spot') {
            this.bus.publish('market:cvd_spot', payload);
            return;
        }
        if (marketType === 'futures') {
            this.bus.publish('market:cvd_futures', payload);
            return;
        }
    }
}
