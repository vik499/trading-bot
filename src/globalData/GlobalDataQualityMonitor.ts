import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    createMeta,
    eventBus as defaultEventBus,
    type DataMismatch,
    type DataStale,
    type DataConfidence,
    type EventBus,
    type MarketCvdAggEvent,
    type MarketFundingAggEvent,
    type MarketLiquidationsAggEvent,
    type MarketLiquidityAggEvent,
    type MarketOpenInterestAggEvent,
    type MarketPriceCanonicalEvent,
    type MarketPriceIndexEvent,
    type MarketVolumeAggEvent,
} from '../core/events/EventBus';
import { computeConfidenceScore } from '../core/confidence';

const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === 'object' && value !== null;

function hasVenueBreakdown(evt: unknown): evt is { venueBreakdown: Record<string, number> } {
    if (!isRecord(evt)) return false;
    const breakdown = evt.venueBreakdown;
    return isRecord(breakdown);
}

function hasQualityFlags(evt: unknown): evt is {
    qualityFlags: { staleSourcesDropped?: string[]; mismatchDetected?: boolean; sequenceBroken?: boolean };
} {
    if (!isRecord(evt)) return false;
    const qualityFlags = evt.qualityFlags;
    return isRecord(qualityFlags);
}

export interface GlobalDataQualityConfig {
    expectedIntervalMs?: Partial<Record<string, number>>;
    staleMultiplier?: number;
    mismatchThresholdPct?: number;
    mismatchWindowMs?: number;
    logThrottleMs?: number;
}

type AggregatedEvent =
    | MarketOpenInterestAggEvent
    | MarketFundingAggEvent
    | MarketLiquidationsAggEvent
    | MarketLiquidityAggEvent
    | MarketVolumeAggEvent
    | MarketCvdAggEvent
    | MarketPriceIndexEvent
    | MarketPriceCanonicalEvent;

export class GlobalDataQualityMonitor {
    private readonly bus: EventBus;
    private readonly config: Required<GlobalDataQualityConfig>;
    private started = false;
    private readonly unsubscribers: Array<() => void> = [];
    private readonly lastTsByKey = new Map<string, number>();
    private readonly lastLogByKey = new Map<string, number>();
    private readonly degradedByKey = new Map<
        string,
        {
            sourceId: string;
            stale: boolean;
            mismatch: boolean;
            degraded: boolean;
            lastErrorTs?: number;
            lastSuccessTs?: number;
        }
    >();
    private readonly mismatchByKey = new Map<
        string,
        {
            firstTs: number;
            lastTs: number;
            active: boolean;
        }
    >();
    private readonly referencePriceBySymbol = new Map<string, { price: number; ts: number }>();

    constructor(bus: EventBus = defaultEventBus, config: GlobalDataQualityConfig = {}) {
        this.bus = bus;
        this.config = {
            expectedIntervalMs: config.expectedIntervalMs ?? {},
            staleMultiplier: Math.max(1, config.staleMultiplier ?? 2),
            mismatchThresholdPct: Math.max(0, config.mismatchThresholdPct ?? 0.1),
            mismatchWindowMs: Math.max(0, config.mismatchWindowMs ?? 60_000),
            logThrottleMs: Math.max(0, config.logThrottleMs ?? 30_000),
        };
    }

    start(): void {
        if (this.started) return;
        const onOi = (evt: MarketOpenInterestAggEvent) => this.onAggEvent('market:oi_agg', evt);
        const onFunding = (evt: MarketFundingAggEvent) => this.onAggEvent('market:funding_agg', evt);
        const onLiquidations = (evt: MarketLiquidationsAggEvent) => this.onAggEvent('market:liquidations_agg', evt);
        const onLiquidity = (evt: MarketLiquidityAggEvent) => this.onAggEvent('market:liquidity_agg', evt);
        const onVolume = (evt: MarketVolumeAggEvent) => this.onAggEvent('market:volume_agg', evt);
        const onCvdSpot = (evt: MarketCvdAggEvent) => this.onAggEvent('market:cvd_spot_agg', evt);
        const onCvdFutures = (evt: MarketCvdAggEvent) => this.onAggEvent('market:cvd_futures_agg', evt);
        const onIndex = (evt: MarketPriceIndexEvent) => this.onAggEvent('market:price_index', evt);
        const onCanonical = (evt: MarketPriceCanonicalEvent) => this.onPriceCanonical(evt);

        this.bus.subscribe('market:oi_agg', onOi);
        this.bus.subscribe('market:funding_agg', onFunding);
        this.bus.subscribe('market:liquidations_agg', onLiquidations);
        this.bus.subscribe('market:liquidity_agg', onLiquidity);
        this.bus.subscribe('market:volume_agg', onVolume);
        this.bus.subscribe('market:cvd_spot_agg', onCvdSpot);
        this.bus.subscribe('market:cvd_futures_agg', onCvdFutures);
        this.bus.subscribe('market:price_index', onIndex);
        this.bus.subscribe('market:price_canonical', onCanonical);

        this.unsubscribers.push(
            () => this.bus.unsubscribe('market:oi_agg', onOi),
            () => this.bus.unsubscribe('market:funding_agg', onFunding),
            () => this.bus.unsubscribe('market:liquidations_agg', onLiquidations),
            () => this.bus.unsubscribe('market:liquidity_agg', onLiquidity),
            () => this.bus.unsubscribe('market:volume_agg', onVolume),
            () => this.bus.unsubscribe('market:cvd_spot_agg', onCvdSpot),
            () => this.bus.unsubscribe('market:cvd_futures_agg', onCvdFutures),
            () => this.bus.unsubscribe('market:price_index', onIndex),
            () => this.bus.unsubscribe('market:price_canonical', onCanonical)
        );

        this.started = true;
        logger.info(m('lifecycle', '[GlobalDataQualityMonitor] started'));
    }

    stop(): void {
        if (!this.started) return;
        this.unsubscribers.forEach((fn) => fn());
        this.unsubscribers.length = 0;
        this.started = false;
    }

    private onAggEvent(topic: string, evt: AggregatedEvent): void {
        this.detectStale(topic, evt);
        this.detectMismatch(topic, evt);
        this.emitConfidence(topic, evt);
    }

    private onPriceCanonical(evt: MarketPriceCanonicalEvent): void {
        const price = evt.indexPrice ?? evt.markPrice ?? evt.lastPrice;
        if (Number.isFinite(price)) {
            this.referencePriceBySymbol.set(evt.symbol, { price: price as number, ts: evt.meta.ts });
        }
        this.onAggEvent('market:price_canonical', evt);
    }

    private detectStale(topic: string, evt: AggregatedEvent): void {
        const expected = this.config.expectedIntervalMs[topic];
        const key = this.makeKey(topic, evt.symbol, evt.provider);
        if (!expected) {
            this.lastTsByKey.set(key, evt.ts);
            return;
        }
        const last = this.lastTsByKey.get(key);
        this.lastTsByKey.set(key, evt.ts);
        if (last === undefined) return;
        const delta = evt.ts - last;
        if (delta <= expected * this.config.staleMultiplier) {
            this.clearDegradedFlag(key, 'stale', evt.meta.ts);
            return;
        }

        const payload: DataStale = {
            sourceId: this.resolveSourceId(topic, evt),
            topic,
            symbol: evt.symbol,
            lastTs: last,
            currentTs: evt.ts,
            expectedMs: expected,
            meta: createMeta('global_data', { ts: evt.meta.ts }),
        };
        this.bus.publish('data:stale', payload);
        this.logThrottled(`${key}:stale`, evt.meta.ts, `[GlobalData] stale topic=${topic} symbol=${evt.symbol} deltaMs=${delta}`);
        this.markDegraded(key, this.resolveSourceId(topic, evt), last, evt.meta.ts, 'stale');
    }

    private detectMismatch(topic: string, evt: AggregatedEvent): void {
        const breakdown = hasVenueBreakdown(evt) ? evt.venueBreakdown : undefined;
        if (!breakdown) return;
        const values = Object.entries(breakdown).filter(
            (entry): entry is [string, number] => typeof entry[1] === 'number' && Number.isFinite(entry[1])
        );
        if (values.length < 2) return;
        const nums = Object.values(breakdown).filter((value): value is number => typeof value === 'number');
        const min = Math.min(...nums);
        const max = Math.max(...nums);
        if (min <= 0) return;
        const diffPct = (max - min) / min;
        const key = this.makeKey(topic, evt.symbol, evt.provider);

        if (diffPct < this.config.mismatchThresholdPct) {
            const state = this.mismatchByKey.get(key);
            if (state?.active) {
                this.clearDegradedFlag(key, 'mismatch', evt.meta.ts);
            }
            this.mismatchByKey.delete(key);
            return;
        }

        const next = this.mismatchByKey.get(key) ?? { firstTs: evt.ts, lastTs: evt.ts, active: false };
        next.lastTs = evt.ts;
        this.mismatchByKey.set(key, next);

        if (!next.active && evt.ts - next.firstTs >= this.config.mismatchWindowMs) {
            const payload: DataMismatch = {
                symbol: evt.symbol,
                topic,
                ts: evt.ts,
                values: Object.fromEntries(values),
                thresholdPct: this.config.mismatchThresholdPct,
                meta: createMeta('global_data', { ts: evt.meta.ts }),
            };
            this.bus.publish('data:mismatch', payload);
            this.logThrottled(
                `${key}:mismatch`,
                evt.meta.ts,
                `[GlobalData] mismatch topic=${topic} symbol=${evt.symbol} diffPct=${diffPct.toFixed(4)}`
            );
            next.active = true;
            this.mismatchByKey.set(key, next);
            this.markDegraded(key, this.resolveSourceId(topic, evt), evt.ts, evt.meta.ts, 'mismatch');
        }
    }

    private hasReferencePrice(symbol: string, tsMeta: number): boolean {
        const ref = this.referencePriceBySymbol.get(symbol);
        if (!ref || !Number.isFinite(ref.price)) return false;
        const expected = this.config.expectedIntervalMs['market:price_canonical'];
        if (!expected) return true;
        return tsMeta - ref.ts <= expected * this.config.staleMultiplier;
    }

    private emitConfidence(topic: string, evt: AggregatedEvent): void {
        const freshSourcesCount = evt.freshSourcesCount ?? evt.sourcesUsed?.length ?? 0;
        const staleSourcesDropped =
            evt.staleSourcesDropped ?? (hasQualityFlags(evt) ? evt.qualityFlags.staleSourcesDropped : undefined) ?? [];
        const mismatchDetected =
            evt.mismatchDetected ?? (hasQualityFlags(evt) ? evt.qualityFlags.mismatchDetected : undefined) ?? false;
        const sequenceBroken =
            (hasQualityFlags(evt) ? evt.qualityFlags.sequenceBroken : undefined) ?? false;
        const confidenceScore =
            ('confidence' in evt && typeof evt.confidence === 'number' ? evt.confidence : evt.confidenceScore) ??
            computeConfidenceScore({
                freshSourcesCount,
                staleSourcesDroppedCount: staleSourcesDropped.length,
                mismatchDetected,
                sequenceBroken,
            });
        const payload: DataConfidence = {
            sourceId: this.resolveSourceId(topic, evt),
            topic,
            symbol: evt.symbol,
            ts: evt.ts,
            confidenceScore,
            freshSourcesCount,
            staleSourcesDropped: staleSourcesDropped.length ? staleSourcesDropped : undefined,
            mismatchDetected,
            meta: createMeta('global_data', { ts: evt.meta.ts }),
        };
        this.bus.publish('data:confidence', payload);
    }

    private logThrottled(key: string, ts: number, message: string): void {
        const last = this.lastLogByKey.get(key) ?? 0;
        if (ts - last < this.config.logThrottleMs) return;
        this.lastLogByKey.set(key, ts);
        logger.warn(m('warn', message));
    }

    private makeKey(topic: string, symbol: string, provider?: string): string {
        return `${topic}:${symbol}:${provider ?? 'global'}`;
    }

    private resolveSourceId(topic: string, evt: AggregatedEvent): string {
        return evt.provider ? `${evt.provider}:${topic}` : `global:${topic}`;
    }

    private markDegraded(key: string, sourceId: string, lastSuccessTs: number, ts: number, reason: 'stale' | 'mismatch'): void {
        const state =
            this.degradedByKey.get(key) ?? {
                sourceId,
                stale: false,
                mismatch: false,
                degraded: false,
            };
        state[reason] = true;
        state.lastErrorTs = ts;
        state.lastSuccessTs = lastSuccessTs;
        if (!state.degraded) {
            state.degraded = true;
            this.bus.publish('data:sourceDegraded', {
                sourceId,
                reason,
                lastSuccessTs: state.lastSuccessTs,
                meta: createMeta('global_data', { ts }),
            });
        }
        this.degradedByKey.set(key, state);
    }

    private clearDegradedFlag(key: string, reason: 'stale' | 'mismatch', ts: number): void {
        const state = this.degradedByKey.get(key);
        if (!state) return;
        state[reason] = false;
        if (!state.stale && !state.mismatch && state.degraded) {
            this.bus.publish('data:sourceRecovered', {
                sourceId: state.sourceId,
                recoveredTs: ts,
                lastErrorTs: state.lastErrorTs,
                meta: createMeta('global_data', { ts }),
            });
            this.degradedByKey.delete(key);
            return;
        }
        this.degradedByKey.set(key, state);
    }
}
