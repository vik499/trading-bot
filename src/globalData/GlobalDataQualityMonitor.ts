import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    asTsMs,
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
import { stableRecordFromEntries, stableSortStrings } from '../core/determinism';
import { makeDataQualityKey, makeDataQualitySourceId } from '../core/observability/dataQualityKeys';
import { resolveStalenessPolicy, type StalenessPolicyRule } from '../core/observability/stalenessPolicy';

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
    stalenessPolicy?: StalenessPolicyRule[];
    mismatchThresholdPct?: number;
    mismatchWindowMs?: number;
    logThrottleMs?: number;
    mismatchBaselineEpsilon?: number;
}

export interface GlobalDataQualitySnapshot {
    ts: number;
    degradedCount: number;
    mismatchCount: number;
    degraded?: Array<{
        key: string;
        sourceId: string;
        stale: boolean;
        mismatch: boolean;
        degraded: boolean;
        lastErrorTs?: number;
        lastSuccessTs?: number;
    }>;
    mismatch?: Array<{
        key: string;
        active: boolean;
        diffPct?: number;
        diffAbs?: number;
        baseline?: number;
        observed?: number;
        baselineEpsilon?: number;
        insufficientBaseline?: boolean;
        baselineVenue?: string;
        observedVenue?: string;
        venues?: string[];
        venueBreakdown?: Record<string, number>;
        bucket?: string;
        unit?: string;
        resetHint?: string;
    }>;
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

interface MismatchInfo {
    diffPct: number;
    diffAbs: number;
    baseline: number;
    observed: number;
    baselineEpsilon: number;
    insufficientBaseline: boolean;
    venueBreakdown?: Record<string, number>;
    baselineVenue?: string;
    observedVenue?: string;
    venues?: string[];
    bucket?: string;
    unit?: string;
    resetHint?: string;
}

export class GlobalDataQualityMonitor {
    private readonly bus: EventBus;
    private readonly config: Required<GlobalDataQualityConfig>;
    private started = false;
    private readonly unsubscribers: Array<() => void> = [];
    private readonly lastTsByKey = new Map<string, number>();
    private readonly firstIngestTsByKey = new Map<string, number>();
    private readonly staleSamplesByKey = new Map<string, number>();
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
            lastInfo?: MismatchInfo;
        }
    >();
    private readonly referencePriceBySymbol = new Map<string, { price: number; ts: number }>();
    private lastMetaTs?: number;

    constructor(bus: EventBus = defaultEventBus, config: GlobalDataQualityConfig = {}) {
        this.bus = bus;
        this.config = {
            expectedIntervalMs: config.expectedIntervalMs ?? {},
            staleMultiplier: Math.max(1, config.staleMultiplier ?? 2),
            stalenessPolicy: config.stalenessPolicy ?? [],
            mismatchThresholdPct: Math.max(0, config.mismatchThresholdPct ?? 0.1),
            mismatchWindowMs: Math.max(0, config.mismatchWindowMs ?? 60_000),
            logThrottleMs: Math.max(0, config.logThrottleMs ?? 30_000),
            mismatchBaselineEpsilon: Math.max(0, config.mismatchBaselineEpsilon ?? 1e-6),
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

    snapshot(maxItems = 25): GlobalDataQualitySnapshot {
        const ts = this.lastMetaTs ?? 0;
        const degradedEntries = Array.from(this.degradedByKey.entries()).map(([key, value]) => ({
            key,
            sourceId: value.sourceId,
            stale: value.stale,
            mismatch: value.mismatch,
            degraded: value.degraded,
            lastErrorTs: value.lastErrorTs,
            lastSuccessTs: value.lastSuccessTs,
        }));
        const mismatchEntries = Array.from(this.mismatchByKey.entries())
            .filter(([, value]) => value.active)
            .map(([key, value]) => ({
                key,
                active: value.active,
                diffPct: value.lastInfo?.diffPct,
                diffAbs: value.lastInfo?.diffAbs,
                baseline: value.lastInfo?.baseline,
                observed: value.lastInfo?.observed,
                baselineEpsilon: value.lastInfo?.baselineEpsilon,
                insufficientBaseline: value.lastInfo?.insufficientBaseline,
                baselineVenue: value.lastInfo?.baselineVenue,
                observedVenue: value.lastInfo?.observedVenue,
                venues: value.lastInfo?.venues?.slice(0, 10),
                venueBreakdown: value.lastInfo?.venueBreakdown ? this.capRecord(value.lastInfo.venueBreakdown, 10) : undefined,
                bucket: value.lastInfo?.bucket,
                unit: value.lastInfo?.unit,
                resetHint: value.lastInfo?.resetHint,
            }));

        return {
            ts,
            degradedCount: degradedEntries.length,
            mismatchCount: mismatchEntries.length,
            degraded: degradedEntries.slice(0, maxItems),
            mismatch: mismatchEntries.slice(0, maxItems),
        };
    }

    private onAggEvent(topic: string, evt: AggregatedEvent): void {
        this.lastMetaTs = evt.meta.tsEvent;
        this.detectStale(topic, evt);
        this.detectMismatch(topic, evt);
        this.emitConfidence(topic, evt);
    }

    private onPriceCanonical(evt: MarketPriceCanonicalEvent): void {
        this.lastMetaTs = evt.meta.tsEvent;
        const price = evt.indexPrice ?? evt.markPrice ?? evt.lastPrice;
        if (Number.isFinite(price)) {
            this.referencePriceBySymbol.set(evt.symbol, { price: price as number, ts: evt.meta.ts });
        }
        this.onAggEvent('market:price_canonical', evt);
    }

    private detectStale(topic: string, evt: AggregatedEvent): void {
        const key = makeDataQualityKey(topic, evt.symbol, evt.provider);
        const sourceId = makeDataQualitySourceId(topic, evt.provider);
        const policy = resolveStalenessPolicy(this.config.stalenessPolicy, {
            topic,
            symbol: evt.symbol,
            marketType: 'marketType' in evt ? evt.marketType : undefined,
        });

        const expected = policy?.expectedIntervalMs ?? this.config.expectedIntervalMs[topic];
        if (!expected) {
            this.lastTsByKey.set(key, evt.ts);
            return;
        }

        const staleThresholdMs = policy?.staleThresholdMs ?? expected * this.config.staleMultiplier;
        const ingestNow = evt.meta.tsIngest ?? evt.meta.tsEvent;
        if (!this.firstIngestTsByKey.has(key)) {
            this.firstIngestTsByKey.set(key, ingestNow);
        }

        const last = this.lastTsByKey.get(key);
        this.lastTsByKey.set(key, evt.ts);
        if (last === undefined) return;

        const sampleCount = (this.staleSamplesByKey.get(key) ?? 0) + 1;
        this.staleSamplesByKey.set(key, sampleCount);

        const delta = evt.ts - last;
        if (delta <= staleThresholdMs) {
            this.clearDegradedFlag(key, 'stale', evt.meta.ts);
            return;
        }

        // Startup/backfill protection: only for topics with explicit policy rules.
        if (policy) {
            const firstIngestTs = this.firstIngestTsByKey.get(key) ?? ingestNow;
            if (policy.startupGraceMs > 0 && ingestNow - firstIngestTs < policy.startupGraceMs) return;
            if (sampleCount < policy.minSamples) return;
        }

        const payload: DataStale = {
            sourceId,
            topic,
            symbol: evt.symbol,
            lastTs: last,
            currentTs: evt.ts,
            expectedMs: expected,
            meta: createMeta('global_data', { tsEvent: asTsMs(evt.meta.tsEvent) }),
        };
        this.bus.publish('data:stale', payload);
        this.logThrottled(
            `${key}:stale`,
            evt.meta.ts,
            `[GlobalData] stale key=${key} sourceId=${sourceId} topic=${topic} symbol=${evt.symbol} ` +
                `deltaMs=${delta} expectedIntervalMs=${expected} staleThresholdMs=${staleThresholdMs} ` +
                `lastSuccessTs=${last} lastSeenTs=${evt.ts}`
        );
        this.markDegraded(key, sourceId, last, evt.meta.ts, 'stale');
    }

    private detectMismatch(topic: string, evt: AggregatedEvent): void {
        const breakdown = hasVenueBreakdown(evt) ? evt.venueBreakdown : undefined;
        if (!breakdown) return;
        const values = Object.entries(breakdown).filter(
            (entry): entry is [string, number] => typeof entry[1] === 'number' && Number.isFinite(entry[1])
        );
        if (values.length < 2) return;
        let minVenue: string | undefined;
        let maxVenue: string | undefined;
        let min = Infinity;
        let max = -Infinity;
        for (const [venue, value] of values) {
            if (value < min) {
                min = value;
                minVenue = venue;
            }
            if (value > max) {
                max = value;
                maxVenue = venue;
            }
        }
        if (min <= 0) return;
        const diff = max - min;
        const baseline = min;
        const observed = max;
        const baselineEpsilon = this.config.mismatchBaselineEpsilon;
        const insufficientBaseline = Math.abs(baseline) < baselineEpsilon;
        const diffPct = insufficientBaseline ? diff / baselineEpsilon : diff / baseline;
        const mismatchDetected = insufficientBaseline
            ? diff >= baselineEpsilon
            : diffPct >= this.config.mismatchThresholdPct;
        const key = makeDataQualityKey(topic, evt.symbol, evt.provider);

        if (!mismatchDetected) {
            const state = this.mismatchByKey.get(key);
            if (state?.active) {
                this.clearDegradedFlag(key, 'mismatch', evt.meta.ts);
            }
            this.mismatchByKey.delete(key);
            return;
        }

        const next = this.mismatchByKey.get(key) ?? { firstTs: evt.ts, lastTs: evt.ts, active: false };
        next.lastTs = evt.ts;
        next.lastInfo = {
            diffPct,
            diffAbs: diff,
            baseline,
            observed,
            baselineEpsilon,
            insufficientBaseline,
            venueBreakdown: stableRecordFromEntries(values),
            baselineVenue: minVenue,
            observedVenue: maxVenue,
            venues: stableSortStrings(values.map(([venue]) => venue)),
            bucket: this.describeBucket(evt),
            unit: this.describeUnit(evt),
            resetHint: this.describeResetHint(evt),
        };
        this.mismatchByKey.set(key, next);

        if (!next.active && evt.ts - next.firstTs >= this.config.mismatchWindowMs) {
            const payload: DataMismatch = {
                symbol: evt.symbol,
                topic,
                ts: evt.ts,
                values: stableRecordFromEntries(values),
                thresholdPct: this.config.mismatchThresholdPct,
                meta: createMeta('global_data', { tsEvent: asTsMs(evt.meta.tsEvent) }),
            };
            this.bus.publish('data:mismatch', payload);
            const sourceId = makeDataQualitySourceId(topic, evt.provider);
            const venues = next.lastInfo?.venues ?? [];
            const venuesLabel = venues.length ? venues.slice(0, 8).join(',') + (venues.length > 8 ? ',…' : '') : 'n/a';
            const unitLabel = next.lastInfo?.unit ?? 'n/a';
            this.logThrottled(
                `${key}:mismatch`,
                evt.meta.ts,
                `[GlobalData] mismatch key=${key} sourceId=${sourceId} topic=${topic} symbol=${evt.symbol} ` +
                    `unit=${unitLabel} diffAbs=${diff.toFixed(8)} diffPct=${diffPct.toFixed(4)} ` +
                    `baseline=${baseline.toFixed(8)} observed=${observed.toFixed(8)} ` +
                    `baselineVenue=${minVenue ?? 'n/a'} observedVenue=${maxVenue ?? 'n/a'} venues=${venuesLabel}` +
                    (insufficientBaseline ? ` insufficientBaseline=true baselineEpsilon=${baselineEpsilon.toExponential(2)}` : '')
            );
            this.logMismatchDiagnostics(key, evt.meta.ts, evt.symbol, topic, next.lastInfo);
            next.active = true;
            this.mismatchByKey.set(key, next);
            this.markDegraded(key, sourceId, evt.ts, evt.meta.ts, 'mismatch');
        }
    }

    private logMismatchDiagnostics(
        key: string,
        ts: number,
        symbol: string,
        topic: string,
        info?: MismatchInfo
    ): void {
        if (!info) return;
        const breakdown = info.venueBreakdown ? JSON.stringify(this.capRecord(info.venueBreakdown, 10)) : 'n/a';
        const bucket = info.bucket ?? 'n/a';
        const unit = info.unit ?? 'n/a';
        const resetHint = info.resetHint ?? 'n/a';
        const baselineNote = info.insufficientBaseline
            ? `baseline<epsilon (${info.baseline.toFixed(8)}<${info.baselineEpsilon.toExponential(2)})`
            : `baseline=${info.baseline.toFixed(8)}`;
        const message =
            `[GlobalData] mismatch diagnostics topic=${topic} symbol=${symbol} diffPct=${info.diffPct.toFixed(4)} ` +
            `diffAbs=${info.diffAbs.toFixed(8)} ${baselineNote} unit=${unit} bucket=${bucket} reset=${resetHint} ` +
            `venueBreakdown=${breakdown}`;
        logger.debugThrottled(`${key}:mismatch:diag`, message, this.config.logThrottleMs);
    }

    private describeBucket(evt: AggregatedEvent): string | undefined {
        if (!('bucketStartTs' in evt) || !('bucketEndTs' in evt)) return undefined;
        const start = evt.bucketStartTs;
        const end = evt.bucketEndTs;
        if (typeof start !== 'number' || typeof end !== 'number') return undefined;
        const size = 'bucketSizeMs' in evt ? evt.bucketSizeMs : undefined;
        const sizeLabel = typeof size === 'number' ? `${size}ms` : 'n/a';
        return `${start}-${end} (${sizeLabel})`;
    }

    private describeUnit(evt: AggregatedEvent): string | undefined {
        if ('unit' in evt && typeof evt.unit === 'string') return evt.unit;
        if ('openInterestUnit' in evt && typeof evt.openInterestUnit === 'string') return evt.openInterestUnit;
        return undefined;
    }

    private describeResetHint(evt: AggregatedEvent): string | undefined {
        if (!hasVenueBreakdown(evt)) return undefined;
        if (!('cvdDelta' in evt)) return undefined;
        if (!('cvd' in evt)) return undefined;
        const total = typeof evt.cvd === 'number' ? evt.cvd : undefined;
        const delta = typeof evt.cvdDelta === 'number' ? evt.cvdDelta : undefined;
        if (total === undefined || delta === undefined) return undefined;
        return Math.abs(total) < Math.abs(delta) ? 'total<delta (possible reset)' : 'no';
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
            meta: createMeta('global_data', { tsEvent: asTsMs(evt.meta.tsEvent) }),
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
        return makeDataQualityKey(topic, symbol, provider);
    }

    private resolveSourceId(topic: string, evt: AggregatedEvent): string {
        return makeDataQualitySourceId(topic, evt.provider);
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
        // Make the key visible to observers before emitting the transition event.
        this.degradedByKey.set(key, state);
        if (!state.degraded) {
            state.degraded = true;
            this.bus.publish('data:sourceDegraded', {
                sourceId,
                reason,
                lastSuccessTs: state.lastSuccessTs,
                meta: createMeta('global_data', { tsEvent: asTsMs(ts) }),
            });
            this.degradedByKey.set(key, state);
        }
    }

    private capRecord<T>(record: Record<string, T>, maxKeys: number): Record<string, T> {
        const keys = stableSortStrings(Object.keys(record));
        const limited = keys.slice(0, Math.max(0, Math.floor(maxKeys)));
        const next: Record<string, T> = {};
        for (const key of limited) {
            next[key] = record[key] as T;
        }
        return next;
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
                meta: createMeta('global_data', { tsEvent: asTsMs(ts) }),
            });
            this.degradedByKey.delete(key);
            return;
        }
        this.degradedByKey.set(key, state);
    }
}
