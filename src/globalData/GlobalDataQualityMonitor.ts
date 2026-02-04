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
    type OpenInterestEvent,
    type OpenInterestUnit,
} from '../core/events/EventBus';
import { computeConfidenceScore } from '../core/confidence';
import { stableRecordFromEntries, stableSortStrings } from '../core/determinism';
import { makeDataQualityKey, makeDataQualitySourceId } from '../core/observability/dataQualityKeys';
import { resolveStalenessPolicy, type StalenessPolicyRule } from '../core/observability/stalenessPolicy';
import { normalizeMarketType, normalizeSymbol } from '../core/market/symbols';

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
    oiBaselineStrategy?: 'bybit' | 'median';
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
        suppressed?: boolean;
        suppressionReason?: string;
        baselineStrategy?: string;
        comparableVenues?: string[];
        excludedVenues?: string[];
        excludedReasons?: Record<string, string>;
        unitByVenue?: Record<string, string>;
        instrumentByVenue?: Record<string, string>;
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
    suppressed?: boolean;
    suppressionReason?: string;
    baselineStrategy?: string;
    comparableVenues?: string[];
    excludedVenues?: string[];
    excludedReasons?: Record<string, string>;
    unitByVenue?: Record<string, string>;
    instrumentByVenue?: Record<string, string>;
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
    private readonly oiRawByKey = new Map<
        string,
        Map<string, { ts: number; unit: OpenInterestUnit; value: number; valueUsd?: number }>
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
            oiBaselineStrategy: config.oiBaselineStrategy === 'median' ? 'median' : 'bybit',
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
        const onOiRaw = (evt: OpenInterestEvent) => this.onOiRaw(evt);

        this.bus.subscribe('market:oi_agg', onOi);
        this.bus.subscribe('market:funding_agg', onFunding);
        this.bus.subscribe('market:liquidations_agg', onLiquidations);
        this.bus.subscribe('market:liquidity_agg', onLiquidity);
        this.bus.subscribe('market:volume_agg', onVolume);
        this.bus.subscribe('market:cvd_spot_agg', onCvdSpot);
        this.bus.subscribe('market:cvd_futures_agg', onCvdFutures);
        this.bus.subscribe('market:price_index', onIndex);
        this.bus.subscribe('market:price_canonical', onCanonical);
        this.bus.subscribe('market:oi', onOiRaw);

        this.unsubscribers.push(
            () => this.bus.unsubscribe('market:oi_agg', onOi),
            () => this.bus.unsubscribe('market:funding_agg', onFunding),
            () => this.bus.unsubscribe('market:liquidations_agg', onLiquidations),
            () => this.bus.unsubscribe('market:liquidity_agg', onLiquidity),
            () => this.bus.unsubscribe('market:volume_agg', onVolume),
            () => this.bus.unsubscribe('market:cvd_spot_agg', onCvdSpot),
            () => this.bus.unsubscribe('market:cvd_futures_agg', onCvdFutures),
            () => this.bus.unsubscribe('market:price_index', onIndex),
            () => this.bus.unsubscribe('market:price_canonical', onCanonical),
            () => this.bus.unsubscribe('market:oi', onOiRaw)
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
            .filter(([, value]) => value.active || Boolean(value.lastInfo?.suppressed))
            .map(([key, value]) => ({
                key,
                active: value.active,
                suppressed: value.lastInfo?.suppressed,
                suppressionReason: value.lastInfo?.suppressionReason,
                baselineStrategy: value.lastInfo?.baselineStrategy,
                comparableVenues: value.lastInfo?.comparableVenues?.slice(0, 10),
                excludedVenues: value.lastInfo?.excludedVenues?.slice(0, 10),
                excludedReasons: value.lastInfo?.excludedReasons ? this.capRecord(value.lastInfo.excludedReasons, 10) : undefined,
                unitByVenue: value.lastInfo?.unitByVenue ? this.capRecord(value.lastInfo.unitByVenue, 10) : undefined,
                instrumentByVenue: value.lastInfo?.instrumentByVenue
                    ? this.capRecord(value.lastInfo.instrumentByVenue, 10)
                    : undefined,
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
            mismatchCount: mismatchEntries.filter((entry) => entry.active).length,
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

    private onOiRaw(evt: OpenInterestEvent): void {
        const symbol = normalizeSymbol(evt.symbol);
        const marketType = normalizeMarketType(evt.marketType);
        const key = `${symbol}:${marketType}`;
        const streamId = evt.streamId;
        const ts = evt.meta.tsEvent;

        const byStream = this.oiRawByKey.get(key) ?? new Map<string, { ts: number; unit: OpenInterestUnit; value: number; valueUsd?: number }>();
        const existing = byStream.get(streamId);
        if (existing && ts < existing.ts) return;

        const unit = evt.openInterestUnit ?? 'unknown';
        let valueUsd: number | undefined;

        if (typeof evt.openInterestValueUsd === 'number' && Number.isFinite(evt.openInterestValueUsd)) {
            valueUsd = evt.openInterestValueUsd;
        } else if (unit === 'usd') {
            valueUsd = evt.openInterest;
        } else if (unit === 'base' && this.hasReferencePrice(symbol, ts)) {
            const ref = this.referencePriceBySymbol.get(symbol);
            if (ref && Number.isFinite(ref.price)) {
                valueUsd = evt.openInterest * ref.price;
            }
        }

        byStream.set(streamId, { ts, unit, value: evt.openInterest, valueUsd });
        this.oiRawByKey.set(key, byStream);
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
        if (topic === 'market:oi_agg') {
            this.detectOiMismatch(evt as MarketOpenInterestAggEvent);
            return;
        }
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

    private detectOiMismatch(evt: MarketOpenInterestAggEvent): void {
        const topic = 'market:oi_agg';
        const key = makeDataQualityKey(topic, evt.symbol, evt.provider);

        const symbol = normalizeSymbol(evt.symbol);
        const marketType = normalizeMarketType(evt.marketType);
        const rawKey = `${symbol}:${marketType}`;
        const rawByStream = this.oiRawByKey.get(rawKey);
        const breakdown = hasVenueBreakdown(evt) ? evt.venueBreakdown : undefined;

        const candidateIds = new Set<string>();
        if (breakdown) {
            for (const streamId of Object.keys(breakdown)) candidateIds.add(streamId);
        }
        if (rawByStream) {
            for (const streamId of rawByStream.keys()) candidateIds.add(streamId);
        }
        const orderedCandidates = stableSortStrings(Array.from(candidateIds));
        if (orderedCandidates.length < 2) return;

        const freshnessWindowMs = this.resolveFreshnessWindowMs(topic, evt);
        const nowTs = evt.ts;
        const baselineStrategy = this.config.oiBaselineStrategy;
        const baselineEpsilon = this.config.mismatchBaselineEpsilon;

        const unitByVenue: Record<string, string> = {};
        const instrumentByVenue: Record<string, string> = {};
        const excludedReasons: Record<string, string> = {};

        const groupedByUnit = new Map<OpenInterestUnit, Array<[string, number]>>();
        const usdCandidates: Array<[string, number]> = [];
        const baseCandidates: Array<[string, number]> = [];

        for (const streamId of orderedCandidates) {
            const state = rawByStream?.get(streamId);
            let unit: OpenInterestUnit = state?.unit ?? 'unknown';
            // If we have no raw audit info for this stream, fall back to the aggregate unit.
            // OpenInterestAggregator emits venueBreakdown only for the dominant unit group, so this is safe for breakdown-only ids.
            if (unit === 'unknown' && evt.openInterestUnit) unit = evt.openInterestUnit;
            unitByVenue[streamId] = unit;
            instrumentByVenue[streamId] = this.describeOiInstrumentId(streamId, symbol);

            if (state && freshnessWindowMs !== undefined && nowTs - state.ts > freshnessWindowMs) {
                excludedReasons[streamId] = `STALE(>${freshnessWindowMs}ms)`;
                continue;
            }

            const value = state?.value ?? (breakdown ? breakdown[streamId] : undefined);
            if (typeof value !== 'number' || !Number.isFinite(value)) continue;
            const list = groupedByUnit.get(unit) ?? [];
            list.push([streamId, value]);
            groupedByUnit.set(unit, list);

            if (unit === 'base') {
                baseCandidates.push([streamId, value]);
            }

            let valueUsd = state?.valueUsd;
            if (valueUsd === undefined) {
                if (unit === 'usd') {
                    valueUsd = value;
                } else if (unit === 'base' && this.hasReferencePrice(symbol, evt.meta.tsEvent)) {
                    const ref = this.referencePriceBySymbol.get(symbol);
                    if (ref && Number.isFinite(ref.price)) {
                        valueUsd = value * ref.price;
                    }
                }
            }
            if (typeof valueUsd === 'number' && Number.isFinite(valueUsd)) {
                usdCandidates.push([streamId, valueUsd]);
            }
        }

        const compareUnit = usdCandidates.length >= 2 ? 'usd' : baseCandidates.length >= 2 ? 'base' : undefined;
        const comparable = compareUnit === 'usd' ? usdCandidates : compareUnit === 'base' ? baseCandidates : [];
        const comparableIds = stableSortStrings(comparable.map(([id]) => id));
        const excludedIds = stableSortStrings(orderedCandidates.filter((id) => !comparableIds.includes(id)));

        for (const id of excludedIds) {
            if (excludedReasons[id]) continue;
            const unit = unitByVenue[id] ?? 'unknown';
            if (unit === 'contracts') {
                excludedReasons[id] = 'NON_COMPARABLE(contracts:no_contract_size)';
                continue;
            }
            excludedReasons[id] = `NON_COMPARABLE(unit=${unit})`;
        }

        const suppressionReasons: string[] = [];
        if (!compareUnit) suppressionReasons.push('NO_COMPARABLE_UNIT');
        if (compareUnit && comparable.length < 2) suppressionReasons.push('INSUFFICIENT_COMPARABLE_SOURCES');

        const baseline = compareUnit ? this.computeBaseline(comparable, baselineStrategy) : undefined;
        if (baselineStrategy === 'bybit' && compareUnit && (!baseline || !this.isBybitStreamId(baseline.venue))) {
            suppressionReasons.push('BASELINE_BYBIT_MISSING');
        }

        const mismatchKey = key;
        const existing = this.mismatchByKey.get(mismatchKey);
        if (existing?.active && suppressionReasons.length) {
            this.clearDegradedFlag(mismatchKey, 'mismatch', evt.meta.ts);
        }

        if (suppressionReasons.length) {
            const dominant = this.selectDominantUnitGroup(groupedByUnit);
            const diagnosticValues = dominant?.values ?? [];
            const diagnosticUnit = dominant?.unit;
            const diagnosticBaseline = diagnosticValues.length ? this.computeBaseline(diagnosticValues, baselineStrategy) : undefined;
            const diagnosticObserved =
                diagnosticBaseline && diagnosticValues.length >= 2
                    ? this.computeObserved(diagnosticValues, diagnosticBaseline.value, diagnosticBaseline.venue)
                    : undefined;

            const diffAbs =
                diagnosticBaseline && diagnosticObserved
                    ? Math.abs(diagnosticObserved.value - diagnosticBaseline.value)
                    : 0;
            const insufficientBaseline =
                !diagnosticBaseline || Math.abs(diagnosticBaseline.value) < baselineEpsilon;
            const diffPct = diagnosticBaseline
                ? insufficientBaseline
                    ? diffAbs / baselineEpsilon
                    : diffAbs / Math.abs(diagnosticBaseline.value)
                : 0;

            const state = existing ?? { firstTs: evt.ts, lastTs: evt.ts, active: false };
            state.lastTs = evt.ts;
            state.active = false;
            state.lastInfo = {
                diffPct,
                diffAbs,
                baseline: diagnosticBaseline?.value ?? 0,
                observed: diagnosticObserved?.value ?? 0,
                baselineEpsilon,
                insufficientBaseline,
                suppressed: true,
                suppressionReason: suppressionReasons.join(','),
                baselineStrategy,
                comparableVenues: comparableIds,
                excludedVenues: excludedIds,
                excludedReasons,
                unitByVenue,
                instrumentByVenue,
                venueBreakdown: diagnosticValues.length ? stableRecordFromEntries(diagnosticValues) : undefined,
                baselineVenue: diagnosticBaseline?.venue,
                observedVenue: diagnosticObserved?.venue,
                venues: orderedCandidates,
                unit: diagnosticUnit ?? compareUnit ?? evt.openInterestUnit,
            };
            this.mismatchByKey.set(mismatchKey, state);
            this.logThrottled(
                `${mismatchKey}:mismatch:suppressed`,
                evt.meta.ts,
                `[GlobalData] oi comparability suppressed key=${mismatchKey} topic=${topic} symbol=${evt.symbol} ` +
                    `unit=${state.lastInfo.unit ?? 'n/a'} baseline=${state.lastInfo.baseline.toFixed(8)} ` +
                    `observed=${state.lastInfo.observed.toFixed(8)} diffPct=${state.lastInfo.diffPct.toFixed(4)} ` +
                    `baselineStrategy=${baselineStrategy} reason=${suppressionReasons.join(',')} ` +
                    `comparable=${comparableIds.join(',') || 'n/a'} excluded=${excludedIds.join(',') || 'n/a'}`
            );
            return;
        }

        if (!baseline) {
            this.mismatchByKey.delete(mismatchKey);
            return;
        }

        const observed = this.computeObserved(comparable, baseline.value, baseline.venue);
        if (!observed) {
            this.mismatchByKey.delete(mismatchKey);
            return;
        }

        const diffAbs = Math.abs(observed.value - baseline.value);
        const insufficientBaseline = Math.abs(baseline.value) < baselineEpsilon;
        const diffPct = insufficientBaseline ? diffAbs / baselineEpsilon : diffAbs / Math.abs(baseline.value);
        const mismatchDetected = insufficientBaseline
            ? diffAbs >= baselineEpsilon
            : diffPct >= this.config.mismatchThresholdPct;

        if (!mismatchDetected) {
            const state = this.mismatchByKey.get(mismatchKey);
            if (state?.active) {
                this.clearDegradedFlag(mismatchKey, 'mismatch', evt.meta.ts);
            }
            this.mismatchByKey.delete(mismatchKey);
            return;
        }

        const next =
            existing && !existing.lastInfo?.suppressed ? existing : { firstTs: evt.ts, lastTs: evt.ts, active: false };
        next.lastTs = evt.ts;
        next.lastInfo = {
            diffPct,
            diffAbs,
            baseline: baseline.value,
            observed: observed.value,
            baselineEpsilon,
            insufficientBaseline,
            baselineStrategy,
            comparableVenues: comparableIds,
            excludedVenues: excludedIds,
            excludedReasons,
            unitByVenue,
            instrumentByVenue,
            venueBreakdown: stableRecordFromEntries(comparable),
            baselineVenue: baseline.venue,
            observedVenue: observed.venue,
            venues: orderedCandidates,
            unit: compareUnit,
        };
        this.mismatchByKey.set(mismatchKey, next);

        if (!next.active && evt.ts - next.firstTs >= this.config.mismatchWindowMs) {
            const payload: DataMismatch = {
                symbol: evt.symbol,
                topic,
                ts: evt.ts,
                values: stableRecordFromEntries(comparable),
                thresholdPct: this.config.mismatchThresholdPct,
                meta: createMeta('global_data', { tsEvent: asTsMs(evt.meta.tsEvent) }),
            };
            this.bus.publish('data:mismatch', payload);
            const sourceId = makeDataQualitySourceId(topic, evt.provider);
            const venuesLabel = comparableIds.length
                ? comparableIds.slice(0, 8).join(',') + (comparableIds.length > 8 ? ',…' : '')
                : 'n/a';
            this.logThrottled(
                `${mismatchKey}:mismatch`,
                evt.meta.ts,
                `[GlobalData] mismatch key=${mismatchKey} sourceId=${sourceId} topic=${topic} symbol=${evt.symbol} ` +
                    `unit=${compareUnit} diffAbs=${diffAbs.toFixed(8)} diffPct=${diffPct.toFixed(4)} ` +
                    `baseline=${baseline.value.toFixed(8)} observed=${observed.value.toFixed(8)} ` +
                    `baselineVenue=${baseline.venue} observedVenue=${observed.venue} venues=${venuesLabel}`
            );
            next.active = true;
            this.mismatchByKey.set(mismatchKey, next);
            this.markDegraded(mismatchKey, sourceId, evt.ts, evt.meta.ts, 'mismatch');
        }
    }

    private resolveFreshnessWindowMs(topic: string, evt: AggregatedEvent): number | undefined {
        const policy = resolveStalenessPolicy(this.config.stalenessPolicy, {
            topic,
            symbol: evt.symbol,
            marketType: 'marketType' in evt ? evt.marketType : undefined,
        });
        const expected = policy?.expectedIntervalMs ?? this.config.expectedIntervalMs[topic];
        if (!expected) return undefined;
        const staleThresholdMs = policy?.staleThresholdMs ?? expected * this.config.staleMultiplier;
        return Math.max(expected, staleThresholdMs);
    }

    private computeBaseline(values: Array<[string, number]>, strategy: 'bybit' | 'median'): { venue: string; value: number } | undefined {
        const filtered = values.filter(([, value]) => Number.isFinite(value));
        if (filtered.length === 0) return undefined;

        if (strategy === 'bybit') {
            const bybit = filtered.filter(([venue]) => this.isBybitStreamId(venue)).sort(([a], [b]) => a.localeCompare(b));
            if (bybit.length === 0) return undefined;
            return { venue: bybit[0][0], value: bybit[0][1] };
        }

        const ordered = filtered
            .slice()
            .sort(([venueA, valueA], [venueB, valueB]) => (valueA !== valueB ? valueA - valueB : venueA.localeCompare(venueB)));
        const mid = Math.floor(ordered.length / 2);
        if (ordered.length % 2 === 1) {
            return { venue: ordered[mid][0], value: ordered[mid][1] };
        }
        const lo = ordered[mid - 1];
        const hi = ordered[mid];
        return { venue: 'median', value: (lo[1] + hi[1]) / 2 };
    }

    private computeObserved(
        values: Array<[string, number]>,
        baseline: number,
        baselineVenue?: string
    ): { venue: string; value: number } | undefined {
        let best: { venue: string; value: number; diff: number } | undefined;
        for (const [venue, value] of values) {
            if (!Number.isFinite(value)) continue;
            if (baselineVenue && baselineVenue !== 'median' && venue === baselineVenue) continue;
            const diff = Math.abs(value - baseline);
            if (!best || diff > best.diff || (diff === best.diff && venue.localeCompare(best.venue) < 0)) {
                best = { venue, value, diff };
            }
        }
        if (!best) return undefined;
        return { venue: best.venue, value: best.value };
    }

    private selectDominantUnitGroup(
        grouped: Map<OpenInterestUnit, Array<[string, number]>>
    ): { unit: OpenInterestUnit; values: Array<[string, number]> } | undefined {
        const entries = Array.from(grouped.entries()).filter(([, values]) => values.length > 0);
        if (entries.length === 0) return undefined;
        entries.sort(([unitA, valuesA], [unitB, valuesB]) => {
            if (valuesB.length !== valuesA.length) return valuesB.length - valuesA.length;
            return unitA.localeCompare(unitB);
        });
        const [unit, values] = entries[0];
        if (values.length < 2) return undefined;
        return { unit, values };
    }

    private isBybitStreamId(streamId: string): boolean {
        return this.resolveStreamVenue(streamId) === 'bybit';
    }

    private resolveStreamVenue(streamId: string): string {
        const prefix = streamId.split('.')[0]?.trim().toLowerCase();
        if (!prefix) return 'unknown';
        if (prefix === 'bybit' || prefix === 'binance' || prefix === 'okx') return prefix;
        return prefix;
    }

    private describeOiInstrumentId(streamId: string, symbol: string): string {
        const venue = this.resolveStreamVenue(streamId);
        if (venue === 'okx') {
            const split = this.splitSymbol(symbol);
            if (split) return `${split.base}-${split.quote}-SWAP`;
            return `${symbol}-SWAP`;
        }
        return symbol;
    }

    private splitSymbol(symbol: string): { base: string; quote: string } | undefined {
        const upper = normalizeSymbol(symbol);
        const quotes = ['USDT', 'USDC', 'USD'];
        for (const quote of quotes) {
            if (upper.endsWith(quote) && upper.length > quote.length) {
                return { base: upper.slice(0, -quote.length), quote };
            }
        }
        return undefined;
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
