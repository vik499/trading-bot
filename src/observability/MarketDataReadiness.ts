import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import { bucketCloseTs } from '../core/buckets';
import {
    asTsMs,
    createMeta,
    eventBus as defaultEventBus,
    nowMs,
    type DataConfidence,
    type DataGapDetected,
    type DataMismatch,
    type DataSequenceGapOrOutOfOrder,
    type DataTimeOutOfOrder,
    type EventBus,
    type MarketCvdAggEvent,
    type MarketDataStatusPayload,
    type MarketFundingAggEvent,
    type MarketLiquidationsAggEvent,
    type MarketLiquidityAggEvent,
    type MarketOpenInterestAggEvent,
    type MarketPriceCanonicalEvent,
    type OrderbookL2DeltaEvent,
    type OrderbookL2SnapshotEvent,
    type OpenInterestEvent,
    type FundingRateEvent,
    type TradeEvent,
    type TickerEvent,
    type KlineEvent,
    type MarketType,
    type ReadinessBlock,
    type MarketConnected,
    type MarketDisconnected,
} from '../core/events/EventBus';
import {
    SourceRegistry,
    sourceRegistry,
    type RawFeed,
    type SourceRegistrySnapshot,
} from '../core/market/SourceRegistry';
import { stableSortStrings } from '../core/determinism';
import { inferMarketTypeFromStreamId, normalizeMarketType, normalizeSymbol } from '../core/market/symbols';
import type { ExpectedSourcesConfig } from '../core/market/expectedSources';
import { resolveExpectedSources } from '../core/market/expectedSources';

export interface MarketDataReadinessOptions {
    bucketMs?: number;
    warmingWindowMs?: number;
    startupGraceWindowMs?: number;
    expectedSources?: number;
    expectedSourcesByBlock?: Partial<Record<ReadinessBlock, string[]>>;
    expectedSourcesConfig?: ExpectedSourcesConfig;
    expectedFlowTypes?: MarketType[];
    expectedDerivativeKinds?: Array<'oi' | 'funding' | 'liquidations'>;
    targetMarketType?: MarketType;
    sourceRegistry?: SourceRegistry;
    logIntervalMs?: number;
    wsRecoveryWindowMs?: number;
    noDataWindowMs?: number;
    outOfOrderToleranceMs?: number;
    confidenceStaleWindowMs?: number;
    derivativesStaleWindowMs?: number;
    readinessStabilityWindowMs?: number;
    lagWindowMs?: number;
    lagThresholdMs?: number;
    lagEwmaAlpha?: number;
    timebasePenaltyWindowMs?: number;
    thresholds?: {
        criticalBlock?: number;
        overall?: number;
    };
    weights?: {
        price?: number;
        flow?: number;
        liquidity?: number;
        derivatives?: number;
    };
    criticalBlocks?: ReadinessBlock[];
    now?: () => number;
    marketStatusJson?: boolean;
}

export interface MarketReadinessSnapshot {
    symbol: string;
    marketType: MarketType;
    status: 'READY' | 'WARMING' | 'DEGRADED';
    conf: number;
    warnings: string[];
    degradedReasons: string[];
    lagExP95?: number;
    lagExEwma?: number;
    lagPrP95?: number;
    lagPrEwma?: number;
}

type AggregatedWithMeta = {
    symbol: string;
    ts: number;
    meta: { tsEvent: number; tsIngest?: number };
    confidenceScore?: number;
    sourcesUsed?: string[];
    staleSourcesDropped?: string[];
    mismatchDetected?: boolean;
};

type Reason =
    | 'PRICE_STALE'
    | 'PRICE_LOW_CONF'
    | 'FLOW_LOW_CONF'
    | 'LIQUIDITY_LOW_CONF'
    | 'DERIVATIVES_LOW_CONF'
    | 'SOURCES_MISSING'
    | 'NO_DATA'
    | 'LAG_TOO_HIGH'
    | 'GAPS_DETECTED'
    | 'MISMATCH_DETECTED'
    | 'EXPECTED_SOURCES_MISSING_CONFIG'
    | 'NO_REF_PRICE'
    | 'WS_DISCONNECTED';

type WarningReason = 'TIMEBASE_QUALITY_WARN' | 'NON_MONOTONIC_TIMEBASE' | 'EXCHANGE_LAG_TOO_HIGH';

const DEFAULT_BUCKET_MS = 1_000;
const DEFAULT_WARMUP_MS = 30 * 60_000;
const DEFAULT_LOG_INTERVAL_MS = 10_000;
const DEFAULT_WS_RECOVERY_WINDOW_MS = 15_000;
const DEFAULT_STABILITY_WINDOW_MS = 10_000;
const DEFAULT_LAG_WINDOW_MS = 30_000;
const DEFAULT_LAG_THRESHOLD_MS = 2_000;
const DEFAULT_LAG_EWMA_ALPHA = 0.2;
const DEFAULT_TIMEBASE_PENALTY_WINDOW_MS = 10_000;
const EXCHANGE_LAG_MAX_EVENT_AGE_MS = 10 * 60 * 1000;
const CRITICAL_BLOCK_THRESHOLD = 0.55;
const OVERALL_THRESHOLD = 0.65;
const TIMEBASE_CONFIDENCE_PENALTY = 0.9;

const WEIGHTS: Record<ReadinessBlock, number> = {
    price: 0.35,
    flow: 0.3,
    liquidity: 0.2,
    derivatives: 0.15,
};

const REASON_ORDER: Reason[] = [
    'PRICE_STALE',
    'PRICE_LOW_CONF',
    'FLOW_LOW_CONF',
    'LIQUIDITY_LOW_CONF',
    'DERIVATIVES_LOW_CONF',
    'WS_DISCONNECTED',
    'SOURCES_MISSING',
    'EXPECTED_SOURCES_MISSING_CONFIG',
    'NO_DATA',
    'LAG_TOO_HIGH',
    'GAPS_DETECTED',
    'MISMATCH_DETECTED',
    'NO_REF_PRICE',
];

const WARNING_ORDER: WarningReason[] = ['TIMEBASE_QUALITY_WARN', 'NON_MONOTONIC_TIMEBASE', 'EXCHANGE_LAG_TOO_HIGH'];

function capArray<T>(items: T[], limit: number): T[] {
    if (items.length <= limit) return items;
    return items.slice(0, limit);
}

export class MarketDataReadiness {
    private readonly bus: EventBus;
    private readonly bucketMs: number;
    private readonly warmingWindowMs: number;
    private readonly expectedSources: number;
    private readonly expectedSourcesByBlock: Partial<Record<ReadinessBlock, string[]>>;
    private readonly expectedSourcesConfig?: ExpectedSourcesConfig;
    private readonly expectedFlowTypes: Set<MarketType>;
    private readonly expectedDerivativeKinds: Set<'oi' | 'funding' | 'liquidations'>;
    private readonly targetMarketType?: MarketType;
    private readonly registry: SourceRegistry;
    private readonly logIntervalMs: number;
    private readonly wsRecoveryWindowMs: number;
    private readonly noDataWindowMs: number;
    private readonly outOfOrderToleranceMs: number;
    private readonly confidenceStaleWindowMs: number;
    private readonly derivativesStaleWindowMs: number;
    private readonly readinessStabilityWindowMs: number;
    private readonly lagWindowMs: number;
    private readonly lagThresholdMs: number;
    private readonly lagEwmaAlpha: number;
    private readonly timebasePenaltyWindowMs: number;
    private readonly thresholds: { criticalBlock: number; overall: number };
    private readonly weights: Record<ReadinessBlock, number>;
    private readonly criticalBlocks: Set<ReadinessBlock>;
    private readonly startupGraceWindowMs: number;
    private readonly marketStatusJson: boolean;
    private readonly flowDebug: boolean;
    private readonly readinessDebug: boolean;
    private readonly gapDebug: boolean;
    private readonly now: () => number;

    private started = false;
    private readonly unsubscribers: Array<() => void> = [];

    private readonly lastByBlock = new Map<ReadinessBlock, AggregatedWithMeta>();
    private readonly sourcesByBlock = new Map<ReadinessBlock, Set<string>>();
    private readonly mismatchByBlock = new Map<ReadinessBlock, boolean>();

    private readonly gapByBlock = new Map<ReadinessBlock, boolean>();
    private readonly timebaseIssueByBlock = new Map<ReadinessBlock, number>();
    private readonly lastTimeOutOfOrderByBlock = new Map<ReadinessBlock, DataTimeOutOfOrder>();
    private readonly lastSequenceIssueByBlock = new Map<ReadinessBlock, DataSequenceGapOrOutOfOrder>();

    private readonly dataConfidenceByTopic = new Map<string, DataConfidence>();

    private readonly exchangeLagSamples: Array<{ ts: number; lagMs: number }> = [];
    private exchangeLagEwma?: number;
    private lastExchangeLagSampleTs?: number;
    private readonly processingLagSamples: Array<{ ts: number; lagMs: number }> = [];
    private processingLagEwma?: number;
    private lastProcessingLagSampleTs?: number;
    private lastProcessingLagIngestTs?: number;

    private readonly reasonSince = new Map<Reason, number>();
    private readonly warningSince = new Map<WarningReason, number>();

    private lastSymbol?: string;
    private lastMarketType?: MarketType;

    private firstBucketTs?: number;
    private lastBucketTs?: number;
    private lastStatus?: MarketDataStatusPayload;
    private lastSnapshot?: SourceRegistrySnapshot;
    private lastLogTs?: number;
    private lastLogSignature?: string;
    private readonly lastFlowDebugByKey = new Map<string, number>();
    private readonly lastNoRefPriceDebugByKey = new Map<string, number>();
    private readonly missingExpectedLogged = new Set<string>();

    private wsDegraded = false;
    private wsLastDisconnectTs?: number;
    private wsRecoveryStartTs?: number;
    private startedAtTs?: number;

    constructor(bus: EventBus = defaultEventBus, options: MarketDataReadinessOptions = {}) {
        this.bus = bus;
        this.bucketMs = Math.max(100, options.bucketMs ?? DEFAULT_BUCKET_MS);
        this.warmingWindowMs = Math.max(1_000, options.warmingWindowMs ?? DEFAULT_WARMUP_MS);
        this.expectedSources = Math.max(0, options.expectedSources ?? 0);
        this.expectedSourcesByBlock = options.expectedSourcesByBlock ?? {};
        this.expectedSourcesConfig = options.expectedSourcesConfig;
        this.expectedFlowTypes = new Set(options.expectedFlowTypes ?? []);
        this.expectedDerivativeKinds = new Set(options.expectedDerivativeKinds ?? []);
        this.targetMarketType = isTargetMarketType(options.targetMarketType)
            ? normalizeMarketType(options.targetMarketType)
            : undefined;
        this.registry = options.sourceRegistry ?? sourceRegistry;
        this.logIntervalMs = Math.max(0, options.logIntervalMs ?? DEFAULT_LOG_INTERVAL_MS);
        this.wsRecoveryWindowMs = Math.max(1_000, options.wsRecoveryWindowMs ?? DEFAULT_WS_RECOVERY_WINDOW_MS);
        this.noDataWindowMs = Math.max(0, options.noDataWindowMs ?? 0);
        this.outOfOrderToleranceMs = Math.max(0, options.outOfOrderToleranceMs ?? this.bucketMs);
        this.confidenceStaleWindowMs = Math.max(0, options.confidenceStaleWindowMs ?? this.bucketMs);
        this.derivativesStaleWindowMs = Math.max(0, options.derivativesStaleWindowMs ?? this.confidenceStaleWindowMs);
        const isTestEnv = process.env.NODE_ENV === 'test' || Boolean(process.env.VITEST);
        this.readinessStabilityWindowMs = Math.max(
            0,
            options.readinessStabilityWindowMs ?? (isTestEnv ? 0 : DEFAULT_STABILITY_WINDOW_MS)
        );
        this.lagWindowMs = Math.max(1_000, options.lagWindowMs ?? DEFAULT_LAG_WINDOW_MS);
        this.lagThresholdMs = Math.max(0, options.lagThresholdMs ?? DEFAULT_LAG_THRESHOLD_MS);
        this.lagEwmaAlpha = Math.min(1, Math.max(0.01, options.lagEwmaAlpha ?? DEFAULT_LAG_EWMA_ALPHA));
        this.timebasePenaltyWindowMs = Math.max(0, options.timebasePenaltyWindowMs ?? DEFAULT_TIMEBASE_PENALTY_WINDOW_MS);
        this.thresholds = {
            criticalBlock: Math.max(0, options.thresholds?.criticalBlock ?? CRITICAL_BLOCK_THRESHOLD),
            overall: Math.max(0, options.thresholds?.overall ?? OVERALL_THRESHOLD),
        };
        this.criticalBlocks = new Set(options.criticalBlocks ?? ['price', 'flow', 'liquidity']);
        const rawWeights = {
            price: options.weights?.price ?? WEIGHTS.price,
            flow: options.weights?.flow ?? WEIGHTS.flow,
            liquidity: options.weights?.liquidity ?? WEIGHTS.liquidity,
            derivatives: options.weights?.derivatives ?? WEIGHTS.derivatives,
        };
        this.weights = this.normalizeWeights(rawWeights, this.criticalBlocks);
        this.startupGraceWindowMs = Math.max(0, options.startupGraceWindowMs ?? 0);
        this.marketStatusJson = options.marketStatusJson ?? readFlag('MARKET_STATUS_JSON', false);
        this.flowDebug = readFlag('BOT_FLOW_DEBUG', false);
        this.readinessDebug = readFlag('BOT_READINESS_DEBUG', false);
        this.gapDebug = readFlag('BOT_GAP_DEBUG', false);
        this.now = options.now ?? nowMs;
    }

    start(): void {
        if (this.started) return;

        this.startedAtTs = undefined;

        const onPrice = (evt: MarketPriceCanonicalEvent) => this.onPrice(evt);
        const onCvdSpot = (evt: MarketCvdAggEvent) => this.onFlow('spot', evt);
        const onCvdFutures = (evt: MarketCvdAggEvent) => this.onFlow('futures', evt);
        const onLiquidity = (evt: MarketLiquidityAggEvent) => this.onLiquidity(evt);
        const onOi = (evt: MarketOpenInterestAggEvent) => this.onDerivatives('oi', evt);
        const onFunding = (evt: MarketFundingAggEvent) => this.onDerivatives('funding', evt);
        const onLiq = (evt: MarketLiquidationsAggEvent) => this.onDerivatives('liquidations', evt);
        const onConfidence = (evt: DataConfidence) => this.onConfidence(evt);
        const onMismatch = (evt: DataMismatch) => this.onMismatch(evt);
        const onGap = (evt: DataGapDetected) => this.onGap(evt);
        const onTimeOutOfOrder = (evt: DataTimeOutOfOrder) => this.onTimeOutOfOrder(evt);
        const onSequenceGap = (evt: DataSequenceGapOrOutOfOrder) => this.onSequenceGap(evt);
        const onTrade = (evt: TradeEvent) => this.onRaw('trades', evt.symbol, evt.marketType, evt.streamId, evt.meta);
        const onOrderbookSnapshot = (evt: OrderbookL2SnapshotEvent) =>
            this.onRaw('orderbook', evt.symbol, evt.marketType, evt.streamId, evt.meta);
        const onOrderbookDelta = (evt: OrderbookL2DeltaEvent) =>
            this.onRaw('orderbook', evt.symbol, evt.marketType, evt.streamId, evt.meta);
        const onOiRaw = (evt: OpenInterestEvent) => this.onRaw('oi', evt.symbol, evt.marketType, evt.streamId, evt.meta);
        const onFundingRaw = (evt: FundingRateEvent) => this.onRaw('funding', evt.symbol, evt.marketType, evt.streamId, evt.meta);
        const onTicker = (evt: TickerEvent) => this.onTickerRaw(evt);
        const onKline = (evt: KlineEvent) => this.onRaw('klines', evt.symbol, evt.marketType, evt.streamId, evt.meta);
        const onConnected = (evt: MarketConnected) => this.onWsConnected(evt);
        const onDisconnected = (evt: MarketDisconnected) => this.onWsDisconnected(evt);

        this.bus.subscribe('market:price_canonical', onPrice);
        this.bus.subscribe('market:cvd_spot_agg', onCvdSpot);
        this.bus.subscribe('market:cvd_futures_agg', onCvdFutures);
        this.bus.subscribe('market:liquidity_agg', onLiquidity);
        this.bus.subscribe('market:oi_agg', onOi);
        this.bus.subscribe('market:funding_agg', onFunding);
        this.bus.subscribe('market:liquidations_agg', onLiq);
        this.bus.subscribe('data:confidence', onConfidence);
        this.bus.subscribe('data:mismatch', onMismatch);
        this.bus.subscribe('data:gapDetected', onGap);
        this.bus.subscribe('data:time_out_of_order', onTimeOutOfOrder);
        this.bus.subscribe('data:sequence_gap_or_out_of_order', onSequenceGap);
        this.bus.subscribe('market:trade', onTrade);
        this.bus.subscribe('market:orderbook_l2_snapshot', onOrderbookSnapshot);
        this.bus.subscribe('market:orderbook_l2_delta', onOrderbookDelta);
        this.bus.subscribe('market:oi', onOiRaw);
        this.bus.subscribe('market:funding', onFundingRaw);
        this.bus.subscribe('market:ticker', onTicker);
        this.bus.subscribe('market:kline', onKline);
        this.bus.subscribe('market:connected', onConnected);
        this.bus.subscribe('market:disconnected', onDisconnected);

        this.unsubscribers.push(
            () => this.bus.unsubscribe('market:price_canonical', onPrice),
            () => this.bus.unsubscribe('market:cvd_spot_agg', onCvdSpot),
            () => this.bus.unsubscribe('market:cvd_futures_agg', onCvdFutures),
            () => this.bus.unsubscribe('market:liquidity_agg', onLiquidity),
            () => this.bus.unsubscribe('market:oi_agg', onOi),
            () => this.bus.unsubscribe('market:funding_agg', onFunding),
            () => this.bus.unsubscribe('market:liquidations_agg', onLiq),
            () => this.bus.unsubscribe('data:confidence', onConfidence),
            () => this.bus.unsubscribe('data:mismatch', onMismatch),
            () => this.bus.unsubscribe('data:gapDetected', onGap),
            () => this.bus.unsubscribe('data:time_out_of_order', onTimeOutOfOrder),
            () => this.bus.unsubscribe('data:sequence_gap_or_out_of_order', onSequenceGap),
            () => this.bus.unsubscribe('market:trade', onTrade),
            () => this.bus.unsubscribe('market:orderbook_l2_snapshot', onOrderbookSnapshot),
            () => this.bus.unsubscribe('market:orderbook_l2_delta', onOrderbookDelta),
            () => this.bus.unsubscribe('market:oi', onOiRaw),
            () => this.bus.unsubscribe('market:funding', onFundingRaw),
            () => this.bus.unsubscribe('market:ticker', onTicker),
            () => this.bus.unsubscribe('market:kline', onKline),
            () => this.bus.unsubscribe('market:connected', onConnected),
            () => this.bus.unsubscribe('market:disconnected', onDisconnected)
        );

        this.started = true;
    }

    stop(): void {
        if (!this.started) return;
        this.unsubscribers.forEach((fn) => fn());
        this.unsubscribers.length = 0;
        this.started = false;
    }

    getHealthSnapshot(): MarketReadinessSnapshot | undefined {
        const status = this.lastStatus;
        if (!status) return undefined;
        const symbol = this.lastSymbol ?? 'UNKNOWN';
        const marketType = this.lastMarketType ?? 'unknown';
        const readiness = status.warmingUp ? 'WARMING' : status.degraded ? 'DEGRADED' : 'READY';
        return {
            symbol,
            marketType,
            status: readiness,
            conf: status.overallConfidence,
            warnings: capArray(status.warnings ?? [], 20),
            degradedReasons: capArray(status.degradedReasons ?? [], 20),
            lagExP95: status.exchangeLagP95Ms,
            lagExEwma: status.exchangeLagEwmaMs,
            lagPrP95: status.processingLagP95Ms,
            lagPrEwma: status.processingLagEwmaMs,
        };
    }

    seedExpectedSources(input: { symbol: string; marketType: MarketType }): void {
        const normalizedSymbol = normalizeSymbol(input.symbol);
        const normalizedMarketType = normalizeMarketType(input.marketType);
        if (!this.shouldTrackMarketType(normalizedMarketType)) return;

        for (const block of ['price', 'flow', 'liquidity', 'derivatives'] as const) {
            this.registerExpectedSources(block, normalizedSymbol, normalizedMarketType);
        }

        this.lastSymbol = normalizedSymbol;
        this.lastMarketType = this.targetMarketType ?? normalizedMarketType;
    }

    private onPrice(evt: MarketPriceCanonicalEvent): void {
        this.recordBlock('price', evt, evt.sourcesUsed);
    }

    private onFlow(kind: 'spot' | 'futures', evt: MarketCvdAggEvent): void {
        const key = kind === 'spot' ? 'flow_spot' : 'flow_futures';
        this.dataConfidenceByTopic.set(key, this.toConfidence(evt, key));
        this.recordBlock('flow', evt, evt.sourcesUsed);
    }

    private onLiquidity(evt: MarketLiquidityAggEvent): void {
        this.recordBlock('liquidity', evt, evt.sourcesUsed);
    }

    private onDerivatives(kind: 'oi' | 'funding' | 'liquidations', evt: AggregatedWithMeta): void {
        const key = `derivatives_${kind}`;
        this.dataConfidenceByTopic.set(key, this.toConfidence(evt, key));
        this.recordBlock('derivatives', evt, evt.sourcesUsed);
    }

    private onConfidence(evt: DataConfidence): void {
        this.dataConfidenceByTopic.set(evt.topic, evt);
    }

    private onMismatch(evt: DataMismatch): void {
        const block = this.mapTopicToBlock(evt.topic);
        if (!block) return;
        this.mismatchByBlock.set(block, true);
    }

    private onGap(evt: DataGapDetected): void {
        const block = this.mapTopicToBlock(evt.topic);
        if (!block) return;
        this.gapByBlock.set(block, true);
        if (this.gapDebug) {
            this.logGapDebug('data:gapDetected', evt, block);
        }
    }

    private onSequenceGap(evt: DataSequenceGapOrOutOfOrder): void {
        const block = this.mapTopicToBlock(evt.topic);
        if (!block) return;
        this.gapByBlock.set(block, true);
        this.lastSequenceIssueByBlock.set(block, evt);
        if (this.gapDebug) {
            this.logGapDebug('data:sequence_gap_or_out_of_order', evt, block);
        }
    }

    private onTimeOutOfOrder(evt: DataTimeOutOfOrder): void {
        const block = this.mapTopicToBlock(evt.topic);
        if (!block) return;
        const delta = evt.prevTs - evt.currTs;
        if (delta <= this.outOfOrderToleranceMs) return;
        this.timebaseIssueByBlock.set(block, evt.meta.tsEvent ?? evt.currTs);
        this.lastTimeOutOfOrderByBlock.set(block, evt);
    }

    private onWsConnected(evt: MarketConnected): void {
        if (!this.wsDegraded) return;
        // Подключение само по себе не означает готовность — ждём стабильного потока данных.
        if (this.wsLastDisconnectTs === undefined) {
            this.wsLastDisconnectTs = evt.meta.tsEvent;
        }
    }

    private onWsDisconnected(evt: MarketDisconnected): void {
        this.wsDegraded = true;
        this.wsLastDisconnectTs = evt.meta.tsEvent;
        this.wsRecoveryStartTs = undefined;
    }

    private markWsFlow(ts: number): void {
        if (!this.wsDegraded) return;
        if (this.wsLastDisconnectTs !== undefined && ts < this.wsLastDisconnectTs) return;
        if (this.wsRecoveryStartTs === undefined) {
            this.wsRecoveryStartTs = ts;
            return;
        }
        if (ts - this.wsRecoveryStartTs >= this.wsRecoveryWindowMs) {
            this.wsDegraded = false;
            this.wsLastDisconnectTs = undefined;
            this.wsRecoveryStartTs = undefined;
        }
    }

    private onRaw(
        feed: RawFeed,
        symbol: string,
        marketType: MarketType | undefined,
        streamId: string | undefined,
        meta: { tsEvent: number; tsIngest?: number }
    ): void {
        const sourceId = streamId ?? 'unknown';
        const inferred = marketType ?? inferMarketTypeFromStreamId(streamId);
        const normalizedMarketType = normalizeMarketType(inferred);
        if (!this.shouldTrackMarketType(normalizedMarketType)) return;
        const normalizedSymbol = normalizeSymbol(symbol);
        this.registry.markRawSeen({ symbol: normalizedSymbol, marketType: normalizedMarketType, feed }, sourceId, meta.tsEvent);
        this.lastSymbol = normalizedSymbol;
        this.lastMarketType = normalizedMarketType;
        this.recordExchangeLagSample(meta.tsEvent, meta.tsIngest);
        this.recordProcessingLagSample(meta.tsIngest);
        this.markWsFlow(meta.tsIngest ?? meta.tsEvent);
    }

    private onTickerRaw(evt: TickerEvent): void {
        const normalizedMarketType = normalizeMarketType(evt.marketType);
        if (!this.shouldTrackMarketType(normalizedMarketType)) return;
        const normalizedSymbol = normalizeSymbol(evt.symbol);
        const sourceId = evt.streamId;
        this.recordExchangeLagSample(evt.meta.tsEvent, evt.meta.tsIngest);
        this.recordProcessingLagSample(evt.meta.tsIngest);
        if (evt.markPrice !== undefined) {
            this.registry.markRawSeen(
                { symbol: normalizedSymbol, marketType: normalizedMarketType, feed: 'markPrice' },
                sourceId,
                evt.meta.tsEvent
            );
        }
        if (evt.indexPrice !== undefined) {
            this.registry.markRawSeen(
                { symbol: normalizedSymbol, marketType: normalizedMarketType, feed: 'indexPrice' },
                sourceId,
                evt.meta.tsEvent
            );
        }
        this.lastSymbol = normalizedSymbol;
        this.lastMarketType = normalizedMarketType;
        this.markWsFlow(evt.meta.tsIngest ?? evt.meta.tsEvent);
    }

    private recordBlock(block: ReadinessBlock, evt: AggregatedWithMeta, sources?: string[]): void {
        if (block === 'price' && (!sources || sources.length === 0)) {
            const normalizedMarketType = normalizeMarketType((evt as { marketType?: MarketType }).marketType);
            const normalizedSymbol = normalizeSymbol(evt.symbol);
            sourceRegistry.recordSuppression(
                { symbol: normalizedSymbol, marketType: normalizedMarketType, metric: 'price' },
                'NO_CANONICAL_PRICE'
            );
            return;
        }
        this.lastByBlock.set(block, evt);
        if (sources?.length) {
            this.sourcesByBlock.set(block, new Set(sources));
        }
        if (evt.mismatchDetected !== undefined) {
            this.mismatchByBlock.set(block, evt.mismatchDetected ?? false);
        }

        const normalizedMarketType = normalizeMarketType((evt as { marketType?: MarketType }).marketType);
        if (!this.shouldTrackMarketType(normalizedMarketType)) return;
        const normalizedSymbol = normalizeSymbol(evt.symbol);
        this.registerExpectedSources(block, normalizedSymbol, normalizedMarketType);
        this.registry.markAggEmitted(
            { symbol: normalizedSymbol, marketType: normalizedMarketType, metric: block },
            sources ?? [],
            evt.meta.tsEvent
        );
        this.lastSymbol = normalizedSymbol;
        this.lastMarketType = normalizedMarketType;
        this.markWsFlow(evt.meta.tsIngest ?? evt.meta.tsEvent);

        const bucketTs = this.bucketEndTs(evt.meta.tsEvent);
        this.lastBucketTs = bucketTs;
        if (this.firstBucketTs === undefined) {
            this.firstBucketTs = bucketTs;
        }
        this.emitIfReady(bucketTs);
    }

    private registerExpectedSources(block: ReadinessBlock, symbol: string, marketType: MarketType): void {
        const expected =
            resolveExpectedSources(this.expectedSourcesConfig, symbol, marketType, block) ?? this.expectedSourcesByBlock[block];
        if (!expected || expected.length === 0) return;
        this.registry.registerExpected({ symbol, marketType, metric: block }, expected);
    }

    private emitIfReady(bucketTs: number): void {
        if (this.lastBucketTs !== bucketTs) return;
        if (this.startedAtTs === undefined) {
            this.startedAtTs = bucketTs;
        }
        const payload = this.buildStatus(bucketTs);
        this.lastStatus = payload;
        this.bus.publish('system:market_data_status', payload);
        this.maybeLog(payload);
        this.resetFlags();
    }

    private buildStatus(bucketTs: number): MarketDataStatusPayload {
        const snapshot = this.buildSnapshot(bucketTs);
        const blockConfidence = this.computeBlockConfidence(bucketTs, snapshot);
        const overallConfidence = this.computeOverallConfidence(blockConfidence);
        const derivedSources = this.deriveExpectedSources(snapshot);
        this.maybeLogMissingExpectedConfig(snapshot, derivedSources.missingConfig);
        const seenAggSources = stableSortStrings(this.unionSources(snapshot.usedAgg));
        const seenRawSources = stableSortStrings(this.unionSources(snapshot.usedRaw));
        const expectedAggSources = derivedSources.expectedAggSources;
        const expectedRawSources = derivedSources.expectedRawSources;
        const expectedAggSet = new Set(expectedAggSources);
        const expectedRawSet = new Set(expectedRawSources);
        const aggUsedSources = seenAggSources.filter((source) => expectedAggSet.has(source));
        const rawUsedSources = seenRawSources.filter((source) => expectedRawSet.has(source));
        const aggUnexpectedSources = seenAggSources.filter((source) => !expectedAggSet.has(source));
        const rawUnexpectedSources = seenRawSources.filter((source) => !expectedRawSet.has(source));
        const activeSourcesAgg = aggUsedSources.length;
        const activeSourcesRaw = rawUsedSources.length;
        const { degradedReasons, warnings, exchangeLagP95Ms, exchangeLagEwmaMs, processingLagP95Ms, processingLagEwmaMs } =
            this.computeDegradedReasons(
            blockConfidence,
            overallConfidence,
            activeSourcesAgg,
            derivedSources.expectedAggCount,
            derivedSources.missingConfig,
            snapshot,
            bucketTs
        );
        const degraded = degradedReasons.length > 0;
        const warmingWindowMs = this.warmingWindowMs;
        const startTs = this.firstBucketTs ?? bucketTs;
        const elapsed = Math.max(0, bucketTs - startTs);
        const warmingProgressRaw = warmingWindowMs > 0 ? elapsed / warmingWindowMs : 1;
        const warmingProgress = Math.max(0, Math.min(1, warmingProgressRaw));
        const warmingUp = warmingProgress < 1;

        return {
            overallConfidence,
            blockConfidence,
            degraded,
            degradedReasons,
            warnings,
            exchangeLagP95Ms,
            exchangeLagEwmaMs,
            processingLagP95Ms,
            processingLagEwmaMs,
            warmingUp,
            warmingProgress,
            warmingWindowMs,
            activeSources: activeSourcesAgg,
            expectedSources: derivedSources.expectedAggCount,
            activeSourcesAgg,
            activeSourcesRaw,
            expectedSourcesAgg: derivedSources.expectedAggCount,
            expectedSourcesRaw: derivedSources.expectedRawCount,
            aggSourcesUsed: aggUsedSources,
            aggSourcesSeen: seenAggSources,
            aggSourcesUnexpected: aggUnexpectedSources.length ? aggUnexpectedSources : undefined,
            rawSourcesUsed: rawUsedSources,
            rawSourcesSeen: seenRawSources,
            rawSourcesUnexpected: rawUnexpectedSources.length ? rawUnexpectedSources : undefined,
            lastBucketTs: bucketTs,
            meta: createMeta('system', { tsEvent: asTsMs(bucketTs), tsIngest: asTsMs(this.resolveStatusIngestTs(bucketTs)) }),
        };
    }

    private computeBlockConfidence(bucketTs: number, snapshot: SourceRegistrySnapshot): Record<ReadinessBlock, number> {
        const priceRaw = this.expectedBlockEmpty(snapshot, 'price') ? 1 : this.confidenceForBlock('price', bucketTs);
        const price = this.applyTimebasePenalty('price', priceRaw, bucketTs);
        const flowSpot = this.confidenceForTopic('flow_spot', bucketTs);
        const flowFutures = this.confidenceForTopic('flow_futures', bucketTs);
        const flowTypesUsed = this.resolveFlowTypesForMarketType(snapshot.marketType);
        const flowCandidates: number[] = [];
        if (flowTypesUsed.includes('spot')) flowCandidates.push(flowSpot);
        if (flowTypesUsed.includes('futures')) flowCandidates.push(flowFutures);
        const flowRaw = flowCandidates.length ? Math.min(...flowCandidates) : Math.min(flowSpot, flowFutures);
        const flowValue = this.expectedBlockEmpty(snapshot, 'flow') ? 1 : flowRaw;
        const flow = this.applyTimebasePenalty('flow', flowValue, bucketTs);
        if (this.flowDebug) {
            this.logFlowDebug(snapshot.symbol, snapshot.marketType, bucketTs, flowSpot, flowFutures, flowRaw, flow, flowTypesUsed);
        }
        const liquidityRaw = this.expectedBlockEmpty(snapshot, 'liquidity') ? 1 : this.confidenceForBlock('liquidity', bucketTs);
        const liquidity = this.applyTimebasePenalty('liquidity', liquidityRaw, bucketTs);
        const derivOi = this.confidenceForTopic('derivatives_oi', bucketTs);
        const derivFunding = this.confidenceForTopic('derivatives_funding', bucketTs);
        const derivLiq = this.confidenceForTopic('derivatives_liquidations', bucketTs);
        const derivativeCandidates: number[] = [];
        if (this.expectedDerivativeKinds.has('oi')) derivativeCandidates.push(derivOi);
        if (this.expectedDerivativeKinds.has('funding')) derivativeCandidates.push(derivFunding);
        if (this.expectedDerivativeKinds.has('liquidations')) derivativeCandidates.push(derivLiq);
        const derivativesRaw = derivativeCandidates.length ? Math.min(...derivativeCandidates) : Math.min(derivOi, derivFunding, derivLiq);
        const derivativesValue = this.expectedBlockEmpty(snapshot, 'derivatives') ? 1 : derivativesRaw;
        const derivatives = this.applyTimebasePenalty('derivatives', derivativesValue, bucketTs);
        return { price, flow, liquidity, derivatives };
    }

    private confidenceForBlock(block: ReadinessBlock, bucketTs: number): number {
        const evt = this.lastByBlock.get(block);
        if (!evt) return 0;
        if (bucketTs - evt.meta.tsEvent > this.confidenceStaleWindowMs) return 0;
        return this.confidenceValue(evt.confidenceScore);
    }

    private confidenceForTopic(key: string, bucketTs: number): number {
        const evt = this.dataConfidenceByTopic.get(key);
        if (!evt) return 0;
        const staleWindow = key.startsWith('derivatives_') ? this.derivativesStaleWindowMs : this.confidenceStaleWindowMs;
        if (bucketTs - evt.meta.tsEvent > staleWindow) return 0;
        return this.confidenceValue(evt.confidenceScore);
    }

    private computeOverallConfidence(blockConfidence: Record<ReadinessBlock, number>): number {
        const weighted =
            blockConfidence.price * this.weights.price +
            blockConfidence.flow * this.weights.flow +
            blockConfidence.liquidity * this.weights.liquidity +
            blockConfidence.derivatives * this.weights.derivatives;
        return this.clamp(weighted, 0, 1);
    }

    private computeDegradedReasons(
        blockConfidence: Record<ReadinessBlock, number>,
        overallConfidence: number,
        activeSourcesAgg: number,
        expectedSourcesAgg: number,
        missingConfig: boolean,
        snapshot: SourceRegistrySnapshot,
        bucketTs: number
    ): {
        degradedReasons: Reason[];
        warnings: WarningReason[];
        exchangeLagP95Ms?: number;
        exchangeLagEwmaMs?: number;
        processingLagP95Ms?: number;
        processingLagEwmaMs?: number;
    } {
        const reasons = new Set<Reason>();
        const warnings = new Set<WarningReason>();
        const withinStartupGrace =
            this.startupGraceWindowMs > 0 &&
            this.startedAtTs !== undefined &&
            bucketTs - this.startedAtTs < this.startupGraceWindowMs;
        const expectedPrice = !this.expectedBlockEmpty(snapshot, 'price');
        const expectedFlow = !this.expectedBlockEmpty(snapshot, 'flow');
        const expectedLiquidity = !this.expectedBlockEmpty(snapshot, 'liquidity');
        const expectedDerivatives = !this.expectedBlockEmpty(snapshot, 'derivatives');

        const priceEvt = this.lastByBlock.get('price');
        const priceBucketMatch = priceEvt ? this.bucketEndTs(priceEvt.meta.tsEvent) === bucketTs : false;
        if (!priceEvt || !priceBucketMatch) {
            if (!withinStartupGrace && this.isCriticalBlock('price')) {
                reasons.add('PRICE_STALE');
            }
        } else if (
            expectedPrice &&
            this.isCriticalBlock('price') &&
            this.confidenceValue(priceEvt.confidenceScore) < this.thresholds.criticalBlock
        ) {
            reasons.add('PRICE_LOW_CONF');
        }

        if (expectedFlow && this.isCriticalBlock('flow') && blockConfidence.flow < this.thresholds.criticalBlock) {
            reasons.add('FLOW_LOW_CONF');
        }
        if (expectedLiquidity && this.isCriticalBlock('liquidity') && blockConfidence.liquidity < this.thresholds.criticalBlock) {
            reasons.add('LIQUIDITY_LOW_CONF');
        }
        if (expectedDerivatives && this.isCriticalBlock('derivatives') && blockConfidence.derivatives < this.thresholds.criticalBlock) {
            reasons.add('DERIVATIVES_LOW_CONF');
        }

        if (expectedSourcesAgg > 0 && activeSourcesAgg < expectedSourcesAgg) {
            if (!withinStartupGrace) {
                reasons.add('SOURCES_MISSING');
            }
        }

        if (missingConfig && activeSourcesAgg > 0) {
            reasons.add('EXPECTED_SOURCES_MISSING_CONFIG');
        }

        if (overallConfidence < this.thresholds.overall) {
            const lowest = this.lowestBlock(blockConfidence);
            if (lowest === 'price' && expectedPrice && this.isCriticalBlock('price')) reasons.add('PRICE_LOW_CONF');
            if (lowest === 'flow' && expectedFlow && this.isCriticalBlock('flow')) reasons.add('FLOW_LOW_CONF');
            if (lowest === 'liquidity' && expectedLiquidity && this.isCriticalBlock('liquidity')) reasons.add('LIQUIDITY_LOW_CONF');
            if (lowest === 'derivatives' && expectedDerivatives && this.isCriticalBlock('derivatives')) reasons.add('DERIVATIVES_LOW_CONF');
        }

        if (this.wsDegraded) {
            if (!withinStartupGrace) {
                reasons.add('WS_DISCONNECTED');
            }
        }

        if (this.noDataWindowMs > 0) {
            const lastRawTs = this.maxLastRawTs(snapshot.lastSeenRawTs);
            if (lastRawTs !== null && bucketTs - lastRawTs > this.noDataWindowMs) {
                if (!withinStartupGrace) {
                    reasons.add('NO_DATA');
                }
            }
        }
        const lagNowTs = this.lastExchangeLagSampleTs ?? bucketTs;
        const exchangeLagStats = this.computeExchangeLagStats(lagNowTs);
        // INVARIANT: exchange/transport lag MUST NOT degrade readiness. Degrade only on processing lag.
        if (
            this.lagThresholdMs > 0 &&
            exchangeLagStats.p95 !== undefined &&
            exchangeLagStats.p95 > this.lagThresholdMs &&
            !withinStartupGrace
        ) {
            warnings.add('EXCHANGE_LAG_TOO_HIGH');
        }
        const processingNowTs = this.now();
        if (
            this.lastProcessingLagIngestTs !== undefined &&
            (this.lastProcessingLagSampleTs === undefined || processingNowTs > this.lastProcessingLagSampleTs)
        ) {
            this.recordProcessingLagSample(this.lastProcessingLagIngestTs);
        }
        const processingLagStats = this.computeProcessingLagStats(processingNowTs);
        if (
            this.lagThresholdMs > 0 &&
            processingLagStats.p95 !== undefined &&
            processingLagStats.p95 > this.lagThresholdMs &&
            !withinStartupGrace
        ) {
            reasons.add('LAG_TOO_HIGH');
        }
        if (Array.from(this.gapByBlock.values()).some(Boolean)) reasons.add('GAPS_DETECTED');

        const hasMismatch = Array.from(this.mismatchByBlock.values()).some(Boolean);
        const priceValid = priceBucketMatch && this.confidenceValue(priceEvt?.confidenceScore) >= this.thresholds.criticalBlock;
        if (hasMismatch) {
            if (priceValid) reasons.add('MISMATCH_DETECTED');
            else {
                reasons.add('NO_REF_PRICE');
                if (this.readinessDebug) {
                    this.logNoRefPriceDebug(bucketTs, snapshot, priceEvt, priceBucketMatch);
                }
            }
        }

        if (snapshot.nonMonotonicSources.length > 0) {
            warnings.add('NON_MONOTONIC_TIMEBASE');
        }

        const timebaseWarnActive = Array.from(this.timebaseIssueByBlock.entries()).some(
            ([, ts]) => bucketTs - ts <= this.timebasePenaltyWindowMs
        );
        if (timebaseWarnActive) warnings.add('TIMEBASE_QUALITY_WARN');

        const stableReasons = this.stabilizeReasons(reasons, bucketTs);
        const stableWarnings = this.stabilizeWarnings(warnings, bucketTs);

        return {
            degradedReasons: this.orderReasons(stableReasons),
            warnings: this.orderWarnings(stableWarnings),
            exchangeLagP95Ms: exchangeLagStats.p95,
            exchangeLagEwmaMs: exchangeLagStats.ewma,
            processingLagP95Ms: processingLagStats.p95,
            processingLagEwmaMs: processingLagStats.ewma,
        };
    }

    private buildSnapshot(bucketTs: number): SourceRegistrySnapshot {
        const symbol = this.lastSymbol ?? 'UNKNOWN';
        const marketType = this.targetMarketType ?? this.lastMarketType ?? 'unknown';
        const snapshot = this.registry.snapshot(bucketTs, symbol, marketType);
        this.lastSnapshot = snapshot;
        return snapshot;
    }

    private expectedBlockEmpty(snapshot: SourceRegistrySnapshot, block: ReadinessBlock): boolean {
        return snapshot.expected[block].length === 0;
    }

    private shouldTrackMarketType(marketType: MarketType): boolean {
        if (!this.targetMarketType) return true;
        return marketType === this.targetMarketType;
    }
    private maybeLogMissingExpectedConfig(snapshot: SourceRegistrySnapshot, missingConfig: boolean): void {
        if (!missingConfig) return;
        const key = `${snapshot.symbol}:${snapshot.marketType}`;
        if (this.missingExpectedLogged.has(key)) return;
        this.missingExpectedLogged.add(key);
        logger.warn(
            m(
                'warn',
                `[MarketDataReadiness] expected sources config missing for symbol=${snapshot.symbol} marketType=${snapshot.marketType}`
            )
        );
    }

    private deriveExpectedSources(snapshot: SourceRegistrySnapshot): {
        expectedAggSources: string[];
        expectedRawSources: string[];
        expectedAggCount: number;
        expectedRawCount: number;
        missingConfig: boolean;
    } {
        const expectedAggSources = this.unionSources(snapshot.expected);
        const usedAggSources = this.unionSources(snapshot.usedAgg);
        const usedRawSources = this.unionSources(snapshot.usedRaw);
        if (expectedAggSources.length === 0) {
            if (this.expectedSources > 0) {
                const orderedAgg = stableSortStrings(usedAggSources);
                const orderedRaw = stableSortStrings(usedRawSources);
                return {
                    expectedAggSources: orderedAgg.slice(0, this.expectedSources),
                    expectedRawSources: orderedRaw.slice(0, this.expectedSources),
                    expectedAggCount: this.expectedSources,
                    expectedRawCount: this.expectedSources,
                    missingConfig: false,
                };
            }
            if (usedAggSources.length > 0 || usedRawSources.length > 0) {
                const fallbackAgg = usedAggSources.length ? usedAggSources : usedRawSources;
                const fallbackRaw = usedRawSources.length ? usedRawSources : usedAggSources;
                return {
                    expectedAggSources: stableSortStrings(fallbackAgg),
                    expectedRawSources: stableSortStrings(fallbackRaw),
                    expectedAggCount: fallbackAgg.length,
                    expectedRawCount: fallbackRaw.length,
                    missingConfig: true,
                };
            }
        }
        const expectedCount = expectedAggSources.length || this.expectedSources;
        const orderedExpected = stableSortStrings(expectedAggSources);
        return {
            expectedAggSources: orderedExpected,
            expectedRawSources: orderedExpected,
            expectedAggCount: expectedCount,
            expectedRawCount: expectedCount,
            missingConfig: false,
        };
    }

    private countSources(record: Record<string, string[]>): number {
        return this.unionSources(record).length;
    }

    private unionSources(record: Record<string, string[]>): string[] {
        const union = new Set<string>();
        for (const sources of Object.values(record)) {
            for (const source of sources) {
                union.add(source);
            }
        }
        return Array.from(union);
    }

    private maxLastRawTs(record: Record<string, number | null>): number | null {
        let max: number | null = null;
        for (const value of Object.values(record)) {
            if (value === null || value === undefined) continue;
            if (max === null || value > max) max = value;
        }
        return max;
    }

    private orderReasons(reasons: Set<Reason>): Reason[] {
        return REASON_ORDER.filter((r) => reasons.has(r));
    }

    private orderWarnings(warnings: Set<WarningReason>): WarningReason[] {
        return WARNING_ORDER.filter((w) => warnings.has(w));
    }

    private stabilizeReasons(reasons: Set<Reason>, ts: number): Set<Reason> {
        if (this.readinessStabilityWindowMs === 0) return reasons;
        const stable = new Set<Reason>();
        const exempt = new Set<Reason>(['EXPECTED_SOURCES_MISSING_CONFIG']);
        for (const reason of reasons) {
            if (exempt.has(reason)) {
                stable.add(reason);
                continue;
            }
            const since = this.reasonSince.get(reason);
            if (since === undefined) {
                this.reasonSince.set(reason, ts);
                continue;
            }
            if (ts - since >= this.readinessStabilityWindowMs) {
                stable.add(reason);
            }
        }
        for (const reason of Array.from(this.reasonSince.keys())) {
            if (!reasons.has(reason)) this.reasonSince.delete(reason);
        }
        return stable;
    }

    private stabilizeWarnings(warnings: Set<WarningReason>, ts: number): Set<WarningReason> {
        if (this.readinessStabilityWindowMs === 0) return warnings;
        const stable = new Set<WarningReason>();
        for (const warning of warnings) {
            const since = this.warningSince.get(warning);
            if (since === undefined) {
                this.warningSince.set(warning, ts);
                continue;
            }
            if (ts - since >= this.readinessStabilityWindowMs) {
                stable.add(warning);
            }
        }
        for (const warning of Array.from(this.warningSince.keys())) {
            if (!warnings.has(warning)) this.warningSince.delete(warning);
        }
        return stable;
    }

    private maybeLog(payload: MarketDataStatusPayload): void {
        const last = this.lastStatus;
        const ts = payload.lastBucketTs;
        const shouldLog = this.shouldLog(ts, last, payload);
        if (!shouldLog) return;

        const logSignature = this.buildLogSignature(payload);
        if (logSignature === this.lastLogSignature) return;
        this.lastLogSignature = logSignature;

        this.lastLogTs = ts;

        if (payload.degraded && !last?.degraded) {
            this.logDegradedTransition(payload);
        }

        const summaryLine = this.buildSummaryLine(payload);
        if (this.marketStatusJson) {
            const snapshotLog = this.buildSnapshotLog(payload);
            logger.debug(JSON.stringify(snapshotLog));
        }
        logger.info(summaryLine);
    }

    private buildSummaryLine(payload: MarketDataStatusPayload): string {
        const symbol = this.lastSymbol ?? '';
        const marketType = this.lastMarketType;
        const shouldShowMarketType = marketType !== undefined && marketType !== 'unknown';
        const hasContext = symbol.length > 0 && shouldShowMarketType;
        const contextPrefix = hasContext ? `symbol=${symbol} marketType=${marketType}` : '';
        const formatLag = (value?: number): string =>
            value === undefined || !Number.isFinite(value) ? 'n/a' : Math.round(value).toString();
        const lagExP95 = `lagExP95=${formatLag(payload.exchangeLagP95Ms)}`;
        const lagExEwma = `lagExEwma=${formatLag(payload.exchangeLagEwmaMs)}`;
        const lagPrP95 = `lagPrP95=${formatLag(payload.processingLagP95Ms)}`;
        const lagPrEwma = `lagPrEwma=${formatLag(payload.processingLagEwmaMs)}`;
        const status = payload.warmingUp
            ? 'WARMING'
            : payload.degraded
              ? 'DEGRADED'
              : 'READY';
        const aggUsed = payload.aggSourcesUsed?.length ?? payload.activeSourcesAgg;
        const aggSeen = payload.aggSourcesSeen?.length ?? payload.activeSourcesAgg;
        const rawUsed = payload.rawSourcesUsed?.length ?? payload.activeSourcesRaw;
        const rawSeen = payload.rawSourcesSeen?.length ?? payload.activeSourcesRaw;
        const aggUnexpected = payload.aggSourcesUnexpected?.join(',') ?? '';
        const rawUnexpected = payload.rawSourcesUnexpected?.join(',') ?? '';
        const reasons = payload.degradedReasons.length ? payload.degradedReasons.join(',') : '';
        const warnings = payload.warnings?.length ? payload.warnings.join(',') : '';
        return [
            contextPrefix,
            `MarketData status=${status}`,
            `conf=${payload.overallConfidence.toFixed(2)}`,
            `agg=${aggUsed}/${payload.expectedSourcesAgg}`,
            `raw=${rawUsed}/${payload.expectedSourcesRaw}`,
            `rawSeen=${rawSeen}`,
            `price=${payload.blockConfidence.price.toFixed(2)}`,
            `flow=${payload.blockConfidence.flow.toFixed(2)}`,
            `liq=${payload.blockConfidence.liquidity.toFixed(2)}`,
            `deriv=${payload.blockConfidence.derivatives.toFixed(2)}`,
            `warm=${payload.warmingUp ? payload.warmingProgress.toFixed(2) : 'NO'}`,
            `degraded=${payload.degraded ? 'YES' : 'NO'}`,
            aggUnexpected ? `unexpectedAgg=${aggUnexpected}` : '',
            rawUnexpected ? `unexpectedRaw=${rawUnexpected}` : '',
            reasons ? `reasons=${reasons}` : '',
            warnings ? `warnings=${warnings}` : '',
            lagExP95,
            lagExEwma,
            lagPrP95,
            lagPrEwma,
        ]
            .filter((part) => part.length > 0)
            .join(' ');
    }

    private buildLogSignature(payload: MarketDataStatusPayload): string {
        const symbol = this.lastSymbol ?? '';
        const marketType = this.lastMarketType;
        const shouldShowMarketType = marketType !== undefined && marketType !== 'unknown';
        const hasContext = symbol.length > 0 && shouldShowMarketType;
        const symbolTag = hasContext ? symbol : '';
        const marketTypeTag = hasContext ? marketType ?? '' : '';
        const aggUsed = payload.aggSourcesUsed?.length ?? payload.activeSourcesAgg;
        const aggSeen = payload.aggSourcesSeen?.length ?? payload.activeSourcesAgg;
        const rawUsed = payload.rawSourcesUsed?.length ?? payload.activeSourcesRaw;
        const rawSeen = payload.rawSourcesSeen?.length ?? payload.activeSourcesRaw;
        const aggUnexpected = payload.aggSourcesUnexpected ?? [];
        const rawUnexpected = payload.rawSourcesUnexpected ?? [];
        return [
            symbolTag,
            marketTypeTag,
            payload.warmingUp ? 'WARMING' : payload.degraded ? 'DEGRADED' : 'READY',
            payload.overallConfidence.toFixed(2),
            payload.blockConfidence.price.toFixed(2),
            payload.blockConfidence.flow.toFixed(2),
            payload.blockConfidence.liquidity.toFixed(2),
            payload.blockConfidence.derivatives.toFixed(2),
            `${aggUsed}/${payload.expectedSourcesAgg}`,
            `${rawUsed}/${payload.expectedSourcesRaw}`,
            `${rawSeen}`,
            `${aggSeen}`,
            payload.warmingUp ? payload.warmingProgress.toFixed(2) : 'NO',
            payload.degraded ? 'YES' : 'NO',
            payload.degradedReasons.join(','),
            (payload.warnings ?? []).join(','),
            aggUnexpected.join(','),
            rawUnexpected.join(','),
        ].join('|');
    }

    private logDegradedTransition(payload: MarketDataStatusPayload): void {
        const snapshot = this.lastSnapshot ?? this.buildSnapshot(payload.lastBucketTs);
        const lowest = this.lowestBlock(payload.blockConfidence);
        const suppressions = snapshot.suppressions[lowest];
        const suppressionReasons = suppressions.count
            ? Object.entries(suppressions.reasons)
                  .map(([reason, count]) => `${reason}:${count}`)
                  .join(',')
            : 'none';
        const topReasons = payload.degradedReasons.slice(0, 3);
        const evidence = topReasons.map((reason) => this.buildReasonEvidence(reason as Reason, payload, snapshot));
        logger.warn(
            m(
                'warn',
                `[MarketDataReadiness] degraded block=${lowest} conf=${payload.blockConfidence[lowest].toFixed(2)} overall=${payload.overallConfidence.toFixed(2)} reasons=${payload.degradedReasons.join(',') || 'n/a'} suppressions=${suppressionReasons}`
            )
        );
        logger.warn(
            JSON.stringify({
                type: 'market_data_degraded',
                ts: payload.lastBucketTs,
                symbol: snapshot.symbol,
                marketType: snapshot.marketType,
                overallConfidence: payload.overallConfidence,
                blockConfidence: payload.blockConfidence,
                reasons: payload.degradedReasons,
                topReasons,
                evidence,
            })
        );
    }

    private buildReasonEvidence(reason: Reason, payload: MarketDataStatusPayload, snapshot: SourceRegistrySnapshot): Record<string, unknown> {
        const block = this.reasonToBlock(reason);
        const blockConfidence = block ? payload.blockConfidence[block] : undefined;
        const lastAgg = block ? this.lastByBlock.get(block) : undefined;
        const sourcesUsed = lastAgg?.sourcesUsed;
        const staleDropped = lastAgg?.staleSourcesDropped;
        const confidenceExplain = (lastAgg as { confidenceExplain?: unknown })?.confidenceExplain;
        const venueStatus = (lastAgg as { venueStatus?: unknown })?.venueStatus;
        const sequenceBrokenSources = venueStatus
            ? Object.entries(venueStatus as Record<string, { sequenceBroken?: boolean }>)
                  .filter(([, status]) => status.sequenceBroken)
                  .map(([streamId]) => streamId)
                  .sort()
            : undefined;
        const sequenceBrokenUsedSources = sequenceBrokenSources && sourcesUsed?.length
            ? sourcesUsed.filter((source) => sequenceBrokenSources.includes(source)).sort()
            : undefined;
        const sequenceIssue = block ? this.lastSequenceIssueByBlock.get(block) : undefined;
        const timeIssue = block ? this.lastTimeOutOfOrderByBlock.get(block) : undefined;
        const expectedAgg = block ? snapshot.expected[block] : undefined;
        const usedAgg = block ? snapshot.usedAgg[block] : undefined;
        const exchangeLagNowTs = this.lastExchangeLagSampleTs ?? payload.lastBucketTs;
        const exchangeLagStats = this.computeExchangeLagStats(exchangeLagNowTs);
        const lagStats =
            reason === 'LAG_TOO_HIGH'
                ? this.computeProcessingLagStats(this.lastProcessingLagSampleTs ?? this.now())
                : exchangeLagStats;
        return {
            reason,
            block,
            confidence: blockConfidence,
            threshold: block ? this.thresholds.criticalBlock : undefined,
            overallThreshold: this.thresholds.overall,
            sourcesUsed,
            staleDropped,
            expectedSources: expectedAgg,
            usedAggSources: usedAgg,
            lastAggTs: block ? snapshot.lastSeenAggTs[this.blockToAggKey(block)] : undefined,
            lastRawTs: this.maxLastRawTs(snapshot.lastSeenRawTs),
            noDataWindowMs: this.noDataWindowMs,
            lagP95Ms: lagStats.p95,
            lagEwmaMs: lagStats.ewma,
            timeOutOfOrder: timeIssue
                ? {
                      topic: timeIssue.topic,
                      prevTs: timeIssue.prevTs,
                      currTs: timeIssue.currTs,
                      deltaMs: timeIssue.prevTs - timeIssue.currTs,
                      tsSource: timeIssue.tsSource,
                  }
                : undefined,
            sequenceIssue: sequenceIssue
                ? {
                      topic: sequenceIssue.topic,
                      prevSeq: sequenceIssue.prevSeq,
                      currSeq: sequenceIssue.currSeq,
                      kind: sequenceIssue.kind,
                  }
                : undefined,
            confidenceExplain,
            venueStatus,
            sequenceBrokenSources,
            sequenceBrokenUsedSources,
            nonMonotonicSources: snapshot.nonMonotonicSources,
        };
    }

    private reasonToBlock(reason: Reason): ReadinessBlock | undefined {
        if (reason.startsWith('PRICE')) return 'price';
        if (reason.startsWith('FLOW')) return 'flow';
        if (reason.startsWith('LIQUIDITY')) return 'liquidity';
        if (reason.startsWith('DERIVATIVES')) return 'derivatives';
        if (reason === 'GAPS_DETECTED' || reason === 'LAG_TOO_HIGH') return undefined;
        if (reason === 'SOURCES_MISSING') return undefined;
        return undefined;
    }

    private blockToAggKey(block: ReadinessBlock): keyof SourceRegistrySnapshot['lastSeenAggTs'] {
        if (block === 'price') return 'priceCanonical';
        if (block === 'flow') return 'flowAgg';
        if (block === 'liquidity') return 'liquidityAgg';
        return 'derivativesAgg';
    }

    private buildSnapshotLog(payload: MarketDataStatusPayload): Record<string, unknown> {
        const snapshot = this.lastSnapshot ?? this.buildSnapshot(payload.lastBucketTs);
        return {
            tsMeta: payload.meta.tsEvent,
            symbol: snapshot.symbol,
            marketType: snapshot.marketType,
            expected: snapshot.expected,
            usedAgg: snapshot.usedAgg,
            usedRaw: snapshot.usedRaw,
            lastSeenRawTs: snapshot.lastSeenRawTs,
            lastSeenAggTs: snapshot.lastSeenAggTs,
            suppressions: snapshot.suppressions,
            readiness: {
                status: payload.warmingUp ? 'WARMING' : payload.degraded ? 'DEGRADED' : 'READY',
                confidenceTotal: payload.overallConfidence,
                blocks: payload.blockConfidence,
                reasons: payload.degradedReasons,
                warnings: payload.warnings,
                aggSourcesUsed: payload.aggSourcesUsed,
                aggSourcesSeen: payload.aggSourcesSeen,
                aggSourcesUnexpected: payload.aggSourcesUnexpected,
                rawSourcesUsed: payload.rawSourcesUsed,
                rawSourcesSeen: payload.rawSourcesSeen,
                rawSourcesUnexpected: payload.rawSourcesUnexpected,
            },
        };
    }

    private shouldLog(ts: number, last: MarketDataStatusPayload | undefined, next: MarketDataStatusPayload): boolean {
        if (!last) return true;
        if (this.buildLogSignature(last) !== this.buildLogSignature(next)) return true;
        const lastLogTs = this.lastLogTs ?? 0;
        if (ts - lastLogTs >= this.logIntervalMs) return true;
        return false;
    }

    private resetFlags(): void {
        this.gapByBlock.clear();
        this.mismatchByBlock.clear();
    }

    private recordExchangeLagSample(tsEvent: number, tsIngest?: number): void {
        if (!Number.isFinite(tsEvent)) return;
        if (typeof tsIngest !== 'number' || !Number.isFinite(tsIngest)) return;
        if (tsIngest < tsEvent) return;
        const nowTs = this.now();
        if (!Number.isFinite(nowTs)) return;
        if (tsEvent < nowTs - EXCHANGE_LAG_MAX_EVENT_AGE_MS) return;
        const lagMs = Math.max(0, tsIngest - tsEvent);
        this.lastExchangeLagSampleTs = tsIngest;
        this.exchangeLagSamples.push({ ts: tsIngest, lagMs });
        if (this.exchangeLagEwma === undefined) {
            this.exchangeLagEwma = lagMs;
        } else {
            const alpha = this.lagEwmaAlpha;
            this.exchangeLagEwma = (1 - alpha) * this.exchangeLagEwma + alpha * lagMs;
        }
        this.pruneExchangeLagSamples(tsIngest);
    }

    private recordProcessingLagSample(tsIngest?: number): void {
        if (typeof tsIngest !== 'number' || !Number.isFinite(tsIngest)) return;
        const nowTs = this.now();
        if (!Number.isFinite(nowTs)) return;
        const lagMs = Math.max(0, nowTs - tsIngest);
        this.lastProcessingLagIngestTs = tsIngest;
        this.lastProcessingLagSampleTs = nowTs;
        this.processingLagSamples.push({ ts: nowTs, lagMs });
        if (this.processingLagEwma === undefined) {
            this.processingLagEwma = lagMs;
        } else {
            const alpha = this.lagEwmaAlpha;
            this.processingLagEwma = (1 - alpha) * this.processingLagEwma + alpha * lagMs;
        }
        this.pruneProcessingLagSamples(nowTs);
    }

    private pruneExchangeLagSamples(nowTs: number): void {
        const cutoff = nowTs - this.lagWindowMs;
        let idx = 0;
        while (idx < this.exchangeLagSamples.length && this.exchangeLagSamples[idx].ts < cutoff) idx += 1;
        if (idx > 0) this.exchangeLagSamples.splice(0, idx);
    }

    private pruneProcessingLagSamples(nowTs: number): void {
        const cutoff = nowTs - this.lagWindowMs;
        let idx = 0;
        while (idx < this.processingLagSamples.length && this.processingLagSamples[idx].ts < cutoff) idx += 1;
        if (idx > 0) this.processingLagSamples.splice(0, idx);
    }

    private computeExchangeLagStats(nowTs: number): { p95?: number; ewma?: number } {
        this.pruneExchangeLagSamples(nowTs);
        if (this.exchangeLagSamples.length === 0) return { p95: undefined, ewma: this.exchangeLagEwma };
        const values = this.exchangeLagSamples.map((sample) => sample.lagMs).sort((a, b) => a - b);
        return { p95: this.percentile(values, 0.95), ewma: this.exchangeLagEwma };
    }

    private computeProcessingLagStats(nowTs: number): { p95?: number; ewma?: number } {
        this.pruneProcessingLagSamples(nowTs);
        if (this.processingLagSamples.length === 0) return { p95: undefined, ewma: this.processingLagEwma };
        const values = this.processingLagSamples.map((sample) => sample.lagMs).sort((a, b) => a - b);
        return { p95: this.percentile(values, 0.95), ewma: this.processingLagEwma };
    }

    private percentile(sortedValues: number[], p: number): number {
        if (sortedValues.length === 0) return 0;
        if (p <= 0) return sortedValues[0];
        if (p >= 1) return sortedValues[sortedValues.length - 1];
        const rank = Math.ceil(p * sortedValues.length) - 1;
        return sortedValues[Math.max(0, Math.min(sortedValues.length - 1, rank))];
    }

    private bucketEndTs(ts: number): number {
        return bucketCloseTs(ts, this.bucketMs);
    }

    private confidenceValue(value?: number): number {
        if (value === undefined || !Number.isFinite(value)) return 0;
        return this.clamp(value, 0, 1);
    }

    private clamp(value: number, min: number, max: number): number {
        return Math.max(min, Math.min(max, value));
    }

    private applyTimebasePenalty(block: ReadinessBlock, value: number, bucketTs: number): number {
        const issueTs = this.timebaseIssueByBlock.get(block);
        if (issueTs === undefined || this.timebasePenaltyWindowMs === 0) return value;
        if (bucketTs - issueTs > this.timebasePenaltyWindowMs) {
            this.timebaseIssueByBlock.delete(block);
            return value;
        }
        return this.clamp(value * TIMEBASE_CONFIDENCE_PENALTY, 0, 1);
    }

    private logFlowDebug(
        symbol: string,
        marketType: MarketType,
        bucketTs: number,
        flowSpot: number,
        flowFutures: number,
        flowRaw: number,
        flowAfterPenalty: number,
        flowTypesUsed: MarketType[]
    ): void {
        const key = `${symbol}:${marketType}`;
        const lastBucket = this.lastFlowDebugByKey.get(key);
        if (lastBucket === bucketTs) return;
        this.lastFlowDebugByKey.set(key, bucketTs);

        const expectedFlowTypes = flowTypesUsed.length ? flowTypesUsed : (['spot', 'futures'] as const);
        let minSource = 'n/a';
        if (expectedFlowTypes.length === 1) {
            minSource = expectedFlowTypes[0];
        } else if (flowSpot < flowFutures) {
            minSource = 'spot';
        } else if (flowFutures < flowSpot) {
            minSource = 'futures';
        } else {
            minSource = 'tie';
        }

        logger.info(
            `[FlowDebug] ${JSON.stringify({
                symbol,
                marketType,
                bucketTs,
                flowSpot,
                flowFutures,
                flowRaw,
                flowAfterPenalty,
                expectedFlowTypes,
                minSource,
            })}`
        );
    }

    private resolveFlowTypesForMarketType(marketType: MarketType): MarketType[] {
        const hasSpot = this.expectedFlowTypes.has('spot');
        const hasFutures = this.expectedFlowTypes.has('futures');
        if (marketType === 'spot' && hasSpot) return ['spot'];
        if (marketType === 'futures' && hasFutures) return ['futures'];
        const fallback: MarketType[] = [];
        if (hasSpot) fallback.push('spot');
        if (hasFutures) fallback.push('futures');
        return fallback.length ? fallback : ['spot', 'futures'];
    }

    private logNoRefPriceDebug(
        bucketTs: number,
        snapshot: SourceRegistrySnapshot,
        priceEvt: AggregatedWithMeta | undefined,
        priceBucketMatch: boolean
    ): void {
        const key = `${snapshot.symbol}:${snapshot.marketType}`;
        const lastBucket = this.lastNoRefPriceDebugByKey.get(key);
        if (lastBucket === bucketTs) return;
        this.lastNoRefPriceDebugByKey.set(key, bucketTs);

        const priceTsEvent = priceEvt?.meta.tsEvent;
        const priceBucketTs = priceTsEvent !== undefined ? this.bucketEndTs(priceTsEvent) : undefined;
        const priceConfidence = this.confidenceValue(priceEvt?.confidenceScore);
        const mismatchBlocks = Array.from(this.mismatchByBlock.entries())
            .filter(([, value]) => value)
            .map(([block]) => block)
            .sort();

        logger.info(
            `[ReadinessDebug] ${JSON.stringify({
                bucketTs,
                symbol: snapshot.symbol,
                marketType: snapshot.marketType,
                priceTsEvent,
                priceBucketTs,
                priceBucketMatch,
                priceConfidence,
                mismatchBlocks,
            })}`
        );
    }

    private logGapDebug(
        type: 'data:gapDetected' | 'data:sequence_gap_or_out_of_order',
        evt: DataGapDetected | DataSequenceGapOrOutOfOrder,
        block: ReadinessBlock
    ): void {
        const marketType = inferMarketTypeFromStreamId(evt.streamId);
        if (type === 'data:gapDetected') {
            const gap = evt as DataGapDetected;
            logger.info(
                `[GapDebug] ${JSON.stringify({
                    type,
                    block,
                    topic: gap.topic,
                    symbol: gap.symbol,
                    marketType,
                    streamId: gap.streamId,
                    prevTsExchange: gap.prevTsExchange,
                    currTsExchange: gap.currTsExchange,
                    deltaMs: gap.deltaMs,
                })}`
            );
            return;
        }
        const seq = evt as DataSequenceGapOrOutOfOrder;
        logger.info(
            `[GapDebug] ${JSON.stringify({
                type,
                block,
                topic: seq.topic,
                symbol: seq.symbol,
                marketType,
                streamId: seq.streamId,
                prevSeq: seq.prevSeq,
                currSeq: seq.currSeq,
                kind: seq.kind,
            })}`
        );
    }

    private mapTopicToBlock(topic: string): ReadinessBlock | undefined {
        if (topic.startsWith('market:price')) return 'price';
        if (topic.startsWith('market:cvd')) return 'flow';
        if (topic.startsWith('market:orderbook') || topic.startsWith('market:liquidity')) return 'liquidity';
        if (topic.startsWith('market:oi') || topic.startsWith('market:funding') || topic.startsWith('market:liquidation')) return 'derivatives';
        return undefined;
    }

    private isSequenceBasedTopic(topic: string): boolean {
        return topic.startsWith('market:orderbook');
    }

    private toConfidence(evt: AggregatedWithMeta, topic: string): DataConfidence {
        const tsIngest = typeof evt.meta.tsIngest === 'number' ? asTsMs(evt.meta.tsIngest) : undefined;
        return {
            sourceId: evt.symbol,
            topic,
            symbol: evt.symbol,
            ts: evt.ts,
            confidenceScore: this.confidenceValue(evt.confidenceScore),
            freshSourcesCount: evt.sourcesUsed?.length ?? 0,
            staleSourcesDropped: evt.staleSourcesDropped,
            mismatchDetected: evt.mismatchDetected,
            meta: createMeta('global_data', { tsEvent: asTsMs(evt.meta.tsEvent), tsIngest }),
        };
    }

    private resolveStatusIngestTs(bucketTs: number): number {
        const candidates: number[] = [];
        for (const evt of this.lastByBlock.values()) {
            const ingest = evt.meta.tsIngest ?? evt.meta.tsEvent;
            if (ingest !== undefined) candidates.push(ingest);
        }
        for (const evt of this.dataConfidenceByTopic.values()) {
            const ingest = evt.meta.tsIngest ?? evt.meta.tsEvent;
            if (ingest !== undefined) candidates.push(ingest);
        }
        if (!candidates.length) return bucketTs;
        return Math.max(...candidates);
    }

    private lowestBlock(blockConfidence: Record<ReadinessBlock, number>): ReadinessBlock {
        const allBlocks: ReadinessBlock[] = ['price', 'flow', 'liquidity', 'derivatives'];
        const blocks = allBlocks.filter((block): block is ReadinessBlock => this.isCriticalBlock(block));
        if (!blocks.length) return 'price';

        let lowest: ReadinessBlock = blocks[0];
        let lowestValue = blockConfidence[lowest];
        for (const block of blocks.slice(1)) {
            const value = blockConfidence[block];
            if (value < lowestValue) {
                lowestValue = value;
                lowest = block;
            }
        }
        return lowest;
    }

    private isCriticalBlock(block: ReadinessBlock): boolean {
        return this.criticalBlocks.has(block);
    }

    private normalizeWeights(weights: Record<ReadinessBlock, number>, criticalBlocks: Set<ReadinessBlock>): Record<ReadinessBlock, number> {
        const normalized: Record<ReadinessBlock, number> = {
            price: weights.price,
            flow: weights.flow,
            liquidity: weights.liquidity,
            derivatives: weights.derivatives,
        };
        for (const block of Object.keys(normalized) as ReadinessBlock[]) {
            if (!criticalBlocks.has(block)) {
                normalized[block] = 0;
            }
        }
        const sum = Object.values(normalized).reduce((acc, value) => acc + value, 0);
        if (sum <= 0) {
            return { price: 1, flow: 0, liquidity: 0, derivatives: 0 };
        }
        return {
            price: normalized.price / sum,
            flow: normalized.flow / sum,
            liquidity: normalized.liquidity / sum,
            derivatives: normalized.derivatives / sum,
        };
    }
}

function isTargetMarketType(value: MarketType | undefined): value is 'spot' | 'futures' {
    return value === 'spot' || value === 'futures';
}

function readFlag(name: string, fallback: boolean): boolean {
    const raw = process.env[name];
    if (raw === undefined) return fallback;
    const normalized = raw.trim().toLowerCase();
    if (normalized === '0' || normalized === 'false' || normalized === 'off') return false;
    if (normalized === '1' || normalized === 'true' || normalized === 'on') return true;
    return fallback;
}
