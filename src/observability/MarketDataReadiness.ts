import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    createMeta,
    eventBus as defaultEventBus,
    type DataConfidence,
    type DataGapDetected,
    type DataMismatch,
    type DataOutOfOrder,
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
    confidenceStaleWindowMs?: number;
    derivativesStaleWindowMs?: number;
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
    marketStatusJson?: boolean;
}

type AggregatedWithMeta = {
    symbol: string;
    ts: number;
    meta: { ts: number };
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
    | 'LAG_TOO_HIGH'
    | 'GAPS_DETECTED'
    | 'MISMATCH_DETECTED'
    | 'EXPECTED_SOURCES_MISSING_CONFIG'
    | 'NON_MONOTONIC_TIMEBASE'
    | 'NO_REF_PRICE'
    | 'WS_DISCONNECTED';

const DEFAULT_BUCKET_MS = 1_000;
const DEFAULT_WARMUP_MS = 30 * 60_000;
const DEFAULT_LOG_INTERVAL_MS = 10_000;
const DEFAULT_WS_RECOVERY_WINDOW_MS = 15_000;
const CRITICAL_BLOCK_THRESHOLD = 0.55;
const OVERALL_THRESHOLD = 0.65;

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
    'LAG_TOO_HIGH',
    'GAPS_DETECTED',
    'MISMATCH_DETECTED',
    'NO_REF_PRICE',
    'NON_MONOTONIC_TIMEBASE',
];

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
    private readonly confidenceStaleWindowMs: number;
    private readonly derivativesStaleWindowMs: number;
    private readonly thresholds: { criticalBlock: number; overall: number };
    private readonly weights: Record<ReadinessBlock, number>;
    private readonly criticalBlocks: Set<ReadinessBlock>;
    private readonly startupGraceWindowMs: number;
    private readonly marketStatusJson: boolean;

    private started = false;
    private readonly unsubscribers: Array<() => void> = [];

    private readonly lastByBlock = new Map<ReadinessBlock, AggregatedWithMeta>();
    private readonly sourcesByBlock = new Map<ReadinessBlock, Set<string>>();
    private readonly mismatchByBlock = new Map<ReadinessBlock, boolean>();

    private readonly gapByBlock = new Map<ReadinessBlock, boolean>();
    private readonly lagByBlock = new Map<ReadinessBlock, boolean>();

    private readonly dataConfidenceByTopic = new Map<string, DataConfidence>();

    private lastSymbol?: string;
    private lastMarketType?: MarketType;

    private firstBucketTs?: number;
    private lastBucketTs?: number;
    private lastStatus?: MarketDataStatusPayload;
    private lastSnapshot?: SourceRegistrySnapshot;
    private lastLogTs?: number;
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
        this.confidenceStaleWindowMs = Math.max(0, options.confidenceStaleWindowMs ?? this.bucketMs);
        this.derivativesStaleWindowMs = Math.max(0, options.derivativesStaleWindowMs ?? this.confidenceStaleWindowMs);
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
        const onOutOfOrder = (evt: DataOutOfOrder) => this.onOutOfOrder(evt);
        const onTrade = (evt: TradeEvent) => this.onRaw('trades', evt.symbol, evt.marketType, evt.streamId, evt.meta.ts);
        const onOrderbookSnapshot = (evt: OrderbookL2SnapshotEvent) =>
            this.onRaw('orderbook', evt.symbol, evt.marketType, evt.streamId, evt.meta.ts);
        const onOrderbookDelta = (evt: OrderbookL2DeltaEvent) =>
            this.onRaw('orderbook', evt.symbol, evt.marketType, evt.streamId, evt.meta.ts);
        const onOiRaw = (evt: OpenInterestEvent) => this.onRaw('oi', evt.symbol, evt.marketType, evt.streamId, evt.meta.ts);
        const onFundingRaw = (evt: FundingRateEvent) => this.onRaw('funding', evt.symbol, evt.marketType, evt.streamId, evt.meta.ts);
        const onTicker = (evt: TickerEvent) => this.onTickerRaw(evt);
        const onKline = (evt: KlineEvent) => this.onRaw('klines', evt.symbol, evt.marketType, evt.streamId, evt.meta.ts);
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
        this.bus.subscribe('data:outOfOrder', onOutOfOrder);
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
            () => this.bus.unsubscribe('data:outOfOrder', onOutOfOrder),
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
    }

    private onOutOfOrder(evt: DataOutOfOrder): void {
        const block = this.mapTopicToBlock(evt.topic);
        if (!block) return;
        this.lagByBlock.set(block, true);
    }

    private onWsConnected(evt: MarketConnected): void {
        if (!this.wsDegraded) return;
        // Подключение само по себе не означает готовность — ждём стабильного потока данных.
        if (this.wsLastDisconnectTs === undefined) {
            this.wsLastDisconnectTs = evt.meta.ts;
        }
    }

    private onWsDisconnected(evt: MarketDisconnected): void {
        this.wsDegraded = true;
        this.wsLastDisconnectTs = evt.meta.ts;
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

    private onRaw(feed: RawFeed, symbol: string, marketType: MarketType | undefined, streamId: string | undefined, ts: number): void {
        const sourceId = streamId ?? 'unknown';
        const inferred = marketType ?? inferMarketTypeFromStreamId(streamId);
        const normalizedMarketType = normalizeMarketType(inferred);
        if (!this.shouldTrackMarketType(normalizedMarketType)) return;
        const normalizedSymbol = normalizeSymbol(symbol);
        this.registry.markRawSeen({ symbol: normalizedSymbol, marketType: normalizedMarketType, feed }, sourceId, ts);
        this.lastSymbol = normalizedSymbol;
        this.lastMarketType = normalizedMarketType;
        this.markWsFlow(ts);
    }

    private onTickerRaw(evt: TickerEvent): void {
        const inferred = evt.marketType ?? inferMarketTypeFromStreamId(evt.streamId);
        const normalizedMarketType = normalizeMarketType(inferred);
        if (!this.shouldTrackMarketType(normalizedMarketType)) return;
        const normalizedSymbol = normalizeSymbol(evt.symbol);
        const sourceId = evt.streamId ?? 'unknown';
        if (evt.markPrice !== undefined) {
            this.registry.markRawSeen(
                { symbol: normalizedSymbol, marketType: normalizedMarketType, feed: 'markPrice' },
                sourceId,
                evt.meta.ts
            );
        }
        if (evt.indexPrice !== undefined) {
            this.registry.markRawSeen(
                { symbol: normalizedSymbol, marketType: normalizedMarketType, feed: 'indexPrice' },
                sourceId,
                evt.meta.ts
            );
        }
        this.lastSymbol = normalizedSymbol;
        this.lastMarketType = normalizedMarketType;
        this.markWsFlow(evt.meta.ts);
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
            evt.meta.ts
        );
        this.lastSymbol = normalizedSymbol;
        this.lastMarketType = normalizedMarketType;
        this.markWsFlow(evt.meta.ts);

        const bucketTs = this.bucketEndTs(evt.meta.ts);
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
        const activeSourcesAgg = this.countSources(snapshot.usedAgg);
        const activeSourcesRaw = this.countSources(snapshot.usedRaw);
        const degradedReasons = this.computeDegradedReasons(
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
            warmingUp,
            warmingProgress,
            warmingWindowMs,
            activeSources: activeSourcesAgg,
            expectedSources: derivedSources.expectedAggCount,
            activeSourcesAgg,
            activeSourcesRaw,
            expectedSourcesAgg: derivedSources.expectedAggCount,
            expectedSourcesRaw: derivedSources.expectedRawCount,
            lastBucketTs: bucketTs,
            meta: createMeta('system', { ts: bucketTs }),
        };
    }

    private computeBlockConfidence(bucketTs: number, snapshot: SourceRegistrySnapshot): Record<ReadinessBlock, number> {
        const price = this.expectedBlockEmpty(snapshot, 'price') ? 1 : this.confidenceForBlock('price', bucketTs);
        const flowSpot = this.confidenceForTopic('flow_spot', bucketTs);
        const flowFutures = this.confidenceForTopic('flow_futures', bucketTs);
        const flowCandidates: number[] = [];
        if (this.expectedFlowTypes.has('spot')) flowCandidates.push(flowSpot);
        if (this.expectedFlowTypes.has('futures')) flowCandidates.push(flowFutures);
        const flowRaw = flowCandidates.length ? Math.min(...flowCandidates) : Math.min(flowSpot, flowFutures);
        const flow = this.expectedBlockEmpty(snapshot, 'flow') ? 1 : flowRaw;
        const liquidity = this.expectedBlockEmpty(snapshot, 'liquidity') ? 1 : this.confidenceForBlock('liquidity', bucketTs);
        const derivOi = this.confidenceForTopic('derivatives_oi', bucketTs);
        const derivFunding = this.confidenceForTopic('derivatives_funding', bucketTs);
        const derivLiq = this.confidenceForTopic('derivatives_liquidations', bucketTs);
        const derivativeCandidates: number[] = [];
        if (this.expectedDerivativeKinds.has('oi')) derivativeCandidates.push(derivOi);
        if (this.expectedDerivativeKinds.has('funding')) derivativeCandidates.push(derivFunding);
        if (this.expectedDerivativeKinds.has('liquidations')) derivativeCandidates.push(derivLiq);
        const derivativesRaw = derivativeCandidates.length ? Math.min(...derivativeCandidates) : Math.min(derivOi, derivFunding, derivLiq);
        const derivatives = this.expectedBlockEmpty(snapshot, 'derivatives') ? 1 : derivativesRaw;
        return { price, flow, liquidity, derivatives };
    }

    private confidenceForBlock(block: ReadinessBlock, bucketTs: number): number {
        const evt = this.lastByBlock.get(block);
        if (!evt) return 0;
        if (bucketTs - evt.meta.ts > this.confidenceStaleWindowMs) return 0;
        return this.confidenceValue(evt.confidenceScore);
    }

    private confidenceForTopic(key: string, bucketTs: number): number {
        const evt = this.dataConfidenceByTopic.get(key);
        if (!evt) return 0;
        const staleWindow = key.startsWith('derivatives_') ? this.derivativesStaleWindowMs : this.confidenceStaleWindowMs;
        if (bucketTs - evt.ts > staleWindow) return 0;
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
    ): string[] {
        const reasons = new Set<Reason>();
        const withinStartupGrace =
            this.startupGraceWindowMs > 0 &&
            this.startedAtTs !== undefined &&
            bucketTs - this.startedAtTs < this.startupGraceWindowMs;
        const expectedPrice = !this.expectedBlockEmpty(snapshot, 'price');
        const expectedFlow = !this.expectedBlockEmpty(snapshot, 'flow');
        const expectedLiquidity = !this.expectedBlockEmpty(snapshot, 'liquidity');
        const expectedDerivatives = !this.expectedBlockEmpty(snapshot, 'derivatives');

        const priceEvt = this.lastByBlock.get('price');
        const priceBucketMatch = priceEvt && this.bucketEndTs(priceEvt.meta.ts) === bucketTs;
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
                    reasons.add('LAG_TOO_HIGH');
                }
            }
        }

        if (Array.from(this.lagByBlock.values()).some(Boolean) && !withinStartupGrace) reasons.add('LAG_TOO_HIGH');
        if (Array.from(this.gapByBlock.values()).some(Boolean)) reasons.add('GAPS_DETECTED');

        const hasMismatch = Array.from(this.mismatchByBlock.values()).some(Boolean);
        const priceValid = priceBucketMatch && this.confidenceValue(priceEvt?.confidenceScore) >= this.thresholds.criticalBlock;
        if (hasMismatch) {
            if (priceValid) reasons.add('MISMATCH_DETECTED');
            else reasons.add('NO_REF_PRICE');
        }

        if (snapshot.nonMonotonicSources.length > 0) {
            reasons.add('NON_MONOTONIC_TIMEBASE');
        }

        return this.orderReasons(reasons);
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
        expectedAggCount: number;
        expectedRawCount: number;
        missingConfig: boolean;
    } {
        const expectedAggSources = this.unionSources(snapshot.expected);
        const usedRawSources = this.unionSources(snapshot.usedRaw);
        if (expectedAggSources.length === 0 && usedRawSources.length > 0) {
            return {
                expectedAggCount: usedRawSources.length,
                expectedRawCount: usedRawSources.length,
                missingConfig: true,
            };
        }
        const expectedCount = expectedAggSources.length || this.expectedSources;
        return {
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

    private orderReasons(reasons: Set<Reason>): string[] {
        return REASON_ORDER.filter((r) => reasons.has(r));
    }

    private maybeLog(payload: MarketDataStatusPayload): void {
        const last = this.lastStatus;
        const ts = payload.lastBucketTs;
        const shouldLog = this.shouldLog(ts, last, payload);
        if (!shouldLog) return;

        this.lastLogTs = ts;

        const status = payload.warmingUp
            ? 'WARMING'
            : payload.degraded
              ? 'DEGRADED'
              : 'READY';

        const snapshotLog = this.buildSnapshotLog(payload);
        if (this.marketStatusJson) {
            const snapshotLog = this.buildSnapshotLog(payload);
            logger.debug(JSON.stringify(snapshotLog));
        }

        const line = [
            `Market Data: ${status} (confidence ${payload.overallConfidence.toFixed(2)})`,
            `SourcesAgg: ${payload.activeSourcesAgg}/${payload.expectedSourcesAgg}`,
            `SourcesRaw: ${payload.activeSourcesRaw}/${payload.expectedSourcesRaw}`,
            `Price: ${payload.blockConfidence.price.toFixed(2)} | Flow: ${payload.blockConfidence.flow.toFixed(2)} | Liquidity: ${payload.blockConfidence.liquidity.toFixed(2)} | Derivatives: ${payload.blockConfidence.derivatives.toFixed(2)}`,
            `Warming Up: ${payload.warmingUp ? `YES (${payload.warmingProgress.toFixed(2)})` : 'NO'}`,
            `Degraded: ${payload.degraded ? 'YES' : 'NO'}`,
        ];
        if (payload.degraded && payload.degradedReasons.length) {
            line.push(`Reasons: ${payload.degradedReasons.join(',')}`);
        }
        logger.info(line.join('\n'));
    }

    private buildSnapshotLog(payload: MarketDataStatusPayload): Record<string, unknown> {
        const snapshot = this.lastSnapshot ?? this.buildSnapshot(payload.lastBucketTs);
        return {
            tsMeta: payload.meta.ts,
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
            },
        };
    }

    private shouldLog(ts: number, last: MarketDataStatusPayload | undefined, next: MarketDataStatusPayload): boolean {
        if (!last) return true;
        if (last.degraded !== next.degraded) return true;
        if (last.warmingUp !== next.warmingUp) return true;
        if (last.degradedReasons.join(',') !== next.degradedReasons.join(',')) return true;
        const lastLogTs = this.lastLogTs ?? 0;
        if (ts - lastLogTs >= this.logIntervalMs) return true;
        return false;
    }

    private resetFlags(): void {
        this.gapByBlock.clear();
        this.lagByBlock.clear();
        this.mismatchByBlock.clear();
    }

    private bucketEndTs(ts: number): number {
        const adjusted = Math.max(0, ts - 1);
        const start = Math.floor(adjusted / this.bucketMs) * this.bucketMs;
        return start + this.bucketMs;
    }

    private confidenceValue(value?: number): number {
        if (value === undefined || !Number.isFinite(value)) return 0;
        return this.clamp(value, 0, 1);
    }

    private clamp(value: number, min: number, max: number): number {
        return Math.max(min, Math.min(max, value));
    }

    private mapTopicToBlock(topic: string): ReadinessBlock | undefined {
        if (topic.startsWith('market:price')) return 'price';
        if (topic.startsWith('market:cvd')) return 'flow';
        if (topic.startsWith('market:orderbook') || topic.startsWith('market:liquidity')) return 'liquidity';
        if (topic.startsWith('market:oi') || topic.startsWith('market:funding') || topic.startsWith('market:liquidation')) return 'derivatives';
        return undefined;
    }

    private toConfidence(evt: AggregatedWithMeta, topic: string): DataConfidence {
        return {
            sourceId: evt.symbol,
            topic,
            symbol: evt.symbol,
            ts: evt.ts,
            confidenceScore: this.confidenceValue(evt.confidenceScore),
            freshSourcesCount: evt.sourcesUsed?.length ?? 0,
            staleSourcesDropped: evt.staleSourcesDropped,
            mismatchDetected: evt.mismatchDetected,
            meta: createMeta('global_data', { ts: evt.meta.ts }),
        };
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
