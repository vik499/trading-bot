import EventEmitter from 'eventemitter3';
import { randomUUID } from 'node:crypto';

// ============================================================================
// Shared meta
// ----------------------------------------------------------------------------
// Базовые поля для трассировки событий в production:
// - source: кто инициатор (CLI/Telegram/System/Risk/etc.)
// - correlationId: связывает цепочку событий в один "flow" (потом пригодится для трассировки)
// - ts: unix ms (когда событие создано внутри бота)
// ============================================================================

// ---------------------------------------------------------------------------
// Общие типы
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Time/Sequence contracts (брендированные типы)
// ---------------------------------------------------------------------------

export type TsMs = number & { __brand: 'TsMs' };
export type Seq = number & { __brand: 'Seq' };

export const asTsMs = (value: number): TsMs => value as TsMs;
export const asSeq = (value: number): Seq => value as Seq;

// Откуда пришло событие/команда (помогает понимать, кто инициировал действие)
export type EventSource =
    | 'cli'
    | 'telegram'
    | 'system'
    | 'market'
    | 'binance'
    | 'okx'
    | 'global_data'
    | 'replay'
    | 'analytics'
    | 'strategy'
    | 'risk'
    | 'trading'
    | 'portfolio'
    | 'storage'
    | 'research'
    | 'metrics'
    | 'state';

export interface EventMeta {
    source: EventSource;
    tsEvent: TsMs; // unix ms (время события)
    /** @deprecated use tsEvent */
    ts: TsMs;
    tsIngest?: TsMs; // unix ms (время приёма)
    tsExchange?: TsMs; // unix ms (время от биржи, если доступно)
    sequence?: Seq; // последовательность (например updateId/seqId)
    streamId?: string; // source stream id (venue+channel+symbol)
    correlationId?: string; // связывает цепочку событий
}

type MetaOptions = {
    correlationId?: string;
    tsEvent?: TsMs | number;
    ts?: TsMs | number;
    tsIngest?: TsMs | number;
    tsExchange?: TsMs | number;
    sequence?: Seq | number;
    streamId?: string;
};

type MetaOptionsWithIngest = MetaOptions & { tsIngest: TsMs | number };

// ---------------------------------------------------------------------------
// Meta helpers (единый способ создавать meta по всему проекту)
// ---------------------------------------------------------------------------

/**
 * Date.now() в виде функции, чтобы легче тестировать и мокать.
 */
export const nowMs = (): number => Date.now();

/**
 * Генератор correlationId. В Node 18+ есть crypto.randomUUID().
 */
export const newCorrelationId = (): string => randomUUID();

/**
 * Создать meta для события/команды.
 *
 * Почему так:
 * - все события в системе должны иметь meta
 * - correlationId связывает цепочку событий (трассировка)
 * - source помогает понять, откуда пришло действие
 */
export function createMeta(source: EventSource, opts: MetaOptionsWithIngest): EventMeta & { tsIngest: TsMs };
export function createMeta(source: EventSource, opts?: MetaOptions): EventMeta;
export function createMeta(source: EventSource, opts: MetaOptions = {}): EventMeta {
    const tsEvent = normalizeTs(opts.tsEvent ?? opts.ts) ?? asTsMs(nowMs());
    return {
        source,
        tsEvent,
        ts: tsEvent,
        tsIngest: normalizeTs(opts.tsIngest),
        tsExchange: normalizeTs(opts.tsExchange),
        sequence: normalizeSeq(opts.sequence),
        streamId: opts.streamId,
        correlationId: opts.correlationId,
    };
}

/**
 * Унаследовать correlationId из родительского события (если он был).
 * Удобно, когда одно событие порождает другое.
 */
export function inheritMeta(
    parent: EventMeta,
    source: EventSource,
    opts: {
        tsEvent?: TsMs | number;
        ts?: TsMs | number;
        tsIngest?: TsMs | number;
        tsExchange?: TsMs | number;
        sequence?: Seq | number;
        streamId?: string;
    } = {}
): EventMeta {
    const tsEvent = normalizeTs(opts.tsEvent ?? opts.ts) ?? asTsMs(nowMs());
    return {
        source,
        tsEvent,
        ts: tsEvent,
        tsIngest: normalizeTs(opts.tsIngest) ?? parent.tsIngest,
        tsExchange: normalizeTs(opts.tsExchange) ?? parent.tsExchange,
        sequence: normalizeSeq(opts.sequence) ?? parent.sequence,
        streamId: opts.streamId ?? parent.streamId,
        correlationId: parent.correlationId ?? parent.tsEvent.toString(),
    };
}

const normalizeTs = (value?: TsMs | number): TsMs | undefined => {
    if (value === undefined) return undefined;
    return asTsMs(Number(value));
};

const normalizeSeq = (value?: Seq | number): Seq | undefined => {
    if (value === undefined) return undefined;
    return asSeq(Number(value));
};

// ---------------------------------------------------------------------------
// Base event
// ---------------------------------------------------------------------------

export interface BaseEvent {
    meta: EventMeta;
}

export interface RawEventMeta extends EventMeta {
    tsIngest: TsMs;
    tsEvent: TsMs;
    tsExchange?: TsMs;
}

// ============================================================================
// EventBus
// ----------------------------------------------------------------------------
// Централизованный брокер событий для всего торгового бота.
// Позволяет модулям обмениваться сообщениями без прямых зависимостей.
//
// ВАЖНО:
// - Сюда летят "нормализованные" события (наши собственные типы),
//   а не сырые ответы Bybit.
// - Это основа для масштабирования: MarketStore, Analytics, Strategy и т.д.
// ============================================================================

// ---------------------------------------------------------------------------
// Нормализованное событие "тикер" (market:ticker)
//
// Мы сознательно НЕ тащим сюда Bybit-специфичные структуры/названия.
// Внутри бота мы работаем со стабильным контрактом.
// ---------------------------------------------------------------------------
export type MarketType = 'spot' | 'futures' | 'unknown';

export type VenueId = 'bybit' | 'binance' | 'okx';

export interface TickerEvent extends BaseEvent {
    // Символ рынка (например BTCUSDT)
    symbol: string;

    // Идентификатор источника (например bybit.public.linear.v5)
    streamId?: string;

    // Тип рынка (spot/futures)
    marketType?: MarketType;

    // Цена последней сделки/обновления (строка, потому что Bybit присылает числа как строки)
    lastPrice?: string;

    // Mark price (цена маркировки, важна для деривативов)
    markPrice?: string;

    // Index price (индексная цена)
    indexPrice?: string;

    // Изменение за 24 часа (доля), например "-0.021485" ≈ -2.1485%
    price24hPcnt?: string;

    // Максимум/минимум за 24 часа
    highPrice24h?: string;
    lowPrice24h?: string;

    // Объём и оборот за 24 часа
    volume24h?: string;
    turnover24h?: string;

    // Биржевой timestamp сообщения (если доступен)
    exchangeTs?: number;
}

// ---------------------------------------------------------------------------
// Нормализованное событие "свеча" (market:kline)
// ---------------------------------------------------------------------------
export type KlineInterval = '1' | '3' | '5' | '15' | '30' | '60' | '120' | '240' | '360' | '720' | '1440';

export interface KlinePayload {
    symbol: string;
    streamId?: string;
    marketType?: MarketType;
    interval: KlineInterval;
    tf: string;
    startTs: number;
    endTs: number;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
}

export interface KlineEvent extends BaseEvent, KlinePayload {}

export type Kline = KlinePayload;

// ---------------------------------------------------------------------------
// Нормализованные события рынка: trades / orderbook / oi / funding
// ---------------------------------------------------------------------------

export interface TradeEvent extends BaseEvent {
    symbol: string;
    streamId: string;
    tradeId?: string;
    side: 'Buy' | 'Sell';
    price: number;
    size: number;
    tradeTs: number;
    exchangeTs?: number;
    marketType?: MarketType;
}

// ---------------------------------------------------------------------------
// Normalized raw events (multi-venue, lossless string fields)
// ---------------------------------------------------------------------------

export type RawSide = 'buy' | 'sell';
export type RawLevel = [price: string, size: string];

export interface TradeRawEvent extends BaseEvent {
    venue: VenueId;
    symbol: string;
    venueSymbol?: string;
    exchangeTsMs: number;
    recvTsMs: number;
    price: string;
    size: string;
    side: RawSide;
    tradeId?: string;
    seq?: number;
    meta: RawEventMeta;
}

export interface OrderbookSnapshotRawEvent extends BaseEvent {
    venue: VenueId;
    symbol: string;
    venueSymbol?: string;
    exchangeTsMs: number;
    recvTsMs: number;
    bids: RawLevel[];
    asks: RawLevel[];
    sequence?: number;
    meta: RawEventMeta;
}

export interface OrderbookDeltaRawEvent extends BaseEvent {
    venue: VenueId;
    symbol: string;
    venueSymbol?: string;
    exchangeTsMs: number;
    recvTsMs: number;
    bids: RawLevel[];
    asks: RawLevel[];
    sequence?: number;
    prevSequence?: number;
    range?: { start: number; end: number };
    meta: RawEventMeta;
}

export interface CandleRawEvent extends BaseEvent {
    venue: VenueId;
    symbol: string;
    venueSymbol?: string;
    interval: string;
    startTsMs: number;
    endTsMs: number;
    open: string;
    high: string;
    low: string;
    close: string;
    volume: string;
    isClosed: boolean;
    exchangeTsMs: number;
    recvTsMs: number;
    meta: RawEventMeta;
}

export interface MarkPriceRawEvent extends BaseEvent {
    venue: VenueId;
    symbol: string;
    venueSymbol?: string;
    exchangeTsMs: number;
    recvTsMs: number;
    markPrice: string;
    meta: RawEventMeta;
}

export interface IndexPriceRawEvent extends BaseEvent {
    venue: VenueId;
    symbol: string;
    venueSymbol?: string;
    exchangeTsMs: number;
    recvTsMs: number;
    indexPrice: string;
    meta: RawEventMeta;
}

export interface FundingRawEvent extends BaseEvent {
    venue: VenueId;
    symbol: string;
    venueSymbol?: string;
    exchangeTsMs: number;
    recvTsMs: number;
    fundingRate: string;
    nextFundingTsMs?: number;
    meta: RawEventMeta;
}

export interface OpenInterestRawEvent extends BaseEvent {
    venue: VenueId;
    symbol: string;
    venueSymbol?: string;
    exchangeTsMs: number;
    recvTsMs: number;
    openInterest: string;
    openInterestUsd?: string;
    meta: RawEventMeta;
}

export interface LiquidationRawEvent extends BaseEvent {
    venue: VenueId;
    symbol: string;
    venueSymbol?: string;
    exchangeTsMs: number;
    recvTsMs: number;
    side?: RawSide;
    price?: string;
    size?: string;
    notionalUsd?: string;
    meta: RawEventMeta;
}

export interface WsEventRaw extends BaseEvent {
    venue: VenueId;
    streamId?: string;
    event: string;
    code?: string;
    msg?: string;
    payload: Record<string, unknown>;
}

export interface OrderbookLevel {
    price: number;
    size: number;
}

export interface OrderbookL2SnapshotEvent extends BaseEvent {
    symbol: string;
    streamId: string;
    updateId: number;
    exchangeTs?: number;
    marketType?: MarketType;
    bids: OrderbookLevel[];
    asks: OrderbookLevel[];
}

export interface OrderbookL2DeltaEvent extends BaseEvent {
    symbol: string;
    streamId: string;
    updateId: number;
    exchangeTs?: number;
    marketType?: MarketType;
    bids: OrderbookLevel[];
    asks: OrderbookLevel[];
}

export interface OpenInterestEvent extends BaseEvent {
    symbol: string;
    streamId: string;
    openInterest: number;
    openInterestUnit: OpenInterestUnit;
    openInterestValueUsd?: number;
    exchangeTs?: number;
    marketType?: MarketType;
}

export type OpenInterestUnit = 'base' | 'contracts' | 'usd' | 'quote' | 'unknown';

export interface FundingRateEvent extends BaseEvent {
    symbol: string;
    streamId: string;
    fundingRate: number;
    exchangeTs?: number;
    nextFundingTs?: number;
    marketType?: MarketType;
}

export interface LiquidationEvent extends BaseEvent {
    symbol: string;
    streamId: string;
    side?: 'Buy' | 'Sell';
    price?: number;
    size?: number;
    notionalUsd?: number;
    exchangeTs?: number;
    marketType?: MarketType;
}

// ---------------------------------------------------------------------------
// Aggregated/global market data (multi-venue)
// ---------------------------------------------------------------------------

export interface AggregatedVenueBreakdown {
    [venue: string]: number;
}

export interface AggregatedSideBreakdown {
    buy?: number;
    sell?: number;
}

export interface AggregatedQualityFlags {
    consistentUnits?: boolean;
    mismatchDetected?: boolean;
    staleSourcesDropped?: string[];
    sequenceBroken?: boolean;
}

export interface ConfidenceExplain {
    score: number;
    penalties: Array<{ reason: string; value: number }>;
    inputs: Record<string, number | boolean | string[] | undefined>;
}

export interface MarketAggBase {
    confidence?: number;
    sourcesUsed?: string[];
    venueBreakdown?: AggregatedVenueBreakdown;
    mismatchDetected?: boolean;
    staleSourcesDropped?: string[];
    qualityFlags?: AggregatedQualityFlags;
    confidenceExplain?: ConfidenceExplain;
}

export type CvdUnit = 'base' | 'usd';

export type CanonicalPriceType = 'index' | 'mark' | 'last';

export type CanonicalPriceFallbackReason = 'NO_INDEX' | 'INDEX_STALE' | 'NO_MARK' | 'MARK_STALE';

export type LiquidationUnit = 'base' | 'usd';

export type LiquidityDepthMethod = 'levels' | 'usd_band' | 'bps_band';
export type LiquidityDepthUnit = 'base' | 'usd';

export interface MarketOpenInterestAggEvent extends BaseEvent, MarketAggBase {
    symbol: string;
    ts: number;
    openInterest: number;
    openInterestUnit: OpenInterestUnit;
    openInterestValueUsd?: number;
    priceTypeUsed?: CanonicalPriceType;
    marketType?: MarketType;
    freshSourcesCount?: number;
    confidenceScore?: number;
    provider?: string;
    weightsUsed?: AggregatedVenueBreakdown;
}

export interface MarketFundingAggEvent extends BaseEvent, MarketAggBase {
    symbol: string;
    ts: number;
    fundingRate: number;
    marketType?: MarketType;
    freshSourcesCount?: number;
    confidenceScore?: number;
    provider?: string;
    weightsUsed?: AggregatedVenueBreakdown;
}

export interface MarketLiquidationsAggEvent extends BaseEvent, MarketAggBase {
    symbol: string;
    ts: number;
    liquidationCount: number;
    liquidationNotional: number;
    unit: LiquidationUnit;
    sideBreakdown?: AggregatedSideBreakdown;
    marketType?: MarketType;
    freshSourcesCount?: number;
    confidenceScore?: number;
    provider?: string;
    weightsUsed?: AggregatedVenueBreakdown;
    bucketStartTs?: number;
    bucketEndTs?: number;
    bucketSizeMs?: number;
}

export interface MarketVolumeAggEvent extends BaseEvent, MarketAggBase {
    symbol: string;
    ts: number;
    spotVolumeUsd?: number;
    futuresVolumeUsd?: number;
    freshSourcesCount?: number;
    confidenceScore?: number;
    provider?: string;
    weightsUsed?: AggregatedVenueBreakdown;
    bucketStartTs?: number;
    bucketEndTs?: number;
}

export interface MarketCvdEvent extends BaseEvent {
    symbol: string;
    streamId: string;
    marketType: MarketType;
    bucketStartTs: number;
    bucketEndTs: number;
    bucketSizeMs: number;
    cvdDelta: number;
    cvdTotal: number;
    unit: CvdUnit;
    exchangeTs?: number;
}

export interface MarketCvdAggEvent extends BaseEvent, MarketAggBase {
    symbol: string;
    ts: number;
    cvd: number;
    cvdSpot?: number;
    cvdFutures?: number;
    marketType?: MarketType;
    unit?: CvdUnit;
    bucketSizeMs?: number;
    freshSourcesCount?: number;
    confidenceScore?: number;
    provider?: string;
    weightsUsed?: AggregatedVenueBreakdown;
    cvdDelta?: number;
    bucketStartTs?: number;
    bucketEndTs?: number;
}

export interface MarketPriceIndexEvent extends BaseEvent, MarketAggBase {
    symbol: string;
    ts: number;
    indexPrice: number;
    marketType?: MarketType;
    freshSourcesCount?: number;
    confidenceScore?: number;
    provider?: string;
    weightsUsed?: AggregatedVenueBreakdown;
}

export interface MarketPriceCanonicalEvent extends BaseEvent {
    symbol: string;
    ts: number;
    marketType?: MarketType;
    indexPrice?: number;
    markPrice?: number;
    lastPrice?: number;
    priceTypeUsed?: CanonicalPriceType;
    fallbackReason?: CanonicalPriceFallbackReason;
    sourcesUsed: string[];
    freshSourcesCount: number;
    staleSourcesDropped?: string[];
    mismatchDetected?: boolean;
    confidenceScore?: number;
    provider?: string;
}

export interface MarketLiquidityAggEvent extends BaseEvent, MarketAggBase {
    symbol: string;
    ts: number;
    bestBid?: number;
    bestAsk?: number;
    spread?: number;
    depthBid?: number;
    depthAsk?: number;
    imbalance?: number;
    midPrice?: number;
    depthMethod: LiquidityDepthMethod;
    depthLevels?: number;
    depthUnit?: LiquidityDepthUnit;
    priceTypeUsed?: CanonicalPriceType;
    bucketStartTs?: number;
    bucketEndTs?: number;
    bucketSizeMs?: number;
    marketType?: MarketType;
    freshSourcesCount?: number;
    confidenceScore?: number;
    provider?: string;
    weightsUsed?: AggregatedVenueBreakdown;
    venueStatus?: Record<
        string,
        {
            freshness: 'fresh' | 'stale' | 'dropped';
            sequenceBroken?: boolean;
            lastSeq?: Seq;
            prevSeq?: Seq;
            lastTsEvent?: TsMs;
        }
    >;
}

// ---------------------------------------------------------------------------
// Control Plane: команды и состояние бота
// ---------------------------------------------------------------------------

// Режимы работы бота (верхнеуровневая бизнес-логика)
export type BotMode = 'LIVE' | 'PAPER' | 'BACKTEST';

export type BotLifecycle = 'STARTING' | 'RUNNING' | 'PAUSED' | 'STOPPING' | 'STOPPED' | 'ERROR';

export type ControlCommand =
    | { type: 'pause'; meta: EventMeta; reason?: string }
    | { type: 'resume'; meta: EventMeta; reason?: string }
    | { type: 'status'; meta: EventMeta }
    | { type: 'shutdown'; meta: EventMeta; reason?: string }
    | { type: 'set_mode'; mode: BotMode; meta: EventMeta; reason?: string };

export interface ControlState extends BaseEvent {
    mode: BotMode;
    paused: boolean;
    lifecycle: BotLifecycle;
    startedAt: number;
    lastCommandAt: number;
    lastCommand?: ControlCommand['type'];
    lastCommandReason?: string;
    shuttingDown?: boolean;
}

// ---------------------------------------------------------------------------
// Strategy / Risk / Paper Execution / Portfolio
// ---------------------------------------------------------------------------

export type StrategySide = 'LONG' | 'SHORT' | 'FLAT';

export interface StrategyIntentEvent extends BaseEvent {
    intentId: string;
    symbol: string;
    side: StrategySide;
    targetExposureUsd: number;
    reason: string;
    constraints?: {
        maxSlippageBps?: number;
        timeInForce?: string;
        validityMs?: number;
    };
    ts: number;
}

export interface StrategySignalEvent extends BaseEvent {
    symbol: string;
    signal: string;
    strength?: number;
    ts: number;
}

export interface StrategyStateEvent extends BaseEvent {
    symbol: string;
    lastIntentAt?: number;
    throttled?: boolean;
    ts: number;
}

export interface RiskApprovedIntentEvent extends BaseEvent {
    intent: StrategyIntentEvent;
    approvedAtTs: number;
    riskVersion: string;
    notes?: string;
}

export interface RiskRejectedIntentEvent extends BaseEvent {
    intent: StrategyIntentEvent;
    rejectedAtTs: number;
    reasonCode: 'PAUSED' | 'LIFECYCLE' | 'COOLDOWN' | 'LIMIT' | 'VOLATILITY' | 'MODE' | 'MARKET_DATA' | 'UNKNOWN';
    reason: string;
}

export interface PaperFillEvent extends BaseEvent {
    intentId: string;
    symbol: string;
    side: StrategySide;
    fillPrice: number;
    fillQty: number;
    notionalUsd: number;
    ts: number;
}

export interface PortfolioUpdateEvent extends BaseEvent {
    symbol: string;
    qty: number;
    avgPrice: number;
    realizedPnl: number;
    updatedAtTs: number;
}

export interface PortfolioSnapshotEvent extends BaseEvent {
    positions: Record<string, { qty: number; avgPrice: number; realizedPnl: number; updatedAtTs: number }>;
    ts: number;
}

// ---------------------------------------------------------------------------
// State snapshot / recovery
// ---------------------------------------------------------------------------

export interface SnapshotRequested extends BaseEvent {
    runId?: string;
    reason?: string;
    pathOverride?: string;
}

export interface SnapshotWritten extends BaseEvent {
    runId?: string;
    path: string;
    bytes: number;
    tsSnapshot: number;
}

export interface RecoveryRequested extends BaseEvent {
    runId?: string;
    pathOverride?: string;
}

export interface RecoveryLoaded extends BaseEvent {
    runId?: string;
    path: string;
    state: Record<string, unknown>;
}

export interface RecoveryFailed extends BaseEvent {
    runId?: string;
    path?: string;
    reason: string;
    error?: unknown;
}

// ---------------------------------------------------------------------------
// Metrics / backtest
// ---------------------------------------------------------------------------

export interface BacktestSummary extends BaseEvent {
    runId: string;
    symbol?: string;
    totalFills: number;
    totalApprovedIntents?: number;
    totalRejectedIntents?: number;
    realizedPnl: number;
    maxDrawdown: number;
    startTs: number;
    endTs: number;
    notes?: string;
}

// ---------------------------------------------------------------------------
// Market lifecycle (event-driven connect/disconnect/subscribe)
// ---------------------------------------------------------------------------

export interface MarketConnectRequest extends BaseEvent {
    url?: string;
    subscriptions?: string[]; // например: ['tickers.BTCUSDT']
    venue?: VenueId;
    marketType?: MarketType;
}

export interface MarketDisconnectRequest extends BaseEvent {
    reason?: string;
}

export interface MarketSubscribeRequest extends BaseEvent {
    topics: string[]; // например: ['tickers.BTCUSDT']
    venue?: VenueId;
    marketType?: MarketType;
}

export interface MarketConnected extends BaseEvent {
    url?: string;
    details?: Record<string, unknown>;
}

export interface MarketDisconnected extends BaseEvent {
    reason?: string;
    details?: Record<string, unknown>;
}

export interface MarketErrorEvent extends BaseEvent {
    phase: 'connect' | 'disconnect' | 'subscribe' | 'unknown';
    message: string;
    error?: unknown;
}

export interface MarketResyncRequested extends BaseEvent {
    venue: VenueId;
    symbol: string;
    channel: 'orderbook';
    reason: 'gap' | 'out_of_order' | 'snapshot_missing' | 'sequence_reset' | 'crc_mismatch' | 'unknown';
    streamId?: string;
    lastSequence?: number;
    expectedSequence?: number;
    details?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Market bootstrap (kline history)
// ---------------------------------------------------------------------------

export interface KlineBootstrapRequested extends BaseEvent {
    symbol: string;
    interval: KlineInterval;
    tf: string;
    limit: number;
    sinceTs?: number;
}

export interface KlineBootstrapCompleted extends BaseEvent {
    symbol: string;
    interval: KlineInterval;
    tf: string;
    received: number;
    emitted: number;
    startTs: number;
    endTs: number;
    durationMs: number;
}

export interface KlineBootstrapFailed extends BaseEvent {
    symbol: string;
    interval: KlineInterval;
    tf: string;
    reason: string;
    error?: unknown;
}

// ---------------------------------------------------------------------------
// Analytics: признаки (features) и контекст рынка
// ---------------------------------------------------------------------------

export interface AnalyticsFeaturesEvent extends BaseEvent {
    symbol: string;
    ts: number; // тот же ts, что и в meta.ts входящего события
    lastPrice: number;
    return1?: number;
    sma20?: number;
    volatility?: number;
    momentum?: number;
    emaFast?: number;
    emaSlow?: number;
    rsi?: number;
    atr?: number;
    sampleCount: number;
    featuresReady: boolean;
    windowSize: number;
    smaPeriod?: number;
    closeTs?: number;
    klineTf?: string;
    sourceTopic?: 'market:kline' | 'market:ticker';
}

export type MarketRegime = 'unknown' | 'calm' | 'volatile';
export type MarketRegimeV2 = 'calm_range' | 'trend_bull' | 'trend_bear' | 'storm';

export interface MarketContextEvent extends BaseEvent {
    symbol: string;
    ts: number;
    regime: MarketRegime;
    regimeV2?: MarketRegimeV2;
    volatility?: number;
    atr?: number;
    atrPct?: number;
    featuresReady: boolean;
    contextReady: boolean;
    processedCount: number;
    tf?: string;
    sourceTopic?: 'market:kline' | 'market:ticker';
}

export type AnalyticsReadyReason = 'tickerWarmup' | 'klineWarmup' | 'macroWarmup';

export interface AnalyticsReadyEvent extends BaseEvent {
    symbol: string;
    ts: number;
    tf?: string;
    ready: boolean;
    reason: AnalyticsReadyReason;
    readyTfs?: string[];
}

export type FlowRegime = 'buyPressure' | 'sellPressure' | 'neutral';

export interface AnalyticsFlowEvent extends BaseEvent {
    symbol: string;
    ts: number;
    cvdSpot?: number;
    cvdFutures?: number;
    oi?: number;
    oiDelta?: number;
    fundingRate?: number;
    flowRegime?: FlowRegime;
}

export interface AnalyticsLiquidityEvent extends BaseEvent {
    symbol: string;
    ts: number;
    bestBid?: number;
    bestAsk?: number;
    spread?: number;
    depthBid?: number;
    depthAsk?: number;
    imbalance?: number;
}

export interface MarketViewMicro {
    tf?: string;
    close?: number;
    indexPrice?: number;
    emaFast?: number;
    emaSlow?: number;
    rsi?: number;
    atr?: number;
}

export interface MarketViewMacro {
    tf?: string;
    close?: number;
    indexPrice?: number;
    emaFast?: number;
    emaSlow?: number;
    atr?: number;
    atrPct?: number;
}

export interface MarketViewFlow {
    cvdAgg?: number;
    cvdSpotAgg?: number;
    cvdFuturesAgg?: number;
    oiAgg?: number;
    fundingAgg?: number;
    liquidationsAgg?: number;
    spotVolumeAgg?: number;
    futuresVolumeAgg?: number;
}

export interface MarketViewLiquidity {
    bestBid?: number;
    bestAsk?: number;
    spread?: number;
    depthBid?: number;
    depthAsk?: number;
    imbalance?: number;
}

export interface AnalyticsMarketViewEvent extends BaseEvent {
    symbol: string;
    ts: number;
    micro?: MarketViewMicro;
    macro?: MarketViewMacro;
    flow?: MarketViewFlow;
    liquidity?: MarketViewLiquidity;
    sourceTopics?: string[];
}

export interface AnalyticsRegimeEvent extends BaseEvent {
    symbol: string;
    ts: number;
    regime: MarketRegime;
    regimeV2?: MarketRegimeV2;
    confidence?: number;
    sourceTopic?: 'analytics:market_view';
}

export interface AnalyticsRegimeExplainEvent extends BaseEvent {
    symbol: string;
    ts: number;
    regime: MarketRegime;
    regimeV2?: MarketRegimeV2;
    factors: Record<string, string | number | boolean | undefined>;
    sourceTopic?: 'analytics:market_view';
}

// ---------------------------------------------------------------------------
// Storage/Recovery + Errors
// ---------------------------------------------------------------------------

export interface StateSnapshot extends BaseEvent {
    // snapshot может включать режим, позиции, последние свечи и т.д.
    data: Record<string, unknown>;
}

export interface BotErrorEvent extends BaseEvent {
    scope: 'system' | 'ws' | 'rest' | 'market' | 'analytics' | 'strategy' | 'risk' | 'trading';
    message: string;
    error?: unknown;
    details?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Data quality / Storage signals
// ---------------------------------------------------------------------------

export interface DataGapDetected extends BaseEvent {
    symbol: string;
    streamId: string;
    topic: 'market:ticker' | 'market:kline' | 'market:trade' | 'market:orderbook_l2_snapshot' | 'market:orderbook_l2_delta' | 'market:oi' | 'market:funding';
    prevTsExchange?: number;
    currTsExchange?: number;
    deltaMs?: number;
}

export interface DataOutOfOrder extends BaseEvent {
    symbol: string;
    streamId: string;
    topic: 'market:ticker' | 'market:kline' | 'market:trade' | 'market:orderbook_l2_snapshot' | 'market:orderbook_l2_delta' | 'market:oi' | 'market:funding';
    prevTsExchange?: number;
    currTsExchange?: number;
}

export interface DataTimeOutOfOrder extends BaseEvent {
    symbol: string;
    streamId: string;
    topic: 'market:ticker' | 'market:kline' | 'market:trade' | 'market:orderbook_l2_snapshot' | 'market:orderbook_l2_delta' | 'market:oi' | 'market:funding';
    prevTs: TsMs;
    currTs: TsMs;
    tsSource: 'event' | 'exchange';
}

export interface DataSequenceGapOrOutOfOrder extends BaseEvent {
    symbol: string;
    streamId: string;
    topic: 'market:orderbook_l2_snapshot' | 'market:orderbook_l2_delta' | 'market:trade' | 'market:oi' | 'market:funding';
    prevSeq?: Seq;
    currSeq?: Seq;
    kind: 'gap' | 'out_of_order' | 'duplicate';
}

export interface LatencySpikeDetected extends BaseEvent {
    symbol: string;
    streamId: string;
    topic: 'market:ticker' | 'market:trade' | 'market:orderbook_l2_snapshot' | 'market:orderbook_l2_delta' | 'market:oi' | 'market:funding';
    latencyMs: number;
    tsExchange?: number;
    tsIngest: number;
    thresholdMs: number;
}

export interface DataDuplicateDetected extends BaseEvent {
    symbol: string;
    streamId: string;
    topic: 'market:ticker' | 'market:kline' | 'market:trade' | 'market:orderbook_l2_snapshot' | 'market:orderbook_l2_delta' | 'market:oi' | 'market:funding';
    key?: string;
    tsExchange?: number;
}

export interface DataSourceDegraded extends BaseEvent {
    sourceId: string;
    reason: string;
    lastSuccessTs?: number;
    consecutiveErrors?: number;
}

export interface DataSourceRecovered extends BaseEvent {
    sourceId: string;
    recoveredTs: number;
    lastErrorTs?: number;
}

export interface DataStale extends BaseEvent {
    sourceId: string;
    topic: string;
    symbol?: string;
    lastTs: number;
    currentTs: number;
    expectedMs?: number;
}

export interface DataMismatch extends BaseEvent {
    symbol: string;
    topic: string;
    ts: number;
    values: Record<string, number>;
    thresholdPct?: number;
}

export interface DataConfidence extends BaseEvent {
    sourceId: string;
    topic: string;
    symbol: string;
    ts: number;
    confidenceScore: number;
    freshSourcesCount?: number;
    staleSourcesDropped?: string[];
    mismatchDetected?: boolean;
}

// ---------------------------------------------------------------------------
// System readiness / Market data status
// ---------------------------------------------------------------------------

export type ReadinessBlock = 'price' | 'flow' | 'liquidity' | 'derivatives';

export interface MarketDataStatusPayload extends BaseEvent {
    overallConfidence: number;
    blockConfidence: Record<ReadinessBlock, number>;
    degraded: boolean;
    degradedReasons: string[];
    warnings?: string[];
    warmingUp: boolean;
    warmingProgress: number;
    warmingWindowMs: number;
    activeSources: number;
    expectedSources: number;
    activeSourcesAgg: number;
    activeSourcesRaw: number;
    expectedSourcesAgg: number;
    expectedSourcesRaw: number;
    lastBucketTs: number;
}

export interface StorageWriteFailed extends BaseEvent {
    path: string;
    error: unknown;
}

// ---------------------------------------------------------------------------
// Replay lifecycle
// ---------------------------------------------------------------------------

export interface ReplayStarted extends BaseEvent {
    streamId: string;
    symbol: string;
    filesCount: number;
    dateFrom?: string;
    dateTo?: string;
    mode: 'max' | 'accelerated' | 'realtime';
    speedFactor?: number;
}

export interface ReplayProgress extends BaseEvent {
    streamId: string;
    symbol: string;
    emittedCount: number;
    warningsCount: number;
    currentFile?: string;
    lastSeq?: number;
    lastTsIngest?: number;
}

export interface ReplayFinished extends BaseEvent {
    streamId: string;
    symbol: string;
    emittedCount: number;
    warningCount: number;
    durationMs: number;
    filesProcessed: number;
}

export interface ReplayWarning extends BaseEvent {
    file: string;
    lineNumber: number;
    reason: string;
}

export interface ReplayError extends BaseEvent {
    message: string;
    file?: string;
    error?: unknown;
}

// ---------------------------------------------------------------------------
// Research + Walk-forward
// ---------------------------------------------------------------------------

export interface ResearchPatternFound extends BaseEvent {
    symbol: string;
    ts: number;
    patternId: string;
    score: number;
    window: number;
    sourceTopic?: 'analytics:features' | 'analytics:market_view';
}

export interface ResearchPatternStats extends BaseEvent {
    symbol: string;
    ts: number;
    patternId: string;
    occurrences: number;
    avgReturn?: number;
    winRate?: number;
}

export interface WalkForwardSummary extends BaseEvent {
    runId: string;
    symbol: string;
    folds: number;
    trainWindowMs: number;
    testWindowMs: number;
    stepMs: number;
    startTs: number;
    endTs: number;
}

// ---------------------------------------------------------------------------
// Research / Discovery plane
// ---------------------------------------------------------------------------

export type DriftSeverity = 'low' | 'medium' | 'high';

export interface MarketDriftDetected extends BaseEvent {
    symbol: string;
    window: {
        currentSize: number;
        baselineSize: number;
    };
    stats: Record<string, { meanDelta?: number; varDelta?: number }>;
    severity: DriftSeverity;
}

export interface FeatureVectorRecorded extends BaseEvent {
    symbol: string;
    timeframe?: string;
    featureVersion?: string;
    features: Record<string, number>;
}

export interface OutcomeRecorded extends BaseEvent {
    symbol: string;
    horizonMs: number;
    outcome: {
        pnl?: number;
        mae?: number;
        mfe?: number;
        hit?: boolean;
    };
    context?: Record<string, unknown>;
}

export interface NewClusterFound extends BaseEvent {
    clusterId: string;
    summary?: Record<string, unknown>;
}

export interface CandidateScenarioBuilt extends BaseEvent {
    scenarioId: string;
    source?: { clusterId?: string; ruleId?: string };
    entry: Record<string, unknown>;
    exit: Record<string, unknown>;
    riskDefaults?: Record<string, unknown>;
}

export interface BacktestCompleted extends BaseEvent {
    scenarioId: string;
    period: { from: number; to: number };
    metrics: { sharpe?: number; ddMax?: number; ev?: number; hitRate?: number; trades?: number };
    verdict: 'pass' | 'fail';
    notes?: string;
}

export interface ScenarioApproved extends BaseEvent {
    scenarioId: string;
    constraints?: Record<string, unknown>;
}

export interface ScenarioRejected extends BaseEvent {
    scenarioId: string;
    reason: string;
}

export interface ScenarioEnabled extends BaseEvent {
    scenarioId: string;
    reason?: string;
}

export interface ScenarioDisabled extends BaseEvent {
    scenarioId: string;
    reason?: string;
}

export interface RollbackRequested extends BaseEvent {
    reason?: string;
    targetScenarioId?: string;
}

// ---------------------------------------------------------------------------
// Карта всех событий бота.
//
// ВАЖНО: EventEmitter3 лучше всего типизируется через map "topic -> args tuple".
// Тогда:
// - emit(topic, ...args) типобезопасен
// - on(topic, (...args) => {}) типобезопасен
//
// Мы сознательно делаем ПООДНОМУ payload-объекту на событие.
// То есть каждый topic несёт 1 аргумент: (payload).
// ---------------------------------------------------------------------------
export type BotEventMap = {
    // Market Data
    'market:ticker': [payload: TickerEvent];
    'market:kline': [payload: KlineEvent];
    'market:trade': [payload: TradeEvent];
    'market:orderbook_l2_snapshot': [payload: OrderbookL2SnapshotEvent];
    'market:orderbook_l2_delta': [payload: OrderbookL2DeltaEvent];
    'market:oi': [payload: OpenInterestEvent];
    'market:funding': [payload: FundingRateEvent];
    'market:liquidation': [payload: LiquidationEvent];
    'market:trade_raw': [payload: TradeRawEvent];
    'market:orderbook_snapshot_raw': [payload: OrderbookSnapshotRawEvent];
    'market:orderbook_delta_raw': [payload: OrderbookDeltaRawEvent];
    'market:candle_raw': [payload: CandleRawEvent];
    'market:mark_price_raw': [payload: MarkPriceRawEvent];
    'market:index_price_raw': [payload: IndexPriceRawEvent];
    'market:funding_raw': [payload: FundingRawEvent];
    'market:open_interest_raw': [payload: OpenInterestRawEvent];
    'market:liquidation_raw': [payload: LiquidationRawEvent];
    'market:ws_event_raw': [payload: WsEventRaw];
    'market:oi_agg': [payload: MarketOpenInterestAggEvent];
    'market:funding_agg': [payload: MarketFundingAggEvent];
    'market:liquidations_agg': [payload: MarketLiquidationsAggEvent];
    'market:volume_agg': [payload: MarketVolumeAggEvent];
    'market:cvd_spot': [payload: MarketCvdEvent];
    'market:cvd_futures': [payload: MarketCvdEvent];
    'market:cvd_agg': [payload: MarketCvdAggEvent];
    'market:cvd_spot_agg': [payload: MarketCvdAggEvent];
    'market:cvd_futures_agg': [payload: MarketCvdAggEvent];
    'market:price_index': [payload: MarketPriceIndexEvent];
    'market:price_canonical': [payload: MarketPriceCanonicalEvent];
    'market:liquidity_agg': [payload: MarketLiquidityAggEvent];

    // Control Plane
    'control:command': [payload: ControlCommand];
    'control:state': [payload: ControlState];

    // Analytics
    'analytics:features': [payload: AnalyticsFeaturesEvent];
    'analytics:context': [payload: MarketContextEvent];
    'analytics:ready': [payload: AnalyticsReadyEvent];
    'analytics:flow': [payload: AnalyticsFlowEvent];
    'analytics:liquidity': [payload: AnalyticsLiquidityEvent];
    'analytics:market_view': [payload: AnalyticsMarketViewEvent];
    'analytics:regime': [payload: AnalyticsRegimeEvent];
    'analytics:regime_explain': [payload: AnalyticsRegimeExplainEvent];

    // Strategy
    'strategy:intent': [payload: StrategyIntentEvent];
    'strategy:signal': [payload: StrategySignalEvent];
    'strategy:state': [payload: StrategyStateEvent];

    // Risk
    'risk:approved_intent': [payload: RiskApprovedIntentEvent];
    'risk:rejected_intent': [payload: RiskRejectedIntentEvent];

    // Trading / Execution
    'exec:paper_fill': [payload: PaperFillEvent];

    // Portfolio
    'portfolio:update': [payload: PortfolioUpdateEvent];
    'portfolio:snapshot': [payload: PortfolioSnapshotEvent];

    // State snapshot / recovery
    'state:snapshot_requested': [payload: SnapshotRequested];
    'state:snapshot_written': [payload: SnapshotWritten];
    'state:recovery_requested': [payload: RecoveryRequested];
    'state:recovery_loaded': [payload: RecoveryLoaded];
    'state:recovery_failed': [payload: RecoveryFailed];

    // Metrics
    'metrics:backtest_summary': [payload: BacktestSummary];
    'metrics:walkforward_summary': [payload: WalkForwardSummary];

    // Market lifecycle (commands + signals)
    'market:connect': [payload: MarketConnectRequest];
    'market:disconnect': [payload: MarketDisconnectRequest];
    'market:subscribe': [payload: MarketSubscribeRequest];
    'market:connected': [payload: MarketConnected];
    'market:disconnected': [payload: MarketDisconnected];
    'market:error': [payload: MarketErrorEvent];
    'market:resync_requested': [payload: MarketResyncRequested];
    'market:kline_bootstrap_requested': [payload: KlineBootstrapRequested];
    'market:kline_bootstrap_completed': [payload: KlineBootstrapCompleted];
    'market:kline_bootstrap_failed': [payload: KlineBootstrapFailed];

    // Storage / Recovery
    'state:snapshot': [payload: StateSnapshot];
    'state:recovery': [payload: StateSnapshot];

    // Data quality / Storage
    'data:gapDetected': [payload: DataGapDetected];
    'data:outOfOrder': [payload: DataOutOfOrder];
    'data:time_out_of_order': [payload: DataTimeOutOfOrder];
    'data:sequence_gap_or_out_of_order': [payload: DataSequenceGapOrOutOfOrder];
    'data:latencySpike': [payload: LatencySpikeDetected];
    'data:duplicateDetected': [payload: DataDuplicateDetected];
    'data:sourceDegraded': [payload: DataSourceDegraded];
    'data:sourceRecovered': [payload: DataSourceRecovered];
    'data:stale': [payload: DataStale];
    'data:mismatch': [payload: DataMismatch];
    'data:confidence': [payload: DataConfidence];
    'system:market_data_status': [payload: MarketDataStatusPayload];
    'storage:writeFailed': [payload: StorageWriteFailed];

    // Replay lifecycle
    'replay:started': [payload: ReplayStarted];
    'replay:progress': [payload: ReplayProgress];
    'replay:finished': [payload: ReplayFinished];
    'replay:warning': [payload: ReplayWarning];
    'replay:error': [payload: ReplayError];

    // Errors
    'error:event': [payload: BotErrorEvent];

    // Research / Discovery
    'analytics:marketDriftDetected': [payload: MarketDriftDetected];
    'research:featureVectorRecorded': [payload: FeatureVectorRecorded];
    'research:outcomeRecorded': [payload: OutcomeRecorded];
    'research:patternFound': [payload: ResearchPatternFound];
    'research:patternStats': [payload: ResearchPatternStats];
    'research:newClusterFound': [payload: NewClusterFound];
    'research:candidateScenarioBuilt': [payload: CandidateScenarioBuilt];
    'research:backtestCompleted': [payload: BacktestCompleted];
    'research:scenarioApproved': [payload: ScenarioApproved];
    'research:scenarioRejected': [payload: ScenarioRejected];
    'strategy:scenarioEnabled': [payload: ScenarioEnabled];
    'strategy:scenarioDisabled': [payload: ScenarioDisabled];
    'control:rollbackModel': [payload: RollbackRequested];
};

export type BotEventName = keyof BotEventMap;

// Типизированный EventBus (EventEmitter3 поддерживает generic map)
export class EventBus extends EventEmitter<BotEventMap> {
    /**
     * Явный alias для emit, чтобы в коде читалось как pub/sub.
     *
     * Мы сознательно делаем 1 payload на событие (а не несколько аргументов),
     * чтобы типизация была стабильной и не ломалась на union tuple.
     */
    public publish<T extends BotEventName>(topic: T, payload: BotEventMap[T][0]): boolean {
        // Небольшой каст нужен из-за пересечения union tuple в типах eventemitter3
        return (super.emit as any)(topic, payload);
    }

    /**
     * Явный alias для on, чтобы в коде читалось как pub/sub.
     */
    public subscribe<T extends BotEventName>(topic: T, handler: (...args: BotEventMap[T]) => void): this {
        return super.on(topic, handler);
    }

    /**
     * Удобное отписывание.
     */
    public unsubscribe<T extends BotEventName>(topic: T, handler: (...args: BotEventMap[T]) => void): this {
        return super.off(topic, handler);
    }
}

export const eventBus = new EventBus();
