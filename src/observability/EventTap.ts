import { logger } from '../infra/logger';
import {
  eventBus as defaultEventBus,
  type AnalyticsFeaturesEvent,
  type AnalyticsFlowEvent,
  type AnalyticsLiquidityEvent,
  type AnalyticsMarketViewEvent,
  type AnalyticsReadyEvent,
  type AnalyticsRegimeEvent,
  type AnalyticsRegimeExplainEvent,
  type EventBus,
  type EventMeta,
  type KlineBootstrapCompleted,
  type KlineEvent,
  type MarketCvdAggEvent,
  type MarketContextEvent,
  type MarketFundingAggEvent,
  type MarketLiquidationsAggEvent,
  type MarketLiquidityAggEvent,
  type MarketOpenInterestAggEvent,
  type MarketPriceIndexEvent,
  type MarketVolumeAggEvent,
  type OpenInterestEvent,
  type OrderbookL2DeltaEvent,
  type OrderbookL2SnapshotEvent,
  type PaperFillEvent,
  type PortfolioUpdateEvent,
  type ReplayFinished,
  type ReplayStarted,
  type RiskApprovedIntentEvent,
  type RiskRejectedIntentEvent,
  type StrategyIntentEvent,
  type FundingRateEvent,
  type TradeEvent,
  type TickerEvent,
} from '../core/events/EventBus';

export interface EventTapOptions {
  summaryIntervalMs?: number;
  summaryThrottleMs?: number;
  trackTicks?: boolean;
}

export interface EventTapCounters {
  ticksSeen: number;
  klinesSeen: number;
  klinesByTf: Record<string, number>;
  tradesSeen: number;
  orderbookSnapshotsSeen: number;
  orderbookDeltasSeen: number;
  oiUpdatesSeen: number;
  fundingUpdatesSeen: number;
  featuresEmitted: number;
  contextsEmitted: number;
  flowsEmitted: number;
  liquidityEmitted: number;
  marketViewSeen: number;
  regimeSeen: number;
  regimeExplainSeen: number;
  oiAggSeen: number;
  fundingAggSeen: number;
  liquidationsAggSeen: number;
  liquidityAggSeen: number;
  volumeAggSeen: number;
  cvdSpotAggSeen: number;
  cvdFuturesAggSeen: number;
  priceIndexSeen: number;
  intents: number;
  approvals: number;
  rejections: number;
  fills: number;
  portfolioUpdates: number;
  klineBootstrapsCompleted: number;
}

export class EventTap {
  private readonly bus: EventBus;
  private readonly opts: Required<EventTapOptions>;
  private started = false;
  private readonly unsubscribers: Array<() => void> = [];
  private summaryTimer?: NodeJS.Timeout;

  private counters: EventTapCounters = this.freshCounters();
  private lastMeta?: EventMeta;

  private readonly readySymbols = new Set<string>();
  private readonly intentSymbols = new Set<string>();
  private readonly riskDecisionSymbols = new Set<string>();
  private readonly fillSymbols = new Set<string>();
  private readonly klineSymbols = new Set<string>();
  private readonly bootstrapKeys = new Set<string>();
  private readonly tradeSymbols = new Set<string>();
  private readonly orderbookSymbols = new Set<string>();
  private readonly oiSymbols = new Set<string>();
  private readonly fundingSymbols = new Set<string>();

  constructor(bus: EventBus = defaultEventBus, opts: EventTapOptions = {}) {
    const summaryIntervalMs = opts.summaryIntervalMs ?? 10_000;
    this.bus = bus;
    this.opts = {
      summaryIntervalMs,
      summaryThrottleMs: opts.summaryThrottleMs ?? summaryIntervalMs,
      trackTicks: opts.trackTicks ?? true,
    };
  }

  start(): void {
    if (this.started) return;

    if (this.opts.trackTicks) {
      const onTicker = (evt: TickerEvent) => this.onTicker(evt);
      this.bus.subscribe('market:ticker', onTicker);
      this.unsubscribers.push(() => this.bus.unsubscribe('market:ticker', onTicker));
    }

    const onKline = (evt: KlineEvent) => this.onKline(evt);
    this.bus.subscribe('market:kline', onKline);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:kline', onKline));

    const onTrade = (evt: TradeEvent) => this.onTrade(evt);
    this.bus.subscribe('market:trade', onTrade);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:trade', onTrade));

    const onOrderbookSnapshot = (evt: OrderbookL2SnapshotEvent) => this.onOrderbookSnapshot(evt);
    this.bus.subscribe('market:orderbook_l2_snapshot', onOrderbookSnapshot);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:orderbook_l2_snapshot', onOrderbookSnapshot));

    const onOrderbookDelta = (evt: OrderbookL2DeltaEvent) => this.onOrderbookDelta(evt);
    this.bus.subscribe('market:orderbook_l2_delta', onOrderbookDelta);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:orderbook_l2_delta', onOrderbookDelta));

    const onOi = (evt: OpenInterestEvent) => this.onOpenInterest(evt);
    this.bus.subscribe('market:oi', onOi);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:oi', onOi));

    const onFunding = (evt: FundingRateEvent) => this.onFunding(evt);
    this.bus.subscribe('market:funding', onFunding);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:funding', onFunding));

    const onOiAgg = (evt: MarketOpenInterestAggEvent) => this.onOiAgg(evt);
    this.bus.subscribe('market:oi_agg', onOiAgg);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:oi_agg', onOiAgg));

    const onFundingAgg = (evt: MarketFundingAggEvent) => this.onFundingAgg(evt);
    this.bus.subscribe('market:funding_agg', onFundingAgg);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:funding_agg', onFundingAgg));

    const onLiquidationsAgg = (evt: MarketLiquidationsAggEvent) => this.onLiquidationsAgg(evt);
    this.bus.subscribe('market:liquidations_agg', onLiquidationsAgg);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:liquidations_agg', onLiquidationsAgg));

    const onLiquidityAgg = (evt: MarketLiquidityAggEvent) => this.onLiquidityAgg(evt);
    this.bus.subscribe('market:liquidity_agg', onLiquidityAgg);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:liquidity_agg', onLiquidityAgg));

    const onVolumeAgg = (evt: MarketVolumeAggEvent) => this.onVolumeAgg(evt);
    this.bus.subscribe('market:volume_agg', onVolumeAgg);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:volume_agg', onVolumeAgg));

    const onCvdSpotAgg = (evt: MarketCvdAggEvent) => this.onCvdSpotAgg(evt);
    this.bus.subscribe('market:cvd_spot_agg', onCvdSpotAgg);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:cvd_spot_agg', onCvdSpotAgg));

    const onCvdFuturesAgg = (evt: MarketCvdAggEvent) => this.onCvdFuturesAgg(evt);
    this.bus.subscribe('market:cvd_futures_agg', onCvdFuturesAgg);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:cvd_futures_agg', onCvdFuturesAgg));

    const onPriceIndex = (evt: MarketPriceIndexEvent) => this.onPriceIndex(evt);
    this.bus.subscribe('market:price_index', onPriceIndex);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:price_index', onPriceIndex));

    const onBootstrapCompleted = (evt: KlineBootstrapCompleted) => this.onBootstrapCompleted(evt);
    this.bus.subscribe('market:kline_bootstrap_completed', onBootstrapCompleted);
    this.unsubscribers.push(() => this.bus.unsubscribe('market:kline_bootstrap_completed', onBootstrapCompleted));

    const onReady = (evt: AnalyticsReadyEvent) => this.onAnalyticsReady(evt);
    this.bus.subscribe('analytics:ready', onReady);
    this.unsubscribers.push(() => this.bus.unsubscribe('analytics:ready', onReady));

    const onFeatures = (evt: AnalyticsFeaturesEvent) => this.onFeatures(evt);
    this.bus.subscribe('analytics:features', onFeatures);
    this.unsubscribers.push(() => this.bus.unsubscribe('analytics:features', onFeatures));

    const onFlow = (evt: AnalyticsFlowEvent) => this.onFlow(evt);
    this.bus.subscribe('analytics:flow', onFlow);
    this.unsubscribers.push(() => this.bus.unsubscribe('analytics:flow', onFlow));

    const onLiquidity = (evt: AnalyticsLiquidityEvent) => this.onLiquidity(evt);
    this.bus.subscribe('analytics:liquidity', onLiquidity);
    this.unsubscribers.push(() => this.bus.unsubscribe('analytics:liquidity', onLiquidity));

    const onMarketView = (evt: AnalyticsMarketViewEvent) => this.onMarketView(evt);
    this.bus.subscribe('analytics:market_view', onMarketView);
    this.unsubscribers.push(() => this.bus.unsubscribe('analytics:market_view', onMarketView));

    const onRegime = (evt: AnalyticsRegimeEvent) => this.onRegime(evt);
    this.bus.subscribe('analytics:regime', onRegime);
    this.unsubscribers.push(() => this.bus.unsubscribe('analytics:regime', onRegime));

    const onRegimeExplain = (evt: AnalyticsRegimeExplainEvent) => this.onRegimeExplain(evt);
    this.bus.subscribe('analytics:regime_explain', onRegimeExplain);
    this.unsubscribers.push(() => this.bus.unsubscribe('analytics:regime_explain', onRegimeExplain));

    const onContext = (evt: MarketContextEvent) => this.onContext(evt);
    this.bus.subscribe('analytics:context', onContext);
    this.unsubscribers.push(() => this.bus.unsubscribe('analytics:context', onContext));

    const onIntent = (evt: StrategyIntentEvent) => this.onIntent(evt);
    this.bus.subscribe('strategy:intent', onIntent);
    this.unsubscribers.push(() => this.bus.unsubscribe('strategy:intent', onIntent));

    const onApproved = (evt: RiskApprovedIntentEvent) => this.onApproved(evt);
    this.bus.subscribe('risk:approved_intent', onApproved);
    this.unsubscribers.push(() => this.bus.unsubscribe('risk:approved_intent', onApproved));

    const onRejected = (evt: RiskRejectedIntentEvent) => this.onRejected(evt);
    this.bus.subscribe('risk:rejected_intent', onRejected);
    this.unsubscribers.push(() => this.bus.unsubscribe('risk:rejected_intent', onRejected));

    const onFill = (evt: PaperFillEvent) => this.onFill(evt);
    this.bus.subscribe('exec:paper_fill', onFill);
    this.unsubscribers.push(() => this.bus.unsubscribe('exec:paper_fill', onFill));

    const onPortfolio = (evt: PortfolioUpdateEvent) => this.onPortfolio(evt);
    this.bus.subscribe('portfolio:update', onPortfolio);
    this.unsubscribers.push(() => this.bus.unsubscribe('portfolio:update', onPortfolio));

    const onReplayStarted = (evt: ReplayStarted) => this.onReplayStarted(evt);
    const onReplayFinished = (evt: ReplayFinished) => this.onReplayFinished(evt);
    this.bus.subscribe('replay:started', onReplayStarted);
    this.bus.subscribe('replay:finished', onReplayFinished);
    this.unsubscribers.push(
      () => this.bus.unsubscribe('replay:started', onReplayStarted),
      () => this.bus.unsubscribe('replay:finished', onReplayFinished)
    );

    if (this.opts.summaryIntervalMs > 0) {
      this.summaryTimer = setInterval(() => {
        this.emitSummary();
      }, this.opts.summaryIntervalMs);
    }

    this.started = true;
  }

  stop(): void {
    if (!this.started) return;
    this.unsubscribers.forEach((fn) => fn());
    this.unsubscribers.length = 0;
    if (this.summaryTimer) {
      clearInterval(this.summaryTimer);
      this.summaryTimer = undefined;
    }
    this.started = false;
  }

  getCounters(): EventTapCounters {
    return { ...this.counters };
  }

  emitSummary(): void {
    const msg = this.formatSummary();
    const throttleMs = Math.max(0, this.opts.summaryThrottleMs);
    if (throttleMs > 0) {
      logger.infoThrottled('eventtap:summary', msg, throttleMs);
    } else {
      logger.info(msg);
    }
  }

  private onReplayStarted(evt: ReplayStarted): void {
    this.lastMeta = evt.meta;
    this.reset();
  }

  private onReplayFinished(evt: ReplayFinished): void {
    this.lastMeta = evt.meta;
    if (this.opts.summaryIntervalMs <= 0) {
      this.emitSummary();
    }
  }

  private onTicker(evt: TickerEvent): void {
    this.lastMeta = evt.meta;
    this.counters.ticksSeen += 1;
  }

  private onKline(evt: KlineEvent): void {
    this.lastMeta = evt.meta;
    this.counters.klinesSeen += 1;
    const tf = evt.tf ?? 'unknown';
    this.counters.klinesByTf[tf] = (this.counters.klinesByTf[tf] ?? 0) + 1;
    this.logOnce(this.klineSymbols, evt.symbol, '[Market] first kline', evt.meta);
  }

  private onTrade(evt: TradeEvent): void {
    this.lastMeta = evt.meta;
    this.counters.tradesSeen += 1;
    this.logOnce(this.tradeSymbols, evt.symbol, '[Market] first trade', evt.meta);
  }

  private onOrderbookSnapshot(evt: OrderbookL2SnapshotEvent): void {
    this.lastMeta = evt.meta;
    this.counters.orderbookSnapshotsSeen += 1;
    this.logOnce(this.orderbookSymbols, evt.symbol, '[Market] first orderbook snapshot', evt.meta);
  }

  private onOrderbookDelta(evt: OrderbookL2DeltaEvent): void {
    this.lastMeta = evt.meta;
    this.counters.orderbookDeltasSeen += 1;
  }

  private onOpenInterest(evt: OpenInterestEvent): void {
    this.lastMeta = evt.meta;
    this.counters.oiUpdatesSeen += 1;
    this.logOnce(this.oiSymbols, evt.symbol, '[Market] first open interest', evt.meta);
  }

  private onFunding(evt: FundingRateEvent): void {
    this.lastMeta = evt.meta;
    this.counters.fundingUpdatesSeen += 1;
    this.logOnce(this.fundingSymbols, evt.symbol, '[Market] first funding', evt.meta);
  }

  private onOiAgg(evt: MarketOpenInterestAggEvent): void {
    this.lastMeta = evt.meta;
    this.counters.oiAggSeen += 1;
  }

  private onFundingAgg(evt: MarketFundingAggEvent): void {
    this.lastMeta = evt.meta;
    this.counters.fundingAggSeen += 1;
  }

  private onLiquidationsAgg(evt: MarketLiquidationsAggEvent): void {
    this.lastMeta = evt.meta;
    this.counters.liquidationsAggSeen += 1;
  }

  private onLiquidityAgg(evt: MarketLiquidityAggEvent): void {
    this.lastMeta = evt.meta;
    this.counters.liquidityAggSeen += 1;
  }

  private onVolumeAgg(evt: MarketVolumeAggEvent): void {
    this.lastMeta = evt.meta;
    this.counters.volumeAggSeen += 1;
  }

  private onCvdSpotAgg(evt: MarketCvdAggEvent): void {
    this.lastMeta = evt.meta;
    this.counters.cvdSpotAggSeen += 1;
  }

  private onCvdFuturesAgg(evt: MarketCvdAggEvent): void {
    this.lastMeta = evt.meta;
    this.counters.cvdFuturesAggSeen += 1;
  }

  private onPriceIndex(evt: MarketPriceIndexEvent): void {
    this.lastMeta = evt.meta;
    this.counters.priceIndexSeen += 1;
  }

  private onBootstrapCompleted(evt: KlineBootstrapCompleted): void {
    this.lastMeta = evt.meta;
    this.counters.klineBootstrapsCompleted += 1;
    const key = `${evt.symbol}:${evt.tf}`;
    if (this.bootstrapKeys.has(key)) return;
    this.bootstrapKeys.add(key);
    logger.info(
      `[KlineBootstrap] completed symbol=${evt.symbol} tf=${evt.tf} interval=${evt.interval} received=${evt.received} emitted=${evt.emitted} range=${evt.startTs}-${evt.endTs} durationMs=${evt.durationMs}`
    );
  }

  private onFeatures(evt: AnalyticsFeaturesEvent): void {
    this.lastMeta = evt.meta;
    this.counters.featuresEmitted += 1;
  }

  private onFlow(evt: AnalyticsFlowEvent): void {
    this.lastMeta = evt.meta;
    this.counters.flowsEmitted += 1;
  }

  private onLiquidity(evt: AnalyticsLiquidityEvent): void {
    this.lastMeta = evt.meta;
    this.counters.liquidityEmitted += 1;
  }

  private onMarketView(evt: AnalyticsMarketViewEvent): void {
    this.lastMeta = evt.meta;
    this.counters.marketViewSeen += 1;
  }

  private onRegime(evt: AnalyticsRegimeEvent): void {
    this.lastMeta = evt.meta;
    this.counters.regimeSeen += 1;
  }

  private onRegimeExplain(evt: AnalyticsRegimeExplainEvent): void {
    this.lastMeta = evt.meta;
    this.counters.regimeExplainSeen += 1;
  }

  private onContext(evt: MarketContextEvent): void {
    this.lastMeta = evt.meta;
    this.counters.contextsEmitted += 1;
  }

  private onAnalyticsReady(evt: AnalyticsReadyEvent): void {
    this.lastMeta = evt.meta;
    this.logOnce(this.readySymbols, evt.symbol, '[Analytics] READY', evt.meta);
  }

  private onIntent(evt: StrategyIntentEvent): void {
    this.lastMeta = evt.meta;
    this.counters.intents += 1;
    this.logOnce(this.intentSymbols, evt.symbol, '[Strategy] first intent', evt.meta);
  }

  private onApproved(evt: RiskApprovedIntentEvent): void {
    this.lastMeta = evt.meta;
    this.counters.approvals += 1;
    this.logRiskDecision(evt.intent.symbol, true, evt.meta);
  }

  private onRejected(evt: RiskRejectedIntentEvent): void {
    this.lastMeta = evt.meta;
    this.counters.rejections += 1;
    this.logRiskDecision(evt.intent.symbol, false, evt.meta);
  }

  private onFill(evt: PaperFillEvent): void {
    this.lastMeta = evt.meta;
    this.counters.fills += 1;
    this.logOnce(this.fillSymbols, evt.symbol, '[Exec] first fill', evt.meta);
  }

  private onPortfolio(evt: PortfolioUpdateEvent): void {
    this.lastMeta = evt.meta;
    this.counters.portfolioUpdates += 1;
  }

  private logRiskDecision(symbol: string, approved: boolean, meta: EventMeta): void {
    if (this.riskDecisionSymbols.has(symbol)) return;
    this.riskDecisionSymbols.add(symbol);
    const verdict = approved ? 'approved' : 'rejected';
    this.logLine(`[Risk] first decision`, symbol, meta, verdict);
  }

  private logOnce(set: Set<string>, symbol: string, prefix: string, meta: EventMeta): void {
    if (set.has(symbol)) return;
    set.add(symbol);
    this.logLine(prefix, symbol, meta);
  }

  private logLine(prefix: string, symbol: string, meta: EventMeta, extra?: string): void {
    const parts = [`${prefix} symbol=${symbol}`];
    if (extra) parts.push(extra);
    parts.push(`ts=${meta.ts}`);
    if (meta.correlationId) parts.push(`corr=${meta.correlationId}`);
    logger.info(parts.join(' '));
  }

  private formatSummary(): string {
    const meta = this.lastMeta;
    const parts = [
      `[EventTap] counters`,
      `ticks=${this.counters.ticksSeen}`,
      `klines=${this.counters.klinesSeen}`,
      `trades=${this.counters.tradesSeen}`,
      `orderbookSnapshots=${this.counters.orderbookSnapshotsSeen}`,
      `orderbookDeltas=${this.counters.orderbookDeltasSeen}`,
      `oi=${this.counters.oiUpdatesSeen}`,
      `funding=${this.counters.fundingUpdatesSeen}`,
      `features=${this.counters.featuresEmitted}`,
      `flow=${this.counters.flowsEmitted}`,
      `liquidity=${this.counters.liquidityEmitted}`,
      `marketView=${this.counters.marketViewSeen}`,
      `regime=${this.counters.regimeSeen}`,
      `contexts=${this.counters.contextsEmitted}`,
      `intents=${this.counters.intents}`,
      `approvals=${this.counters.approvals}`,
      `rejections=${this.counters.rejections}`,
      `fills=${this.counters.fills}`,
      `portfolio=${this.counters.portfolioUpdates}`,
      `bootstraps=${this.counters.klineBootstrapsCompleted}`,
      `oiAgg=${this.counters.oiAggSeen}`,
      `fundingAgg=${this.counters.fundingAggSeen}`,
    ];
    if (meta) {
      parts.push(`ts=${meta.ts}`);
      if (meta.correlationId) parts.push(`corr=${meta.correlationId}`);
    }
    return parts.join(' ');
  }

  private reset(): void {
    this.counters = this.freshCounters();
    this.readySymbols.clear();
    this.intentSymbols.clear();
    this.riskDecisionSymbols.clear();
    this.fillSymbols.clear();
    this.klineSymbols.clear();
    this.bootstrapKeys.clear();
    this.tradeSymbols.clear();
    this.orderbookSymbols.clear();
    this.oiSymbols.clear();
    this.fundingSymbols.clear();
  }

  private freshCounters(): EventTapCounters {
    return {
      ticksSeen: 0,
      klinesSeen: 0,
      klinesByTf: {},
      tradesSeen: 0,
      orderbookSnapshotsSeen: 0,
      orderbookDeltasSeen: 0,
      oiUpdatesSeen: 0,
      fundingUpdatesSeen: 0,
      featuresEmitted: 0,
      contextsEmitted: 0,
      flowsEmitted: 0,
      liquidityEmitted: 0,
      marketViewSeen: 0,
      regimeSeen: 0,
      regimeExplainSeen: 0,
      oiAggSeen: 0,
      fundingAggSeen: 0,
      liquidationsAggSeen: 0,
      liquidityAggSeen: 0,
      volumeAggSeen: 0,
      cvdSpotAggSeen: 0,
      cvdFuturesAggSeen: 0,
      priceIndexSeen: 0,
      intents: 0,
      approvals: 0,
      rejections: 0,
      fills: 0,
      portfolioUpdates: 0,
      klineBootstrapsCompleted: 0,
    };
  }
}
