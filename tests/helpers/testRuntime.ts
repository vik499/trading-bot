import path from 'node:path';
import fs from 'node:fs/promises';
import {
  asTsMs,
  createMeta,
  type AnalyticsFlowEvent,
  type AnalyticsLiquidityEvent,
  type AnalyticsFeaturesEvent,
  type AnalyticsReadyEvent,
  type BotEventMap,
  type BotEventName,
  type FundingRateEvent,
  type KlineEvent,
  type KlineInterval,
  type KnownMarketType,
  type MarketContextEvent,
  type OpenInterestEvent,
  type OpenInterestUnit,
  type OrderbookL2DeltaEvent,
  type OrderbookL2SnapshotEvent,
  type OrderbookLevel,
  type PaperFillEvent,
  type PortfolioUpdateEvent,
  type RiskApprovedIntentEvent,
  type RiskRejectedIntentEvent,
  type ReplayFinished,
  type StrategyIntentEvent,
  type TradeEvent,
  type TickerEvent,
} from '../../src/core/events/EventBus';
import { createTestEventBus } from '../../src/core/events/testing';
import { FeatureEngine, KlineFeatureEngine } from '../../src/analytics/FeatureEngine';
import { FlowEngine } from '../../src/analytics/FlowEngine';
import { LiquidityEngine } from '../../src/analytics/LiquidityEngine';
import { MarketContextBuilder } from '../../src/analytics/MarketContextBuilder';
import { StrategyEngine } from '../../src/strategy/StrategyEngine';
import { RiskManager } from '../../src/risk/RiskManager';
import { PaperExecutionEngine } from '../../src/execution/PaperExecutionEngine';
import { PortfolioManager } from '../../src/portfolio/PortfolioManager';
import { createEventJournal, type EventJournal } from '../../src/storage/eventJournal';
import { createJournalReplayRunner, type JournalReplayOptions } from '../../src/replay/JournalReplayRunner';
import { BacktestMetricsCollector } from '../../src/metrics/BacktestMetricsCollector';
import { SnapshotCoordinator } from '../../src/state/SnapshotCoordinator';
import { FakeClock } from './fakeClock';

export interface TestRuntimeOptions {
  baseDir: string;
  runId?: string;
  streamId?: string;
  symbol?: string;
  marketType?: KnownMarketType;
  startJournal?: boolean;
  startAnalytics?: boolean;
  startStrategy?: boolean;
  startRisk?: boolean;
  startExecution?: boolean;
  startPortfolio?: boolean;
  startMetrics?: boolean;
  startSnapshot?: boolean;
}

const DEFAULT_SYMBOL = 'BTCUSDT';
const DEFAULT_STREAM_ID = 'bybit.public.linear.v5';

const TRACKED_TOPICS: BotEventName[] = [
  'market:ticker',
  'market:kline',
  'market:trade',
  'market:orderbook_l2_snapshot',
  'market:orderbook_l2_delta',
  'market:oi',
  'market:funding',
  'analytics:features',
  'analytics:context',
  'analytics:ready',
  'analytics:flow',
  'analytics:liquidity',
  'strategy:intent',
  'risk:approved_intent',
  'risk:rejected_intent',
  'exec:paper_fill',
  'portfolio:update',
  'replay:started',
  'replay:finished',
  'replay:warning',
  'replay:error',
  'storage:writeFailed',
  'data:gapDetected',
  'data:outOfOrder',
  'data:latencySpike',
  'data:duplicateDetected',
];

export class TestRuntime {
  readonly bus = createTestEventBus();
  readonly baseDir: string;
  readonly runId: string;
  readonly streamId: string;
  readonly symbol: string;
  readonly marketType: KnownMarketType;
  readonly clock: FakeClock;

  private readonly events: Record<string, unknown[]> = {};
  private readonly unsubscribers: Array<() => void> = [];
  private readonly stopFns: Array<() => Promise<void> | void> = [];
  private stopped = false;

  private tradeCounter = 0;
  private orderbookUpdateId = 0;

  private journal?: EventJournal;

  constructor(options: TestRuntimeOptions, clock = new FakeClock()) {
    this.baseDir = options.baseDir;
    this.runId = options.runId ?? 'run-test';
    this.streamId = options.streamId ?? DEFAULT_STREAM_ID;
    this.symbol = options.symbol ?? DEFAULT_SYMBOL;
    this.marketType = options.marketType ?? 'futures';
    this.clock = clock;

    this.attachCollectors();
    this.startModules(options);
  }

  private attachCollectors(): void {
    for (const topic of TRACKED_TOPICS) {
      this.events[topic] = [];
      const handler = (payload: BotEventMap[BotEventName][0]) => {
        this.events[topic].push(payload);
      };
      this.bus.subscribe(topic, handler as (...args: BotEventMap[BotEventName]) => void);
      this.unsubscribers.push(() => this.bus.unsubscribe(topic, handler as (...args: BotEventMap[BotEventName]) => void));
    }
  }

  private startModules(options: TestRuntimeOptions): void {
    const startAnalytics = options.startAnalytics ?? true;
    const startStrategy = options.startStrategy ?? true;
    const startRisk = options.startRisk ?? true;
    const startExecution = options.startExecution ?? true;
    const startPortfolio = options.startPortfolio ?? true;
    const startMetrics = options.startMetrics ?? false;
    const startSnapshot = options.startSnapshot ?? false;
    const startJournal = options.startJournal ?? false;

    let portfolio: PortfolioManager | undefined;
    let risk: RiskManager | undefined;
    let strategy: StrategyEngine | undefined;

    if (startAnalytics) {
      const featureEngine = new FeatureEngine(this.bus);
      const klineFeatureEngine = new KlineFeatureEngine(this.bus);
      const flowEngine = new FlowEngine(this.bus);
      const liquidityEngine = new LiquidityEngine(this.bus);
      const contextBuilder = new MarketContextBuilder(this.bus);

      featureEngine.start();
      klineFeatureEngine.start();
      flowEngine.start();
      liquidityEngine.start();
      contextBuilder.start();

      this.stopFns.push(
        () => contextBuilder.stop(),
        () => liquidityEngine.stop(),
        () => flowEngine.stop(),
        () => klineFeatureEngine.stop(),
        () => featureEngine.stop()
      );
    }

    if (startStrategy) {
      strategy = new StrategyEngine(this.bus);
      strategy.start();
      this.stopFns.push(() => strategy?.stop());
    }

    if (startRisk) {
      risk = new RiskManager(this.bus);
      risk.start();
      this.stopFns.push(() => risk?.stop());
    }

    if (startExecution) {
      const execution = new PaperExecutionEngine(this.bus);
      execution.start();
      this.stopFns.push(() => execution.stop());
    }

    if (startPortfolio) {
      portfolio = new PortfolioManager(this.bus);
      portfolio.start();
      this.stopFns.push(() => portfolio?.stop());
    }

    if (startMetrics) {
      const metrics = new BacktestMetricsCollector(this.bus);
      metrics.start();
      this.stopFns.push(() => metrics.stop());
    }

    if (startSnapshot && portfolio && risk && strategy) {
      const snapshot = new SnapshotCoordinator(this.bus, { portfolio, risk, strategy });
      snapshot.start();
      this.stopFns.push(() => snapshot.stop());
    }

    if (startJournal) {
      this.journal = createEventJournal(this.bus, {
        baseDir: this.baseDir,
        streamId: this.streamId,
        runId: this.runId,
        flushIntervalMs: 5,
        maxBatchSize: 1,
      });
      this.journal.start();
      this.stopFns.push(() => this.journal?.stop());
    }
  }

  getEvents<T extends BotEventName>(topic: T): BotEventMap[T][0][] {
    return (this.events[topic] ?? []) as BotEventMap[T][0][];
  }

  clearEvents(): void {
    for (const key of Object.keys(this.events)) {
      this.events[key] = [];
    }
  }

  publishMarketTicker(params: {
    symbol?: string;
    lastPrice?: string;
    exchangeTs?: number;
    ts?: number;
    correlationId?: string;
  } = {}): TickerEvent {
    const ts = params.ts ?? this.clock.next();
    const event: TickerEvent = {
      symbol: params.symbol ?? this.symbol,
      streamId: this.streamId,
      marketType: this.marketType,
      lastPrice: params.lastPrice ?? '100',
      exchangeTs: params.exchangeTs ?? ts,
      meta: createMeta('market', {
        tsEvent: asTsMs(ts),
        tsIngest: asTsMs(ts),
        streamId: this.streamId,
        correlationId: params.correlationId,
      }),
    };
    this.bus.publish('market:ticker', event);
    return event;
  }

  publishMarketKline(params: {
    symbol?: string;
    tf?: string;
    interval?: KlineInterval;
    startTs?: number;
    endTs?: number;
    open?: number;
    high?: number;
    low?: number;
    close?: number;
    volume?: number;
    correlationId?: string;
  } = {}): KlineEvent {
    const tf = params.tf ?? '5m';
    const interval = params.interval ?? tfToInterval(tf);
    const intervalMs = intervalToMs(interval);
    const endTs = params.endTs ?? this.clock.next(intervalMs);
    const startTs = params.startTs ?? endTs - intervalMs;
    const base = params.close ?? 100;
    const event: KlineEvent = {
      symbol: params.symbol ?? this.symbol,
      streamId: this.streamId,
      marketType: this.marketType,
      interval,
      tf,
      startTs,
      endTs,
      open: params.open ?? base,
      high: params.high ?? base + 1,
      low: params.low ?? base - 1,
      close: params.close ?? base,
      volume: params.volume ?? 1,
      meta: createMeta('market', {
        tsEvent: asTsMs(endTs),
        tsIngest: asTsMs(endTs),
        streamId: this.streamId,
        correlationId: params.correlationId,
      }),
    };
    this.bus.publish('market:kline', event);
    return event;
  }

  publishMarketTrade(params: {
    symbol?: string;
    side?: 'Buy' | 'Sell';
    price?: number;
    size?: number;
    tradeTs?: number;
    exchangeTs?: number;
    ts?: number;
    tradeId?: string;
    correlationId?: string;
  } = {}): TradeEvent {
    const ts = params.ts ?? this.clock.next();
    const tradeTs = params.tradeTs ?? ts;
    const exchangeTs = params.exchangeTs ?? tradeTs;
    const tradeId = params.tradeId ?? `trade-${++this.tradeCounter}`;
    const event: TradeEvent = {
      symbol: params.symbol ?? this.symbol,
      streamId: this.streamId,
      tradeId,
      side: params.side ?? 'Buy',
      price: params.price ?? 100,
      size: params.size ?? 1,
      tradeTs,
      exchangeTs,
      marketType: this.marketType,
      meta: createMeta('market', {
        tsEvent: asTsMs(tradeTs),
        tsIngest: asTsMs(ts),
        streamId: this.streamId,
        correlationId: params.correlationId,
      }),
    };
    this.bus.publish('market:trade', event);
    return event;
  }

  publishMarketOrderbookL2(params: {
    symbol?: string;
    kind?: 'snapshot' | 'delta';
    updateId?: number;
    exchangeTs?: number;
    ts?: number;
    bids?: OrderbookLevel[];
    asks?: OrderbookLevel[];
    correlationId?: string;
  } = {}): OrderbookL2SnapshotEvent | OrderbookL2DeltaEvent {
    const ts = params.ts ?? this.clock.next();
    const updateId = params.updateId ?? ++this.orderbookUpdateId;
    const exchangeTs = params.exchangeTs ?? ts;
    const bids = params.bids ?? [{ price: 100, size: 1 }];
    const asks = params.asks ?? [{ price: 101, size: 1 }];
    const meta = createMeta('market', {
      tsEvent: asTsMs(exchangeTs),
      tsIngest: asTsMs(ts),
      streamId: this.streamId,
      correlationId: params.correlationId,
    });

    if (params.kind === 'delta') {
      const event: OrderbookL2DeltaEvent = {
        symbol: params.symbol ?? this.symbol,
        streamId: this.streamId,
        updateId,
        exchangeTs,
        marketType: this.marketType,
        bids,
        asks,
        meta,
      };
      this.bus.publish('market:orderbook_l2_delta', event);
      return event;
    }

    const event: OrderbookL2SnapshotEvent = {
      symbol: params.symbol ?? this.symbol,
      streamId: this.streamId,
      updateId,
      exchangeTs,
      marketType: this.marketType,
      bids,
      asks,
      meta,
    };
    this.bus.publish('market:orderbook_l2_snapshot', event);
    return event;
  }

  publishMarketOi(params: {
    symbol?: string;
    openInterest?: number;
    openInterestUnit?: OpenInterestUnit;
    openInterestValueUsd?: number;
    exchangeTs?: number;
    ts?: number;
    correlationId?: string;
  } = {}): OpenInterestEvent {
    const ts = params.ts ?? this.clock.next();
    const exchangeTs = params.exchangeTs ?? ts;
    const event: OpenInterestEvent = {
      symbol: params.symbol ?? this.symbol,
      streamId: this.streamId,
      openInterest: params.openInterest ?? 1000,
      openInterestUnit: params.openInterestUnit ?? 'base',
      openInterestValueUsd: params.openInterestValueUsd,
      exchangeTs,
      marketType: this.marketType,
      meta: createMeta('market', {
        tsEvent: asTsMs(exchangeTs),
        tsIngest: asTsMs(ts),
        streamId: this.streamId,
        correlationId: params.correlationId,
      }),
    };
    this.bus.publish('market:oi', event);
    return event;
  }

  publishMarketFunding(params: {
    symbol?: string;
    fundingRate?: number;
    exchangeTs?: number;
    nextFundingTs?: number;
    ts?: number;
    correlationId?: string;
  } = {}): FundingRateEvent {
    const ts = params.ts ?? this.clock.next();
    const exchangeTs = params.exchangeTs ?? ts;
    const event: FundingRateEvent = {
      symbol: params.symbol ?? this.symbol,
      streamId: this.streamId,
      fundingRate: params.fundingRate ?? 0.0001,
      exchangeTs,
      nextFundingTs: params.nextFundingTs,
      marketType: this.marketType,
      meta: createMeta('market', {
        tsEvent: asTsMs(exchangeTs),
        tsIngest: asTsMs(ts),
        streamId: this.streamId,
        correlationId: params.correlationId,
      }),
    };
    this.bus.publish('market:funding', event);
    return event;
  }

  async runReplay(options: Omit<JournalReplayOptions, 'baseDir' | 'streamId' | 'symbol' | 'runId'> & { runId?: string } = {}): Promise<ReplayFinished> {
    const runner = createJournalReplayRunner(this.bus, {
      baseDir: this.baseDir,
      streamId: this.streamId,
      symbol: this.symbol,
      runId: options.runId ?? this.runId,
      ...options,
    });
    return runner.run();
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;

    for (let i = this.unsubscribers.length - 1; i >= 0; i -= 1) {
      this.unsubscribers[i]();
    }

    for (let i = this.stopFns.length - 1; i >= 0; i -= 1) {
      await this.stopFns[i]();
    }
  }

  async ensureDirExists(relPath: string): Promise<void> {
    await fs.mkdir(path.join(this.baseDir, relPath), { recursive: true });
  }
}

function tfToInterval(tf: string): KlineInterval {
  const map: Record<string, KlineInterval> = {
    '1m': '1',
    '3m': '3',
    '5m': '5',
    '15m': '15',
    '30m': '30',
    '1h': '60',
    '2h': '120',
    '4h': '240',
    '6h': '360',
    '12h': '720',
    '1d': '1440',
  };
  const interval = map[tf];
  if (!interval) {
    throw new Error(`Unsupported tf: ${tf}`);
  }
  return interval;
}

function intervalToMs(interval: KlineInterval): number {
  return Number.parseInt(interval, 10) * 60_000;
}

export type {
  AnalyticsFlowEvent,
  AnalyticsLiquidityEvent,
  AnalyticsFeaturesEvent,
  AnalyticsReadyEvent,
  MarketContextEvent,
  StrategyIntentEvent,
  RiskApprovedIntentEvent,
  RiskRejectedIntentEvent,
  PaperFillEvent,
  PortfolioUpdateEvent,
};
