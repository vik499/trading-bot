import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
  eventBus as defaultEventBus,
  inheritMeta,
  type BacktestSummary,
  type EventBus,
  type EventMeta,
  type PaperFillEvent,
  type PortfolioUpdateEvent,
  type TickerEvent,
  type ReplayFinished,
  type ReplayStarted,
  type RiskApprovedIntentEvent,
  type RiskRejectedIntentEvent,
} from '../core/events/EventBus';

interface CollectorState {
  runId?: string;
  symbol?: string;
  replayStartTs?: number;
  firstMarketTs?: number;
  lastMarketTs?: number;
  totalFills: number;
  totalApprovedIntents: number;
  totalRejectedIntents: number;
  realizedBySymbol: Map<string, number>;
  peakEquity: number;
  maxDrawdown: number;
}

export class BacktestMetricsCollector {
  private readonly bus: EventBus;
  private started = false;
  private state: CollectorState = this.freshState();
  private unsubscribes: Array<() => void> = [];

  constructor(bus: EventBus = defaultEventBus) {
    this.bus = bus;
  }

  start(): void {
    if (this.started) return;
    this.subscribe('replay:started', (evt) => this.handleReplayStarted(evt));
    this.subscribe('replay:finished', (evt) => this.handleReplayFinished(evt));
    this.subscribe('market:ticker', (evt) => this.handleMarketTick(evt));
    this.subscribe('exec:paper_fill', (evt) => this.handleFill(evt));
    this.subscribe('portfolio:update', (evt) => this.handlePortfolioUpdate(evt));
    this.subscribe('risk:approved_intent', () => this.handleApproved());
    this.subscribe('risk:rejected_intent', () => this.handleRejected());
    this.started = true;
    logger.info(m('lifecycle', '[BacktestMetricsCollector] started'));
  }

  stop(): void {
    if (!this.started) return;
    this.unsubscribes.forEach((fn) => fn());
    this.unsubscribes = [];
    this.started = false;
  }

  private handleReplayStarted(evt: ReplayStarted): void {
    this.state = this.freshState();
    this.state.runId = evt.streamId;
    this.state.symbol = evt.symbol;
    this.state.replayStartTs = evt.meta.ts;
  }

  private handleReplayFinished(evt: ReplayFinished): void {
    if (this.state.replayStartTs === undefined) return;
    const startTs = this.state.firstMarketTs ?? this.state.replayStartTs ?? evt.meta.ts;
    const endTs = this.state.lastMarketTs ?? startTs;
    const summary: BacktestSummary = {
      runId: this.state.runId ?? 'unknown',
      symbol: this.state.symbol,
      totalFills: this.state.totalFills,
      totalApprovedIntents: this.state.totalApprovedIntents || undefined,
      totalRejectedIntents: this.state.totalRejectedIntents || undefined,
      realizedPnl: this.totalRealized(),
      maxDrawdown: this.state.maxDrawdown,
      startTs,
      endTs,
      meta: this.buildMeta(evt.meta),
    };
    this.bus.publish('metrics:backtest_summary', summary);
  }

  private handleMarketTick(evt: TickerEvent): void {
    const ts = evt.meta.ts;
    if (this.state.firstMarketTs === undefined) {
      this.state.firstMarketTs = ts;
    }
    this.state.lastMarketTs = ts;
  }

  private handleFill(_evt: PaperFillEvent): void {
    this.state.totalFills += 1;
  }

  private handleApproved(): void {
    this.state.totalApprovedIntents += 1;
  }

  private handleRejected(): void {
    this.state.totalRejectedIntents += 1;
  }

  private handlePortfolioUpdate(evt: PortfolioUpdateEvent): void {
    // realizedPnl is cumulative per symbol; track map and derive equity as sum
    this.state.realizedBySymbol.set(evt.symbol, evt.realizedPnl);
    const currentEquity = this.totalRealized();
    if (currentEquity > this.state.peakEquity) {
      this.state.peakEquity = currentEquity;
    }
    const drawdown = this.state.peakEquity - currentEquity;
    if (drawdown > this.state.maxDrawdown) {
      this.state.maxDrawdown = drawdown;
    }
  }

  private totalRealized(): number {
    let total = 0;
    for (const v of this.state.realizedBySymbol.values()) {
      total += v;
    }
    return total;
  }

  private buildMeta(parent: EventMeta): EventMeta {
    return inheritMeta(parent, 'metrics', { ts: parent.ts });
  }

  private subscribe<T extends keyof import('../core/events/EventBus').BotEventMap>(
    topic: T,
    handler: (...args: import('../core/events/EventBus').BotEventMap[T]) => void,
  ): void {
    this.bus.subscribe(topic, handler as any);
    this.unsubscribes.push(() => this.bus.unsubscribe(topic, handler as any));
  }

  private freshState(): CollectorState {
    return {
      totalFills: 0,
      totalApprovedIntents: 0,
      totalRejectedIntents: 0,
      realizedBySymbol: new Map<string, number>(),
      peakEquity: 0,
      maxDrawdown: 0,
    };
  }
}
