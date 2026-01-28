import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
  eventBus as defaultEventBus,
  inheritMeta,
  type AnalyticsFlowEvent,
  type EventBus,
  type EventMeta,
  type FundingRateEvent,
  type OpenInterestEvent,
  type TradeEvent,
} from '../core/events/EventBus';

export interface FlowEngineConfig {
  minEmitIntervalMs?: number;
  maxTradesBeforeEmit?: number;
}

interface FlowState {
  cvdFutures: number;
  lastTradeDelta?: number;
  lastOi?: number;
  lastFundingRate?: number;
  lastEmitTs?: number;
  tradesSinceEmit: number;
}

export class FlowEngine {
  private readonly bus: EventBus;
  private readonly config: Required<FlowEngineConfig>;
  private started = false;
  private readonly state = new Map<string, FlowState>();
  private readonly unsubscribers: Array<() => void> = [];

  constructor(bus: EventBus = defaultEventBus, config: FlowEngineConfig = {}) {
    this.bus = bus;
    this.config = {
      minEmitIntervalMs: Math.max(0, config.minEmitIntervalMs ?? 500),
      maxTradesBeforeEmit: Math.max(1, config.maxTradesBeforeEmit ?? 50),
    };
  }

  start(): void {
    if (this.started) return;

    const onTrade = (evt: TradeEvent) => this.onTrade(evt);
    const onOi = (evt: OpenInterestEvent) => this.onOpenInterest(evt);
    const onFunding = (evt: FundingRateEvent) => this.onFunding(evt);

    this.bus.subscribe('market:trade', onTrade);
    this.bus.subscribe('market:oi', onOi);
    this.bus.subscribe('market:funding', onFunding);

    this.unsubscribers.push(
      () => this.bus.unsubscribe('market:trade', onTrade),
      () => this.bus.unsubscribe('market:oi', onOi),
      () => this.bus.unsubscribe('market:funding', onFunding)
    );

    this.started = true;
    logger.info(m('lifecycle', '[FlowEngine] started'));
  }

  stop(): void {
    if (!this.started) return;
    this.unsubscribers.forEach((fn) => fn());
    this.unsubscribers.length = 0;
    this.started = false;
  }

  private onTrade(evt: TradeEvent): void {
    const symbol = evt.symbol ?? 'n/a';
    const state = this.ensureState(symbol);

    const delta = evt.side === 'Buy' ? evt.size : -evt.size;
    state.cvdFutures += delta;
    state.lastTradeDelta = delta;
    state.tradesSinceEmit += 1;

    const shouldEmit = this.shouldEmit(state, evt.meta);
    if (!shouldEmit) return;

    this.emitFlow(evt.meta, symbol, state, { sourceTopic: 'market:trade' });
  }

  private onOpenInterest(evt: OpenInterestEvent): void {
    const symbol = evt.symbol ?? 'n/a';
    const state = this.ensureState(symbol);
    const oiDelta = state.lastOi !== undefined ? evt.openInterest - state.lastOi : undefined;
    state.lastOi = evt.openInterest;

    this.emitFlow(evt.meta, symbol, state, {
      oi: evt.openInterest,
      oiDelta,
      sourceTopic: 'market:oi',
    });
  }

  private onFunding(evt: FundingRateEvent): void {
    const symbol = evt.symbol ?? 'n/a';
    const state = this.ensureState(symbol);
    state.lastFundingRate = evt.fundingRate;

    this.emitFlow(evt.meta, symbol, state, {
      fundingRate: evt.fundingRate,
      sourceTopic: 'market:funding',
    });
  }

  private emitFlow(
    parentMeta: EventMeta,
    symbol: string,
    state: FlowState,
    snapshot: {
      oi?: number;
      oiDelta?: number;
      fundingRate?: number;
      sourceTopic: 'market:trade' | 'market:oi' | 'market:funding';
    }
  ): void {
    const meta = inheritMeta(parentMeta, 'analytics', { tsEvent: parentMeta.tsEvent ?? parentMeta.ts });
    const flowRegime = this.computeFlowRegime(state.lastTradeDelta, snapshot.oiDelta);

    const payload: AnalyticsFlowEvent = {
      symbol,
      ts: parentMeta.ts,
      cvdFutures: state.cvdFutures,
      oi: snapshot.oi ?? state.lastOi,
      oiDelta: snapshot.oiDelta,
      fundingRate: snapshot.fundingRate ?? state.lastFundingRate,
      flowRegime,
      meta,
    };

    this.bus.publish('analytics:flow', payload);
    state.lastEmitTs = parentMeta.ts;
    state.tradesSinceEmit = 0;
  }

  private shouldEmit(state: FlowState, meta: EventMeta): boolean {
    const lastEmit = state.lastEmitTs;
    if (lastEmit === undefined) return true;
    const sinceLast = meta.ts - lastEmit;
    return sinceLast >= this.config.minEmitIntervalMs || state.tradesSinceEmit >= this.config.maxTradesBeforeEmit;
  }

  private computeFlowRegime(lastTradeDelta?: number, oiDelta?: number): 'buyPressure' | 'sellPressure' | 'neutral' {
    if (lastTradeDelta === undefined) return 'neutral';
    if (lastTradeDelta > 0 && (oiDelta === undefined || oiDelta >= 0)) return 'buyPressure';
    if (lastTradeDelta < 0 && (oiDelta === undefined || oiDelta >= 0)) return 'sellPressure';
    return 'neutral';
  }

  private ensureState(symbol: string): FlowState {
    const existing = this.state.get(symbol);
    if (existing) return existing;
    const fresh: FlowState = {
      cvdFutures: 0,
      tradesSinceEmit: 0,
    };
    this.state.set(symbol, fresh);
    return fresh;
  }
}
