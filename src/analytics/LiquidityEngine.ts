import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
  eventBus as defaultEventBus,
  inheritMeta,
  type AnalyticsLiquidityEvent,
  type EventBus,
  type EventMeta,
  type OrderbookL2DeltaEvent,
  type OrderbookL2SnapshotEvent,
  type OrderbookLevel,
} from '../core/events/EventBus';

export interface LiquidityEngineConfig {
  depthLevels?: number;
  minEmitIntervalMs?: number;
  maxUpdatesBeforeEmit?: number;
}

interface OrderbookState {
  bids: Map<number, number>;
  asks: Map<number, number>;
  lastEmitTs?: number;
  updatesSinceEmit: number;
}

export class LiquidityEngine {
  private readonly bus: EventBus;
  private readonly config: Required<LiquidityEngineConfig>;
  private started = false;
  private readonly state = new Map<string, OrderbookState>();
  private readonly unsubscribers: Array<() => void> = [];

  constructor(bus: EventBus = defaultEventBus, config: LiquidityEngineConfig = {}) {
    this.bus = bus;
    this.config = {
      depthLevels: Math.max(1, config.depthLevels ?? 10),
      minEmitIntervalMs: Math.max(0, config.minEmitIntervalMs ?? 500),
      maxUpdatesBeforeEmit: Math.max(1, config.maxUpdatesBeforeEmit ?? 20),
    };
  }

  start(): void {
    if (this.started) return;

    const onSnapshot = (evt: OrderbookL2SnapshotEvent) => this.onSnapshot(evt);
    const onDelta = (evt: OrderbookL2DeltaEvent) => this.onDelta(evt);

    this.bus.subscribe('market:orderbook_l2_snapshot', onSnapshot);
    this.bus.subscribe('market:orderbook_l2_delta', onDelta);

    this.unsubscribers.push(
      () => this.bus.unsubscribe('market:orderbook_l2_snapshot', onSnapshot),
      () => this.bus.unsubscribe('market:orderbook_l2_delta', onDelta)
    );

    this.started = true;
    logger.info(m('lifecycle', '[LiquidityEngine] started'));
  }

  stop(): void {
    if (!this.started) return;
    this.unsubscribers.forEach((fn) => fn());
    this.unsubscribers.length = 0;
    this.started = false;
  }

  private onSnapshot(evt: OrderbookL2SnapshotEvent): void {
    const symbol = evt.symbol ?? 'n/a';
    const state = this.ensureState(symbol);
    state.bids = toLevelMap(evt.bids);
    state.asks = toLevelMap(evt.asks);
    state.updatesSinceEmit += 1;

    if (this.shouldEmit(state, evt.meta)) {
      this.emitLiquidity(evt.meta, symbol, state);
    }
  }

  private onDelta(evt: OrderbookL2DeltaEvent): void {
    const symbol = evt.symbol ?? 'n/a';
    const state = this.ensureState(symbol);

    applyLevels(state.bids, evt.bids);
    applyLevels(state.asks, evt.asks);
    state.updatesSinceEmit += 1;

    if (this.shouldEmit(state, evt.meta)) {
      this.emitLiquidity(evt.meta, symbol, state);
    }
  }

  private emitLiquidity(parentMeta: EventMeta, symbol: string, state: OrderbookState): void {
    const bestBid = bestPrice(state.bids, 'bid');
    const bestAsk = bestPrice(state.asks, 'ask');
    const depthBid = depthSum(state.bids, 'bid', this.config.depthLevels);
    const depthAsk = depthSum(state.asks, 'ask', this.config.depthLevels);
    const spread = bestBid !== undefined && bestAsk !== undefined ? bestAsk - bestBid : undefined;
    const totalDepth = depthBid + depthAsk;
    const imbalance = totalDepth > 0 ? (depthBid - depthAsk) / totalDepth : undefined;

    const payload: AnalyticsLiquidityEvent = {
      symbol,
      ts: parentMeta.ts,
      bestBid,
      bestAsk,
      spread,
      depthBid,
      depthAsk,
      imbalance,
      meta: inheritMeta(parentMeta, 'analytics', { tsEvent: parentMeta.tsEvent ?? parentMeta.ts }),
    };

    this.bus.publish('analytics:liquidity', payload);
    state.lastEmitTs = parentMeta.ts;
    state.updatesSinceEmit = 0;
  }

  private shouldEmit(state: OrderbookState, meta: EventMeta): boolean {
    const lastEmit = state.lastEmitTs;
    if (lastEmit === undefined) return true;
    const sinceLast = meta.ts - lastEmit;
    return sinceLast >= this.config.minEmitIntervalMs || state.updatesSinceEmit >= this.config.maxUpdatesBeforeEmit;
  }

  private ensureState(symbol: string): OrderbookState {
    const existing = this.state.get(symbol);
    if (existing) return existing;
    const fresh: OrderbookState = {
      bids: new Map<number, number>(),
      asks: new Map<number, number>(),
      updatesSinceEmit: 0,
    };
    this.state.set(symbol, fresh);
    return fresh;
  }
}

function toLevelMap(levels: OrderbookLevel[]): Map<number, number> {
  const map = new Map<number, number>();
  for (const lvl of levels) {
    if (!Number.isFinite(lvl.price) || !Number.isFinite(lvl.size)) continue;
    if (lvl.size <= 0) continue;
    map.set(lvl.price, lvl.size);
  }
  return map;
}

function applyLevels(map: Map<number, number>, levels: OrderbookLevel[]): void {
  for (const lvl of levels) {
    if (!Number.isFinite(lvl.price)) continue;
    if (!Number.isFinite(lvl.size) || lvl.size <= 0) {
      map.delete(lvl.price);
      continue;
    }
    map.set(lvl.price, lvl.size);
  }
}

function bestPrice(map: Map<number, number>, side: 'bid' | 'ask'): number | undefined {
  if (map.size === 0) return undefined;
  let best: number | undefined;
  for (const price of map.keys()) {
    if (best === undefined) {
      best = price;
      continue;
    }
    if (side === 'bid' && price > best) best = price;
    if (side === 'ask' && price < best) best = price;
  }
  return best;
}

function depthSum(map: Map<number, number>, side: 'bid' | 'ask', levels: number): number {
  if (map.size === 0) return 0;
  const prices = Array.from(map.keys()).sort((a, b) => (side === 'bid' ? b - a : a - b));
  let sum = 0;
  for (let i = 0; i < prices.length && i < levels; i += 1) {
    const size = map.get(prices[i]) ?? 0;
    sum += size;
  }
  return sum;
}
