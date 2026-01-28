import { describe, expect, it } from 'vitest';
import { LiquidityAggregator } from '../src/globalData/LiquidityAggregator';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type MarketLiquidityAggEvent, type OrderbookL2DeltaEvent, type OrderbookL2SnapshotEvent } from '../src/core/events/EventBus';

describe('L2 resync state machine', () => {
  it('delta before snapshot triggers RESYNCING and no emit until snapshot', () => {
    const bus = createTestEventBus();
    const agg = new LiquidityAggregator(bus, { bucketMs: 1000, depthLevels: 1, ttlMs: 5_000 });
    const outputs: MarketLiquidityAggEvent[] = [];
    bus.subscribe('market:liquidity_agg', (evt) => outputs.push(evt));
    agg.start();

    const delta: OrderbookL2DeltaEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      updateId: 1,
      exchangeTs: 1000,
      bids: [{ price: 100, size: 1 }],
      asks: [{ price: 101, size: 1 }],
      meta: createMeta('market', { ts: 1000 }),
    };
    bus.publish('market:orderbook_l2_delta', delta);
    expect(outputs).toHaveLength(0);

    const snapshot: OrderbookL2SnapshotEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      updateId: 5,
      exchangeTs: 1500,
      bids: [{ price: 100, size: 1 }],
      asks: [{ price: 101, size: 1 }],
      meta: createMeta('market', { ts: 1500 }),
    };
    bus.publish('market:orderbook_l2_snapshot', snapshot);
    expect(outputs).toHaveLength(0);

    bus.publish('market:orderbook_l2_snapshot', {
      ...snapshot,
      updateId: 6,
      exchangeTs: 2100,
      meta: createMeta('market', { ts: 2100 }),
    });

    expect(outputs).toHaveLength(1);

    agg.stop();
  });

  it('sequence gap on unused source does not penalize aggregate confidence', () => {
    const bus = createTestEventBus();
    const agg = new LiquidityAggregator(bus, { bucketMs: 1000, depthLevels: 1, ttlMs: 5_000 });
    const outputs: MarketLiquidityAggEvent[] = [];
    bus.subscribe('market:liquidity_agg', (evt) => outputs.push(evt));
    agg.start();

    const snapshot1: OrderbookL2SnapshotEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      updateId: 10,
      exchangeTs: 1500,
      bids: [{ price: 100, size: 1 }],
      asks: [{ price: 101, size: 1 }],
      meta: createMeta('market', { ts: 1500 }),
    };
    bus.publish('market:orderbook_l2_snapshot', snapshot1);

    const gapDelta: OrderbookL2DeltaEvent = {
      symbol: 'BTCUSDT',
      streamId: 's2',
      updateId: 1,
      exchangeTs: 1600,
      bids: [{ price: 100, size: 1 }],
      asks: [{ price: 101, size: 1 }],
      meta: createMeta('market', { ts: 1600 }),
    };
    bus.publish('market:orderbook_l2_delta', gapDelta);

    bus.publish('market:orderbook_l2_snapshot', {
      ...snapshot1,
      updateId: 11,
      exchangeTs: 2100,
      meta: createMeta('market', { ts: 2100 }),
    });

    const last = outputs[outputs.length - 1];
    expect(last.qualityFlags?.sequenceBroken).toBe(false);
    expect(last.confidenceScore).toBe(1);

    agg.stop();
  });
});
