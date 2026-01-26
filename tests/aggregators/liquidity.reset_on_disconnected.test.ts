import { describe, expect, it } from 'vitest';
import {
  createMeta,
  type MarketLiquidityAggEvent,
  type OrderbookL2DeltaEvent,
  type OrderbookL2SnapshotEvent,
} from '../../src/core/events/EventBus';
import { createTestEventBus } from '../../src/core/events/testing';
import { LiquidityAggregator } from '../../src/globalData/LiquidityAggregator';

describe('LiquidityAggregator reset on disconnected', () => {
  it('clears state and stops emitting after disconnect until new snapshot', () => {
    const bus = createTestEventBus();
    const aggregator = new LiquidityAggregator(bus, { bucketMs: 1000, ttlMs: 10_000 });
    const emitted: MarketLiquidityAggEvent[] = [];

    bus.subscribe('market:liquidity_agg', (evt) => emitted.push(evt));
    aggregator.start();

    const snapshot: OrderbookL2SnapshotEvent = {
      symbol: 'BTCUSDT',
      streamId: 'stream-1',
      updateId: 1,
      exchangeTs: 1000,
      marketType: 'futures',
      bids: [{ price: 100, size: 1 }],
      asks: [{ price: 101, size: 1 }],
      meta: createMeta('market', { ts: 1000 }),
    };
    const delta: OrderbookL2DeltaEvent = {
      symbol: 'BTCUSDT',
      streamId: 'stream-1',
      updateId: 2,
      exchangeTs: 2000,
      marketType: 'futures',
      bids: [{ price: 100, size: 2 }],
      asks: [{ price: 101, size: 1 }],
      meta: createMeta('market', { ts: 2000 }),
    };

    bus.publish('market:orderbook_l2_snapshot', snapshot);
    bus.publish('market:orderbook_l2_delta', delta);

    expect(emitted.length).toBeGreaterThanOrEqual(1);

    bus.publish('market:disconnected', {
      meta: createMeta('market', { ts: 2500 }),
    });

    const countBefore = emitted.length;

    const deltaAfterDisconnect: OrderbookL2DeltaEvent = {
      symbol: 'BTCUSDT',
      streamId: 'stream-1',
      updateId: 3,
      exchangeTs: 3000,
      marketType: 'futures',
      bids: [{ price: 100, size: 3 }],
      asks: [{ price: 101, size: 1 }],
      meta: createMeta('market', { ts: 3000 }),
    };

    bus.publish('market:orderbook_l2_delta', deltaAfterDisconnect);

    const countAfter = emitted.length;
    expect(countAfter).toBe(countBefore);

    aggregator.stop();
  });
});
