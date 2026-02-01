import { describe, expect, it } from 'vitest';
import { LiquidityAggregator } from '../src/globalData/LiquidityAggregator';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type MarketLiquidityAggEvent, type OrderbookL2SnapshotEvent } from '../src/core/events/EventBus';

describe('LiquidityAggregator', () => {
  it('aggregates orderbook metrics across venues', () => {
    const bus = createTestEventBus();
    const agg = new LiquidityAggregator(bus, {
      depthLevels: 2,
      bucketMs: 1000,
    });
    const outputs: MarketLiquidityAggEvent[] = [];
    bus.subscribe('market:liquidity_agg', (evt) => outputs.push(evt));
    agg.start();

    const snapshot1: OrderbookL2SnapshotEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      updateId: 1,
      exchangeTs: 1000,
      marketType: 'futures',
      bids: [
        { price: 100, size: 1 },
        { price: 99, size: 2 },
      ],
      asks: [
        { price: 101, size: 1 },
        { price: 102, size: 2 },
      ],
      meta: createMeta('market', { tsEvent: 1000, tsIngest: 1000, streamId: 's1' }),
    };
    bus.publish('market:orderbook_l2_snapshot', snapshot1);

    const snapshot2: OrderbookL2SnapshotEvent = {
      symbol: 'BTCUSDT',
      streamId: 's2',
      updateId: 2,
      exchangeTs: 1500,
      marketType: 'futures',
      bids: [
        { price: 200, size: 1 },
        { price: 199, size: 1 },
      ],
      asks: [
        { price: 201, size: 1 },
        { price: 202, size: 1 },
      ],
      meta: createMeta('market', { tsEvent: 1500, tsIngest: 1500, streamId: 's2' }),
    };
    bus.publish('market:orderbook_l2_snapshot', snapshot2);

    bus.publish('market:orderbook_l2_snapshot', {
      ...snapshot1,
      updateId: 3,
      exchangeTs: 2100,
      meta: createMeta('market', { tsEvent: 2100, tsIngest: 2100, streamId: 's1' }),
    });

    expect(outputs).toHaveLength(1);
    const first = outputs[0];
    expect(first.bestBid).toBe(150);
    expect(first.bestAsk).toBe(151);
    expect(first.spread).toBe(1);
    expect(first.depthBid).toBe(2.5);
    expect(first.depthAsk).toBe(2.5);
    expect(first.imbalance).toBe(0);
    expect(first.venueBreakdown?.s1).toBe(100.5);
    expect(first.venueBreakdown?.s2).toBe(200.5);
    expect(first.depthMethod).toBe('levels');
    expect(first.depthUnit).toBe('base');
    expect(first.meta.ts).toBe(2000);

    agg.stop();
  });
});
