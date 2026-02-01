import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import { LiquidityEngine } from '../src/analytics/LiquidityEngine';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type AnalyticsLiquidityEvent, type OrderbookL2SnapshotEvent } from '../src/core/events/EventBus';

describe('LiquidityEngine', () => {
  let bus = createTestEventBus();
  let engine: LiquidityEngine;

  beforeEach(() => {
    bus = createTestEventBus();
    engine = new LiquidityEngine(bus, { minEmitIntervalMs: 0, maxUpdatesBeforeEmit: 1, depthLevels: 2 });
    engine.start();
  });

  afterEach(() => {
    engine.stop();
  });

  it('emits liquidity metrics from orderbook snapshot', () => {
    const events: AnalyticsLiquidityEvent[] = [];
    bus.subscribe('analytics:liquidity', (evt) => events.push(evt));

    const snapshot: OrderbookL2SnapshotEvent = {
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
        { price: 101, size: 1.5 },
        { price: 102, size: 3 },
      ],
      meta: createMeta('market', { tsEvent: 1000, tsIngest: 1500, streamId: 's1', correlationId: 'corr-1' }),
    };

    bus.publish('market:orderbook_l2_snapshot', snapshot);

    expect(events).toHaveLength(1);
    const evt = events[0];
    expect(evt.bestBid).toBe(100);
    expect(evt.bestAsk).toBe(101);
    expect(evt.spread).toBe(1);
    expect(evt.depthBid).toBe(3);
    expect(evt.depthAsk).toBe(4.5);
    expect(evt.imbalance).toBeCloseTo((3 - 4.5) / (3 + 4.5));
    expect(evt.meta.ts).toBe(1000);
    expect(evt.meta.correlationId).toBe('corr-1');
  });
});
