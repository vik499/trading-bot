import { describe, expect, it } from 'vitest';
import { LiquidationAggregator } from '../src/globalData/LiquidationAggregator';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type LiquidationEvent, type MarketLiquidationsAggEvent } from '../src/core/events/EventBus';

describe('LiquidationAggregator', () => {
  it('buckets liquidations and emits weighted aggregate', () => {
    const bus = createTestEventBus();
    const agg = new LiquidationAggregator(bus, { bucketMs: 1000, ttlMs: 10_000 });
    const outputs: MarketLiquidationsAggEvent[] = [];
    bus.subscribe('market:liquidations_agg', (evt) => outputs.push(evt));
    agg.start();

    const make = (streamId: string, ts: number, side: LiquidationEvent['side'], price: number, size: number): LiquidationEvent => ({
      symbol: 'BTCUSDT',
      streamId,
      side,
      price,
      size,
      notionalUsd: price * size,
      exchangeTs: ts,
      meta: createMeta('market', { ts }),
    });

    bus.publish('market:liquidation', make('s1', 500, 'Buy', 100, 1));
    bus.publish('market:liquidation', make('s2', 700, 'Sell', 200, 2));
    bus.publish('market:liquidation', make('s1', 1500, 'Buy', 150, 1));

    expect(outputs).toHaveLength(1);
    const evt = outputs[0];
    expect(evt.bucketStartTs).toBe(0);
    expect(evt.bucketEndTs).toBe(1000);
    expect(evt.bucketSizeMs).toBe(1000);
    expect(evt.unit).toBe('usd');
    expect(evt.liquidationNotional).toBe(500);
    expect(evt.liquidationCount).toBe(2);
    expect(evt.sideBreakdown?.buy).toBe(100);
    expect(evt.sideBreakdown?.sell).toBe(400);
    expect(evt.meta.ts).toBe(1000);

    agg.stop();
  });
});
