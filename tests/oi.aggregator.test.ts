import { describe, it, expect } from 'vitest';
import { createMeta, type MarketOpenInterestAggEvent } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { OpenInterestAggregator } from '../src/globalData/OpenInterestAggregator';

describe('OpenInterestAggregator', () => {
  it('aggregates multiple sources and preserves correlationId', () => {
    const bus = createTestEventBus();
    const agg = new OpenInterestAggregator(bus, {
      ttlMs: 60_000,
      weights: { bybit: 1, binance: 0.5 },
    });
    const outputs: MarketOpenInterestAggEvent[] = [];
    bus.subscribe('market:oi_agg', (evt) => outputs.push(evt));
    agg.start();

    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'bybit',
      openInterest: 100,
      openInterestUnit: 'base',
      exchangeTs: 1_000,
      meta: createMeta('market', { ts: 1_000, correlationId: 'corr-1' }),
    });

    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'binance',
      openInterest: 200,
      openInterestUnit: 'base',
      exchangeTs: 1_100,
      meta: createMeta('market', { ts: 1_100, correlationId: 'corr-2' }),
    });

    const last = outputs[outputs.length - 1];
    expect(last.openInterest).toBe(200);
    expect(last.openInterestUnit).toBe('base');
    expect(last.venueBreakdown?.bybit).toBe(100);
    expect(last.venueBreakdown?.binance).toBe(200);
    expect(last.meta.ts).toBe(1_100);
    expect(last.meta.correlationId).toBe('corr-2');
  });

  it('drops stale sources outside TTL window', () => {
    const bus = createTestEventBus();
    const agg = new OpenInterestAggregator(bus, { ttlMs: 1_000 });
    const outputs: MarketOpenInterestAggEvent[] = [];
    bus.subscribe('market:oi_agg', (evt) => outputs.push(evt));
    agg.start();

    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'bybit',
      openInterest: 100,
      openInterestUnit: 'base',
      exchangeTs: 1_000,
      meta: createMeta('market', { ts: 1_000 }),
    });

    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'okx',
      openInterest: 50,
      openInterestUnit: 'base',
      exchangeTs: 25_000,
      meta: createMeta('market', { ts: 25_000 }),
    });

    const last = outputs[outputs.length - 1];
    expect(last.openInterest).toBe(50);
    expect(last.venueBreakdown?.okx).toBe(50);
    expect(last.venueBreakdown?.bybit).toBeUndefined();
  });
});
