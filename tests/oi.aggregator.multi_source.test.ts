import { describe, it, expect } from 'vitest';
import { createMeta } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { OpenInterestAggregator } from '../src/globalData/OpenInterestAggregator';

describe('OpenInterestAggregator multi-source', () => {
  it('keeps units consistent and ignores incompatible sources', () => {
    const bus = createTestEventBus();
    const agg = new OpenInterestAggregator(bus, {
      ttlMs: 60_000,
      weights: { bybit: 1, binance: 2, okx: 1 },
    });
    const outputs: Array<{ openInterest: number; openInterestUnit: string; venueBreakdown?: Record<string, number> }> = [];
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
      streamId: 'binance',
      openInterest: 200,
      openInterestUnit: 'base',
      exchangeTs: 1_100,
      meta: createMeta('market', { ts: 1_100 }),
    });

    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'okx',
      openInterest: 500,
      openInterestUnit: 'contracts',
      exchangeTs: 1_200,
      meta: createMeta('market', { ts: 1_200 }),
    });

    const last = outputs[outputs.length - 1];
    expect(last.openInterest).toBe(500);
    expect(last.openInterestUnit).toBe('base');
    expect(last.venueBreakdown?.bybit).toBe(100);
    expect(last.venueBreakdown?.binance).toBe(200);
    expect(last.venueBreakdown?.okx).toBeUndefined();
  });
});
