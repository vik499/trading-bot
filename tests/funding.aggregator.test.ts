import { describe, it, expect } from 'vitest';
import { createMeta, type MarketFundingAggEvent } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { FundingAggregator } from '../src/globalData/FundingAggregator';

describe('FundingAggregator', () => {
  it('computes weighted average and drops stale sources', () => {
    const bus = createTestEventBus();
    const agg = new FundingAggregator(bus, {
      ttlMs: 2_000,
      weights: { bybit: 1, binance: 3, okx: 2 },
    });
    const outputs: MarketFundingAggEvent[] = [];
    bus.subscribe('market:funding_agg', (evt) => outputs.push(evt));
    agg.start();

    bus.publish('market:funding', {
      symbol: 'BTCUSDT',
      streamId: 'bybit',
      fundingRate: 0.01,
      exchangeTs: 1_000,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000, streamId: 'bybit' }),
    });

    bus.publish('market:funding', {
      symbol: 'BTCUSDT',
      streamId: 'binance',
      fundingRate: 0.03,
      exchangeTs: 1_100,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_100, tsIngest: 1_100, streamId: 'binance' }),
    });

    bus.publish('market:funding', {
      symbol: 'BTCUSDT',
      streamId: 'okx',
      fundingRate: 0.02,
      exchangeTs: 1_150,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_150, tsIngest: 1_150, streamId: 'okx' }),
    });

    const weighted = outputs[outputs.length - 1];
    expect(weighted.fundingRate).toBeCloseTo((0.01 * 1 + 0.03 * 3 + 0.02 * 2) / 6, 8);
    expect(weighted.venueBreakdown?.bybit).toBe(0.01);
    expect(weighted.venueBreakdown?.binance).toBe(0.03);
    expect(weighted.venueBreakdown?.okx).toBe(0.02);

    bus.publish('market:funding', {
      symbol: 'BTCUSDT',
      streamId: 'binance',
      fundingRate: 0.02,
      exchangeTs: 12_500,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 12_500, tsIngest: 12_500, streamId: 'binance' }),
    });

    const last = outputs[outputs.length - 1];
    expect(last.fundingRate).toBeCloseTo(0.02, 8);
    expect(last.venueBreakdown?.bybit).toBeUndefined();
    expect(last.venueBreakdown?.binance).toBe(0.02);
    expect(last.venueBreakdown?.okx).toBeUndefined();
  });

  it('uses local ingest time when tsIngest is missing', () => {
    const NOW = 5_000;
    const bus = createTestEventBus();
    const agg = new FundingAggregator(bus, { ttlMs: 2_000, now: () => NOW });
    const outputs: MarketFundingAggEvent[] = [];
    bus.subscribe('market:funding_agg', (evt) => outputs.push(evt));
    agg.start();

    bus.publish('market:funding', {
      symbol: 'BTCUSDT',
      streamId: 'bybit',
      fundingRate: 0.01,
      exchangeTs: 1_000,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, streamId: 'bybit' }),
    });

    const last = outputs[outputs.length - 1];
    expect(last.meta.tsIngest).toBe(NOW);
    expect(last.meta.tsIngest).not.toBe(last.meta.tsEvent);
  });
});
