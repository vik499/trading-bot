import { describe, it, expect, vi } from 'vitest';
import { BybitDerivativesRestPoller } from '../src/exchange/bybit/restClient';
import { FundingAggregator } from '../src/globalData/FundingAggregator';
import { OpenInterestAggregator } from '../src/globalData/OpenInterestAggregator';
import { createTestEventBus } from '../src/core/events/testing';
import type { MarketFundingAggEvent, MarketOpenInterestAggEvent, OpenInterestEvent } from '../src/core/events/EventBus';

describe('Open interest pipeline', () => {
  it('collector publishes market:oi and aggregator emits market:oi_agg', async () => {
    const bus = createTestEventBus();
    const restClient = {
      fetchOpenInterest: vi.fn().mockResolvedValue({ openInterest: 123, ts: 1_000 }),
      fetchFundingRate: vi.fn().mockResolvedValue({ fundingRate: 0.01, ts: 1_000 }),
    };
    const poller = new BybitDerivativesRestPoller(restClient as any, bus, {
      oiIntervalTime: '5min',
    });
    const aggregator = new OpenInterestAggregator(bus, { ttlMs: 60_000 });
    const fundingAggregator = new FundingAggregator(bus, { ttlMs: 60_000 });
    const oiEvents: OpenInterestEvent[] = [];
    const aggEvents: MarketOpenInterestAggEvent[] = [];
    const fundingAggEvents: MarketFundingAggEvent[] = [];

    bus.subscribe('market:oi', (evt) => oiEvents.push(evt));
    bus.subscribe('market:oi_agg', (evt) => aggEvents.push(evt));
    bus.subscribe('market:funding_agg', (evt) => fundingAggEvents.push(evt));
    aggregator.start();
    fundingAggregator.start();

    await (poller as any).pollOpenInterest('BTCUSDT');
    await (poller as any).pollFundingRate('BTCUSDT');

    expect(oiEvents).toHaveLength(1);
    expect(aggEvents).toHaveLength(1);
    expect(aggEvents[0].openInterest).toBe(123);
    expect(aggEvents[0].openInterestUnit).toBe('contracts');
    expect(aggEvents[0].meta.ts).toBe(1_000);
    expect(fundingAggEvents).toHaveLength(1);
    expect(fundingAggEvents[0].fundingRate).toBe(0.01);
  });
});
