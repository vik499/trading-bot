import { describe, it, expect, vi } from 'vitest';
import { BinanceFundingCollector, BinanceOpenInterestCollector } from '../src/exchange/binance/restClient';
import { createTestEventBus } from '../src/core/events/testing';
import type { FundingRateEvent, OpenInterestEvent } from '../src/core/events/EventBus';

describe('Binance collectors', () => {
  it('publishes funding and open interest with notional', async () => {
    const bus = createTestEventBus();
    const restClient = {
      fetchPremiumIndex: vi.fn().mockResolvedValue({
        fundingRate: 0.0005,
        ts: 1_000,
        nextFundingTs: 2_000,
        markPrice: 10,
      }),
      fetchOpenInterest: vi.fn().mockResolvedValue({ openInterest: 2, ts: 1_100 }),
    };

    const shared = { markPriceBySymbol: new Map<string, { price: number; ts: number }>() };
    const fundingCollector = new BinanceFundingCollector(restClient as any, bus, shared, { fundingIntervalMs: 60_000 });
    const oiCollector = new BinanceOpenInterestCollector(restClient as any, bus, shared, {
      oiIntervalMs: 60_000,
      markPriceMaxAgeMs: 10_000,
    });

    const fundingEvents: FundingRateEvent[] = [];
    const oiEvents: OpenInterestEvent[] = [];
    bus.subscribe('market:funding', (evt) => fundingEvents.push(evt));
    bus.subscribe('market:oi', (evt) => oiEvents.push(evt));

    await (fundingCollector as any).pollFunding('BTCUSDT');
    await (oiCollector as any).pollOpenInterest('BTCUSDT');

    expect(fundingEvents).toHaveLength(1);
    expect(oiEvents).toHaveLength(1);
    expect(oiEvents[0].openInterestValueUsd).toBeUndefined();
    expect(oiEvents[0].openInterestUnit).toBe('contracts');
    expect(oiEvents[0].meta.source).toBe('binance');
  });
});
