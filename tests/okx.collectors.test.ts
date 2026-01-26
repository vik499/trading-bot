import { describe, it, expect, vi } from 'vitest';
import { OkxFundingCollector, OkxOpenInterestCollector } from '../src/exchange/okx/restClient';
import { createTestEventBus } from '../src/core/events/testing';
import type { FundingRateEvent, OpenInterestEvent } from '../src/core/events/EventBus';

describe('OKX collectors', () => {
  it('publishes funding and open interest with deterministic meta.ts', async () => {
    const bus = createTestEventBus();
    const restClient = {
      fetchOpenInterest: vi.fn().mockResolvedValue({
        openInterest: 12.5,
        openInterestUnit: 'base',
        openInterestValueUsd: 250000,
        ts: 1_000,
      }),
      fetchFundingRate: vi.fn().mockResolvedValue({
        fundingRate: 0.0002,
        fundingTime: 900,
        nextFundingTime: 1_800,
        ts: 1_050,
      }),
    };

    const oiCollector = new OkxOpenInterestCollector(restClient as any, bus, { oiIntervalMs: 60_000 });
    const fundingCollector = new OkxFundingCollector(restClient as any, bus, { fundingIntervalMs: 60_000 });

    const oiEvents: OpenInterestEvent[] = [];
    const fundingEvents: FundingRateEvent[] = [];
    bus.subscribe('market:oi', (evt) => oiEvents.push(evt));
    bus.subscribe('market:funding', (evt) => fundingEvents.push(evt));

    await (oiCollector as any).pollOpenInterest('BTCUSDT');
    await (fundingCollector as any).pollFunding('BTCUSDT');

    expect(oiEvents).toHaveLength(1);
    expect(fundingEvents).toHaveLength(1);
    expect(oiEvents[0].meta.source).toBe('okx');
    expect(oiEvents[0].meta.ts).toBe(1_000);
    expect(oiEvents[0].openInterestUnit).toBe('base');
    expect(oiEvents[0].openInterestValueUsd).toBe(250000);
    expect(fundingEvents[0].meta.source).toBe('okx');
    expect(fundingEvents[0].meta.ts).toBe(1_050);
    expect(fundingEvents[0].nextFundingTs).toBe(1_800);
  });
});
