import { describe, expect, it } from 'vitest';
import {
  createMeta,
  type MarketDataStatusPayload,
  type MarketFundingAggEvent,
  type MarketPriceCanonicalEvent,
} from '../../src/core/events/EventBus';
import { createTestEventBus } from '../../src/core/events/testing';
import { MarketDataReadiness } from '../../src/observability/MarketDataReadiness';

const makePrice = (ts: number): MarketPriceCanonicalEvent => ({
  symbol: 'BTCUSDT',
  ts,
  marketType: 'futures',
  indexPrice: 100,
  sourcesUsed: ['s1'],
  freshSourcesCount: 1,
  confidenceScore: 0.9,
  meta: createMeta('global_data', { ts }),
});

const makeFunding = (ts: number): MarketFundingAggEvent => ({
  symbol: 'BTCUSDT',
  ts,
  fundingRate: 0.0001,
  marketType: 'futures',
  sourcesUsed: ['s1'],
  confidenceScore: 0.9,
  meta: createMeta('global_data', { ts }),
});

describe('MarketDataReadiness derivatives stale window', () => {
  it('does not stale derivatives within derivativesStaleWindowMs', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      logIntervalMs: 0,
      expectedSourcesByBlock: { price: ['s1'], derivatives: ['s1'] },
      expectedDerivativeKinds: ['funding'],
      confidenceStaleWindowMs: 1000,
      derivativesStaleWindowMs: 10_000,
    });
    const outputs: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => outputs.push(evt));
    readiness.start();

    bus.publish('market:funding_agg', makeFunding(5000));
    bus.publish('market:price_canonical', makePrice(10_000));

    const last = outputs[outputs.length - 1];
    expect(last.degradedReasons).not.toContain('DERIVATIVES_LOW_CONF');

    readiness.stop();
  });
});
