import { describe, expect, it } from 'vitest';
import {
  createMeta,
  type MarketCvdAggEvent,
  type MarketDataStatusPayload,
  type MarketPriceCanonicalEvent,
} from '../../src/core/events/EventBus';
import { createTestEventBus } from '../../src/core/events/testing';
import { MarketDataReadiness } from '../../src/observability/MarketDataReadiness';

const makePrice = (ts: number): MarketPriceCanonicalEvent => ({
  symbol: 'BTCUSDT',
  ts,
  marketType: 'spot',
  indexPrice: 100,
  sourcesUsed: ['s1'],
  freshSourcesCount: 1,
  confidenceScore: 0.9,
  meta: createMeta('global_data', { ts }),
});

const makeCvd = (ts: number): MarketCvdAggEvent => ({
  symbol: 'BTCUSDT',
  ts,
  cvd: 10,
  cvdDelta: 1,
  bucketStartTs: ts - 1000,
  bucketEndTs: ts,
  bucketSizeMs: 1000,
  unit: 'base',
  sourcesUsed: ['s1'],
  confidenceScore: 0.9,
  marketType: 'spot',
  meta: createMeta('global_data', { ts }),
});

describe('MarketDataReadiness expectedEmpty does not penalize blocks', () => {
  it('keeps liquidity/derivatives at block=1 and no low-conf reasons', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      logIntervalMs: 0,
      expectedSourcesByBlock: {
        price: ['s1'],
        flow: ['s1'],
        liquidity: [],
        derivatives: [],
      },
      expectedFlowTypes: ['spot'],
      expectedDerivativeKinds: [],
    });
    const outputs: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => outputs.push(evt));
    readiness.start();

    const ts = 1000;
    bus.publish('market:price_canonical', makePrice(ts));
    bus.publish('market:cvd_spot_agg', makeCvd(ts));

    const last = outputs[outputs.length - 1];
    expect(last.blockConfidence.liquidity).toBe(1);
    expect(last.blockConfidence.derivatives).toBe(1);
    expect(last.degradedReasons).not.toContain('LIQUIDITY_LOW_CONF');
    expect(last.degradedReasons).not.toContain('DERIVATIVES_LOW_CONF');

    readiness.stop();
  });
});
