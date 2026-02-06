import { describe, expect, it } from 'vitest';
import {
  createMeta,
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

describe('MarketDataReadiness LOW_CONF gated by expected', () => {
  it('does not degrade flow when flow expected empty', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      logIntervalMs: 0,
      expectedSourcesByBlock: {
        price: ['s1'],
        flow: [],
      },
      expectedFlowTypes: ['spot'],
    });
    const outputs: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => outputs.push(evt));
    readiness.start();

    const ts = 1000;
    bus.publish('market:price_canonical', makePrice(ts));

    const last = outputs[outputs.length - 1];
    expect(last.blockConfidence.flow).toBe(1);
    expect(last.degradedReasons).not.toContain('FLOW_LOW_CONF');

    readiness.stop();
  });
});
