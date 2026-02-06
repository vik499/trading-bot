import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../../src/core/events/testing';
import { createMeta, type MarketCvdAggEvent, type MarketPriceCanonicalEvent } from '../../src/core/events/EventBus';
import type { ExpectedSourcesConfig } from '../../src/core/market/expectedSources';
import { SourceRegistry } from '../../src/core/market/SourceRegistry';
import { MarketDataReadiness } from '../../src/observability/MarketDataReadiness';

describe('MarketDataReadiness price bucket alignment', () => {
  it('does not emit NO_REF_PRICE when price aligns to the same bucket-close as flow', () => {
    const bus = createTestEventBus();
    const registry = new SourceRegistry();
    const readiness = new MarketDataReadiness(bus, {
      sourceRegistry: registry,
      bucketMs: 1000,
      warmingWindowMs: 0,
      expectedFlowTypes: ['spot'],
      expectedDerivativeKinds: [],
    });

    const statuses: Array<{ degradedReasons: string[] }> = [];
    bus.subscribe('system:market_data_status', (evt) => statuses.push(evt));

    readiness.start();

    const price: MarketPriceCanonicalEvent = {
      symbol: 'BTCUSDT',
      ts: 1017,
      marketType: 'spot',
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.spot.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.9,
      meta: createMeta('global_data', { tsEvent: 1017, tsIngest: 1017 }),
    };

    const flow: MarketCvdAggEvent = {
      symbol: 'BTCUSDT',
      ts: 1000,
      cvd: 10,
      cvdDelta: 1,
      marketType: 'spot',
      bucketStartTs: 0,
      bucketEndTs: 1000,
      bucketSizeMs: 1000,
      sourcesUsed: ['binance.spot.public', 'okx.public.spot'],
      mismatchDetected: true,
      confidenceScore: 0.475,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    };

    bus.publish('market:price_canonical', price);
    bus.publish('market:cvd_spot_agg', flow);

    const last = statuses[statuses.length - 1];
    expect(last).toBeTruthy();
    expect(last.degradedReasons).not.toContain('NO_REF_PRICE');
    expect(last.degradedReasons).toContain('MISMATCH_DETECTED');

    readiness.stop();
  });

  it('does not let spot flow mismatch drag down futures readiness', () => {
    const bus = createTestEventBus();
    const registry = new SourceRegistry();
    const expectedSourcesConfig: ExpectedSourcesConfig = {
      default: {
        spot: { flow: ['binance.spot.public', 'okx.public.spot'] },
        futures: { flow: ['binance.usdm.public'] },
      },
    };
    const readiness = new MarketDataReadiness(bus, {
      sourceRegistry: registry,
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      expectedFlowTypes: ['spot', 'futures'],
      expectedDerivativeKinds: [],
      expectedSourcesConfig,
    });

    const statuses: Array<{ degradedReasons: string[] }> = [];
    bus.subscribe('system:market_data_status', (evt) => statuses.push(evt));

    readiness.start();

    const spotFlow: MarketCvdAggEvent = {
      symbol: 'BTCUSDT',
      ts: 1000,
      cvd: 1,
      cvdDelta: 1,
      marketType: 'spot',
      bucketStartTs: 0,
      bucketEndTs: 1000,
      bucketSizeMs: 1000,
      sourcesUsed: ['binance.spot.public', 'okx.public.spot'],
      mismatchDetected: true,
      confidenceScore: 0.475,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    };

    const futuresFlow: MarketCvdAggEvent = {
      symbol: 'BTCUSDT',
      ts: 1000,
      cvd: 1,
      cvdDelta: 1,
      marketType: 'futures',
      bucketStartTs: 0,
      bucketEndTs: 1000,
      bucketSizeMs: 1000,
      sourcesUsed: ['binance.usdm.public'],
      mismatchDetected: false,
      confidenceScore: 0.95,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    };

    bus.publish('market:cvd_spot_agg', spotFlow);
    bus.publish('market:cvd_futures_agg', futuresFlow);

    const last = statuses[statuses.length - 1];
    expect(last).toBeTruthy();
    expect(last.degradedReasons).not.toContain('FLOW_LOW_CONF');

    readiness.stop();
  });
});
