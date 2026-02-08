import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../../src/core/events/testing';
import {
  createMeta,
  type DataMismatch,
  type MarketCvdAggEvent,
  type MarketPriceCanonicalEvent,
} from '../../src/core/events/EventBus';
import { MarketDataReadiness } from '../../src/observability/MarketDataReadiness';

describe('MarketDataReadiness price telemetry', () => {
  it('attaches priceStaleTelemetry when PRICE_STALE is emitted', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
    });

    readiness.start();

    const price: MarketPriceCanonicalEvent = {
      symbol: 'BTCUSDT',
      ts: 1000,
      marketType: 'futures',
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.usdm.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.9,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    };

    const flow: MarketCvdAggEvent = {
      symbol: 'BTCUSDT',
      ts: 4000,
      cvd: 10,
      cvdDelta: 1,
      marketType: 'futures',
      bucketStartTs: 3000,
      bucketEndTs: 4000,
      bucketSizeMs: 1000,
      sourcesUsed: ['binance.usdm.public'],
      mismatchDetected: false,
      confidenceScore: 0.95,
      meta: createMeta('global_data', { tsEvent: 4000, tsIngest: 4000 }),
    };

    bus.publish('market:price_canonical', price);
    bus.publish('market:cvd_futures_agg', flow);

    const snapshot = readiness.getHealthSnapshot();
    expect(snapshot?.degradedReasons).toContain('PRICE_STALE');
    expect(snapshot?.priceStaleTelemetry?.ageMs).toBeGreaterThan(0);
    expect(snapshot?.priceStaleTelemetry?.staleMs).toBe(1000);
    expect(snapshot?.priceStaleTelemetry?.sourcesUsed?.length).toBe(1);

    readiness.stop();
  });

  it('attaches refPriceTelemetry when NO_REF_PRICE is emitted', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
    });

    readiness.start();

    const price: MarketPriceCanonicalEvent = {
      symbol: 'BTCUSDT',
      ts: 1000,
      marketType: 'futures',
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.usdm.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.4,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    };

    const mismatch: DataMismatch = {
      symbol: 'BTCUSDT',
      topic: 'market:price_canonical',
      ts: 4000,
      values: { a: 1, b: 2 },
      thresholdPct: 0.1,
      meta: createMeta('global_data', { tsEvent: 4000, tsIngest: 4000 }),
    };

    const flow: MarketCvdAggEvent = {
      symbol: 'BTCUSDT',
      ts: 4000,
      cvd: 10,
      cvdDelta: 1,
      marketType: 'futures',
      bucketStartTs: 3000,
      bucketEndTs: 4000,
      bucketSizeMs: 1000,
      sourcesUsed: ['binance.usdm.public'],
      mismatchDetected: false,
      confidenceScore: 0.95,
      meta: createMeta('global_data', { tsEvent: 4000, tsIngest: 4000 }),
    };

    bus.publish('market:price_canonical', price);
    bus.publish('data:mismatch', mismatch);
    bus.publish('market:cvd_futures_agg', flow);

    const snapshot = readiness.getHealthSnapshot();
    expect(snapshot?.degradedReasons).toContain('NO_REF_PRICE');
    expect(snapshot?.refPriceTelemetry?.hasPriceEvent).toBe(true);
    expect(snapshot?.refPriceTelemetry?.priceBucketMatch).toBe(false);
    expect(snapshot?.refPriceTelemetry?.sourceCounts?.used).toBe(1);

    readiness.stop();
  });

  it('omits price telemetry when no price-related reasons are present', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
    });

    readiness.start();

    const price: MarketPriceCanonicalEvent = {
      symbol: 'BTCUSDT',
      ts: 1500,
      marketType: 'futures',
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.usdm.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.9,
      meta: createMeta('global_data', { tsEvent: 1500, tsIngest: 1500 }),
    };

    const flow: MarketCvdAggEvent = {
      symbol: 'BTCUSDT',
      ts: 1500,
      cvd: 10,
      cvdDelta: 1,
      marketType: 'futures',
      bucketStartTs: 1000,
      bucketEndTs: 2000,
      bucketSizeMs: 1000,
      sourcesUsed: ['binance.usdm.public'],
      mismatchDetected: false,
      confidenceScore: 0.95,
      meta: createMeta('global_data', { tsEvent: 1500, tsIngest: 1500 }),
    };

    bus.publish('market:price_canonical', price);
    bus.publish('market:cvd_futures_agg', flow);

    const snapshot = readiness.getHealthSnapshot();
    expect(snapshot?.degradedReasons).not.toContain('PRICE_STALE');
    expect(snapshot?.degradedReasons).not.toContain('NO_REF_PRICE');
    expect(snapshot?.priceStaleTelemetry).toBeUndefined();
    expect(snapshot?.refPriceTelemetry).toBeUndefined();

    readiness.stop();
  });
});
