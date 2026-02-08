import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../../src/core/events/testing';
import { createMeta, type DataMismatch, type MarketCvdAggEvent, type MarketDataStatusPayload, type MarketPriceCanonicalEvent } from '../../src/core/events/EventBus';
import { MarketDataReadiness, buildMarketReadinessSnapshot } from '../../src/observability/MarketDataReadiness';

describe('MarketDataReadiness snapshot', () => {
  it('preserves degraded reasons and warnings in health snapshot', () => {
    const payload: MarketDataStatusPayload = {
      overallConfidence: 0.4,
      blockConfidence: {
        price: 0.4,
        flow: 0.4,
        liquidity: 0.4,
        derivatives: 0.4,
      },
      degraded: true,
      degradedReasons: ['PRICE_STALE', 'NO_DATA'],
      warnings: ['EXCHANGE_LAG_TOO_HIGH'],
      warmingUp: false,
      warmingProgress: 1,
      warmingWindowMs: 60_000,
      activeSources: 1,
      expectedSources: 1,
      activeSourcesAgg: 1,
      activeSourcesRaw: 1,
      expectedSourcesAgg: 1,
      expectedSourcesRaw: 1,
      lastBucketTs: 1_000,
      meta: createMeta('global_data', { tsEvent: 1_000, tsIngest: 1_000 }),
    };

    const snapshot = buildMarketReadinessSnapshot(payload, 'BTCUSDT', 'futures');

    expect(snapshot.status).toBe('DEGRADED');
    expect(snapshot.degradedReasons).toEqual(['PRICE_STALE', 'NO_DATA']);
    expect(snapshot.warnings).toEqual(['EXCHANGE_LAG_TOO_HIGH']);
  });

  it('tracks worstStatusInMinute and reasonsUnionInMinute when minute ends READY', () => {
    const bus = createTestEventBus();
    let nowTs = 1000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      expectedSources: 1,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
      now: () => nowTs,
    });

    readiness.start();

    const basePrice: Omit<MarketPriceCanonicalEvent, 'ts' | 'meta'> = {
      symbol: 'BTCUSDT',
      marketType: 'futures',
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.usdm.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.95,
    };
    const baseFlow: Omit<MarketCvdAggEvent, 'ts' | 'bucketStartTs' | 'bucketEndTs' | 'meta'> = {
      symbol: 'BTCUSDT',
      cvd: 10,
      cvdDelta: 1,
      marketType: 'futures',
      bucketSizeMs: 1000,
      sourcesUsed: ['binance.usdm.public'],
      mismatchDetected: false,
      confidenceScore: 0.95,
    };

    bus.publish('market:price_canonical', {
      ...basePrice,
      ts: 1000,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    });
    nowTs = 1000;
    bus.publish('market:cvd_futures_agg', {
      ...baseFlow,
      ts: 1000,
      bucketStartTs: 0,
      bucketEndTs: 1000,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    });

    const mismatch: DataMismatch = {
      symbol: 'BTCUSDT',
      topic: 'market:price_canonical',
      ts: 4000,
      values: { a: 1, b: 2 },
      thresholdPct: 0.1,
      meta: createMeta('global_data', { tsEvent: 4000, tsIngest: 4000 }),
    };
    bus.publish('data:mismatch', mismatch);
    nowTs = 4000;
    bus.publish('market:cvd_futures_agg', {
      ...baseFlow,
      ts: 4000,
      bucketStartTs: 3000,
      bucketEndTs: 4000,
      meta: createMeta('global_data', { tsEvent: 4000, tsIngest: 4000 }),
    });

    bus.publish('market:price_canonical', {
      ...basePrice,
      ts: 5000,
      meta: createMeta('global_data', { tsEvent: 5000, tsIngest: 5000 }),
    });
    nowTs = 5000;
    bus.publish('market:cvd_futures_agg', {
      ...baseFlow,
      ts: 5000,
      bucketStartTs: 4000,
      bucketEndTs: 5000,
      meta: createMeta('global_data', { tsEvent: 5000, tsIngest: 5000 }),
    });

    const snapshot = readiness.getHealthSnapshot();
    expect(snapshot).toBeTruthy();
    expect(snapshot?.status).toBe('READY');
    expect(snapshot?.degradedReasons).toEqual([]);
    expect(snapshot?.worstStatusInMinute).toBe('DEGRADED');
    expect(snapshot?.reasonsUnionInMinute).toEqual(['PRICE_STALE', 'NO_REF_PRICE']);

    readiness.stop();
  });
});
