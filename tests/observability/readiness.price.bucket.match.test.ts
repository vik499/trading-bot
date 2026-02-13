import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../../src/core/events/testing';
import { createMeta, type MarketCvdAggEvent, type MarketPriceCanonicalEvent } from '../../src/core/events/EventBus';
import type { ExpectedSourcesConfig } from '../../src/core/market/expectedSources';
import { SourceRegistry } from '../../src/core/market/SourceRegistry';
import { MarketDataReadiness, bucketLabelTsForReadiness } from '../../src/observability/MarketDataReadiness';

describe('MarketDataReadiness price bucket alignment', () => {
  it('maps boundary timestamps and +1ms jitter into the same readiness bucket label', () => {
    expect(bucketLabelTsForReadiness(1770843245000, 5000)).toBe(1770843245000);
    expect(bucketLabelTsForReadiness(1770843245001, 5000)).toBe(1770843245000);
  });

  it('does not emit PRICE_BUCKET_MISMATCH for boundary-close price timestamps', () => {
    const bus = createTestEventBus();
    const expectedSourcesConfig: ExpectedSourcesConfig = {
      default: {
        futures: {
          flow: ['okx.public.swap'],
          price: ['binance.usdm.public', 'bybit.public.linear.v5'],
        },
      },
    };
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 5000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
      expectedSourcesConfig,
    });

    const statuses: Array<{ degraded: boolean; degradedReasons: string[]; warnings?: string[] }> = [];
    bus.subscribe('system:market_data_status', (evt) => statuses.push(evt));

    readiness.start();

    const price: MarketPriceCanonicalEvent = {
      symbol: 'BTCUSDT',
      ts: 1770843245001,
      marketType: 'futures',
      indexPrice: 67539.21641304501,
      markPrice: 67503.69,
      lastPrice: 67509.7,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.usdm.public', 'bybit.public.linear.v5'],
      freshSourcesCount: 2,
      mismatchDetected: false,
      confidenceScore: 1,
      meta: createMeta('global_data', { tsEvent: 1770843245001, tsIngest: 1770843245153 }),
    };

    const flow: MarketCvdAggEvent = {
      symbol: 'BTCUSDT',
      ts: 1770843245000,
      cvd: 0.5973999999999999,
      cvdDelta: 0.5973999999999999,
      cvdFutures: 0.5973999999999999,
      marketType: 'futures',
      bucketStartTs: 1770843240000,
      bucketEndTs: 1770843245000,
      bucketSizeMs: 5000,
      unit: 'base',
      sourcesUsed: ['okx.public.swap'],
      mismatchDetected: false,
      confidenceScore: 1,
      meta: createMeta('global_data', { tsEvent: 1770843245000, tsIngest: 1770843244486 }),
    };

    bus.publish('market:price_canonical', price);
    bus.publish('market:cvd_futures_agg', flow);

    const last = statuses[statuses.length - 1];
    expect(last).toBeTruthy();
    expect(last.degraded).toBe(false);
    expect(last.degradedReasons).not.toContain('PRICE_STALE');
    expect(last.degradedReasons).not.toContain('NO_VALID_REF_PRICE');
    expect(last.warnings ?? []).not.toContain('PRICE_BUCKET_MISMATCH');

    readiness.stop();
  });

  it('emits PRICE_BUCKET_MISMATCH warning and keeps ref-price valid (no NO_VALID_REF_PRICE)', () => {
    const bus = createTestEventBus();
    const registry = new SourceRegistry();
    const readiness = new MarketDataReadiness(bus, {
      sourceRegistry: registry,
      bucketMs: 1000,
      warmingWindowMs: 0,
      expectedFlowTypes: ['spot'],
      expectedDerivativeKinds: [],
    });

    const statuses: Array<{ degradedReasons: string[]; warnings?: string[] }> = [];
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
      ts: 2500,
      cvd: 10,
      cvdDelta: 1,
      marketType: 'spot',
      bucketStartTs: 2000,
      bucketEndTs: 3000,
      bucketSizeMs: 1000,
      sourcesUsed: ['binance.spot.public', 'okx.public.spot'],
      mismatchDetected: true,
      confidenceScore: 0.475,
      meta: createMeta('global_data', { tsEvent: 2500, tsIngest: 2500 }),
    };

    bus.publish('market:price_canonical', price);
    bus.publish('market:cvd_spot_agg', flow);

    const last = statuses[statuses.length - 1];
    expect(last).toBeTruthy();
    expect(last.degradedReasons).not.toContain('PRICE_STALE');
    expect(last.degradedReasons).not.toContain('NO_VALID_REF_PRICE');
    expect(last.degradedReasons).toContain('MISMATCH_DETECTED');
    expect(last.warnings).toContain('PRICE_BUCKET_MISMATCH');

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
