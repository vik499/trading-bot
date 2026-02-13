import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../../src/core/events/testing';
import {
  createMeta,
  type DataGapDetected,
  type DataMismatch,
  type DataTimeOutOfOrder,
  type MarketCvdAggEvent,
  type MarketDataStatusPayload,
  type MarketPriceCanonicalEvent,
  type TradeEvent,
} from '../../src/core/events/EventBus';
import { SourceRegistry } from '../../src/core/market/SourceRegistry';
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

  it('normalizes legacy NO_REF_PRICE to NO_VALID_REF_PRICE in snapshot output', () => {
    const payload: MarketDataStatusPayload = {
      overallConfidence: 0.4,
      blockConfidence: {
        price: 0.4,
        flow: 0.4,
        liquidity: 0.4,
        derivatives: 0.4,
      },
      degraded: true,
      degradedReasons: ['PRICE_STALE', 'NO_REF_PRICE'],
      warnings: [],
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

    expect(snapshot.degradedReasons).toEqual(['PRICE_STALE', 'NO_VALID_REF_PRICE']);
    expect(snapshot.reasonsUnionInMinute).toEqual(['PRICE_STALE', 'NO_VALID_REF_PRICE']);
    expect(snapshot.degradedReasons).not.toContain('NO_REF_PRICE');
    expect(snapshot.reasonsUnionInMinute).not.toContain('NO_REF_PRICE');
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
    expect(snapshot?.reasonsUnionInMinute).toEqual(['PRICE_STALE', 'MISMATCH_DETECTED', 'NO_VALID_REF_PRICE']);

    readiness.stop();
  });

  it('keeps runtime READY while minute truth captures transient raw degradation', () => {
    const bus = createTestEventBus();
    let nowTs = 1000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      hardReasonEnterWindowMs: 5000,
      hardReasonExitWindowMs: 12000,
      softReasonEnterWindowMs: 5000,
      softReasonExitWindowMs: 2000,
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

    const publishPrice = (ts: number, mismatchDetected = false) =>
      bus.publish('market:price_canonical', {
        ...basePrice,
        mismatchDetected,
        ts,
        meta: createMeta('global_data', { tsEvent: ts, tsIngest: ts }),
      });
    const publishFlow = (ts: number) =>
      bus.publish('market:cvd_futures_agg', {
        ...baseFlow,
        ts,
        bucketStartTs: ts - 1000,
        bucketEndTs: ts,
        meta: createMeta('global_data', { tsEvent: ts, tsIngest: ts }),
      });

    publishPrice(1000);
    nowTs = 1000;
    publishFlow(1000);

    publishPrice(2000, true);
    nowTs = 2000;
    publishFlow(2000);

    publishPrice(3000);
    nowTs = 3000;
    publishFlow(3000);

    const snapshot = readiness.getHealthSnapshot();
    expect(snapshot).toBeTruthy();
    expect(snapshot?.status).toBe('READY');
    expect(snapshot?.degradedReasons).toEqual([]);
    expect(snapshot?.reasonsUnionInMinute).toContain('MISMATCH_DETECTED');
    expect(snapshot?.worstStatusInMinute).toBe('DEGRADED');

    readiness.stop();
  });

  it('does not degrade on transient hard gap pulse within enter window', () => {
    const bus = createTestEventBus();
    const statuses: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => statuses.push(evt));

    let nowTs = 1000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      hardFastReasonEnterWindowMs: 1000,
      hardReasonEnterWindowMs: 5000,
      hardReasonExitWindowMs: 4000,
      softReasonEnterWindowMs: 15000,
      softReasonExitWindowMs: 2000,
      expectedSources: 1,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
      now: () => nowTs,
    });
    readiness.start();

    const priceBase: Omit<MarketPriceCanonicalEvent, 'ts' | 'meta'> = {
      symbol: 'BTCUSDT',
      marketType: 'futures',
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.usdm.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.99,
    };
    const flowBase: Omit<MarketCvdAggEvent, 'ts' | 'bucketStartTs' | 'bucketEndTs' | 'meta'> = {
      symbol: 'BTCUSDT',
      cvd: 10,
      cvdDelta: 1,
      marketType: 'futures',
      bucketSizeMs: 1000,
      sourcesUsed: ['binance.usdm.public'],
      mismatchDetected: false,
      confidenceScore: 0.99,
    };

    const publishPrice = (ts: number) =>
      bus.publish('market:price_canonical', {
        ...priceBase,
        ts,
        meta: createMeta('global_data', { tsEvent: ts, tsIngest: ts }),
      });
    const publishFlow = (ts: number) =>
      bus.publish('market:cvd_futures_agg', {
        ...flowBase,
        ts,
        bucketStartTs: ts - 1000,
        bucketEndTs: ts,
        meta: createMeta('global_data', { tsEvent: ts, tsIngest: ts }),
      });

    publishPrice(1000);
    nowTs = 1000;
    publishFlow(1000);

    const transientGap: DataGapDetected = {
      symbol: 'BTCUSDT',
      streamId: 'binance.usdm.public',
      topic: 'market:orderbook_l2_delta',
      deltaMs: 7000,
      meta: createMeta('market_data', { tsEvent: 2000, tsIngest: 2000 }),
    };
    bus.publish('data:gapDetected', transientGap);
    nowTs = 2000;
    publishFlow(2000);

    publishPrice(3000);
    nowTs = 3000;
    publishFlow(3000);

    expect(statuses.some((status) => status.degraded)).toBe(false);
    expect(readiness.getHealthSnapshot()?.status).toBe('READY');

    readiness.stop();
  });

  it('degrades on sustained hard gap and recovers only after exit window', () => {
    const bus = createTestEventBus();
    const statuses = new Map<number, MarketDataStatusPayload>();
    bus.subscribe('system:market_data_status', (evt) => statuses.set(evt.lastBucketTs, evt));

    let nowTs = 1000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      hardFastReasonEnterWindowMs: 500,
      hardReasonEnterWindowMs: 5000,
      hardReasonExitWindowMs: 12000,
      softReasonEnterWindowMs: 15000,
      softReasonExitWindowMs: 2000,
      expectedSources: 1,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
      now: () => nowTs,
    });
    readiness.start();

    const priceBase: Omit<MarketPriceCanonicalEvent, 'ts' | 'meta'> = {
      symbol: 'BTCUSDT',
      marketType: 'futures',
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.usdm.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.99,
    };
    const publishPrice = (ts: number) =>
      bus.publish('market:price_canonical', {
        ...priceBase,
        ts,
        meta: createMeta('global_data', { tsEvent: ts, tsIngest: ts }),
      });
    const publishConnectorGap = (ts: number) =>
      bus.publish('market:disconnected', {
        reason: 'resync:gap',
        meta: createMeta('market', { tsEvent: ts, tsIngest: ts }),
      });

    publishPrice(1000);

    for (const ts of [2000, 3000, 4000, 5000, 6000, 7000]) {
      publishConnectorGap(ts);
      nowTs = ts;
      publishPrice(ts);
    }

    for (const ts of [8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000, 16000, 17000, 18000, 19000]) {
      nowTs = ts;
      publishPrice(ts);
    }

    expect(statuses.get(8000)?.degraded).toBe(true);
    expect(statuses.get(8000)?.degradedReasons).toContain('GAPS_DETECTED');
    expect(statuses.get(18000)?.degraded).toBe(true);
    expect(statuses.get(19000)?.degraded).toBe(false);

    readiness.stop();
  });

  it('keeps EXPECTED_SOURCES_MISSING_CONFIG active until config is fixed', () => {
    const bus = createTestEventBus();
    const registry = new SourceRegistry();
    const statuses = new Map<number, MarketDataStatusPayload>();
    bus.subscribe('system:market_data_status', (evt) => statuses.set(evt.lastBucketTs, evt));

    let nowTs = 1000;
    const readiness = new MarketDataReadiness(bus, {
      sourceRegistry: registry,
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      hardFastReasonEnterWindowMs: 1000,
      hardReasonEnterWindowMs: 5000,
      hardReasonExitWindowMs: 12000,
      softReasonEnterWindowMs: 15000,
      softReasonExitWindowMs: 5000,
      expectedSources: 0,
      expectedFlowTypes: [],
      expectedDerivativeKinds: [],
      criticalBlocks: [],
      now: () => nowTs,
    });
    readiness.start();

    const publishPrice = (ts: number) =>
      bus.publish('market:price_canonical', {
        symbol: 'BTCUSDT',
        marketType: 'futures',
        ts,
        indexPrice: 50000,
        priceTypeUsed: 'index',
        sourcesUsed: ['binance.usdm.public'],
        freshSourcesCount: 1,
        mismatchDetected: false,
        confidenceScore: 0.99,
        meta: createMeta('global_data', { tsEvent: ts, tsIngest: ts }),
      });

    const publishFlow = (ts: number) =>
      bus.publish('market:cvd_futures_agg', {
        symbol: 'BTCUSDT',
        cvd: 10,
        cvdDelta: 1,
        marketType: 'futures',
        bucketSizeMs: 1000,
        ts,
        bucketStartTs: ts - 1000,
        bucketEndTs: ts,
        sourcesUsed: ['binance.usdm.public'],
        mismatchDetected: false,
        confidenceScore: 0.99,
        meta: createMeta('global_data', { tsEvent: ts, tsIngest: ts }),
      });

    const publishTrade = (ts: number) =>
      bus.publish('market:trade', {
        symbol: 'BTCUSDT',
        streamId: 'binance.usdm.public',
        side: 'Buy',
        price: 50001,
        size: 0.1,
        tradeTs: ts,
        marketType: 'futures',
        meta: createMeta('market_data', { tsEvent: ts, tsIngest: ts }),
      });

    nowTs = 1000;
    publishPrice(1000);
    publishFlow(1000);
    nowTs = 2000;
    publishPrice(2000);
    publishFlow(2000);

    let snapshot = readiness.getHealthSnapshot();
    expect(snapshot?.status).toBe('DEGRADED');
    expect(snapshot?.degradedReasons).toContain('EXPECTED_SOURCES_MISSING_CONFIG');

    registry.reset();
    nowTs = 3000;
    publishTrade(3000);
    nowTs = 4000;
    publishTrade(4000);

    snapshot = readiness.getHealthSnapshot();
    expect(snapshot?.status).toBe('DEGRADED');
    expect(snapshot?.degradedReasons).toContain('EXPECTED_SOURCES_MISSING_CONFIG');

    registry.registerExpected(
      { symbol: 'BTCUSDT', marketType: 'futures', metric: 'price' },
      ['binance.usdm.public']
    );
    nowTs = 5000;
    publishPrice(5000);
    nowTs = 6000;
    publishPrice(6000);

    snapshot = readiness.getHealthSnapshot();
    expect(snapshot?.status).toBe('READY');
    expect(snapshot?.degradedReasons).not.toContain('EXPECTED_SOURCES_MISSING_CONFIG');
    expect(snapshot?.degradedReasons).toEqual([]);

    readiness.stop();
  });

  it('keeps deterministic union ordering regardless of insertion order', () => {
    const bus = createTestEventBus();
    let nowTs = 1000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      hardFastReasonEnterWindowMs: 0,
      hardReasonEnterWindowMs: 0,
      hardReasonExitWindowMs: 0,
      softReasonEnterWindowMs: 0,
      softReasonExitWindowMs: 0,
      expectedSources: 1,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
      outOfOrderToleranceMs: 100,
      lagThresholdMs: 100,
      now: () => nowTs,
    });
    readiness.start();

    const priceBase: Omit<MarketPriceCanonicalEvent, 'ts' | 'meta'> = {
      symbol: 'BTCUSDT',
      marketType: 'futures',
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.usdm.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.95,
    };
    const flowBase: Omit<MarketCvdAggEvent, 'ts' | 'bucketStartTs' | 'bucketEndTs' | 'meta'> = {
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
      ...priceBase,
      ts: 1000,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    });
    nowTs = 1000;
    bus.publish('market:cvd_futures_agg', {
      ...flowBase,
      ts: 1000,
      bucketStartTs: 0,
      bucketEndTs: 1000,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    });

    const mismatch: DataMismatch = {
      symbol: 'BTCUSDT',
      topic: 'market:price_canonical',
      ts: 2000,
      values: { a: 1, b: 2 },
      thresholdPct: 0.1,
      meta: createMeta('global_data', { tsEvent: 2000, tsIngest: 2000 }),
    };
    bus.publish('data:mismatch', mismatch);

    const gap: DataGapDetected = {
      symbol: 'BTCUSDT',
      streamId: 'binance.usdm.public',
      topic: 'market:orderbook_l2_delta',
      deltaMs: 7000,
      meta: createMeta('market_data', { tsEvent: 2000, tsIngest: 2000 }),
    };
    bus.publish('data:gapDetected', gap);
    bus.publish('market:disconnected', {
      reason: 'resync:gap',
      meta: createMeta('market', { tsEvent: 2000, tsIngest: 2000 }),
    });

    const timeOutOfOrder: DataTimeOutOfOrder = {
      symbol: 'BTCUSDT',
      streamId: 'binance.usdm.public',
      topic: 'market:orderbook_l2_delta',
      prevTs: 5000,
      currTs: 2000,
      tsSource: 'event',
      meta: createMeta('market_data', { tsEvent: 2000, tsIngest: 2000 }),
    };
    bus.publish('data:time_out_of_order', timeOutOfOrder);

    const laggedTrade: TradeEvent = {
      symbol: 'BTCUSDT',
      streamId: 'binance.usdm.public',
      side: 'Buy',
      price: 50001,
      size: 0.1,
      tradeTs: 2000,
      marketType: 'futures',
      meta: createMeta('market_data', { tsEvent: 2000, tsIngest: 2600 }),
    };
    bus.publish('market:trade', laggedTrade);

    nowTs = 2600;
    bus.publish('market:cvd_futures_agg', {
      ...flowBase,
      ts: 2000,
      bucketStartTs: 1000,
      bucketEndTs: 2000,
      meta: createMeta('global_data', { tsEvent: 2000, tsIngest: 2000 }),
    });

    const snapshot = readiness.getHealthSnapshot();
    expect(snapshot?.reasonsUnionInMinute).toEqual(['GAPS_DETECTED', 'MISMATCH_DETECTED']);
    expect(snapshot?.warningsUnionInMinute).toEqual(['TIMEBASE_QUALITY_WARN', 'EXCHANGE_LAG_TOO_HIGH', 'PRICE_BUCKET_MISMATCH']);

    readiness.stop();
  });

  it('emits TIMEBASE_FUTURE_EVENT when tsEvent is in the future beyond tolerance (warning-only)', () => {
    const bus = createTestEventBus();
    let nowTs = 5000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      hardFastReasonEnterWindowMs: 0,
      hardReasonEnterWindowMs: 0,
      hardReasonExitWindowMs: 0,
      softReasonEnterWindowMs: 0,
      softReasonExitWindowMs: 0,
      expectedSources: 1,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
      now: () => nowTs,
    });
    readiness.start();

    bus.publish('market:price_canonical', {
      symbol: 'BTCUSDT',
      marketType: 'futures',
      ts: 5000,
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.usdm.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 5000, tsIngest: 2000 }),
    });
    bus.publish('market:cvd_futures_agg', {
      symbol: 'BTCUSDT',
      cvd: 10,
      cvdDelta: 1,
      marketType: 'futures',
      bucketSizeMs: 1000,
      ts: 5000,
      bucketStartTs: 4000,
      bucketEndTs: 5000,
      sourcesUsed: ['binance.usdm.public'],
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 5000, tsIngest: 5000 }),
    });

    const snapshot = readiness.getHealthSnapshot();
    expect(snapshot?.timebase?.warnings).toContain('TIMEBASE_FUTURE_EVENT');
    expect(snapshot?.status).not.toBe('DEGRADED');
    expect(snapshot?.degradedReasons).toEqual([]);

    readiness.stop();
  });

  it('emits TIMEBASE_NON_MONOTONIC when contributing tsEvent goes backwards (warning-only)', () => {
    const bus = createTestEventBus();
    let nowTs = 2000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      hardFastReasonEnterWindowMs: 0,
      hardReasonEnterWindowMs: 0,
      hardReasonExitWindowMs: 0,
      softReasonEnterWindowMs: 0,
      softReasonExitWindowMs: 0,
      expectedSources: 1,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
      now: () => nowTs,
    });
    readiness.start();

    const publishFlow = (ts: number) =>
      bus.publish('market:cvd_futures_agg', {
        symbol: 'BTCUSDT',
        cvd: 10,
        cvdDelta: 1,
        marketType: 'futures',
        bucketSizeMs: 1000,
        ts,
        bucketStartTs: ts - 1000,
        bucketEndTs: ts,
        sourcesUsed: ['binance.usdm.public'],
        mismatchDetected: false,
        confidenceScore: 0.99,
        meta: createMeta('global_data', { tsEvent: ts, tsIngest: ts }),
      });

    bus.publish('market:price_canonical', {
      symbol: 'BTCUSDT',
      marketType: 'futures',
      ts: 2000,
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.usdm.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 2000, tsIngest: 2000 }),
    });
    publishFlow(2000);

    nowTs = 2500;
    bus.publish('market:price_canonical', {
      symbol: 'BTCUSDT',
      marketType: 'futures',
      ts: 1500,
      indexPrice: 50010,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.usdm.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 1500, tsIngest: 2500 }),
    });

    const snapshot = readiness.getHealthSnapshot();
    expect(snapshot?.timebase?.warnings).toContain('TIMEBASE_NON_MONOTONIC');
    expect(snapshot?.timebase?.byBlock.price.nonMonotonicCount).toBeGreaterThan(0);
    expect(snapshot?.status).not.toBe('DEGRADED');

    readiness.stop();
  });

  it('emits TIMEBASE_AGE_TOO_HIGH when P95 age exceeds threshold (warning-only)', () => {
    const bus = createTestEventBus();
    let nowTs = 700000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      hardFastReasonEnterWindowMs: 0,
      hardReasonEnterWindowMs: 0,
      hardReasonExitWindowMs: 0,
      softReasonEnterWindowMs: 0,
      softReasonExitWindowMs: 0,
      expectedSources: 1,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
      now: () => nowTs,
    });
    readiness.start();

    bus.publish('market:price_canonical', {
      symbol: 'BTCUSDT',
      marketType: 'futures',
      ts: 1000,
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.usdm.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 700000 }),
    });
    bus.publish('market:cvd_futures_agg', {
      symbol: 'BTCUSDT',
      cvd: 10,
      cvdDelta: 1,
      marketType: 'futures',
      bucketSizeMs: 1000,
      ts: 1000,
      bucketStartTs: 0,
      bucketEndTs: 1000,
      sourcesUsed: ['binance.usdm.public'],
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    });

    const snapshot = readiness.getHealthSnapshot();
    expect(snapshot?.timebase?.warnings).toContain('TIMEBASE_AGE_TOO_HIGH');
    expect(snapshot?.status).not.toBe('DEGRADED');
    expect(snapshot?.degradedReasons).toEqual([]);

    readiness.stop();
  });

  it('keeps deterministic timebase warnings ordering', () => {
    const bus = createTestEventBus();
    let nowTs = 2000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      hardFastReasonEnterWindowMs: 0,
      hardReasonEnterWindowMs: 0,
      hardReasonExitWindowMs: 0,
      softReasonEnterWindowMs: 0,
      softReasonExitWindowMs: 0,
      expectedSources: 1,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
      now: () => nowTs,
    });
    readiness.start();

    const publishFlow = (ts: number, tsIngest = ts) =>
      bus.publish('market:cvd_futures_agg', {
        symbol: 'BTCUSDT',
        cvd: 10,
        cvdDelta: 1,
        marketType: 'futures',
        bucketSizeMs: 1000,
        ts,
        bucketStartTs: ts - 1000,
        bucketEndTs: ts,
        sourcesUsed: ['binance.usdm.public'],
        mismatchDetected: false,
        confidenceScore: 0.99,
        meta: createMeta('global_data', { tsEvent: ts, tsIngest }),
      });

    const publishPrice = (tsEvent: number, tsIngest: number) =>
      bus.publish('market:price_canonical', {
        symbol: 'BTCUSDT',
        marketType: 'futures',
        ts: tsEvent,
        indexPrice: 50000,
        priceTypeUsed: 'index',
        sourcesUsed: ['binance.usdm.public'],
        freshSourcesCount: 1,
        mismatchDetected: false,
        confidenceScore: 0.99,
        meta: createMeta('global_data', { tsEvent, tsIngest }),
      });

    publishPrice(2000, 2000);
    publishFlow(2000, 2000);

    nowTs = 1_300_000;
    publishPrice(1_302_500, 1_300_000);
    publishFlow(1_302_500, 1_300_000);
    publishPrice(5_500, 1_300_000);

    const warnings = readiness.getHealthSnapshot()?.timebase?.warnings ?? [];
    expect(warnings).toEqual(['TIMEBASE_AGE_TOO_HIGH', 'TIMEBASE_FUTURE_EVENT', 'TIMEBASE_NON_MONOTONIC']);

    readiness.stop();
  });

  it('computes deterministic source coverage (expected/active/missing)', () => {
    const bus = createTestEventBus();
    let nowTs = 1000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      expectedSourcesByBlock: {
        price: ['bybit.public', 'binance.public'],
        flow: ['okx.public', 'binance.public'],
      },
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
      now: () => nowTs,
    });
    readiness.start();

    bus.publish('market:price_canonical', {
      symbol: 'BTCUSDT',
      marketType: 'futures',
      ts: 1000,
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    });

    nowTs = 1000;
    bus.publish('market:cvd_futures_agg', {
      symbol: 'BTCUSDT',
      cvd: 10,
      cvdDelta: 1,
      marketType: 'futures',
      bucketSizeMs: 1000,
      ts: 1000,
      bucketStartTs: 0,
      bucketEndTs: 1000,
      sourcesUsed: ['okx.public'],
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    });

    const coverage = readiness.getHealthSnapshot()?.sourceCoverage;
    expect(coverage).toBeTruthy();
    expect(coverage?.expected.price).toEqual(['binance.public', 'bybit.public']);
    expect(coverage?.active.price).toEqual(['binance.public']);
    expect(coverage?.missing.price).toEqual(['bybit.public']);
    expect(coverage?.coverageRatio.price).toBe(0.5);
    expect(coverage?.expectedTotal.price).toBe(2);
    expect(coverage?.activeTotal.price).toBe(1);

    expect(coverage?.expected.flow).toEqual(['binance.public', 'okx.public']);
    expect(coverage?.active.flow).toEqual(['okx.public']);
    expect(coverage?.missing.flow).toEqual(['binance.public']);
    expect(coverage?.coverageRatio.flow).toBe(0.5);

    readiness.stop();
  });

  it('coverage ratio is 1 when expected is empty', () => {
    const bus = createTestEventBus();
    let nowTs = 1000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      expectedSourcesByBlock: {
        price: ['binance.public'],
      },
      expectedFlowTypes: [],
      expectedDerivativeKinds: [],
      criticalBlocks: ['price'],
      now: () => nowTs,
    });
    readiness.start();

    bus.publish('market:price_canonical', {
      symbol: 'BTCUSDT',
      marketType: 'futures',
      ts: 1000,
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['binance.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    });

    nowTs = 1000;
    bus.publish('market:cvd_futures_agg', {
      symbol: 'BTCUSDT',
      cvd: 10,
      cvdDelta: 1,
      marketType: 'futures',
      bucketSizeMs: 1000,
      ts: 1000,
      bucketStartTs: 0,
      bucketEndTs: 1000,
      sourcesUsed: ['binance.public'],
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    });

    const coverage = readiness.getHealthSnapshot()?.sourceCoverage;
    expect(coverage).toBeTruthy();
    expect(coverage?.expected.liquidity).toEqual([]);
    expect(coverage?.active.liquidity).toEqual([]);
    expect(coverage?.missing.liquidity).toEqual([]);
    expect(coverage?.expectedTotal.liquidity).toBe(0);
    expect(coverage?.activeTotal.liquidity).toBe(0);
    expect(coverage?.coverageRatio.liquidity).toBe(1);

    readiness.stop();
  });

  it('keeps deterministic source coverage ordering regardless of insertion order', () => {
    const bus = createTestEventBus();
    let nowTs = 1000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      expectedSourcesByBlock: {
        price: ['zeta.public', 'alpha.public', 'alpha.public', 'beta.public'],
      },
      expectedFlowTypes: [],
      expectedDerivativeKinds: [],
      criticalBlocks: ['price'],
      now: () => nowTs,
    });
    readiness.start();

    bus.publish('market:price_canonical', {
      symbol: 'BTCUSDT',
      marketType: 'futures',
      ts: 1000,
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['beta.public', 'beta.public', 'alpha.public', 'gamma.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    });

    nowTs = 1000;
    bus.publish('market:cvd_futures_agg', {
      symbol: 'BTCUSDT',
      cvd: 10,
      cvdDelta: 1,
      marketType: 'futures',
      bucketSizeMs: 1000,
      ts: 1000,
      bucketStartTs: 0,
      bucketEndTs: 1000,
      sourcesUsed: ['beta.public', 'alpha.public'],
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    });

    const coverage = readiness.getHealthSnapshot()?.sourceCoverage;
    expect(coverage).toBeTruthy();
    expect(coverage?.expected.price).toEqual(['alpha.public', 'beta.public', 'zeta.public']);
    expect(coverage?.active.price).toEqual(['alpha.public', 'beta.public', 'gamma.public']);
    expect(coverage?.missing.price).toEqual(['zeta.public']);
    expect(coverage?.coverageRatio.price).toBe(2 / 3);

    readiness.stop();
  });

  it('captures unexpected sources (active not in expected)', () => {
    const bus = createTestEventBus();
    let nowTs = 1000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      expectedSourcesByBlock: {
        price: ['alpha.public', 'beta.public'],
      },
      expectedFlowTypes: [],
      expectedDerivativeKinds: [],
      criticalBlocks: ['price'],
      now: () => nowTs,
    });
    readiness.start();

    bus.publish('market:price_canonical', {
      symbol: 'BTCUSDT',
      marketType: 'futures',
      ts: 1000,
      indexPrice: 50000,
      priceTypeUsed: 'index',
      sourcesUsed: ['alpha.public', 'charlie.public'],
      freshSourcesCount: 1,
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    });

    nowTs = 1000;
    bus.publish('market:cvd_futures_agg', {
      symbol: 'BTCUSDT',
      cvd: 10,
      cvdDelta: 1,
      marketType: 'futures',
      bucketSizeMs: 1000,
      ts: 1000,
      bucketStartTs: 0,
      bucketEndTs: 1000,
      sourcesUsed: ['alpha.public'],
      mismatchDetected: false,
      confidenceScore: 0.99,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    });

    const coverage = readiness.getHealthSnapshot()?.sourceCoverage;
    expect(coverage).toBeTruthy();
    expect(coverage?.expected.price).toEqual(['alpha.public', 'beta.public']);
    expect(coverage?.active.price).toEqual(['alpha.public', 'charlie.public']);
    expect(coverage?.unexpected.price).toEqual(['charlie.public']);
    expect(coverage?.missing.price).toEqual(['beta.public']);
    expect(coverage?.coverageRatio.price).toBe(0.5);
    expect(coverage?.unexpectedTotal.price).toBe(1);

    readiness.stop();
  });
});
