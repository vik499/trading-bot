import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../../src/core/events/testing';
import {
  createMeta,
  type DataGapDetected,
  type MarketCvdAggEvent,
} from '../../src/core/events/EventBus';
import { MarketDataReadiness } from '../../src/observability/MarketDataReadiness';

describe('MarketDataReadiness gap telemetry', () => {
  it('populates gapTelemetry when gaps are detected', () => {
    const bus = createTestEventBus();
    let nowTs = 10_000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
      now: () => nowTs,
    });

    readiness.start();

    const timeGap: DataGapDetected = {
      meta: createMeta('storage', { tsEvent: 1000 }),
      symbol: 'BTCUSDT',
      streamId: 'bybit.public.linear.v5',
      topic: 'market:orderbook_l2_delta',
      prevTsExchange: 1000,
      currTsExchange: 7000,
      deltaMs: 6000,
    };

    const flow: MarketCvdAggEvent = {
      symbol: 'BTCUSDT',
      ts: 1000,
      cvd: 10,
      cvdDelta: 1,
      marketType: 'futures',
      bucketStartTs: 0,
      bucketEndTs: 1000,
      bucketSizeMs: 1000,
      sourcesUsed: ['bybit.public.linear.v5'],
      mismatchDetected: false,
      confidenceScore: 0.95,
      meta: createMeta('global_data', { tsEvent: 1000, tsIngest: 1000 }),
    };

    bus.publish('data:gapDetected', timeGap);
    bus.publish('market:cvd_futures_agg', flow);

    const snapshot = readiness.getHealthSnapshot();
    expect(snapshot).toBeTruthy();
    expect(snapshot?.degradedReasons).toContain('GAPS_DETECTED');
    expect(snapshot?.gapTelemetry?.markersCountBySource.onGap).toBeGreaterThan(0);
    expect(snapshot?.gapTelemetry?.markersCountBySource.detectOrderbookQuality).toBeGreaterThan(0);
    expect(snapshot?.gapTelemetry?.markersTop.some((entry) => entry.marker === 'detectOrderbookQuality')).toBe(true);
    expect(snapshot?.gapTelemetry?.inputGapStats.maxMs).toBe(6000);
    expect(snapshot?.gapTelemetry?.inputGapStats.overThresholdCount).toBe(1);

    readiness.stop();
  });

  it('adds GAP_ATTRIBUTION_MISSING warning when evidence is missing', () => {
    const bus = createTestEventBus();
    let nowTs = 120_000;
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      startupGraceWindowMs: 0,
      readinessStabilityWindowMs: 0,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
      now: () => nowTs,
    });

    readiness.start();

    const staleGap: DataGapDetected = {
      meta: createMeta('storage', { tsEvent: 0 }),
      symbol: 'BTCUSDT',
      streamId: 'bybit.public.linear.v5',
      topic: 'market:orderbook_l2_delta',
      prevTsExchange: 0,
      currTsExchange: 6000,
      deltaMs: 6000,
    };

    const flow: MarketCvdAggEvent = {
      symbol: 'BTCUSDT',
      ts: 120_000,
      cvd: 10,
      cvdDelta: 1,
      marketType: 'futures',
      bucketStartTs: 119_000,
      bucketEndTs: 120_000,
      bucketSizeMs: 1000,
      sourcesUsed: ['bybit.public.linear.v5'],
      mismatchDetected: false,
      confidenceScore: 0.95,
      meta: createMeta('global_data', { tsEvent: 120_000, tsIngest: 120_000 }),
    };

    bus.publish('data:gapDetected', staleGap);
    bus.publish('market:cvd_futures_agg', flow);

    const snapshot = readiness.getHealthSnapshot();
    expect(snapshot?.degradedReasons).toContain('GAPS_DETECTED');
    expect(snapshot?.warnings).toContain('GAP_ATTRIBUTION_MISSING');
    expect(snapshot?.gapTelemetry?.inputGapStats.samples).toBe(0);

    readiness.stop();
  });
});
