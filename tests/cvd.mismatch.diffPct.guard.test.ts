import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type MarketCvdAggEvent } from '../src/core/events/EventBus';
import { GlobalDataQualityMonitor } from '../src/globalData/GlobalDataQualityMonitor';

describe('CVD mismatch diffPct guard', () => {
  it('does not emit mismatch when baseline is near zero and abs diff below epsilon', () => {
    const bus = createTestEventBus();
    const monitor = new GlobalDataQualityMonitor(bus, {
      mismatchThresholdPct: 0.1,
      mismatchWindowMs: 0,
      logThrottleMs: 0,
      mismatchBaselineEpsilon: 0.001,
    });

    const mismatches: Array<{ topic: string }> = [];
    bus.subscribe('data:mismatch', (evt) => mismatches.push(evt));

    monitor.start();

    const evt: MarketCvdAggEvent = {
      symbol: 'BTCUSDT',
      ts: 1_000,
      cvd: 0.00012,
      cvdFutures: 0.00012,
      venueBreakdown: { bybit: 0.0001, binance: 0.00015 },
      bucketStartTs: 0,
      bucketEndTs: 1_000,
      bucketSizeMs: 1_000,
      unit: 'base',
      marketType: 'futures',
      meta: createMeta('global_data', { tsEvent: 1_000, tsIngest: 1_000 }),
    };

    bus.publish('market:cvd_futures_agg', evt);

    expect(mismatches).toHaveLength(0);
    monitor.stop();
  });

  it('uses absolute diff when baseline is near zero', () => {
    const bus = createTestEventBus();
    const monitor = new GlobalDataQualityMonitor(bus, {
      mismatchThresholdPct: 0.1,
      mismatchWindowMs: 0,
      logThrottleMs: 0,
      mismatchBaselineEpsilon: 0.001,
    });

    const mismatches: Array<{ topic: string }> = [];
    bus.subscribe('data:mismatch', (evt) => mismatches.push(evt));

    monitor.start();

    const evt: MarketCvdAggEvent = {
      symbol: 'BTCUSDT',
      ts: 2_000,
      cvd: 0.02,
      cvdFutures: 0.02,
      venueBreakdown: { bybit: 0.0001, binance: 0.01 },
      bucketStartTs: 1_000,
      bucketEndTs: 2_000,
      bucketSizeMs: 1_000,
      unit: 'base',
      marketType: 'futures',
      meta: createMeta('global_data', { tsEvent: 2_000, tsIngest: 2_000 }),
    };

    bus.publish('market:cvd_futures_agg', evt);

    expect(mismatches).toHaveLength(1);
    monitor.stop();
  });
});
