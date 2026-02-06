import { describe, it, expect } from 'vitest';
import { createMeta } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { GlobalDataQualityMonitor } from '../src/globalData/GlobalDataQualityMonitor';

describe('GlobalDataQualityMonitor', () => {
  it('emits stale -> degraded -> recovered for aggregated topics', () => {
    const bus = createTestEventBus();
    const monitor = new GlobalDataQualityMonitor(bus, {
      expectedIntervalMs: { 'market:oi_agg': 1_000 },
      staleMultiplier: 2,
      logThrottleMs: 0,
    });

    const staleEvents: Array<{ topic: string }> = [];
    const degraded: Array<{ sourceId: string }> = [];
    const recovered: Array<{ sourceId: string }> = [];
    bus.subscribe('data:stale', (evt) => staleEvents.push(evt));
    bus.subscribe('data:sourceDegraded', (evt) => degraded.push(evt));
    bus.subscribe('data:sourceRecovered', (evt) => recovered.push(evt));

    monitor.start();

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 1_000,
      openInterest: 100,
      openInterestUnit: 'base',
      venueBreakdown: { bybit: 100, binance: 101, okx: 99 },
      provider: 'local_oi_agg',
      marketType: 'futures',
      meta: createMeta('global_data', { ts: 1_000 }),
    });

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 4_000,
      openInterest: 102,
      openInterestUnit: 'base',
      venueBreakdown: { bybit: 102, binance: 103, okx: 101 },
      provider: 'local_oi_agg',
      marketType: 'futures',
      meta: createMeta('global_data', { ts: 4_000 }),
    });

    expect(staleEvents).toHaveLength(1);
    expect(degraded).toHaveLength(1);

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 4_500,
      openInterest: 103,
      openInterestUnit: 'base',
      venueBreakdown: { bybit: 103, binance: 104, okx: 102 },
      provider: 'local_oi_agg',
      marketType: 'futures',
      meta: createMeta('global_data', { ts: 4_500 }),
    });

    expect(recovered).toHaveLength(1);
    monitor.stop();
  });

  it('emits mismatch after sustained divergence and recovers on convergence', () => {
    const bus = createTestEventBus();
    const monitor = new GlobalDataQualityMonitor(bus, {
      mismatchThresholdPct: 0.1,
      mismatchWindowMs: 1_000,
      logThrottleMs: 0,
    });

    const mismatches: Array<{ topic: string }> = [];
    const degraded: Array<{ sourceId: string }> = [];
    const recovered: Array<{ sourceId: string }> = [];
    bus.subscribe('data:mismatch', (evt) => mismatches.push(evt));
    bus.subscribe('data:sourceDegraded', (evt) => degraded.push(evt));
    bus.subscribe('data:sourceRecovered', (evt) => recovered.push(evt));

    monitor.start();

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 1_000,
      openInterest: 100,
      openInterestUnit: 'base',
      venueBreakdown: { bybit: 100, binance: 200, okx: 150 },
      provider: 'local_oi_agg',
      marketType: 'futures',
      meta: createMeta('global_data', { ts: 1_000 }),
    });

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 2_500,
      openInterest: 110,
      openInterestUnit: 'base',
      venueBreakdown: { bybit: 110, binance: 220, okx: 180 },
      provider: 'local_oi_agg',
      marketType: 'futures',
      meta: createMeta('global_data', { ts: 2_500 }),
    });

    expect(mismatches).toHaveLength(1);
    expect(degraded).toHaveLength(1);

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 2_600,
      openInterest: 105,
      openInterestUnit: 'base',
      venueBreakdown: { bybit: 105, binance: 108, okx: 106 },
      provider: 'local_oi_agg',
      marketType: 'futures',
      meta: createMeta('global_data', { ts: 2_600 }),
    });

    expect(recovered).toHaveLength(1);
    monitor.stop();
  });
});
