import { describe, it, expect } from 'vitest';
import { createMeta } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { GlobalDataQualityMonitor } from '../src/globalData/GlobalDataQualityMonitor';

describe('GlobalDataQualityMonitor mismatch context', () => {
  it('exports baseline/observed + venues in snapshot for active mismatches', () => {
    const bus = createTestEventBus();
    const monitor = new GlobalDataQualityMonitor(bus, {
      mismatchThresholdPct: 0.1,
      mismatchWindowMs: 0,
      logThrottleMs: 0,
    });

    monitor.start();

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 1_000,
      openInterest: 100,
      openInterestUnit: 'contracts',
      venueBreakdown: { bybit: 100, binance: 200, okx: 150 },
      provider: 'local_oi_agg',
      marketType: 'futures',
      meta: createMeta('global_data', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    const snapshot = monitor.snapshot(10);
    expect(snapshot.mismatchCount).toBe(1);
    const entry = snapshot.mismatch?.[0];
    expect(entry?.key).toBe('market:oi_agg:BTCUSDT:local_oi_agg');
    expect(entry?.baseline).toBe(100);
    expect(entry?.observed).toBe(200);
    expect(entry?.baselineVenue).toBe('bybit');
    expect(entry?.observedVenue).toBe('binance');
    expect(entry?.venues).toEqual(['binance', 'bybit', 'okx']);
    expect(entry?.venueBreakdown).toEqual({ binance: 200, bybit: 100, okx: 150 });

    monitor.stop();
  });
});
