import { describe, it, expect } from 'vitest';
import { createMeta } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { GlobalDataQualityMonitor } from '../src/globalData/GlobalDataQualityMonitor';

describe('GlobalDataQualityMonitor mismatch context', () => {
  it('exports baseline/observed + OI comparability context for active mismatches', () => {
    const bus = createTestEventBus();
    const monitor = new GlobalDataQualityMonitor(bus, {
      mismatchThresholdPct: 0.1,
      mismatchWindowMs: 0,
      logThrottleMs: 0,
      oiBaselineStrategy: 'bybit',
    });

    monitor.start();

    // Provide raw OI audit inputs. Two USD-notional sources are comparable; one contracts source is not.
    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'bybit.public.linear.v5',
      openInterest: 100,
      openInterestUnit: 'usd',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000 }),
    });
    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'binance.usdm.public',
      openInterest: 200,
      openInterestUnit: 'usd',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000 }),
    });
    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'okx.public.swap',
      openInterest: 150,
      openInterestUnit: 'contracts',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 1_000,
      openInterest: 100,
      openInterestUnit: 'usd',
      venueBreakdown: { 'bybit.public.linear.v5': 100, 'binance.usdm.public': 200 },
      provider: 'local_oi_agg',
      marketType: 'futures',
      meta: createMeta('global_data', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    const snapshot = monitor.snapshot(10);
    expect(snapshot.mismatchCount).toBe(1);
    const entry = snapshot.mismatch?.[0];
    expect(entry?.key).toBe('market:oi_agg:BTCUSDT:local_oi_agg');
    expect(entry?.baselineStrategy).toBe('bybit');
    expect(entry?.comparableVenues).toEqual(['binance.usdm.public', 'bybit.public.linear.v5']);
    expect(entry?.excludedVenues).toEqual(['okx.public.swap']);
    expect(entry?.excludedReasons).toEqual({ 'okx.public.swap': 'NON_COMPARABLE(contracts:no_contract_size)' });
    expect(entry?.baseline).toBe(100);
    expect(entry?.observed).toBe(200);
    expect(entry?.baselineVenue).toBe('bybit.public.linear.v5');
    expect(entry?.observedVenue).toBe('binance.usdm.public');
    expect(entry?.venues).toEqual(['binance.usdm.public', 'bybit.public.linear.v5', 'okx.public.swap']);
    expect(entry?.unit).toBe('usd');
    expect(entry?.venueBreakdown).toEqual({ 'binance.usdm.public': 200, 'bybit.public.linear.v5': 100 });

    monitor.stop();
  });
});
