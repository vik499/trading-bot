import { describe, expect, it } from 'vitest';
import { createMeta, type MarketPriceCanonicalEvent } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { GlobalDataQualityMonitor } from '../src/globalData/GlobalDataQualityMonitor';

describe('OI comparability audit + deterministic baseline', () => {
  it('excludes non-comparable units (contracts vs base) and records suppression context', () => {
    const bus = createTestEventBus();
    const monitor = new GlobalDataQualityMonitor(bus, {
      mismatchThresholdPct: 0.1,
      mismatchWindowMs: 0,
      logThrottleMs: 0,
      oiBaselineStrategy: 'bybit',
    });

    monitor.start();

    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'bybit.public.linear.v5',
      openInterest: 100,
      openInterestUnit: 'contracts',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'okx.public.swap',
      openInterest: 1,
      openInterestUnit: 'base',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 1_000,
      openInterest: 100,
      openInterestUnit: 'contracts',
      venueBreakdown: { 'bybit.public.linear.v5': 100 },
      provider: 'local_oi_agg',
      marketType: 'futures',
      meta: createMeta('global_data', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    const snapshot = monitor.snapshot(10);
    expect(snapshot.mismatchCount).toBe(0);
    expect(snapshot.mismatch?.length).toBe(1);

    const entry = snapshot.mismatch?.[0];
    expect(entry?.suppressed).toBe(true);
    expect(entry?.suppressionReason).toContain('NO_COMPARABLE_UNIT');
    expect(entry?.unitByVenue).toEqual({
      'bybit.public.linear.v5': 'contracts',
      'okx.public.swap': 'base',
    });
    expect(entry?.instrumentByVenue).toEqual({
      'bybit.public.linear.v5': 'BTCUSDT',
      'okx.public.swap': 'BTC-USDT-SWAP',
    });
    expect(entry?.excludedReasons).toEqual({
      'bybit.public.linear.v5': 'NON_COMPARABLE(contracts:no_contract_size)',
      'okx.public.swap': 'NON_COMPARABLE(unit=base)',
    });

    monitor.stop();
  });

  it('converts base->usd via canonical price and avoids mismatch when values align', () => {
    const bus = createTestEventBus();
    const monitor = new GlobalDataQualityMonitor(bus, {
      mismatchThresholdPct: 0.1,
      mismatchWindowMs: 0,
      logThrottleMs: 0,
      expectedIntervalMs: { 'market:price_canonical': 60_000, 'market:oi_agg': 60_000 },
      oiBaselineStrategy: 'bybit',
    });

    monitor.start();

    const canonical: MarketPriceCanonicalEvent = {
      symbol: 'BTCUSDT',
      ts: 1_000,
      marketType: 'futures',
      indexPrice: 100,
      priceTypeUsed: 'index',
      sourcesUsed: ['bybit.public.linear.v5'],
      freshSourcesCount: 1,
      provider: 'local_canonical',
      meta: createMeta('global_data', { tsEvent: 1_000, tsIngest: 1_000 }),
    };
    bus.publish('market:price_canonical', canonical);

    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'bybit.public.linear.v5',
      openInterest: 1,
      openInterestUnit: 'base',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'binance.usdm.public',
      openInterest: 100,
      openInterestUnit: 'usd',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 1_000,
      openInterest: 100,
      openInterestUnit: 'usd',
      venueBreakdown: { 'bybit.public.linear.v5': 1, 'binance.usdm.public': 100 },
      provider: 'local_oi_agg',
      marketType: 'futures',
      meta: createMeta('global_data', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    const snapshot = monitor.snapshot(10);
    expect(snapshot.mismatchCount).toBe(0);
    expect(snapshot.mismatch?.length ?? 0).toBe(0);

    monitor.stop();
  });

  it('is deterministic across event and breakdown order (baseline=median)', () => {
    const run = (order: 'A' | 'B') => {
      const bus = createTestEventBus();
      const monitor = new GlobalDataQualityMonitor(bus, {
        mismatchThresholdPct: 0.1,
        mismatchWindowMs: 0,
        logThrottleMs: 0,
        oiBaselineStrategy: 'median',
      });

      monitor.start();

      const publishRaw = (streamId: string, value: number) =>
        bus.publish('market:oi', {
          symbol: 'BTCUSDT',
          streamId,
          openInterest: value,
          openInterestUnit: 'usd',
          marketType: 'futures',
          meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000 }),
        });

      if (order === 'A') {
        publishRaw('bybit.public.linear.v5', 100);
        publishRaw('binance.usdm.public', 200);
        publishRaw('okx.public.swap', 150);
        bus.publish('market:oi_agg', {
          symbol: 'BTCUSDT',
          ts: 1_000,
          openInterest: 150,
          openInterestUnit: 'usd',
          venueBreakdown: {
            'bybit.public.linear.v5': 100,
            'binance.usdm.public': 200,
            'okx.public.swap': 150,
          },
          provider: 'local_oi_agg',
          marketType: 'futures',
          meta: createMeta('global_data', { tsEvent: 1_000, tsIngest: 1_000 }),
        });
      } else {
        publishRaw('okx.public.swap', 150);
        publishRaw('bybit.public.linear.v5', 100);
        publishRaw('binance.usdm.public', 200);
        bus.publish('market:oi_agg', {
          symbol: 'BTCUSDT',
          ts: 1_000,
          openInterest: 150,
          openInterestUnit: 'usd',
          venueBreakdown: {
            'okx.public.swap': 150,
            'binance.usdm.public': 200,
            'bybit.public.linear.v5': 100,
          },
          provider: 'local_oi_agg',
          marketType: 'futures',
          meta: createMeta('global_data', { tsEvent: 1_000, tsIngest: 1_000 }),
        });
      }

      const snapshot = monitor.snapshot(10);
      const entry = snapshot.mismatch?.[0];
      monitor.stop();

      return {
        mismatchCount: snapshot.mismatchCount,
        baseline: entry?.baseline,
        observed: entry?.observed,
        baselineVenue: entry?.baselineVenue,
        observedVenue: entry?.observedVenue,
        venues: entry?.venues,
        unit: entry?.unit,
        baselineStrategy: entry?.baselineStrategy,
      };
    };

    expect(run('A')).toEqual(run('B'));
  });

  it('suppresses the historical diffPct≈0.89 case with a clear explanation (non-comparable)', () => {
    const bus = createTestEventBus();
    const monitor = new GlobalDataQualityMonitor(bus, {
      mismatchThresholdPct: 0.1,
      mismatchWindowMs: 0,
      logThrottleMs: 0,
      oiBaselineStrategy: 'bybit',
    });

    monitor.start();

    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'bybit.public.linear.v5',
      openInterest: 47_298.491,
      openInterestUnit: 'contracts',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'binance.usdm.public',
      openInterest: 89_455.864,
      openInterestUnit: 'contracts',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    bus.publish('market:oi', {
      symbol: 'BTCUSDT',
      streamId: 'okx.public.swap',
      openInterest: 1,
      openInterestUnit: 'base',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 1_000,
      openInterest: 47_298.491,
      openInterestUnit: 'contracts',
      venueBreakdown: { 'bybit.public.linear.v5': 47_298.491, 'binance.usdm.public': 89_455.864 },
      provider: 'local_oi_agg',
      marketType: 'futures',
      meta: createMeta('global_data', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    const snapshot = monitor.snapshot(10);
    expect(snapshot.mismatchCount).toBe(0);
    expect(snapshot.mismatch?.length).toBe(1);

    const entry = snapshot.mismatch?.[0];
    expect(entry?.suppressed).toBe(true);
    expect(entry?.suppressionReason).toContain('NO_COMPARABLE_UNIT');
    expect(entry?.unit).toBe('contracts');
    expect(entry?.baseline).toBeCloseTo(47_298.491, 6);
    expect(entry?.observed).toBeCloseTo(89_455.864, 6);
    expect(entry?.diffPct).toBeCloseTo(0.891, 3);
    expect(entry?.excludedReasons).toEqual({
      'binance.usdm.public': 'NON_COMPARABLE(contracts:no_contract_size)',
      'bybit.public.linear.v5': 'NON_COMPARABLE(contracts:no_contract_size)',
      'okx.public.swap': 'NON_COMPARABLE(unit=base)',
    });

    monitor.stop();
  });
});

