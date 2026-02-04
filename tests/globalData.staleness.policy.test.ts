import { describe, it, expect } from 'vitest';
import { createMeta } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { DEFAULT_STALENESS_POLICY_RULES } from '../src/core/observability/stalenessPolicy';
import { GlobalDataQualityMonitor } from '../src/globalData/GlobalDataQualityMonitor';

describe('Staleness policy (defaults)', () => {
  it('does not emit stale for market:oi_agg at ~4 minutes', () => {
    const bus = createTestEventBus();
    const monitor = new GlobalDataQualityMonitor(bus, {
      stalenessPolicy: DEFAULT_STALENESS_POLICY_RULES,
      logThrottleMs: 0,
    });

    const stale: unknown[] = [];
    const degraded: unknown[] = [];
    bus.subscribe('data:stale', (evt) => stale.push(evt));
    bus.subscribe('data:sourceDegraded', (evt) => degraded.push(evt));

    monitor.start();

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 1_000_000,
      openInterest: 100,
      openInterestUnit: 'contracts',
      marketType: 'futures',
      provider: 'local_oi_agg',
      meta: createMeta('global_data', { tsEvent: 1_000_000, tsIngest: 1_000_000 }),
    });

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 1_240_000, // +4min
      openInterest: 101,
      openInterestUnit: 'contracts',
      marketType: 'futures',
      provider: 'local_oi_agg',
      meta: createMeta('global_data', { tsEvent: 1_240_000, tsIngest: 1_240_000 }),
    });

    expect(stale).toHaveLength(0);
    expect(degraded).toHaveLength(0);
    monitor.stop();
  });

  it('does not emit stale for market:funding_agg at ~6 hours', () => {
    const bus = createTestEventBus();
    const monitor = new GlobalDataQualityMonitor(bus, {
      stalenessPolicy: DEFAULT_STALENESS_POLICY_RULES,
      logThrottleMs: 0,
    });

    const stale: unknown[] = [];
    const degraded: unknown[] = [];
    bus.subscribe('data:stale', (evt) => stale.push(evt));
    bus.subscribe('data:sourceDegraded', (evt) => degraded.push(evt));

    monitor.start();

    bus.publish('market:funding_agg', {
      symbol: 'BTCUSDT',
      ts: 10_000_000,
      fundingRate: 0.0001,
      marketType: 'futures',
      provider: 'local_funding_agg',
      meta: createMeta('global_data', { tsEvent: 10_000_000, tsIngest: 10_000_000 }),
    });

    bus.publish('market:funding_agg', {
      symbol: 'BTCUSDT',
      ts: 10_000_000 + 6 * 60 * 60_000, // +6h
      fundingRate: 0.0001,
      marketType: 'futures',
      provider: 'local_funding_agg',
      meta: createMeta('global_data', { tsEvent: 10_000_000 + 6 * 60 * 60_000, tsIngest: 10_000_000 + 6 * 60 * 60_000 }),
    });

    expect(stale).toHaveLength(0);
    expect(degraded).toHaveLength(0);
    monitor.stop();
  });
});

