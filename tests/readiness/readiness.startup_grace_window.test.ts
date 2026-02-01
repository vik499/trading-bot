import { describe, expect, it } from 'vitest';
import { createMeta, type MarketCvdAggEvent, type MarketDataStatusPayload } from '../../src/core/events/EventBus';
import { createTestEventBus } from '../../src/core/events/testing';
import { MarketDataReadiness } from '../../src/observability/MarketDataReadiness';

describe('MarketDataReadiness startup grace window', () => {
  it('does not degrade within startup grace window but degrades after', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      logIntervalMs: 0,
      noDataWindowMs: 500,
      expectedSources: 2,
      startupGraceWindowMs: 2_000,
      expectedSourcesByBlock: {
        price: ['s1'],
      },
    });
    const outputs: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => outputs.push(evt));
    readiness.start();

    const makeCvd = (ts: number): MarketCvdAggEvent => ({
      symbol: 'BTCUSDT',
      ts,
      cvd: 10,
      cvdDelta: 1,
      bucketStartTs: ts - 1000,
      bucketEndTs: ts,
      bucketSizeMs: 1000,
      unit: 'base',
      sourcesUsed: ['s1'],
      confidenceScore: 0.9,
      marketType: 'futures',
      meta: createMeta('global_data', { tsEvent: ts, tsIngest: ts }),
    });

    // Within grace window: missing price + ws disconnected should not degrade
    bus.publish('market:disconnected', {
      reason: 'test-disconnect',
      details: {
        symbol: 'BTCUSDT',
        channel: 'orderbook',
        streamId: 'stream-1',
      },
      meta: createMeta('market', { tsEvent: 1000, tsIngest: 1000, streamId: 'stream-1' }),
    });
    bus.publish('market:cvd_futures_agg', makeCvd(1000));

    let last = outputs[outputs.length - 1];
    expect(last.degraded).toBe(false);

    // After grace window: still missing price + ws disconnected => degraded allowed
    bus.publish('market:cvd_futures_agg', makeCvd(4000));

    last = outputs[outputs.length - 1];
    expect(last.degraded).toBe(true);

    readiness.stop();
  });
});
