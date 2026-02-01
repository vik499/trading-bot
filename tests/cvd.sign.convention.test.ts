import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type MarketCvdAggEvent, type MarketCvdEvent } from '../src/core/events/EventBus';
import { CvdAggregator } from '../src/globalData/CvdAggregator';

describe('CVD sign convention normalization', () => {
  it('applies per-stream sign overrides to align conventions', () => {
    const bus = createTestEventBus();
    const agg = new CvdAggregator(bus, {
      ttlMs: 10_000,
      signOverrides: { s2: -1 },
    });

    const outputs: MarketCvdAggEvent[] = [];
    bus.subscribe('market:cvd_futures_agg', (evt) => outputs.push(evt));
    agg.start();

    const make = (streamId: string, total: number, delta: number): MarketCvdEvent => ({
      symbol: 'BTCUSDT',
      streamId,
      marketType: 'futures',
      bucketStartTs: 0,
      bucketEndTs: 1_000,
      bucketSizeMs: 1_000,
      cvdTotal: total,
      cvdDelta: delta,
      unit: 'base',
      exchangeTs: 1_000,
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000, streamId }),
    });

    bus.publish('market:cvd_futures', make('s1', 10, 2));
    bus.publish('market:cvd_futures', make('s2', 10, 2));

    const last = outputs[outputs.length - 1];
    expect(last.cvd).toBeCloseTo(0, 8);
    expect(last.cvdDelta).toBeCloseTo(0, 8);
    expect(last.venueBreakdown?.s1).toBeCloseTo(10, 8);
    expect(last.venueBreakdown?.s2).toBeCloseTo(-10, 8);

    agg.stop();
  });
});
