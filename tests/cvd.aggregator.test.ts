import { describe, expect, it } from 'vitest';
import { CvdAggregator } from '../src/globalData/CvdAggregator';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type MarketCvdAggEvent, type MarketCvdEvent } from '../src/core/events/EventBus';

describe('CvdAggregator', () => {
  it('aggregates spot cvd with weights and drops stale sources', () => {
    const bus = createTestEventBus();
    const agg = new CvdAggregator(bus, { ttlMs: 1000, weights: { s1: 1, s2: 2 } });
    const spotAgg: MarketCvdAggEvent[] = [];

    bus.subscribe('market:cvd_spot_agg', (evt) => spotAgg.push(evt));
    agg.start();

    const make = (streamId: string, ts: number, total: number, delta: number): MarketCvdEvent => ({
      symbol: 'BTCUSDT',
      streamId,
      marketType: 'spot',
      bucketStartTs: 0,
      bucketEndTs: 1000,
      bucketSizeMs: 1000,
      cvdTotal: total,
      cvdDelta: delta,
      unit: 'base',
      exchangeTs: ts,
      meta: createMeta('market', { tsEvent: ts, tsIngest: ts, streamId }),
    });

    bus.publish('market:cvd_spot', make('s1', 1000, 10, 1));
    bus.publish('market:cvd_spot', make('s2', 1000, 20, 2));

    const first = spotAgg[spotAgg.length - 1];
    expect(first.cvd).toBe(10 * 1 + 20 * 2);
    expect(first.cvdDelta).toBe(1 * 1 + 2 * 2);
    expect(first.sourcesUsed).toEqual(expect.arrayContaining(['s1', 's2']));
    expect(first.weightsUsed?.s2).toBe(2);

    bus.publish('market:cvd_spot', {
      symbol: 'BTCUSDT',
      streamId: 's1',
      marketType: 'spot',
      bucketStartTs: 2000,
      bucketEndTs: 3000,
      bucketSizeMs: 1000,
      cvdTotal: 15,
      cvdDelta: 3,
      unit: 'base',
      exchangeTs: 4000,
      meta: createMeta('market', { tsEvent: 4000, tsIngest: 4000, streamId: 's1' }),
    });

    const last = spotAgg[spotAgg.length - 1];
    expect(last.sourcesUsed).toEqual(['s1']);
    expect(last.qualityFlags?.staleSourcesDropped).toEqual(['s2']);

    agg.stop();
  });

  it('computes confidence 0.475 with mismatch + binance penalty', () => {
    const bus = createTestEventBus();
    const agg = new CvdAggregator(bus, { ttlMs: 10_000 });
    const spotAgg: MarketCvdAggEvent[] = [];

    bus.subscribe('market:cvd_spot_agg', (evt) => spotAgg.push(evt));
    agg.start();

    const make = (streamId: string, total: number): MarketCvdEvent => ({
      symbol: 'BTCUSDT',
      streamId,
      marketType: 'spot',
      bucketStartTs: 0,
      bucketEndTs: 1000,
      bucketSizeMs: 1000,
      cvdTotal: total,
      cvdDelta: total,
      unit: 'base',
      exchangeTs: 1000,
      meta: createMeta('market', { tsEvent: 1000, tsIngest: 1000, streamId }),
    });

    bus.publish('market:cvd_spot', make('binance.usdm.public', 100));
    bus.publish('market:cvd_spot', make('bybit.public.linear.v5', 120));

    const last = spotAgg[spotAgg.length - 1];
    expect(last.mismatchDetected).toBe(true);
    expect(last.confidenceScore).toBeDefined();
    expect(last.confidenceScore!).toBeCloseTo(0.475, 6);
    expect(last.confidenceScore!.toFixed(2)).toBe('0.47');

    agg.stop();
  });
});
