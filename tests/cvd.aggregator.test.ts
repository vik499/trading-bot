import { describe, expect, it } from 'vitest';
import {
  CvdAggregator,
  DEFAULT_CVD_MISMATCH_POLICY,
  evaluateCvdMismatchV1,
} from '../src/globalData/CvdAggregator';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type MarketCvdAggEvent, type MarketCvdEvent } from '../src/core/events/EventBus';

type CvdAggWithMismatch = MarketCvdAggEvent & {
  mismatchType?: 'NONE' | 'SIGN' | 'DISPERSION';
  mismatchReason?: 'SIGN' | 'DISPERSION';
  signAgreementRatio?: number | null;
  scaleFactors?: Record<string, number>;
  scaledVenueBreakdown?: Record<string, number>;
};

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

  it('keeps mismatch false for same-sign different magnitudes after scaling', () => {
    const bus = createTestEventBus();
    const agg = new CvdAggregator(bus, { ttlMs: 10_000 });
    const futuresAgg: CvdAggWithMismatch[] = [];

    bus.subscribe('market:cvd_futures_agg', (evt) => futuresAgg.push(evt));
    agg.start();

    const make = (streamId: string, total: number, delta: number): MarketCvdEvent => ({
      symbol: 'BTCUSDT',
      streamId,
      marketType: 'futures',
      bucketStartTs: 0,
      bucketEndTs: 1000,
      bucketSizeMs: 1000,
      cvdTotal: total,
      cvdDelta: delta,
      unit: 'base',
      exchangeTs: 1000,
      meta: createMeta('market', { tsEvent: 1000, tsIngest: 1000, streamId }),
    });

    bus.publish('market:cvd_futures', make('venue.a', 100, 100));
    bus.publish('market:cvd_futures', make('venue.b', 200, 200));
    bus.publish('market:cvd_futures', make('venue.c', 500, 500));

    const last = futuresAgg[futuresAgg.length - 1];
    expect(last.mismatchType).toBe('NONE');
    expect(last.mismatchDetected).toBe(false);
    expect(last.signAgreementRatio).toBeGreaterThanOrEqual(DEFAULT_CVD_MISMATCH_POLICY.signAgreementThreshold);
    expect(last.confidenceScore).toBeDefined();
    expect(last.confidenceScore!).toBeGreaterThan(0.8);

    agg.stop();
  });

  it('flags mismatch on sign divergence', () => {
    const bus = createTestEventBus();
    const agg = new CvdAggregator(bus, { ttlMs: 10_000 });
    const futuresAgg: CvdAggWithMismatch[] = [];

    bus.subscribe('market:cvd_futures_agg', (evt) => futuresAgg.push(evt));
    agg.start();

    const make = (streamId: string, delta: number): MarketCvdEvent => ({
      symbol: 'BTCUSDT',
      streamId,
      marketType: 'futures',
      bucketStartTs: 0,
      bucketEndTs: 1000,
      bucketSizeMs: 1000,
      cvdTotal: delta,
      cvdDelta: delta,
      unit: 'base',
      exchangeTs: 1000,
      meta: createMeta('market', { tsEvent: 1000, tsIngest: 1000, streamId }),
    });

    bus.publish('market:cvd_futures', make('venue.a', 120));
    bus.publish('market:cvd_futures', make('venue.b', -110));

    const last = futuresAgg[futuresAgg.length - 1];
    expect(last.mismatchType).toBe('SIGN');
    expect(last.mismatchReason).toBe('SIGN');
    expect(last.mismatchDetected).toBe(true);
    expect(last.confidenceScore).toBeLessThanOrEqual(DEFAULT_CVD_MISMATCH_POLICY.penaltySign);

    agg.stop();
  });

  it('flags mismatch on extreme outlier after scaling', () => {
    const bus = createTestEventBus();
    const agg = new CvdAggregator(bus, { ttlMs: 10_000 });
    const futuresAgg: CvdAggWithMismatch[] = [];

    bus.subscribe('market:cvd_futures_agg', (evt) => futuresAgg.push(evt));
    agg.start();

    const make = (streamId: string, delta: number): MarketCvdEvent => ({
      symbol: 'BTCUSDT',
      streamId,
      marketType: 'futures',
      bucketStartTs: 0,
      bucketEndTs: 1000,
      bucketSizeMs: 1000,
      cvdTotal: delta,
      cvdDelta: delta,
      unit: 'base',
      exchangeTs: 1000,
      meta: createMeta('market', { tsEvent: 1000, tsIngest: 1000, streamId }),
    });

    bus.publish('market:cvd_futures', make('venue.a', 100));
    bus.publish('market:cvd_futures', make('venue.b', 120));
    bus.publish('market:cvd_futures', make('venue.c', 5000));

    const last = futuresAgg[futuresAgg.length - 1];
    expect(last.mismatchType).toBe('DISPERSION');
    expect(last.mismatchReason).toBe('DISPERSION');
    expect(last.mismatchDetected).toBe(true);
    expect(last.confidenceScore).toBeLessThan(1);

    agg.stop();
  });

  it('ignores near-zero buckets for mismatch detection', () => {
    const bus = createTestEventBus();
    const agg = new CvdAggregator(bus, { ttlMs: 10_000 });
    const futuresAgg: CvdAggWithMismatch[] = [];

    bus.subscribe('market:cvd_futures_agg', (evt) => futuresAgg.push(evt));
    agg.start();

    const make = (streamId: string, delta: number): MarketCvdEvent => ({
      symbol: 'BTCUSDT',
      streamId,
      marketType: 'futures',
      bucketStartTs: 0,
      bucketEndTs: 1000,
      bucketSizeMs: 1000,
      cvdTotal: delta,
      cvdDelta: delta,
      unit: 'base',
      exchangeTs: 1000,
      meta: createMeta('market', { tsEvent: 1000, tsIngest: 1000, streamId }),
    });

    bus.publish('market:cvd_futures', make('venue.a', 0.0001));
    bus.publish('market:cvd_futures', make('venue.b', -0.0002));
    bus.publish('market:cvd_futures', make('venue.c', 0.00015));

    const last = futuresAgg[futuresAgg.length - 1];
    expect(last.mismatchType).toBe('NONE');
    expect(last.mismatchDetected).toBe(false);
    expect(last.signAgreementRatio).toBeNull();

    agg.stop();
  });

  it('applies stronger penalty for SIGN than DISPERSION', () => {
    const policy = DEFAULT_CVD_MISMATCH_POLICY;
    const sign = evaluateCvdMismatchV1(
      'BTCUSDT:futures',
      [
        ['venue.a', 120],
        ['venue.b', -110],
      ],
      new Map(),
      policy
    );
    const dispersion = evaluateCvdMismatchV1(
      'ETHUSDT:futures',
      [
        ['venue.a', 100],
        ['venue.b', 120],
        ['venue.c', 5000],
      ],
      new Map(),
      policy
    );

    expect(sign.mismatchType).toBe('SIGN');
    expect(dispersion.mismatchType).toBe('DISPERSION');
    expect(sign.confidencePenalty).toBeLessThan(dispersion.confidencePenalty);
  });

  it('is deterministic across input ordering', () => {
    const policy = DEFAULT_CVD_MISMATCH_POLICY;
    const stateA = new Map<string, Map<string, number>>();
    const stateB = new Map<string, Map<string, number>>();

    const a = evaluateCvdMismatchV1(
      'BTCUSDT:futures',
      [
        ['venue.a', 100],
        ['venue.b', 120],
        ['venue.c', 5000],
      ],
      stateA,
      policy
    );
    const b = evaluateCvdMismatchV1(
      'BTCUSDT:futures',
      [
        ['venue.c', 5000],
        ['venue.a', 100],
        ['venue.b', 120],
      ],
      stateB,
      policy
    );

    expect(a.mismatchType).toBe(b.mismatchType);
    expect(a.signAgreementRatio).toBe(b.signAgreementRatio);
  });

  it('lowers confidence when stale sources are dropped', () => {
    const bus = createTestEventBus();
    const agg = new CvdAggregator(bus, { ttlMs: 1000 });
    const futuresAgg: MarketCvdAggEvent[] = [];

    bus.subscribe('market:cvd_futures_agg', (evt) => futuresAgg.push(evt));
    agg.start();

    const make = (streamId: string, ts: number, delta: number): MarketCvdEvent => ({
      symbol: 'BTCUSDT',
      streamId,
      marketType: 'futures',
      bucketStartTs: 0,
      bucketEndTs: 1000,
      bucketSizeMs: 1000,
      cvdTotal: delta,
      cvdDelta: delta,
      unit: 'base',
      exchangeTs: ts,
      meta: createMeta('market', { tsEvent: ts, tsIngest: ts, streamId }),
    });

    bus.publish('market:cvd_futures', make('binance.usdm.public', 1000, 100));
    bus.publish('market:cvd_futures', make('bybit.public.linear.v5', 1000, 110));
    const fresh = futuresAgg[futuresAgg.length - 1];

    bus.publish('market:cvd_futures', make('binance.usdm.public', 3000, 105));
    const stale = futuresAgg[futuresAgg.length - 1];

    expect(fresh.confidenceScore).toBeDefined();
    expect(stale.confidenceScore).toBeDefined();
    expect(stale.confidenceScore!).toBeLessThan(fresh.confidenceScore!);

    agg.stop();
  });
});
