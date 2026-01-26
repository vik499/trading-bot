import { describe, expect, it } from 'vitest';
import {
  createMeta,
  type MarketCvdAggEvent,
  type MarketPriceCanonicalEvent,
  type TradeEvent,
} from '../../src/core/events/EventBus';
import { createTestEventBus } from '../../src/core/events/testing';
import { MarketDataReadiness } from '../../src/observability/MarketDataReadiness';

const makePrice = (
  ts: number,
  marketType: 'spot' | 'futures',
  sourcesUsed: string[]
): MarketPriceCanonicalEvent => ({
  symbol: 'BTCUSDT',
  ts,
  marketType,
  indexPrice: 100,
  sourcesUsed,
  freshSourcesCount: sourcesUsed.length,
  confidenceScore: 0.9,
  meta: createMeta('global_data', { ts }),
});

const makeCvd = (
  ts: number,
  marketType: 'spot' | 'futures',
  sourcesUsed: string[]
): MarketCvdAggEvent => ({
  symbol: 'BTCUSDT',
  ts,
  cvd: 10,
  cvdDelta: 1,
  bucketStartTs: ts - 1000,
  bucketEndTs: ts,
  bucketSizeMs: 1000,
  unit: 'base',
  sourcesUsed,
  confidenceScore: 0.9,
  marketType,
  meta: createMeta('global_data', { ts }),
});

const makeTrade = (ts: number, marketType: 'spot' | 'futures', streamId: string): TradeEvent => ({
  symbol: 'BTCUSDT',
  streamId,
  side: 'Buy',
  price: 100,
  size: 1,
  tradeTs: ts,
  exchangeTs: ts,
  marketType,
  meta: createMeta('market', { ts }),
});

describe('MarketDataReadiness ignores non-target marketType events', () => {
  it('keeps snapshot pinned to target and ignores spot sources', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      logIntervalMs: 0,
      targetMarketType: 'futures',
      expectedSourcesByBlock: {
        price: ['s1'],
        flow: ['s1'],
        liquidity: ['s1'],
        derivatives: ['s1'],
      },
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: ['oi'],
    });

    readiness.seedExpectedSources({ symbol: 'BTCUSDT', marketType: 'futures' });
    readiness.start();

    bus.publish('market:trade', makeTrade(1000, 'spot', 'spot-stream'));
    bus.publish('market:price_canonical', makePrice(1000, 'spot', ['spot-src']));
    bus.publish('market:cvd_spot_agg', makeCvd(1000, 'spot', ['spot-src']));

    const snapshotAfterSpot = (readiness as any).buildSnapshot(1000);
    expect(snapshotAfterSpot.marketType).toBe('futures');
    expect(snapshotAfterSpot.usedRaw.trades).toEqual([]);
    expect(snapshotAfterSpot.usedAgg.price).toEqual([]);
    expect(snapshotAfterSpot.lastSeenAggTs.priceCanonical).toBeNull();

    bus.publish('market:trade', makeTrade(2000, 'futures', 'futures-stream'));
    bus.publish('market:price_canonical', makePrice(2000, 'futures', ['futures-src']));
    bus.publish('market:cvd_futures_agg', makeCvd(2000, 'futures', ['futures-src']));

    const snapshotAfterFutures = (readiness as any).buildSnapshot(2000);
    expect(snapshotAfterFutures.marketType).toBe('futures');
    expect(snapshotAfterFutures.usedRaw.trades).toEqual(['futures-stream']);
    expect(snapshotAfterFutures.usedAgg.price).toEqual(['futures-src']);
    expect(snapshotAfterFutures.lastSeenAggTs.priceCanonical).toBe(2000);

    readiness.stop();
  });
});
