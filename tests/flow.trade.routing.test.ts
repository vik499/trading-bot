import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../src/core/events/testing';
import { SourceRegistry } from '../src/core/market/SourceRegistry';
import { MarketDataReadiness } from '../src/observability/MarketDataReadiness';
import { CvdCalculator } from '../src/globalData/CvdCalculator';
import { CvdAggregator } from '../src/globalData/CvdAggregator';
import { createMeta, type MarketCvdAggEvent, type TradeEvent } from '../src/core/events/EventBus';

describe('Trade routing -> CVD aggregation', () => {
  it('updates lastSeenRawTs.trades and emits cvd_futures_agg', async () => {
    const bus = createTestEventBus();
    const registry = new SourceRegistry();
    const readiness = new MarketDataReadiness(bus, {
      sourceRegistry: registry,
      bucketMs: 1_000,
      warmingWindowMs: 0,
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: [],
    });
    const cvdCalculator = new CvdCalculator(bus, { bucketMs: 1_000, marketTypes: ['futures'] });
    const cvdAggregator = new CvdAggregator(bus, { ttlMs: 10_000, weights: { 'binance.usdm.public': 1 } });

    const agg: MarketCvdAggEvent[] = [];
    bus.subscribe('market:cvd_futures_agg', (evt) => agg.push(evt));

    readiness.start();
    cvdCalculator.start();
    cvdAggregator.start();

    const trade1: TradeEvent = {
      symbol: 'BTCUSDT',
      streamId: 'binance.usdm.public',
      side: 'Buy',
      price: 100,
      size: 1,
      tradeTs: 1_000,
      exchangeTs: 1_000,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000, streamId: 'binance.usdm.public' }),
    } as TradeEvent;
    const trade2: TradeEvent = {
      symbol: 'BTCUSDT',
      streamId: 'binance.usdm.public',
      side: 'Sell',
      price: 99,
      size: 1,
      tradeTs: 2_000,
      exchangeTs: 2_000,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 2_000, tsIngest: 2_000, streamId: 'binance.usdm.public' }),
    } as TradeEvent;

    bus.publish('market:trade', trade1);
    bus.publish('market:trade', trade2);

    await Promise.resolve();

    const snapshot = registry.snapshot(2_000, 'BTCUSDT', 'futures');
    expect(snapshot.lastSeenRawTs.trades).toBe(2_000);
    expect(snapshot.usedRaw.trades).toContain('binance.usdm.public');
    expect(agg.length).toBeGreaterThan(0);

    cvdCalculator.stop();
    cvdAggregator.stop();
    readiness.stop();
  });
});
