import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createMeta, type AnalyticsFeaturesEvent, type AnalyticsLiquidityEvent, type AnalyticsMarketViewEvent, type MarketOpenInterestAggEvent } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { MarketViewBuilder } from '../src/analytics/MarketViewBuilder';

describe('MarketViewBuilder', () => {
  it('combines micro, flow, and liquidity into analytics:market_view', () => {
    const bus = createTestEventBus();
    const builder = new MarketViewBuilder(bus);
    builder.start();

    const views: AnalyticsMarketViewEvent[] = [];
    bus.subscribe('analytics:market_view', (evt) => views.push(evt));

    const oiAgg: MarketOpenInterestAggEvent = {
      symbol: 'BTCUSDT',
      ts: 1000,
      openInterest: 1200,
      openInterestUnit: 'base',
      marketType: 'futures',
      meta: createMeta('global_data', { ts: 1000 }),
    };
    bus.publish('market:oi_agg', oiAgg);

    const features: AnalyticsFeaturesEvent = {
      symbol: 'BTCUSDT',
      ts: 2000,
      lastPrice: 100,
      emaFast: 101,
      emaSlow: 99,
      rsi: 55,
      atr: 0.5,
      sampleCount: 30,
      featuresReady: true,
      windowSize: 30,
      klineTf: '5m',
      sourceTopic: 'market:kline',
      meta: createMeta('analytics', { ts: 2000, correlationId: 'cid-1' }),
    };
    bus.publish('analytics:features', features);

    const liquidity: AnalyticsLiquidityEvent = {
      symbol: 'BTCUSDT',
      ts: 3000,
      bestBid: 99,
      bestAsk: 101,
      spread: 2,
      depthBid: 1000,
      depthAsk: 900,
      imbalance: 0.05,
      meta: createMeta('analytics', { ts: 3000, correlationId: 'cid-1' }),
    };
    bus.publish('analytics:liquidity', liquidity);

    const last = views[views.length - 1];
    expect(last.symbol).toBe('BTCUSDT');
    expect(last.meta.ts).toBe(3000);
    expect(last.meta.correlationId).toBe('cid-1');
    expect(last.micro?.tf).toBe('5m');
    expect(last.micro?.emaFast).toBe(101);
    expect(last.flow?.oiAgg).toBe(1200);
    expect(last.liquidity?.bestBid).toBe(99);

    builder.stop();
  });
});
