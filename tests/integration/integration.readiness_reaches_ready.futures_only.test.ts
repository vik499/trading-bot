import { describe, expect, it } from 'vitest';
import {
  createMeta,
  type MarketCvdAggEvent,
  type MarketDataStatusPayload,
  type MarketFundingAggEvent,
  type MarketOpenInterestAggEvent,
  type MarketPriceCanonicalEvent,
  type MarketLiquidityAggEvent,
  type TradeEvent,
} from '../../src/core/events/EventBus';
import { createTestEventBus } from '../../src/core/events/testing';
import { MarketDataReadiness } from '../../src/observability/MarketDataReadiness';

const makePrice = (ts: number): MarketPriceCanonicalEvent => ({
  symbol: 'BTCUSDT',
  ts,
  marketType: 'futures',
  indexPrice: 100,
  sourcesUsed: ['binance.usdm.public'],
  freshSourcesCount: 1,
  confidenceScore: 0.95,
  meta: createMeta('global_data', { ts }),
});

const makeCvd = (ts: number): MarketCvdAggEvent => ({
  symbol: 'BTCUSDT',
  ts,
  cvd: 10,
  cvdDelta: 1,
  bucketStartTs: ts - 1000,
  bucketEndTs: ts,
  bucketSizeMs: 1000,
  unit: 'base',
  sourcesUsed: ['binance.usdm.public'],
  confidenceScore: 0.9,
  marketType: 'futures',
  meta: createMeta('global_data', { ts }),
});

const makeLiquidity = (ts: number): MarketLiquidityAggEvent => ({
  symbol: 'BTCUSDT',
  ts,
  bestBid: 99,
  bestAsk: 101,
  spread: 2,
  depthBid: 10,
  depthAsk: 10,
  imbalance: 0,
  depthMethod: 'levels',
  sourcesUsed: ['binance.usdm.public'],
  confidenceScore: 0.9,
  meta: createMeta('global_data', { ts }),
});

const makeOi = (ts: number): MarketOpenInterestAggEvent => ({
  symbol: 'BTCUSDT',
  ts,
  openInterest: 1000,
  openInterestUnit: 'base',
  sourcesUsed: ['binance.usdm.public'],
  confidenceScore: 0.9,
  meta: createMeta('global_data', { ts }),
});

const makeFunding = (ts: number): MarketFundingAggEvent => ({
  symbol: 'BTCUSDT',
  ts,
  fundingRate: 0.0001,
  sourcesUsed: ['binance.usdm.public'],
  confidenceScore: 0.9,
  meta: createMeta('global_data', { ts }),
});

const makeTrade = (ts: number): TradeEvent => ({
  symbol: 'BTCUSDT',
  streamId: 'binance.usdm.public',
  side: 'Buy',
  price: 100,
  size: 1,
  tradeTs: ts,
  exchangeTs: ts,
  marketType: 'futures',
  meta: createMeta('market', { ts }),
});

describe('Integration readiness reaches READY (futures only)', () => {
  it('reaches READY with synthetic futures stream', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 1000,
      logIntervalMs: 0,
      targetMarketType: 'futures',
      expectedSourcesByBlock: {
        price: ['binance.usdm.public'],
        flow: ['binance.usdm.public'],
        liquidity: ['binance.usdm.public'],
        derivatives: ['binance.usdm.public'],
      },
      expectedFlowTypes: ['futures'],
      expectedDerivativeKinds: ['oi', 'funding'],
      derivativesStaleWindowMs: 15 * 60_000,
    });
    const outputs: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => outputs.push(evt));
    readiness.start();

    const t1 = 1000;
    bus.publish('market:trade', makeTrade(t1));
    bus.publish('market:price_canonical', makePrice(t1));
    bus.publish('market:cvd_futures_agg', makeCvd(t1));
    bus.publish('market:liquidity_agg', makeLiquidity(t1));
    bus.publish('market:oi_agg', makeOi(t1));
    bus.publish('market:funding_agg', makeFunding(t1));

    const t2 = 2000;
    bus.publish('market:trade', makeTrade(t2));
    bus.publish('market:price_canonical', makePrice(t2));
    bus.publish('market:cvd_futures_agg', makeCvd(t2));
    bus.publish('market:liquidity_agg', makeLiquidity(t2));
    bus.publish('market:oi_agg', makeOi(t2));
    bus.publish('market:funding_agg', makeFunding(t2));

    const last = outputs[outputs.length - 1];
    expect(last.warmingUp).toBe(false);
    expect(last.degraded).toBe(false);
    expect(last.overallConfidence).toBeGreaterThan(0.7);

    readiness.stop();
  });
});
