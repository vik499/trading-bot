import { describe, expect, it } from 'vitest';
import {
  createMeta,
  type MarketCvdAggEvent,
  type MarketDataStatusPayload,
  type MarketFundingAggEvent,
  type MarketLiquidationsAggEvent,
  type MarketLiquidityAggEvent,
  type MarketOpenInterestAggEvent,
  type MarketPriceCanonicalEvent,
  type TradeEvent,
} from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { MarketDataReadiness } from '../src/observability/MarketDataReadiness';

const makePrice = (ts: number, confidenceScore = 0.9): MarketPriceCanonicalEvent => ({
  symbol: 'BTCUSDT',
  ts,
  indexPrice: 100,
  sourcesUsed: ['s1'],
  freshSourcesCount: 1,
  confidenceScore,
  meta: createMeta('global_data', { ts }),
});

const makeCvd = (ts: number, confidenceScore = 0.9): MarketCvdAggEvent => ({
  symbol: 'BTCUSDT',
  ts,
  cvd: 10,
  cvdDelta: 1,
  bucketStartTs: ts - 1000,
  bucketEndTs: ts,
  bucketSizeMs: 1000,
  unit: 'base',
  sourcesUsed: ['s1'],
  confidenceScore,
  marketType: 'spot',
  meta: createMeta('global_data', { ts }),
});

const makeLiquidity = (ts: number, confidenceScore = 0.9): MarketLiquidityAggEvent => ({
  symbol: 'BTCUSDT',
  ts,
  bestBid: 99,
  bestAsk: 101,
  spread: 2,
  depthBid: 10,
  depthAsk: 10,
  imbalance: 0,
  depthMethod: 'levels',
  sourcesUsed: ['s1'],
  confidenceScore,
  meta: createMeta('global_data', { ts }),
});

const makeOi = (ts: number, confidenceScore = 0.9): MarketOpenInterestAggEvent => ({
  symbol: 'BTCUSDT',
  ts,
  openInterest: 1000,
  openInterestUnit: 'base',
  sourcesUsed: ['s1'],
  confidenceScore,
  meta: createMeta('global_data', { ts }),
});

const makeFunding = (ts: number, confidenceScore = 0.9): MarketFundingAggEvent => ({
  symbol: 'BTCUSDT',
  ts,
  fundingRate: 0.0001,
  sourcesUsed: ['s1'],
  confidenceScore,
  meta: createMeta('global_data', { ts }),
});

const makeLiq = (ts: number, confidenceScore = 0.9): MarketLiquidationsAggEvent => ({
  symbol: 'BTCUSDT',
  ts,
  liquidationCount: 1,
  liquidationNotional: 100,
  unit: 'usd',
  sourcesUsed: ['s1'],
  confidenceScore,
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

function collectStatuses() {
  const bus = createTestEventBus();
  const readiness = new MarketDataReadiness(bus, {
    bucketMs: 1000,
    warmingWindowMs: 1000,
    expectedSources: 1,
    logIntervalMs: 0,
  });
  const outputs: MarketDataStatusPayload[] = [];
  bus.subscribe('system:market_data_status', (evt) => outputs.push(evt));
  readiness.start();
  return { bus, readiness, outputs };
}

describe('MarketDataReadiness', () => {
  it('emits READY when all blocks are confident and warmed', () => {
    const { bus, readiness, outputs } = collectStatuses();

    const bucketTs = 2000;
    bus.publish('market:price_canonical', makePrice(bucketTs));
    bus.publish('market:cvd_spot_agg', makeCvd(bucketTs));
    bus.publish('market:cvd_futures_agg', { ...makeCvd(bucketTs), marketType: 'futures' });
    bus.publish('market:liquidity_agg', makeLiquidity(bucketTs));
    bus.publish('market:oi_agg', makeOi(bucketTs));
    bus.publish('market:funding_agg', makeFunding(bucketTs));
    bus.publish('market:liquidations_agg', makeLiq(bucketTs));

    const bucketTs2 = 3000;
    bus.publish('market:price_canonical', makePrice(bucketTs2));
    bus.publish('market:cvd_spot_agg', makeCvd(bucketTs2));
    bus.publish('market:cvd_futures_agg', { ...makeCvd(bucketTs2), marketType: 'futures' });
    bus.publish('market:liquidity_agg', makeLiquidity(bucketTs2));
    bus.publish('market:oi_agg', makeOi(bucketTs2));
    bus.publish('market:funding_agg', makeFunding(bucketTs2));
    bus.publish('market:liquidations_agg', makeLiq(bucketTs2));

    const last = outputs[outputs.length - 1];
    expect(last.warmingUp).toBe(false);
    expect(last.degraded).toBe(false);
    expect(last.overallConfidence).toBeGreaterThan(0.7);

    readiness.stop();
  });

  it('flags PRICE_STALE when canonical price is missing', () => {
    const { bus, readiness, outputs } = collectStatuses();

    const bucketTs = 1000;
    bus.publish('market:cvd_spot_agg', makeCvd(bucketTs));
    bus.publish('market:cvd_futures_agg', { ...makeCvd(bucketTs), marketType: 'futures' });
    bus.publish('market:liquidity_agg', makeLiquidity(bucketTs));
    bus.publish('market:oi_agg', makeOi(bucketTs));
    bus.publish('market:funding_agg', makeFunding(bucketTs));
    bus.publish('market:liquidations_agg', makeLiq(bucketTs));

    const last = outputs[outputs.length - 1];
    expect(last.degraded).toBe(true);
    expect(last.degradedReasons).toContain('PRICE_STALE');

    readiness.stop();
  });

  it('flags SOURCES_MISSING when active sources fall below expected', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      expectedSources: 2,
      logIntervalMs: 0,
    });
    const outputs: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => outputs.push(evt));
    readiness.start();

    const bucketTs = 1000;
    bus.publish('market:price_canonical', makePrice(bucketTs));
    bus.publish('market:cvd_spot_agg', makeCvd(bucketTs));
    bus.publish('market:cvd_futures_agg', { ...makeCvd(bucketTs), marketType: 'futures' });
    bus.publish('market:liquidity_agg', makeLiquidity(bucketTs));
    bus.publish('market:oi_agg', makeOi(bucketTs));
    bus.publish('market:funding_agg', makeFunding(bucketTs));
    bus.publish('market:liquidations_agg', makeLiq(bucketTs));

    const last = outputs[outputs.length - 1];
    expect(last.degradedReasons).toContain('SOURCES_MISSING');

    readiness.stop();
  });

  it('tracks warming progress deterministically', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 3000,
      expectedSources: 1,
      logIntervalMs: 0,
    });
    const outputs: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => outputs.push(evt));
    readiness.start();

    const bucketTs = 1000;
    bus.publish('market:price_canonical', makePrice(bucketTs));
    bus.publish('market:cvd_spot_agg', makeCvd(bucketTs));
    bus.publish('market:cvd_futures_agg', { ...makeCvd(bucketTs), marketType: 'futures' });
    bus.publish('market:liquidity_agg', makeLiquidity(bucketTs));
    bus.publish('market:oi_agg', makeOi(bucketTs));
    bus.publish('market:funding_agg', makeFunding(bucketTs));
    bus.publish('market:liquidations_agg', makeLiq(bucketTs));

    const first = outputs[outputs.length - 1];
    expect(first.warmingUp).toBe(true);
    expect(first.warmingProgress).toBeCloseTo(0);

    const bucketTs2 = 4000;
    bus.publish('market:price_canonical', makePrice(bucketTs2));
    bus.publish('market:cvd_spot_agg', makeCvd(bucketTs2));
    bus.publish('market:cvd_futures_agg', { ...makeCvd(bucketTs2), marketType: 'futures' });
    bus.publish('market:liquidity_agg', makeLiquidity(bucketTs2));
    bus.publish('market:oi_agg', makeOi(bucketTs2));
    bus.publish('market:funding_agg', makeFunding(bucketTs2));
    bus.publish('market:liquidations_agg', makeLiq(bucketTs2));

    const second = outputs[outputs.length - 1];
    expect(second.warmingUp).toBe(false);
    expect(second.warmingProgress).toBe(1);

    readiness.stop();
  });

  it('seedExpectedSources populates expected before agg', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      logIntervalMs: 0,
      targetMarketType: 'futures',
      expectedSourcesConfig: {
        default: {
          futures: {
            price: ['bybit.public.linear.v5', 'binance.usdm.public'],
            flow: ['bybit.public.linear.v5'],
            liquidity: ['bybit.public.linear.v5'],
            derivatives: ['bybit.public.linear.v5'],
          },
        },
      },
    });
    readiness.seedExpectedSources({ symbol: 'BTCUSDT', marketType: 'futures' });

    const snapshot = (readiness as any).buildSnapshot(1000);
    expect(snapshot.marketType).toBe('futures');
    expect(snapshot.expected.price).toEqual(['bybit.public.linear.v5', 'binance.usdm.public']);
    expect(snapshot.expected.flow).toEqual(['bybit.public.linear.v5']);
    expect(snapshot.expected.liquidity).toEqual(['bybit.public.linear.v5']);
    expect(snapshot.expected.derivatives).toEqual(['bybit.public.linear.v5']);

    readiness.stop();
  });

  it('ignores price_canonical without sourcesUsed', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      logIntervalMs: 0,
      expectedSourcesByBlock: { price: ['s1'] },
    });
    readiness.start();

    bus.publish('market:price_canonical', {
      symbol: 'BTCUSDT',
      ts: 1000,
      indexPrice: 100,
      confidenceScore: 0.9,
      meta: createMeta('global_data', { ts: 1000 }),
    } as MarketPriceCanonicalEvent);

    const snapshot = (readiness as any).buildSnapshot(1000);
    expect(snapshot.lastSeenAggTs.priceCanonical).toBeNull();
    expect(snapshot.usedAgg.price).toEqual([]);

    readiness.stop();
  });

  it('uses separate stale window for derivatives', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      logIntervalMs: 0,
      expectedSourcesByBlock: { price: ['s1'], derivatives: ['s1'] },
      expectedDerivativeKinds: ['funding'],
      confidenceStaleWindowMs: 1000,
      derivativesStaleWindowMs: 10_000,
    });
    const outputs: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => outputs.push(evt));
    readiness.start();

    bus.publish('market:price_canonical', makePrice(10_000));
    bus.publish('market:funding_agg', makeFunding(5_000));

    const last = outputs[outputs.length - 1];
    expect(last.degradedReasons).not.toContain('DERIVATIVES_LOW_CONF');

    readiness.stop();
  });

  it('does not degrade when liquidity/derivatives expected empty', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      logIntervalMs: 0,
      expectedSourcesByBlock: {
        price: ['s1'],
        flow: ['s1'],
        liquidity: [],
        derivatives: [],
      },
      expectedFlowTypes: ['spot', 'futures'],
      expectedDerivativeKinds: [],
    });
    const outputs: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => outputs.push(evt));
    readiness.start();

    const bucketTs = 1000;
    bus.publish('market:price_canonical', makePrice(bucketTs));
    bus.publish('market:cvd_spot_agg', makeCvd(bucketTs));
    bus.publish('market:cvd_futures_agg', { ...makeCvd(bucketTs), marketType: 'futures' });

    const last = outputs[outputs.length - 1];
    expect(last.blockConfidence.liquidity).toBe(1);
    expect(last.blockConfidence.derivatives).toBe(1);
    expect(last.degradedReasons).not.toContain('LIQUIDITY_LOW_CONF');
    expect(last.degradedReasons).not.toContain('DERIVATIVES_LOW_CONF');

    readiness.stop();
  });

  it('pins snapshot marketType when targetMarketType provided', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      logIntervalMs: 0,
      expectedSourcesByBlock: {
        price: ['s1'],
        flow: ['s1'],
      },
      expectedFlowTypes: ['spot'],
      targetMarketType: 'futures',
    });
    const outputs: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => outputs.push(evt));
    readiness.start();

    const bucketTs = 1000;
    bus.publish('market:trade', makeTrade(bucketTs, 'spot', 'spot-stream'));
    bus.publish('market:trade', makeTrade(bucketTs + 1, 'futures', 'futures-stream'));

    const snapshot = (readiness as any).buildSnapshot(1000);
    expect(snapshot.marketType).toBe('futures');
    expect(snapshot.usedRaw.trades).toEqual(['futures-stream']);

    readiness.stop();
  });
});
