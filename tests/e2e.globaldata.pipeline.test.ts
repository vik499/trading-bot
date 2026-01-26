import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../src/core/events/testing';
import {
  createMeta,
  type MarketCvdAggEvent,
  type MarketLiquidityAggEvent,
  type MarketLiquidationsAggEvent,
  type MarketPriceIndexEvent,
  type OrderbookL2SnapshotEvent,
  type TradeEvent,
  type LiquidationEvent,
  type TickerEvent,
} from '../src/core/events/EventBus';
import { CvdCalculator } from '../src/globalData/CvdCalculator';
import { CvdAggregator } from '../src/globalData/CvdAggregator';
import { LiquidityAggregator } from '../src/globalData/LiquidityAggregator';
import { LiquidationAggregator } from '../src/globalData/LiquidationAggregator';
import { PriceIndexAggregator } from '../src/globalData/PriceIndexAggregator';

describe('E2E global data pipeline', () => {
  it('emits aggregated events from raw sources', () => {
    const bus = createTestEventBus();
    const cvdCalculator = new CvdCalculator(bus, { bucketMs: 1000 });
    const cvdAggregator = new CvdAggregator(bus, { ttlMs: 5000 });
    const liquidityAggregator = new LiquidityAggregator(bus, {
      depthLevels: 1,
      bucketMs: 1000,
    });
    const liquidationAggregator = new LiquidationAggregator(bus, { bucketMs: 1000, ttlMs: 5000 });
    const priceIndexAggregator = new PriceIndexAggregator(bus, { ttlMs: 5000 });

    const cvdAggEvents: MarketCvdAggEvent[] = [];
    const liquidityAggEvents: MarketLiquidityAggEvent[] = [];
    const liquidationAggEvents: MarketLiquidationsAggEvent[] = [];
    const priceIndexEvents: MarketPriceIndexEvent[] = [];

    bus.subscribe('market:cvd_futures_agg', (evt) => cvdAggEvents.push(evt));
    bus.subscribe('market:liquidity_agg', (evt) => liquidityAggEvents.push(evt));
    bus.subscribe('market:liquidations_agg', (evt) => liquidationAggEvents.push(evt));
    bus.subscribe('market:price_index', (evt) => priceIndexEvents.push(evt));

    cvdCalculator.start();
    cvdAggregator.start();
    liquidityAggregator.start();
    liquidationAggregator.start();
    priceIndexAggregator.start();

    const trade1: TradeEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      side: 'Buy',
      price: 100,
      size: 2,
      tradeTs: 900,
      exchangeTs: 900,
      marketType: 'futures',
      meta: createMeta('market', { ts: 1000 }),
    };
    const trade2: TradeEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      side: 'Sell',
      price: 100,
      size: 1,
      tradeTs: 1900,
      exchangeTs: 1900,
      marketType: 'futures',
      meta: createMeta('market', { ts: 2100 }),
    };
    bus.publish('market:trade', trade1);
    bus.publish('market:trade', trade2);

    const snapshot: OrderbookL2SnapshotEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      updateId: 1,
      exchangeTs: 2500,
      bids: [{ price: 100, size: 1 }],
      asks: [{ price: 101, size: 1 }],
      meta: createMeta('market', { ts: 2500 }),
    };
    bus.publish('market:orderbook_l2_snapshot', snapshot);
    bus.publish('market:orderbook_l2_snapshot', {
      ...snapshot,
      updateId: 2,
      exchangeTs: 3200,
      meta: createMeta('market', { ts: 3200 }),
    });

    const liq1: LiquidationEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      side: 'Buy',
      price: 100,
      size: 1,
      notionalUsd: 100,
      exchangeTs: 2600,
      meta: createMeta('market', { ts: 2600 }),
    };
    const liq2: LiquidationEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      side: 'Sell',
      price: 200,
      size: 1,
      notionalUsd: 200,
      exchangeTs: 3100,
      meta: createMeta('market', { ts: 3100 }),
    };
    bus.publish('market:liquidation', liq1);
    bus.publish('market:liquidation', liq2);

    const ticker: TickerEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      indexPrice: '100',
      exchangeTs: 3200,
      meta: createMeta('market', { ts: 3200 }),
    };
    bus.publish('market:ticker', ticker);

    expect(cvdAggEvents.length).toBeGreaterThan(0);
    const cvdAgg = cvdAggEvents[cvdAggEvents.length - 1];
    expect(cvdAgg.cvd).toBe(2);
    expect(cvdAgg.marketType).toBe('futures');
    expect(cvdAgg.meta.ts).toBe(cvdAgg.ts);

    expect(liquidityAggEvents.length).toBeGreaterThan(0);
    const liquidityAgg = liquidityAggEvents[liquidityAggEvents.length - 1];
    expect(liquidityAgg.bestBid).toBe(100);
    expect(liquidityAgg.bestAsk).toBe(101);
    expect(liquidityAgg.meta.ts).toBe(3000);
    expect(liquidityAgg.depthUnit).toBe('base');

    expect(liquidationAggEvents.length).toBeGreaterThan(0);
    const liquidationAgg = liquidationAggEvents[liquidationAggEvents.length - 1];
    expect(liquidationAgg.liquidationNotional).toBe(100);
    expect(liquidationAgg.unit).toBe('usd');
    expect(liquidationAgg.meta.ts).toBe(3000);

    expect(priceIndexEvents.length).toBeGreaterThan(0);
    const priceIndex = priceIndexEvents[priceIndexEvents.length - 1];
    expect(priceIndex.indexPrice).toBe(100);
    expect(priceIndex.meta.ts).toBe(3200);

    cvdCalculator.stop();
    cvdAggregator.stop();
    liquidityAggregator.stop();
    liquidationAggregator.stop();
    priceIndexAggregator.stop();
  });
});
