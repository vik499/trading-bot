import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import path from 'node:path';
import os from 'node:os';
import { mkdtemp, rm } from 'node:fs/promises';
import {
  createMeta,
  type FundingRateEvent,
  type LiquidationEvent,
  type MarketCvdAggEvent,
  type MarketFundingAggEvent,
  type MarketLiquidityAggEvent,
  type MarketLiquidationsAggEvent,
  type MarketOpenInterestAggEvent,
  type MarketPriceCanonicalEvent,
  type MarketPriceIndexEvent,
  type OpenInterestEvent,
  type OrderbookL2SnapshotEvent,
  type TickerEvent,
  type TradeEvent,
} from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { createEventJournal } from '../src/storage/eventJournal';
import { createJournalReplayRunner } from '../src/replay/JournalReplayRunner';
import { OpenInterestAggregator } from '../src/globalData/OpenInterestAggregator';
import { FundingAggregator } from '../src/globalData/FundingAggregator';
import { CvdCalculator } from '../src/globalData/CvdCalculator';
import { CvdAggregator } from '../src/globalData/CvdAggregator';
import { LiquidityAggregator } from '../src/globalData/LiquidityAggregator';
import { LiquidationAggregator } from '../src/globalData/LiquidationAggregator';
import { PriceIndexAggregator } from '../src/globalData/PriceIndexAggregator';
import { CanonicalPriceAggregator } from '../src/globalData/CanonicalPriceAggregator';

const SYMBOL = 'BTCUSDT';
const STREAM_ID = 'stream-1';
const RUN_ID = 'run-determinism';

function startAggregators(bus: ReturnType<typeof createTestEventBus>) {
  const oiAgg: MarketOpenInterestAggEvent[] = [];
  const fundingAgg: MarketFundingAggEvent[] = [];
  const cvdSpotAgg: MarketCvdAggEvent[] = [];
  const cvdFuturesAgg: MarketCvdAggEvent[] = [];
  const liquidityAgg: MarketLiquidityAggEvent[] = [];
  const liquidationAgg: MarketLiquidationsAggEvent[] = [];
  const priceIndex: MarketPriceIndexEvent[] = [];
  const priceCanonical: MarketPriceCanonicalEvent[] = [];

  const oiAggregator = new OpenInterestAggregator(bus, { ttlMs: 5_000 });
  const fundingAggregator = new FundingAggregator(bus, { ttlMs: 5_000 });
  const cvdCalculator = new CvdCalculator(bus, { bucketMs: 1_000 });
  const cvdAggregator = new CvdAggregator(bus, { ttlMs: 5_000 });
  const liquidityAggregator = new LiquidityAggregator(bus, { ttlMs: 5_000, depthLevels: 1, bucketMs: 1_000 });
  const liquidationAggregator = new LiquidationAggregator(bus, { ttlMs: 5_000, bucketMs: 1_000 });
  const priceIndexAggregator = new PriceIndexAggregator(bus, { ttlMs: 5_000 });
  const canonicalPriceAggregator = new CanonicalPriceAggregator(bus, { ttlMs: 5_000 });

  bus.subscribe('market:oi_agg', (evt) => oiAgg.push(evt));
  bus.subscribe('market:funding_agg', (evt) => fundingAgg.push(evt));
  bus.subscribe('market:cvd_spot_agg', (evt) => cvdSpotAgg.push(evt));
  bus.subscribe('market:cvd_futures_agg', (evt) => cvdFuturesAgg.push(evt));
  bus.subscribe('market:liquidity_agg', (evt) => liquidityAgg.push(evt));
  bus.subscribe('market:liquidations_agg', (evt) => liquidationAgg.push(evt));
  bus.subscribe('market:price_index', (evt) => priceIndex.push(evt));
  bus.subscribe('market:price_canonical', (evt) => priceCanonical.push(evt));

  oiAggregator.start();
  fundingAggregator.start();
  cvdCalculator.start();
  cvdAggregator.start();
  liquidityAggregator.start();
  liquidationAggregator.start();
  priceIndexAggregator.start();
  canonicalPriceAggregator.start();

  return {
    outputs: {
      oiAgg,
      fundingAgg,
      cvdSpotAgg,
      cvdFuturesAgg,
      liquidityAgg,
      liquidationAgg,
      priceIndex,
      priceCanonical,
    },
    stop: () => {
      canonicalPriceAggregator.stop();
      priceIndexAggregator.stop();
      liquidationAggregator.stop();
      liquidityAggregator.stop();
      cvdAggregator.stop();
      cvdCalculator.stop();
      fundingAggregator.stop();
      oiAggregator.stop();
    },
  };
}

describe('Global data replay determinism', () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), 'globaldata-replay-'));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it('replays identical aggregates from journaled raw events', async () => {
    const bus = createTestEventBus();
    const { outputs: original, stop } = startAggregators(bus);
    const journal = createEventJournal(bus, { baseDir: tmpDir, streamId: STREAM_ID, runId: RUN_ID, flushIntervalMs: 1, maxBatchSize: 1 });
    journal.start();

    const trade1: TradeEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      side: 'Buy',
      price: 100,
      size: 2,
      tradeTs: 900,
      exchangeTs: 900,
      marketType: 'futures',
      meta: createMeta('market', { ts: 900 }),
    };
    const trade2: TradeEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      side: 'Sell',
      price: 100,
      size: 1,
      tradeTs: 1_900,
      exchangeTs: 1_900,
      marketType: 'futures',
      meta: createMeta('market', { ts: 1_900 }),
    };
    bus.publish('market:trade', trade1);
    bus.publish('market:trade', trade2);

    const orderbook1: OrderbookL2SnapshotEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      updateId: 1,
      exchangeTs: 1_100,
      bids: [{ price: 100, size: 1 }],
      asks: [{ price: 101, size: 1 }],
      meta: createMeta('market', { ts: 1_100 }),
    };
    const orderbook2: OrderbookL2SnapshotEvent = {
      ...orderbook1,
      updateId: 2,
      exchangeTs: 2_100,
      meta: createMeta('market', { ts: 2_100 }),
    };
    bus.publish('market:orderbook_l2_snapshot', orderbook1);
    bus.publish('market:orderbook_l2_snapshot', orderbook2);

    const liq1: LiquidationEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      side: 'Buy',
      price: 100,
      size: 1,
      notionalUsd: 100,
      exchangeTs: 1_200,
      meta: createMeta('market', { ts: 1_200 }),
    };
    const liq2: LiquidationEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      side: 'Sell',
      price: 100,
      size: 1,
      notionalUsd: 100,
      exchangeTs: 2_100,
      meta: createMeta('market', { ts: 2_100 }),
    };
    bus.publish('market:liquidation', liq1);
    bus.publish('market:liquidation', liq2);

    const oi: OpenInterestEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      openInterest: 10,
      openInterestUnit: 'base',
      exchangeTs: 1_300,
      meta: createMeta('market', { ts: 1_300 }),
    };
    bus.publish('market:oi', oi);

    const funding: FundingRateEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      fundingRate: 0.0001,
      exchangeTs: 1_400,
      nextFundingTs: 2_400,
      meta: createMeta('market', { ts: 1_400 }),
    };
    bus.publish('market:funding', funding);

    const ticker: TickerEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      indexPrice: '100',
      markPrice: '99.5',
      lastPrice: '100.2',
      exchangeTs: 1_500,
      meta: createMeta('market', { ts: 1_500 }),
    };
    bus.publish('market:ticker', ticker);

    await journal.stop();
    stop();

    const replayBus = createTestEventBus();
    const { outputs: replayed, stop: stopReplay } = startAggregators(replayBus);

    await createJournalReplayRunner(replayBus, {
      baseDir: tmpDir,
      streamId: STREAM_ID,
      symbol: SYMBOL,
      runId: RUN_ID,
      topic: 'market:trade',
      mode: 'max',
    }).run();
    await createJournalReplayRunner(replayBus, {
      baseDir: tmpDir,
      streamId: STREAM_ID,
      symbol: SYMBOL,
      runId: RUN_ID,
      topic: 'market:orderbook_l2_snapshot',
      mode: 'max',
    }).run();
    await createJournalReplayRunner(replayBus, {
      baseDir: tmpDir,
      streamId: STREAM_ID,
      symbol: SYMBOL,
      runId: RUN_ID,
      topic: 'market:liquidation',
      mode: 'max',
    }).run();
    await createJournalReplayRunner(replayBus, {
      baseDir: tmpDir,
      streamId: STREAM_ID,
      symbol: SYMBOL,
      runId: RUN_ID,
      topic: 'market:oi',
      mode: 'max',
    }).run();
    await createJournalReplayRunner(replayBus, {
      baseDir: tmpDir,
      streamId: STREAM_ID,
      symbol: SYMBOL,
      runId: RUN_ID,
      topic: 'market:funding',
      mode: 'max',
    }).run();
    await createJournalReplayRunner(replayBus, {
      baseDir: tmpDir,
      streamId: STREAM_ID,
      symbol: SYMBOL,
      runId: RUN_ID,
      topic: 'market:ticker',
      mode: 'max',
    }).run();

    expect(replayed.oiAgg).toEqual(original.oiAgg);
    expect(replayed.fundingAgg).toEqual(original.fundingAgg);
    expect(replayed.cvdSpotAgg).toEqual(original.cvdSpotAgg);
    expect(replayed.cvdFuturesAgg).toEqual(original.cvdFuturesAgg);
    expect(replayed.liquidityAgg).toEqual(original.liquidityAgg);
    expect(replayed.liquidationAgg).toEqual(original.liquidationAgg);
    expect(replayed.priceIndex).toEqual(original.priceIndex);
    expect(replayed.priceCanonical).toEqual(original.priceCanonical);

    stopReplay();
  });
});
