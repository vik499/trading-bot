import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import path from 'node:path';
import os from 'node:os';
import { mkdtemp, rm } from 'node:fs/promises';
import {
  createMeta,
  type FundingRateEvent,
  type LiquidationEvent,
  type MarketCvdAggEvent,
  type MarketDataStatusPayload,
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
import { MarketDataReadiness } from '../src/observability/MarketDataReadiness';

const SYMBOL = 'BTCUSDT';
const STREAM_ID = 'stream-1';
const RUN_ID = 'run-ordering';

function startAggregators(bus: ReturnType<typeof createTestEventBus>) {
  const oiAgg: MarketOpenInterestAggEvent[] = [];
  const fundingAgg: MarketFundingAggEvent[] = [];
  const cvdSpotAgg: MarketCvdAggEvent[] = [];
  const cvdFuturesAgg: MarketCvdAggEvent[] = [];
  const liquidityAgg: MarketLiquidityAggEvent[] = [];
  const liquidationAgg: MarketLiquidationsAggEvent[] = [];
  const priceIndex: MarketPriceIndexEvent[] = [];
  const priceCanonical: MarketPriceCanonicalEvent[] = [];
  const readiness: MarketDataStatusPayload[] = [];

  const oiAggregator = new OpenInterestAggregator(bus, { ttlMs: 5_000 });
  const fundingAggregator = new FundingAggregator(bus, { ttlMs: 5_000 });
  const cvdCalculator = new CvdCalculator(bus, { bucketMs: 1_000 });
  const cvdAggregator = new CvdAggregator(bus, { ttlMs: 5_000 });
  const liquidityAggregator = new LiquidityAggregator(bus, { ttlMs: 5_000, depthLevels: 1, bucketMs: 1_000 });
  const liquidationAggregator = new LiquidationAggregator(bus, { ttlMs: 5_000, bucketMs: 1_000 });
  const priceIndexAggregator = new PriceIndexAggregator(bus, { ttlMs: 5_000 });
  const canonicalPriceAggregator = new CanonicalPriceAggregator(bus, { ttlMs: 5_000 });
  const readinessAgg = new MarketDataReadiness(bus, { bucketMs: 1_000, warmingWindowMs: 0, expectedSources: 1, logIntervalMs: 0 });

  bus.subscribe('market:oi_agg', (evt) => oiAgg.push(evt));
  bus.subscribe('market:funding_agg', (evt) => fundingAgg.push(evt));
  bus.subscribe('market:cvd_spot_agg', (evt) => cvdSpotAgg.push(evt));
  bus.subscribe('market:cvd_futures_agg', (evt) => cvdFuturesAgg.push(evt));
  bus.subscribe('market:liquidity_agg', (evt) => liquidityAgg.push(evt));
  bus.subscribe('market:liquidations_agg', (evt) => liquidationAgg.push(evt));
  bus.subscribe('market:price_index', (evt) => priceIndex.push(evt));
  bus.subscribe('market:price_canonical', (evt) => priceCanonical.push(evt));
  bus.subscribe('system:market_data_status', (evt) => readiness.push(evt));

  oiAggregator.start();
  fundingAggregator.start();
  cvdCalculator.start();
  cvdAggregator.start();
  liquidityAggregator.start();
  liquidationAggregator.start();
  priceIndexAggregator.start();
  canonicalPriceAggregator.start();
  readinessAgg.start();

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
      readiness,
    },
    stop: () => {
      readinessAgg.stop();
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

type ReplayEventEntry =
  | { topic: 'market:trade'; payload: TradeEvent }
  | { topic: 'market:orderbook_l2_snapshot'; payload: OrderbookL2SnapshotEvent }
  | { topic: 'market:liquidation'; payload: LiquidationEvent }
  | { topic: 'market:oi'; payload: OpenInterestEvent }
  | { topic: 'market:funding'; payload: FundingRateEvent }
  | { topic: 'market:ticker'; payload: TickerEvent };

const events: ReplayEventEntry[] = [
  {
    topic: 'market:trade',
    payload: {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      side: 'Buy',
      price: 100,
      size: 2,
      tradeTs: 900,
      exchangeTs: 900,
      marketType: 'futures',
      meta: createMeta('market', { ts: 900 }),
    },
  },
  {
    topic: 'market:trade',
    payload: {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      side: 'Sell',
      price: 100,
      size: 1,
      tradeTs: 1_900,
      exchangeTs: 1_900,
      marketType: 'futures',
      meta: createMeta('market', { ts: 1_900 }),
    },
  },
  {
    topic: 'market:orderbook_l2_snapshot',
    payload: {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      updateId: 1,
      exchangeTs: 1_100,
      bids: [{ price: 100, size: 1 }],
      asks: [{ price: 101, size: 1 }],
      meta: createMeta('market', { ts: 1_100 }),
    },
  },
  {
    topic: 'market:orderbook_l2_snapshot',
    payload: {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      updateId: 2,
      exchangeTs: 2_100,
      bids: [{ price: 100, size: 1 }],
      asks: [{ price: 101, size: 1 }],
      meta: createMeta('market', { ts: 2_100 }),
    },
  },
  {
    topic: 'market:liquidation',
    payload: {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      side: 'Buy',
      price: 100,
      size: 1,
      notionalUsd: 100,
      exchangeTs: 1_200,
      meta: createMeta('market', { ts: 1_200 }),
    },
  },
  {
    topic: 'market:liquidation',
    payload: {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      side: 'Sell',
      price: 100,
      size: 1,
      notionalUsd: 100,
      exchangeTs: 2_100,
      meta: createMeta('market', { ts: 2_100 }),
    },
  },
  {
    topic: 'market:oi',
    payload: {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      openInterest: 10,
      openInterestUnit: 'base',
      exchangeTs: 1_300,
      meta: createMeta('market', { ts: 1_300 }),
    },
  },
  {
    topic: 'market:funding',
    payload: {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      fundingRate: 0.0001,
      exchangeTs: 1_400,
      nextFundingTs: 2_400,
      meta: createMeta('market', { ts: 1_400 }),
    },
  },
  {
    topic: 'market:ticker',
    payload: {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      indexPrice: '100',
      markPrice: '99.5',
      lastPrice: '100.2',
      exchangeTs: 1_500,
      meta: createMeta('market', { ts: 1_500 }),
    },
  },
];

function publishEvents(bus: ReturnType<typeof createTestEventBus>, ordered: ReplayEventEntry[]) {
  for (const evt of ordered) {
    switch (evt.topic) {
      case 'market:trade':
        bus.publish('market:trade', evt.payload);
        break;
      case 'market:orderbook_l2_snapshot':
        bus.publish('market:orderbook_l2_snapshot', evt.payload);
        break;
      case 'market:liquidation':
        bus.publish('market:liquidation', evt.payload);
        break;
      case 'market:oi':
        bus.publish('market:oi', evt.payload);
        break;
      case 'market:funding':
        bus.publish('market:funding', evt.payload);
        break;
      case 'market:ticker':
        bus.publish('market:ticker', evt.payload);
        break;
      default:
        break;
    }
  }
}

describe('Replay determinism with exchange ordering', () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), 'ordering-replay-'));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it('produces identical outputs for ordered vs shuffled input', async () => {
    const orderedDir = path.join(tmpDir, 'ordered');
    const shuffledDir = path.join(tmpDir, 'shuffled');

    const run = async (baseDir: string, orderedEvents: ReplayEventEntry[]) => {
      const bus = createTestEventBus();
      const { outputs, stop } = startAggregators(bus);
      const journal = createEventJournal(bus, {
        baseDir,
        streamId: STREAM_ID,
        runId: RUN_ID,
        flushIntervalMs: 1,
        maxBatchSize: 1,
      });
      journal.start();
      publishEvents(bus, orderedEvents);
      await journal.stop();
      stop();

      const replayBus = createTestEventBus();
      const { outputs: replayed, stop: stopReplay } = startAggregators(replayBus);
      const topics = [
        'market:trade',
        'market:orderbook_l2_snapshot',
        'market:liquidation',
        'market:oi',
        'market:funding',
        'market:ticker',
      ] as const;

      for (const topic of topics) {
        await createJournalReplayRunner(replayBus, {
          baseDir,
          streamId: STREAM_ID,
          symbol: SYMBOL,
          runId: RUN_ID,
          topic,
          mode: 'max',
          ordering: 'exchange',
        }).run();
      }
      stopReplay();
      return replayed;
    };

    const shuffled = [...events].reverse();

    const orderedOutputs = await run(orderedDir, events);
    const shuffledOutputs = await run(shuffledDir, shuffled);

    expect(JSON.stringify(orderedOutputs)).toBe(JSON.stringify(shuffledOutputs));
  });
});
