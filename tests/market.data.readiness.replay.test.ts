import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import path from 'node:path';
import os from 'node:os';
import { mkdtemp, rm } from 'node:fs/promises';
import { createMeta, type MarketDataStatusPayload, type OpenInterestEvent, type FundingRateEvent, type LiquidationEvent, type OrderbookL2SnapshotEvent, type TickerEvent, type TradeEvent } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { createEventJournal } from '../src/storage/eventJournal';
import { createJournalReplayRunner } from '../src/replay/JournalReplayRunner';
import { OpenInterestAggregator } from '../src/globalData/OpenInterestAggregator';
import { FundingAggregator } from '../src/globalData/FundingAggregator';
import { CvdCalculator } from '../src/globalData/CvdCalculator';
import { CvdAggregator } from '../src/globalData/CvdAggregator';
import { LiquidityAggregator } from '../src/globalData/LiquidityAggregator';
import { LiquidationAggregator } from '../src/globalData/LiquidationAggregator';
import { CanonicalPriceAggregator } from '../src/globalData/CanonicalPriceAggregator';
import { MarketDataReadiness } from '../src/observability/MarketDataReadiness';

const SYMBOL = 'BTCUSDT';
const STREAM_ID = 'stream-1';
const RUN_ID = 'run-readiness';
const READINESS_NOW_TS = 10_000;

function startPipeline(bus: ReturnType<typeof createTestEventBus>) {
  const outputs: MarketDataStatusPayload[] = [];
  const readiness = new MarketDataReadiness(bus, {
    bucketMs: 1000,
    warmingWindowMs: 0,
    expectedSources: 1,
    logIntervalMs: 0,
    now: () => READINESS_NOW_TS,
  });

  const oiAggregator = new OpenInterestAggregator(bus, { ttlMs: 5_000 });
  const fundingAggregator = new FundingAggregator(bus, { ttlMs: 5_000 });
  const cvdCalculator = new CvdCalculator(bus, { bucketMs: 1_000 });
  const cvdAggregator = new CvdAggregator(bus, { ttlMs: 5_000 });
  const liquidityAggregator = new LiquidityAggregator(bus, { ttlMs: 5_000, depthLevels: 1, bucketMs: 1_000 });
  const liquidationAggregator = new LiquidationAggregator(bus, { ttlMs: 5_000, bucketMs: 1_000 });
  const canonicalPriceAggregator = new CanonicalPriceAggregator(bus, { ttlMs: 5_000 });

  bus.subscribe('system:market_data_status', (evt) => outputs.push(evt));

  oiAggregator.start();
  fundingAggregator.start();
  cvdCalculator.start();
  cvdAggregator.start();
  liquidityAggregator.start();
  liquidationAggregator.start();
  canonicalPriceAggregator.start();
  readiness.start();

  return {
    outputs,
    stop: () => {
      readiness.stop();
      canonicalPriceAggregator.stop();
      liquidationAggregator.stop();
      liquidityAggregator.stop();
      cvdAggregator.stop();
      cvdCalculator.stop();
      fundingAggregator.stop();
      oiAggregator.stop();
    },
  };
}

describe('MarketDataReadiness replay determinism', () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), 'readiness-replay-'));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it('replays identical readiness outputs from journaled raw events', async () => {
    const bus = createTestEventBus();
    const { outputs: original, stop } = startPipeline(bus);
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
      meta: createMeta('market', { tsEvent: 900, tsIngest: 900, streamId: STREAM_ID }),
    };
    const trade2: TradeEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      side: 'Sell',
      price: 100,
      size: 1,
      tradeTs: 1100,
      exchangeTs: 1100,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1100, tsIngest: 1100, streamId: STREAM_ID }),
    };
    bus.publish('market:trade', trade1);
    bus.publish('market:trade', trade2);

    const orderbook1: OrderbookL2SnapshotEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      updateId: 1,
      exchangeTs: 900,
      marketType: 'futures',
      bids: [{ price: 100, size: 1 }],
      asks: [{ price: 101, size: 1 }],
      meta: createMeta('market', { tsEvent: 900, tsIngest: 900, streamId: STREAM_ID }),
    };
    const orderbook2: OrderbookL2SnapshotEvent = {
      ...orderbook1,
      updateId: 2,
      exchangeTs: 1100,
      meta: createMeta('market', { tsEvent: 1100, tsIngest: 1100, streamId: STREAM_ID }),
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
      exchangeTs: 500,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 500, tsIngest: 500, streamId: STREAM_ID }),
    };
    const liq2: LiquidationEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      side: 'Sell',
      price: 100,
      size: 1,
      notionalUsd: 100,
      exchangeTs: 1500,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1500, tsIngest: 1500, streamId: STREAM_ID }),
    };
    bus.publish('market:liquidation', liq1);
    bus.publish('market:liquidation', liq2);

    const oi: OpenInterestEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      openInterest: 10,
      openInterestUnit: 'base',
      exchangeTs: 1000,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1000, tsIngest: 1000, streamId: STREAM_ID }),
    };
    bus.publish('market:oi', oi);

    const funding: FundingRateEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      fundingRate: 0.0001,
      exchangeTs: 1000,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1000, tsIngest: 1000, streamId: STREAM_ID }),
    };
    bus.publish('market:funding', funding);

    const ticker: TickerEvent = {
      symbol: SYMBOL,
      streamId: STREAM_ID,
      marketType: 'futures',
      indexPrice: '100',
      exchangeTs: 1000,
      meta: createMeta('market', { tsEvent: 1000, tsIngest: 1000, streamId: STREAM_ID }),
    };
    bus.publish('market:ticker', ticker);

    await journal.stop();
    stop();

    const replayBus = createTestEventBus();
    const { outputs: replayed, stop: stopReplay } = startPipeline(replayBus);

    await createJournalReplayRunner(replayBus, { baseDir: tmpDir, streamId: STREAM_ID, symbol: SYMBOL, runId: RUN_ID, topic: 'market:trade', mode: 'max' }).run();
    await createJournalReplayRunner(replayBus, { baseDir: tmpDir, streamId: STREAM_ID, symbol: SYMBOL, runId: RUN_ID, topic: 'market:orderbook_l2_snapshot', mode: 'max' }).run();
    await createJournalReplayRunner(replayBus, { baseDir: tmpDir, streamId: STREAM_ID, symbol: SYMBOL, runId: RUN_ID, topic: 'market:liquidation', mode: 'max' }).run();
    await createJournalReplayRunner(replayBus, { baseDir: tmpDir, streamId: STREAM_ID, symbol: SYMBOL, runId: RUN_ID, topic: 'market:oi', mode: 'max' }).run();
    await createJournalReplayRunner(replayBus, { baseDir: tmpDir, streamId: STREAM_ID, symbol: SYMBOL, runId: RUN_ID, topic: 'market:funding', mode: 'max' }).run();
    await createJournalReplayRunner(replayBus, { baseDir: tmpDir, streamId: STREAM_ID, symbol: SYMBOL, runId: RUN_ID, topic: 'market:ticker', mode: 'max' }).run();

    expect(replayed).toEqual(original);

    stopReplay();
  });
});
