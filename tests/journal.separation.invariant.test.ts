import { describe, expect, test, afterEach } from 'vitest';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createEventJournal, type EventJournal } from '../src/storage/eventJournal';
import { createAggregatedEventJournal, type AggregatedEventJournal } from '../src/storage/aggregatedJournal';
import { createMeta, type MarketOpenInterestAggEvent, type TradeRawEvent } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';

const wait = (ms = 30) => new Promise((resolve) => setTimeout(resolve, ms));

describe('Journal separation invariants', () => {
  let tmpDir: string;
  let journal: EventJournal | undefined;
  let aggJournal: AggregatedEventJournal | undefined;

  afterEach(async () => {
    await journal?.stop();
    await aggJournal?.stop();
    if (tmpDir) {
      await rm(tmpDir, { recursive: true, force: true });
    }
  });

  test('aggregated journal ignores raw topics', async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), 'journal-sep-'));
    const bus = createTestEventBus();
    aggJournal = createAggregatedEventJournal(bus, {
      baseDir: tmpDir,
      runId: 'run-agg',
      flushIntervalMs: 5,
      maxBatchSize: 1,
    });
    aggJournal.start();

    const rawTrade: TradeRawEvent = {
      venue: 'binance',
      symbol: 'BTCUSDT',
      exchangeTsMs: 1_000,
      recvTsMs: 1_000,
      price: '100',
      size: '0.1',
      side: 'buy',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000 }),
    };

    bus.publish('market:trade_raw', rawTrade);

    await wait(40);
    const entries = await readdir(tmpDir);
    expect(entries).toHaveLength(0);
  });

  test('event journal ignores aggregated topics', async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), 'journal-sep-'));
    const bus = createTestEventBus();
    journal = createEventJournal(bus, {
      baseDir: tmpDir,
      streamId: 's1',
      runId: 'run-raw',
      flushIntervalMs: 5,
      maxBatchSize: 1,
    });
    journal.start();

    const meta = createMeta('global_data', { tsEvent: 1_000, tsIngest: 1_000 });
    const oiAgg: MarketOpenInterestAggEvent = {
      symbol: 'BTCUSDT',
      ts: meta.ts,
      openInterest: 100,
      openInterestUnit: 'base',
      marketType: 'futures',
      sourcesUsed: ['s1'],
      freshSourcesCount: 1,
      confidenceScore: 1,
      provider: 'test',
      meta,
    };

    bus.publish('market:oi_agg', oiAgg);

    await wait(40);
    const entries = await readdir(tmpDir);
    expect(entries).toHaveLength(0);
  });
});
