import { describe, expect, test, beforeEach, afterEach } from 'vitest';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createAggregatedEventJournal, type AggregatedEventJournal } from '../src/storage/aggregatedJournal';
import { createMeta, type MarketDataStatusPayload, type MarketOpenInterestAggEvent } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';

const wait = (ms = 30) => new Promise((resolve) => setTimeout(resolve, ms));

describe('AggregatedEventJournal', () => {
  let tmpDir: string;
  let journal: AggregatedEventJournal;

  beforeEach(async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), 'agg-journal-'));
  });

  afterEach(async () => {
    await journal?.stop();
    await rm(tmpDir, { recursive: true, force: true });
  });

  test('writes system:market_data_status records', async () => {
    const bus = createTestEventBus();
    journal = createAggregatedEventJournal(bus, {
      baseDir: tmpDir,
      runId: 'run-test',
      flushIntervalMs: 10,
      maxBatchSize: 1,
    });
    journal.start();

    const meta = createMeta('system', { ts: Date.UTC(2023, 0, 2, 0, 0, 0) });
    const payload: MarketDataStatusPayload = {
      meta,
      overallConfidence: 0.5,
      blockConfidence: {
        price: 0.5,
        flow: 0.5,
        liquidity: 0.5,
        derivatives: 0.5,
      },
      degraded: true,
      degradedReasons: ['WS_DISCONNECTED'],
      warmingUp: false,
      warmingProgress: 1,
      warmingWindowMs: 0,
      activeSources: 0,
      expectedSources: 0,
      activeSourcesAgg: 0,
      activeSourcesRaw: 0,
      expectedSourcesAgg: 0,
      expectedSourcesRaw: 0,
      lastBucketTs: meta.ts,
    };

    bus.publish('system:market_data_status', payload);

    await wait(40);
    const expectedPath = path.join(tmpDir, 'system', 'market-data-status', 'run-test', '2023-01-02.jsonl');
    const content = await readFile(expectedPath, 'utf8');
    expect(content.length).toBeGreaterThan(0);
  });

  test('writes aggregated market events', async () => {
    const bus = createTestEventBus();
    journal = createAggregatedEventJournal(bus, {
      baseDir: tmpDir,
      runId: 'run-test',
      flushIntervalMs: 10,
      maxBatchSize: 1,
    });
    journal.start();

    const meta = createMeta('global_data', { ts: Date.UTC(2023, 0, 2, 0, 0, 0) });
    const payload: MarketOpenInterestAggEvent = {
      symbol: 'BTCUSDT',
      ts: meta.ts,
      openInterest: 100,
      openInterestUnit: 'base',
      marketType: 'futures',
      sourcesUsed: ['s1'],
      freshSourcesCount: 1,
      confidenceScore: 0.9,
      provider: 'test',
      meta,
    };

    bus.publish('market:oi_agg', payload);

    await wait(40);
    const expectedPath = path.join(tmpDir, 'aggregated', 'market-oi_agg', 'BTCUSDT', 'run-test', '2023-01-02.jsonl');
    const content = await readFile(expectedPath, 'utf8');
    expect(content.length).toBeGreaterThan(0);
  });
});
