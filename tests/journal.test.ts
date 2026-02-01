import { describe, expect, test, beforeEach, afterEach, vi } from 'vitest';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createEventJournal, EventJournal } from '../src/storage/eventJournal';
import { createMeta, type KlineEvent, type TradeEvent } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import * as fsPromises from 'node:fs/promises';

vi.mock('node:fs/promises', async () => {
  const actual = await vi.importActual<typeof import('node:fs/promises')>('node:fs/promises');
  return {
    ...actual,
    appendFile: vi.fn(actual.appendFile),
  };
});

const wait = (ms = 30) => new Promise((resolve) => setTimeout(resolve, ms));

describe('EventJournal', () => {
  let tmpDir: string;
  let journal: EventJournal;
  const symbol = 'BTCUSDT';
  const runId = 'run-test';

  beforeEach(async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), 'journal-'));
  });

  afterEach(async () => {
    await journal?.stop();
    await rm(tmpDir, { recursive: true, force: true });
  });

  test('appends records with increasing seq', async () => {
    const bus = createTestEventBus();
    journal = createEventJournal(bus, {
      baseDir: tmpDir,
      streamId: 'test-stream',
      runId,
      flushIntervalMs: 10,
      maxBatchSize: 1,
    });
    journal.start();

    const meta1 = createMeta('market', { tsEvent: 1_700_000_000_000, tsIngest: 1_700_000_000_000, streamId: 'test-stream' });
    const meta2 = createMeta('market', { tsEvent: 1_700_000_000_500, tsIngest: 1_700_000_000_500, streamId: 'test-stream' });

    bus.publish('market:ticker', { meta: meta1, symbol, streamId: 'test-stream', marketType: 'futures', exchangeTs: 1_700_000_000_000 });
    bus.publish('market:ticker', { meta: meta2, symbol, streamId: 'test-stream', marketType: 'futures', exchangeTs: 1_700_000_000_400 });

    await wait(40);
    const filePath = journal.partitionPath(symbol, meta1.ts);
    const data = await readFile(filePath, 'utf8');
    const lines = data.trim().split('\n');
    const records = lines.map((l) => JSON.parse(l));

    expect(records).toHaveLength(2);
    expect(records[0].seq).toBe(1);
    expect(records[1].seq).toBe(2);
  });

  test('partition path includes stream/symbol/topic/day', async () => {
    const bus = createTestEventBus();
    journal = createEventJournal(bus, { baseDir: tmpDir, streamId: 's1', runId, flushIntervalMs: 5, maxBatchSize: 1 });
    journal.start();

    const meta = createMeta('market', { tsEvent: Date.UTC(2023, 0, 2, 0, 0, 0), tsIngest: Date.UTC(2023, 0, 2, 0, 0, 0), streamId: 's1' });
    bus.publish('market:ticker', { meta, symbol, streamId: 's1', marketType: 'futures', exchangeTs: meta.ts });

    await wait(30);
    const expectedPath = path.join(tmpDir, 's1', symbol, 'market-ticker', runId, '2023-01-02.jsonl');
    const content = await readFile(expectedPath, 'utf8');
    expect(content.length).toBeGreaterThan(0);
  });

  test('records klines with partition path and seq ordering', async () => {
    const bus = createTestEventBus();
    journal = createEventJournal(bus, { baseDir: tmpDir, streamId: 's1', runId, flushIntervalMs: 5, maxBatchSize: 1 });
    journal.start();

    const startTs = Date.UTC(2023, 0, 2, 0, 0, 0);
    const kline1: KlineEvent = {
      symbol,
      interval: '5',
      tf: '5m',
      startTs,
      endTs: startTs + 5 * 60_000,
      open: 100,
      high: 101,
      low: 99,
      close: 100.5,
      volume: 10,
      streamId: 's1',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: startTs + 5 * 60_000, tsIngest: startTs + 5 * 60_000, streamId: 's1' }),
    };
    const kline2: KlineEvent = {
      symbol,
      interval: '5',
      tf: '5m',
      startTs: startTs + 5 * 60_000,
      endTs: startTs + 10 * 60_000,
      open: 100.5,
      high: 102,
      low: 100,
      close: 101,
      volume: 12,
      streamId: 's1',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: startTs + 10 * 60_000, tsIngest: startTs + 10 * 60_000, streamId: 's1' }),
    };

    bus.publish('market:kline', kline1);
    bus.publish('market:kline', kline2);

    await wait(40);

    const expectedPath = path.join(tmpDir, 's1', symbol, 'market-kline', '5m', runId, '2023-01-02.jsonl');
    const data = await readFile(expectedPath, 'utf8');
    const records = data.trim().split('\n').map((line) => JSON.parse(line));

    expect(records).toHaveLength(2);
    expect(records[0].seq).toBe(1);
    expect(records[1].seq).toBe(2);
  });

  test('records trades under market-trade partition path', async () => {
    const bus = createTestEventBus();
    journal = createEventJournal(bus, { baseDir: tmpDir, streamId: 's1', runId, flushIntervalMs: 5, maxBatchSize: 1 });
    journal.start();

    const trade: TradeEvent = {
      symbol,
      streamId: 's1',
      tradeId: 't-1',
      side: 'Buy',
      price: 100,
      size: 0.5,
      tradeTs: Date.UTC(2023, 0, 2, 0, 0, 0),
      exchangeTs: Date.UTC(2023, 0, 2, 0, 0, 0),
      marketType: 'futures',
      meta: createMeta('market', {
        tsEvent: Date.UTC(2023, 0, 2, 0, 0, 0),
        tsIngest: Date.UTC(2023, 0, 2, 0, 0, 1),
        streamId: 's1',
      }),
    };

    bus.publish('market:trade', trade);

    await wait(30);
    const expectedPath = path.join(tmpDir, 's1', symbol, 'market-trade', runId, '2023-01-02.jsonl');
    const content = await readFile(expectedPath, 'utf8');
    expect(content.length).toBeGreaterThan(0);
  });

  test('seq resets per runId scope', async () => {
    const bus = createTestEventBus();

    journal = createEventJournal(bus, { baseDir: tmpDir, streamId: 's1', runId: 'run-a', flushIntervalMs: 5, maxBatchSize: 1 });
    journal.start();
    const metaA = createMeta('market', { tsEvent: 1_000, tsIngest: 1_000, streamId: 's1' });
    bus.publish('market:ticker', { meta: metaA, symbol, streamId: 's1', marketType: 'futures', exchangeTs: 1_000 });
    await wait(20);
    await journal.stop();
    const fileA = journal.partitionPath(symbol, metaA.ts);
    const recA = JSON.parse((await readFile(fileA, 'utf8')).trim().split('\n')[0]);

    journal = createEventJournal(bus, { baseDir: tmpDir, streamId: 's1', runId: 'run-b', flushIntervalMs: 5, maxBatchSize: 1 });
    journal.start();
    const metaB = createMeta('market', { tsEvent: 2_000, tsIngest: 2_000, streamId: 's1' });
    bus.publish('market:ticker', { meta: metaB, symbol, streamId: 's1', marketType: 'futures', exchangeTs: 2_000 });
    await wait(20);
    await journal.stop();
    const fileB = journal.partitionPath(symbol, metaB.ts);
    const recB = JSON.parse((await readFile(fileB, 'utf8')).trim().split('\n')[0]);

    expect(recA.seq).toBe(1);
    expect(recB.seq).toBe(1);
  });

  test('emits latencySpike when latency exceeds threshold', async () => {
    const bus = createTestEventBus();
    const spikes: unknown[] = [];
    bus.subscribe('data:latencySpike', (evt) => spikes.push(evt));

    journal = createEventJournal(bus, { baseDir: tmpDir, runId, latencySpikeMs: 500, flushIntervalMs: 5, maxBatchSize: 1 });
    journal.start();

    const meta = createMeta('market', { tsEvent: 2_000, tsIngest: 2_000, streamId: 's1' });
    bus.publish('market:ticker', { meta, symbol, streamId: 's1', marketType: 'futures', exchangeTs: 0 });

    await wait(20);
    expect(spikes.length).toBe(1);
  });

  test('emits outOfOrder when exchange ts decreases', async () => {
    const bus = createTestEventBus();
    const outOfOrder: unknown[] = [];
    bus.subscribe('data:time_out_of_order', (evt) => outOfOrder.push(evt));

    journal = createEventJournal(bus, { baseDir: tmpDir, runId, flushIntervalMs: 5, maxBatchSize: 1 });
    journal.start();

    const meta1 = createMeta('market', { tsEvent: 10, tsIngest: 10, streamId: 's1' });
    const meta2 = createMeta('market', { tsEvent: 20, tsIngest: 20, streamId: 's1' });
    bus.publish('market:ticker', { meta: meta1, symbol, streamId: 's1', marketType: 'futures', exchangeTs: 2000 });
    bus.publish('market:ticker', { meta: meta2, symbol, streamId: 's1', marketType: 'futures', exchangeTs: 1000 });

    await wait(20);
    expect(outOfOrder.length).toBe(1);
  });

  test('emits outOfOrder for klines per tf', async () => {
    const bus = createTestEventBus();
    const outOfOrder: unknown[] = [];
    bus.subscribe('data:time_out_of_order', (evt) => outOfOrder.push(evt));

    journal = createEventJournal(bus, { baseDir: tmpDir, runId, flushIntervalMs: 5, maxBatchSize: 1 });
    journal.start();

    const klineA: KlineEvent = {
      symbol,
      interval: '5',
      tf: '5m',
      startTs: 0,
      endTs: 300_000,
      open: 1,
      high: 1,
      low: 1,
      close: 1,
      volume: 1,
      streamId: 's1',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 300_000, tsIngest: 300_000, streamId: 's1' }),
    };
    const klineB: KlineEvent = {
      symbol,
      interval: '5',
      tf: '5m',
      startTs: 0,
      endTs: 100_000,
      open: 1,
      high: 1,
      low: 1,
      close: 1,
      volume: 1,
      streamId: 's1',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 100_000, tsIngest: 100_000, streamId: 's1' }),
    };
    const klineOtherTf: KlineEvent = {
      symbol,
      interval: '60',
      tf: '1h',
      startTs: 0,
      endTs: 3_600_000,
      open: 1,
      high: 1,
      low: 1,
      close: 1,
      volume: 1,
      streamId: 's1',
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 3_600_000, tsIngest: 3_600_000, streamId: 's1' }),
    };

    bus.publish('market:kline', klineA);
    bus.publish('market:kline', klineOtherTf);
    bus.publish('market:kline', klineB);

    await wait(30);
    expect(outOfOrder.length).toBe(1);
  });

  test('emits outOfOrder for trades when tradeTs decreases', async () => {
    const bus = createTestEventBus();
    const outOfOrder: unknown[] = [];
    bus.subscribe('data:time_out_of_order', (evt) => outOfOrder.push(evt));

    journal = createEventJournal(bus, { baseDir: tmpDir, runId, flushIntervalMs: 5, maxBatchSize: 1 });
    journal.start();

    const tradeA: TradeEvent = {
      symbol,
      streamId: 's1',
      tradeId: 't-1',
      side: 'Buy',
      price: 100,
      size: 1,
      tradeTs: 2_000,
      exchangeTs: 2_000,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 2_000, tsIngest: 2_000, streamId: 's1' }),
    };
    const tradeB: TradeEvent = {
      symbol,
      streamId: 's1',
      tradeId: 't-2',
      side: 'Sell',
      price: 99,
      size: 1,
      tradeTs: 1_000,
      exchangeTs: 1_000,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 2_500, streamId: 's1' }),
    };

    bus.publish('market:trade', tradeA);
    bus.publish('market:trade', tradeB);

    await wait(30);
    expect(outOfOrder.length).toBe(1);
  });

  test('emits writeFailed when append fails', async () => {
    const bus = createTestEventBus();
    const failures: unknown[] = [];
    bus.subscribe('storage:writeFailed', (evt) => failures.push(evt));

    journal = createEventJournal(bus, { baseDir: tmpDir, runId, flushIntervalMs: 5, maxBatchSize: 1 });
    journal.start();

    const appendMock = fsPromises.appendFile as unknown as ReturnType<typeof vi.fn>;
    appendMock.mockRejectedValueOnce(new Error('disk full'));

    const meta = createMeta('market', { tsEvent: 100, tsIngest: 100, streamId: 's1' });
    bus.publish('market:ticker', { meta, symbol, exchangeTs: 50 });

    await wait(30);

    expect(failures.length).toBe(1);
  });
});
