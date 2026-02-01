import { describe, expect, test, beforeEach, afterEach, vi } from 'vitest';
import { mkdtemp, writeFile, mkdir, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createJournalReplayRunner } from '../src/replay/JournalReplayRunner';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta } from '../src/core/events/EventBus';

const makeRecord = (seq: number, tsIngest: number, symbol: string) => ({
  seq,
  streamId: 'stream-1',
  topic: 'market:ticker' as const,
  symbol,
  tsIngest,
  payload: {
    symbol,
    streamId: 'stream-1',
    marketType: 'futures',
    lastPrice: String(seq),
    meta: createMeta('market', { tsEvent: tsIngest, tsIngest, streamId: 'stream-1' }),
  },
});

async function writeJournalFile(
  baseDir: string,
  streamId: string,
  symbol: string,
  date: string,
  lines: string[],
  runId?: string,
  opts: { topicDir?: string } = {}
) {
  const topicDir = opts.topicDir ?? 'market-ticker';
  const dir = runId
    ? path.join(baseDir, streamId, symbol, topicDir, runId)
    : path.join(baseDir, streamId, symbol, topicDir);
  await mkdir(dir, { recursive: true });
  const filePath = path.join(dir, `${date}.jsonl`);
  await writeFile(filePath, lines.join('\n'), 'utf8');
  return filePath;
}

async function writeKlineJournalFile(
  baseDir: string,
  streamId: string,
  symbol: string,
  tf: string,
  date: string,
  lines: string[],
  runId?: string
) {
  const topicDir = path.join('market-kline', tf);
  return writeJournalFile(baseDir, streamId, symbol, date, lines, runId, { topicDir });
}

describe('JournalReplayRunner', () => {
  let tmpDir: string;
  const runId = 'run-1';

  beforeEach(async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), 'replay-'));
  });

  afterEach(async () => {
    vi.useRealTimers();
    await rm(tmpDir, { recursive: true, force: true });
  });

  test('emits market:ticker in order with replay meta', async () => {
    const bus = createTestEventBus();
    const events: number[] = [];
    bus.subscribe('market:ticker', (evt) => events.push(evt.meta.ts));

    const lines = [makeRecord(1, 1000, 'BTCUSDT'), makeRecord(2, 2000, 'BTCUSDT'), makeRecord(3, 3000, 'BTCUSDT')].map((r) => JSON.stringify(r));
    await writeJournalFile(tmpDir, 'stream-1', 'BTCUSDT', '2023-01-01', lines, runId);

    const runner = createJournalReplayRunner(bus, { baseDir: tmpDir, streamId: 'stream-1', symbol: 'BTCUSDT', runId, mode: 'max' });
    await runner.run();

    expect(events).toEqual([1000, 2000, 3000]);
  });

  test('corrupted line emits warning and is skipped', async () => {
    const bus = createTestEventBus();
    const warnings: unknown[] = [];
    bus.subscribe('replay:warning', (w) => warnings.push(w));

    const lines = [
      JSON.stringify(makeRecord(1, 1000, 'BTCUSDT')),
      '{bad json',
      JSON.stringify(makeRecord(2, 2000, 'BTCUSDT')),
    ];
    await writeJournalFile(tmpDir, 'stream-1', 'BTCUSDT', '2023-01-01', lines, runId);

    const runner = createJournalReplayRunner(bus, { baseDir: tmpDir, streamId: 'stream-1', symbol: 'BTCUSDT', runId, mode: 'max' });
    await runner.run();

    expect(warnings).toHaveLength(1);
  });

  test('invalid record shape emits warning', async () => {
    const bus = createTestEventBus();
    const warnings: unknown[] = [];
    bus.subscribe('replay:warning', (w) => warnings.push(w));

    const badRecord = { seq: 1, streamId: 'stream-1', topic: 'market:ticker', symbol: 'BTCUSDT', payload: {} };
    const lines = [JSON.stringify(badRecord)];
    await writeJournalFile(tmpDir, 'stream-1', 'BTCUSDT', '2023-01-01', lines, runId);

    const runner = createJournalReplayRunner(bus, { baseDir: tmpDir, streamId: 'stream-1', symbol: 'BTCUSDT', runId, mode: 'max' });
    await runner.run();

    expect(warnings).toHaveLength(1);
  });

  test('accelerated mode scales timing', async () => {
    const bus = createTestEventBus();
    const emissions: number[] = [];
    const sleeps: number[] = [];
    bus.subscribe('market:ticker', (evt) => emissions.push(evt.meta.ts));

    const lines = [
      makeRecord(1, 0, 'BTCUSDT'),
      makeRecord(2, 1000, 'BTCUSDT'),
      makeRecord(3, 2000, 'BTCUSDT'),
    ].map((r) => JSON.stringify(r));
    await writeJournalFile(tmpDir, 'stream-1', 'BTCUSDT', '2023-01-01', lines, runId);

    const runner = createJournalReplayRunner(bus, {
      baseDir: tmpDir,
      streamId: 'stream-1',
      symbol: 'BTCUSDT',
      runId,
      mode: 'accelerated',
      speedFactor: 10,
      sleepFn: async (ms) => {
        sleeps.push(ms);
      },
    });
    await runner.run();

    expect(emissions).toEqual([0, 1000, 2000]);
    expect(sleeps).toEqual([100, 100]);
  });

  test('date filtering respects from/to', async () => {
    const bus = createTestEventBus();
    const emissions: string[] = [];
    bus.subscribe('market:ticker', (evt) => emissions.push(evt.symbol));

    await writeJournalFile(tmpDir, 'stream-1', 'BTCUSDT', '2023-01-01', [JSON.stringify(makeRecord(1, 1000, 'BTCUSDT'))], runId);
    await writeJournalFile(tmpDir, 'stream-1', 'BTCUSDT', '2023-01-02', [JSON.stringify(makeRecord(2, 2000, 'BTCUSDT'))], runId);

    const runner = createJournalReplayRunner(bus, { baseDir: tmpDir, streamId: 'stream-1', symbol: 'BTCUSDT', runId, mode: 'max', dateFrom: '2023-01-02', dateTo: '2023-01-02' });
    await runner.run();

    expect(emissions).toEqual(['BTCUSDT']);
  });

  test('runId filter isolates records for a single run', async () => {
    const bus = createTestEventBus();
    const emissions: string[] = [];
    bus.subscribe('market:ticker', (evt) => emissions.push(evt.lastPrice ?? ''));

    const runA = 'run-A';
    const runB = 'run-B';
    const linesA = [makeRecord(1, 1000, 'BTCUSDT'), makeRecord(2, 2000, 'BTCUSDT'), makeRecord(3, 3000, 'BTCUSDT')].map((r) =>
      JSON.stringify(r)
    );
    const linesB = [
      makeRecord(10, 10_000, 'BTCUSDT'),
      makeRecord(11, 11_000, 'BTCUSDT'),
      makeRecord(12, 12_000, 'BTCUSDT'),
      makeRecord(13, 13_000, 'BTCUSDT'),
      makeRecord(14, 14_000, 'BTCUSDT'),
    ].map((r) => JSON.stringify(r));

    await writeJournalFile(tmpDir, 'stream-1', 'BTCUSDT', '2023-01-01', linesA, runA);
    await writeJournalFile(tmpDir, 'stream-1', 'BTCUSDT', '2023-01-01', linesB, runB);

    const runner = createJournalReplayRunner(bus, { baseDir: tmpDir, streamId: 'stream-1', symbol: 'BTCUSDT', runId: runA, mode: 'max' });
    await runner.run();

    expect(emissions).toEqual(['1', '2', '3']);
  });

  test('legacy layout is replayable when runId folder is missing', async () => {
    const bus = createTestEventBus();
    const emissions: number[] = [];
    bus.subscribe('market:ticker', (evt) => emissions.push(evt.meta.ts));

    await writeJournalFile(tmpDir, 'stream-1', 'BTCUSDT', '2023-01-01', [JSON.stringify(makeRecord(1, 1000, 'BTCUSDT'))]);

    const runner = createJournalReplayRunner(bus, { baseDir: tmpDir, streamId: 'stream-1', symbol: 'BTCUSDT', runId: 'missing-run', mode: 'max' });
    await runner.run();

    expect(emissions).toEqual([1000]);
  });

  test('replays kline records with deterministic meta', async () => {
    const bus = createTestEventBus();
    const events: number[] = [];
    const sources: string[] = [];
    bus.subscribe('market:kline', (evt) => {
      events.push(evt.meta.ts);
      sources.push(evt.meta.source);
    });

    const klineRecord = (seq: number, endTs: number) => ({
      seq,
      streamId: 'stream-1',
      topic: 'market:kline' as const,
      symbol: 'BTCUSDT',
      tsIngest: endTs,
      payload: {
        symbol: 'BTCUSDT',
        streamId: 'stream-1',
        marketType: 'futures',
        interval: '5',
        tf: '5m',
        startTs: endTs - 300_000,
        endTs,
        open: 100,
        high: 101,
        low: 99,
        close: 100,
        volume: 1,
        meta: createMeta('market', { tsEvent: endTs, tsIngest: endTs, streamId: 'stream-1' }),
      },
    });

    const lines = [klineRecord(1, 1000), klineRecord(2, 2000)].map((r) => JSON.stringify(r));
    await writeKlineJournalFile(tmpDir, 'stream-1', 'BTCUSDT', '5m', '2023-01-01', lines, runId);

    const runner = createJournalReplayRunner(bus, {
      baseDir: tmpDir,
      streamId: 'stream-1',
      symbol: 'BTCUSDT',
      runId,
      mode: 'max',
      topic: 'market:kline',
      tf: '5m',
    });
    await runner.run();

    expect(events).toEqual([1000, 2000]);
    expect(sources).toEqual(['replay', 'replay']);
  });

  test('replays trade records with meta.ts = tradeTs', async () => {
    const bus = createTestEventBus();
    const events: number[] = [];
    bus.subscribe('market:trade', (evt) => events.push(evt.meta.ts));

    const tradeRecord = (seq: number, tradeTs: number, tsIngest: number) => ({
      seq,
      streamId: 'stream-1',
      topic: 'market:trade' as const,
      symbol: 'BTCUSDT',
      tsIngest,
      payload: {
        symbol: 'BTCUSDT',
        streamId: 'stream-1',
        marketType: 'futures',
        tradeId: `t-${seq}`,
        side: 'Buy',
        price: 100,
        size: 1,
        tradeTs,
        exchangeTs: tradeTs,
        meta: createMeta('market', { tsEvent: tradeTs, tsIngest, streamId: 'stream-1' }),
      },
    });

    const lines = [tradeRecord(1, 1000, 5000), tradeRecord(2, 2000, 6000)].map((r) => JSON.stringify(r));
    await writeJournalFile(tmpDir, 'stream-1', 'BTCUSDT', '2023-01-01', lines, runId, { topicDir: 'market-trade' });

    const runner = createJournalReplayRunner(bus, {
      baseDir: tmpDir,
      streamId: 'stream-1',
      symbol: 'BTCUSDT',
      runId,
      mode: 'max',
      topic: 'market:trade',
    });
    await runner.run();

    expect(events).toEqual([1000, 2000]);
  });

  test('replays orderbook records with meta.ts = exchangeTs', async () => {
    const bus = createTestEventBus();
    const events: number[] = [];
    bus.subscribe('market:orderbook_l2_snapshot', (evt) => events.push(evt.meta.ts));

    const orderbookRecord = (seq: number, exchangeTs: number, tsIngest: number) => ({
      seq,
      streamId: 'stream-1',
      topic: 'market:orderbook_l2_snapshot' as const,
      symbol: 'BTCUSDT',
      tsIngest,
      payload: {
        symbol: 'BTCUSDT',
        streamId: 'stream-1',
        updateId: seq,
        exchangeTs,
        marketType: 'futures',
        bids: [{ price: 100, size: 1 }],
        asks: [{ price: 101, size: 1 }],
        meta: createMeta('market', { tsEvent: exchangeTs, tsIngest, streamId: 'stream-1' }),
      },
    });

    const lines = [orderbookRecord(1, 1111, 5000), orderbookRecord(2, 2222, 6000)].map((r) => JSON.stringify(r));
    await writeJournalFile(tmpDir, 'stream-1', 'BTCUSDT', '2023-01-01', lines, runId, { topicDir: 'market-orderbook-l2' });

    const runner = createJournalReplayRunner(bus, {
      baseDir: tmpDir,
      streamId: 'stream-1',
      symbol: 'BTCUSDT',
      runId,
      mode: 'max',
      topic: 'market:orderbook_l2_snapshot',
    });
    await runner.run();

    expect(events).toEqual([1111, 2222]);
  });
});
