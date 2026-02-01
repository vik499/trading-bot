import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs/promises';
import { mkdtemp, readFile, rm, writeFile, mkdir } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createMeta } from '../src/core/events/EventBus';
import { TestRuntime } from './helpers/testRuntime';
import { FakeClock } from './helpers/fakeClock';

interface JournalRecord {
  seq: number;
  streamId: string;
  topic: string;
  symbol: string;
  tsIngest: number;
  payload: Record<string, unknown>;
}

const SYMBOL = 'BTCUSDT';
const STREAM_ID = 'bybit.public.linear.v5';
const RUN_ID = 'run-test';

describe('E2E storage + replay', () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), 'e2e-storage-'));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it('journal writes all new market topics under runId layout', async () => {
    const clock = new FakeClock(Date.UTC(2024, 0, 1, 0, 0, 0), 1000);
    const runtime = new TestRuntime({
      baseDir: tmpDir,
      runId: RUN_ID,
      streamId: STREAM_ID,
      symbol: SYMBOL,
      startJournal: true,
      startAnalytics: false,
      startStrategy: false,
      startRisk: false,
      startExecution: false,
      startPortfolio: false,
      startMetrics: false,
    }, clock);

    runtime.publishMarketTicker({ lastPrice: '100', ts: clock.next(), exchangeTs: clock.next() - 200 });
    runtime.publishMarketTicker({ lastPrice: '101', ts: clock.next(), exchangeTs: clock.next() - 200 });

    runtime.publishMarketKline({ tf: '5m', close: 100 });
    runtime.publishMarketKline({ tf: '5m', close: 101 });
    runtime.publishMarketKline({ tf: '5m', close: 102 });

    runtime.publishMarketTrade({ side: 'Buy', price: 100, size: 1, ts: 1_700_000_000_500, tradeTs: 1_700_000_000_100, exchangeTs: 1_700_000_000_100 });
    runtime.publishMarketTrade({ side: 'Sell', price: 99, size: 2, ts: 1_700_000_001_000, tradeTs: 1_700_000_000_600, exchangeTs: 1_700_000_000_600 });
    runtime.publishMarketTrade({ side: 'Buy', price: 101, size: 1.5, ts: 1_700_000_001_500, tradeTs: 1_700_000_001_100, exchangeTs: 1_700_000_001_100 });

    runtime.publishMarketOrderbookL2({ kind: 'snapshot', updateId: 10, ts: 1_700_000_002_000, exchangeTs: 1_700_000_001_900 });
    runtime.publishMarketOrderbookL2({ kind: 'delta', updateId: 11, ts: 1_700_000_002_500, exchangeTs: 1_700_000_002_400 });

    runtime.publishMarketOi({ openInterest: 1000, ts: 1_700_000_003_000, exchangeTs: 1_700_000_002_900 });
    runtime.publishMarketOi({ openInterest: 1005, ts: 1_700_000_003_500, exchangeTs: 1_700_000_003_400 });

    runtime.publishMarketFunding({ fundingRate: 0.0001, ts: 1_700_000_004_000, exchangeTs: 1_700_000_003_900 });
    runtime.publishMarketFunding({ fundingRate: 0.00012, ts: 1_700_000_004_500, exchangeTs: 1_700_000_004_400 });

    await runtime.stop();

    const basePath = path.join(tmpDir, STREAM_ID, SYMBOL);
    const topicDirs: Record<string, string> = {
      'market:ticker': 'market-ticker',
      'market:kline': 'market-kline',
      'market:trade': 'market-trade',
      'market:orderbook_l2_snapshot': 'market-orderbook-l2',
      'market:orderbook_l2_delta': 'market-orderbook-l2',
      'market:oi': 'market-oi',
      'market:funding': 'market-funding',
    };

    const assertTopic = async (topic: string, extraPath: string[] = []) => {
      const dir = path.join(basePath, topicDirs[topic], ...extraPath, RUN_ID);
      const files = await listJsonlFiles(dir);
      expect(files.length).toBeGreaterThan(0);
      const first = await readJsonLines(files[0]);
      expect(first.length).toBeGreaterThan(0);
      for (const record of first) {
        expect(record.seq).toBeDefined();
        expect(record.streamId).toBeDefined();
        expect(record.topic).toBeDefined();
        expect(record.symbol).toBeDefined();
        expect(record.tsIngest).toBeDefined();
        expect(record.payload).toBeDefined();
      }
    };

    await assertTopic('market:ticker');
    await assertTopic('market:kline', ['5m']);
    await assertTopic('market:trade');
    await assertTopic('market:orderbook_l2_snapshot');
    await assertTopic('market:oi');
    await assertTopic('market:funding');

    const klineDir = path.join(basePath, 'market-kline', '5m', RUN_ID);
    const klineDirExists = await dirExists(klineDir);
    expect(klineDirExists).toBe(true);
  });

  it('replay re-emits deterministically for all market topics', async () => {
    const clock = new FakeClock(Date.UTC(2024, 0, 1, 0, 0, 0), 1000);
    const runtime = new TestRuntime({
      baseDir: tmpDir,
      runId: RUN_ID,
      streamId: STREAM_ID,
      symbol: SYMBOL,
      startJournal: true,
      startAnalytics: true,
      startStrategy: false,
      startRisk: false,
      startExecution: false,
      startPortfolio: false,
      startMetrics: false,
    }, clock);

    runtime.publishMarketTicker({ ts: 1_700_000_000_000, exchangeTs: 1_700_000_000_000, lastPrice: '100' });
    runtime.publishMarketKline({ tf: '5m', endTs: 1_700_000_300_000, close: 100 });
    runtime.publishMarketTrade({ ts: 1_700_000_001_000, tradeTs: 1_700_000_000_600, exchangeTs: 1_700_000_000_600, side: 'Buy' });
    runtime.publishMarketOrderbookL2({ kind: 'snapshot', ts: 1_700_000_002_000, exchangeTs: 1_700_000_001_500, updateId: 5 });
    runtime.publishMarketOrderbookL2({ kind: 'delta', ts: 1_700_000_002_500, exchangeTs: 1_700_000_002_000, updateId: 6 });
    runtime.publishMarketOi({ ts: 1_700_000_003_000, exchangeTs: 1_700_000_002_800, openInterest: 2000 });
    runtime.publishMarketFunding({ ts: 1_700_000_004_000, exchangeTs: 1_700_000_003_800, fundingRate: 0.0002 });

    await runtime.stop();

    const records = await loadJournalRecords(tmpDir, STREAM_ID, SYMBOL);
    const expectedByTopic = groupRecordsByTopic(records);

    const replayRuntime = new TestRuntime({
      baseDir: tmpDir,
      runId: RUN_ID,
      streamId: STREAM_ID,
      symbol: SYMBOL,
      startJournal: false,
      startAnalytics: true,
      startStrategy: false,
      startRisk: false,
      startExecution: false,
      startPortfolio: false,
      startMetrics: false,
    }, new FakeClock());

    await replayRuntime.runReplay({ topic: 'market:ticker', mode: 'max' });
    await replayRuntime.runReplay({ topic: 'market:kline', tf: '5m', mode: 'max' });
    await replayRuntime.runReplay({ topic: 'market:trade', mode: 'max' });
    await replayRuntime.runReplay({ topic: 'market:orderbook_l2_snapshot', mode: 'max' });
    await replayRuntime.runReplay({ topic: 'market:oi', mode: 'max' });
    await replayRuntime.runReplay({ topic: 'market:funding', mode: 'max' });

    const replayedTicker = replayRuntime.getEvents('market:ticker');
    const replayedKline = replayRuntime.getEvents('market:kline');
    const replayedTrade = replayRuntime.getEvents('market:trade');
    const replayedSnapshot = replayRuntime.getEvents('market:orderbook_l2_snapshot');
    const replayedDelta = replayRuntime.getEvents('market:orderbook_l2_delta');
    const replayedOi = replayRuntime.getEvents('market:oi');
    const replayedFunding = replayRuntime.getEvents('market:funding');

    expect(replayedTicker).toHaveLength(expectedByTopic['market:ticker']?.length ?? 0);
    expect(replayedKline).toHaveLength(expectedByTopic['market:kline']?.length ?? 0);
    expect(replayedTrade).toHaveLength(expectedByTopic['market:trade']?.length ?? 0);
    expect(replayedSnapshot.length + replayedDelta.length).toBe((expectedByTopic['market:orderbook_l2_snapshot']?.length ?? 0) + (expectedByTopic['market:orderbook_l2_delta']?.length ?? 0));
    expect(replayedOi).toHaveLength(expectedByTopic['market:oi']?.length ?? 0);
    expect(replayedFunding).toHaveLength(expectedByTopic['market:funding']?.length ?? 0);

    const expectedTickerTs = expectedByTopic['market:ticker']?.map((r) => r.tsIngest) ?? [];
    expect(replayedTicker.map((evt) => evt.meta.ts)).toEqual(expectedTickerTs);
    replayedTicker.forEach((evt) => expect(evt.meta.source).toBe('replay'));

    const expectedKlineTs = expectedByTopic['market:kline']?.map((r) => r.tsIngest) ?? [];
    expect(replayedKline.map((evt) => evt.meta.ts)).toEqual(expectedKlineTs);
    replayedKline.forEach((evt) => expect(evt.meta.source).toBe('replay'));

    const expectedTradeTs = expectedByTopic['market:trade']?.map((r) => r.payload.tradeTs as number) ?? [];
    expect(replayedTrade.map((evt) => evt.meta.ts)).toEqual(expectedTradeTs);
    replayedTrade.forEach((evt) => expect(evt.meta.source).toBe('replay'));

    const expectedSnapshotTs = expectedByTopic['market:orderbook_l2_snapshot']?.map((r) => r.payload.exchangeTs as number) ?? [];
    const expectedDeltaTs = expectedByTopic['market:orderbook_l2_delta']?.map((r) => r.payload.exchangeTs as number) ?? [];
    expect(replayedSnapshot.map((evt) => evt.meta.ts)).toEqual(expectedSnapshotTs);
    expect(replayedDelta.map((evt) => evt.meta.ts)).toEqual(expectedDeltaTs);

    const expectedOiTs = expectedByTopic['market:oi']?.map((r) => r.payload.exchangeTs as number) ?? [];
    expect(replayedOi.map((evt) => evt.meta.ts)).toEqual(expectedOiTs);

    const expectedFundingTs = expectedByTopic['market:funding']?.map((r) => r.payload.exchangeTs as number) ?? [];
    expect(replayedFunding.map((evt) => evt.meta.ts)).toEqual(expectedFundingTs);

    const analyticsFeatures = replayRuntime.getEvents('analytics:features');
    const analyticsFlow = replayRuntime.getEvents('analytics:flow');
    const analyticsLiquidity = replayRuntime.getEvents('analytics:liquidity');

    const sourceTs = new Set<number>([
      ...replayedTicker.map((evt) => evt.meta.ts),
      ...replayedKline.map((evt) => evt.meta.ts),
      ...replayedTrade.map((evt) => evt.meta.ts),
      ...replayedSnapshot.map((evt) => evt.meta.ts),
      ...replayedDelta.map((evt) => evt.meta.ts),
      ...replayedOi.map((evt) => evt.meta.ts),
      ...replayedFunding.map((evt) => evt.meta.ts),
    ]);

    analyticsFeatures.forEach((evt) => expect(sourceTs.has(evt.meta.ts)).toBe(true));
    analyticsFlow.forEach((evt) => expect(sourceTs.has(evt.meta.ts)).toBe(true));
    analyticsLiquidity.forEach((evt) => expect(sourceTs.has(evt.meta.ts)).toBe(true));

    await replayRuntime.stop();
  });

  it('replay discovery supports legacy and runId layouts', async () => {
    const legacyDir = path.join(tmpDir, STREAM_ID, SYMBOL, 'market-ticker');
    const runDir = path.join(tmpDir, STREAM_ID, SYMBOL, 'market-ticker', RUN_ID);
    await mkdir(legacyDir, { recursive: true });
    await mkdir(runDir, { recursive: true });

    const legacyRecord = {
      seq: 1,
      streamId: STREAM_ID,
      topic: 'market:ticker',
      symbol: SYMBOL,
      tsIngest: 1000,
      payload: {
        symbol: SYMBOL,
        streamId: STREAM_ID,
        marketType: 'futures',
        lastPrice: '100',
        meta: createMeta('market', { tsEvent: 1000, tsIngest: 1000, streamId: STREAM_ID }),
      },
    };
    const runRecord = {
      seq: 1,
      streamId: STREAM_ID,
      topic: 'market:ticker',
      symbol: SYMBOL,
      tsIngest: 2000,
      payload: {
        symbol: SYMBOL,
        streamId: STREAM_ID,
        marketType: 'futures',
        lastPrice: '101',
        meta: createMeta('market', { tsEvent: 2000, tsIngest: 2000, streamId: STREAM_ID }),
      },
    };

    await writeFile(path.join(legacyDir, '2024-01-01.jsonl'), JSON.stringify(legacyRecord), 'utf8');
    await writeFile(path.join(runDir, '2024-01-02.jsonl'), JSON.stringify(runRecord), 'utf8');

    const runtime = new TestRuntime({
      baseDir: tmpDir,
      runId: RUN_ID,
      streamId: STREAM_ID,
      symbol: SYMBOL,
      startJournal: false,
      startAnalytics: false,
      startStrategy: false,
      startRisk: false,
      startExecution: false,
      startPortfolio: false,
      startMetrics: false,
    }, new FakeClock());

    await runtime.runReplay({ topic: 'market:ticker', mode: 'max' });
    const runEvents = runtime.getEvents('market:ticker');
    expect(runEvents.map((evt) => evt.meta.ts)).toEqual([2000]);

    const legacyRuntime = new TestRuntime({
      baseDir: tmpDir,
      runId: 'missing-run',
      streamId: STREAM_ID,
      symbol: SYMBOL,
      startJournal: false,
      startAnalytics: false,
      startStrategy: false,
      startRisk: false,
      startExecution: false,
      startPortfolio: false,
      startMetrics: false,
    }, new FakeClock());

    await legacyRuntime.runReplay({ topic: 'market:ticker', mode: 'max', runId: 'missing-run' });
    const legacyEvents = legacyRuntime.getEvents('market:ticker');
    expect(legacyEvents.map((evt) => evt.meta.ts)).toEqual([1000]);

    await runtime.stop();
    await legacyRuntime.stop();
  });
});

async function listJsonlFiles(dir: string): Promise<string[]> {
  let entries: { name: string; isDirectory: () => boolean; isFile: () => boolean }[] = [];
  try {
    entries = await fs.readdir(dir, { withFileTypes: true });
  } catch {
    return [];
  }
  const files: string[] = [];
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await listJsonlFiles(fullPath)));
    } else if (entry.isFile() && entry.name.endsWith('.jsonl')) {
      files.push(fullPath);
    }
  }
  return files;
}

async function readJsonLines(filePath: string): Promise<JournalRecord[]> {
  const content = await readFile(filePath, 'utf8');
  return content
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line) as JournalRecord);
}

async function loadJournalRecords(baseDir: string, streamId: string, symbol: string): Promise<JournalRecord[]> {
  const files = await listJsonlFiles(path.join(baseDir, streamId, symbol));
  const records: JournalRecord[] = [];
  for (const file of files) {
    records.push(...(await readJsonLines(file)));
  }
  return records;
}

function groupRecordsByTopic(records: JournalRecord[]): Record<string, JournalRecord[]> {
  const grouped: Record<string, JournalRecord[]> = {};
  for (const record of records) {
    const list = grouped[record.topic] ?? [];
    list.push(record);
    grouped[record.topic] = list;
  }
  return grouped;
}

async function dirExists(dir: string): Promise<boolean> {
  try {
    const stat = await fs.stat(dir);
    return stat.isDirectory();
  } catch {
    return false;
  }
}
