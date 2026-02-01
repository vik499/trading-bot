import { createReadStream, type Dirent } from 'node:fs';
import * as fs from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import { inferMarketTypeFromStreamId } from '../core/market/symbols';
import {
  createMeta,
  eventBus,
  type EventBus,
  asTsMs,
  type FundingRateEvent,
  type KlineEvent,
  type LiquidationEvent,
  type OpenInterestEvent,
  type OrderbookL2DeltaEvent,
  type OrderbookL2SnapshotEvent,
  type ReplayError,
  type ReplayFinished,
  type ReplayProgress,
  type ReplayStarted,
  type ReplayWarning,
  type TickerEvent,
  type TradeEvent,
} from '../core/events/EventBus';

export type ReplayMode = 'max' | 'accelerated' | 'realtime';

export interface JournalReplayOptions {
  baseDir?: string;
  streamId: string;
  symbol: string;
  topic?:
    | 'market:ticker'
    | 'market:kline'
    | 'market:trade'
    | 'market:orderbook_l2_snapshot'
    | 'market:orderbook_l2_delta'
    | 'market:oi'
    | 'market:funding'
    | 'market:liquidation';
  tf?: string;
  runId?: string;
  dateFrom?: string;
  dateTo?: string;
  mode?: ReplayMode;
  speedFactor?: number;
  progressEveryMs?: number;
  progressEveryRecords?: number;
  yieldEvery?: number;
  skipBadFiles?: boolean;
  stopOnError?: boolean;
  sleepFn?: (ms: number) => Promise<void>;
  ordering?: 'ingest' | 'exchange';
}

interface ReplayStats {
  emittedCount: number;
  warningCount: number;
  lastSeq?: number;
  lastTsIngest?: number;
  filesProcessed: number;
}

interface JournalRecord {
  seq: number;
  streamId: string;
  runId?: string;
  topic:
    | 'market:ticker'
    | 'market:kline'
    | 'market:trade'
    | 'market:orderbook_l2_snapshot'
    | 'market:orderbook_l2_delta'
    | 'market:oi'
    | 'market:funding'
    | 'market:liquidation';
  symbol: string;
  tsIngest: number;
  payload:
    | TickerEvent
    | KlineEvent
    | TradeEvent
    | OrderbookL2SnapshotEvent
    | OrderbookL2DeltaEvent
    | OpenInterestEvent
    | FundingRateEvent
    | LiquidationEvent;
}

type SleepFn = (ms: number) => Promise<void>;

type ResolvedReplayOptions = Required<
  Omit<
    JournalReplayOptions,
    'dateFrom' | 'dateTo' | 'baseDir' | 'streamId' | 'symbol' | 'runId' | 'topic' | 'tf'
  >
> & {
  baseDir: string;
  streamId: string;
  symbol: string;
  topic:
    | 'market:ticker'
    | 'market:kline'
    | 'market:trade'
    | 'market:orderbook_l2_snapshot'
    | 'market:orderbook_l2_delta'
    | 'market:oi'
    | 'market:funding'
    | 'market:liquidation';
  tf?: string;
  runId?: string;
  dateFrom?: string;
  dateTo?: string;
};

function parseDateFromFilename(filename: string): string | undefined {
  const match = filename.match(/(\d{4}-\d{2}-\d{2})\.jsonl$/);
  return match?.[1];
}

function dateWithinRange(date: string, from?: string, to?: string): boolean {
  if (from && date < from) return false;
  if (to && date > to) return false;
  return true;
}

function sortJournalRecords(records: JournalRecord[]): JournalRecord[] {
  return [...records].sort((a, b) => {
    const tsA = recordExchangeTs(a);
    const tsB = recordExchangeTs(b);
    if (tsA !== tsB) return tsA - tsB;
    const seqA = recordSequenceId(a);
    const seqB = recordSequenceId(b);
    if (seqA !== seqB) return seqA - seqB;
    return a.streamId.localeCompare(b.streamId);
  });
}

function recordExchangeTs(record: JournalRecord): number {
  const payload = record.payload as JournalRecord['payload'];
  if ('exchangeTs' in payload && typeof payload.exchangeTs === 'number') return payload.exchangeTs;
  if ('tradeTs' in payload && typeof payload.tradeTs === 'number') return payload.tradeTs;
  if ('endTs' in payload && typeof payload.endTs === 'number') return payload.endTs;
  if ('meta' in payload && payload.meta && typeof payload.meta.ts === 'number') return payload.meta.ts;
  return record.tsIngest;
}

function recordSequenceId(record: JournalRecord): number {
  const payload = record.payload as JournalRecord['payload'];
  if ('updateId' in payload && typeof payload.updateId === 'number') return payload.updateId;
  if ('tradeId' in payload && payload.tradeId !== undefined) {
    const parsed = Number.parseInt(String(payload.tradeId), 10);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function createReplayMeta() {
  return createMeta('replay');
}

export class JournalReplayRunner {
  private readonly bus: EventBus;
  private readonly options: ResolvedReplayOptions;

  constructor(bus: EventBus = eventBus, options: JournalReplayOptions) {
    const defaultSleep: SleepFn = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
    this.bus = bus;
    this.options = {
      baseDir: options.baseDir ?? process.env.BOT_JOURNAL_DIR ?? './data/journal',
      streamId: options.streamId,
      symbol: options.symbol,
      topic: options.topic ?? 'market:ticker',
      tf: options.tf,
      runId: options.runId,
      dateFrom: options.dateFrom,
      dateTo: options.dateTo,
      mode: options.mode ?? 'max',
      speedFactor: options.speedFactor ?? 10,
      progressEveryMs: options.progressEveryMs ?? 1000,
      progressEveryRecords: options.progressEveryRecords ?? 500,
      yieldEvery: options.yieldEvery ?? 500,
      skipBadFiles: options.skipBadFiles ?? false,
      stopOnError: options.stopOnError ?? true,
      sleepFn: options.sleepFn ?? defaultSleep,
      ordering: options.ordering ?? 'ingest',
    };
  }

  public async run(): Promise<ReplayFinished> {
    const startedAt = Date.now();
    const files = await this.discoverFiles();

    const startedPayload: ReplayStarted = {
      meta: createReplayMeta(),
      streamId: this.options.streamId,
      symbol: this.options.symbol,
      filesCount: files.length,
      dateFrom: this.options.dateFrom,
      dateTo: this.options.dateTo,
      mode: this.options.mode,
      speedFactor: this.options.speedFactor,
    };
    this.bus.publish('replay:started', startedPayload);

    const stats: ReplayStats = {
      emittedCount: 0,
      warningCount: 0,
      filesProcessed: 0,
    };

    let prevTs: number | undefined;
    let lastProgressAt = Date.now();

    for (const file of files) {
      try {
        await this.processFile(file, stats, () => {
          lastProgressAt = this.maybeProgress(stats, file, lastProgressAt);
          if (this.options.mode === 'max' && stats.emittedCount > 0 && stats.emittedCount % this.options.yieldEvery === 0) {
            return this.options.sleepFn(0);
          }
          return Promise.resolve();
        }, (tsIngest) => {
          if (prevTs === undefined) {
            prevTs = tsIngest;
            return Promise.resolve();
          }
          const delta = Math.max(0, tsIngest - prevTs);
          prevTs = tsIngest;
          if (this.options.mode === 'max') return Promise.resolve();
          if (this.options.mode === 'accelerated') {
            const waitMs = delta / this.options.speedFactor;
            return waitMs > 0 ? this.options.sleepFn(waitMs) : Promise.resolve();
          }
          // realtime
          return delta > 0 ? this.options.sleepFn(delta) : Promise.resolve();
        });
        stats.filesProcessed += 1;
        lastProgressAt = this.maybeProgress(stats, file, lastProgressAt, true);
      } catch (error) {
        const errPayload: ReplayError = {
          meta: createReplayMeta(),
          file,
          message: (error as Error)?.message ?? 'replay error',
          error,
        };
        this.bus.publish('replay:error', errPayload);
        if (this.options.stopOnError) break;
        if (!this.options.skipBadFiles) break;
      }
    }

    const finishedPayload: ReplayFinished = {
      meta: createReplayMeta(),
      streamId: this.options.streamId,
      symbol: this.options.symbol,
      emittedCount: stats.emittedCount,
      warningCount: stats.warningCount,
      durationMs: Date.now() - startedAt,
      filesProcessed: stats.filesProcessed,
    };
    this.bus.publish('replay:finished', finishedPayload);
    return finishedPayload;
  }

  private async discoverFiles(): Promise<string[]> {
    const topicDir = topicToDir(this.options.topic);
    const baseDir = path.join(this.options.baseDir, this.options.streamId, this.options.symbol, topicDir);
    if (this.options.topic === 'market:kline' && this.options.tf) {
      const tfDir = path.join(baseDir, this.options.tf);
      const tfFiles = await this.discoverFilesInDir(tfDir);
      if (tfFiles.length) return tfFiles;
    }
    return this.discoverFilesInDir(baseDir);
  }

  private async discoverFilesInDir(dir: string): Promise<string[]> {
    let entries: Dirent[] = [];
    try {
      entries = await fs.readdir(dir, { withFileTypes: true });
    } catch (error) {
      const errPayload: ReplayError = {
        meta: createReplayMeta(),
        file: dir,
        message: (error as Error)?.message ?? 'failed to read journal directory',
        error,
      };
      this.bus.publish('replay:error', errPayload);
      throw error;
    }

    const legacyFiles = entries
      .filter((entry) => entry.isFile())
      .map((entry) => ({ name: entry.name, date: parseDateFromFilename(entry.name) }))
      .filter((x) => x.date && dateWithinRange(x.date, this.options.dateFrom, this.options.dateTo))
      .map((x) => path.join(dir, x.name));

    if (this.options.runId) {
      const hasRunDir = entries.some((entry) => entry.isDirectory() && entry.name === this.options.runId);
      if (hasRunDir) {
        const runDir = path.join(dir, this.options.runId);
        const runFiles = await this.listDateFiles(runDir);
        return runFiles.sort();
      }
      return legacyFiles.sort();
    }

    const runDirs = entries.filter((entry) => entry.isDirectory()).map((entry) => path.join(dir, entry.name));
    const runFiles: string[] = [];
    for (const runDir of runDirs) {
      runFiles.push(...(await this.listDateFiles(runDir)));
    }

    return [...legacyFiles, ...runFiles].sort();
  }

  private async listDateFiles(dir: string): Promise<string[]> {
    let entries: string[] = [];
    try {
      entries = await fs.readdir(dir);
    } catch {
      return [];
    }

    return entries
      .map((name) => ({ name, date: parseDateFromFilename(name) }))
      .filter((x) => x.date && dateWithinRange(x.date, this.options.dateFrom, this.options.dateTo))
      .map((x) => path.join(dir, x.name));
  }

  private async processFile(
    filePath: string,
    stats: ReplayStats,
    onProgressYield: () => Promise<void>,
    onTiming: (tsIngest: number) => Promise<void>
  ): Promise<void> {
    let lineNumber = 0;
    let lastSeq: number | undefined;
    const records: JournalRecord[] = [];

    const stream = createReadStream(filePath, { encoding: 'utf8' });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

    try {
      for await (const line of rl) {
        lineNumber += 1;
        let record: JournalRecord | undefined;
        try {
          record = JSON.parse(line) as JournalRecord;
        } catch (error) {
          this.emitWarning({ file: filePath, lineNumber, reason: `invalid json: ${(error as Error).message}` });
          stats.warningCount += 1;
          continue;
        }

        if (!this.isRecordValid(record)) {
          this.emitWarning({ file: filePath, lineNumber, reason: 'invalid record shape' });
          stats.warningCount += 1;
          continue;
        }

        if (lastSeq !== undefined && record.seq < lastSeq) {
          this.emitWarning({ file: filePath, lineNumber, reason: `seq decreased: ${record.seq} < ${lastSeq}` });
          stats.warningCount += 1;
        }
        lastSeq = record.seq;
        stats.lastSeq = record.seq;
        stats.lastTsIngest = record.tsIngest;
        records.push(record);
      }
    } finally {
      rl.close();
      stream.close();
    }

    const ordered = this.options.ordering === 'exchange' ? sortJournalRecords(records) : records;

    for (const record of ordered) {
      await onTiming(record.tsIngest);

      const metaTs = resolveReplayMetaTs(record);
      if (record.topic === 'market:ticker') {
        const raw = record.payload as TickerEvent;
        const streamId = raw.streamId ?? record.streamId;
        const marketType = raw.marketType ?? inferMarketTypeFromStreamId(streamId) ?? 'futures';
        const payload: TickerEvent = {
          ...raw,
          streamId,
          marketType,
          meta: createMeta('replay', {
            tsEvent: asTsMs(metaTs),
            tsIngest: asTsMs(record.tsIngest),
            tsExchange: raw.meta?.tsExchange,
            sequence: raw.meta?.sequence,
            streamId,
            correlationId: raw.meta?.correlationId,
          }),
        };
        this.bus.publish('market:ticker', payload);
        stats.emittedCount += 1;
      } else if (record.topic === 'market:kline') {
        const raw = record.payload as KlineEvent;
        const streamId = raw.streamId ?? record.streamId;
        const marketType = raw.marketType ?? inferMarketTypeFromStreamId(streamId) ?? 'futures';
        const payload: KlineEvent = {
          ...raw,
          streamId,
          marketType,
          meta: createMeta('replay', {
            tsEvent: asTsMs(metaTs),
            tsIngest: asTsMs(record.tsIngest),
            tsExchange: raw.meta?.tsExchange,
            sequence: raw.meta?.sequence,
            streamId,
            correlationId: raw.meta?.correlationId,
          }),
        };
        this.bus.publish('market:kline', payload);
        stats.emittedCount += 1;
      } else if (record.topic === 'market:trade') {
        const raw = record.payload as TradeEvent;
        const streamId = raw.streamId ?? record.streamId;
        const marketType = raw.marketType ?? inferMarketTypeFromStreamId(streamId) ?? 'futures';
        const payload: TradeEvent = {
          ...raw,
          streamId,
          marketType,
          meta: createMeta('replay', {
            tsEvent: asTsMs(metaTs),
            tsIngest: asTsMs(record.tsIngest),
            tsExchange: raw.meta?.tsExchange,
            sequence: raw.meta?.sequence,
            streamId,
            correlationId: raw.meta?.correlationId,
          }),
        };
        this.bus.publish('market:trade', payload);
        stats.emittedCount += 1;
      } else if (record.topic === 'market:orderbook_l2_snapshot') {
        const raw = record.payload as OrderbookL2SnapshotEvent;
        const streamId = raw.streamId ?? record.streamId;
        const marketType = raw.marketType ?? inferMarketTypeFromStreamId(streamId) ?? 'futures';
        const payload: OrderbookL2SnapshotEvent = {
          ...raw,
          streamId,
          marketType,
          meta: createMeta('replay', {
            tsEvent: asTsMs(metaTs),
            tsIngest: asTsMs(record.tsIngest),
            tsExchange: raw.meta?.tsExchange,
            sequence: raw.meta?.sequence,
            streamId,
            correlationId: raw.meta?.correlationId,
          }),
        };
        this.bus.publish('market:orderbook_l2_snapshot', payload);
        stats.emittedCount += 1;
      } else if (record.topic === 'market:orderbook_l2_delta') {
        const raw = record.payload as OrderbookL2DeltaEvent;
        const streamId = raw.streamId ?? record.streamId;
        const marketType = raw.marketType ?? inferMarketTypeFromStreamId(streamId) ?? 'futures';
        const payload: OrderbookL2DeltaEvent = {
          ...raw,
          streamId,
          marketType,
          meta: createMeta('replay', {
            tsEvent: asTsMs(metaTs),
            tsIngest: asTsMs(record.tsIngest),
            tsExchange: raw.meta?.tsExchange,
            sequence: raw.meta?.sequence,
            streamId,
            correlationId: raw.meta?.correlationId,
          }),
        };
        this.bus.publish('market:orderbook_l2_delta', payload);
        stats.emittedCount += 1;
      } else if (record.topic === 'market:oi') {
        const raw = record.payload as OpenInterestEvent;
        const streamId = raw.streamId ?? record.streamId;
        const marketType = raw.marketType ?? inferMarketTypeFromStreamId(streamId) ?? 'futures';
        const payload: OpenInterestEvent = {
          ...raw,
          streamId,
          marketType,
          openInterestUnit: raw.openInterestUnit ?? 'base',
          meta: createMeta('replay', {
            tsEvent: asTsMs(metaTs),
            tsIngest: asTsMs(record.tsIngest),
            tsExchange: raw.meta?.tsExchange,
            sequence: raw.meta?.sequence,
            streamId,
            correlationId: raw.meta?.correlationId,
          }),
        };
        this.bus.publish('market:oi', payload);
        stats.emittedCount += 1;
      } else if (record.topic === 'market:funding') {
        const raw = record.payload as FundingRateEvent;
        const streamId = raw.streamId ?? record.streamId;
        const marketType = raw.marketType ?? inferMarketTypeFromStreamId(streamId) ?? 'futures';
        const payload: FundingRateEvent = {
          ...raw,
          streamId,
          marketType,
          meta: createMeta('replay', {
            tsEvent: asTsMs(metaTs),
            tsIngest: asTsMs(record.tsIngest),
            tsExchange: raw.meta?.tsExchange,
            sequence: raw.meta?.sequence,
            streamId,
            correlationId: raw.meta?.correlationId,
          }),
        };
        this.bus.publish('market:funding', payload);
        stats.emittedCount += 1;
      } else if (record.topic === 'market:liquidation') {
        const raw = record.payload as LiquidationEvent;
        const streamId = raw.streamId ?? record.streamId;
        const marketType = raw.marketType ?? inferMarketTypeFromStreamId(streamId) ?? 'futures';
        const payload: LiquidationEvent = {
          ...raw,
          streamId,
          marketType,
          meta: createMeta('replay', {
            tsEvent: asTsMs(metaTs),
            tsIngest: asTsMs(record.tsIngest),
            tsExchange: raw.meta?.tsExchange,
            sequence: raw.meta?.sequence,
            streamId,
            correlationId: raw.meta?.correlationId,
          }),
        };
        this.bus.publish('market:liquidation', payload);
        stats.emittedCount += 1;
      }

      await onProgressYield();
    }
  }

  private emitWarning(payload: Omit<ReplayWarning, 'meta'>): void {
    const warning: ReplayWarning = { meta: createReplayMeta(), ...payload };
    this.bus.publish('replay:warning', warning);
  }

  private isRecordValid(record: Partial<JournalRecord>): record is JournalRecord {
    if (
      typeof record !== 'object' ||
      record === null ||
      typeof record.seq !== 'number' ||
      typeof record.tsIngest !== 'number' ||
      typeof record.symbol !== 'string' ||
      typeof record.streamId !== 'string' ||
      typeof record.payload !== 'object' ||
      record.payload === null
    ) {
      return false;
    }

    if (record.topic === 'market:ticker') {
      return typeof (record.payload as TickerEvent).symbol === 'string';
    }
    if (record.topic === 'market:kline') {
      const payload = record.payload as KlineEvent;
      return (
        typeof payload.symbol === 'string' &&
        typeof payload.interval === 'string' &&
        typeof payload.tf === 'string' &&
        typeof payload.startTs === 'number' &&
        typeof payload.endTs === 'number' &&
        typeof payload.close === 'number'
      );
    }
    if (record.topic === 'market:trade') {
      const payload = record.payload as TradeEvent;
      return (
        typeof payload.symbol === 'string' &&
        typeof payload.price === 'number' &&
        typeof payload.size === 'number' &&
        typeof payload.tradeTs === 'number'
      );
    }
    if (record.topic === 'market:orderbook_l2_snapshot' || record.topic === 'market:orderbook_l2_delta') {
      const payload = record.payload as OrderbookL2SnapshotEvent | OrderbookL2DeltaEvent;
      return (
        typeof payload.symbol === 'string' &&
        typeof payload.updateId === 'number' &&
        Array.isArray(payload.bids) &&
        Array.isArray(payload.asks)
      );
    }
    if (record.topic === 'market:oi') {
      const payload = record.payload as OpenInterestEvent;
      return typeof payload.symbol === 'string' && typeof payload.openInterest === 'number';
    }
    if (record.topic === 'market:funding') {
      const payload = record.payload as FundingRateEvent;
      return typeof payload.symbol === 'string' && typeof payload.fundingRate === 'number';
    }
    if (record.topic === 'market:liquidation') {
      const payload = record.payload as LiquidationEvent;
      return typeof payload.symbol === 'string';
    }
    return false;
  }

  private maybeProgress(stats: ReplayStats, currentFile: string, lastProgressAt: number, force = false): number {
    const now = Date.now();
    const shouldEmitByTime = now - lastProgressAt >= this.options.progressEveryMs;
    const shouldEmitByCount = stats.emittedCount > 0 && stats.emittedCount % this.options.progressEveryRecords === 0;
    if (!force && !shouldEmitByTime && !shouldEmitByCount) return lastProgressAt;

    const progress: ReplayProgress = {
      meta: createReplayMeta(),
      streamId: this.options.streamId,
      symbol: this.options.symbol,
      emittedCount: stats.emittedCount,
      warningsCount: stats.warningCount,
      currentFile,
      lastSeq: stats.lastSeq,
      lastTsIngest: stats.lastTsIngest,
    };
    this.bus.publish('replay:progress', progress);
    return now;
  }
}

function topicToDir(topic: JournalRecord['topic']): string {
  if (topic === 'market:orderbook_l2_snapshot' || topic === 'market:orderbook_l2_delta') {
    return 'market-orderbook-l2';
  }
  if (topic === 'market:trade') return 'market-trade';
  if (topic === 'market:oi') return 'market-oi';
  if (topic === 'market:funding') return 'market-funding';
  return topic.replace(':', '-');
}

function resolveReplayMetaTs(record: JournalRecord): number {
  if (record.topic === 'market:trade') {
    const payload = record.payload as TradeEvent;
    return payload.tradeTs ?? record.tsIngest;
  }
  if (record.topic === 'market:orderbook_l2_snapshot' || record.topic === 'market:orderbook_l2_delta') {
    const payload = record.payload as OrderbookL2SnapshotEvent | OrderbookL2DeltaEvent;
    return payload.exchangeTs ?? record.tsIngest;
  }
  if (record.topic === 'market:oi') {
    const payload = record.payload as OpenInterestEvent;
    return payload.exchangeTs ?? record.tsIngest;
  }
  if (record.topic === 'market:funding') {
    const payload = record.payload as FundingRateEvent;
    return payload.exchangeTs ?? record.tsIngest;
  }
  if (record.topic === 'market:liquidation') {
    const payload = record.payload as LiquidationEvent;
    return payload.exchangeTs ?? record.tsIngest;
  }
  return record.tsIngest;
}

export function createJournalReplayRunner(bus: EventBus = eventBus, options: JournalReplayOptions): JournalReplayRunner {
  return new JournalReplayRunner(bus, options);
}
