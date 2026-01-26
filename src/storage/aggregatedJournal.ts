import { randomUUID } from 'node:crypto';
import * as fs from 'node:fs/promises';
import path from 'node:path';
import {
  createMeta,
  eventBus,
  type EventBus,
  type MarketCvdAggEvent,
  type MarketFundingAggEvent,
  type MarketLiquidationsAggEvent,
  type MarketLiquidityAggEvent,
  type MarketDataStatusPayload,
  type MarketOpenInterestAggEvent,
  type MarketPriceCanonicalEvent,
  type MarketPriceIndexEvent,
  type MarketVolumeAggEvent,
  type StorageWriteFailed,
} from '../core/events/EventBus';
import { formatUtcDate } from '../core/time/utc';

export interface AggregatedEventJournalOptions {
  baseDir: string;
  runId: string;
  flushIntervalMs: number;
  maxBatchSize: number;
}

interface PendingRecord {
  path: string;
  line: string;
}

interface JournalRecord {
  seq: number;
  runId?: string;
  topic:
    | 'system:market_data_status'
    | 'market:oi_agg'
    | 'market:funding_agg'
    | 'market:liquidations_agg'
    | 'market:volume_agg'
    | 'market:cvd_spot_agg'
    | 'market:cvd_futures_agg'
    | 'market:cvd_agg'
    | 'market:price_index'
    | 'market:price_canonical'
    | 'market:liquidity_agg';
  tsIngest: number;
  payload:
    | MarketDataStatusPayload
    | MarketOpenInterestAggEvent
    | MarketFundingAggEvent
    | MarketLiquidationsAggEvent
    | MarketVolumeAggEvent
    | MarketCvdAggEvent
    | MarketPriceIndexEvent
    | MarketPriceCanonicalEvent
    | MarketLiquidityAggEvent;
}

export class AggregatedEventJournal {
  private readonly options: AggregatedEventJournalOptions;
  private readonly bus: EventBus;
  private queue: PendingRecord[] = [];
  private timer?: NodeJS.Timeout;
  private readonly seqByPath = new Map<string, number>();
  private isStarted = false;
  private flushPromise?: Promise<void>;

  private readonly onOiAgg = (payload: MarketOpenInterestAggEvent) => this.onAgg('market:oi_agg', payload);
  private readonly onFundingAgg = (payload: MarketFundingAggEvent) => this.onAgg('market:funding_agg', payload);
  private readonly onLiqAgg = (payload: MarketLiquidationsAggEvent) => this.onAgg('market:liquidations_agg', payload);
  private readonly onVolumeAgg = (payload: MarketVolumeAggEvent) => this.onAgg('market:volume_agg', payload);
  private readonly onCvdSpotAgg = (payload: MarketCvdAggEvent) => this.onAgg('market:cvd_spot_agg', payload);
  private readonly onCvdFuturesAgg = (payload: MarketCvdAggEvent) => this.onAgg('market:cvd_futures_agg', payload);
  private readonly onCvdAgg = (payload: MarketCvdAggEvent) => this.onAgg('market:cvd_agg', payload);
  private readonly onPriceIndex = (payload: MarketPriceIndexEvent) => this.onAgg('market:price_index', payload);
  private readonly onPriceCanonical = (payload: MarketPriceCanonicalEvent) => this.onAgg('market:price_canonical', payload);
  private readonly onLiquidityAgg = (payload: MarketLiquidityAggEvent) => this.onAgg('market:liquidity_agg', payload);

  constructor(bus: EventBus = eventBus, options: Partial<AggregatedEventJournalOptions> = {}) {
    const runId = options.runId ?? process.env.BOT_RUN_ID ?? `run-${randomUUID()}`;
    this.bus = bus;
    this.options = {
      baseDir: './data/journal',
      runId,
      flushIntervalMs: 200,
      maxBatchSize: 50,
      ...options,
    };
  }

  start(): void {
    if (this.isStarted) return;
    this.bus.subscribe('system:market_data_status', this.onMarketStatus);
    this.bus.subscribe('market:oi_agg', this.onOiAgg);
    this.bus.subscribe('market:funding_agg', this.onFundingAgg);
    this.bus.subscribe('market:liquidations_agg', this.onLiqAgg);
    this.bus.subscribe('market:volume_agg', this.onVolumeAgg);
    this.bus.subscribe('market:cvd_spot_agg', this.onCvdSpotAgg);
    this.bus.subscribe('market:cvd_futures_agg', this.onCvdFuturesAgg);
    this.bus.subscribe('market:cvd_agg', this.onCvdAgg);
    this.bus.subscribe('market:price_index', this.onPriceIndex);
    this.bus.subscribe('market:price_canonical', this.onPriceCanonical);
    this.bus.subscribe('market:liquidity_agg', this.onLiquidityAgg);
    this.isStarted = true;
  }

  async stop(): Promise<void> {
    if (!this.isStarted) return;
    this.bus.unsubscribe('system:market_data_status', this.onMarketStatus);
    this.bus.unsubscribe('market:oi_agg', this.onOiAgg);
    this.bus.unsubscribe('market:funding_agg', this.onFundingAgg);
    this.bus.unsubscribe('market:liquidations_agg', this.onLiqAgg);
    this.bus.unsubscribe('market:volume_agg', this.onVolumeAgg);
    this.bus.unsubscribe('market:cvd_spot_agg', this.onCvdSpotAgg);
    this.bus.unsubscribe('market:cvd_futures_agg', this.onCvdFuturesAgg);
    this.bus.unsubscribe('market:cvd_agg', this.onCvdAgg);
    this.bus.unsubscribe('market:price_index', this.onPriceIndex);
    this.bus.unsubscribe('market:price_canonical', this.onPriceCanonical);
    this.bus.unsubscribe('market:liquidity_agg', this.onLiquidityAgg);
    this.isStarted = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    await this.flush();
  }

  public partitionPath(tsIngest: number, topic: JournalRecord['topic'], symbol?: string): string {
    const day = formatUtcDate(tsIngest);
    if (topic === 'system:market_data_status') {
      return path.join(this.options.baseDir, 'system', 'market-data-status', this.options.runId, `${day}.jsonl`);
    }
    const safeSymbol = symbol ?? 'UNKNOWN';
    const dir = topic.replace(':', '-');
    return path.join(this.options.baseDir, 'aggregated', dir, safeSymbol, this.options.runId, `${day}.jsonl`);
  }

  private onMarketStatus = (payload: MarketDataStatusPayload): void => {
    const record = this.makeRecord('system:market_data_status', payload);
    this.queueRecord(record);
  };

  private onAgg = (
    topic: JournalRecord['topic'],
    payload:
      | MarketOpenInterestAggEvent
      | MarketFundingAggEvent
      | MarketLiquidationsAggEvent
      | MarketVolumeAggEvent
      | MarketCvdAggEvent
      | MarketPriceIndexEvent
      | MarketPriceCanonicalEvent
      | MarketLiquidityAggEvent
  ): void => {
    const record = this.makeRecord(topic, payload);
    this.queueRecord(record);
  };

  private makeRecord(topic: JournalRecord['topic'], payload: JournalRecord['payload']): JournalRecord {
    const tsIngest = payload.meta.ts;
    const symbol = (payload as { symbol?: string }).symbol;
    const filePath = this.partitionPath(tsIngest, topic, symbol);
    const seq = (this.seqByPath.get(filePath) ?? 0) + 1;
    this.seqByPath.set(filePath, seq);
    return {
      seq,
      runId: this.options.runId,
      topic,
      tsIngest,
      payload,
    };
  }

  private queueRecord(record: JournalRecord): void {
    const filePath = this.partitionPath(record.tsIngest, record.topic, (record.payload as { symbol?: string }).symbol);
    this.queue.push({ path: filePath, line: `${JSON.stringify(record)}\n` });
    this.scheduleFlush();
  }


  private scheduleFlush(): void {
    if (this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.flush();
    }, this.options.flushIntervalMs);
  }

  private async flush(): Promise<void> {
    if (this.flushPromise) return this.flushPromise;
    if (this.queue.length === 0) return;

    const batch = this.queue.splice(0, this.options.maxBatchSize);
    this.flushPromise = (async () => {
      for (const item of batch) {
        try {
          await fs.mkdir(path.dirname(item.path), { recursive: true });
          await fs.appendFile(item.path, item.line, 'utf8');
        } catch (error) {
          const payload: StorageWriteFailed = {
            path: item.path,
            error,
            meta: createMeta('storage'),
          };
          this.bus.publish('storage:writeFailed', payload);
        }
      }
    })();

    try {
      await this.flushPromise;
    } finally {
      this.flushPromise = undefined;
    }
  }
}

export function createAggregatedEventJournal(
  bus: EventBus = eventBus,
  options: Partial<AggregatedEventJournalOptions> = {}
): AggregatedEventJournal {
  return new AggregatedEventJournal(bus, options);
}
