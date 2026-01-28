import { randomUUID } from 'node:crypto';
import * as fs from 'node:fs/promises';
import path from 'node:path';
import {
  asSeq,
  asTsMs,
  createMeta,
  eventBus,
  type EventBus,
  type FundingRateEvent,
  type KlineEvent,
  type LiquidationEvent,
  type CandleRawEvent,
  type FundingRawEvent,
  type IndexPriceRawEvent,
  type LiquidationRawEvent,
  type MarkPriceRawEvent,
  type OpenInterestRawEvent,
  type OrderbookDeltaRawEvent,
  type OrderbookSnapshotRawEvent,
  type TradeRawEvent,
  type OpenInterestEvent,
  type OrderbookL2DeltaEvent,
  type OrderbookL2SnapshotEvent,
  type TradeEvent,
  type TickerEvent,
  type DataGapDetected,
  type DataDuplicateDetected,
  type DataSequenceGapOrOutOfOrder,
  type DataTimeOutOfOrder,
  type LatencySpikeDetected,
  type StorageWriteFailed,
} from '../core/events/EventBus';
import { formatUtcDate } from '../core/time/utc';

export interface EventJournalOptions {
  baseDir: string;
  streamId: string;
  runId: string;
  latencySpikeMs: number;
  gapMs: number;
  oiGapMs: number;
  fundingGapMs: number;
  flushIntervalMs: number;
  maxBatchSize: number;
}

interface PendingRecord {
  path: string;
  line: string;
}

type JournalTopic =
  | 'market:ticker'
  | 'market:kline'
  | 'market:trade'
  | 'market:orderbook_l2_snapshot'
  | 'market:orderbook_l2_delta'
  | 'market:oi'
  | 'market:funding'
  | 'market:liquidation'
  | 'market:trade_raw'
  | 'market:orderbook_snapshot_raw'
  | 'market:orderbook_delta_raw'
  | 'market:candle_raw'
  | 'market:mark_price_raw'
  | 'market:index_price_raw'
  | 'market:funding_raw'
  | 'market:open_interest_raw'
  | 'market:liquidation_raw';

interface JournalRecord {
  seq: number;
  streamId: string;
  runId?: string;
  topic: JournalTopic;
  symbol: string;
  tsIngest: number;
  tsExchange?: number;
  payload:
    | TickerEvent
    | KlineEvent
    | TradeEvent
    | OrderbookL2SnapshotEvent
    | OrderbookL2DeltaEvent
    | OpenInterestEvent
    | FundingRateEvent
    | LiquidationEvent
    | TradeRawEvent
    | OrderbookSnapshotRawEvent
    | OrderbookDeltaRawEvent
    | CandleRawEvent
    | MarkPriceRawEvent
    | IndexPriceRawEvent
    | FundingRawEvent
    | OpenInterestRawEvent
    | LiquidationRawEvent;
}

const RAW_FORBIDDEN_FIELDS = [
  'qualityFlags',
  'confidenceScore',
  'venueBreakdown',
  'staleSourcesDropped',
  'mismatchDetected',
  'sourcesUsed',
  'weightsUsed',
];

export class EventJournal {
  private readonly options: EventJournalOptions;
  private readonly bus: EventBus;
  private queue: PendingRecord[] = [];
  private timer?: NodeJS.Timeout;
  private readonly seqByPath = new Map<string, number>();
  private lastExchangeTs = new Map<string, number>();
  private lastKlineEndTs = new Map<string, number>();
  private lastTradeTs = new Map<string, number>();
  private lastTradeId = new Map<string, string>();
  private lastOrderbookUpdateId = new Map<string, number>();
  private lastOrderbookTs = new Map<string, number>();
  private lastOiTs = new Map<string, number>();
  private lastFundingTs = new Map<string, number>();
  private readonly failedPaths = new Set<string>();
  private isStarted = false;
  private flushPromise?: Promise<void>;

  constructor(bus: EventBus = eventBus, options: Partial<EventJournalOptions> = {}) {
    this.bus = bus;
    const runId = options.runId ?? process.env.BOT_RUN_ID ?? `run-${randomUUID()}`;
    this.options = {
      baseDir: './data/journal',
      streamId: 'bybit.public.linear.v5',
      runId,
      latencySpikeMs: 2000,
      gapMs: 5000,
      oiGapMs: 120_000,
      fundingGapMs: 300_000,
      flushIntervalMs: 200,
      maxBatchSize: 50,
      ...options,
    };
  }

  start(): void {
    if (this.isStarted) return;
    this.bus.subscribe('market:ticker', this.onTicker);
    this.bus.subscribe('market:kline', this.onKline);
    this.bus.subscribe('market:trade', this.onTrade);
    this.bus.subscribe('market:orderbook_l2_snapshot', this.onOrderbookSnapshot);
    this.bus.subscribe('market:orderbook_l2_delta', this.onOrderbookDelta);
    this.bus.subscribe('market:oi', this.onOpenInterest);
    this.bus.subscribe('market:funding', this.onFunding);
    this.bus.subscribe('market:liquidation', this.onLiquidation);
    this.bus.subscribe('market:trade_raw', this.onTradeRaw);
    this.bus.subscribe('market:orderbook_snapshot_raw', this.onOrderbookSnapshotRaw);
    this.bus.subscribe('market:orderbook_delta_raw', this.onOrderbookDeltaRaw);
    this.bus.subscribe('market:candle_raw', this.onCandleRaw);
    this.bus.subscribe('market:mark_price_raw', this.onMarkPriceRaw);
    this.bus.subscribe('market:index_price_raw', this.onIndexPriceRaw);
    this.bus.subscribe('market:funding_raw', this.onFundingRaw);
    this.bus.subscribe('market:open_interest_raw', this.onOpenInterestRaw);
    this.bus.subscribe('market:liquidation_raw', this.onLiquidationRaw);
    this.isStarted = true;
  }

  async stop(): Promise<void> {
    if (!this.isStarted) return;
    this.bus.unsubscribe('market:ticker', this.onTicker);
    this.bus.unsubscribe('market:kline', this.onKline);
    this.bus.unsubscribe('market:trade', this.onTrade);
    this.bus.unsubscribe('market:orderbook_l2_snapshot', this.onOrderbookSnapshot);
    this.bus.unsubscribe('market:orderbook_l2_delta', this.onOrderbookDelta);
    this.bus.unsubscribe('market:oi', this.onOpenInterest);
    this.bus.unsubscribe('market:funding', this.onFunding);
    this.bus.unsubscribe('market:liquidation', this.onLiquidation);
    this.bus.unsubscribe('market:trade_raw', this.onTradeRaw);
    this.bus.unsubscribe('market:orderbook_snapshot_raw', this.onOrderbookSnapshotRaw);
    this.bus.unsubscribe('market:orderbook_delta_raw', this.onOrderbookDeltaRaw);
    this.bus.unsubscribe('market:candle_raw', this.onCandleRaw);
    this.bus.unsubscribe('market:mark_price_raw', this.onMarkPriceRaw);
    this.bus.unsubscribe('market:index_price_raw', this.onIndexPriceRaw);
    this.bus.unsubscribe('market:funding_raw', this.onFundingRaw);
    this.bus.unsubscribe('market:open_interest_raw', this.onOpenInterestRaw);
    this.bus.unsubscribe('market:liquidation_raw', this.onLiquidationRaw);
    this.isStarted = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    await this.flush();
  }

  /**
   * Returns the partition path for a given symbol/topic/day. Useful for tests.
   */
  public partitionPath(symbol: string, tsIngest: number, topic: string = 'market:ticker', tf?: string): string {
    const day = this.dayString(tsIngest);
    const topicDir = this.topicDir(topic);
    if (topic === 'market:kline' || topic === 'market:candle_raw') {
      const tfDir = tf ?? 'unknown-tf';
      return path.join(this.options.baseDir, this.options.streamId, symbol, topicDir, tfDir, this.options.runId, `${day}.jsonl`);
    }
    return path.join(this.options.baseDir, this.options.streamId, symbol, topicDir, this.options.runId, `${day}.jsonl`);
  }

  private onTicker = (payload: TickerEvent): void => {
    const record = this.makeRecord('market:ticker', payload, payload.exchangeTs);
    this.queueRecord(record);
    this.detectTickerQuality(payload, record);
  };

  private onKline = (payload: KlineEvent): void => {
    const record = this.makeRecord('market:kline', payload);
    this.queueRecord(record);
    this.detectKlineQuality(payload);
  };

  private onTrade = (payload: TradeEvent): void => {
    const record = this.makeRecord('market:trade', payload, payload.exchangeTs ?? payload.tradeTs);
    this.queueRecord(record);
    this.detectTradeQuality(payload, record);
  };

  private onOrderbookSnapshot = (payload: OrderbookL2SnapshotEvent): void => {
    const record = this.makeRecord('market:orderbook_l2_snapshot', payload, payload.exchangeTs);
    this.queueRecord(record);
    this.detectOrderbookQuality(payload, record, 'market:orderbook_l2_snapshot');
  };

  private onOrderbookDelta = (payload: OrderbookL2DeltaEvent): void => {
    const record = this.makeRecord('market:orderbook_l2_delta', payload, payload.exchangeTs);
    this.queueRecord(record);
    this.detectOrderbookQuality(payload, record, 'market:orderbook_l2_delta');
  };

  private onOpenInterest = (payload: OpenInterestEvent): void => {
    const record = this.makeRecord('market:oi', payload, payload.exchangeTs);
    this.queueRecord(record);
    this.detectOiQuality(payload, record);
  };

  private onFunding = (payload: FundingRateEvent): void => {
    const record = this.makeRecord('market:funding', payload, payload.exchangeTs);
    this.queueRecord(record);
    this.detectFundingQuality(payload, record);
  };

  private onLiquidation = (payload: LiquidationEvent): void => {
    const record = this.makeRecord('market:liquidation', payload, payload.exchangeTs);
    this.queueRecord(record);
  };

  private onTradeRaw = (payload: TradeRawEvent): void => {
    this.assertRawPayload(payload, 'market:trade_raw');
    const record = this.makeRecord('market:trade_raw', payload, payload.exchangeTsMs);
    this.queueRecord(record);
  };

  private onOrderbookSnapshotRaw = (payload: OrderbookSnapshotRawEvent): void => {
    this.assertRawPayload(payload, 'market:orderbook_snapshot_raw');
    const record = this.makeRecord('market:orderbook_snapshot_raw', payload, payload.exchangeTsMs);
    this.queueRecord(record);
  };

  private onOrderbookDeltaRaw = (payload: OrderbookDeltaRawEvent): void => {
    this.assertRawPayload(payload, 'market:orderbook_delta_raw');
    const record = this.makeRecord('market:orderbook_delta_raw', payload, payload.exchangeTsMs);
    this.queueRecord(record);
  };

  private onCandleRaw = (payload: CandleRawEvent): void => {
    this.assertRawPayload(payload, 'market:candle_raw');
    const record = this.makeRecord('market:candle_raw', payload, payload.exchangeTsMs);
    this.queueRecord(record);
  };

  private onMarkPriceRaw = (payload: MarkPriceRawEvent): void => {
    this.assertRawPayload(payload, 'market:mark_price_raw');
    const record = this.makeRecord('market:mark_price_raw', payload, payload.exchangeTsMs);
    this.queueRecord(record);
  };

  private onIndexPriceRaw = (payload: IndexPriceRawEvent): void => {
    this.assertRawPayload(payload, 'market:index_price_raw');
    const record = this.makeRecord('market:index_price_raw', payload, payload.exchangeTsMs);
    this.queueRecord(record);
  };

  private onFundingRaw = (payload: FundingRawEvent): void => {
    this.assertRawPayload(payload, 'market:funding_raw');
    const record = this.makeRecord('market:funding_raw', payload, payload.exchangeTsMs);
    this.queueRecord(record);
  };

  private onOpenInterestRaw = (payload: OpenInterestRawEvent): void => {
    this.assertRawPayload(payload, 'market:open_interest_raw');
    const record = this.makeRecord('market:open_interest_raw', payload, payload.exchangeTsMs);
    this.queueRecord(record);
  };

  private onLiquidationRaw = (payload: LiquidationRawEvent): void => {
    this.assertRawPayload(payload, 'market:liquidation_raw');
    const record = this.makeRecord('market:liquidation_raw', payload, payload.exchangeTsMs);
    this.queueRecord(record);
  };

  private assertRawPayload(payload: object, topic: string): void {
    const record = payload as Record<string, unknown>;
    for (const key of RAW_FORBIDDEN_FIELDS) {
      if (key in record) {
        throw new Error(`[EventJournal] raw event contains forbidden field: ${key} topic=${topic}`);
      }
    }
  }

  private makeRecord(
    topic: JournalTopic,
    payload:
      | TickerEvent
      | KlineEvent
      | TradeEvent
      | OrderbookL2SnapshotEvent
      | OrderbookL2DeltaEvent
      | OpenInterestEvent
      | FundingRateEvent
      | LiquidationEvent
      | TradeRawEvent
      | OrderbookSnapshotRawEvent
      | OrderbookDeltaRawEvent
      | CandleRawEvent
      | MarkPriceRawEvent
      | IndexPriceRawEvent
      | FundingRawEvent
      | OpenInterestRawEvent
      | LiquidationRawEvent,
    tsExchange?: number
  ): JournalRecord {
    const tsIngest = payload.meta.tsIngest ?? payload.meta.tsEvent;
    const tf =
      topic === 'market:kline'
        ? (payload as KlineEvent).tf
        : topic === 'market:candle_raw'
          ? (payload as CandleRawEvent).interval
          : undefined;
    const filePath = this.partitionPath(payload.symbol, tsIngest, topic, tf);
    const seq = (this.seqByPath.get(filePath) ?? 0) + 1;
    this.seqByPath.set(filePath, seq);
    return {
      seq,
      streamId: this.options.streamId,
      runId: this.options.runId,
      topic,
      symbol: payload.symbol,
      tsIngest,
      tsExchange,
      payload,
    };
  }

  private queueRecord(record: JournalRecord): void {
    const tf = record.topic === 'market:kline' ? (record.payload as KlineEvent).tf : undefined;
    const filePath = this.partitionPath(record.symbol, record.tsIngest, record.topic, tf);
    this.queue.push({ path: filePath, line: `${JSON.stringify(record)}\n` });
    this.scheduleFlush();
  }

  private detectTickerQuality(payload: TickerEvent, record: JournalRecord): void {
    const exchangeTs = payload.exchangeTs;
    if (exchangeTs !== undefined) {
      const key = this.qualityKey(payload.symbol, payload.streamId, 'market:ticker');
      const streamId = this.payloadStreamId(payload);
      const prev = this.lastExchangeTs.get(key);
      if (prev !== undefined) {
        if (exchangeTs < prev) {
          const evt: DataTimeOutOfOrder = {
            meta: createMeta('storage'),
            symbol: payload.symbol,
            streamId,
            topic: 'market:ticker',
            prevTs: asTsMs(prev),
            currTs: asTsMs(exchangeTs),
            tsSource: 'exchange',
          };
          this.bus.publish('data:time_out_of_order', evt);
        }
        const delta = exchangeTs - prev;
        if (delta > this.options.gapMs) {
          const evt: DataGapDetected = {
            meta: createMeta('storage'),
            symbol: payload.symbol,
            streamId,
            topic: 'market:ticker',
            prevTsExchange: prev,
            currTsExchange: exchangeTs,
            deltaMs: delta,
          };
          this.bus.publish('data:gapDetected', evt);
        }
      }
      const latency = record.tsIngest - exchangeTs;
      if (latency > this.options.latencySpikeMs) {
        const evt: LatencySpikeDetected = {
          meta: createMeta('storage'),
          symbol: payload.symbol,
          streamId,
          topic: 'market:ticker',
          latencyMs: latency,
          tsExchange: exchangeTs,
          tsIngest: record.tsIngest,
          thresholdMs: this.options.latencySpikeMs,
        };
        this.bus.publish('data:latencySpike', evt);
      }
      this.lastExchangeTs.set(key, exchangeTs);
    }
  }

  private detectKlineQuality(payload: KlineEvent): void {
    const endTs = payload.meta.tsEvent;
    const key = this.qualityKey(payload.symbol, payload.streamId, `market:kline:${payload.tf ?? 'unknown'}`);
    const prev = this.lastKlineEndTs.get(key);
    if (prev !== undefined && endTs < prev) {
      const streamId = this.payloadStreamId(payload);
      const evt: DataTimeOutOfOrder = {
        meta: createMeta('storage'),
        symbol: payload.symbol,
        streamId,
        topic: 'market:kline',
        prevTs: asTsMs(prev),
        currTs: asTsMs(endTs),
        tsSource: 'event',
      };
      this.bus.publish('data:time_out_of_order', evt);
    }
    this.lastKlineEndTs.set(key, endTs);
  }

  private detectTradeQuality(payload: TradeEvent, record: JournalRecord): void {
    const tradeTs = payload.tradeTs;
    const key = this.qualityKey(payload.symbol, payload.streamId, 'market:trade');
    const streamId = this.payloadStreamId(payload);
    const prevTs = this.lastTradeTs.get(key);
    if (prevTs !== undefined) {
      if (tradeTs < prevTs) {
        const evt: DataTimeOutOfOrder = {
          meta: createMeta('storage'),
          symbol: payload.symbol,
          streamId,
          topic: 'market:trade',
          prevTs: asTsMs(prevTs),
          currTs: asTsMs(tradeTs),
          tsSource: 'event',
        };
        this.bus.publish('data:time_out_of_order', evt);
      } else if (tradeTs === prevTs) {
        const evt: DataDuplicateDetected = {
          meta: createMeta('storage'),
          symbol: payload.symbol,
          streamId,
          topic: 'market:trade',
          key: payload.tradeId,
          tsExchange: tradeTs,
        };
        this.bus.publish('data:duplicateDetected', evt);
      }
    }

    const lastId = this.lastTradeId.get(key);
    if (payload.tradeId && lastId === payload.tradeId) {
      const evt: DataDuplicateDetected = {
        meta: createMeta('storage'),
        symbol: payload.symbol,
        streamId,
        topic: 'market:trade',
        key: payload.tradeId,
        tsExchange: tradeTs,
      };
      this.bus.publish('data:duplicateDetected', evt);
    }

    const exchangeTs = payload.exchangeTs ?? tradeTs;
    if (exchangeTs !== undefined) {
      const latency = record.tsIngest - exchangeTs;
      if (latency > this.options.latencySpikeMs) {
        const evt: LatencySpikeDetected = {
          meta: createMeta('storage'),
          symbol: payload.symbol,
          streamId,
          topic: 'market:trade',
          latencyMs: latency,
          tsExchange: exchangeTs,
          tsIngest: record.tsIngest,
          thresholdMs: this.options.latencySpikeMs,
        };
        this.bus.publish('data:latencySpike', evt);
      }
    }

    this.lastTradeTs.set(key, tradeTs);
    if (payload.tradeId) this.lastTradeId.set(key, payload.tradeId);
  }

  private detectOrderbookQuality(
    payload: OrderbookL2SnapshotEvent | OrderbookL2DeltaEvent,
    record: JournalRecord,
    topic: 'market:orderbook_l2_snapshot' | 'market:orderbook_l2_delta'
  ): void {
    const key = this.qualityKey(payload.symbol, payload.streamId, topic);
    const exchangeTs = payload.exchangeTs;
    const streamId = this.payloadStreamId(payload);
    const sequence = payload.meta.sequence !== undefined ? Number(payload.meta.sequence) : undefined;
    if (sequence !== undefined) {
      const prev = this.lastOrderbookUpdateId.get(key);
      if (prev !== undefined) {
        if (sequence < prev) {
          const evt: DataSequenceGapOrOutOfOrder = {
            meta: createMeta('storage'),
            symbol: payload.symbol,
            streamId,
            topic,
            prevSeq: asSeq(prev),
            currSeq: asSeq(sequence),
            kind: 'out_of_order',
          };
          this.bus.publish('data:sequence_gap_or_out_of_order', evt);
        } else if (sequence === prev) {
          const evt: DataSequenceGapOrOutOfOrder = {
            meta: createMeta('storage'),
            symbol: payload.symbol,
            streamId,
            topic,
            prevSeq: asSeq(prev),
            currSeq: asSeq(sequence),
            kind: 'duplicate',
          };
          this.bus.publish('data:sequence_gap_or_out_of_order', evt);
        } else if (sequence > prev + 1) {
          const evt: DataSequenceGapOrOutOfOrder = {
            meta: createMeta('storage'),
            symbol: payload.symbol,
            streamId,
            topic,
            prevSeq: asSeq(prev),
            currSeq: asSeq(sequence),
            kind: 'gap',
          };
          this.bus.publish('data:sequence_gap_or_out_of_order', evt);
        }
      }
      this.lastOrderbookUpdateId.set(key, sequence);
    }

    const prevTs = this.lastOrderbookTs.get(key);
    if (exchangeTs !== undefined && prevTs !== undefined) {
      const delta = exchangeTs - prevTs;
      if (delta > this.options.gapMs) {
        const evt: DataGapDetected = {
          meta: createMeta('storage'),
          symbol: payload.symbol,
          streamId,
          topic,
          prevTsExchange: prevTs,
          currTsExchange: exchangeTs,
          deltaMs: delta,
        };
        this.bus.publish('data:gapDetected', evt);
      }

      const latency = record.tsIngest - exchangeTs;
      if (latency > this.options.latencySpikeMs) {
        const evt: LatencySpikeDetected = {
          meta: createMeta('storage'),
          symbol: payload.symbol,
          streamId,
          topic,
          latencyMs: latency,
          tsExchange: exchangeTs,
          tsIngest: record.tsIngest,
          thresholdMs: this.options.latencySpikeMs,
        };
        this.bus.publish('data:latencySpike', evt);
      }
    }

    if (exchangeTs !== undefined) this.lastOrderbookTs.set(key, exchangeTs);
  }

  private detectOiQuality(payload: OpenInterestEvent, record: JournalRecord): void {
    const ts = payload.exchangeTs ?? payload.meta.tsExchange ?? payload.meta.tsEvent;
    const key = this.qualityKey(payload.symbol, payload.streamId, 'market:oi');
    const streamId = this.payloadStreamId(payload);
    const prev = this.lastOiTs.get(key);
    if (prev !== undefined) {
      if (ts < prev) {
        const evt: DataTimeOutOfOrder = {
          meta: createMeta('storage'),
          symbol: payload.symbol,
          streamId,
          topic: 'market:oi',
          prevTs: asTsMs(prev),
          currTs: asTsMs(ts),
          tsSource: payload.exchangeTs !== undefined || payload.meta.tsExchange !== undefined ? 'exchange' : 'event',
        };
        this.bus.publish('data:time_out_of_order', evt);
      } else if (ts === prev) {
        const evt: DataDuplicateDetected = {
          meta: createMeta('storage'),
          symbol: payload.symbol,
          streamId,
          topic: 'market:oi',
          tsExchange: ts,
        };
        this.bus.publish('data:duplicateDetected', evt);
      } else {
        const delta = ts - prev;
        if (delta > this.options.oiGapMs) {
          const evt: DataGapDetected = {
            meta: createMeta('storage'),
            symbol: payload.symbol,
            streamId,
            topic: 'market:oi',
            prevTsExchange: prev,
            currTsExchange: ts,
            deltaMs: delta,
          };
          this.bus.publish('data:gapDetected', evt);
        }
      }
    }

    const exchangeTs = payload.exchangeTs;
    if (exchangeTs !== undefined) {
      const latency = record.tsIngest - exchangeTs;
      if (latency > this.options.latencySpikeMs) {
        const evt: LatencySpikeDetected = {
          meta: createMeta('storage'),
          symbol: payload.symbol,
          streamId,
          topic: 'market:oi',
          latencyMs: latency,
          tsExchange: exchangeTs,
          tsIngest: record.tsIngest,
          thresholdMs: this.options.latencySpikeMs,
        };
        this.bus.publish('data:latencySpike', evt);
      }
    }

    this.lastOiTs.set(key, ts);
  }

  private detectFundingQuality(payload: FundingRateEvent, record: JournalRecord): void {
    const ts = payload.exchangeTs ?? payload.meta.tsExchange ?? payload.meta.tsEvent;
    const key = this.qualityKey(payload.symbol, payload.streamId, 'market:funding');
    const streamId = this.payloadStreamId(payload);
    const prev = this.lastFundingTs.get(key);
    if (prev !== undefined) {
      if (ts < prev) {
        const evt: DataTimeOutOfOrder = {
          meta: createMeta('storage'),
          symbol: payload.symbol,
          streamId,
          topic: 'market:funding',
          prevTs: asTsMs(prev),
          currTs: asTsMs(ts),
          tsSource: payload.exchangeTs !== undefined || payload.meta.tsExchange !== undefined ? 'exchange' : 'event',
        };
        this.bus.publish('data:time_out_of_order', evt);
      } else if (ts === prev) {
        const evt: DataDuplicateDetected = {
          meta: createMeta('storage'),
          symbol: payload.symbol,
          streamId,
          topic: 'market:funding',
          tsExchange: ts,
        };
        this.bus.publish('data:duplicateDetected', evt);
      } else {
        const delta = ts - prev;
        if (delta > this.options.fundingGapMs) {
          const evt: DataGapDetected = {
            meta: createMeta('storage'),
            symbol: payload.symbol,
            streamId,
            topic: 'market:funding',
            prevTsExchange: prev,
            currTsExchange: ts,
            deltaMs: delta,
          };
          this.bus.publish('data:gapDetected', evt);
        }
      }
    }

    const exchangeTs = payload.exchangeTs;
    if (exchangeTs !== undefined) {
      const latency = record.tsIngest - exchangeTs;
      if (latency > this.options.latencySpikeMs) {
        const evt: LatencySpikeDetected = {
          meta: createMeta('storage'),
          symbol: payload.symbol,
          streamId,
          topic: 'market:funding',
          latencyMs: latency,
          tsExchange: exchangeTs,
          tsIngest: record.tsIngest,
          thresholdMs: this.options.latencySpikeMs,
        };
        this.bus.publish('data:latencySpike', evt);
      }
    }

    this.lastFundingTs.set(key, ts);
  }

  private qualityKey(symbol: string, streamId: string | undefined, scope: string): string {
    const normalizedStream = streamId && streamId.length > 0 ? streamId : this.options.streamId;
    return `${symbol}:${normalizedStream}:${scope}`;
  }

  private payloadStreamId(payload: { streamId?: string; meta?: { streamId?: string } }): string {
    return payload.streamId ?? payload.meta?.streamId ?? this.options.streamId;
  }


  private scheduleFlush(): void {
    if (this.queue.length >= this.options.maxBatchSize) {
      void this.flush();
      return;
    }
    if (!this.timer) {
      this.timer = setTimeout(() => {
        this.timer = undefined;
        void this.flush();
      }, this.options.flushIntervalMs);
    }
  }

  async flush(): Promise<void> {
    if (this.flushPromise) return this.flushPromise;

    this.flushPromise = (async () => {
      while (this.queue.length > 0) {
        const batch = this.queue.splice(0, this.options.maxBatchSize);
        for (const item of batch) {
          try {
            await fs.mkdir(path.dirname(item.path), { recursive: true });
            await fs.appendFile(item.path, item.line, 'utf8');
          } catch (error) {
            if (!this.failedPaths.has(item.path)) {
              this.failedPaths.add(item.path);
              const evt: StorageWriteFailed = {
                meta: createMeta('storage'),
                path: item.path,
                error,
              };
              this.bus.publish('storage:writeFailed', evt);
            }
          }
        }
      }
    })();

    try {
      await this.flushPromise;
    } finally {
      this.flushPromise = undefined;
      if (this.queue.length > 0) {
        await this.flush();
      }
    }
  }

  private dayString(ts: number): string {
    return formatUtcDate(ts);
  }

  private topicDir(topic: string): string {
    if (topic === 'market:orderbook_l2_snapshot' || topic === 'market:orderbook_l2_delta') {
      return 'market-orderbook-l2';
    }
    if (topic === 'market:orderbook_snapshot_raw' || topic === 'market:orderbook_delta_raw') {
      return 'market-orderbook-raw';
    }
    if (topic === 'market:trade') return 'market-trade';
    if (topic === 'market:trade_raw') return 'market-trade-raw';
    if (topic === 'market:oi') return 'market-oi';
    if (topic === 'market:open_interest_raw') return 'market-oi-raw';
    if (topic === 'market:funding') return 'market-funding';
    if (topic === 'market:funding_raw') return 'market-funding-raw';
    if (topic === 'market:mark_price_raw') return 'market-mark-price-raw';
    if (topic === 'market:index_price_raw') return 'market-index-price-raw';
    if (topic === 'market:liquidation_raw') return 'market-liquidation-raw';
    if (topic === 'market:candle_raw') return 'market-candle-raw';
    return topic.replace(':', '-');
  }
}

export function createEventJournal(bus: EventBus = eventBus, options: Partial<EventJournalOptions> = {}): EventJournal {
  return new EventJournal(bus, options);
}
