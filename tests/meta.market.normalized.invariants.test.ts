import { describe, expect, it } from 'vitest';
import {
  createMeta,
  type FundingRateEvent,
  type KlineEvent,
  type LiquidationEvent,
  type OpenInterestEvent,
  type OrderbookL2DeltaEvent,
  type OrderbookL2SnapshotEvent,
  type TickerEvent,
  type TradeEvent,
} from '../src/core/events/EventBus';

type NormalizedMarketEvent =
  | TickerEvent
  | KlineEvent
  | TradeEvent
  | OrderbookL2SnapshotEvent
  | OrderbookL2DeltaEvent
  | OpenInterestEvent
  | FundingRateEvent
  | LiquidationEvent;

const assertNormalizedInvariant = (evt: NormalizedMarketEvent) => {
  expect(evt.marketType).not.toBe('unknown');
  expect(typeof evt.meta.tsEvent).toBe('number');
  expect(typeof evt.meta.tsIngest).toBe('number');
  expect(evt.meta.streamId).toBe(evt.streamId);
};

describe('Normalized market event meta invariants', () => {
  it('enforces streamId/marketType/tsEvent/tsIngest invariants', () => {
    const baseMeta = (streamId: string, ts: number) =>
      createMeta('market', { tsEvent: ts, tsIngest: ts, streamId });

    const events: NormalizedMarketEvent[] = [
      {
        symbol: 'BTCUSDT',
        streamId: 'bybit.public.linear.v5',
        marketType: 'futures',
        lastPrice: '100',
        exchangeTs: 1_000,
        meta: baseMeta('bybit.public.linear.v5', 1_000),
      } satisfies TickerEvent,
      {
        symbol: 'BTCUSDT',
        streamId: 'bybit.public.linear.v5',
        marketType: 'futures',
        interval: '5',
        tf: '5m',
        startTs: 1_000,
        endTs: 1_300,
        open: 100,
        high: 101,
        low: 99,
        close: 100,
        volume: 1,
        meta: baseMeta('bybit.public.linear.v5', 1_300),
      } satisfies KlineEvent,
      {
        symbol: 'BTCUSDT',
        streamId: 'bybit.public.linear.v5',
        marketType: 'futures',
        tradeId: 't-1',
        side: 'Buy',
        price: 100,
        size: 1,
        tradeTs: 1_000,
        exchangeTs: 1_000,
        meta: baseMeta('bybit.public.linear.v5', 1_000),
      } satisfies TradeEvent,
      {
        symbol: 'BTCUSDT',
        streamId: 'bybit.public.linear.v5',
        marketType: 'futures',
        updateId: 1,
        exchangeTs: 1_000,
        bids: [{ price: 100, size: 1 }],
        asks: [{ price: 101, size: 1 }],
        meta: baseMeta('bybit.public.linear.v5', 1_000),
      } satisfies OrderbookL2SnapshotEvent,
      {
        symbol: 'BTCUSDT',
        streamId: 'bybit.public.linear.v5',
        marketType: 'futures',
        updateId: 2,
        exchangeTs: 1_100,
        bids: [{ price: 100, size: 1 }],
        asks: [{ price: 101, size: 1 }],
        meta: baseMeta('bybit.public.linear.v5', 1_100),
      } satisfies OrderbookL2DeltaEvent,
      {
        symbol: 'BTCUSDT',
        streamId: 'bybit.public.linear.v5',
        marketType: 'futures',
        openInterest: 1000,
        openInterestUnit: 'base',
        exchangeTs: 1_000,
        meta: baseMeta('bybit.public.linear.v5', 1_000),
      } satisfies OpenInterestEvent,
      {
        symbol: 'BTCUSDT',
        streamId: 'bybit.public.linear.v5',
        marketType: 'futures',
        fundingRate: 0.0001,
        exchangeTs: 1_000,
        meta: baseMeta('bybit.public.linear.v5', 1_000),
      } satisfies FundingRateEvent,
      {
        symbol: 'BTCUSDT',
        streamId: 'bybit.public.linear.v5',
        marketType: 'futures',
        side: 'Buy',
        price: 100,
        size: 1,
        notionalUsd: 100,
        exchangeTs: 1_000,
        meta: baseMeta('bybit.public.linear.v5', 1_000),
      } satisfies LiquidationEvent,
    ];

    events.forEach(assertNormalizedInvariant);
  });
});
