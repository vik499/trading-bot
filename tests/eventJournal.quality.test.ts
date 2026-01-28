import { describe, expect, it } from 'vitest';
import { asTsMs, createMeta, type OrderbookL2DeltaEvent, type OrderbookL2SnapshotEvent, type TradeEvent } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { EventJournal } from '../src/storage/eventJournal';

function makeTrade(ts: number, streamId: string): TradeEvent {
  return {
    symbol: 'BTCUSDT',
    streamId,
    side: 'Buy',
    price: 100,
    size: 1,
    tradeTs: ts,
    exchangeTs: ts,
    meta: createMeta('market', { tsEvent: asTsMs(ts), tsIngest: asTsMs(ts + 5), streamId }),
  } as TradeEvent;
}

function makeSnapshot(ts: number, updateId: number, streamId: string): OrderbookL2SnapshotEvent {
  return {
    symbol: 'BTCUSDT',
    streamId,
    updateId,
    exchangeTs: ts,
    bids: [{ price: 100, size: 1 }],
    asks: [{ price: 101, size: 1 }],
    meta: createMeta('market', { tsEvent: asTsMs(ts), tsIngest: asTsMs(ts + 1), tsExchange: asTsMs(ts), streamId }),
  } as OrderbookL2SnapshotEvent;
}

function makeDelta(ts: number, updateId: number, streamId: string): OrderbookL2DeltaEvent {
  return {
    symbol: 'BTCUSDT',
    streamId,
    updateId,
    exchangeTs: ts,
    bids: [{ price: 100, size: 2 }],
    asks: [{ price: 101, size: 1 }],
    meta: createMeta('market', { tsEvent: asTsMs(ts), tsIngest: asTsMs(ts + 1), tsExchange: asTsMs(ts), streamId }),
  } as OrderbookL2DeltaEvent;
}

describe('EventJournal quality signals', () => {
  it('does not emit out-of-order for interleaved venues', () => {
    const bus = createTestEventBus();
    const journal = new EventJournal(bus, { gapMs: 500, latencySpikeMs: 10_000 });
    const outOfOrder: string[] = [];
    bus.subscribe('data:time_out_of_order', (evt) => outOfOrder.push(evt.topic));
    journal.start();

    bus.publish('market:trade', makeTrade(2000, 'binance.usdm.public'));
    bus.publish('market:trade', makeTrade(1500, 'bybit.public.linear.v5'));
    bus.publish('market:trade', makeTrade(2500, 'binance.usdm.public'));

    expect(outOfOrder).toHaveLength(0);
    journal.stop();
  });

  it('skips sequence gap checks when meta.sequence is absent', () => {
    const bus = createTestEventBus();
    const journal = new EventJournal(bus, { gapMs: 500, latencySpikeMs: 10_000 });
    const seqIssues: string[] = [];
    bus.subscribe('data:sequence_gap_or_out_of_order', (evt) => seqIssues.push(evt.kind));
    journal.start();

    bus.publish('market:orderbook_l2_snapshot', makeSnapshot(1000, 1000, 'okx.public.swap'));
    bus.publish('market:orderbook_l2_delta', makeDelta(3000, 3000, 'okx.public.swap'));

    expect(seqIssues).toHaveLength(0);
    journal.stop();
  });
});
