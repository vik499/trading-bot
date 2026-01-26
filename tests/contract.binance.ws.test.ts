import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { BinancePublicWsClient } from '../src/exchange/binance/wsClient';
import { createTestEventBus } from '../src/core/events/testing';
import type { OrderbookLevel, OrderbookL2DeltaEvent, OrderbookL2SnapshotEvent, TradeEvent, KlineEvent, LiquidationEvent } from '../src/core/events/EventBus';

const load = (name: string) =>
  JSON.parse(readFileSync(path.join(__dirname, 'fixtures', 'exchanges', 'binance', name), 'utf-8')) as Record<string, unknown>;

const send = (client: BinancePublicWsClient, payload: Record<string, unknown>) => {
  (client as unknown as { onMessage: (raw: string) => void }).onMessage(JSON.stringify(payload));
};

const toLevels = (raw: unknown): OrderbookLevel[] => {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((row) => {
      if (!Array.isArray(row) || row.length < 2) return undefined;
      return { price: Number(row[0]), size: Number(row[1]) } as OrderbookLevel;
    })
    .filter((lvl): lvl is OrderbookLevel => Boolean(lvl && Number.isFinite(lvl.price) && Number.isFinite(lvl.size)));
};

describe('Binance WS contracts', () => {
  it('maps aggTrade to market:trade', () => {
    const bus = createTestEventBus();
    const client = new BinancePublicWsClient('ws://test', { eventBus: bus });
    const outputs: TradeEvent[] = [];
    bus.subscribe('market:trade', (evt) => outputs.push(evt));

    send(client, load('trade.json'));

    const last = outputs[0];
    expect(last.symbol).toBe('BTCUSDT');
    expect(last.side).toBe('Buy');
    expect(last.price).toBe(100.5);
    expect(last.size).toBe(0.1);
    expect(last.exchangeTs).toBe(1700000000000);
  });

  it('maps kline to market:kline', () => {
    const bus = createTestEventBus();
    const client = new BinancePublicWsClient('ws://test', { eventBus: bus });
    const outputs: KlineEvent[] = [];
    bus.subscribe('market:kline', (evt) => outputs.push(evt));

    send(client, load('kline.json'));

    const last = outputs[0];
    expect(last.symbol).toBe('BTCUSDT');
    expect(last.interval).toBe('1');
    expect(last.startTs).toBe(1700000000000);
    expect(last.endTs).toBe(1700000060000);
  });

  it('maps depthUpdate to orderbook snapshot + delta with injected snapshot', () => {
    const bus = createTestEventBus();
    const client = new BinancePublicWsClient('ws://test', { eventBus: bus });
    const snapshots: OrderbookL2SnapshotEvent[] = [];
    const deltas: OrderbookL2DeltaEvent[] = [];
    bus.subscribe('market:orderbook_l2_snapshot', (evt) => snapshots.push(evt));
    bus.subscribe('market:orderbook_l2_delta', (evt) => deltas.push(evt));

    const snapshotFixture = load('orderbook_snapshot.json') as { lastUpdateId: number; bids: unknown; asks: unknown };
    const depthState = (client as unknown as {
      depthState: Map<
        string,
        {
          snapshot?: { lastUpdateId: number; bids: OrderbookLevel[]; asks: OrderbookLevel[] };
          lastUpdateId?: number;
          buffered: Array<{ lastUpdateId: number }>;
          snapshotEmitted: boolean;
          snapshotInFlight: boolean;
        }
      >;
    }).depthState;

    depthState.set('BTCUSDT', {
      snapshot: {
        lastUpdateId: snapshotFixture.lastUpdateId,
        bids: toLevels(snapshotFixture.bids),
        asks: toLevels(snapshotFixture.asks),
      },
      lastUpdateId: snapshotFixture.lastUpdateId,
      buffered: [],
      snapshotEmitted: false,
      snapshotInFlight: false,
    });

    send(client, load('orderbook_delta.json'));

    expect(snapshots).toHaveLength(1);
    expect(deltas).toHaveLength(1);
    expect(snapshots[0].updateId).toBe(0);
    expect(deltas[0].updateId).toBe(1);
  });

  it('maps liquidation to market:liquidation', () => {
    const bus = createTestEventBus();
    const client = new BinancePublicWsClient('ws://test', { eventBus: bus, supportsLiquidations: true });
    const outputs: LiquidationEvent[] = [];
    bus.subscribe('market:liquidation', (evt) => outputs.push(evt));

    send(client, load('liquidation.json'));

    const last = outputs[0];
    expect(last.symbol).toBe('BTCUSDT');
    expect(last.side).toBe('Sell');
    expect(last.price).toBe(100);
    expect(last.size).toBe(1);
    expect(last.exchangeTs).toBe(1700000000000);
  });
});
