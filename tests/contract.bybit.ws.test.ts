import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { BybitPublicWsClient } from '../src/exchange/bybit/wsClient';
import { createTestEventBus } from '../src/core/events/testing';
import type { OrderbookL2DeltaEvent, OrderbookL2SnapshotEvent, TradeEvent, KlineEvent, LiquidationEvent } from '../src/core/events/EventBus';

const load = (name: string) =>
  JSON.parse(readFileSync(path.join(__dirname, 'fixtures', 'exchanges', 'bybit', name), 'utf-8')) as Record<string, unknown>;

const send = (client: BybitPublicWsClient, payload: Record<string, unknown>) => {
  (client as unknown as { handleMessage: (raw: string) => void }).handleMessage(JSON.stringify(payload));
};

describe('Bybit WS contracts', () => {
  it('maps trades to market:trade', () => {
    const bus = createTestEventBus();
    const client = new BybitPublicWsClient('ws://test', { eventBus: bus });
    const outputs: TradeEvent[] = [];
    bus.subscribe('market:trade', (evt) => outputs.push(evt));

    send(client, load('trade.json'));

    const last = outputs[0];
    expect(last.symbol).toBe('BTCUSDT');
    expect(last.side).toBe('Buy');
    expect(last.price).toBe(100);
    expect(last.size).toBe(1);
    expect(last.exchangeTs).toBe(1700000000000);
  });

  it('maps kline to market:kline', () => {
    const bus = createTestEventBus();
    const client = new BybitPublicWsClient('ws://test', { eventBus: bus });
    const outputs: KlineEvent[] = [];
    bus.subscribe('market:kline', (evt) => outputs.push(evt));

    send(client, load('kline.json'));

    const last = outputs[0];
    expect(last.symbol).toBe('BTCUSDT');
    expect(last.interval).toBe('1');
    expect(last.startTs).toBe(1700000000000);
    expect(last.endTs).toBe(1700000060000);
  });

  it('maps orderbook snapshot + delta', () => {
    const bus = createTestEventBus();
    const client = new BybitPublicWsClient('ws://test', { eventBus: bus });
    const snapshots: OrderbookL2SnapshotEvent[] = [];
    const deltas: OrderbookL2DeltaEvent[] = [];
    bus.subscribe('market:orderbook_l2_snapshot', (evt) => snapshots.push(evt));
    bus.subscribe('market:orderbook_l2_delta', (evt) => deltas.push(evt));

    send(client, load('orderbook_snapshot.json'));
    send(client, load('orderbook_delta.json'));

    expect(snapshots).toHaveLength(1);
    expect(deltas).toHaveLength(1);
    expect(snapshots[0].updateId).toBe(1);
    expect(deltas[0].updateId).toBe(2);
  });

  it('maps liquidation to market:liquidation', () => {
    const bus = createTestEventBus();
    const client = new BybitPublicWsClient('ws://test', { eventBus: bus, supportsLiquidations: true });
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
