import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { OkxPublicWsClient } from '../src/exchange/okx/wsClient';
import { CvdCalculator } from '../src/globalData/CvdCalculator';
import { CvdAggregator } from '../src/globalData/CvdAggregator';
import { getOkxSwapCtVal } from '../src/exchange/okx/instrumentMeta';
import { createTestEventBus } from '../src/core/events/testing';
import {
  createMeta,
  type MarketCvdAggEvent,
  type OrderbookL2DeltaEvent,
  type OrderbookL2SnapshotEvent,
  type TradeEvent,
  type KlineEvent,
  type LiquidationEvent,
} from '../src/core/events/EventBus';

const load = (name: string) =>
  JSON.parse(readFileSync(path.join(__dirname, 'fixtures', 'exchanges', 'okx', name), 'utf-8')) as Record<string, unknown>;

const send = (client: OkxPublicWsClient, payload: Record<string, unknown>) => {
  (client as unknown as { onMessage: (raw: string) => void }).onMessage(JSON.stringify(payload));
};

describe('OKX WS contracts', () => {
  it('maps trades to market:trade', () => {
    const bus = createTestEventBus();
    const client = new OkxPublicWsClient('ws://test', { eventBus: bus });
    const outputs: TradeEvent[] = [];
    bus.subscribe('market:trade', (evt) => outputs.push(evt));

    send(client, load('trade.json'));

    const last = outputs[0];
    expect(last.symbol).toBe('BTCUSDT');
    expect(last.side).toBe('Buy');
    expect(last.price).toBe(100);
    expect(last.size).toBe(0.01);
    expect(last.exchangeTs).toBe(1700000000000);
  });

  it('reads ctVal from OKX instrument metadata', () => {
    expect(getOkxSwapCtVal('BTC-USDT-SWAP')).toBe(0.01);
  });

  it('normalizes OKX swap trade size to base units for CVD alignment', () => {
    const bus = createTestEventBus();
    const calc = new CvdCalculator(bus, { bucketMs: 1000, marketTypes: ['futures'] });
    const agg = new CvdAggregator(bus, { ttlMs: 10_000 });
    const outputs: MarketCvdAggEvent[] = [];

    bus.subscribe('market:cvd_futures_agg', (evt) => outputs.push(evt));
    calc.start();
    agg.start();

    const makeTrade = (streamId: string, ts: number, size: number): TradeEvent => ({
      symbol: 'BTCUSDT',
      streamId,
      side: 'Buy',
      price: 100,
      size,
      tradeTs: ts,
      exchangeTs: ts,
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: ts, tsIngest: ts, streamId }),
    });

    const tsA = 1_000;
    const tsB = 2_100;

    bus.publish('market:trade', makeTrade('binance.usdm.public', tsA, 1));
    bus.publish('market:trade', makeTrade('bybit.public.linear.v5', tsA, 1));

    let now = tsA;
    const client = new OkxPublicWsClient('ws://test', {
      streamId: 'okx.public.swap',
      marketType: 'futures',
      eventBus: bus,
      now: () => now,
    });

    send(client, {
      arg: { channel: 'trades', instId: 'BTC-USDT-SWAP' },
      data: [{ tradeId: '1', px: '100', sz: '100', side: 'buy', ts: String(tsA) }],
    });

    bus.publish('market:trade', makeTrade('binance.usdm.public', tsB, 1));
    bus.publish('market:trade', makeTrade('bybit.public.linear.v5', tsB, 1));

    now = tsB;
    send(client, {
      arg: { channel: 'trades', instId: 'BTC-USDT-SWAP' },
      data: [{ tradeId: '2', px: '100', sz: '1', side: 'buy', ts: String(tsB) }],
    });

    const bucketEvents = outputs.filter((evt) => evt.bucketStartTs === 1_000);
    expect(bucketEvents.length).toBeGreaterThan(0);
    const last = bucketEvents[bucketEvents.length - 1];
    expect(last.mismatchDetected).toBe(false);
    expect(last.venueBreakdown?.['binance.usdm.public']).toBeCloseTo(1, 8);
    expect(last.venueBreakdown?.['bybit.public.linear.v5']).toBeCloseTo(1, 8);
    expect(last.venueBreakdown?.['okx.public.swap']).toBeCloseTo(1, 8);
    expect(last.confidenceScore).toBeGreaterThan(0.9);
    expect(last.confidenceScore).not.toBeCloseTo(0.475, 3);

    calc.stop();
    agg.stop();
  });

  it('maps klines to market:kline', () => {
    const bus = createTestEventBus();
    const client = new OkxPublicWsClient('ws://test', { eventBus: bus });
    const outputs: KlineEvent[] = [];
    bus.subscribe('market:kline', (evt) => outputs.push(evt));

    send(client, load('kline.json'));

    const last = outputs[0];
    expect(last.symbol).toBe('BTCUSDT');
    expect(last.interval).toBe('1');
    expect(last.startTs).toBe(1700000000000);
  });

  it('maps orderbook snapshot + delta', () => {
    const bus = createTestEventBus();
    const client = new OkxPublicWsClient('ws://test', { eventBus: bus });
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
    const client = new OkxPublicWsClient('ws://test', { eventBus: bus, supportsLiquidations: true });
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
