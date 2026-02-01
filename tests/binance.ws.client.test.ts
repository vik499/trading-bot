import { describe, expect, it } from 'vitest';
import { BinancePublicWsClient } from '../src/exchange/binance/wsClient';
import { eventBus, type KlineEvent, type LiquidationEvent, type TradeEvent } from '../src/core/events/EventBus';

describe('BinancePublicWsClient', () => {
  it('parses aggTrade, kline, and liquidation events', () => {
    const trades: TradeEvent[] = [];
    const klines: KlineEvent[] = [];
    const liquidations: LiquidationEvent[] = [];

    const onTrade = (evt: TradeEvent) => trades.push(evt);
    const onKline = (evt: KlineEvent) => klines.push(evt);
    const onLiq = (evt: LiquidationEvent) => liquidations.push(evt);

    eventBus.subscribe('market:trade', onTrade);
    eventBus.subscribe('market:kline', onKline);
    eventBus.subscribe('market:liquidation', onLiq);

    const client = new BinancePublicWsClient('wss://fstream.binance.com/ws', {
      streamId: 'binance.usdm.public',
      marketType: 'futures',
      supportsLiquidations: true,
    });

    (client as any).onMessage(
      JSON.stringify({
        e: 'aggTrade',
        s: 'BTCUSDT',
        p: '100',
        q: '0.5',
        T: 1700000000000,
        m: true,
        a: 42,
      })
    );

    (client as any).onMessage(
      JSON.stringify({
        e: 'kline',
        s: 'BTCUSDT',
        k: {
          t: 1700000000000,
          T: 1700000059999,
          o: '100',
          h: '110',
          l: '90',
          c: '105',
          v: '12',
          i: '1m',
          x: true,
        },
      })
    );

    (client as any).onMessage(
      JSON.stringify({
        e: 'forceOrder',
        E: 1700000007000,
        o: {
          s: 'BTCUSDT',
          S: 'SELL',
          p: '100',
          q: '0.25',
          T: 1700000007000,
        },
      })
    );

    eventBus.unsubscribe('market:trade', onTrade);
    eventBus.unsubscribe('market:kline', onKline);
    eventBus.unsubscribe('market:liquidation', onLiq);

    expect(trades).toHaveLength(1);
    expect(trades[0].symbol).toBe('BTCUSDT');
    expect(trades[0].side).toBe('Sell');
    expect(trades[0].price).toBe(100);

    expect(klines).toHaveLength(1);
    expect(klines[0].interval).toBe('1');
    expect(klines[0].tf).toBe('1m');
    expect(klines[0].meta.ts).toBe(1700000059999);

    expect(liquidations).toHaveLength(1);
    expect(liquidations[0].side).toBe('Sell');
    expect(liquidations[0].notionalUsd).toBeCloseTo(25, 8);
  });

  it('subscribes aggTrade for futures', () => {
    const client = new BinancePublicWsClient('wss://fstream.binance.com/ws', {
      marketType: 'futures',
    });

    client.subscribeTrades('BTCUSDT');

    const snap = (client as any).subscriptions.snapshot();
    const subscriptions = [...snap.desired, ...snap.active, ...snap.pending];
    expect(subscriptions).toContain('btcusdt@aggTrade');
  });
});
