import { describe, expect, it } from 'vitest';
import { OkxPublicWsClient } from '../src/exchange/okx/wsClient';
import { eventBus, type KlineEvent, type LiquidationEvent, type TradeEvent } from '../src/core/events/EventBus';

describe('OkxPublicWsClient', () => {
  it('parses trades, klines, and liquidations', () => {
    const trades: TradeEvent[] = [];
    const klines: KlineEvent[] = [];
    const liquidations: LiquidationEvent[] = [];

    const onTrade = (evt: TradeEvent) => trades.push(evt);
    const onKline = (evt: KlineEvent) => klines.push(evt);
    const onLiq = (evt: LiquidationEvent) => liquidations.push(evt);

    eventBus.subscribe('market:trade', onTrade);
    eventBus.subscribe('market:kline', onKline);
    eventBus.subscribe('market:liquidation', onLiq);

    const client = new OkxPublicWsClient('wss://ws.okx.com:8443/ws/v5/public', {
      streamId: 'okx.public.swap',
      marketType: 'futures',
      supportsLiquidations: true,
    });

    (client as any).onMessage(
      JSON.stringify({
        arg: { channel: 'trades', instId: 'BTC-USDT-SWAP' },
        data: [
          { px: '100', sz: '2', side: 'buy', ts: '1700000000000' },
        ],
      })
    );

    (client as any).onMessage(
      JSON.stringify({
        arg: { channel: 'candle1m', instId: 'BTC-USDT-SWAP' },
        data: [
          ['1700000000000', '100', '110', '90', '105', '12'],
        ],
      })
    );

    (client as any).onMessage(
      JSON.stringify({
        arg: { channel: 'liquidation-orders', instId: 'BTC-USDT-SWAP' },
        data: [
          { px: '100', sz: '3', side: 'sell', ts: '1700000005000' },
        ],
      })
    );

    eventBus.unsubscribe('market:trade', onTrade);
    eventBus.unsubscribe('market:kline', onKline);
    eventBus.unsubscribe('market:liquidation', onLiq);

    expect(trades).toHaveLength(1);
    expect(trades[0].symbol).toBe('BTCUSDT');
    expect(trades[0].side).toBe('Buy');
    expect(trades[0].price).toBe(100);

    expect(klines).toHaveLength(1);
    expect(klines[0].symbol).toBe('BTCUSDT');
    expect(klines[0].interval).toBe('1');
    expect(klines[0].endTs).toBe(1700000000000 + 60_000);

    expect(liquidations).toHaveLength(1);
    expect(liquidations[0].side).toBe('Sell');
    expect(liquidations[0].notionalUsd).toBeCloseTo(300, 8);
  });
});
