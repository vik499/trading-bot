import { describe, expect, it } from 'vitest';
import { BybitPublicWsClient } from '../src/exchange/bybit/wsClient';
import { eventBus, type LiquidationEvent } from '../src/core/events/EventBus';

describe('BybitPublicWsClient', () => {
  it('parses allLiquidation payloads', () => {
    const liquidations: LiquidationEvent[] = [];
    const onLiq = (evt: LiquidationEvent) => liquidations.push(evt);
    eventBus.subscribe('market:liquidation', onLiq);

    const client = new BybitPublicWsClient('wss://stream.bybit.com/v5/public/linear', {
      streamId: 'bybit.public.linear.v5',
      marketType: 'futures',
      supportsLiquidations: true,
    });

    (client as any).handleMessage(
      JSON.stringify({
        topic: 'allLiquidation.BTCUSDT',
        type: 'snapshot',
        ts: 1700000000000,
        data: [
          {
            T: 1700000000001,
            s: 'BTCUSDT',
            S: 'Sell',
            v: '2',
            p: '100',
          },
        ],
      })
    );

    eventBus.unsubscribe('market:liquidation', onLiq);

    expect(liquidations).toHaveLength(1);
    expect(liquidations[0].symbol).toBe('BTCUSDT');
    expect(liquidations[0].side).toBe('Sell');
    expect(liquidations[0].price).toBe(100);
    expect(liquidations[0].size).toBe(2);
    expect(liquidations[0].exchangeTs).toBe(1700000000001);
  });
});
