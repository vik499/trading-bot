import { describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { BinancePublicWsClient } from '../../../src/exchange/binance/wsClient';

describe('BinancePublicWsClient subscribe dedup', () => {
  it('deduplicates subscribe frames on reconcile', () => {
    const client = new BinancePublicWsClient('wss://fstream.binance.com/ws', {
      streamId: 'binance.usdm.public',
      marketType: 'futures',
      supportsLiquidations: true,
      instanceId: 'test-1',
    });

    const sent: string[] = [];
    (client as any).socket = {
      readyState: WebSocket.OPEN,
      send: (msg: string) => sent.push(msg),
    };

    client.subscribeTrades('BTCUSDT');
    client.subscribeTrades('BTCUSDT');
    (client as any).flushSubscriptions();
    (client as any).flushSubscriptions();

    expect(sent).toHaveLength(1);
  });
});
