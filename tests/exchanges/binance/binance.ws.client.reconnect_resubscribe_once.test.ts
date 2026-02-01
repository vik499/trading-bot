import { describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { BinancePublicWsClient } from '../../../src/exchange/binance/wsClient';

describe('BinancePublicWsClient reconnect resubscribe', () => {
  it('resubscribes once per key after reconnect', () => {
    const client = new BinancePublicWsClient('wss://fstream.binance.com/ws', {
      streamId: 'binance.usdm.public',
      marketType: 'futures',
      supportsLiquidations: true,
      instanceId: 'test-2',
    });

    const sent: string[] = [];
    (client as any).socket = {
      readyState: WebSocket.OPEN,
      send: (msg: string) => sent.push(msg),
    };

    client.subscribeTrades('BTCUSDT');
    (client as any).flushSubscriptions();

    (client as any).subscriptions.onClose();
    (client as any).subscriptions.onOpen();
    (client as any).flushSubscriptions();
    (client as any).flushSubscriptions();

    expect(sent).toHaveLength(2);
  });
});
