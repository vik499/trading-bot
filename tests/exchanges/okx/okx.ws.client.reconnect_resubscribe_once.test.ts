import { describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { OkxPublicWsClient } from '../../../src/exchange/okx/wsClient';

describe('OkxPublicWsClient reconnect resubscribe', () => {
  it('resubscribes once per key after reconnect', () => {
    const client = new OkxPublicWsClient('wss://ws.okx.com:8443/ws/v5/public', {
      streamId: 'okx.public.swap',
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
