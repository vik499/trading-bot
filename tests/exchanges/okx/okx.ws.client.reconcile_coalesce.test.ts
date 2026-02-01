import { describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { OkxPublicWsClient } from '../../../src/exchange/okx/wsClient';

describe('OkxPublicWsClient reconcile coalesce', () => {
  it('coalesces repeated reconcile calls', () => {
    const client = new OkxPublicWsClient('wss://ws.okx.com:8443/ws/v5/public', {
      streamId: 'okx.public.swap',
      marketType: 'futures',
      supportsLiquidations: true,
      instanceId: 'test-3',
    });

    const sent: string[] = [];
    (client as any).socket = {
      readyState: WebSocket.OPEN,
      send: (msg: string) => sent.push(msg),
    };

    (client as any).reconcileInFlight = true;
    client.subscribeTrades('BTCUSDT');
    (client as any).flushSubscriptions();
    (client as any).flushSubscriptions();

    expect(sent).toHaveLength(0);

    (client as any).reconcileInFlight = false;
    (client as any).flushSubscriptions();

    expect(sent).toHaveLength(1);
  });
});
