import { describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { BinancePublicWsClient } from '../../../src/exchange/binance/wsClient';

describe('BinancePublicWsClient reconcile coalesce', () => {
  it('coalesces repeated reconcile calls', () => {
    const client = new BinancePublicWsClient('wss://fstream.binance.com/ws', {
      streamId: 'binance.usdm.public',
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
