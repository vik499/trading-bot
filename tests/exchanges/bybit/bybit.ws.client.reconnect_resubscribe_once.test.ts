import { describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { BybitPublicWsClient } from '../../../src/exchange/bybit/wsClient';

describe('BybitPublicWsClient reconnect resubscribe', () => {
  it('resubscribes once per key after reconnect', () => {
    const client = new BybitPublicWsClient('wss://stream.bybit.com/v5/public/linear', {
      streamId: 'bybit.public.linear.v5',
      marketType: 'futures',
      supportsLiquidations: true,
    });

    client.subscribeTrades('BTCUSDT');
    client.subscribeTicker('BTCUSDT');
    client.subscribeOrderbook('BTCUSDT');

    const sent: Array<{ op?: string; args?: string[]; req_id?: string }> = [];
    (client as any).socket = {
      readyState: WebSocket.OPEN,
      send: (msg: string) => sent.push(JSON.parse(msg) as { op?: string; args?: string[] }),
    };

    (client as any).flushSubscriptions();
    const firstReqId = sent[0]?.req_id;
    (client as any).handleMessage(
      JSON.stringify({ success: true, ret_msg: '', req_id: firstReqId, op: 'subscribe' })
    );

    (client as any).cleanupSocket('test-close');
    (client as any).socket = {
      readyState: WebSocket.OPEN,
      send: (msg: string) => sent.push(JSON.parse(msg) as { op?: string; args?: string[] }),
    };
    (client as any).flushSubscriptions();
    const secondReqId = sent[1]?.req_id;
    (client as any).handleMessage(
      JSON.stringify({ success: true, ret_msg: '', req_id: secondReqId, op: 'subscribe' })
    );

    expect(sent).toHaveLength(2);
    expect(sent[0]?.args).toHaveLength(3);
    expect(sent[1]?.args).toHaveLength(3);
  });
});
