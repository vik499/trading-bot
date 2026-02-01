import { describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { BybitPublicWsClient } from '../../../src/exchange/bybit/wsClient';

describe('BybitPublicWsClient subscribe dedup', () => {
  it('deduplicates queued subscriptions on open', () => {
    const client = new BybitPublicWsClient('wss://stream.bybit.com/v5/public/linear', {
      streamId: 'bybit.public.linear.v5',
      marketType: 'futures',
      supportsLiquidations: true,
    });

    client.subscribeTrades('BTCUSDT');
    client.subscribeTrades('BTCUSDT');

    const sent: Array<{ op?: string; args?: string[]; req_id?: string }> = [];
    (client as any).socket = {
      readyState: WebSocket.OPEN,
      send: (msg: string) => sent.push(JSON.parse(msg) as { op?: string; args?: string[] }),
    };

    (client as any).flushSubscriptions();

    expect(sent).toHaveLength(1);
    expect(sent[0]?.op).toBe('subscribe');
    expect(sent[0]?.args).toEqual(['publicTrade.BTCUSDT']);
    expect(typeof sent[0]?.req_id).toBe('string');
    (client as any).handleMessage(
      JSON.stringify({ success: true, ret_msg: '', req_id: sent[0]?.req_id, op: 'subscribe' })
    );
  });

  it('deduplicates subscriptions while open', () => {
    const client = new BybitPublicWsClient('wss://stream.bybit.com/v5/public/linear', {
      streamId: 'bybit.public.linear.v5',
      marketType: 'futures',
      supportsLiquidations: true,
    });

    const sent: Array<{ op?: string; args?: string[]; req_id?: string }> = [];
    (client as any).socket = {
      readyState: WebSocket.OPEN,
      send: (msg: string) => sent.push(JSON.parse(msg) as { op?: string; args?: string[] }),
    };

    client.subscribeTicker('BTCUSDT');
    client.subscribeTicker('BTCUSDT');

    expect(sent).toHaveLength(1);
    expect(sent[0]?.op).toBe('subscribe');
    expect(sent[0]?.args).toEqual(['tickers.BTCUSDT']);
    expect(typeof sent[0]?.req_id).toBe('string');
    (client as any).handleMessage(
      JSON.stringify({ success: true, ret_msg: '', req_id: sent[0]?.req_id, op: 'subscribe' })
    );
  });
});
