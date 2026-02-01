import { describe, expect, it, vi } from 'vitest';
import WebSocket from 'ws';
import { BybitPublicWsClient } from '../../../src/exchange/bybit/wsClient';

describe('BybitPublicWsClient subscribe ack handling', () => {
  it('marks active only after ack', () => {
    const client = new BybitPublicWsClient('wss://stream.bybit.com/v5/public/linear', {
      streamId: 'bybit.public.linear.v5',
      marketType: 'futures',
    });

    const sent: Array<{ op?: string; args?: string[]; req_id?: string }> = [];
    (client as any).socket = {
      readyState: WebSocket.OPEN,
      send: (msg: string) => sent.push(JSON.parse(msg) as { op?: string; args?: string[]; req_id?: string }),
    };

    client.subscribeTrades('BTCUSDT');

    const before = (client as any).subscriptions.counts();
    expect(before.pending).toBe(1);
    expect(before.active).toBe(0);

    const reqId = sent[0]?.req_id;
    (client as any).handleMessage(
      JSON.stringify({ success: true, ret_msg: '', req_id: reqId, op: 'subscribe' })
    );

    const after = (client as any).subscriptions.counts();
    expect(after.pending).toBe(0);
    expect(after.active).toBe(1);
  });

  it('closes socket on subscribe ack timeout', () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);

    const client = new BybitPublicWsClient('wss://stream.bybit.com/v5/public/linear', {
      streamId: 'bybit.public.linear.v5',
      marketType: 'futures',
    });

    const close = vi.fn();
    (client as any).socket = {
      readyState: WebSocket.OPEN,
      send: () => {
        // noop
      },
      close,
    };

    client.subscribeTrades('BTCUSDT');

    vi.advanceTimersByTime(8_001);

    expect(close).toHaveBeenCalledTimes(1);

    vi.useRealTimers();
  });
});
