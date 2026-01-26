import { describe, expect, it, vi } from 'vitest';
import WebSocket from 'ws';
import { BinancePublicWsClient } from '../src/exchange/binance/wsClient';

describe('BinancePublicWsClient reliability', () => {
  it('deduplicates duplicate subscriptions', () => {
    const client = new BinancePublicWsClient('wss://fstream.binance.com/ws', { marketType: 'futures' });
    const send = vi.fn();
    (client as any).socket = { readyState: WebSocket.OPEN, send };

    client.subscribeTrades('BTCUSDT');
    client.subscribeTrades('BTCUSDT');

    expect(send).toHaveBeenCalledTimes(1);
  });

  it('applies minimum cooldown on rate limit closes', () => {
    const client = new BinancePublicWsClient('wss://fstream.binance.com/ws', { marketType: 'futures' });
    vi.useFakeTimers();

    (client as any).scheduleReconnect('close', 1008);
    const delay = (client as any).lastReconnectDelayMs as number;

    expect(delay).toBeGreaterThanOrEqual(5_000);

    if ((client as any).reconnectTimer) {
      clearTimeout((client as any).reconnectTimer);
    }
    vi.useRealTimers();
  });
});
