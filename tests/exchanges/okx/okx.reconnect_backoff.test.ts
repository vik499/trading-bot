import { describe, expect, it, vi } from 'vitest';
import { OkxPublicWsClient } from '../../../src/exchange/okx/wsClient';

describe('OkxPublicWsClient reconnect backoff', () => {
  it('backs off exponentially and resets after stable window', async () => {
    vi.useFakeTimers();

    const client = new OkxPublicWsClient('wss://ws.okx.com:8443/ws/v5/public', {
      reconnectBaseMs: 500,
      reconnectMaxMs: 8000,
      backoffResetMs: 1000,
    });

    const connectSpy = vi.spyOn(client as any, 'connect').mockResolvedValue(undefined);

    const delays: number[] = [];
    for (let i = 0; i < 4; i += 1) {
      (client as any).scheduleReconnect('close');
      delays.push((client as any).getLastReconnectDelayMs());
      await vi.runOnlyPendingTimersAsync();
    }

    expect(delays.length).toBe(4);
    for (let i = 1; i < delays.length; i += 1) {
      expect(delays[i]).toBeGreaterThanOrEqual(delays[i - 1]);
      expect(delays[i]).toBeLessThanOrEqual(8000);
    }

    (client as any).scheduleStableReset();
    await vi.runOnlyPendingTimersAsync();

    (client as any).scheduleReconnect('close');
    const resetDelay = (client as any).getLastReconnectDelayMs();
    expect(resetDelay).toBeGreaterThanOrEqual(500);
    expect(resetDelay).toBeLessThanOrEqual(8000);

    connectSpy.mockRestore();
    vi.useRealTimers();
  });
});
