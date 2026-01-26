import { describe, expect, it } from 'vitest';
import { computeBybitReconnectDelayMs } from '../src/exchange/bybit/wsClient';

describe('Bybit WS backoff jitter', () => {
  it('computes deterministic delays for reconnect attempts', () => {
    expect(computeBybitReconnectDelayMs(1)).toBe(1443);
    expect(computeBybitReconnectDelayMs(2)).toBe(2443);
  });

  it('caps base delay at max', () => {
    const delay = computeBybitReconnectDelayMs(6, 30_000);
    expect(delay).toBeGreaterThanOrEqual(30_000);
    expect(delay).toBeLessThanOrEqual(30_499);
  });
});
