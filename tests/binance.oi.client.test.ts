import { describe, it, expect, vi } from 'vitest';
import { BinancePublicRestClient } from '../src/exchange/binance/restClient';

describe('Binance REST open interest client', () => {
  it('builds request and parses open interest response', async () => {
    const originalFetch = globalThis.fetch;
    const fetchSpy = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({
        symbol: 'BTCUSDT',
        openInterest: '123.45',
        time: 1700000000000,
      }),
      headers: { forEach: () => undefined },
    });
    (globalThis as any).fetch = fetchSpy;

    try {
      const client = new BinancePublicRestClient('https://fapi.binance.com');
      const res = await client.fetchOpenInterest({ symbol: 'BTCUSDT' });

      const url = fetchSpy.mock.calls[0][0] as string;
      const parsed = new URL(url);
      expect(parsed.pathname).toBe('/fapi/v1/openInterest');
      expect(parsed.searchParams.get('symbol')).toBe('BTCUSDT');
      expect(res.openInterest).toBe(123.45);
      expect(res.ts).toBe(1700000000000);
    } finally {
      (globalThis as any).fetch = originalFetch;
    }
  });
});
