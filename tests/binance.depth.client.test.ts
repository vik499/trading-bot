import { describe, expect, it, vi } from 'vitest';
import { BinancePublicRestClient } from '../src/exchange/binance/restClient';

describe('Binance REST depth client', () => {
  it('builds request and parses depth snapshot', async () => {
    const originalFetch = globalThis.fetch;
    const fetchSpy = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({
        lastUpdateId: 123,
        bids: [
          ['100', '1.5'],
          ['99', '0.5'],
        ],
        asks: [
          ['101', '2'],
          ['102', '0.25'],
        ],
      }),
      headers: { forEach: () => undefined },
    });
    (globalThis as any).fetch = fetchSpy;

    try {
      const client = new BinancePublicRestClient('https://fapi.binance.com');
      const res = await client.fetchDepthSnapshot({ symbol: 'BTCUSDT', limit: 500 });

      const url = fetchSpy.mock.calls[0][0] as string;
      const parsed = new URL(url);
      expect(parsed.pathname).toBe('/fapi/v1/depth');
      expect(parsed.searchParams.get('symbol')).toBe('BTCUSDT');
      expect(parsed.searchParams.get('limit')).toBe('500');

      expect(res.lastUpdateId).toBe(123);
      expect(res.bids[0]).toEqual({ price: 100, size: 1.5 });
      expect(res.asks[0]).toEqual({ price: 101, size: 2 });
    } finally {
      (globalThis as any).fetch = originalFetch;
    }
  });
});
