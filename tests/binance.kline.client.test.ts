import { describe, expect, it, vi } from 'vitest';
import { BinancePublicRestClient } from '../src/exchange/binance/restClient';

describe('Binance REST kline client', () => {
  it('builds request and parses klines', async () => {
    const originalFetch = globalThis.fetch;
    const fetchSpy = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ([
        [1700000000000, '100', '110', '90', '105', '12', 1700000059999],
        [1700000060000, '105', '115', '95', '110', '15', 1700000119999],
      ]),
      headers: { forEach: () => undefined },
    });
    (globalThis as any).fetch = fetchSpy;

    try {
      const client = new BinancePublicRestClient('https://fapi.binance.com');
      const res = await client.fetchKlines({
        symbol: 'BTCUSDT',
        interval: '5',
        limit: 2,
        sinceTs: 1700000000000,
      });

      const url = fetchSpy.mock.calls[0][0] as string;
      const parsed = new URL(url);
      expect(parsed.pathname).toBe('/fapi/v1/klines');
      expect(parsed.searchParams.get('symbol')).toBe('BTCUSDT');
      expect(parsed.searchParams.get('interval')).toBe('5m');
      expect(parsed.searchParams.get('limit')).toBe('2');
      expect(parsed.searchParams.get('startTime')).toBe('1700000000000');

      expect(res.klines).toHaveLength(2);
      expect(res.klines[0].open).toBe(100);
      expect(res.klines[0].close).toBe(105);
      expect(res.klines[0].endTs).toBe(1700000059999);
    } finally {
      (globalThis as any).fetch = originalFetch;
    }
  });
});
