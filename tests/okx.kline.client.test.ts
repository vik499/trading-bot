import { describe, expect, it, vi } from 'vitest';
import { OkxPublicRestClient } from '../src/exchange/okx/restClient';

describe('OKX REST kline client', () => {
  it('builds request and parses klines', async () => {
    const originalFetch = globalThis.fetch;
    const fetchSpy = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({
        code: '0',
        msg: '',
        data: [
          ['1700000000000', '100', '110', '90', '105', '12'],
        ],
      }),
      headers: { forEach: () => undefined },
    });
    (globalThis as any).fetch = fetchSpy;

    try {
      const client = new OkxPublicRestClient('https://www.okx.com');
      const res = await client.fetchKlines({
        symbol: 'BTCUSDT',
        interval: '1',
        limit: 1,
        sinceTs: 1700000000000,
      });

      const url = fetchSpy.mock.calls[0][0] as string;
      const parsed = new URL(url);
      expect(parsed.pathname).toBe('/api/v5/market/candles');
      expect(parsed.searchParams.get('instId')).toBe('BTC-USDT-SWAP');
      expect(parsed.searchParams.get('bar')).toBe('1m');
      expect(parsed.searchParams.get('after')).toBe('1700000000000');

      expect(res.klines).toHaveLength(1);
      expect(res.klines[0].startTs).toBe(1700000000000);
      expect(res.klines[0].endTs).toBe(1700000000000 + 60_000);
      expect(res.klines[0].close).toBe(105);
    } finally {
      (globalThis as any).fetch = originalFetch;
    }
  });
});
