import { describe, it, expect, vi } from 'vitest';
import { BybitPublicRestClient } from '../src/exchange/bybit/restClient';

describe('Bybit REST open interest client', () => {
  it('includes intervalTime param and parses openInterest response', async () => {
    const originalFetch = globalThis.fetch;
    const fetchSpy = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({
        retCode: 0,
        retMsg: 'OK',
        result: {
          list: [{ openInterest: '123.4', timestamp: '1700000000000' }],
        },
      }),
      headers: { forEach: () => undefined },
    });
    (globalThis as any).fetch = fetchSpy;

    try {
      const client = new BybitPublicRestClient('https://api.bybit.com');
      const res = await client.fetchOpenInterest({ symbol: 'BTCUSDT', intervalTime: '5min' });

      const url = fetchSpy.mock.calls[0][0] as string;
      const parsed = new URL(url);
      expect(parsed.searchParams.get('intervalTime')).toBe('5min');
      expect(res.openInterest).toBe(123.4);
      expect(res.ts).toBe(1700000000000);
    } finally {
      (globalThis as any).fetch = originalFetch;
    }
  });
});
