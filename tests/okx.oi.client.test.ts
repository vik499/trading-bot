import { describe, it, expect, vi } from 'vitest';
import { OkxPublicRestClient } from '../src/exchange/okx/restClient';

describe('OKX REST open interest client', () => {
  it('builds request and parses open interest response', async () => {
    const originalFetch = globalThis.fetch;
    const fetchSpy = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({
        code: '0',
        msg: '',
        data: [
          {
            instId: 'BTC-USDT-SWAP',
            oi: '5000',
            oiCcy: '10.5',
            oiUsd: '300000',
            ts: '1700000000000',
          },
        ],
      }),
      headers: { forEach: () => undefined },
    });
    (globalThis as any).fetch = fetchSpy;

    try {
      const client = new OkxPublicRestClient('https://www.okx.com');
      const res = await client.fetchOpenInterest({ instId: 'BTC-USDT-SWAP' });

      const url = fetchSpy.mock.calls[0][0] as string;
      const parsed = new URL(url);
      expect(parsed.pathname).toBe('/api/v5/public/open-interest');
      expect(parsed.searchParams.get('instType')).toBe('SWAP');
      expect(parsed.searchParams.get('instId')).toBe('BTC-USDT-SWAP');
      expect(res.openInterest).toBe(10.5);
      expect(res.openInterestUnit).toBe('base');
      expect(res.openInterestValueUsd).toBe(300000);
      expect(res.ts).toBe(1700000000000);
    } finally {
      (globalThis as any).fetch = originalFetch;
    }
  });
});
