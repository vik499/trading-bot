import { describe, it, expect, vi } from 'vitest';
import { OkxPublicRestClient } from '../src/exchange/okx/restClient';

describe('OKX REST funding rate client', () => {
  it('builds request and parses funding response', async () => {
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
            fundingRate: '0.00015',
            fundingTime: '1700000000000',
            nextFundingTime: '1700003600000',
            ts: '1700000005000',
          },
        ],
      }),
      headers: { forEach: () => undefined },
    });
    (globalThis as any).fetch = fetchSpy;

    try {
      const client = new OkxPublicRestClient('https://www.okx.com');
      const res = await client.fetchFundingRate({ instId: 'BTC-USDT-SWAP' });

      const url = fetchSpy.mock.calls[0][0] as string;
      const parsed = new URL(url);
      expect(parsed.pathname).toBe('/api/v5/public/funding-rate');
      expect(parsed.searchParams.get('instId')).toBe('BTC-USDT-SWAP');
      expect(res.fundingRate).toBe(0.00015);
      expect(res.fundingTime).toBe(1700000000000);
      expect(res.nextFundingTime).toBe(1700003600000);
      expect(res.ts).toBe(1700000005000);
    } finally {
      (globalThis as any).fetch = originalFetch;
    }
  });
});
