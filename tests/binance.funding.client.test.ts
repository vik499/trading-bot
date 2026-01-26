import { describe, it, expect, vi } from 'vitest';
import { BinancePublicRestClient } from '../src/exchange/binance/restClient';

describe('Binance REST premium index client', () => {
  it('builds request and parses funding response', async () => {
    const originalFetch = globalThis.fetch;
    const fetchSpy = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({
        symbol: 'BTCUSDT',
        lastFundingRate: '0.00025',
        nextFundingTime: 1700003600000,
        time: 1700000000000,
        markPrice: '64000.5',
        indexPrice: '63990.1',
      }),
      headers: { forEach: () => undefined },
    });
    (globalThis as any).fetch = fetchSpy;

    try {
      const client = new BinancePublicRestClient('https://fapi.binance.com');
      const res = await client.fetchPremiumIndex({ symbol: 'BTCUSDT' });

      const url = fetchSpy.mock.calls[0][0] as string;
      const parsed = new URL(url);
      expect(parsed.pathname).toBe('/fapi/v1/premiumIndex');
      expect(parsed.searchParams.get('symbol')).toBe('BTCUSDT');
      expect(res.fundingRate).toBe(0.00025);
      expect(res.ts).toBe(1700000000000);
      expect(res.nextFundingTs).toBe(1700003600000);
      expect(res.markPrice).toBe(64000.5);
      expect(res.indexPrice).toBe(63990.1);
    } finally {
      (globalThis as any).fetch = originalFetch;
    }
  });
});
