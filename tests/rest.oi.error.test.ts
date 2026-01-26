import { describe, it, expect, vi } from 'vitest';
import { BybitDerivativesRestPoller, BybitRestError } from '../src/exchange/bybit/restClient';
import { createTestEventBus } from '../src/core/events/testing';
import { logger } from '../src/infra/logger';

describe('Bybit REST open interest error logging', () => {
  it('includes status, retCode, params, request id, and rate limits', async () => {
    const err = new BybitRestError('Bybit REST open interest returned error', {
      status: 400,
      statusText: 'Bad Request',
      retCode: 1001,
      retMsg: 'invalid symbol',
      endpoint: '/v5/market/open-interest',
      params: { category: 'linear', symbol: 'BTCUSDT', limit: '1' },
      requestId: 'req-123',
      rateLimit: { 'x-bapi-limit-status': '19', 'x-bapi-limit-reset': '10' },
    });

    const restClient = {
      fetchOpenInterest: vi.fn().mockRejectedValue(err),
      fetchFundingRate: vi.fn().mockResolvedValue({ fundingRate: 0.001, ts: 1700 }),
    };

    const poller = new BybitDerivativesRestPoller(restClient as any, createTestEventBus(), {
      oiIntervalMs: 60_000,
      fundingIntervalMs: 60_000,
    });

    const warnSpy = vi.spyOn(logger, 'warn');
    await (poller as any).pollOpenInterest('BTCUSDT');
    poller.stop();

    const logged = warnSpy.mock.calls.map((call) => call[0]).join(' ');
    expect(logged).toContain('open interest failed for BTCUSDT');
    expect(logged).toContain('status=400');
    expect(logged).toContain('retCode=1001');
    expect(logged).toContain('retMsg=invalid symbol');
    expect(logged).toContain('endpoint=/v5/market/open-interest');
    expect(logged).toContain('params=');
    expect(logged).toContain('requestId=req-123');
    expect(logged).toContain('rateLimit=');

    warnSpy.mockRestore();
  });
});
